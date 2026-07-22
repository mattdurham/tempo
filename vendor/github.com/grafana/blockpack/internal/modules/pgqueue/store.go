package pgqueue

import (
	"context"
	_ "embed"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/grafana/blockpack/internal/pgschema"
)

// JobType identifies which compaction-worker handler a job dispatches to.
type JobType string

// Job types compaction-worker's dispatch switch branches on.
const (
	JobTypeViCompaction     JobType = "vi_compaction"
	JobTypeVcntCompaction   JobType = "vcnt_compaction"
	JobTypeCubeCompaction   JobType = "cube_compaction"
	JobTypeTraceCompaction  JobType = "trace_compaction"
	JobTypeCatalogReconcile JobType = "catalog_reconcile"
	JobTypeCatalogReap      JobType = "catalog_reap"
	JobTypeViBackfill       JobType = "vi_backfill"
	JobTypeCubeBackfill     JobType = "cube_backfill"
)

// Status is a compaction_jobs row's lifecycle state.
type Status string

// Lifecycle states a compaction_jobs row moves through.
const (
	StatusPending   Status = "pending"
	StatusClaimed   Status = "claimed"
	StatusRunning   Status = "running"
	StatusSucceeded Status = "succeeded"
	StatusFailed    Status = "failed"
)

// Job is one compaction_jobs row, decoded for Go callers. Detail is left as
// raw JSON -- callers unmarshal into whichever detail struct their JobType
// implies, mirroring tempo's jobstore.Job.
type Job struct {
	ID        string
	Type      JobType
	Subsystem string
	Tenant    string
	Status    Status
	Detail    json.RawMessage
	Retries   int
}

//go:embed schema.sql
var compactionJobsSchemaSQL string

// ApplyCompactionJobsSchema applies compaction_jobs' schema against pool.
// Exported, never called automatically by any constructor -- the embedding
// binary (cmd/compaction-planner, cmd/compaction-worker) calls it once at its
// own startup.
func ApplyCompactionJobsSchema(ctx context.Context, pool *pgxpool.Pool) error {
	return pgschema.ApplyStatements(ctx, pool, compactionJobsSchemaSQL)
}

// Store is the Postgres-backed compaction_jobs implementation.
type Store struct{ pool *pgxpool.Pool }

// New constructs a Store over pool.
func New(pool *pgxpool.Pool) *Store {
	return &Store{pool: pool}
}

// insertJobSQL's ON CONFLICT predicate must match idx_compaction_jobs_dedup_active
// exactly, including value order -- Postgres's conflict target inference
// matches a partial unique index by parse-tree equality of its predicate, not
// just column list (mirrors tempo jobstore.go's own insertJobSQL comment).
//
// priority is included explicitly (not left to its DEFAULT 0) so insertWithPriority can share
// this one statement for every caller -- Insert's own callers all pass 0, preserving their
// exact current FIFO-by-created_at behavior.
const insertJobSQL = `
	INSERT INTO compaction_jobs (id, job_type, subsystem, tenant, status, detail, dedup_key, priority)
	VALUES ($1, $2, $3, $4, 'pending', $5, $6, $7)
	ON CONFLICT (dedup_key) WHERE status != 'succeeded' DO NOTHING`

// Insert inserts a pending job at priority 0 (today's plain FIFO-by-created_at behavior).
// Idempotent: if a non-terminal job for the same dedupKey already exists, this is a silent
// no-op -- the INSERT either succeeds or is absorbed by the partial unique index, in one round
// trip, no separate check-then-insert race window.
func (s *Store) Insert(ctx context.Context, jobType JobType, subsystem, tenant, dedupKey string, detail any) error {
	return s.insertWithPriority(ctx, jobType, subsystem, tenant, dedupKey, detail, 0)
}

func (s *Store) insertWithPriority(
	ctx context.Context, jobType JobType, subsystem, tenant, dedupKey string, detail any, priority int64,
) error {
	rawDetail, err := json.Marshal(detail)
	if err != nil {
		return fmt.Errorf("pgqueue: marshal %s detail: %w", jobType, err)
	}
	if _, err := s.pool.Exec(
		ctx, insertJobSQL, uuid.NewString(), string(jobType), subsystem, tenant, rawDetail, dedupKey, priority,
	); err != nil {
		return fmt.Errorf("pgqueue: insert %s job: %w", jobType, err)
	}
	return nil
}

// InsertCubeBackfill mirrors the old InsertViBackfill, deduped on
// job_type+tenant+cubeID.
func (s *Store) InsertCubeBackfill(ctx context.Context, tenant string, d CubeBackfillDetail) error {
	dedupKey := string(JobTypeCubeBackfill) + "|" + tenant + "|" + d.CubeID
	return s.Insert(ctx, JobTypeCubeBackfill, "cube", tenant, dedupKey, d)
}

// ViBackfillColumn identifies the (tenant, column) a batch of vi_backfill windows covers -- the
// parts of ViBackfillDetail that stay constant across every window in one InsertViBackfillWindows
// call.
type ViBackfillColumn struct {
	ColumnHash string
	ColumnName string
	ColumnType string
}

// WindowSpec is one [StartSec, EndSec) vi_backfill job to insert. Priority is the row's claim
// priority (0 = highest/newest; see claimJobSQL's ORDER BY and schema.sql's priority column
// doc comment for why higher values can never starve other job types).
type WindowSpec struct {
	StartSec int64
	EndSec   int64
	Priority int64
}

// viBackfillDedupKey is one row per (column, 1-minute window) pair -- unlike the old
// column-wide InsertViBackfill dedup key, so the SAME column can have many independently
// claimable, independently idempotent jobs in flight at once (issue #529).
func viBackfillDedupKey(tenant, columnHash, columnType string, windowEndSec int64) string {
	return fmt.Sprintf("%s|%s|%s|%s|%d", JobTypeViBackfill, tenant, columnHash, columnType, windowEndSec)
}

// maxViBackfillWindowsPerInsert bounds each multi-row INSERT statement's size. Chunking a large
// InsertViBackfillWindows call (e.g. bulk-inserting a month of 1-minute windows, tens of
// thousands of rows) keeps any single statement's parameter count and lock duration bounded,
// at the cost of more round trips -- an acceptable tradeoff since this only runs once per
// newly-triggered column (plus small, frequent trailing-window batches thereafter).
const maxViBackfillWindowsPerInsert = 1000

// viBackfillInsertParamsPerRow is id, job_type, subsystem, tenant, detail, dedup_key, priority,
// column_hash, column_type, window_start_sec, window_end_sec -- every compaction_jobs column
// this insert populates except the literal 'pending' status. The last four are a
// query-optimization-only denormalized copy of fields already inside detail -- see
// idx_vi_backfill_coverage_gap's own doc comment in schema.sql for why they exist as real,
// indexed columns instead of only living in the JSONB blob.
const viBackfillInsertParamsPerRow = 11

// InsertViBackfillWindows bulk-inserts one pending vi_backfill job per window in windows,
// chunked to stay under maxViBackfillWindowsPerInsert rows per statement. Idempotent per row,
// exactly like Insert -- a window whose dedup key already exists (non-succeeded) is a silent
// no-op, so this is safe to call repeatedly for the same trailing range every planner tick
// (issue #529's "ongoing coverage" mechanism) without needing a separate watermark to track
// what was already enqueued.
func (s *Store) InsertViBackfillWindows(
	ctx context.Context,
	tenant string,
	col ViBackfillColumn,
	windows []WindowSpec,
) error {
	for start := 0; start < len(windows); start += maxViBackfillWindowsPerInsert {
		end := start + maxViBackfillWindowsPerInsert
		if end > len(windows) {
			end = len(windows)
		}
		if err := s.insertViBackfillWindowChunk(ctx, tenant, col, windows[start:end]); err != nil {
			return fmt.Errorf("pgqueue: insert vi_backfill windows [%d:%d): %w", start, end, err)
		}
	}
	return nil
}

func (s *Store) insertViBackfillWindowChunk(
	ctx context.Context,
	tenant string,
	col ViBackfillColumn,
	windows []WindowSpec,
) error {
	if len(windows) == 0 {
		return nil
	}

	valuesSQL := make([]string, 0, len(windows))
	args := make([]any, 0, len(windows)*viBackfillInsertParamsPerRow)
	for i, w := range windows {
		detail := ViBackfillDetail{
			ColumnHash: col.ColumnHash, ColumnName: col.ColumnName, ColumnType: col.ColumnType,
			WindowStartSec: w.StartSec, WindowEndSec: w.EndSec,
		}
		rawDetail, err := json.Marshal(detail)
		if err != nil {
			return fmt.Errorf("marshal detail for window ending %d: %w", w.EndSec, err)
		}
		base := i * viBackfillInsertParamsPerRow
		valuesSQL = append(valuesSQL, fmt.Sprintf(
			"($%d,$%d,$%d,$%d,'pending',$%d,$%d,$%d,$%d,$%d,$%d,$%d)",
			base+1, base+2, base+3, base+4, base+5, base+6, base+7, base+8, base+9, base+10, base+11,
		))
		args = append(
			args,
			uuid.NewString(), string(JobTypeViBackfill), "vi", tenant, rawDetail,
			viBackfillDedupKey(tenant, col.ColumnHash, col.ColumnType, w.EndSec), w.Priority,
			col.ColumnHash, col.ColumnType, w.StartSec, w.EndSec,
		)
	}

	query := "INSERT INTO compaction_jobs " +
		"(id, job_type, subsystem, tenant, status, detail, dedup_key, priority, column_hash, column_type, window_start_sec, window_end_sec) " +
		"VALUES " + strings.Join(valuesSQL, ",") + " ON CONFLICT (dedup_key) WHERE status != 'succeeded' DO NOTHING"
	if _, err := s.pool.Exec(ctx, query, args...); err != nil {
		return fmt.Errorf("pgqueue: batch insert vi_backfill windows: %w", err)
	}
	return nil
}

// WindowRange is one contiguous [StartSec, EndSec) span. ViBackfillGapRanges returns a
// tenant/column's NOT-yet-succeeded windows merged into the minimal number of these.
type WindowRange struct {
	StartSec int64
	EndSec   int64
}

const viBackfillGapRangesSQL = `
	SELECT window_start_sec, window_end_sec
	FROM compaction_jobs
	WHERE job_type = 'vi_backfill' AND status != 'succeeded'
	  AND tenant = $1 AND column_hash = $2 AND column_type = $3
	ORDER BY window_start_sec`

// ViBackfillGapRanges returns every NOT-yet-succeeded window for (tenant, columnHash,
// columnType), merged into the minimal number of contiguous [StartSec, EndSec) ranges (issue
// #529) -- the real, query-time-efficient replacement for a single scalar watermark, which
// cannot represent a column whose 1-minute windows complete out of order across many parallel
// workers (a genuinely older window can finish after a genuinely newer one). An empty result
// means fully covered (every enqueued window for this column has succeeded); the caller's own
// Triggered check still gates "never indexed at all" separately, exactly like the old
// watermark-based CoversRange did.
//
// Backed by idx_vi_backfill_coverage_gap -- see that index's own doc comment in schema.sql for
// why window_start_sec/window_end_sec are real, indexed columns rather than only living in
// detail's JSONB blob.
func (s *Store) ViBackfillGapRanges(ctx context.Context, tenant string, col ViBackfillColumn) ([]WindowRange, error) {
	rows, err := s.pool.Query(ctx, viBackfillGapRangesSQL, tenant, col.ColumnHash, col.ColumnType)
	if err != nil {
		return nil, fmt.Errorf("pgqueue: vi_backfill gap ranges: %w", err)
	}
	defer rows.Close()

	var merged []WindowRange
	for rows.Next() {
		var r WindowRange
		if err := rows.Scan(&r.StartSec, &r.EndSec); err != nil {
			return nil, fmt.Errorf("pgqueue: scan vi_backfill gap range: %w", err)
		}
		if n := len(merged); n > 0 && merged[n-1].EndSec >= r.StartSec {
			// Adjacent or overlapping with the previous range (ORDER BY window_start_sec
			// guarantees ranges arrive in a merge-friendly order) -- extend it instead of
			// appending a new one.
			if r.EndSec > merged[n-1].EndSec {
				merged[n-1].EndSec = r.EndSec
			}
			continue
		}
		merged = append(merged, r)
	}
	if rows.Err() != nil {
		return nil, fmt.Errorf("pgqueue: iterate vi_backfill gap ranges: %w", rows.Err())
	}
	return merged, nil
}

// claimJobSQL is the standard single-statement SKIP LOCKED claim idiom: find
// the highest-priority, oldest claimable row of ANY job_type (fresh pending, OR claimed/running
// with an expired lease -- crashed-worker self-heal, OR failed with a due
// retry), lock it, mark it claimed, all in one round trip. SKIP LOCKED means N
// concurrent callers never block each other and never claim the same row.
// Deliberately no job_type filter -- mirrors the "any pod, any job" claim
// philosophy (Section G.2).
//
// ORDER BY priority ASC, created_at ASC (issue #529): every job type except vi_backfill's bulk
// windows leaves priority at its DEFAULT 0, so this is IDENTICAL to the old plain
// "ORDER BY created_at" for them -- ties on priority=0 still resolve oldest-first. Only
// vi_backfill's newest-window-first ordinal priorities change behavior, and only relative to
// each other and to fresh priority-0 work of any type, never by starving anyone.
const claimJobSQL = `
	UPDATE compaction_jobs
	SET status = 'claimed', claimed_by = $1, claimed_at = now(),
	    lease_expires_at = now() + interval '30 minutes'
	WHERE id = (
		SELECT id FROM compaction_jobs
		WHERE status = 'pending'
		   OR (status = 'claimed' AND lease_expires_at < now())
		   OR (status = 'running' AND lease_expires_at < now())
		   OR (status = 'failed' AND next_retry_at IS NOT NULL AND next_retry_at <= now())
		ORDER BY priority ASC, created_at ASC
		FOR UPDATE SKIP LOCKED
		LIMIT 1
	)
	RETURNING id, job_type, subsystem, tenant, detail, retries`

// Claim atomically claims and returns the oldest claimable job of any type
// for workerID, or (nil, nil) if none exists -- not an error.
func (s *Store) Claim(ctx context.Context, workerID string) (*Job, error) {
	row := s.pool.QueryRow(ctx, claimJobSQL, workerID)
	var (
		id, jobType, subsystem, tenant string
		detail                         []byte
		retries                        int
	)
	err := row.Scan(&id, &jobType, &subsystem, &tenant, &detail, &retries)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("pgqueue: claim job: %w", err)
	}
	return &Job{
		ID: id, Type: JobType(jobType), Subsystem: subsystem, Tenant: tenant,
		Status: StatusClaimed, Detail: detail, Retries: retries,
	}, nil
}

// renewLeaseSQL extends jobID's lease by another 30 minutes from the CURRENT
// now(), not the original claim time. The status guard makes this a no-op
// once the job has reached a terminal state.
const renewLeaseSQL = `
	UPDATE compaction_jobs
	SET lease_expires_at = now() + interval '30 minutes'
	WHERE id = $1 AND status IN ('claimed', 'running')`

// RenewLease extends jobID's lease, preventing Claim's expired-lease clause
// from letting a second worker reclaim it while the original worker is still
// actively processing it. Callers are expected to invoke this periodically
// (well under the 30-minute lease TTL) for the duration of active processing.
func (s *Store) RenewLease(ctx context.Context, jobID string) error {
	if _, err := s.pool.Exec(ctx, renewLeaseSQL, jobID); err != nil {
		return fmt.Errorf("pgqueue: renew lease %s: %w", jobID, err)
	}
	return nil
}

const completeJobSQL = `UPDATE compaction_jobs SET status = 'succeeded', finished_at = now() WHERE id = $1`

// Complete marks jobID succeeded.
func (s *Store) Complete(ctx context.Context, jobID string) error {
	if _, err := s.pool.Exec(ctx, completeJobSQL, jobID); err != nil {
		return fmt.Errorf("pgqueue: complete job %s: %w", jobID, err)
	}
	return nil
}

// Fail records a failed attempt for jobID and unconditionally schedules a
// retry (next_retry_at set per backoffDuration, status stays 'failed' but is
// claimable again per Claim's retry-due clause) -- a transient failure has no
// reason to ever stop retrying; the exponential backoff (capped at 30m)
// already bounds how much worker capacity a persistently-failing job can
// consume, without ever giving up on it outright (mirrors tempo jobstore.go's
// identical, deliberate design).
func (s *Store) Fail(ctx context.Context, jobID, errMsg string) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("pgqueue: fail: begin: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	var retries int
	row := tx.QueryRow(ctx, `SELECT retries FROM compaction_jobs WHERE id = $1 FOR UPDATE`, jobID)
	if err := row.Scan(&retries); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return fmt.Errorf("pgqueue: fail: job %s not found", jobID)
		}
		return fmt.Errorf("pgqueue: fail: load retries: %w", err)
	}

	newRetries := retries + 1
	nextRetryAt := time.Now().Add(backoffDuration(newRetries))

	if _, err := tx.Exec(ctx, `
		UPDATE compaction_jobs
		SET status = 'failed', last_error = $2, retries = $3, next_retry_at = $4
		WHERE id = $1`, jobID, errMsg, newRetries, nextRetryAt); err != nil {
		return fmt.Errorf("pgqueue: fail: update: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("pgqueue: fail: commit: %w", err)
	}
	return nil
}

// backoffDuration returns the delay before retry attempt retryAttempt
// (1-indexed: the first retry after the initial failure is attempt 1).
// Doubles from a 1-minute base, capped at 30 minutes.
func backoffDuration(retryAttempt int) time.Duration {
	d := time.Minute * time.Duration(int64(1)<<retryAttempt)
	if d > 30*time.Minute {
		return 30 * time.Minute
	}
	return d
}
