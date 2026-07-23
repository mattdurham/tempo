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
	// string(rawDetail), not the raw []byte: under simple_protocol query mode (required for
	// pgbouncer transaction pooling, see schema.sql/deploy config comments), pgx encodes a []byte
	// argument as a bytea hex literal, which Postgres then rejects trying to cast into detail's
	// jsonb column ("invalid input syntax for type json") -- a plain Go string is sent as a text
	// literal instead, which Postgres CAN implicitly cast to jsonb.
	if _, err := s.pool.Exec(
		ctx, insertJobSQL, uuid.NewString(), string(jobType), subsystem, tenant, string(rawDetail), dedupKey, priority,
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

// ViBackfillColumn identifies the (tenant, column) a batch of vi_backfill blocks covers -- the
// parts of ViBackfillDetail that stay constant across every block in one InsertViBackfillBlocks
// call.
type ViBackfillColumn struct {
	ColumnHash string
	ColumnName string
	ColumnType string
}

// BlockSpec is one vi_backfill block-shaped job to insert (issue #532): exactly one real trace
// block, identified by its own object key, replacing the old synthetic 1-minute WindowSpec.
// MinSec/MaxSec are the block's own real wall-clock time range (from blockpack_file_catalog),
// stored into the row's window_start_sec/window_end_sec columns so ViBackfillGapRanges' existing
// time-range coverage query keeps answering correctly across both job shapes without needing to
// know which one produced a given row. Priority is the row's claim-ordering tiebreaker -- same
// "0 = newest, increasing for older" convention the old WindowSpec used, just computed from the
// block's own MinSec instead of a synthetic minute-index ordinal.
type BlockSpec struct {
	ObjectKey string
	MinSec    int64
	MaxSec    int64
	Priority  int64
	// SizeBytes is the block's known object size (pgcatalog.Row.SizeBytes), threaded into
	// ViBackfillDetail.BlockSizeBytes -- see that field's own doc comment for why.
	SizeBytes int64
}

// viBackfillBlockDedupKey is one row per exact block (issue #533) -- column identity no longer
// participates in job dedup at all now that columns are membership rows in
// vi_backfill_job_columns, not part of the parent job's identity, so N columns triggered for the
// SAME block all collapse onto exactly one parent row instead of N independently-dedup'd rows.
func viBackfillBlockDedupKey(tenant, blockObjectKey string) string {
	return fmt.Sprintf("%s|%s|%s", JobTypeViBackfill, tenant, blockObjectKey)
}

// maxViBackfillBlocksPerInsert bounds each multi-row INSERT statement's size, mirroring the old
// maxViBackfillWindowsPerInsert's identical chunking rationale.
const maxViBackfillBlocksPerInsert = 1000

// viBackfillBlockInsertParamsPerRow is id, job_type, subsystem, tenant, detail, dedup_key,
// priority, window_start_sec, window_end_sec, block_object_key -- every compaction_jobs column
// round trip 1 populates except the literal 'pending' status. column_hash/column_type/
// column_name no longer appear here (issue #533) -- they move to round trip 2's child-row insert.
const viBackfillBlockInsertParamsPerRow = 10

// viBackfillJobColumnsInsertParamsPerRow is job_id, column_hash, column_type, column_name --
// round trip 2's per-row params (status is the literal 'pending').
const viBackfillJobColumnsInsertParamsPerRow = 4

// InsertViBackfillBlocks bulk-inserts one pending vi_backfill job per block in blocks, chunked
// to stay under maxViBackfillBlocksPerInsert rows per statement (issue #532). Idempotent per
// row, exactly like Insert -- a block whose dedup key already exists (non-succeeded) is a
// silent no-op, so this is safe to call repeatedly for the same candidate block set every
// planner tick without needing a separate watermark. Callers should filter through
// MissingViBackfillBlocks first (status-agnostic) so an already-SUCCEEDED block is never
// re-inserted as a duplicate row -- the partial unique dedup index only protects against
// non-succeeded duplicates, mirroring the old window-shaped model's identical requirement.
func (s *Store) InsertViBackfillBlocks(
	ctx context.Context,
	tenant string,
	col ViBackfillColumn,
	blocks []BlockSpec,
) error {
	for start := 0; start < len(blocks); start += maxViBackfillBlocksPerInsert {
		end := start + maxViBackfillBlocksPerInsert
		if end > len(blocks) {
			end = len(blocks)
		}
		if err := s.insertViBackfillBlockChunk(ctx, tenant, col, blocks[start:end]); err != nil {
			return fmt.Errorf("pgqueue: insert vi_backfill blocks [%d:%d): %w", start, end, err)
		}
	}
	return nil
}

// insertViBackfillBlockChunk is the get-or-create-parent-then-insert-child bulk insert path
// (issue #533): one explicit transaction, exactly 2 round trips.
//
// Round trip 1 gets-or-creates each block's parent compaction_jobs row via a genuine
// ON CONFLICT ... DO UPDATE (not DO NOTHING) -- a well-known Postgres idiom for "get-or-create
// with a row lock, returning the id either way, in one INSERT statement." Because it's a real
// UPDATE, Postgres always returns a row via RETURNING whether the row was freshly inserted or
// already existed, AND takes a row-level lock on that row for the WHOLE TRANSACTION's duration
// (not just this one statement) -- this is the mechanism that makes the parent-row lock
// (SPEC-PGQUEUE-9) actually work: a concurrent ViBackfillFinalize's own FOR UPDATE on the same row
// blocks until this transaction commits, and vice versa. Every other column (detail, priority,
// etc.) is left completely untouched on conflict (only dedup_key is "updated," to its own
// existing value) -- an existing job's priority stays "set exactly once."
//
// Round trip 2 bulk-inserts the child membership rows using round trip 1's id mapping.
// col.ColumnHash/col.ColumnType/col.ColumnName are identical across every tuple in this statement
// (InsertViBackfillBlocks is always scoped to one column already); only job_id varies per block.
func (s *Store) insertViBackfillBlockChunk(
	ctx context.Context,
	tenant string,
	col ViBackfillColumn,
	blocks []BlockSpec,
) error {
	if len(blocks) == 0 {
		return nil
	}

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("pgqueue: insert vi_backfill block chunk: begin: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	jobIDByDedupKey, err := insertViBackfillParentJobs(ctx, tx, tenant, blocks)
	if err != nil {
		return err
	}
	if err := insertViBackfillJobColumns(ctx, tx, tenant, col, blocks, jobIDByDedupKey); err != nil {
		return err
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("pgqueue: insert vi_backfill block chunk: commit: %w", err)
	}
	return nil
}

// insertViBackfillParentJobs is insertViBackfillBlockChunk's round trip 1 -- see that function's
// own doc comment for the get-or-create-with-lock rationale. Returns each block's job id, keyed
// by that block's own dedup key.
func insertViBackfillParentJobs(
	ctx context.Context, tx pgx.Tx, tenant string, blocks []BlockSpec,
) (map[string]string, error) {
	valuesSQL := make([]string, 0, len(blocks))
	args := make([]any, 0, len(blocks)*viBackfillBlockInsertParamsPerRow)
	for i, b := range blocks {
		detail := ViBackfillDetail{
			BlockObjectKey: b.ObjectKey, BlockSizeBytes: b.SizeBytes,
			WindowStartSec: b.MinSec, WindowEndSec: b.MaxSec,
		}
		rawDetail, err := json.Marshal(detail)
		if err != nil {
			return nil, fmt.Errorf("marshal detail for block %q: %w", b.ObjectKey, err)
		}
		base := i * viBackfillBlockInsertParamsPerRow
		valuesSQL = append(valuesSQL, fmt.Sprintf(
			"($%d,$%d,$%d,$%d,'pending',$%d,$%d,$%d,$%d,$%d,$%d)",
			base+1, base+2, base+3, base+4, base+5, base+6, base+7, base+8, base+9, base+10,
		))
		args = append(
			args,
			// string(rawDetail): see insertWithPriority's identical comment -- []byte encodes as
			// a bytea literal under simple_protocol, which Postgres can't cast to jsonb.
			uuid.NewString(), string(JobTypeViBackfill), "vi", tenant, string(rawDetail),
			viBackfillBlockDedupKey(tenant, b.ObjectKey), b.Priority, b.MinSec, b.MaxSec, b.ObjectKey,
		)
	}

	query := "INSERT INTO compaction_jobs " +
		"(id, job_type, subsystem, tenant, status, detail, dedup_key, priority, window_start_sec, window_end_sec, block_object_key) " +
		"VALUES " + strings.Join(valuesSQL, ",") +
		" ON CONFLICT (dedup_key) WHERE status != 'succeeded' DO UPDATE SET dedup_key = EXCLUDED.dedup_key" +
		" RETURNING id, dedup_key"
	rows, err := tx.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("pgqueue: get-or-create vi_backfill parent jobs: %w", err)
	}
	defer rows.Close()

	jobIDByDedupKey := make(map[string]string, len(blocks))
	for rows.Next() {
		var id, dedupKey string
		if err := rows.Scan(&id, &dedupKey); err != nil {
			return nil, fmt.Errorf("pgqueue: scan vi_backfill parent job: %w", err)
		}
		jobIDByDedupKey[dedupKey] = id
	}
	if rows.Err() != nil {
		return nil, fmt.Errorf("pgqueue: iterate vi_backfill parent jobs: %w", rows.Err())
	}
	return jobIDByDedupKey, nil
}

// insertViBackfillJobColumns is insertViBackfillBlockChunk's round trip 2 -- bulk-inserts col's
// child membership row for every block's parent job id from round trip 1.
func insertViBackfillJobColumns(
	ctx context.Context, tx pgx.Tx, tenant string, col ViBackfillColumn, blocks []BlockSpec, jobIDByDedupKey map[string]string,
) error {
	valuesSQL := make([]string, 0, len(blocks))
	args := make([]any, 0, len(blocks)*viBackfillJobColumnsInsertParamsPerRow)
	for i, b := range blocks {
		jobID, ok := jobIDByDedupKey[viBackfillBlockDedupKey(tenant, b.ObjectKey)]
		if !ok {
			return fmt.Errorf("pgqueue: no parent job id returned for block %q", b.ObjectKey)
		}
		base := i * viBackfillJobColumnsInsertParamsPerRow
		valuesSQL = append(valuesSQL, fmt.Sprintf("($%d,$%d,$%d,$%d,'pending')", base+1, base+2, base+3, base+4))
		args = append(args, jobID, col.ColumnHash, col.ColumnType, col.ColumnName)
	}

	query := "INSERT INTO vi_backfill_job_columns (job_id, column_hash, column_type, column_name, status) " +
		"VALUES " + strings.Join(valuesSQL, ",") +
		" ON CONFLICT (job_id, column_hash, column_type) DO NOTHING"
	if _, err := tx.Exec(ctx, query, args...); err != nil {
		return fmt.Errorf("pgqueue: batch insert vi_backfill job columns: %w", err)
	}
	return nil
}

const viBackfillPendingColumnsSQL = `
	SELECT column_hash, column_type, column_name
	FROM vi_backfill_job_columns
	WHERE job_id = $1 AND status != 'succeeded'`

// ViBackfillPendingColumns returns every still-pending column for jobID (issue #533) -- the
// column-loop primitive compactionworker.processViBackfillJob iterates to process a block-shaped
// job's remaining work.
func (s *Store) ViBackfillPendingColumns(ctx context.Context, jobID string) ([]ViBackfillColumn, error) {
	rows, err := s.pool.Query(ctx, viBackfillPendingColumnsSQL, jobID)
	if err != nil {
		return nil, fmt.Errorf("pgqueue: vi_backfill pending columns: %w", err)
	}
	defer rows.Close()

	var cols []ViBackfillColumn
	for rows.Next() {
		var c ViBackfillColumn
		if err := rows.Scan(&c.ColumnHash, &c.ColumnType, &c.ColumnName); err != nil {
			return nil, fmt.Errorf("pgqueue: scan vi_backfill pending column: %w", err)
		}
		cols = append(cols, c)
	}
	if rows.Err() != nil {
		return nil, fmt.Errorf("pgqueue: iterate vi_backfill pending columns: %w", rows.Err())
	}
	return cols, nil
}

const viBackfillMarkColumnDoneSQL = `
	UPDATE vi_backfill_job_columns
	SET status = 'succeeded', succeeded_at = now()
	WHERE job_id = $1 AND column_hash = $2 AND column_type = $3`

// ViBackfillMarkColumnDone marks jobID's (columnHash, columnType) child row succeeded (issue
// #533). Deliberately no transaction, no parent-row lock (SPEC-PGQUEUE-9's own "extraction/flush
// must never happen inside the row-locking transaction" contract): this call happens once per
// successfully-processed column, potentially minutes apart from the next one, and must never hold
// a lock on the parent row while that column's own extraction/flush I/O runs.
func (s *Store) ViBackfillMarkColumnDone(ctx context.Context, jobID, columnHash, columnType string) error {
	if _, err := s.pool.Exec(ctx, viBackfillMarkColumnDoneSQL, jobID, columnHash, columnType); err != nil {
		return fmt.Errorf("pgqueue: vi_backfill mark column done: %w", err)
	}
	return nil
}

const viBackfillMarkAllColumnsResolvedSQL = `
	UPDATE vi_backfill_job_columns
	SET status = 'succeeded', succeeded_at = now()
	WHERE job_id = $1 AND status != 'succeeded'`

// ViBackfillMarkAllColumnsResolved resolves every still-pending column for jobID in one pass
// (issue #533) -- used when the whole target block is gone (ErrObjectNotFound): a gone block has
// nothing left to index for ANY of its columns, mirroring SPEC-COMPACTIONWORKER-13's existing
// per-job terminal-success contract, now applied per-column. Idempotent: a column already
// succeeded from a prior partial run is simply not matched by the WHERE clause, left untouched.
func (s *Store) ViBackfillMarkAllColumnsResolved(ctx context.Context, jobID string) error {
	if _, err := s.pool.Exec(ctx, viBackfillMarkAllColumnsResolvedSQL, jobID); err != nil {
		return fmt.Errorf("pgqueue: vi_backfill mark all columns resolved: %w", err)
	}
	return nil
}

// ViBackfillFinalize is the race-closing serialization point (issue #533, refining the issue's
// own literal "lock the child rows" text -- see NOTES.md NOTE-PGQUEUE-VI-BLOCK-2 for the full
// Q3-derived rationale): takes a FOR UPDATE lock on the PARENT row FIRST, so it serializes against
// insertViBackfillBlockChunk's own DO-UPDATE-lock on the identical row (round trip 1 above) -- a
// row-level lock on rows that ALREADY EXIST cannot, by itself, stop a brand-new child-row INSERT
// from landing in the gap between a "read the child list" step and a "flip to succeeded" step;
// locking the PARENT is what actually closes that gap, because BOTH sides of the race contend for
// the identical row lock.
//
// Returns (true, nil) once the row is genuinely 'succeeded' (either just now, or already was --
// ErrNoRows on the initial lock query means "no non-succeeded row with this id," treated as
// already-finalized, not an error). Returns (false, nil) when pending children remain -- the
// caller (compactionworker.processViBackfillJob) must reprocess those columns and call this again;
// see SPEC-PGQUEUE-9.
func (s *Store) ViBackfillFinalize(ctx context.Context, jobID string) (bool, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return false, fmt.Errorf("pgqueue: vi_backfill finalize: begin: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	var lockedID string
	row := tx.QueryRow(ctx, `SELECT id FROM compaction_jobs WHERE id = $1 AND status != 'succeeded' FOR UPDATE`, jobID)
	if err := row.Scan(&lockedID); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return true, nil // already succeeded (or gone) -- nothing left to finalize
		}
		return false, fmt.Errorf("pgqueue: vi_backfill finalize: lock job %s: %w", jobID, err)
	}

	var hasPending bool
	hpRow := tx.QueryRow(
		ctx, `SELECT EXISTS (SELECT 1 FROM vi_backfill_job_columns WHERE job_id = $1 AND status != 'succeeded')`, jobID,
	)
	if err := hpRow.Scan(&hasPending); err != nil {
		return false, fmt.Errorf("pgqueue: vi_backfill finalize: check pending children %s: %w", jobID, err)
	}
	if hasPending {
		return false, tx.Commit(ctx) // release the lock; caller reprocesses and retries
	}

	if _, err := tx.Exec(
		ctx, `UPDATE compaction_jobs SET status = 'succeeded', finished_at = now() WHERE id = $1`, jobID,
	); err != nil {
		return false, fmt.Errorf("pgqueue: vi_backfill finalize: mark succeeded %s: %w", jobID, err)
	}
	if err := tx.Commit(ctx); err != nil {
		return false, fmt.Errorf("pgqueue: vi_backfill finalize: commit %s: %w", jobID, err)
	}
	return true, nil
}

// WindowRange is one contiguous [StartSec, EndSec) span. ViBackfillGapRanges returns a
// tenant/column's NOT-yet-succeeded windows merged into the minimal number of these.
type WindowRange struct {
	StartSec int64
	EndSec   int64
}

const viBackfillGapRangesSQL = `
	SELECT cj.window_start_sec, cj.window_end_sec
	FROM compaction_jobs cj
	JOIN vi_backfill_job_columns vc ON vc.job_id = cj.id
	WHERE cj.job_type = 'vi_backfill' AND cj.tenant = $1
	  AND vc.column_hash = $2 AND vc.column_type = $3 AND vc.status != 'succeeded'
	ORDER BY cj.window_start_sec`

// ViBackfillGapRanges returns every NOT-yet-succeeded window for (tenant, columnHash,
// columnType), merged into the minimal number of contiguous [StartSec, EndSec) ranges (issue
// #529) -- the real, query-time-efficient replacement for a single scalar watermark, which
// cannot represent a column whose 1-minute windows complete out of order across many parallel
// workers (a genuinely older window can finish after a genuinely newer one). An empty result
// means fully covered (every enqueued window for this column has succeeded); the caller's own
// Triggered check still gates "never indexed at all" separately, exactly like the old
// watermark-based CoversRange did.
//
// Issue #533: column identity moved off the parent row into vi_backfill_job_columns, so this is
// now a JOIN against that child table's own idx_vi_backfill_job_columns_lookup index instead of a
// direct filter on compaction_jobs.column_hash/column_type (both dropped; the old
// idx_vi_backfill_coverage_gap index that used to back this query is gone too).
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

const viBackfillExistingBlockKeysSQL = `
	SELECT cj.block_object_key
	FROM compaction_jobs cj
	JOIN vi_backfill_job_columns vc ON vc.job_id = cj.id
	WHERE cj.job_type = 'vi_backfill' AND cj.tenant = $1
	  AND vc.column_hash = $2 AND vc.column_type = $3
	  AND cj.block_object_key = ANY($4)`

// MissingViBackfillBlocks is the job-history gap-finder for the block-shaped model (issue #532),
// the direct analog of the old MissingViBackfillWindows: given the full set of candidate block
// object keys a column's retention window currently contains (caller enumerates these from
// blockpack_file_catalog), returns exactly the subset that have NO row at all -- any status,
// including 'succeeded' and 'failed' -- in compaction_jobs for (tenant, columnHash, columnType).
// Callers re-run the idempotent InsertViBackfillBlocks with exactly this missing subset.
//
// This distinction matters for the identical reason it did for windows: a block whose job
// already succeeded does NOT show up as a dedup-key conflict on a fresh insert attempt (the
// unique dedup index is scoped to status != 'succeeded', so OTHER job types can legitimately
// re-enqueue an already-succeeded candidate) -- without this check, a planner tick would insert
// a brand new duplicate row for every already-completed block, forever, unboundedly growing the
// table exactly like the incident MissingViBackfillWindows was added to prevent for windows.
//
// Issue #533: a JOIN against vi_backfill_job_columns, backed by that child table's own
// idx_vi_backfill_job_columns_lookup index (leading columns column_hash, column_type) -- NOT
// scoped to vc.status != 'succeeded' here, since a succeeded child row is exactly the "not
// missing" signal this query needs to see (the old idx_vi_backfill_all_blocks index this used to
// be backed by is gone; column_hash/column_type no longer live on compaction_jobs at all).
func (s *Store) MissingViBackfillBlocks(
	ctx context.Context, tenant string, col ViBackfillColumn, blockKeys []string,
) ([]string, error) {
	if len(blockKeys) == 0 {
		return nil, nil
	}
	rows, err := s.pool.Query(
		ctx,
		viBackfillExistingBlockKeysSQL,
		tenant,
		col.ColumnHash,
		col.ColumnType,
		blockKeys,
	)
	if err != nil {
		return nil, fmt.Errorf("pgqueue: vi_backfill existing block keys: %w", err)
	}
	defer rows.Close()

	existing := make(map[string]struct{}, len(blockKeys))
	for rows.Next() {
		var key string
		if err := rows.Scan(&key); err != nil {
			return nil, fmt.Errorf("pgqueue: scan vi_backfill block key: %w", err)
		}
		existing[key] = struct{}{}
	}
	if rows.Err() != nil {
		return nil, fmt.Errorf("pgqueue: iterate vi_backfill block keys: %w", rows.Err())
	}

	var missing []string
	for _, k := range blockKeys {
		if _, ok := existing[k]; !ok {
			missing = append(missing, k)
		}
	}
	return missing, nil
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
