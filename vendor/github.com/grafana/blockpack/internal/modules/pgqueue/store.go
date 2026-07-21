package pgqueue

import (
	"context"
	_ "embed"
	"encoding/json"
	"errors"
	"fmt"
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
const insertJobSQL = `
	INSERT INTO compaction_jobs (id, job_type, subsystem, tenant, status, detail, dedup_key)
	VALUES ($1, $2, $3, $4, 'pending', $5, $6)
	ON CONFLICT (dedup_key) WHERE status != 'succeeded' DO NOTHING`

// Insert inserts a pending job. Idempotent: if a non-terminal job for the
// same dedupKey already exists, this is a silent no-op -- the INSERT either
// succeeds or is absorbed by the partial unique index, in one round trip, no
// separate check-then-insert race window.
func (s *Store) Insert(ctx context.Context, jobType JobType, subsystem, tenant, dedupKey string, detail any) error {
	rawDetail, err := json.Marshal(detail)
	if err != nil {
		return fmt.Errorf("pgqueue: marshal %s detail: %w", jobType, err)
	}
	if _, err := s.pool.Exec(
		ctx, insertJobSQL, uuid.NewString(), string(jobType), subsystem, tenant, rawDetail, dedupKey,
	); err != nil {
		return fmt.Errorf("pgqueue: insert %s job: %w", jobType, err)
	}
	return nil
}

// InsertViBackfill inserts a pending vi_backfill job, deduped on
// job_type+tenant+columnHash+columnType (mirrors tempo jobstore.go's
// now-retired InsertViBackfill exactly). Unlike the other job types in this
// package, vi_backfill has two distinct call sites -- the reactive
// first-trigger path (root-level InsertViBackfillJob, called from tempo's
// query path) and this package's own chained-continuation planner
// (plan_vi_backfill.go) -- so the dedup-key formula lives here, once, instead
// of being duplicated at each call site.
func (s *Store) InsertViBackfill(ctx context.Context, tenant string, d ViBackfillDetail) error {
	dedupKey := string(JobTypeViBackfill) + "|" + tenant + "|" + d.ColumnHash + "|" + d.ColumnType
	return s.Insert(ctx, JobTypeViBackfill, "vi", tenant, dedupKey, d)
}

// InsertCubeBackfill mirrors InsertViBackfill, deduped on
// job_type+tenant+cubeID.
func (s *Store) InsertCubeBackfill(ctx context.Context, tenant string, d CubeBackfillDetail) error {
	dedupKey := string(JobTypeCubeBackfill) + "|" + tenant + "|" + d.CubeID
	return s.Insert(ctx, JobTypeCubeBackfill, "cube", tenant, dedupKey, d)
}

// claimJobSQL is the standard single-statement SKIP LOCKED claim idiom: find
// the oldest claimable row of ANY job_type (fresh pending, OR claimed/running
// with an expired lease -- crashed-worker self-heal, OR failed with a due
// retry), lock it, mark it claimed, all in one round trip. SKIP LOCKED means N
// concurrent callers never block each other and never claim the same row.
// Deliberately no job_type filter -- mirrors the "any pod, any job" claim
// philosophy (Section G.2).
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
		ORDER BY created_at
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
