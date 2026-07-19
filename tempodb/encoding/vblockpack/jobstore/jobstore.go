// Package jobstore is the Postgres-backed durable job queue for
// vi_backfill/cube_backfill (backend_jobs table, see ../migrate/backend_jobs.sql).
// Sibling to ../pg_entrystore.go's Postgres-backed EntryStore implementations --
// one more Postgres row store following the same no-abstraction-layer,
// pgx-direct pattern, sized as its own package because backend-worker (a
// different module) needs to import the claim path directly.
package jobstore

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type JobType string

const (
	JobTypeViBackfill   JobType = "vi_backfill"
	JobTypeCubeBackfill JobType = "cube_backfill"
)

type Status string

const (
	StatusPending   Status = "pending"
	StatusClaimed   Status = "claimed"
	StatusRunning   Status = "running"
	StatusSucceeded Status = "succeeded"
	StatusFailed    Status = "failed"
)

// Job is one backend_jobs row, decoded for Go callers. Detail is left as raw
// JSON -- callers unmarshal into ViBackfillDetail/CubeBackfillDetail
// themselves, keyed by Type.
type Job struct {
	ID        string
	Type      JobType
	Tenant    string
	Status    Status
	Detail    json.RawMessage
	DedupKey  string
	CreatedAt time.Time
	Retries   int
	LastError string
}

// ViBackfillDetail/CubeBackfillDetail mirror tempopb.ViBackfillDetail/
// CubeBackfillDetail's field shapes, JSON-tagged -- deliberately NOT reusing
// the tempopb types, so this package never imports pkg/tempopb for what is,
// on this path, plain JSONB.
type ViBackfillDetail struct {
	ColumnHash string `json:"column_hash"`
	ColumnName string `json:"column_name"`
	ColumnType string `json:"column_type"`
	// WindowSeconds bounds how far back from the column's current watermark this
	// job processes. Zero means unbounded (full remaining history) -- the
	// reactive first-trigger path (vi_usage_hook.go) still passes zero here,
	// preserving today's "first backfill does everything" behavior;
	// job-planner's chained continuation jobs (issue #518) pass a real bounded
	// value.
	WindowSeconds uint64 `json:"window_seconds"`
}

type CubeBackfillDetail struct {
	CubeID        string `json:"cube_id"`
	WindowMinutes uint32 `json:"window_minutes"`
}

// Store is the Postgres-backed backend_jobs implementation.
type Store struct{ pool *pgxpool.Pool }

func New(pool *pgxpool.Pool) *Store { return &Store{pool: pool} }

// insertJobSQL's ON CONFLICT predicate must match idx_backend_jobs_dedup_active
// (backend_jobs.sql) exactly, including value order -- Postgres's conflict
// target inference matches a partial unique index by parse-tree equality of
// its predicate, not just column list.
const insertJobSQL = `
	INSERT INTO backend_jobs (id, job_type, tenant, status, detail, dedup_key)
	VALUES ($1, $2, $3, 'pending', $4, $5)
	ON CONFLICT (dedup_key) WHERE status IN ('pending', 'claimed', 'running') DO NOTHING`

// InsertViBackfill inserts a pending vi_backfill job, deduped on
// job_type+tenant+colHash+colType (the job_type prefix is defense-in-depth:
// today's colHash/colType and cubeID formats can never collide, but nothing
// guarantees that stays true forever). Idempotent: if a non-terminal job for
// the same dedup key already exists, this is a silent no-op -- the INSERT
// either succeeds or is absorbed by the partial unique index, in one round
// trip, no separate check-then-insert race window.
func (s *Store) InsertViBackfill(ctx context.Context, tenant string, d ViBackfillDetail) error {
	dedupKey := string(JobTypeViBackfill) + "|" + tenant + "|" + d.ColumnHash + "|" + d.ColumnType
	detail, err := json.Marshal(d)
	if err != nil {
		return fmt.Errorf("jobstore: marshal vi_backfill detail: %w", err)
	}
	if _, err := s.pool.Exec(ctx, insertJobSQL, uuid.NewString(), string(JobTypeViBackfill), tenant, detail, dedupKey); err != nil {
		return fmt.Errorf("jobstore: insert vi_backfill job: %w", err)
	}
	return nil
}

// InsertCubeBackfill mirrors InsertViBackfill, deduped on
// job_type+tenant+cubeID.
func (s *Store) InsertCubeBackfill(ctx context.Context, tenant string, d CubeBackfillDetail) error {
	dedupKey := string(JobTypeCubeBackfill) + "|" + tenant + "|" + d.CubeID
	detail, err := json.Marshal(d)
	if err != nil {
		return fmt.Errorf("jobstore: marshal cube_backfill detail: %w", err)
	}
	if _, err := s.pool.Exec(ctx, insertJobSQL, uuid.NewString(), string(JobTypeCubeBackfill), tenant, detail, dedupKey); err != nil {
		return fmt.Errorf("jobstore: insert cube_backfill job: %w", err)
	}
	return nil
}

// claimJobSQL is the standard single-statement SKIP LOCKED claim idiom: find
// the oldest claimable row of jobType (fresh pending, OR claimed/running with
// an expired lease -- crashed-worker self-heal, OR failed with a due retry),
// lock it, mark it claimed, all in one round trip. SKIP LOCKED means N
// concurrent callers never block each other and never claim the same row.
const claimJobSQL = `
	UPDATE backend_jobs
	SET status = 'claimed', claimed_by = $2, claimed_at = now(),
		lease_expires_at = now() + interval '30 minutes'
	WHERE id = (
		SELECT id FROM backend_jobs
		WHERE job_type = $1
		  AND (
			  status = 'pending'
			  OR (status = 'claimed' AND lease_expires_at < now())
			  OR (status = 'running' AND lease_expires_at < now())
			  OR (status = 'failed' AND next_retry_at IS NOT NULL AND next_retry_at <= now())
		  )
		ORDER BY created_at
		FOR UPDATE SKIP LOCKED
		LIMIT 1
	)
	RETURNING id, tenant, detail, retries`

// Claim atomically claims and returns the oldest claimable job of jobType for
// workerID, or (nil, nil) if none exists -- not an error.
func (s *Store) Claim(ctx context.Context, jobType JobType, workerID string) (*Job, error) {
	row := s.pool.QueryRow(ctx, claimJobSQL, string(jobType), workerID)
	var (
		id, tenant string
		detail     []byte
		retries    int
	)
	err := row.Scan(&id, &tenant, &detail, &retries)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("jobstore: claim %s job: %w", jobType, err)
	}
	return &Job{ID: id, Type: jobType, Tenant: tenant, Status: StatusClaimed, Detail: detail, Retries: retries}, nil
}

// renewLeaseSQL extends jobID's lease by another 30 minutes from the CURRENT
// now(), not the original claim time -- the status guard makes this a no-op
// once the job has reached a terminal state (issue #520: defense-in-depth
// against a benign race between the renewal loop's last tick and
// Complete/Fail, not a condition expected to matter in practice, since the
// caller only ever renews while it still holds and is actively processing the
// job).
const renewLeaseSQL = `
	UPDATE backend_jobs
	SET lease_expires_at = now() + interval '30 minutes'
	WHERE id = $1 AND status IN ('claimed', 'running')`

// RenewLease extends jobID's lease, preventing Claim's expired-lease clause
// from letting a second worker reclaim it while the original worker is still
// actively processing it (issue #520: today's lease is set once at claim time
// and never renewed, so any job genuinely running longer than 30 minutes was
// silently subject to double-execution). Callers are expected to invoke this
// periodically (well under the 30-minute lease TTL) for the duration of
// active processing -- see backend-worker's renewLeasePeriodically.
func (s *Store) RenewLease(ctx context.Context, jobID string) error {
	if _, err := s.pool.Exec(ctx, renewLeaseSQL, jobID); err != nil {
		return fmt.Errorf("jobstore: renew lease %s: %w", jobID, err)
	}
	return nil
}

const completeJobSQL = `UPDATE backend_jobs SET status = 'succeeded', finished_at = now() WHERE id = $1`

// Complete marks jobID succeeded.
func (s *Store) Complete(ctx context.Context, jobID string) error {
	if _, err := s.pool.Exec(ctx, completeJobSQL, jobID); err != nil {
		return fmt.Errorf("jobstore: complete job %s: %w", jobID, err)
	}
	return nil
}

// Fail records a failed attempt for jobID and unconditionally schedules a retry
// (next_retry_at set per backoffDuration, status stays 'failed' but is claimable again per
// Claim's retry-due clause) -- 2026-07-17: reversal of the prior "#181 §8.1 locked default: 5
// attempts, then permanently failed" ruling. A transient failure (a compacted-away block, a
// registry entry not yet visible to a stale connection, ...) has no reason to ever stop
// retrying; the exponential backoff (capped at 30m, backoffDuration) already bounds how much
// worker capacity a persistently-failing job can consume, without ever giving up on it outright.
func (s *Store) Fail(ctx context.Context, jobID, errMsg string) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("jobstore: fail: begin: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	var retries int
	row := tx.QueryRow(ctx, `SELECT retries FROM backend_jobs WHERE id = $1 FOR UPDATE`, jobID)
	if err := row.Scan(&retries); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return fmt.Errorf("jobstore: fail: job %s not found", jobID)
		}
		return fmt.Errorf("jobstore: fail: load retries: %w", err)
	}

	newRetries := retries + 1
	nextRetryAt := time.Now().Add(backoffDuration(newRetries))

	if _, err := tx.Exec(ctx, `
		UPDATE backend_jobs
		SET status = 'failed', last_error = $2, retries = $3, next_retry_at = $4
		WHERE id = $1`, jobID, errMsg, newRetries, nextRetryAt); err != nil {
		return fmt.Errorf("jobstore: fail: update: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("jobstore: fail: commit: %w", err)
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
