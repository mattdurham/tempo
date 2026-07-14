package jobstore

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
)

func TestStore_InsertViBackfill_CreatesPendingRow(t *testing.T) {
	pool := newTestPostgresPool(t)
	store := New(pool)
	ctx := context.Background()

	if err := store.InsertViBackfill(ctx, "tenant-a", ViBackfillDetail{ColumnHash: "h1", ColumnName: "span.name", ColumnType: "string"}); err != nil {
		t.Fatalf("InsertViBackfill: %v", err)
	}

	var status, jobType string
	row := pool.QueryRow(ctx, `SELECT status, job_type FROM backend_jobs WHERE dedup_key = $1`, "vi_backfill|tenant-a|h1|string")
	if err := row.Scan(&status, &jobType); err != nil {
		t.Fatalf("querying inserted row: %v", err)
	}
	if status != string(StatusPending) {
		t.Fatalf("expected status=pending, got %q", status)
	}
	if jobType != string(JobTypeViBackfill) {
		t.Fatalf("expected job_type=vi_backfill, got %q", jobType)
	}
}

func TestStore_InsertViBackfill_DuplicateDedupKeyIsNoOp(t *testing.T) {
	pool := newTestPostgresPool(t)
	store := New(pool)
	ctx := context.Background()

	detail := ViBackfillDetail{ColumnHash: "h1", ColumnName: "span.name", ColumnType: "string"}
	if err := store.InsertViBackfill(ctx, "tenant-a", detail); err != nil {
		t.Fatalf("InsertViBackfill (first): %v", err)
	}
	if err := store.InsertViBackfill(ctx, "tenant-a", detail); err != nil {
		t.Fatalf("InsertViBackfill (duplicate): %v", err)
	}

	var count int
	row := pool.QueryRow(ctx, `SELECT count(*) FROM backend_jobs WHERE dedup_key = $1`, "vi_backfill|tenant-a|h1|string")
	if err := row.Scan(&count); err != nil {
		t.Fatalf("counting rows: %v", err)
	}
	if count != 1 {
		t.Fatalf("expected exactly 1 row after duplicate insert, got %d", count)
	}
}

func TestStore_InsertViBackfill_NewRowAllowedAfterPriorTerminal(t *testing.T) {
	pool := newTestPostgresPool(t)
	store := New(pool)
	ctx := context.Background()

	detail := ViBackfillDetail{ColumnHash: "h1", ColumnName: "span.name", ColumnType: "string"}
	if err := store.InsertViBackfill(ctx, "tenant-a", detail); err != nil {
		t.Fatalf("InsertViBackfill (first): %v", err)
	}
	if _, err := pool.Exec(ctx, `UPDATE backend_jobs SET status = 'succeeded' WHERE dedup_key = $1`, "vi_backfill|tenant-a|h1|string"); err != nil {
		t.Fatalf("marking first row succeeded: %v", err)
	}
	if err := store.InsertViBackfill(ctx, "tenant-a", detail); err != nil {
		t.Fatalf("InsertViBackfill (after terminal): %v", err)
	}

	var count int
	row := pool.QueryRow(ctx, `SELECT count(*) FROM backend_jobs WHERE dedup_key = $1`, "vi_backfill|tenant-a|h1|string")
	if err := row.Scan(&count); err != nil {
		t.Fatalf("counting rows: %v", err)
	}
	if count != 2 {
		t.Fatalf("expected 2 rows (terminal + new), got %d", count)
	}
}

func TestStore_InsertCubeBackfill_CreatesPendingRow(t *testing.T) {
	pool := newTestPostgresPool(t)
	store := New(pool)
	ctx := context.Background()

	if err := store.InsertCubeBackfill(ctx, "tenant-a", CubeBackfillDetail{CubeID: "cube1", WindowMinutes: 60}); err != nil {
		t.Fatalf("InsertCubeBackfill: %v", err)
	}

	var status, jobType string
	row := pool.QueryRow(ctx, `SELECT status, job_type FROM backend_jobs WHERE dedup_key = $1`, "cube_backfill|tenant-a|cube1")
	if err := row.Scan(&status, &jobType); err != nil {
		t.Fatalf("querying inserted row: %v", err)
	}
	if status != string(StatusPending) {
		t.Fatalf("expected status=pending, got %q", status)
	}
	if jobType != string(JobTypeCubeBackfill) {
		t.Fatalf("expected job_type=cube_backfill, got %q", jobType)
	}
}

func TestStore_InsertCubeBackfill_DuplicateDedupKeyIsNoOp(t *testing.T) {
	pool := newTestPostgresPool(t)
	store := New(pool)
	ctx := context.Background()

	detail := CubeBackfillDetail{CubeID: "cube1", WindowMinutes: 60}
	if err := store.InsertCubeBackfill(ctx, "tenant-a", detail); err != nil {
		t.Fatalf("InsertCubeBackfill (first): %v", err)
	}
	if err := store.InsertCubeBackfill(ctx, "tenant-a", detail); err != nil {
		t.Fatalf("InsertCubeBackfill (duplicate): %v", err)
	}

	var count int
	row := pool.QueryRow(ctx, `SELECT count(*) FROM backend_jobs WHERE dedup_key = $1`, "cube_backfill|tenant-a|cube1")
	if err := row.Scan(&count); err != nil {
		t.Fatalf("counting rows: %v", err)
	}
	if count != 1 {
		t.Fatalf("expected exactly 1 row after duplicate insert, got %d", count)
	}
}

func TestStore_InsertCubeBackfill_NewRowAllowedAfterPriorTerminal(t *testing.T) {
	pool := newTestPostgresPool(t)
	store := New(pool)
	ctx := context.Background()

	detail := CubeBackfillDetail{CubeID: "cube1", WindowMinutes: 60}
	if err := store.InsertCubeBackfill(ctx, "tenant-a", detail); err != nil {
		t.Fatalf("InsertCubeBackfill (first): %v", err)
	}
	if _, err := pool.Exec(ctx, `UPDATE backend_jobs SET status = 'failed', next_retry_at = NULL WHERE dedup_key = $1`, "cube_backfill|tenant-a|cube1"); err != nil {
		t.Fatalf("marking first row permanently failed: %v", err)
	}
	if err := store.InsertCubeBackfill(ctx, "tenant-a", detail); err != nil {
		t.Fatalf("InsertCubeBackfill (after terminal): %v", err)
	}

	var count int
	row := pool.QueryRow(ctx, `SELECT count(*) FROM backend_jobs WHERE dedup_key = $1`, "cube_backfill|tenant-a|cube1")
	if err := row.Scan(&count); err != nil {
		t.Fatalf("counting rows: %v", err)
	}
	if count != 2 {
		t.Fatalf("expected 2 rows (terminal + new), got %d", count)
	}
}

// TestStore_InsertViBackfillAndCubeBackfill_SameRawDedupComponents_BothRowsSurvive
// proves the job_type prefix added to dedup_key (defense-in-depth against a future
// vi_backfill/cube_backfill ID-format collision, even though today's real ID formats
// can never collide) actually keeps the two job types distinct. It constructs a VI
// job and a cube job whose dedup components WOULD have produced the identical
// dedup_key string under the old (pre-job_type-prefix) key shape: old VI shape was
// tenant+"|"+colHash+"|"+colType, old cube shape was tenant+"|"+cubeID, so setting
// CubeID == colHash+"|"+colType makes both old-shape keys equal
// "tenant-collide|foo|bar". With the job_type prefix, the two keys
// ("vi_backfill|tenant-collide|foo|bar" vs "cube_backfill|tenant-collide|foo|bar")
// are distinct, so both non-terminal rows must coexist instead of the second being
// silently absorbed as a no-op by idx_backend_jobs_dedup_active.
func TestStore_InsertViBackfillAndCubeBackfill_SameRawDedupComponents_BothRowsSurvive(t *testing.T) {
	pool := newTestPostgresPool(t)
	store := New(pool)
	ctx := context.Background()

	const tenant = "tenant-collide"
	if err := store.InsertViBackfill(ctx, tenant, ViBackfillDetail{ColumnHash: "foo", ColumnType: "bar"}); err != nil {
		t.Fatalf("InsertViBackfill: %v", err)
	}
	if err := store.InsertCubeBackfill(ctx, tenant, CubeBackfillDetail{CubeID: "foo|bar"}); err != nil {
		t.Fatalf("InsertCubeBackfill: %v", err)
	}

	var count int
	row := pool.QueryRow(ctx, `SELECT count(*) FROM backend_jobs WHERE tenant = $1 AND status = 'pending'`, tenant)
	if err := row.Scan(&count); err != nil {
		t.Fatalf("counting rows: %v", err)
	}
	if count != 2 {
		t.Fatalf("expected both the vi_backfill and cube_backfill rows to survive as distinct pending rows despite identical raw dedup components, got %d row(s)", count)
	}
}

func TestStore_Claim_ReturnsOldestPendingJobOfType_NotOtherType(t *testing.T) {
	pool := newTestPostgresPool(t)
	store := New(pool)
	ctx := context.Background()

	if err := store.InsertViBackfill(ctx, "tenant-a", ViBackfillDetail{ColumnHash: "h1", ColumnType: "string"}); err != nil {
		t.Fatalf("seed vi_backfill: %v", err)
	}
	if err := store.InsertCubeBackfill(ctx, "tenant-a", CubeBackfillDetail{CubeID: "cube1"}); err != nil {
		t.Fatalf("seed cube_backfill: %v", err)
	}

	job, err := store.Claim(ctx, JobTypeViBackfill, "worker-1")
	if err != nil {
		t.Fatalf("Claim: %v", err)
	}
	if job == nil {
		t.Fatal("expected a claimable vi_backfill job, got nil")
	}
	if job.Type != JobTypeViBackfill {
		t.Fatalf("expected Type=vi_backfill, got %v", job.Type)
	}
	if job.Tenant != "tenant-a" {
		t.Fatalf("expected Tenant=tenant-a, got %q", job.Tenant)
	}
}

func TestStore_Claim_NoClaimableJobsReturnsNilNilNotError(t *testing.T) {
	pool := newTestPostgresPool(t)
	store := New(pool)
	ctx := context.Background()

	job, err := store.Claim(ctx, JobTypeViBackfill, "worker-1")
	if err != nil {
		t.Fatalf("expected no error when nothing is claimable, got: %v", err)
	}
	if job != nil {
		t.Fatalf("expected nil job when nothing is claimable, got: %+v", job)
	}
}

func TestStore_Claim_ExpiredLeaseOnClaimedRowIsReclaimable(t *testing.T) {
	pool := newTestPostgresPool(t)
	store := New(pool)
	ctx := context.Background()

	if err := store.InsertViBackfill(ctx, "tenant-a", ViBackfillDetail{ColumnHash: "h1", ColumnType: "string"}); err != nil {
		t.Fatalf("seed: %v", err)
	}
	if _, err := pool.Exec(ctx, `
		UPDATE backend_jobs SET status = 'claimed', claimed_by = 'dead-worker',
			claimed_at = now() - interval '1 hour', lease_expires_at = now() - interval '30 minutes'
		WHERE dedup_key = $1`, "vi_backfill|tenant-a|h1|string"); err != nil {
		t.Fatalf("seeding expired lease: %v", err)
	}

	job, err := store.Claim(ctx, JobTypeViBackfill, "worker-2")
	if err != nil {
		t.Fatalf("Claim: %v", err)
	}
	if job == nil {
		t.Fatal("expected the expired-lease row to be reclaimable, got nil")
	}
}

func TestStore_Claim_UnexpiredLeaseOnClaimedRowIsNotReclaimable(t *testing.T) {
	pool := newTestPostgresPool(t)
	store := New(pool)
	ctx := context.Background()

	if err := store.InsertViBackfill(ctx, "tenant-a", ViBackfillDetail{ColumnHash: "h1", ColumnType: "string"}); err != nil {
		t.Fatalf("seed: %v", err)
	}
	if _, err := pool.Exec(ctx, `
		UPDATE backend_jobs SET status = 'claimed', claimed_by = 'live-worker',
			claimed_at = now(), lease_expires_at = now() + interval '30 minutes'
		WHERE dedup_key = $1`, "vi_backfill|tenant-a|h1|string"); err != nil {
		t.Fatalf("seeding unexpired lease: %v", err)
	}

	job, err := store.Claim(ctx, JobTypeViBackfill, "worker-2")
	if err != nil {
		t.Fatalf("Claim: %v", err)
	}
	if job != nil {
		t.Fatalf("expected an unexpired-lease row to NOT be reclaimable, got: %+v", job)
	}
}

func TestStore_Claim_FailedJobWithDueRetryIsReclaimable(t *testing.T) {
	pool := newTestPostgresPool(t)
	store := New(pool)
	ctx := context.Background()

	if err := store.InsertViBackfill(ctx, "tenant-a", ViBackfillDetail{ColumnHash: "h1", ColumnType: "string"}); err != nil {
		t.Fatalf("seed: %v", err)
	}
	if _, err := pool.Exec(ctx, `
		UPDATE backend_jobs SET status = 'failed', next_retry_at = now() - interval '1 minute'
		WHERE dedup_key = $1`, "vi_backfill|tenant-a|h1|string"); err != nil {
		t.Fatalf("seeding due retry: %v", err)
	}

	job, err := store.Claim(ctx, JobTypeViBackfill, "worker-2")
	if err != nil {
		t.Fatalf("Claim: %v", err)
	}
	if job == nil {
		t.Fatal("expected a due-retry failed row to be reclaimable, got nil")
	}
}

func TestStore_Claim_FailedJobWithRetryNotYetDueIsNotReclaimable(t *testing.T) {
	pool := newTestPostgresPool(t)
	store := New(pool)
	ctx := context.Background()

	if err := store.InsertViBackfill(ctx, "tenant-a", ViBackfillDetail{ColumnHash: "h1", ColumnType: "string"}); err != nil {
		t.Fatalf("seed: %v", err)
	}
	if _, err := pool.Exec(ctx, `
		UPDATE backend_jobs SET status = 'failed', next_retry_at = now() + interval '1 hour'
		WHERE dedup_key = $1`, "vi_backfill|tenant-a|h1|string"); err != nil {
		t.Fatalf("seeding future retry: %v", err)
	}

	job, err := store.Claim(ctx, JobTypeViBackfill, "worker-2")
	if err != nil {
		t.Fatalf("Claim: %v", err)
	}
	if job != nil {
		t.Fatalf("expected a not-yet-due failed row to NOT be reclaimable, got: %+v", job)
	}
}

func TestStore_Claim_FailedJobWithNoNextRetryAt_PermanentlyFailed_NeverReclaimable(t *testing.T) {
	pool := newTestPostgresPool(t)
	store := New(pool)
	ctx := context.Background()

	if err := store.InsertViBackfill(ctx, "tenant-a", ViBackfillDetail{ColumnHash: "h1", ColumnType: "string"}); err != nil {
		t.Fatalf("seed: %v", err)
	}
	if _, err := pool.Exec(ctx, `
		UPDATE backend_jobs SET status = 'failed', next_retry_at = NULL
		WHERE dedup_key = $1`, "vi_backfill|tenant-a|h1|string"); err != nil {
		t.Fatalf("seeding permanent failure: %v", err)
	}

	job, err := store.Claim(ctx, JobTypeViBackfill, "worker-2")
	if err != nil {
		t.Fatalf("Claim: %v", err)
	}
	if job != nil {
		t.Fatalf("expected a permanently-failed (NULL next_retry_at) row to NEVER be reclaimable, got: %+v", job)
	}
}

func TestStore_Complete_SetsSucceededAndFinishedAt(t *testing.T) {
	pool := newTestPostgresPool(t)
	store := New(pool)
	ctx := context.Background()

	if err := store.InsertViBackfill(ctx, "tenant-a", ViBackfillDetail{ColumnHash: "h1", ColumnType: "string"}); err != nil {
		t.Fatalf("seed: %v", err)
	}
	job, err := store.Claim(ctx, JobTypeViBackfill, "worker-1")
	if err != nil || job == nil {
		t.Fatalf("Claim: job=%+v err=%v", job, err)
	}

	if err := store.Complete(ctx, job.ID); err != nil {
		t.Fatalf("Complete: %v", err)
	}

	var status string
	var finishedAt *time.Time
	row := pool.QueryRow(ctx, `SELECT status, finished_at FROM backend_jobs WHERE id = $1`, job.ID)
	if err := row.Scan(&status, &finishedAt); err != nil {
		t.Fatalf("querying completed row: %v", err)
	}
	if status != string(StatusSucceeded) {
		t.Fatalf("expected status=succeeded, got %q", status)
	}
	if finishedAt == nil {
		t.Fatal("expected finished_at to be set, got NULL")
	}
}

func TestStore_Fail_BelowMaxRetries_SchedulesRetryWithBackoff(t *testing.T) {
	pool := newTestPostgresPool(t)
	store := New(pool)
	ctx := context.Background()

	if err := store.InsertViBackfill(ctx, "tenant-a", ViBackfillDetail{ColumnHash: "h1", ColumnType: "string"}); err != nil {
		t.Fatalf("seed: %v", err)
	}
	job, err := store.Claim(ctx, JobTypeViBackfill, "worker-1")
	if err != nil || job == nil {
		t.Fatalf("Claim: job=%+v err=%v", job, err)
	}

	before := time.Now()
	if err := store.Fail(ctx, job.ID, "transient error", 5); err != nil {
		t.Fatalf("Fail: %v", err)
	}

	var status string
	var retries int
	var nextRetryAt *time.Time
	var lastError string
	row := pool.QueryRow(ctx, `SELECT status, retries, next_retry_at, last_error FROM backend_jobs WHERE id = $1`, job.ID)
	if err := row.Scan(&status, &retries, &nextRetryAt, &lastError); err != nil {
		t.Fatalf("querying failed row: %v", err)
	}
	if status != string(StatusFailed) {
		t.Fatalf("expected status=failed, got %q", status)
	}
	if retries != 1 {
		t.Fatalf("expected retries=1, got %d", retries)
	}
	if lastError != "transient error" {
		t.Fatalf("expected last_error to be recorded, got %q", lastError)
	}
	if nextRetryAt == nil {
		t.Fatal("expected next_retry_at to be set (below maxRetries), got NULL")
	}
	// backoffDuration(1) == 2 minutes.
	wantMin := before.Add(90 * time.Second)
	wantMax := before.Add(150 * time.Second)
	if nextRetryAt.Before(wantMin) || nextRetryAt.After(wantMax) {
		t.Fatalf("expected next_retry_at within [%v, %v] of first failure (2m backoff), got %v", wantMin, wantMax, nextRetryAt)
	}
}

func TestStore_Fail_AtMaxRetries_PermanentlyFails(t *testing.T) {
	pool := newTestPostgresPool(t)
	store := New(pool)
	ctx := context.Background()

	if err := store.InsertViBackfill(ctx, "tenant-a", ViBackfillDetail{ColumnHash: "h1", ColumnType: "string"}); err != nil {
		t.Fatalf("seed: %v", err)
	}
	job, err := store.Claim(ctx, JobTypeViBackfill, "worker-1")
	if err != nil || job == nil {
		t.Fatalf("Claim: job=%+v err=%v", job, err)
	}

	// maxRetries=1: the first failure already reaches the bound.
	if err := store.Fail(ctx, job.ID, "final error", 1); err != nil {
		t.Fatalf("Fail: %v", err)
	}

	var status string
	var retries int
	var nextRetryAt *time.Time
	row := pool.QueryRow(ctx, `SELECT status, retries, next_retry_at FROM backend_jobs WHERE id = $1`, job.ID)
	if err := row.Scan(&status, &retries, &nextRetryAt); err != nil {
		t.Fatalf("querying failed row: %v", err)
	}
	if status != string(StatusFailed) {
		t.Fatalf("expected status=failed, got %q", status)
	}
	if retries != 1 {
		t.Fatalf("expected retries=1, got %d", retries)
	}
	if nextRetryAt != nil {
		t.Fatalf("expected next_retry_at to be NULL (permanently failed, retries>=maxRetries), got %v", nextRetryAt)
	}

	// A permanently-failed job must never be reclaimable.
	reclaimed, err := store.Claim(ctx, JobTypeViBackfill, "worker-2")
	if err != nil {
		t.Fatalf("Claim after permanent failure: %v", err)
	}
	if reclaimed != nil {
		t.Fatalf("expected permanently-failed job to never be reclaimable, got: %+v", reclaimed)
	}
}

// TestStore_Claim_ConcurrentWorkersRacingSameJobType_EachGetsADifferentRow is
// the required worker-claim concurrency test. N goroutines call Claim
// concurrently against a pool of exactly N pre-seeded pending jobs of the
// same type; every goroutine must get a DISTINCT job ID, and the union of all
// claimed IDs must equal the full seeded set.
//
// Uses newTestPostgresPool's MaxConns=50 sizing and a plain
// context.Background() (no tight deadline) deliberately -- a too-small pool
// masks races via connection-queueing, not via the SQL lock itself, per
// ../pg_testutil_test.go's own documented convention.
//
// Mutation-tested (session convention: reintroduce the exact bug a test
// claims to catch, confirm it fails, then revert): temporarily replaced
// claimJobSQL's "FOR UPDATE SKIP LOCKED ... RETURNING id, tenant, detail,
// retries" subselect+update with a plain, unlocked "SELECT id FROM
// backend_jobs WHERE job_type = $1 AND status = 'pending' ORDER BY
// created_at LIMIT 1" (no locking at all) feeding the same outer UPDATE.
// With no locking, multiple goroutines' SELECTs raced to read the SAME
// top-of-queue row before any of their UPDATEs committed, and this test
// failed concretely with "expected 20 distinct claimed IDs, got N < 20" --
// i.e. two or more goroutines claimed (and both saw as their own return
// value) the identical row ID, the precise duplicate-claim failure mode this
// test exists to catch. Reverted afterward; full package suite passes again.
func TestStore_Claim_ConcurrentWorkersRacingSameJobType_EachGetsADifferentRow(t *testing.T) {
	pool := newTestPostgresPool(t)
	store := New(pool)
	ctx := context.Background()

	const n = 20
	seeded := make(map[string]bool, n)
	for i := range n {
		// Distinct dedup_key per seeded row -- this test seeds N independent
		// pending jobs of the same type, not N dedup-colliding attempts.
		dedupKey := fmt.Sprintf("tenant-a|col%d|string", i)
		if _, err := pool.Exec(ctx, `
			INSERT INTO backend_jobs (id, job_type, tenant, status, detail, dedup_key)
			VALUES ($1, $2, $3, 'pending', $4, $5)`,
			uuid.NewString(), string(JobTypeViBackfill), "tenant-a", []byte(`{}`), dedupKey); err != nil {
			t.Fatalf("seeding job %d: %v", i, err)
		}
	}
	rows, err := pool.Query(ctx, `SELECT id FROM backend_jobs WHERE job_type = $1 AND status = 'pending'`, string(JobTypeViBackfill))
	if err != nil {
		t.Fatalf("listing seeded ids: %v", err)
	}
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			t.Fatalf("scanning seeded id: %v", err)
		}
		seeded[id] = true
	}
	rows.Close()
	if len(seeded) != n {
		t.Fatalf("expected %d seeded pending rows, got %d", n, len(seeded))
	}

	var wg sync.WaitGroup
	claimedIDs := make([]string, n)
	claimErrs := make([]error, n)
	for i := range n {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			job, err := store.Claim(ctx, JobTypeViBackfill, "worker")
			claimErrs[i] = err
			if job != nil {
				claimedIDs[i] = job.ID
			}
		}(i)
	}
	wg.Wait()

	distinct := map[string]int{}
	for i, err := range claimErrs {
		if err != nil {
			t.Fatalf("goroutine %d Claim error: %v", i, err)
		}
		if claimedIDs[i] == "" {
			t.Fatalf("goroutine %d claimed no job (nil), expected exactly one of the %d seeded rows", i, n)
		}
		distinct[claimedIDs[i]]++
	}
	if len(distinct) != n {
		t.Fatalf("expected %d distinct claimed IDs (no two goroutines claim the same row), got %d distinct: %v", n, len(distinct), distinct)
	}
	for id, count := range distinct {
		if count != 1 {
			t.Fatalf("job %s was claimed by %d goroutines, expected exactly 1", id, count)
		}
		if !seeded[id] {
			t.Fatalf("claimed job %s was not among the seeded set", id)
		}
	}
}
