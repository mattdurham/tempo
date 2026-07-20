package jobstore

import (
	"context"
	"encoding/json"
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

// TestStore_InsertViBackfill_WindowSecondsRoundTrips pins issue #518's new
// bounded-window field: it must survive the JSONB detail column round-trip
// unchanged, since job-planner's chained continuation jobs rely on it.
func TestStore_InsertViBackfill_WindowSecondsRoundTrips(t *testing.T) {
	pool := newTestPostgresPool(t)
	store := New(pool)
	ctx := context.Background()

	detail := ViBackfillDetail{ColumnHash: "h1", ColumnName: "span.name", ColumnType: "string", WindowSeconds: 21600}
	if err := store.InsertViBackfill(ctx, "tenant-a", detail); err != nil {
		t.Fatalf("InsertViBackfill: %v", err)
	}

	job, err := store.Claim(ctx, JobTypeViBackfill, "worker-1")
	if err != nil {
		t.Fatalf("Claim: %v", err)
	}
	if job == nil {
		t.Fatal("expected a claimable job")
	}
	var got ViBackfillDetail
	if err := json.Unmarshal(job.Detail, &got); err != nil {
		t.Fatalf("unmarshal detail: %v", err)
	}
	if got.WindowSeconds != 21600 {
		t.Fatalf("expected WindowSeconds=21600 to round-trip, got %d", got.WindowSeconds)
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

func TestStore_InsertCatalogReconcile_CreatesPendingRow(t *testing.T) {
	pool := newTestPostgresPool(t)
	store := New(pool)
	ctx := context.Background()

	if err := store.InsertCatalogReconcile(ctx, "vi", "tenant-a"); err != nil {
		t.Fatalf("InsertCatalogReconcile: %v", err)
	}

	var status, jobType, tenant string
	row := pool.QueryRow(ctx, `SELECT status, job_type, tenant FROM backend_jobs WHERE dedup_key = $1`, "catalog_reconcile|vi|tenant-a")
	if err := row.Scan(&status, &jobType, &tenant); err != nil {
		t.Fatalf("querying inserted row: %v", err)
	}
	if status != string(StatusPending) {
		t.Fatalf("expected status=pending, got %q", status)
	}
	if jobType != string(JobTypeCatalogReconcile) {
		t.Fatalf("expected job_type=catalog_reconcile, got %q", jobType)
	}
	if tenant != "tenant-a" {
		t.Fatalf("expected tenant=tenant-a, got %q", tenant)
	}
}

func TestStore_InsertCatalogReconcile_DuplicateDedupKeyIsNoOp(t *testing.T) {
	pool := newTestPostgresPool(t)
	store := New(pool)
	ctx := context.Background()

	if err := store.InsertCatalogReconcile(ctx, "vi", "tenant-a"); err != nil {
		t.Fatalf("InsertCatalogReconcile (first): %v", err)
	}
	if err := store.InsertCatalogReconcile(ctx, "vi", "tenant-a"); err != nil {
		t.Fatalf("InsertCatalogReconcile (duplicate): %v", err)
	}

	var count int
	row := pool.QueryRow(ctx, `SELECT count(*) FROM backend_jobs WHERE dedup_key = $1`, "catalog_reconcile|vi|tenant-a")
	if err := row.Scan(&count); err != nil {
		t.Fatalf("counting rows: %v", err)
	}
	if count != 1 {
		t.Fatalf("expected exactly 1 row after duplicate insert, got %d", count)
	}
}

func TestStore_InsertTraceCompaction_CreatesPendingRow(t *testing.T) {
	pool := newTestPostgresPool(t)
	store := New(pool)
	ctx := context.Background()

	detail := TraceCompactionDetail{InputBlockIDs: []string{"block-b", "block-a"}}
	if err := store.InsertTraceCompaction(ctx, "tenant-a", detail); err != nil {
		t.Fatalf("InsertTraceCompaction: %v", err)
	}

	var status, jobType, tenant string
	var gotDetail []byte
	row := pool.QueryRow(
		ctx, `SELECT status, job_type, tenant, detail FROM backend_jobs WHERE dedup_key = $1`,
		"trace_compaction|tenant-a|block-a,block-b",
	)
	if err := row.Scan(&status, &jobType, &tenant, &gotDetail); err != nil {
		t.Fatalf("querying inserted row: %v", err)
	}
	if status != string(StatusPending) {
		t.Fatalf("expected status=pending, got %q", status)
	}
	if jobType != string(JobTypeTraceCompaction) {
		t.Fatalf("expected job_type=trace_compaction, got %q", jobType)
	}
	if tenant != "tenant-a" {
		t.Fatalf("expected tenant=tenant-a, got %q", tenant)
	}
	var decoded TraceCompactionDetail
	if err := json.Unmarshal(gotDetail, &decoded); err != nil {
		t.Fatalf("unmarshal detail: %v", err)
	}
	if len(decoded.InputBlockIDs) != 2 || decoded.InputBlockIDs[0] != "block-b" || decoded.InputBlockIDs[1] != "block-a" {
		t.Fatalf("expected detail's InputBlockIDs to round-trip in original (unsorted) order, got %+v", decoded.InputBlockIDs)
	}
}

// TestStore_InsertTraceCompaction_DedupKeyIgnoresInputOrder proves the same 2
// input blocks in either order produce the identical dedup key -- job-planner's
// candidate query has no reason to always emit them in a stable order, so the
// sort inside InsertTraceCompaction must make that a non-load-bearing detail.
func TestStore_InsertTraceCompaction_DedupKeyIgnoresInputOrder(t *testing.T) {
	pool := newTestPostgresPool(t)
	store := New(pool)
	ctx := context.Background()

	if err := store.InsertTraceCompaction(ctx, "tenant-a", TraceCompactionDetail{InputBlockIDs: []string{"block-a", "block-b"}}); err != nil {
		t.Fatalf("InsertTraceCompaction (first): %v", err)
	}
	if err := store.InsertTraceCompaction(ctx, "tenant-a", TraceCompactionDetail{InputBlockIDs: []string{"block-b", "block-a"}}); err != nil {
		t.Fatalf("InsertTraceCompaction (reversed order): %v", err)
	}

	var count int
	row := pool.QueryRow(ctx, `SELECT count(*) FROM backend_jobs WHERE dedup_key = $1`, "trace_compaction|tenant-a|block-a,block-b")
	if err := row.Scan(&count); err != nil {
		t.Fatalf("counting rows: %v", err)
	}
	if count != 1 {
		t.Fatalf("expected exactly 1 row regardless of input order, got %d", count)
	}
}

// TestStore_InsertCatalogReconcile_DistinctSubsystemsSameTenant_BothRowsSurvive
// proves subsystem is part of the dedup key -- job-planner's catalog poll
// enumerates (subsystem, tenant) pairs independently per subsystem
// (plan.md Section C), so "vi"/tenant-a and "vcnt"/tenant-a must both get
// their own reconcile job, never absorbed as duplicates of each other.
func TestStore_InsertCatalogReconcile_DistinctSubsystemsSameTenant_BothRowsSurvive(t *testing.T) {
	pool := newTestPostgresPool(t)
	store := New(pool)
	ctx := context.Background()

	if err := store.InsertCatalogReconcile(ctx, "vi", "tenant-a"); err != nil {
		t.Fatalf("InsertCatalogReconcile (vi): %v", err)
	}
	if err := store.InsertCatalogReconcile(ctx, "vcnt", "tenant-a"); err != nil {
		t.Fatalf("InsertCatalogReconcile (vcnt): %v", err)
	}

	var count int
	row := pool.QueryRow(ctx, `SELECT count(*) FROM backend_jobs WHERE job_type = $1 AND tenant = $2`, "catalog_reconcile", "tenant-a")
	if err := row.Scan(&count); err != nil {
		t.Fatalf("counting rows: %v", err)
	}
	if count != 2 {
		t.Fatalf("expected 2 distinct reconcile rows (one per subsystem), got %d", count)
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

func TestStore_Fail_SchedulesRetryWithBackoff(t *testing.T) {
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
	if err := store.Fail(ctx, job.ID, "transient error"); err != nil {
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
		t.Fatal("expected next_retry_at to always be set (no retry limit), got NULL")
	}
	// backoffDuration(1) == 2 minutes.
	wantMin := before.Add(90 * time.Second)
	wantMax := before.Add(150 * time.Second)
	if nextRetryAt.Before(wantMin) || nextRetryAt.After(wantMax) {
		t.Fatalf("expected next_retry_at within [%v, %v] of first failure (2m backoff), got %v", wantMin, wantMax, nextRetryAt)
	}
}

// TestStore_Fail_NeverPermanentlyFails_RetriesIndefinitely is the 2026-07-17 reversal of the
// prior "#181 §8.1 locked default: 5 attempts, then permanently failed" ruling: Fail must
// unconditionally schedule a retry no matter how many times a job has already failed. Drives
// 10 consecutive failures (well past the old maxRetries=5 bound) and asserts next_retry_at is
// non-NULL and the job remains reclaimable after every single one, with backoff correctly
// capping at 30 minutes (backoffDuration) rather than growing unbounded.
func TestStore_Fail_NeverPermanentlyFails_RetriesIndefinitely(t *testing.T) {
	pool := newTestPostgresPool(t)
	store := New(pool)
	ctx := context.Background()

	if err := store.InsertViBackfill(ctx, "tenant-a", ViBackfillDetail{ColumnHash: "h1", ColumnType: "string"}); err != nil {
		t.Fatalf("seed: %v", err)
	}

	const attempts = 10
	for i := 1; i <= attempts; i++ {
		job, err := store.Claim(ctx, JobTypeViBackfill, "worker-1")
		if err != nil || job == nil {
			t.Fatalf("Claim (attempt %d): job=%+v err=%v", i, job, err)
		}
		if err := store.Fail(ctx, job.ID, fmt.Sprintf("failure %d", i)); err != nil {
			t.Fatalf("Fail (attempt %d): %v", i, err)
		}

		var retries int
		var nextRetryAt *time.Time
		row := pool.QueryRow(ctx, `SELECT retries, next_retry_at FROM backend_jobs WHERE id = $1`, job.ID)
		if err := row.Scan(&retries, &nextRetryAt); err != nil {
			t.Fatalf("querying failed row (attempt %d): %v", i, err)
		}
		if retries != i {
			t.Fatalf("attempt %d: expected retries=%d, got %d", i, i, retries)
		}
		if nextRetryAt == nil {
			t.Fatalf("attempt %d: expected next_retry_at to be set (no retry limit), got NULL", i)
		}

		// Move next_retry_at into the past so the loop's next Claim can reclaim it
		// immediately, without a real wall-clock wait.
		if _, err := pool.Exec(ctx, `UPDATE backend_jobs SET next_retry_at = now() - interval '1 second' WHERE id = $1`, job.ID); err != nil {
			t.Fatalf("attempt %d: rewinding next_retry_at: %v", i, err)
		}
	}

	// After 10 failures (double the old maxRetries=5 default), the job must still be
	// reclaimable -- proving there is genuinely no upper bound anymore.
	job, err := store.Claim(ctx, JobTypeViBackfill, "worker-2")
	if err != nil {
		t.Fatalf("Claim after %d failures: %v", attempts, err)
	}
	if job == nil {
		t.Fatalf("expected the job to still be reclaimable after %d failures, got nil", attempts)
	}
}

// TestStore_RenewLease_ExtendsLeaseExpiresAt pins issue #520's core fix: a
// claimed job's lease can be pushed forward by another 30 minutes from the
// CURRENT now(), not just the original claim time -- proving RenewLease
// actually does something, independent of the reclaim-prevention proof below.
func TestStore_RenewLease_ExtendsLeaseExpiresAt(t *testing.T) {
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

	// Rewind the original claim's lease so the renewed value is unambiguously
	// attributable to RenewLease's own now()+30m, not the original claim's.
	if _, err := pool.Exec(ctx, `UPDATE backend_jobs SET lease_expires_at = now() + interval '1 minute' WHERE id = $1`, job.ID); err != nil {
		t.Fatalf("rewinding lease: %v", err)
	}

	before := time.Now()
	if err := store.RenewLease(ctx, job.ID); err != nil {
		t.Fatalf("RenewLease: %v", err)
	}

	var leaseExpiresAt time.Time
	row := pool.QueryRow(ctx, `SELECT lease_expires_at FROM backend_jobs WHERE id = $1`, job.ID)
	if err := row.Scan(&leaseExpiresAt); err != nil {
		t.Fatalf("querying lease_expires_at: %v", err)
	}
	wantMin := before.Add(25 * time.Minute)
	if leaseExpiresAt.Before(wantMin) {
		t.Fatalf("expected lease_expires_at extended to ~now+30m (>= %v), got %v", wantMin, leaseExpiresAt)
	}
}

// TestStore_RenewLease_NoopForTerminalJob proves the status IN ('claimed',
// 'running') guard: renewing a job that has already reached a terminal state
// must not touch its lease_expires_at -- renewal only ever runs concurrently
// with active processing, so this is defense-in-depth against a benign race
// between the renewal loop's last tick and Complete/Fail, not a condition
// expected to matter in practice.
func TestStore_RenewLease_NoopForTerminalJob(t *testing.T) {
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

	var before time.Time
	row := pool.QueryRow(ctx, `SELECT lease_expires_at FROM backend_jobs WHERE id = $1`, job.ID)
	if err := row.Scan(&before); err != nil {
		t.Fatalf("querying lease_expires_at before renew: %v", err)
	}

	if err := store.RenewLease(ctx, job.ID); err != nil {
		t.Fatalf("RenewLease: %v", err)
	}

	var after time.Time
	row = pool.QueryRow(ctx, `SELECT lease_expires_at FROM backend_jobs WHERE id = $1`, job.ID)
	if err := row.Scan(&after); err != nil {
		t.Fatalf("querying lease_expires_at after renew: %v", err)
	}
	if !after.Equal(before) {
		t.Fatalf("expected RenewLease to be a no-op for a terminal (succeeded) job, lease_expires_at changed from %v to %v", before, after)
	}
}

// TestStore_RenewLease_PreventsReclaimPastOriginalLeaseWindow is issue #520's
// core regression pin. Without renewal, a job whose original lease has
// expired becomes reclaimable by a second worker while the first is still
// actively processing it -- exactly what
// TestStore_Claim_ExpiredLeaseOnClaimedRowIsReclaimable (above) already
// proves happens today for that same simulated state. This test proves
// RenewLease closes that gap: given the IDENTICAL simulated "past the
// original lease window" state, calling RenewLease first makes the job NOT
// reclaimable, because renewal sets a fresh lease_expires_at relative to the
// CURRENT now(), independent of how stale the original claim's lease was.
func TestStore_RenewLease_PreventsReclaimPastOriginalLeaseWindow(t *testing.T) {
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

	// Simulate the original 30-minute lease having already expired -- the exact
	// state that (without renewal) makes a job reclaimable.
	if _, err := pool.Exec(ctx, `
		UPDATE backend_jobs SET claimed_at = now() - interval '1 hour', lease_expires_at = now() - interval '5 minutes'
		WHERE id = $1`, job.ID); err != nil {
		t.Fatalf("simulating expired original lease: %v", err)
	}

	if err := store.RenewLease(ctx, job.ID); err != nil {
		t.Fatalf("RenewLease: %v", err)
	}

	reclaimed, err := store.Claim(ctx, JobTypeViBackfill, "worker-2")
	if err != nil {
		t.Fatalf("Claim (worker-2): %v", err)
	}
	if reclaimed != nil {
		t.Fatalf("expected the job to NOT be reclaimable after RenewLease, but worker-2 claimed job %s", reclaimed.ID)
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
