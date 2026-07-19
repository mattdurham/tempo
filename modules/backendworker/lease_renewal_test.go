package backendworker

// lease_renewal_test.go — issue #520: backend_jobs' lease was set once at claim
// time (jobstore.claimJobSQL: lease_expires_at = now() + 30 minutes) and never
// renewed, so any job genuinely running longer than 30 minutes became silently
// reclaimable by a second worker while the original worker was still actively
// processing it. jobstore's own TestStore_RenewLease_* (jobstore_test.go) prove
// RenewLease's SQL effect and its reclaim-prevention property in isolation; this
// file proves the periodic renewal LOOP itself -- ticking on schedule and
// stopping cleanly on ctx cancellation, the exact mechanism dispatchPostgresJob
// wires around job processing via defer.

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/grafana/tempo/tempodb/encoding/vblockpack/jobstore"
)

// TestRenewLeasePeriodically_RenewsUntilCanceled proves renewLeasePeriodically
// actually ticks (real Postgres lease_expires_at advances well past the
// original 30-minute window from a job seeded with an about-to-expire lease)
// and stops promptly once its ctx is canceled -- the same cancellation
// dispatchPostgresJob's own `defer stopRenew()` triggers on completion,
// failure, or panic unwind.
func TestRenewLeasePeriodically_RenewsUntilCanceled(t *testing.T) {
	pool, _ := newTestPostgresPoolAndDSN(t)
	store := jobstore.New(pool)
	ctx := context.Background()

	require.NoError(t, store.InsertViBackfill(ctx, "tenant-a", jobstore.ViBackfillDetail{ColumnHash: "h1", ColumnType: "string"}))
	job, err := store.Claim(ctx, jobstore.JobTypeViBackfill, "worker-1")
	require.NoError(t, err)
	require.NotNil(t, job)

	// Rewind the original claim's lease close to expiry so a later renewal's
	// effect is unambiguous.
	_, err = pool.Exec(ctx, `UPDATE backend_jobs SET lease_expires_at = now() + interval '1 second' WHERE id = $1`, job.ID)
	require.NoError(t, err)

	w := &BackendWorker{jobStore: store}

	orig := leaseRenewInterval
	leaseRenewInterval = 30 * time.Millisecond
	defer func() { leaseRenewInterval = orig }()

	renewCtx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		w.renewLeasePeriodically(renewCtx, job.ID)
		close(done)
	}()

	// Several ticks' worth of real time so at least one renewal fires.
	time.Sleep(150 * time.Millisecond)
	cancel()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("renewLeasePeriodically did not stop promptly after ctx cancellation")
	}

	var leaseExpiresAt time.Time
	row := pool.QueryRow(ctx, `SELECT lease_expires_at FROM backend_jobs WHERE id = $1`, job.ID)
	require.NoError(t, row.Scan(&leaseExpiresAt))
	assert.True(t, leaseExpiresAt.After(time.Now().Add(29*time.Minute)),
		"expected at least one renewal tick to have extended the lease close to now+30m, got %v", leaseExpiresAt)
}

// TestRenewLeasePeriodically_StopsImmediatelyOnAlreadyCanceledCtx proves the
// loop never issues a single RenewLease call when its ctx is already done
// before the first tick -- exactly the "stopped via defer on completion" case
// where a job finishes fast, well under leaseRenewInterval.
func TestRenewLeasePeriodically_StopsImmediatelyOnAlreadyCanceledCtx(t *testing.T) {
	pool, _ := newTestPostgresPoolAndDSN(t)
	store := jobstore.New(pool)
	ctx := context.Background()

	require.NoError(t, store.InsertViBackfill(ctx, "tenant-a", jobstore.ViBackfillDetail{ColumnHash: "h2", ColumnType: "string"}))
	job, err := store.Claim(ctx, jobstore.JobTypeViBackfill, "worker-1")
	require.NoError(t, err)
	require.NotNil(t, job)

	var before time.Time
	row := pool.QueryRow(ctx, `SELECT lease_expires_at FROM backend_jobs WHERE id = $1`, job.ID)
	require.NoError(t, row.Scan(&before))

	w := &BackendWorker{jobStore: store}

	orig := leaseRenewInterval
	leaseRenewInterval = 10 * time.Minute // long enough that no real tick could fire
	defer func() { leaseRenewInterval = orig }()

	renewCtx, cancel := context.WithCancel(context.Background())
	cancel() // already canceled before renewLeasePeriodically is even called

	done := make(chan struct{})
	go func() {
		w.renewLeasePeriodically(renewCtx, job.ID)
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("renewLeasePeriodically did not return immediately for an already-canceled ctx")
	}

	var after time.Time
	row = pool.QueryRow(ctx, `SELECT lease_expires_at FROM backend_jobs WHERE id = $1`, job.ID)
	require.NoError(t, row.Scan(&after))
	assert.True(t, after.Equal(before), "expected no renewal to have run for an already-canceled ctx, lease_expires_at changed from %v to %v", before, after)
}
