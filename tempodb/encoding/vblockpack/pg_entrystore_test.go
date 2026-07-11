package vblockpack

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"

	blockpack "github.com/grafana/blockpack"
)

func TestPgViUsageEntryStore_UpsertEntry_CreateThenMutate(t *testing.T) {
	pool := newTestPostgresPool(t)
	store := newPgViUsageEntryStore(pool)
	ctx := context.Background()

	created := blockpack.Entry{
		Tenant: "tenant-a", ColumnHash: "abc123", ColumnType: "string", ColumnName: "span.name",
		FirstSeenSec: 100, CreatedAt: 100,
	}
	entry, err := store.UpsertEntry(ctx, "tenant-a", "abc123", "string",
		func() blockpack.Entry { return created },
		func(_ *blockpack.Entry) error { return nil },
	)
	if err != nil {
		t.Fatalf("UpsertEntry (create): %v", err)
	}
	if entry.ColumnName != "span.name" {
		t.Fatalf("created entry mismatch: %+v", entry)
	}

	entries, err := store.Load(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("Load after create: %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("expected 1 entry after create, got %d", len(entries))
	}

	mutated, err := store.UpsertEntry(ctx, "tenant-a", "abc123", "string",
		nil,
		func(e *blockpack.Entry) error {
			e.Backfill.Triggered = true
			return nil
		},
	)
	if err != nil {
		t.Fatalf("UpsertEntry (mutate): %v", err)
	}
	if !mutated.Backfill.Triggered {
		t.Fatalf("expected Triggered=true after mutate, got %+v", mutated.Backfill)
	}

	entries, err = store.Load(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("Load after mutate: %v", err)
	}
	if len(entries) != 1 || !entries[0].Backfill.Triggered {
		t.Fatalf("persisted entry not updated: %+v", entries)
	}
}

func TestPgViUsageEntryStore_UpsertEntry_MissingNoCreateIfMissing_Errors(t *testing.T) {
	pool := newTestPostgresPool(t)
	store := newPgViUsageEntryStore(pool)
	ctx := context.Background()

	_, err := store.UpsertEntry(ctx, "tenant-a", "never-registered", "string",
		nil,
		func(_ *blockpack.Entry) error { return nil },
	)
	if err == nil {
		t.Fatal("expected an error for UpsertEntry on a missing entry with nil createIfMissing, got nil")
	}
}

func TestPgViUsageEntryStore_Load_ReturnsAllRowsForTenant_NotOtherTenants(t *testing.T) {
	pool := newTestPostgresPool(t)
	store := newPgViUsageEntryStore(pool)
	ctx := context.Background()

	for _, tc := range []struct{ tenant, colHash string }{
		{"tenant-a", "col1"},
		{"tenant-a", "col2"},
		{"tenant-b", "col1"},
	} {
		_, err := store.UpsertEntry(ctx, tc.tenant, tc.colHash, "string",
			func() blockpack.Entry {
				return blockpack.Entry{Tenant: tc.tenant, ColumnHash: tc.colHash, ColumnType: "string", ColumnName: tc.colHash}
			},
			func(_ *blockpack.Entry) error { return nil },
		)
		if err != nil {
			t.Fatalf("seeding %s/%s: %v", tc.tenant, tc.colHash, err)
		}
	}

	entriesA, err := store.Load(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("Load tenant-a: %v", err)
	}
	if len(entriesA) != 2 {
		t.Fatalf("expected 2 entries for tenant-a, got %d: %+v", len(entriesA), entriesA)
	}
	for _, e := range entriesA {
		if e.Tenant != "tenant-a" {
			t.Fatalf("tenant-a Load leaked a row from another tenant: %+v", e)
		}
	}

	entriesB, err := store.Load(ctx, "tenant-b")
	if err != nil {
		t.Fatalf("Load tenant-b: %v", err)
	}
	if len(entriesB) != 1 {
		t.Fatalf("expected 1 entry for tenant-b, got %d: %+v", len(entriesB), entriesB)
	}
}

func TestPgCubeEntryStore_AddEntry_IdempotentAndMaxCubesEnforced(t *testing.T) {
	pool := newTestPostgresPool(t)
	store := newPgCubeEntryStore(pool)
	ctx := context.Background()

	entry := blockpack.CubeRegistryEntry{
		CubeID: "cube1", Tenant: "tenant-a", Dimensions: []string{"resource.service.name"},
		AggAttrs: []string{"duration"}, Resolution: 1, CreatedAt: 100,
	}
	if err := store.AddEntry(ctx, "tenant-a", entry, 10); err != nil {
		t.Fatalf("AddEntry (first): %v", err)
	}
	// Idempotent: adding the same CubeID again must be a no-op, not an error.
	if err := store.AddEntry(ctx, "tenant-a", entry, 10); err != nil {
		t.Fatalf("AddEntry (idempotent repeat): %v", err)
	}

	entries, err := store.Load(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("expected exactly 1 entry after idempotent repeat, got %d", len(entries))
	}

	// maxCubes=1 is already reached; a genuinely new CubeID must be rejected.
	second := entry
	second.CubeID = "cube2"
	err = store.AddEntry(ctx, "tenant-a", second, 1)
	if err == nil {
		t.Fatal("expected CubeErrLimitReached when maxCubes is already reached, got nil")
	}
	var limitErr *blockpack.CubeErrLimitReached
	if !errors.As(err, &limitErr) {
		t.Fatalf("expected *blockpack.CubeErrLimitReached, got: %v", err)
	}
}

func TestPgCubeEntryStore_RemoveEntry_IdempotentOnAbsent(t *testing.T) {
	pool := newTestPostgresPool(t)
	store := newPgCubeEntryStore(pool)
	ctx := context.Background()

	if err := store.RemoveEntry(ctx, "tenant-a", "never-existed"); err != nil {
		t.Fatalf("RemoveEntry on absent cube should be a no-op, got: %v", err)
	}

	entry := blockpack.CubeRegistryEntry{CubeID: "cube1", Tenant: "tenant-a", Dimensions: []string{"x"}, AggAttrs: []string{"duration"}, Resolution: 1}
	if err := store.AddEntry(ctx, "tenant-a", entry, 10); err != nil {
		t.Fatalf("AddEntry: %v", err)
	}
	if err := store.RemoveEntry(ctx, "tenant-a", "cube1"); err != nil {
		t.Fatalf("RemoveEntry: %v", err)
	}
	entries, err := store.Load(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if len(entries) != 0 {
		t.Fatalf("expected 0 entries after remove, got %d", len(entries))
	}
}

func TestPgCubeEntryStore_UpdateWatermarksEntry_MinOfMinsMaxOfMaxes(t *testing.T) {
	pool := newTestPostgresPool(t)
	store := newPgCubeEntryStore(pool)
	ctx := context.Background()

	entry := blockpack.CubeRegistryEntry{CubeID: "cube1", Tenant: "tenant-a", Dimensions: []string{"x"}, AggAttrs: []string{"duration"}, Resolution: 1}
	if err := store.AddEntry(ctx, "tenant-a", entry, 10); err != nil {
		t.Fatalf("AddEntry: %v", err)
	}

	if err := store.UpdateWatermarksEntry(ctx, "tenant-a", "cube1", 1, 100, 200); err != nil {
		t.Fatalf("UpdateWatermarksEntry (first): %v", err)
	}
	// A narrower window must NOT shrink the existing coverage (min-of-mins, max-of-maxes).
	if err := store.UpdateWatermarksEntry(ctx, "tenant-a", "cube1", 1, 150, 180); err != nil {
		t.Fatalf("UpdateWatermarksEntry (narrower): %v", err)
	}
	// A wider window on the other side must extend coverage.
	if err := store.UpdateWatermarksEntry(ctx, "tenant-a", "cube1", 1, 50, 250); err != nil {
		t.Fatalf("UpdateWatermarksEntry (wider): %v", err)
	}

	entries, err := store.Load(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("expected 1 entry, got %d", len(entries))
	}
	wm, ok := entries[0].Watermarks[1]
	if !ok {
		t.Fatalf("expected watermark at level 1, got %+v", entries[0].Watermarks)
	}
	if wm.MinMinute != 50 || wm.MaxMinute != 250 {
		t.Fatalf("expected min-of-mins/max-of-maxes [50,250], got [%d,%d]", wm.MinMinute, wm.MaxMinute)
	}
}

func TestPgCubeEntryStore_UpdateWatermarksEntry_NotFoundErrors(t *testing.T) {
	pool := newTestPostgresPool(t)
	store := newPgCubeEntryStore(pool)
	ctx := context.Background()

	err := store.UpdateWatermarksEntry(ctx, "tenant-a", "never-existed", 1, 100, 200)
	if err == nil {
		t.Fatal("expected an error updating watermarks for a nonexistent cube, got nil")
	}
}

// TestPgViUsageEntryStore_UpsertEntry_ConcurrentTriggersConvergeOnOneWinner is
// the required regression guard (plan.md Part 5.3): N goroutines racing
// UpsertEntry for the SAME (tenant, colHash, colType) key, with a
// createIfMissing/mutate pair structurally equivalent to
// RecordUseAndMaybeTrigger's post-Part-0 unconditional-trigger logic (first
// caller to observe "not yet triggered" wins and sets its own LeaseOwnerID;
// every other concurrent caller observes Triggered already true and takes the
// no-op branch, returning the WINNER's already-persisted LeaseOwnerID as part
// of its own returned entry snapshot -- not its own). Exactly one distinct
// LeaseOwnerID across all N returned snapshots is the load-bearing assertion:
// SELECT ... FOR UPDATE's row lock is what makes every racer either BE the
// first writer or OBSERVE the first writer's already-committed row, never see
// a stale "not yet triggered" snapshot concurrently with another racer.
//
// Mutation-tested per this session's standing convention (memory:
// "reintroduce the exact bug a test claims to catch, confirm it fails, then
// revert"): temporarily removed loadViUsageEntryForUpdate's "FOR UPDATE"
// clause. With the plain 20-goroutine race alone this did NOT reliably
// reproduce a failure (a local-Docker round-trip is fast and unsynchronized
// goroutine starts rarely land inside the now-unlocked window) -- to make the
// race deterministic, also added a temporary time.Sleep(50ms) between the
// load and the mutate/update inside UpsertEntry, widening the window. With
// both the FOR UPDATE removal AND the artificial delay in place, this test
// correctly FAILED (a duplicate-key error from two racers both observing
// "not found" and both attempting to INSERT the same row -- the row lock's
// absence, made deterministic). Reverted BOTH changes afterward and confirmed
// this test (and the full package suite) passes again.
func TestPgViUsageEntryStore_UpsertEntry_ConcurrentTriggersConvergeOnOneWinner(t *testing.T) {
	pool := newTestPostgresPool(t)
	store := newPgViUsageEntryStore(pool)
	ctx := context.Background()

	const n = 20
	tenant, colHash, colType := "tenant-a", "concurrent-col", "string"

	var wg sync.WaitGroup
	owners := make([]string, n)
	errs := make([]error, n)
	for i := range n {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			ownerID := fmt.Sprintf("owner-%d", i)
			entry, err := store.UpsertEntry(ctx, tenant, colHash, colType,
				func() blockpack.Entry {
					return blockpack.Entry{Tenant: tenant, ColumnHash: colHash, ColumnType: colType, ColumnName: "span.name"}
				},
				func(e *blockpack.Entry) error {
					if e.Backfill.Done || e.Backfill.Triggered {
						return nil // R5/R8: never re-trigger; lease already held by the winner
					}
					e.Backfill.Triggered = true
					e.Backfill.BackfillInProgress = true
					e.Backfill.LeaseOwnerID = ownerID
					e.Backfill.LeaseExpiresAt = 9999999999
					return nil
				},
			)
			errs[i] = err
			if err == nil {
				owners[i] = entry.Backfill.LeaseOwnerID
			}
		}(i)
	}
	wg.Wait()

	for i, err := range errs {
		if err != nil {
			t.Fatalf("goroutine %d UpsertEntry error: %v", i, err)
		}
	}

	entries, err := store.Load(ctx, tenant)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("expected exactly 1 persisted entry (no duplicate rows from a lost race), got %d", len(entries))
	}
	final := entries[0]
	if !final.Backfill.Triggered || !final.Backfill.BackfillInProgress {
		t.Fatalf("expected final row Triggered=true, BackfillInProgress=true, got %+v", final.Backfill)
	}

	distinctOwners := map[string]int{}
	for _, o := range owners {
		distinctOwners[o]++
	}
	if len(distinctOwners) != 1 {
		t.Fatalf("expected exactly one distinct LeaseOwnerID across all %d racers (no lost update / no double-trigger), got %d distinct: %v", n, len(distinctOwners), distinctOwners)
	}
	if _, ok := distinctOwners[final.Backfill.LeaseOwnerID]; !ok {
		t.Fatalf("final persisted LeaseOwnerID %q not among the racers' returned owner IDs: %v", final.Backfill.LeaseOwnerID, distinctOwners)
	}
}
