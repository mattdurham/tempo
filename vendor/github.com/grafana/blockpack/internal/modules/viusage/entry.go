package viusage

// NOTE: SPEC-VIUSAGE-001 — Entry is the full, stable description of one tracked
// (tenant, column) usage/backfill record, stored in the tenant-level
// <tenant>/viusage/index.json. Keyed by (Tenant, ColumnHash, ColumnType) — the same
// colHash/typeName convention valueindex already uses for file sharding
// (valueindex.ColHash/ColTypeName), so a usage entry's key trivially maps onto the same
// on-disk column directory VI writes to. See internal/modules/viusage/NOTES.md for the
// R1-R13 design rulings this module implements.

// Entry is the full, stable description of one tracked (tenant, column) usage/backfill
// record. Stored in the tenant-level index.json (mirrors cube's registry.go pattern,
// NOT its type). Keyed by (Tenant, ColumnHash, ColumnType) — the same colHash/typeName
// convention valueindex already uses for file sharding (valueindex.ColHash/ColTypeName),
// so a usage entry's key trivially maps onto the same on-disk column directory VI writes
// to.
type Entry struct {
	// Tenant this entry belongs to.
	Tenant string `json:"tenant"`
	// ColumnHash is valueindex.ColHash(ColumnName) — the same hash VI's file layout uses.
	ColumnHash string `json:"column_hash"`
	// ColumnName is the human-readable column name (debugging/logging only; ColumnHash
	// is the actual join key against VI's on-disk layout since VI never round-trips
	// names back from hashes).
	ColumnName string `json:"column_name"`
	// ColumnType is valueindex.ColTypeName(colType) — part of the key, mirroring VI's
	// own (colHash, colType) file-sharding: the same name observed as two distinct types
	// tracks as two independent entries, matching l0Group's own keying in
	// valueindex_l0write.go.
	ColumnType string `json:"column_type"`
	// Backfill tracks the trigger/lease/watermark state for this column (see 4.4).
	Backfill BackfillState `json:"backfill"`
	// FirstSeenSec is the unix-second timestamp of the first recorded use.
	FirstSeenSec uint64 `json:"first_seen_sec"`
	// CreatedAt is unix seconds when the entry was first registered.
	CreatedAt uint64 `json:"created_at"`
}

// BackfillState is one column's backfill lifecycle state. Zero value means "never
// triggered" (Triggered=false), which is the correct default for a freshly-created Entry
// that only exists because a use was recorded but the threshold has not yet been crossed.
type BackfillState struct {
	// LeaseOwnerID is an opaque identifier (hostname+pid, or a UUID) for observability —
	// which replica/job instance currently holds the lease. Not used for correctness
	// (TTL expiry is), only for debugging "who is doing this backfill."
	LeaseOwnerID   string `json:"lease_owner_id,omitempty"`
	LeaseExpiresAt uint64 `json:"lease_expires_at,omitempty"`
	// WatermarkSec is the R7 coverage watermark: the oldest wall-clock unix-second for
	// which this column's backfill is confirmed COMPLETE, given a newest-to-oldest fill
	// direction (mirrors cube's BackfillWatermark.WatermarkMinute convention, but in
	// seconds since VI files are second-granular, not minute-granular like cube's L0
	// rollup unit). Zero until the first unit of backfill work completes.
	WatermarkSec uint64 `json:"watermark_sec"`
	// WindowStartSec/WindowEndSec record the backfill window this entry's watermark is
	// scoped to (the config value in effect when the backfill was triggered), so a
	// later config change to the default window does not retroactively reinterpret an
	// already-Done entry's coverage.
	WindowStartSec uint64 `json:"window_start_sec"`
	WindowEndSec   uint64 `json:"window_end_sec"`
	// Triggered is true once the repeated-use threshold has been crossed and a backfill
	// has been (or is being) started. Never reverts to false (R5: no eviction in v1).
	Triggered bool `json:"triggered"`
	// BackfillInProgress + LeaseExpiresAt implement the R8 lease: a second replica's
	// trigger-check consults this pair and skips launching a redundant backfill job
	// while BackfillInProgress is true AND LeaseExpiresAt is in the future. A crashed
	// worker's lease self-heals once LeaseExpiresAt passes — no manual intervention
	// needed (see 4.4 lifecycle).
	BackfillInProgress bool `json:"backfill_in_progress"`
	// Done is job-planner's chaining-stop signal ONLY (#519) -- true iff a run's
	// own window was exhausted AND the resolved floor (minSec) is genuinely 0
	// (true beginning of time, or an unbounded run). NOT a query-correctness
	// input: CoversRange never consults Done, only Triggered/WatermarkSec.
	// Removing Done entirely was considered and rejected (NOTES.md) since
	// job-planner's chaining query still needs SOME "stop enqueuing" signal,
	// and this is simpler than pushing a WatermarkSec==0 check to every caller.
	Done bool `json:"done"`
	// LastCatalogRowID is the highest file_catalog row_id this column's backfill has
	// fully processed (tempo's catalog-cursor-based BlockFetcher, 2026-07-11). Zero
	// means "never run against the catalog" — a catalog-backed fetcher then lists ALL
	// rows for the tenant, equivalent to a full first listing. Only ever advances
	// (monotonic) — see Registry.UpdateCatalogCursor.
	// SPEC-VIUSAGE-9: monotonic file-catalog cursor.
	LastCatalogRowID uint64 `json:"last_catalog_row_id,omitempty"`
}

// CoversRange reports whether bs's backfill state fully covers [minSec, maxSec] — the
// R7 query-time coverage-check primitive. Pure, no I/O. #519: Done is never consulted here —
// see BackfillState.Done's own doc comment.
//
// NOTE (issue #529): this scalar-watermark check has ZERO live callers as of 2026-07-22 --
// vibuilder.ColumnWatermark.CoversRange (the actual, wired-up query-path gate, via
// watermarksForOrNil/tryIndexFetch in tempo) has moved to a per-window GapRanges model, since a
// single scalar cannot correctly represent coverage with gaps in the middle once many
// independent 1-minute vi_backfill jobs can complete out of order across parallel workers. This
// method is kept for whatever legacy/observability role BackfillState itself still serves, but
// it is NO LONGER claimed to be behaviorally identical to ColumnWatermark's -- do not assume
// parity between the two (the coversrange_parity_test.go table-driven test asserting exactly
// that was retired alongside this change).
func (bs BackfillState) CoversRange(minSec, maxSec uint64) bool {
	if !bs.Triggered {
		return false // never indexed — no coverage at all, matches today's "zero files" case
	}
	// #519: Done is NOT consulted here — it is a job-planner scheduling signal
	// only (see BackfillState.Done's own doc comment), never a query-correctness
	// bypass. Coverage is always this range check: [WatermarkSec, now) newest-to-
	// oldest fill, covered only if the query's oldest point (minSec) is not older
	// than the watermark. Once WatermarkSec genuinely reaches 0, this check alone
	// covers any real range with zero special-casing — the same property Done's
	// new definition depends on.
	return minSec >= bs.WatermarkSec
}
