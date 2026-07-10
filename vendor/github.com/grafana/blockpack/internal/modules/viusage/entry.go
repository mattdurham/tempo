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
	// UseTimestamps is a bounded, newest-appended ring of recent distinct-query use
	// timestamps (unix seconds), used to evaluate the rolling-window repeated-use
	// threshold (R4). Bounded at MaxTrackedUses (default 32) entries — old entries are
	// dropped from the front once the bound is hit; this is deliberately small since the
	// only thing ever asked of it is "how many uses fell within the last WindowSeconds,"
	// and a repeated-use threshold in the single digits never needs more than a few tens
	// of samples to answer that, even under bursty query traffic.
	UseTimestamps []uint64 `json:"use_timestamps,omitempty"`
	// Backfill tracks the trigger/lease/watermark state for this column (see 4.4).
	Backfill BackfillState `json:"backfill"`
	// FirstSeenSec is the unix-second timestamp of the first recorded use.
	FirstSeenSec uint64 `json:"first_seen_sec"`
	// CreatedAt is unix seconds when the entry was first registered.
	CreatedAt uint64 `json:"created_at"`
}

// MaxTrackedUses bounds Entry.UseTimestamps (R4/4.1).
const MaxTrackedUses = 32

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
	// Done is true once the full configured backfill window
	// [now - WindowSeconds, now] is confirmed complete. Once Done, ordinary
	// FilesForTimeRange-based discovery is trusted without any watermark gating (R5:
	// same upkeep as any dedicated column from this point forward).
	Done bool `json:"done"`
}

// CoversRange reports whether bs's backfill state fully covers [minSec, maxSec] — the
// R7 query-time coverage-check primitive. Pure, no I/O. This is the ONE function every
// query-path coverage decision for a non-dedicated, usage-tracked column must call before
// trusting a non-empty FilesForTimeRange result.
func (bs BackfillState) CoversRange(minSec, maxSec uint64) bool {
	if bs.Done {
		return true
	}
	if !bs.Triggered {
		return false // never indexed — no coverage at all, matches today's "zero files" case
	}
	// In-progress, newest-to-oldest fill: the covered range is [WatermarkSec, now].
	// The query's window is covered ONLY if its oldest point (minSec) is not older
	// than the watermark — any older sub-range is unconfirmed and must decline.
	return minSec >= bs.WatermarkSec
}
