package vibuilder

// watermark.go -- #496 R7's query-time coverage-check primitive (plan.md
// Section 4.7). ColumnWatermark lives here (not the root blockpack package,
// despite being public API via a root-level type alias) because BuildSource
// -- this package -- is the actual consumer of the gate: root blockpack
// already imports vibuilder (BuildValueIndexSource calls vibuilder.BuildSource),
// so if ColumnWatermark lived in root instead, vibuilder would need to import
// root to reference it, creating an import cycle. Root re-exports this type
// via a plain alias (matching the existing FileStore/ErrFileNotFound
// re-export pattern in valueindex_query.go) so external callers never need to
// import an internal package directly.

// GapRange is one contiguous, NOT-yet-covered [StartSec, EndSec) span. Deliberately a distinct
// name/type from blockpack's root-level WindowRange (pgqueue.WindowRange, int64-based, the raw
// Postgres query-result shape) even though both packages express the same {start,end} pair --
// this one matches ColumnWatermark.WatermarkSec's own pre-existing uint64 convention, and
// giving it its own name avoids a root-export naming collision when both get aliased out of
// their respective internal packages.
type GapRange struct {
	StartSec uint64
	EndSec   uint64
}

// overlaps reports whether r overlaps [minSec, maxSec] (inclusive on both ends, matching
// CoversRange's own inclusive contract).
func (r GapRange) overlaps(minSec, maxSec uint64) bool {
	return r.StartSec <= maxSec && minSec < r.EndSec
}

// ColumnWatermark is the query-time-relevant subset of
// viusage.Entry.BackfillState (duplicated as a tiny value type here rather
// than imported, per the same import-direction reasoning above -- vibuilder
// must not depend on internal/modules/viusage either, to keep this package's
// dependency graph a leaf).
//
// GapRanges (issue #529) replaces a single scalar watermark as the actual coverage-check input:
// with many independent, parallel 1-minute vi_backfill jobs, completion is no longer guaranteed
// monotonic (a genuinely older window can finish after a genuinely newer one), so "covered iff
// minSec >= WatermarkSec" can no longer correctly represent coverage that has gaps in the
// middle. GapRanges is the caller's pre-fetched, already-merged set of NOT-yet-succeeded
// windows (blockpack.Postgres.ViBackfillGapRanges) for this column -- empty means fully covered.
// WatermarkSec is kept for observability/backward-compatible callers that still read it
// directly, but CoversRange no longer consults it.
type ColumnWatermark struct {
	GapRanges    []GapRange
	WatermarkSec uint64
	Triggered    bool
	Done         bool
}

// CoversRange reports whether w's backfill state fully covers [minSec, maxSec] -- the R7
// query-time coverage-check primitive. Pure, no I/O (GapRanges is pre-fetched by the caller,
// once per cache refresh, not per call). This is the ONE function every query-path coverage
// decision for a non-dedicated, usage-tracked column must call before trusting a non-empty
// file-discovery result.
func (w ColumnWatermark) CoversRange(minSec, maxSec uint64) bool {
	if !w.Triggered {
		return false // never indexed -- no coverage at all, matches today's "zero files" case
	}
	for _, gap := range w.GapRanges {
		if gap.overlaps(minSec, maxSec) {
			return false
		}
	}
	return true
}
