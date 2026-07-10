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

// ColumnWatermark is the query-time-relevant subset of
// viusage.Entry.BackfillState (duplicated as a tiny value type here rather
// than imported, per the same import-direction reasoning above -- vibuilder
// must not depend on internal/modules/viusage either, to keep this package's
// dependency graph a leaf). Triggered/Done/WatermarkSec together answer
// exactly one question: does this column's backfill state confirm complete
// coverage for [minSec, maxSec]?
type ColumnWatermark struct {
	Triggered    bool
	Done         bool
	WatermarkSec uint64
}

// CoversRange reports whether w's backfill state fully covers [minSec, maxSec]
// -- the R7 query-time coverage-check primitive. Pure, no I/O. This is the
// ONE function every query-path coverage decision for a non-dedicated,
// usage-tracked column must call before trusting a non-empty file-discovery
// result; logic must stay identical to viusage.BackfillState.CoversRange
// (plan.md Section 4.1) since both express the same contract for the same
// underlying registry state, just via two independent value types (see this
// file's own doc comment for why they cannot share one type without an
// import cycle).
func (w ColumnWatermark) CoversRange(minSec, maxSec uint64) bool {
	if w.Done {
		return true
	}
	if !w.Triggered {
		return false // never indexed -- no coverage at all, matches today's "zero files" case
	}
	// In-progress, newest-to-oldest fill: the covered range is [WatermarkSec, now].
	// The query's window is covered ONLY if its oldest point (minSec) is not older
	// than the watermark -- any older sub-range is unconfirmed and must decline.
	return minSec >= w.WatermarkSec
}
