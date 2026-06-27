package shared

// IsIntrinsicColumn reports whether name is an intrinsic column — one that is stored in the
// file-level intrinsic section AND in block column payloads (dual storage; see writer NOTE-002).
// Both the writer (to classify columns into intrinsic vs attribute tier at block-build time) and
// the reader (to select which TOC tier to consult for a given column name) use this function.
//
// The set covers trace signal intrinsics (trace:id, span:*, resource.service.name).
// resource.service.name is included because it is "practically intrinsic" — it is present
// in the intrinsic section.
func IsIntrinsicColumn(name string) bool {
	_, ok := intrinsicColumnSet[name]
	return ok
}

// SemanticBytesEncoding identifies the dense base encoding kind that a semantic-override entry
// pins for a named bytes column. The writer maps it to the concrete dense/sparse/all-present
// kind ID at flush time; this enum is encoding-family-level so the override table stays
// independent of the kind-ID numbering in constants.go.
type SemanticBytesEncoding uint8

const (
	// SemanticBytesNone means no override — fall through to the cost-based selector.
	SemanticBytesNone SemanticBytesEncoding = iota
	// SemanticBytesDeltaDictionary pins the DeltaDictionary family (kinds 12/13/21).
	SemanticBytesDeltaDictionary
	// SemanticBytesXOR pins the XOR family (kinds 8/9/19 and uniform 24/25/28).
	SemanticBytesXOR
	// SemanticBytesPrefix pins the Prefix family (kinds 10/11/20).
	SemanticBytesPrefix
)

// semanticOverride records a pinned encoding family for a named intrinsic bytes column plus the
// data-shape justification that makes the pin correct a-priori (issue #333, NOTE-221).
type semanticOverride struct {
	reason string
	enc    SemanticBytesEncoding
}

// SemanticBytesOverride returns the pinned bytes encoding family for an intrinsic column name,
// or SemanticBytesNone when the column has no override (the common case → cost-based selector).
//
// Only intrinsic columns (IsIntrinsicColumn) are ever eligible; user-defined attribute names
// never match. Each entry has a-priori knowledge of the value shape that makes the pin beat the
// cost estimator, so it bypasses cost analysis. See issue #333 / writer NOTE-221.
func SemanticBytesOverride(name string) SemanticBytesEncoding {
	o, ok := semanticBytesOverrides[name]
	if !ok {
		return SemanticBytesNone
	}
	return o.enc
}

// semanticBytesOverrides is the small, deliberate allow-list of intrinsic bytes columns whose
// best encoding is known by name. Each entry is justified by the value shape; adding one requires
// a benchmark showing a >10% win over the cost-based path for that column (issue #333).
var semanticBytesOverrides = map[string]semanticOverride{
	// trace:id — 16-byte trace IDs are written in sorted order within a block, so the
	// delta-dictionary form (sorted dict + delta-coded indexes) dominates plain dict and XOR.
	"trace:id": {reason: "sorted 16-byte trace IDs", enc: SemanticBytesDeltaDictionary},
	// span:id / span:parent_id — 8/16-byte IDs that share high-order bits with their
	// predecessor within a block; XOR-against-previous zeroes those bytes (uniform variant
	// further drops the per-row length prefix).
	SpanIDColumnName:       {reason: "fixed-width IDs with shared high-order bits", enc: SemanticBytesXOR},
	SpanParentIDColumnName: {reason: "fixed-width IDs with shared high-order bits", enc: SemanticBytesXOR},
}

// ShouldSketchColumn reports whether the per-column sketch (HLL distinct-count + TopK +
// SketchBloom) should be built for the named column. It returns false for identity /
// high-entropy ID columns whose sketch carries no query value (NOTE-402, issue #354).
//
// The per-column SketchBloom powers predicate block-pruning (queryplanner/scoring.go,
// FuseContains). For high-cardinality identity columns (span:id, span:parent_id and their
// log equivalents) every block holds thousands of distinct random IDs, so the bloom is
// saturated (~5% FPR at 5 000 values) and prunes nothing; meanwhile each such column emits
// a maximal 2 KiB bloom per block — the single most expensive sketch on disk. No TraceQL
// query computes quantiles/histograms or block-prunes on span/parent IDs (ID lookups use
// the trace-ID bloom and equality scans, independent of the per-column sketch), so dropping
// these sketches loses nothing.
//
// The reader already tolerates an absent per-column sketch: ColumnSketch returns nil and the
// scoring/pruning path takes its conservative "pass all candidates" branch. So skipping the
// sketch is behavior-preserving and requires no format change (the writer simply emits fewer
// sketch blobs).
func ShouldSketchColumn(name string) bool {
	_, skip := sketchSkipColumnSet[name]
	return !skip
}

// sketchSkipColumnSet is the deliberate allow-list of identity / high-entropy ID columns whose
// per-column sketch is unconditionally safe to skip (NOTE-402, issue #354). These are exactly
// the fixed-width random-ID columns: their values are equality/bloom-tested via the trace-ID
// path, never quantile/range/block-pruned through the per-column sketch. Adding a column here
// requires confirming no query path block-prunes or computes quantiles on it.
//
// Timestamps (span:start/span:end/__timestamp__) and durations (span:duration) are NOT here —
// they back range-boundary pruning and quantile/histogram estimation and keep their sketches.
var sketchSkipColumnSet = map[string]struct{}{
	"span:id":        {},
	"span:parent_id": {},
}

// intrinsicColumnSet is the canonical set of intrinsic column names. Values are empty
// struct{} for O(1) lookup with zero memory overhead.
var intrinsicColumnSet = map[string]struct{}{
	// Trace signal intrinsics (from SPECS §11.1).
	"trace:id":              {},
	"span:id":               {},
	"span:parent_id":        {},
	"span:name":             {},
	"span:kind":             {},
	"span:start":            {},
	"span:end":              {},
	"span:duration":         {},
	"span:status":           {},
	"span:status_message":   {},
	"resource.service.name": {},
}
