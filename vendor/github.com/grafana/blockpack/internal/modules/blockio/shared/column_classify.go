package shared

// IsIntrinsicColumn reports whether name is an intrinsic column — one that is stored in the
// file-level intrinsic section AND in block column payloads (dual storage; see writer NOTE-002).
// Both the writer (to classify columns into intrinsic vs attribute tier at block-build time) and
// the reader (to select which TOC tier to consult for a given column name) use this function.
//
// The set covers trace signal intrinsics (trace:id, span:*, resource.service.name) and log
// signal intrinsics (log:*). resource.service.name is included because it is "practically
// intrinsic" — it is present in the intrinsic section for all signal types.
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
	"span:id":        {reason: "fixed-width IDs with shared high-order bits", enc: SemanticBytesXOR},
	"span:parent_id": {reason: "fixed-width IDs with shared high-order bits", enc: SemanticBytesXOR},
	"log:trace_id":   {reason: "sorted 16-byte trace IDs", enc: SemanticBytesDeltaDictionary},
	"log:span_id":    {reason: "fixed-width IDs with shared high-order bits", enc: SemanticBytesXOR},
}

// intrinsicColumnSet is the canonical set of intrinsic column names across both trace and log
// signal types. Values are empty struct{} for O(1) lookup with zero memory overhead.
var intrinsicColumnSet = map[string]struct{}{
	// Trace signal intrinsics (from SPECS §11.1)
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
	// Log signal intrinsics (from SPECS §11.4)
	"log:timestamp":          {},
	"log:observed_timestamp": {},
	"log:body":               {},
	"log:severity_number":    {},
	"log:severity_text":      {},
	"log:trace_id":           {},
	"log:span_id":            {},
	"log:flags":              {},
}
