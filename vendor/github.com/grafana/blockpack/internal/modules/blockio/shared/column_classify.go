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
