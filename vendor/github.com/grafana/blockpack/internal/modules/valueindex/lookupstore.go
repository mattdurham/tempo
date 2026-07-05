package valueindex

// lookupstore.go — LookupStore: the object-storage surface GetTraceByID needs to
// consult the trace-by-ID index (issue #428 wiring, Stage 4).
//
// DiscoverIndexFiles only needs List (Lister); actually fetching a discovered
// candidate file's bytes needs a Get too. LookupStore bundles both into a single
// interface-typed parameter so GetTraceByID's signature takes one new parameter,
// not two. valueindexcompactor.IndexStore and blockpack's storage adapters both
// already expose a compatible Get method.

import "context"

// TraceIndexGetter fetches the full bytes of a discovered trace-index file by key.
type TraceIndexGetter interface {
	Get(ctx context.Context, key string) ([]byte, error)
}

// LookupStore is the read-only object-storage surface GetTraceByID needs to
// discover and fetch trace-by-ID index files: list candidate keys (Lister) and
// fetch a candidate's bytes (TraceIndexGetter).
type LookupStore interface {
	Lister
	TraceIndexGetter
}
