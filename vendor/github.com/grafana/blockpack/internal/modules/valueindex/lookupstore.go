package valueindex

// lookupstore.go — LookupStore: the object-storage surface GetTraceByID needs to
// consult the trace-by-ID index (issue #428 wiring, Stage 4).
//
// DiscoverIndexFiles only needs List (Lister); the v2 batched TraceGroup index
// (issue #476) resolves a lookup with targeted partial reads (footer + block
// directory + only the surviving block), so the fetch surface is now the same
// minimal Size + ReadAt contract the search/metrics path already uses
// (TraceRandomReader / ValueIndexFileStore), NOT a whole-object Get. Get is
// retained on TraceIndexGetter for the legacy flat-blob format read during the v2
// rollover window (a v1 file has no footer/TOC, so it must be fetched whole and
// DecodeTraceGroups'd). LookupStore bundles List + Get + Size + ReadAt into one
// interface-typed parameter so GetTraceByID's signature takes one store, not four.
//
// This is a breaking interface change for external consumers (tempo's S3 adapter):
// its minioVIStore already implements Size + ReadAt for the search/metrics path,
// so it satisfies the widened LookupStore without new methods (NOTE-VI-079).

import "context"

// TraceIndexGetter fetches the full bytes of a discovered trace-index file by key.
// Used only for the legacy v1 flat-blob format (no footer/TOC to seek within);
// the v2 batched format resolves via Size + ReadAt partial reads instead.
type TraceIndexGetter interface {
	Get(ctx context.Context, key string) ([]byte, error)
}

// LookupStore is the read-only object-storage surface GetTraceByID needs to
// discover and read trace-by-ID index files: list candidate keys (Lister), fetch
// a legacy whole file (TraceIndexGetter), and partially read a v2 file
// (TraceRandomReader: Size + ReadAt).
type LookupStore interface {
	Lister
	TraceIndexGetter
	TraceRandomReader
}
