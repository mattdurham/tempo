package valueindex

// query.go — multi-file value-index query execution (NOTE-VI-031, issue #430).
//
// QueryFiles executes a predicate against a set of value-index file bytes and
// returns the matching entries sorted by (TimeSec DESC, TraceID ASC). Callers
// supply file data pre-downloaded from S3; this function is pure in-memory.
//
// For file discovery: use ParseFilenameV2+IsInTimeRange to filter the S3 LIST
// result before downloading any files (see NOTE-VI-030, issue #431).

import "slices"

// LookupResult is one matching span from a value-index query.
// It carries enough information to fetch the specific span from a blockpack file
// without scanning unrelated spans: BlockRef gives the page address, RowIdx gives
// the row within the block. SpanID is included in the dedup key below, but per
// NOTE-VI-094 it is unconditionally zero on the live BucketGroup write/query path (every
// version, not just v1-v3), so it does not actually distinguish distinct spans within the same
// trace on that path -- (BlockRef.PageNum, BlockRef.LenPages) is what does the real dedup work
// there today.
type LookupResult struct {
	SourceRef string
	TimeSec   uint64
	BlockRef  BlockRef // zero for v1 files; use BlockID instead
	BlockID   uint32   // v1: block index within SourceRef
	RowIdx    uint16   // zero for v1-v3 files
	TraceID   [16]byte
	SpanID    [8]byte // NOTE-VI-094: zero for EVERY version on the BucketGroup write path (the
	// on-disk SpanRef type has no SpanID field at all by design), not merely "v1-v3 files".
}

// QueryFiles evaluates pred against each file in files and returns all matching
// entries within the optional time range. A nil timeRange means "all times".
// Results are sorted by (TimeSec DESC, TraceID ASC) for newest-first traversal.
//
// # Caller flow
//
//  1. List value-index files: s3Client.List("tenant/vi-index/<colHash>/<colType>/")
//  2. Filter by time range: ParseFilenameV2 + FileMeta.IsInTimeRange
//  3. Download matching files
//  4. Call QueryFiles(pred, timeRange, files...)
func QueryFiles(pred Predicate, timeRange *[2]uint64, files ...[]byte) ([]LookupResult, error) {
	var out []LookupResult
	for _, data := range files {
		r, err := OpenReader(data)
		if err != nil {
			continue // skip corrupt files
		}
		results, err := r.Lookup(pred, timeRange)
		if err != nil {
			continue
		}
		for _, res := range results {
			out = append(out, LookupResult{
				TraceID:   res.TraceID,
				SpanID:    res.SpanID,
				SourceRef: res.SourceRef,
				BlockRef:  res.BlockRef,
				BlockID:   res.BlockID,
				TimeSec:   res.TimeSec,
				RowIdx:    res.RowIdx,
			})
		}
	}
	// Sort: TimeSec DESC (newest first), then TraceID ASC for determinism.
	slices.SortFunc(out, func(a, b LookupResult) int {
		if a.TimeSec != b.TimeSec {
			if a.TimeSec > b.TimeSec {
				return -1 // DESC
			}
			return 1
		}
		for i := range a.TraceID {
			if a.TraceID[i] != b.TraceID[i] {
				if a.TraceID[i] < b.TraceID[i] {
					return -1
				}
				return 1
			}
		}
		return 0
	})
	// Deduplicate by (TraceID, SpanID, BlockRef).
	out = deduplicateLookupResults(out)
	return out, nil
}

// deduplicateLookupResults removes duplicate (TraceID, SpanID, BlockRef) entries.
// The input must be sorted by TimeSec DESC so the first occurrence is the newest.
func deduplicateLookupResults(results []LookupResult) []LookupResult {
	if len(results) == 0 {
		return results
	}
	seen := make(map[[32]byte]struct{}, len(results))
	out := make([]LookupResult, 0, len(results))
	for _, r := range results {
		var key [32]byte
		copy(key[0:16], r.TraceID[:])
		copy(key[16:24], r.SpanID[:])
		// Include block identity in dedup key so same span in different compaction
		// levels isn't collapsed (they have the same TraceID+SpanID but different BlockRefs).
		// Intentional little-endian byte masks (the encoding), not lossy overflow.
		key[24] = byte(r.BlockRef.PageNum)       //nolint:gosec // LE byte 0
		key[25] = byte(r.BlockRef.PageNum >> 8)  //nolint:gosec // LE byte 1
		key[26] = byte(r.BlockRef.PageNum >> 16) //nolint:gosec // LE byte 2 (uint24)
		key[27] = byte(r.BlockRef.LenPages)      //nolint:gosec // LE byte 0
		key[28] = byte(r.BlockRef.LenPages >> 8) //nolint:gosec // LE byte 1
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		out = append(out, r)
	}
	return out
}

// GroupByTrace groups a sorted slice of LookupResults by TraceID.
// Returns a map from TraceID → []LookupResult for that trace.
// Useful for structural queries: after getting matching spans, group by trace
// to then evaluate parent/child relationships.
func GroupByTrace(results []LookupResult) map[[16]byte][]LookupResult {
	out := make(map[[16]byte][]LookupResult)
	for _, r := range results {
		out[r.TraceID] = append(out[r.TraceID], r)
	}
	return out
}
