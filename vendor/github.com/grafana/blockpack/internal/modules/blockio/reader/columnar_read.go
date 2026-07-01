package reader

// NOTE: Any changes to this file must be reflected in the corresponding SPECS.md or NOTES.md.

// SPEC-005: Sub-block column I/O — only the bytes for wantColumns are transferred
// from storage. Two phases per block:
//   Phase 1 — TOC read: read the first tocHintBytes to get the column metadata,
//             which contains each column's (dataOffset, dataLen) within the block.
//   Phase 2 — Column reads: issue one targeted ReadAt per wanted column.
// The assembled sparse buffer is byte-for-byte compatible with parseBlockColumnsReuse
// because wanted column bytes sit at their original offsets; non-wanted regions are
// zero (never accessed by parseBlockColumnsReuse when wantColumns is set).

import (
	"cmp"
	"fmt"
	"log/slog"
	"runtime"
	"runtime/debug"
	"slices"
	"strconv"
	"sync"

	"github.com/grafana/blockpack/internal/modules/blockio/shared"
	"github.com/grafana/blockpack/internal/modules/rw"
)

// NOTE-208: the assembled sparse buffer (make([]byte, bufSize) in
// readBlockColumnarWithCache) is the largest single allocation on the warm metrics
// block-scan path — one per block per query, sized to span the ToC plus the furthest
// wanted column (often most of the block). A querier CPU profile showed heavy GC/runtime
// traffic (growslice/roundupsize/wbBufFlush/findObject) consistent with churning these
// large buffers, and prior alloc profiles attributed the columnar assembled-buffer line
// as a top allocator. This pool recycles those backing arrays across blocks.
//
// SAFETY (why a dirty reused buffer is correct): parseBlockColumnsReuse only reads bytes
// inside (a) the ToC prefix [0,tocEnd) and (b) each WANTED column's exact
// [dataOffset, dataOffset+compressedLen) extent — both of which readBlockColumnarWithCache
// fully overwrites via copy before returning. It never reads the gaps between columns, so
// stale bytes left there from a prior block are never observed. (The original make([]byte)
// zero-filled the gaps; that zeroing was pure overhead — the gaps are dead.)
//
// LIFETIME: the buffer is returned to the caller and ParseBlockFromBytes sub-slices it
// zero-copy into Column.compressedEncoding (lazy decode). It is therefore live until the
// block is fully consumed — the same lifetime point as NOTE-153's lazyColumnStore. Callers
// release it via Reader.ReleaseRawBuffer once the block (both passes, any lazy decode) is
// done. Callers that do not release simply let the buffer be GC'd: no correctness
// dependency on releasing (identical contract to NOTE-153).
var assembledBufPool = sync.Pool{New: func() any { b := make([]byte, 0); return &b }} //nolint:gochecknoglobals

// assembledBufMaxPooledCap caps the backing capacity retained by the pool. Buffers larger
// than this (rare oversized blocks) are not returned to the pool, so a single huge block
// cannot pin a large array in the pool indefinitely.
const assembledBufMaxPooledCap = 16 << 20 // 16 MiB

// acquireAssembledBuffer returns a []byte of exactly length n, drawn from the pool when a
// backing array of sufficient capacity is available, otherwise freshly allocated. The
// returned slice's contents are NOT zeroed — callers must overwrite every byte they read
// (the ToC prefix and each wanted column extent); see NOTE-208 safety note.
func acquireAssembledBuffer(n int64) []byte {
	bp := assembledBufPool.Get().(*[]byte)
	b := *bp
	if int64(cap(b)) >= n {
		*bp = b[:0] // detach the slice header from the pool handle before reuse
		return b[:n]
	}
	// Pooled array too small: allocate a fresh one. The undersized handle is dropped
	// (its backing array will be GC'd) rather than returned, avoiding pool churn.
	return make([]byte, n)
}

// ReleaseRawBuffer returns an assembled block buffer to the pool for reuse. It is safe to
// call with a buffer obtained from ReadGroupColumnar/ReadGroupColumnarCached once the block
// parsed from it is fully consumed (after every row is scanned and any lazy column decode
// has completed). Calling with nil, an empty slice, or a buffer that did not come from the
// pool is harmless — the latter simply seeds the pool with a usable backing array. Buffers
// whose capacity exceeds assembledBufMaxPooledCap are dropped to bound pool memory.
//
// NOTE-208: mirrors NOTE-153's ReleaseLazyColumnStore lifetime contract. Releasing is an
// optimization only; never releasing leaks nothing (the buffer is GC'd as before).
func (r *Reader) ReleaseRawBuffer(buf []byte) {
	if cap(buf) == 0 || cap(buf) > assembledBufMaxPooledCap {
		return
	}
	b := buf[:0]
	assembledBufPool.Put(&b)
}

// tocHintBytes is the initial read size for Phase 1.
// Covers the 24-byte block header plus column metadata for up to ~100 columns:
//
//	V12 entry: 2 (nameLen) + ~15 (name) + 1 (type) + 8 (dataOffset) + 8 (dataLen) = ~34 bytes
//	100 columns × 34 bytes = ~3400 bytes → fits comfortably in 4096.
const tocHintBytes = 4096

// tocGrowthFactor is the geometric growth applied to the Phase-1 ToC read when the
// column-metadata array overflows the current read (NOTE-154). Real trace blocks
// routinely carry hundreds of attribute columns whose metadata exceeds tocHintBytes.
const tocGrowthFactor = 4

// colCoalesceMaxGap is the largest run of unwanted bytes that may be folded into a single
// coalesced Phase-2 cold read (NOTE-173). Adjacent wanted columns separated by at most this
// many bytes are read in one provider call rather than two. The dominant cold-path cost is
// per-read connection establishment, not transferred bytes, so tolerating a modest gap to
// save a round-trip is a net win; the cap stops two distant columns from pulling a huge
// span of unrelated data through the backend.
const colCoalesceMaxGap = 64 * 1024

// ReadGroupColumnar fetches only the bytes for wantColumns within each block in cr.
//
// When the reader has a fileID and section cache, all reads route through the cache
// (ReadGroupColumnarCached): zero S3 on hit, one ranged GET per column on miss.
// Without a cache key (fileID == "") or when wantColumns is nil, falls back to
// ReadCoalescedBlocks (single coalesced S3 read, full block bytes).
func (r *Reader) ReadGroupColumnar(cr shared.CoalescedRead, wantColumns map[string]struct{}) (map[int][]byte, error) {
	if wantColumns == nil {
		return ReadCoalescedBlocks(r.provider, []shared.CoalescedRead{cr})
	}
	// Use cache-aware path when available — avoids downloading unwanted column bytes.
	if r.fileID != "" {
		return r.ReadGroupColumnarCached(cr, wantColumns)
	}
	// No cache key: full download (original ReadGroup behavior).
	return ReadCoalescedBlocks(r.provider, []shared.CoalescedRead{cr})
}

// FilterBlockColumns creates a sparse buffer retaining only the block header, column
// metadata, and the compressed bytes for columns in wantColumns. All other column data
// is zeroed/omitted. The result is byte-for-byte compatible with parseBlockColumnsReuse
// because wanted column bytes sit at their original offsets in the buffer.
//
// Used by blockGroupPipeline to shed unused column bytes after ReadGroup downloads the
// full coalesced group — reduces querier peak memory from ~10GB to ~500MB for typical
// 3-hour histogram queries without issuing additional S3 requests.
//
// Returns raw unchanged on any parse error (safe fallback).
func FilterBlockColumns(raw []byte, wantColumns map[string]struct{}) ([]byte, error) {
	if len(wantColumns) == 0 {
		return raw, nil
	}
	hdr, err := parseBlockHeader(raw)
	if err != nil {
		return raw, nil //nolint:nilerr // intentional: fallback to full bytes on parse error
	}
	metas, tocEnd, err := parseColumnMetadataArray(
		raw,
		int(shared.BlockHeaderV14Size),
		int(hdr.columnCount),
		hdr.version,
	)
	if err != nil {
		return raw, nil //nolint:nilerr // intentional: fallback to full bytes on parse error
	}

	// Compute sparse buffer size: header + metadata + wanted column extents.
	bufSize := int64(tocEnd) //nolint:gosec
	for _, m := range metas {
		if _, ok := wantColumns[m.name]; !ok || m.compressedLen == 0 {
			continue
		}
		colEnd := int64(m.dataOffset) + int64(m.compressedLen) //nolint:gosec
		if colEnd > int64(len(raw)) {
			continue // out of bounds guard
		}
		if colEnd > bufSize {
			bufSize = colEnd
		}
	}

	if bufSize >= int64(len(raw)) {
		return raw, nil // no savings — return original
	}

	assembled := make([]byte, bufSize)
	copy(assembled, raw[:tocEnd])
	for _, m := range metas {
		if _, ok := wantColumns[m.name]; !ok || m.compressedLen == 0 {
			continue
		}
		colStart := int64(m.dataOffset)  //nolint:gosec
		colLen := int64(m.compressedLen) //nolint:gosec
		if colStart+colLen > int64(len(raw)) {
			continue
		}
		copy(assembled[colStart:colStart+colLen], raw[colStart:colStart+colLen])
	}
	return assembled, nil
}

// sectionTypeBlockToc and sectionTypeBlockCol are section cache key namespaces for
// raw internal block bytes. They don't overlap with V8 ToC entry types.
const (
	sectionTypeBlockToc uint32 = 0xBB00
	sectionTypeBlockCol uint32 = 0xBB01
)

// ReadGroupColumnarCached fetches only the bytes for wantColumns within each block,
// routing reads through the section cache.
//
// On cache hit (warm): zero S3 I/O — ToC and column bytes served from disk/memcached.
// On cache miss (cold): one ranged GET for ToC (~4KB) and one per needed column section.
//
// This replaces ReadGroup for query paths. Unlike downloading the full group and
// discarding unused columns (which allocates full bytes then filters), this never
// allocates bytes for unwanted columns.
//
// Falls back to ReadGroup when fileID is empty (no stable cache key) or wantColumns is nil.
func (r *Reader) ReadGroupColumnarCached(
	cr shared.CoalescedRead,
	wantColumns map[string]struct{},
) (map[int][]byte, error) {
	if wantColumns == nil || r.fileID == "" {
		return r.ReadGroup(cr) // WantAll or no cache key: full download
	}

	// NOTE-170: single-block fast path. query-frontend shards one block per querier
	// call, so cr.BlockIDs almost always has length 1. The general path below spawns a
	// goroutine, a semaphore channel, a WaitGroup, and a results slice per call — pure
	// per-read scheduler/alloc overhead (goroutine spawn + channel send/recv + wg.Wait
	// futex) on the universal block-read path for every query. Reading the single block
	// inline on the calling goroutine removes that overhead entirely and keeps the same
	// panic-safety contract: readBlockColumnarWithCache returns an error rather than
	// crashing the querier, and a genuine panic propagates up exactly as a single-block
	// run would (the goroutine recover() only existed to keep other parallel blocks
	// from being lost — irrelevant with one block).
	if len(cr.BlockIDs) == 1 {
		blockIdx := cr.BlockIDs[0]
		data, err := r.readBlockColumnarWithCache(cr.BlockOffsets[0], cr.BlockLengths[0], blockIdx, wantColumns, nil)
		if err != nil {
			return nil, err
		}
		return map[int][]byte{blockIdx: data}, nil
	}

	type blockResult struct {
		err      error
		data     []byte
		blockIdx int
	}

	results := make([]blockResult, len(cr.BlockIDs))
	var wg sync.WaitGroup
	sem := make(chan struct{}, runtime.NumCPU())

	for j, blockIdx := range cr.BlockIDs {
		sem <- struct{}{}
		wg.Add(1)
		go func(j, blockIdx int) {
			defer wg.Done()
			defer func() { <-sem }()
			defer func() {
				if rec := recover(); rec != nil {
					slog.Error("ReadGroupColumnarCached: panic", "block_idx", blockIdx,
						"panic", rec, "stack", string(debug.Stack()))
					results[j] = blockResult{
						blockIdx: blockIdx,
						err:      fmt.Errorf("block %d panic: %v", blockIdx, rec),
					}
				}
			}()
			// Reuse readBlockColumnar's logic but route through the section cache.
			data, err := r.readBlockColumnarWithCache(
				cr.BlockOffsets[j],
				cr.BlockLengths[j],
				blockIdx,
				wantColumns,
				nil,
			)
			results[j] = blockResult{blockIdx: blockIdx, data: data, err: err}
		}(j, blockIdx)
	}
	wg.Wait()

	out := make(map[int][]byte, len(cr.BlockIDs))
	for _, res := range results {
		if res.err != nil {
			return nil, res.err
		}
		out[res.blockIdx] = res.data
	}
	return out, nil
}

// readBlockColumnarWithCache is readBlockColumnar extended with section cache routing.
// Phase 1 (ToC) and Phase 2 (column reads) both go through r.cache so repeated queries
// pay zero S3 cost. Falls back to the full block read on ToC parse errors.
// NOTE-449: cs accumulates cache hit/miss counts when non-nil; nil is safe (no counting).
func (r *Reader) readBlockColumnarWithCache(
	blockOff, blockLen int64,
	blockIdx int,
	wantColumns map[string]struct{},
	cs *CacheStats,
) ([]byte, error) {
	// Encode blockIdx in the name field so subType=0 always, keeping per-block column
	// section keys in a distinct subType namespace from the real ToC subtypes
	// (block_index=7, value-index entries/meta/hashindex). NOTE-422: the retired
	// bloom/trace subtypes no longer participate in this namespace.
	// NOTE-189: strconv.Itoa instead of fmt.Sprintf — this runs once per block per
	// query on the warm read path.
	blockIdxStr := strconv.Itoa(blockIdx)
	tocKey := blockIdxStr

	// NOTE-185: warm-path single round-trip. The ToC and every wanted column live in
	// the same memcache sub-cache, and a wanted column's section key (blockIdx/name)
	// is derivable from the column NAME alone — no ToC decode needed first. So instead
	// of Phase-1 ToC GetOrFetch (round-trip #1) followed by Phase-2 column GetMulti
	// (round-trip #2) we request the ToC key AND all wanted column keys in ONE
	// pipelined GetMulti. The ToC blob from the result supplies the offsets that place
	// each column blob in the assembled buffer; column hits skip Phase-2 entirely.
	// Misses (cold blocks, evicted entries) fall through to the existing fetch paths,
	// so the result is byte-for-byte identical to the two-phase version.
	var (
		toc     []byte
		preHits map[string][]byte // column name -> compressed blob, from the combined batch
		err     error
	)
	if mf, ok := r.cache.(sectionMixedFetcher); ok && len(wantColumns) > 0 {
		// NOTE-214: prune already-decoded columns from the combined fetch. The cached
		// per-block name->colType mapping (populated at first ToC parse) lets us probe
		// parsedV8ColumnCache before building the GetMulti and stash the LIVE snapshot
		// of any column already decoded, so its compressed blob is never requested from
		// memcache. The sizing pass below (NOTE-213) finds it already stashed and excludes
		// it from the buffer/copy. On the first query against a block (colTypes-cache miss)
		// fetchCols == wantColumns and everything is fetched as before.
		fetchCols := r.prunePreDecodedFromFetch(blockOff, wantColumns)
		toc, preHits = r.fetchTocAndColumnsCombined(mf, tocKey, blockIdxStr, fetchCols)
	}

	// Phase 1: ToC — cached. NOTE-154: read a ToC large enough to hold the full
	// column-metadata array. Trace blocks routinely have hundreds of columns whose
	// metadata overflows a fixed 4 KiB hint; the previous code then fell back to a
	// full block read (the #1 querier allocator, ~18% of alloc_space), defeating the
	// columnar cache. The correctly-sized ToC is what gets cached, so warm queries
	// pay one cache hit and one successful parse — no growth, no full-block read.
	if toc == nil {
		var tocFetched bool
		toc, err = r.cache.GetOrFetchV8Section(r.fileID, sectionTypeBlockToc, 0, tocKey, func() ([]byte, error) {
			tocFetched = true
			return r.readSufficientToC(blockOff, blockLen)
		})
		if err != nil {
			return nil, fmt.Errorf("block %d toc: %w", blockIdx, err)
		}
		if cs != nil { // NOTE-449: track ToC hit/miss
			if tocFetched {
				cs.Misses[CacheStatsSectionToc]++
			} else {
				cs.Hits[CacheStatsSectionToc]++
			}
		}
	} else if cs != nil {
		// toc came from the combined ToC+columns batch — section cache hit.
		cs.Hits[CacheStatsSectionToc]++
	}

	// NOTE-241: reuse the per-block parsed ToC if it was cached by an earlier read. The
	// metas + tocEnd are deterministic for a block, so re-running parseBlockHeader +
	// parseColumnMetadataArray on every warm read — allocating one string(name) per column
	// plus the entries slice — is pure per-query waste. On a hit we skip both parses and use
	// the shared read-only metas. On a miss we parse, cache, and proceed as before. The
	// fetched ToC bytes are still needed (for the assembled-buffer prefix copy + parser), so
	// only the parse is elided, not the fetch.
	var (
		metas  []colMetaEntry
		tocEnd int
	)
	if cached := r.getCachedBlockToc(blockOff); cached != nil {
		metas, tocEnd = cached.metas, cached.tocEnd
	} else {
		hdr, hdrErr := parseBlockHeader(toc)
		if hdrErr != nil {
			// Fallback: full block read (same as readBlockColumnar's fallback).
			return r.readFullBlockFallback(blockOff, blockLen, blockIdx)
		}
		metas, tocEnd, err = parseColumnMetadataArray(
			toc,
			int(shared.BlockHeaderV14Size),
			int(hdr.columnCount),
			hdr.version,
		)
		if err != nil {
			return r.readFullBlockFallback(blockOff, blockLen, blockIdx)
		}
		// NOTE-214/241: record this block's parsed ToC so a subsequent warm query can both
		// prune already-decoded columns from the combined fetch and skip this parse.
		r.cacheBlockColTypes(blockOff, metas, tocEnd)
	}

	// NOTE-213: a SINGLE pass over metas both (a) detects already-decoded columns
	// (stashing their live snapshot and excluding them entirely) and (b) sizes the
	// assembled buffer to span ONLY the columns that still need their compressed bytes
	// copied in. Previously bufSize was computed over ALL wanted columns before the
	// NOTE-212 skip loop ran, so a fully-warm wide query (every wanted column already in
	// parsedV8ColumnCache) still acquired a buffer sized to the furthest wanted column —
	// often most of the block — only to write the ToC prefix into it and never touch the
	// column extents. Folding the skip detection into the sizing pass lets that buffer
	// shrink to just the ToC prefix on the fully-warm path, the dominant remaining
	// assembled-buffer allocation/copy cost the prior notes targeted.
	//
	// keepCols collects the wanted columns NOT served from the decoded cache. bufSize
	// grows only for those, so unwanted and pre-decoded columns never inflate it.
	bufSize := int64(tocEnd) //nolint:gosec
	keepCols := make([]colMetaEntry, 0, len(metas))
	for _, m := range metas {
		if _, ok := wantColumns[m.name]; !ok || m.compressedLen == 0 {
			continue
		}
		colEnd := int64(m.dataOffset) + int64(m.compressedLen) //nolint:gosec
		if colEnd > blockLen {
			// A wanted column's data extends beyond blockLen — the reported block length
			// may be approximate. Fall back to reading the full block so ParseBlockFromBytes
			// can access all column data safely.
			return r.readFullBlockFallback(blockOff, blockLen, blockIdx)
		}
		// NOTE-212: if this column's DECODED snapshot is already in the process-level
		// parsedV8ColumnCache (NOTE-200), the parser will serve it from there and never
		// read rawBytes[m.dataOffset:colEnd]. Copying the compressed blob into the
		// assembled buffer would be pure dead work — and on a wide warm metrics query that
		// per-column memmove (plus the assembled-buffer churn) is the dominant warm-path
		// CPU/GC cost. Stash the LIVE snapshot pointer on the Reader and skip the copy; the
		// parser consumes it from r.preDecodedColumns (no re-probe, no eviction race).
		// Pre-decoded columns are also excluded from bufSize (NOTE-213): their extent is
		// never written nor read, so the buffer need not span them.
		if r.stashPreDecodedColumn(blockOff, m) {
			continue
		}
		// NOTE-234: the column's DECODED snapshot is NOT cached (the NOTE-212 stash above
		// failed), but its COMPRESSED blob came back from the combined ToC+columns GetMulti.
		// Stash that blob on the Reader and let the parser decode straight from it, skipping
		// both the copy into the assembled buffer and that column's contribution to bufSize.
		// Previously the blob was copied into assembled[colStart:colEnd] only for the parser
		// to sub-slice it right back out and snappy-decode — a pure warm-path memmove plus
		// the buffer region it forced the assembled buffer to span. (length must match
		// compressedLen exactly, the same guard the copy path used.)
		if blob, hit := preHits[m.name]; hit && int64(len(blob)) == int64(m.compressedLen) { //nolint:gosec
			r.stashPreCompressedColumn(blockOff, m, blob)
			continue
		}
		keepCols = append(keepCols, m)
		// NOTE-367: keepCols are resolved by fetchColumnInto/fetchColumnsBatched, which
		// now STASH each fetched blob on the Reader (stashPreCompressedColumn) rather than
		// copying it into the assembled buffer at its absolute dataOffset. The parser
		// decodes those columns straight from the stash (preCompressedLookup), exactly as
		// the NOTE-234 cache-hit columns do, so it never reads assembled[colStart:colEnd].
		// The assembled buffer therefore only needs to span the ToC prefix — it no longer
		// grows to the furthest kept column, eliminating both the dead gaps BETWEEN
		// scattered columns and the per-column copy. This is the dominant remaining
		// inuse_space frame (acquireAssembledBuffer, ~1 GiB under load): a heavy metrics
		// query touches a few small columns scattered across a large block, so bufSize
		// previously spanned most of the block while only disjoint extents were ever read.
	}

	// NOTE-449: record column-level cache hits and misses after the sizing pass.
	// keepCols are cold misses; the remaining wanted columns are warm hits.
	if cs != nil && len(wantColumns) > 0 {
		cs.Misses[CacheStatsSectionCol] += int32(len(keepCols)) //nolint:gosec
		if wantHits := len(wantColumns) - len(keepCols); wantHits > 0 {
			cs.Hits[CacheStatsSectionCol] += int32(wantHits) //nolint:gosec
		}
	}

	// NOTE-208/367: draw the assembled buffer from a pool. It now holds ONLY the ToC
	// prefix (bufSize == tocEnd); every wanted column is served from a process-cache
	// decoded snapshot, a stashed compressed blob from the combined GetMulti (NOTE-234),
	// or a stashed blob from the keepCols fetch below (NOTE-367) — none of which read the
	// assembled buffer's column region. The caller recycles it via ReleaseRawBuffer once
	// the parsed block is fully consumed.
	assembled := acquireAssembledBuffer(bufSize)
	copy(assembled, toc[:min(int64(len(toc)), int64(tocEnd))]) //nolint:gosec

	// Phase-2 column fetches. Each kept column's compressed blob is cached as an
	// independent section keyed by blockIdx/colName. NOTE-367: the resolved blob is STASHED
	// on the Reader (stashPreCompressedColumn) so the parser decodes straight from it —
	// nothing is written into the assembled buffer, which now holds only the ToC prefix.
	//
	// NOTE-185: columns returned by the combined ToC+columns batch above are satisfied
	// without any further memcache round-trip. NOTE-234: such a column is already stashed in
	// r.preCompressedColumns during the sizing pass and excluded from keepCols entirely.
	// keepCols therefore contains only true misses, which fall into `cols` for individual
	// resolution (and stashing) below.
	cols := keepCols

	// NOTE-187: lazily plan cold runs only when there is at least one cold miss to
	// resolve. On the warm steady-state path (the production state we optimize for) the
	// combined ToC+columns GetMulti above satisfies every wanted column, so `cols` is
	// empty and the coalesced cold-run plan is never consulted. Computing it eagerly on
	// every warm block read paid an O(wanted-columns) scan plus a slices.SortFunc and a
	// `ranges` slice allocation for a result that was thrown away — wasted CPU and GC
	// pressure on the universal hot path. planColdRunsLazy returns a nil plan when there
	// are no cold misses, so warm reads do no planning work and the single/zero-column
	// branch below never calls into `runs`. On the cold path it is the exact same plan as
	// before (over the full `metas`/`wantColumns`), preserving NOTE-173's coalesce.
	runs := r.planColdRunsLazy(cols, metas, wantColumns, blockLen, int64(len(toc)))

	if len(cols) <= 1 {
		// Single (or zero) wanted column: the fan-out machinery is pure overhead. Resolve inline.
		for _, m := range cols {
			if err := r.fetchColumnInto(toc, runs, nil, blockOff, blockIdx, m); err != nil {
				return nil, err
			}
		}
		return assembled, nil
	}

	// NOTE-179: batch the multi-column section-cache fetch into ONE pipelined request.
	// Previously (NOTE-177) every wanted column resolved through its own GetOrFetchV8Section,
	// fanned out concurrently — but each Get is an independent memcache round-trip, and a
	// querier CPU profile attributed ~28% of CPU to memcache (*Client).dial because the
	// concurrent per-column fan-out exhausted the idle pool and forced a fresh dial (plus
	// TLS/TCP setup) per column. gomemcache.GetMulti groups all keys per server and pipelines
	// them over a SINGLE connection, collapsing N dials into ~1 while keeping the single-RTT
	// latency the fan-out provided. When the section cache supports batch fetch we issue one
	// GetMulti for all wanted columns; any columns that miss are resolved (and written back)
	// individually below — rare on the warm steady-state path. The assembled buffer is
	// byte-for-byte identical to the serial/fan-out version (each column lands at its absolute
	// offset). When the cache lacks batch support we fall back to the concurrent fan-out.
	if bg, ok := r.cache.(sectionBatchFetcher); ok {
		if err := r.fetchColumnsBatched(bg, toc, runs, blockOff, blockIdx, cols); err != nil {
			return nil, err
		}
		return assembled, nil
	}

	var (
		runMu    sync.Mutex
		wg       sync.WaitGroup
		firstErr error
		errMu    sync.Mutex
	)
	workers := min(len(cols), runtime.NumCPU())
	sem := make(chan struct{}, workers)
	for _, m := range cols {
		sem <- struct{}{}
		wg.Add(1)
		go func(m colMetaEntry) {
			defer wg.Done()
			defer func() { <-sem }()
			if err := r.fetchColumnInto(toc, runs, &runMu, blockOff, blockIdx, m); err != nil {
				errMu.Lock()
				if firstErr == nil {
					firstErr = err
				}
				errMu.Unlock()
			}
		}(m)
	}
	wg.Wait()
	if firstErr != nil {
		return nil, firstErr
	}

	return assembled, nil
}

// readFullBlockFallback reads the entire block from the provider. It is the shared
// fallback for readBlockColumnarWithCache when the cached ToC cannot be parsed or a wanted
// column's reported extent overruns the block length — in those cases ParseBlockFromBytes
// must be able to access every column, so the columnar sub-read optimization is abandoned
// for this block and the full block bytes are returned.
func (r *Reader) readFullBlockFallback(blockOff, blockLen int64, blockIdx int) ([]byte, error) {
	full := make([]byte, blockLen)
	if _, ferr := r.provider.ReadAt(full, blockOff, rw.DataTypeBlock); ferr != nil {
		return nil, fmt.Errorf("block %d fallback: %w", blockIdx, ferr)
	}
	return full, nil
}

// stashPreDecodedColumn probes the process-level parsedV8ColumnCache (NOTE-200) for column
// m of the block at blockOff. On a hit it stashes the LIVE decoded snapshot in
// r.preDecodedColumns and returns true, signaling readBlockColumnarWithCache to SKIP
// copying the column's compressed blob into the assembled buffer (the parser will serve it
// from the stashed snapshot and never read those bytes — NOTE-212). Returns false when there
// is no fileID or the snapshot is absent, so the caller falls through to the copy path. The
// column's TRUE (name, type) is used because the cache and parser key on type, and one block
// can carry the same name with different types. preDecodedMu guards the map because
// ReadGroupColumnar runs concurrently across blockGroupPipeline workers on the same *Reader.
func (r *Reader) stashPreDecodedColumn(blockOff int64, m colMetaEntry) bool {
	if r.fileID == "" {
		return false
	}
	snap := parsedV8ColumnCache.Get(v8ColumnCacheKey(r.fileID, uint64(blockOff), m.name, m.colType)) //nolint:gosec
	if snap == nil {
		return false
	}
	r.preDecodedMu.Lock()
	if r.preDecodedColumns == nil {
		r.preDecodedColumns = make(map[preDecodedKey]*Column)
	}
	r.preDecodedColumns[preDecodedKey{blockOffset: uint64(blockOff), name: m.name, colType: m.colType}] = snap //nolint:gosec
	r.preDecodedMu.Unlock()
	return true
}

// stashPreCompressedColumn records column m's compressed blob (from the combined
// ToC+columns GetMulti) on the Reader so the parser can decode straight from it instead of
// the assembled buffer (NOTE-234). blob aliases the memcache GetMulti result, which the
// section cache owns for the Reader's lifetime; the parser copies all data out during decode,
// so no longer-lived alias is created. The column's TRUE (name, type) keys the stash so the
// parser's preCompressedLookup matches (the cache and parser key on type, and one block can
// carry the same name with different types). preDecodedMu is shared with preDecodedColumns
// because ReadGroupColumnar runs concurrently across blockGroupPipeline workers on one Reader.
func (r *Reader) stashPreCompressedColumn(blockOff int64, m colMetaEntry, blob []byte) {
	r.preDecodedMu.Lock()
	if r.preCompressedColumns == nil {
		r.preCompressedColumns = make(map[preDecodedKey][]byte)
	}
	r.preCompressedColumns[preDecodedKey{blockOffset: uint64(blockOff), name: m.name, colType: m.colType}] = blob //nolint:gosec
	r.preDecodedMu.Unlock()
}

// prunePreDecodedFromFetch consults the cached per-block name->colType mapping (NOTE-214)
// to drop already-decoded columns from the combined ToC+columns GetMulti. For each wanted
// column whose type is known from the cached mapping AND whose decoded snapshot is present
// in parsedV8ColumnCache, it stashes the LIVE snapshot (same mechanism as the NOTE-212
// sizing-pass skip) and removes the column from the fetch set, so its compressed blob is
// never requested from memcache. Returns the pruned wantColumns set to pass to the combined
// fetch. The returned set aliases wantColumns when nothing was pruned (no allocation on the
// cold/cache-miss path); otherwise it is a fresh map. On a colTypes-cache miss it returns
// wantColumns unchanged, so the first query against a block fetches everything (and the
// mapping is populated after the ToC parse for subsequent queries).
//
// Correctness mirrors stashPreDecodedColumn: the LIVE snapshot pointer is held on the
// Reader so the parser consumes it without a re-probe (no eviction race), and the column's
// TRUE (name, type) keys the stash so the parser's preDecodedLookup matches. A column that
// is pruned here is byte-for-byte equivalent to one skipped by the sizing pass — the parser
// serves it from r.preDecodedColumns and never touches the (now-unfetched) compressed bytes.
func (r *Reader) prunePreDecodedFromFetch(blockOff int64, wantColumns map[string]struct{}) map[string]struct{} {
	if r.fileID == "" {
		return wantColumns
	}
	ct := blockColTypesCache.Get(blockColTypesCacheKey(r.fileID, uint64(blockOff))) //nolint:gosec
	if ct == nil {
		return wantColumns
	}
	var pruned map[string]struct{}
	var typeBuf [4]shared.ColumnType
	for name := range wantColumns {
		types := ct.typesFor(name, &typeBuf)
		if len(types) == 0 {
			continue
		}
		for _, t := range types {
			if r.stashPreDecodedColumn(blockOff, colMetaEntry{name: name, colType: t}) {
				if pruned == nil {
					// Lazily clone wantColumns only when at least one column is pruned.
					pruned = make(map[string]struct{}, len(wantColumns))
					for n := range wantColumns {
						pruned[n] = struct{}{}
					}
				}
				delete(pruned, name)
				break
			}
		}
	}
	if pruned == nil {
		return wantColumns
	}
	return pruned
}

// getCachedBlockToc returns the block's previously-parsed ToC (metas + tocEnd) if present.
// The returned metas slice is READ-ONLY and shared across queries — callers must not mutate
// it. Returns nil on a miss or when there is no fileID. NOTE-241.
func (r *Reader) getCachedBlockToc(blockOff int64) *blockColTypes {
	if r.fileID == "" {
		return nil
	}
	return blockColTypesCache.Get(blockColTypesCacheKey(r.fileID, uint64(blockOff))) //nolint:gosec
}

// cacheBlockColTypes records the block's fully-parsed ToC (metas + tocEnd) in
// blockColTypesCache so a subsequent warm query can both prune already-decoded columns from
// the combined fetch BEFORE re-parsing (NOTE-214) and skip parseColumnMetadataArray entirely
// (NOTE-241). Idempotent: a no-op if the block is already cached. No-op when there is no
// fileID. Called once per block read after parseColumnMetadataArray.
//
// The cached metas are shared read-only across queries, so any inline column's inlineData —
// which sub-slices the transient ToC buffer — is deep-copied into a private backing array to
// avoid aliasing memcache-owned bytes that may be recycled after this read.
func (r *Reader) cacheBlockColTypes(blockOff int64, metas []colMetaEntry, tocEnd int) {
	cacheParsedBlockColTypes(r.fileID, uint64(blockOff), metas, tocEnd) //nolint:gosec
}

// cacheParsedBlockColTypes stores the block's fully-parsed ToC (metas + tocEnd) in
// blockColTypesCache, keyed by fileID+blockOffset. Idempotent: a no-op if already cached or
// when fileID is empty / metas is empty. Shared by the columnar-read path (NOTE-241) and the
// parser (NOTE-242) so both populate the same cache on a first parse and either may serve a
// later warm read from it. The cached metas are shared READ-ONLY across queries, so any inline
// column's inlineData — which sub-slices the transient ToC buffer — is deep-copied into a private
// backing array to avoid aliasing memcache-owned bytes that may be recycled after this read.
func cacheParsedBlockColTypes(fileID string, blockOff uint64, metas []colMetaEntry, tocEnd int) {
	if fileID == "" || len(metas) == 0 {
		return
	}
	key := blockColTypesCacheKey(fileID, blockOff)
	if blockColTypesCache.Get(key) != nil {
		return // already cached for this block
	}
	cached := make([]colMetaEntry, len(metas))
	copy(cached, metas)
	for i := range cached {
		if cached[i].inlineData != nil {
			// Deep-copy inline bytes: the source aliases the transient ToC buffer.
			b := make([]byte, len(cached[i].inlineData))
			copy(b, cached[i].inlineData)
			cached[i].inlineData = b
		}
	}
	_ = blockColTypesCache.Put(key, &blockColTypes{metas: cached, tocEnd: tocEnd})
}

// sectionBatchFetcher is the optional interface a section cache may implement to
// fetch many V8 per-column blobs in one batched (pipelined) request and to write
// back individual blobs that missed the batch (NOTE-179). tieredcache.TypedTieredCache
// implements it; nil/non-batch caches make readBlockColumnarWithCache fall back to the
// concurrent per-column fan-out.
type sectionBatchFetcher interface {
	GetMultiV8Section(fileID string, tocType, subType uint32, names []string) (map[string][]byte, bool, error)
	PutV8Section(fileID string, tocType, subType uint32, name string, value []byte) error
	// PutMultiV8Section writes back every column that missed the batch GET in ONE
	// funneled writeback (NOTE-441). Returns false when the underlying cache has
	// no batch-put support, so the caller falls back to per-name PutV8Section.
	PutMultiV8Section(fileID string, tocType, subType uint32, values map[string][]byte) (bool, error)
}

// sectionMixedFetcher is the optional interface a section cache may implement to
// batch-fetch keys spanning more than one tocType in a single round-trip — used by
// readBlockColumnarWithCache to request a block's ToC and all its wanted columns
// together (NOTE-185). tieredcache.TypedTieredCache implements it.
type sectionMixedFetcher interface {
	GetMultiV8SectionMixed(fileID string, reqs []shared.V8SectionKey) (map[shared.V8SectionKey][]byte, bool, error)
}

// colSectionName builds the per-column section name "blockIdx/name" using string
// concatenation instead of fmt.Sprintf. blockIdxStr is precomputed once per block
// (strconv.Itoa) and reused across the column loop, so each column pays one
// concatenation rather than a fmt.Sprintf with its reflection + interface boxing.
// NOTE-189: this name is later embedded in the full V8 cache key; building it
// cheaply here removes the throwaway-intermediate allocation on the warm read path
// (once per wanted column per block per query).
func colSectionName(blockIdxStr, name string) string {
	return blockIdxStr + "/" + name
}

// fetchTocAndColumnsCombined issues ONE pipelined GetMulti for the block's ToC key
// plus every wanted column's section key (NOTE-185). It returns the ToC blob (nil if
// the ToC missed the batch — caller then falls back to the per-block GetOrFetch) and a
// map of column name -> compressed blob for the columns that hit. Column keys are built
// from blockIdx + name, which is known before the ToC is decoded, so the whole warm
// read collapses to a single memcache round-trip.
func (r *Reader) fetchTocAndColumnsCombined(
	mf sectionMixedFetcher,
	tocKey string,
	blockIdxStr string,
	wantColumns map[string]struct{},
) (toc []byte, colHits map[string][]byte) {
	// NOTE-188: reqs[0] is the ToC; reqs[1:] are the wanted columns, index-aligned with
	// colNames so the batch response can be re-keyed by bare column name WITHOUT a
	// reverse-lookup (V8SectionKey -> name) map. This batch runs once per block per query
	// on the warm read path (NOTE-185), so eliminating the per-block map allocation matters.
	reqs := make([]shared.V8SectionKey, 0, len(wantColumns)+1)
	reqs = append(reqs, shared.V8SectionKey{TocType: sectionTypeBlockToc, SubType: 0, Name: tocKey})
	colNames := make([]string, 0, len(wantColumns))
	for name := range wantColumns {
		reqs = append(reqs, shared.V8SectionKey{
			TocType: sectionTypeBlockCol,
			SubType: 0,
			Name:    colSectionName(blockIdxStr, name),
		})
		colNames = append(colNames, name)
	}

	hits, ok, err := mf.GetMultiV8SectionMixed(r.fileID, reqs)
	if err != nil || !ok {
		// Batch unsupported or errored: caller falls back to the two-phase path.
		return nil, nil
	}

	toc = hits[reqs[0]]
	if toc == nil {
		// ToC missed — without it we cannot place columns; let the caller's
		// GetOrFetch resolve the ToC (and Phase-2 resolves columns the usual way).
		return nil, nil
	}
	colHits = make(map[string][]byte, len(hits))
	for i, name := range colNames {
		// reqs[i+1] is the column request whose bare name is colNames[i].
		if blob, found := hits[reqs[i+1]]; found {
			colHits[name] = blob
		}
	}
	return toc, colHits
}

// fetchColumnsBatched resolves all wanted columns for one block with a single batched
// section-cache fetch, STASHING each resolved blob on the Reader (stashPreCompressedColumn)
// so the parser decodes straight from it (NOTE-367) — no assembled-buffer copy. Columns
// that miss the batch are read from the (cached ToC or coalesced cold) source, written back
// to the cache, and stashed. NOTE-179.
func (r *Reader) fetchColumnsBatched(
	bg sectionBatchFetcher,
	toc []byte,
	runs coldRuns,
	blockOff int64,
	blockIdx int,
	cols []colMetaEntry,
) error {
	blockIdxStr := strconv.Itoa(blockIdx)
	names := make([]string, len(cols))
	colByName := make(map[string]colMetaEntry, len(cols))
	for i, m := range cols {
		name := colSectionName(blockIdxStr, m.name)
		names[i] = name
		colByName[name] = m
	}

	hits, ok, err := bg.GetMultiV8Section(r.fileID, sectionTypeBlockCol, 0, names)
	if err != nil {
		return fmt.Errorf("block %d batch cols: %w", blockIdx, err)
	}
	if !ok {
		// Cache reported no batch support after the type assertion (shouldn't happen);
		// resolve every column individually as a safe fallback.
		for _, m := range cols {
			if ferr := r.fetchColumnInto(toc, runs, nil, blockOff, blockIdx, m); ferr != nil {
				return ferr
			}
		}
		return nil
	}

	// NOTE-441: collect every cold-miss writeback and flush them in ONE batched
	// SetMulti after the loop rather than one PutV8Section (one memcache Set / one
	// connection acquisition) per missing column. Under concurrent cold-block load
	// the per-column writeback fan drained the idle pool exactly like the per-column
	// reads NOTE-179 collapsed, forcing a fresh dial per column. writebacks holds the
	// freshly-allocated copies keyed by their section name; it is nil on the warm path
	// (every column hit the batch GET) so warm reads allocate nothing extra.
	var writebacks map[string][]byte
	for name, m := range colByName {
		colStart := int64(m.dataOffset)  //nolint:gosec
		colLen := int64(m.compressedLen) //nolint:gosec
		if blob, found := hits[name]; found {
			// NOTE-367: stash the cache-owned blob instead of copying it into the assembled
			// buffer; the parser decodes straight from it (preCompressedLookup). The blob is
			// owned by the section cache for the Reader's lifetime, matching NOTE-234.
			r.stashPreCompressedColumn(blockOff, m, blob)
			continue
		}
		// Miss: read the compressed blob from the cached ToC or a coalesced cold run, queue
		// it for the batched writeback, then stash the freshly-allocated copy (NOTE-367) —
		// the parser decodes from it directly. The cold path mutates shared run state but is
		// single-goroutine here (no fan-out), so no lock needed.
		var blob []byte
		if colStart+colLen <= int64(len(toc)) {
			blob = toc[colStart : colStart+colLen]
		} else {
			rn, rerr := runs.ensure(r, blockOff, colStart)
			if rerr != nil {
				return fmt.Errorf("block %d col %q: %w", blockIdx, m.name, rerr)
			}
			blob = rn.buf[colStart-rn.start : colStart-rn.start+colLen]
		}
		cp := make([]byte, colLen)
		copy(cp, blob)
		if writebacks == nil {
			writebacks = make(map[string][]byte)
		}
		writebacks[name] = cp
		r.stashPreCompressedColumn(blockOff, m, cp)
	}

	// NOTE-441: flush all cold-miss writebacks in ONE funneled PutMultiV8Section. If the cache
	// has no batch-put support, fall back to per-name PutV8Section (the prior behavior).
	if len(writebacks) > 0 {
		if batched, _ := bg.PutMultiV8Section(r.fileID, sectionTypeBlockCol, 0, writebacks); !batched {
			for name, cp := range writebacks {
				_ = bg.PutV8Section(r.fileID, sectionTypeBlockCol, 0, name, cp)
			}
		}
	}
	return nil
}

// fetchColumnInto resolves one wanted column through the section cache and STASHES its
// compressed blob on the Reader (stashPreCompressedColumn) so the parser decodes straight
// from it — never reading the assembled buffer (NOTE-367). Cold-path provider reads
// (coldRuns.ensure) mutate shared run state and are serialized under runMu; runMu may be
// nil on the single-column inline path where no concurrency is in flight. NOTE-177.
//
// The stashed blob is owned by the section cache (GetOrFetchV8Section result) for the
// Reader's lifetime, or is a freshly-allocated copy from the cold/ToC source written back
// into that cache — in both cases its backing array outlives the parse, matching the NOTE-234
// stash lifetime contract (the parser copies all data out during decode anyway). The
// `toc` remains as the cold-fetch fallback source; no assembled-buffer write happens.
func (r *Reader) fetchColumnInto(
	toc []byte,
	runs coldRuns,
	runMu *sync.Mutex,
	blockOff int64,
	blockIdx int,
	m colMetaEntry,
) error {
	colStart := int64(m.dataOffset)  //nolint:gosec
	colLen := int64(m.compressedLen) //nolint:gosec

	colBytes, fetchErr := r.cache.GetOrFetchV8Section(
		r.fileID,
		sectionTypeBlockCol,
		0,
		colSectionName(strconv.Itoa(blockIdx), m.name),
		func() ([]byte, error) {
			if colStart+colLen <= int64(len(toc)) {
				cp := make([]byte, colLen)
				copy(cp, toc[colStart:colStart+colLen])
				return cp, nil
			}
			if runMu != nil {
				runMu.Lock()
				defer runMu.Unlock()
			}
			rn, err := runs.ensure(r, blockOff, colStart)
			if err != nil {
				return nil, fmt.Errorf("col %q: %w", m.name, err)
			}
			cp := make([]byte, colLen)
			copy(cp, rn.buf[colStart-rn.start:colStart-rn.start+colLen])
			return cp, nil
		},
	)
	if fetchErr != nil {
		return fmt.Errorf("block %d col %q: %w", blockIdx, m.name, fetchErr)
	}
	r.stashPreCompressedColumn(blockOff, m, colBytes)
	return nil
}

// coldRun is one coalesced byte span of cold (cache-missing) column data within a block,
// read from the provider once and shared by every column that falls inside it (NOTE-173).
// Field order is chosen to minimize the GC pointer-scan range (fieldalignment).
type coldRun struct {
	err   error
	buf   []byte // populated on first cold miss within the run
	start int64  // file-relative offset of the run
	end   int64  // exclusive
}

// coldRuns is the set of coalesced cold-read runs for one block, sorted ascending by start.
type coldRuns []coldRun

// planColdRuns builds the coalesced cold-read runs for a block: it collects the (start,end)
// ranges of every wanted column whose data lies past the already-cached ToC, sorts them by
// start, and merges ranges separated by at most colCoalesceMaxGap unwanted bytes into runs.
// Sorting makes run construction independent of the metadata array's column ordering, so
// coldRuns.ensure always finds a run that fully covers any cold column's range. Columns that
// fit within the ToC, fall outside the block, or are absent are skipped.
// planColdRunsLazy returns the coalesced cold-run plan only when there is at least one
// cold miss to resolve (NOTE-187). cols is the list of wanted columns that missed the
// section cache; when it is empty (the warm steady-state path) no provider read is
// needed and a nil plan is returned without scanning/sorting. When cols is non-empty the
// returned plan is identical to a direct planColdRuns call over the full metadata.
func (r *Reader) planColdRunsLazy(
	cols []colMetaEntry,
	metas []colMetaEntry,
	wantColumns map[string]struct{},
	blockLen, tocLen int64,
) coldRuns {
	if len(cols) == 0 {
		return nil
	}
	// NOTE-173: Phase-2 coalesced cold fetch. Previously every wanted column that missed
	// the section cache issued its own r.provider.ReadAt — one ranged GET per column. A
	// heavy metrics query touches dozens of small columns across hundreds of cold blocks,
	// and each ReadAt acquires/establishes a backend (S3) connection. A querier CPU profile
	// is dominated by connection setup (kernel __inet_hash_connect / __inet_check_established
	// + TLS handshake crypto), i.e. round-trip count, not bytes. planColdRuns groups the cold
	// columns into a few coalesced runs read once each.
	return r.planColdRuns(metas, wantColumns, blockLen, tocLen)
}

func (r *Reader) planColdRuns(
	metas []colMetaEntry,
	wantColumns map[string]struct{},
	blockLen, tocLen int64,
) coldRuns {
	type byteRange struct{ start, end int64 }
	var ranges []byteRange
	for _, m := range metas {
		if _, ok := wantColumns[m.name]; !ok || m.compressedLen == 0 {
			continue
		}
		colStart := int64(m.dataOffset)  //nolint:gosec
		colLen := int64(m.compressedLen) //nolint:gosec
		if colStart+colLen > blockLen {
			continue
		}
		// Only columns whose data extends past the cached ToC require a provider read.
		if colStart+colLen <= tocLen {
			continue
		}
		ranges = append(ranges, byteRange{start: colStart, end: colStart + colLen})
	}
	slices.SortFunc(ranges, func(a, b byteRange) int { return cmp.Compare(a.start, b.start) })
	var runs coldRuns
	for _, rg := range ranges {
		if n := len(runs); n > 0 && rg.start-runs[n-1].end <= colCoalesceMaxGap {
			// Merge into the current run (handles overlap and small gaps).
			if rg.end > runs[n-1].end {
				runs[n-1].end = rg.end
			}
			continue
		}
		runs = append(runs, coldRun{start: rg.start, end: rg.end})
	}
	return runs
}

// ensure reads (once) the coalesced run containing offset colStart and returns it. The first
// missing column in a run triggers its provider read; subsequent misses in the same run reuse
// the buffer. blockOff is the block's file-relative base offset.
func (runs coldRuns) ensure(r *Reader, blockOff, colStart int64) (*coldRun, error) {
	for i := range runs {
		if colStart >= runs[i].start && colStart < runs[i].end {
			rn := &runs[i]
			if rn.buf == nil && rn.err == nil {
				buf := make([]byte, rn.end-rn.start)
				if _, readErr := r.provider.ReadAt(buf, blockOff+rn.start, rw.DataTypeBlock); readErr != nil {
					rn.err = readErr
				} else {
					rn.buf = buf
				}
			}
			return rn, rn.err
		}
	}
	return nil, fmt.Errorf("no coalesced run covers offset %d", colStart)
}

// readSufficientToC reads the block ToC (header + column-metadata array) starting at
// blockOff, growing the read geometrically until the full metadata array is covered or
// the entire block has been read (NOTE-154). Returning a ToC that contains the whole
// metadata array lets readBlockColumnarWithCache take the columnar Phase-2 path instead
// of falling back to a full block read. The returned buffer is cached by the caller, so
// the growth/parse cost is paid once per block on a cold miss and never on warm queries.
func (r *Reader) readSufficientToC(blockOff, blockLen int64) ([]byte, error) {
	size := min(blockLen, int64(tocHintBytes))
	for {
		buf := make([]byte, size)
		if _, err := r.provider.ReadAt(buf, blockOff, rw.DataTypeMetadata); err != nil {
			return nil, fmt.Errorf("toc read: %w", err)
		}
		if size >= blockLen {
			// Whole block already read; the outer parse handles any genuine corruption.
			return buf, nil
		}
		hdr, err := parseBlockHeader(buf)
		if err != nil {
			// Header error: let the outer fallback handle it (full block read).
			return buf, nil //nolint:nilerr // intentional: defer corruption handling to caller
		}
		if _, _, err := parseColumnMetadataArray(buf, int(shared.BlockHeaderV14Size), int(hdr.columnCount), hdr.version); err == nil {
			return buf, nil // metadata array fully covered
		}
		size = min(blockLen, size*tocGrowthFactor)
	}
}
