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
	"sync"

	"github.com/grafana/blockpack/internal/modules/blockio/shared"
	"github.com/grafana/blockpack/internal/modules/rw"
)

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
	metas, tocEnd, err := parseColumnMetadataArray(raw, int(shared.BlockHeaderV14Size), int(hdr.columnCount))
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
		data, err := r.readBlockColumnarWithCache(cr.BlockOffsets[0], cr.BlockLengths[0], blockIdx, wantColumns)
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
			data, err := r.readBlockColumnarWithCache(cr.BlockOffsets[j], cr.BlockLengths[j], blockIdx, wantColumns)
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
func (r *Reader) readBlockColumnarWithCache(
	blockOff, blockLen int64,
	blockIdx int,
	wantColumns map[string]struct{},
) ([]byte, error) {
	// Encode blockIdx in the name field so subType=0 always, avoiding accidental
	// collision with ToCSubTypeBloom(3), ToCSubTypeIntrinsic(4), ToCSubTypeTrace(5).
	tocKey := fmt.Sprintf("%d", blockIdx)

	// Phase 1: ToC — cached. NOTE-154: read a ToC large enough to hold the full
	// column-metadata array. Trace blocks routinely have hundreds of columns whose
	// metadata overflows a fixed 4 KiB hint; the previous code then fell back to a
	// full block read (the #1 querier allocator, ~18% of alloc_space), defeating the
	// columnar cache. The correctly-sized ToC is what gets cached, so warm queries
	// pay one cache hit and one successful parse — no growth, no full-block read.
	toc, err := r.cache.GetOrFetchV8Section(r.fileID, sectionTypeBlockToc, 0, tocKey, func() ([]byte, error) {
		return r.readSufficientToC(blockOff, blockLen)
	})
	if err != nil {
		return nil, fmt.Errorf("block %d toc: %w", blockIdx, err)
	}

	hdr, err := parseBlockHeader(toc)
	if err != nil {
		// Fallback: full block read (same as readBlockColumnar's fallback).
		full := make([]byte, blockLen)
		if _, ferr := r.provider.ReadAt(full, blockOff, rw.DataTypeBlock); ferr != nil {
			return nil, fmt.Errorf("block %d fallback: %w", blockIdx, ferr)
		}
		return full, nil
	}

	metas, tocEnd, err := parseColumnMetadataArray(toc, int(shared.BlockHeaderV14Size), int(hdr.columnCount))
	if err != nil {
		full := make([]byte, blockLen)
		if _, ferr := r.provider.ReadAt(full, blockOff, rw.DataTypeBlock); ferr != nil {
			return nil, fmt.Errorf("block %d fallback: %w", blockIdx, ferr)
		}
		return full, nil
	}

	bufSize := int64(tocEnd) //nolint:gosec
	for _, m := range metas {
		if _, ok := wantColumns[m.name]; !ok || m.compressedLen == 0 {
			continue
		}
		colEnd := int64(m.dataOffset) + int64(m.compressedLen) //nolint:gosec
		if colEnd > blockLen {
			// A wanted column's data extends beyond blockLen — the reported block length
			// may be approximate. Fall back to reading the full block so ParseBlockFromBytes
			// can access all column data safely.
			full := make([]byte, blockLen)
			if _, ferr := r.provider.ReadAt(full, blockOff, rw.DataTypeBlock); ferr != nil {
				return nil, fmt.Errorf("block %d full fallback: %w", blockIdx, ferr)
			}
			return full, nil
		}
		if colEnd > bufSize {
			bufSize = colEnd
		}
	}

	assembled := make([]byte, bufSize)
	copy(assembled, toc[:min(int64(len(toc)), int64(tocEnd))]) //nolint:gosec

	// NOTE-173: Phase-2 coalesced cold fetch. Previously every wanted column that missed
	// the section cache issued its own r.provider.ReadAt — one ranged GET per column. A
	// heavy metrics query touches dozens of small columns across hundreds of cold blocks,
	// and each ReadAt acquires/establishes a backend (S3) connection. A querier CPU
	// profile is dominated by connection setup (kernel __inet_hash_connect /
	// __inet_check_established + TLS handshake crypto), i.e. round-trip count, not bytes.
	// planColdRuns groups the cold columns into a few coalesced runs read once each.
	runs := r.planColdRuns(metas, wantColumns, blockLen, int64(len(toc)))

	// Phase-2 column fetches. Each wanted column's compressed blob is cached as an
	// independent section keyed by blockIdx/colName and written into a DISJOINT region
	// of `assembled` (column extents never overlap).
	cols := make([]colMetaEntry, 0, len(metas))
	for _, m := range metas {
		if _, ok := wantColumns[m.name]; !ok || m.compressedLen == 0 {
			continue
		}
		colStart := int64(m.dataOffset)  //nolint:gosec
		colLen := int64(m.compressedLen) //nolint:gosec
		if colStart+colLen > blockLen {
			// Should not reach here (handled above), but guard defensively.
			continue
		}
		cols = append(cols, m)
	}

	if len(cols) <= 1 {
		// Single (or zero) wanted column: the fan-out machinery is pure overhead. Resolve inline.
		for _, m := range cols {
			if err := r.fetchColumnInto(assembled, toc, runs, nil, blockOff, blockIdx, m); err != nil {
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
		if err := r.fetchColumnsBatched(bg, assembled, toc, runs, blockOff, blockIdx, cols); err != nil {
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
			if err := r.fetchColumnInto(assembled, toc, runs, &runMu, blockOff, blockIdx, m); err != nil {
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

// sectionBatchFetcher is the optional interface a section cache may implement to
// fetch many V8 per-column blobs in one batched (pipelined) request and to write
// back individual blobs that missed the batch (NOTE-179). tieredcache.TypedTieredCache
// implements it; nil/non-batch caches make readBlockColumnarWithCache fall back to the
// concurrent per-column fan-out.
type sectionBatchFetcher interface {
	GetMultiV8Section(fileID string, tocType, subType uint32, names []string) (map[string][]byte, bool, error)
	PutV8Section(fileID string, tocType, subType uint32, name string, value []byte) error
}

// fetchColumnsBatched resolves all wanted columns for one block with a single batched
// section-cache fetch, copying each hit into its disjoint region of assembled. Columns
// that miss the batch are read from the (cached ToC or coalesced cold) source and written
// back to the cache individually, then copied in. NOTE-179.
func (r *Reader) fetchColumnsBatched(
	bg sectionBatchFetcher,
	assembled, toc []byte,
	runs coldRuns,
	blockOff int64,
	blockIdx int,
	cols []colMetaEntry,
) error {
	names := make([]string, len(cols))
	colByName := make(map[string]colMetaEntry, len(cols))
	for i, m := range cols {
		name := fmt.Sprintf("%d/%s", blockIdx, m.name)
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
			if ferr := r.fetchColumnInto(assembled, toc, runs, nil, blockOff, blockIdx, m); ferr != nil {
				return ferr
			}
		}
		return nil
	}

	for name, m := range colByName {
		colStart := int64(m.dataOffset)  //nolint:gosec
		colLen := int64(m.compressedLen) //nolint:gosec
		if blob, found := hits[name]; found {
			copy(assembled[colStart:colStart+colLen], blob)
			continue
		}
		// Miss: read the compressed blob from the cached ToC or a coalesced cold run,
		// write it back to the cache, then copy into assembled. The cold path mutates
		// shared run state but is single-goroutine here (no fan-out), so no lock needed.
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
		_ = bg.PutV8Section(r.fileID, sectionTypeBlockCol, 0, name, cp)
		copy(assembled[colStart:colStart+colLen], cp)
	}
	return nil
}

// fetchColumnInto resolves one wanted column through the section cache and copies its
// compressed blob into its disjoint region of assembled. Cold-path provider reads
// (coldRuns.ensure) mutate shared run state and are serialized under runMu; runMu may be
// nil on the single-column inline path where no concurrency is in flight. NOTE-177.
func (r *Reader) fetchColumnInto(
	assembled, toc []byte,
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
		fmt.Sprintf("%d/%s", blockIdx, m.name),
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
	copy(assembled[colStart:colStart+colLen], colBytes)
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
		if _, _, err := parseColumnMetadataArray(buf, int(shared.BlockHeaderV14Size), int(hdr.columnCount)); err == nil {
			return buf, nil // metadata array fully covered
		}
		size = min(blockLen, size*tocGrowthFactor)
	}
}
