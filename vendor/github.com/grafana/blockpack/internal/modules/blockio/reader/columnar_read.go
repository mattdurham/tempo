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
	"fmt"
	"log/slog"
	"runtime"
	"runtime/debug"
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

	// Phase 1: ToC — cached.
	toc, err := r.cache.GetOrFetchV8Section(r.fileID, sectionTypeBlockToc, 0, tocKey, func() ([]byte, error) {
		tocSize := min(blockLen, tocHintBytes)
		buf := make([]byte, tocSize)
		if _, readErr := r.provider.ReadAt(buf, blockOff, rw.DataTypeMetadata); readErr != nil {
			return nil, fmt.Errorf("toc read: %w", readErr)
		}
		return buf, nil
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

	// Phase 2: one cached fetch per needed column.
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
				buf := make([]byte, colLen)
				if _, readErr := r.provider.ReadAt(buf, blockOff+colStart, rw.DataTypeBlock); readErr != nil {
					return nil, fmt.Errorf("col %q: %w", m.name, readErr)
				}
				return buf, nil
			},
		)
		if fetchErr != nil {
			return nil, fmt.Errorf("block %d col %q: %w", blockIdx, m.name, fetchErr)
		}
		copy(assembled[colStart:colStart+colLen], colBytes)
	}

	return assembled, nil
}
