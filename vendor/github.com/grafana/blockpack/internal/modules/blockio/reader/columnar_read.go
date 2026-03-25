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
// It implements SPEC-005: two-phase read to eliminate unnecessary S3 I/O.
//
// When wantColumns is nil, falls back to ReadCoalescedBlocks (full block reads).
// Blocks within the group are processed concurrently; column reads within each
// block are sequential. Falls back to full block read on any TOC parse error.
func (r *Reader) ReadGroupColumnar(cr shared.CoalescedRead, wantColumns map[string]struct{}) (map[int][]byte, error) {
	if wantColumns == nil {
		return ReadCoalescedBlocks(r.provider, []shared.CoalescedRead{cr})
	}

	type blockResult struct {
		err      error  // pointer fields first: type+value ptrs (offsets 0,8)
		data     []byte // backing ptr (offset 16), len+cap non-ptrs (24,32)
		blockIdx int    // non-ptr (offset 40) — last pointer is at 16, scan = 24 bytes
	}

	results := make([]blockResult, len(cr.BlockIDs))
	var wg sync.WaitGroup

	for j, blockIdx := range cr.BlockIDs {
		wg.Add(1)
		go func(j, blockIdx int) {
			defer wg.Done()
			data, err := r.readBlockColumnar(cr.BlockOffsets[j], cr.BlockLengths[j], wantColumns)
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

// readBlockColumnar reads only the wanted column bytes from a single internal block.
//
// Phase 1: read the first tocHintBytes from blockOff — covers header + column metadata.
// Phase 2: for each wanted column, issue a targeted ReadAt at (blockOff + col.dataOffset).
//
// The result is a sparse buffer of size max(col.dataOffset + col.dataLen) for all
// wanted columns. The header and column metadata occupy [0:tocEnd]. Wanted column
// bytes are at their original offsets. All other positions are zero — they are never
// accessed by parseBlockColumnsReuse when wantColumns is set.
//
// Falls back to reading the full block when the column metadata array spans past
// tocHintBytes (i.e. the block has more columns than fit in the initial read). Header
// parse errors (bad magic, unsupported version) are returned as errors — callers
// should fall back to ReadGroup for those cases.
func (r *Reader) readBlockColumnar(blockOff, blockLen int64, wantColumns map[string]struct{}) ([]byte, error) {
	// Phase 1: read TOC.
	tocSize := min(blockLen, tocHintBytes)
	toc := make([]byte, tocSize)
	if _, err := r.provider.ReadAt(toc, blockOff, rw.DataTypeBlock); err != nil {
		return nil, fmt.Errorf("readBlockColumnar: toc: %w", err)
	}

	hdr, err := parseBlockHeader(toc)
	if err != nil {
		return nil, fmt.Errorf("readBlockColumnar: header: %w", err)
	}

	metas, tocEnd, err := parseColumnMetadataArray(toc, 24, int(hdr.columnCount), hdr.version)
	if err != nil {
		// Metadata spills past tocHintBytes — fall back to full block read.
		full := make([]byte, blockLen)
		if _, ferr := r.provider.ReadAt(full, blockOff, rw.DataTypeBlock); ferr != nil {
			return nil, fmt.Errorf("readBlockColumnar: fallback: %w", ferr)
		}
		return full, nil
	}

	// Compute the sparse buffer size: from 0 to the end of the last wanted column.
	// Clamp each column range to blockLen to guard against corrupt/malicious TOC
	// entries that could drive arbitrarily large allocations (SPEC-005d).
	bufSize := int64(tocEnd) //nolint:gosec
	for _, m := range metas {
		if _, ok := wantColumns[m.name]; !ok || m.dataLen == 0 {
			continue
		}
		colEnd := int64(m.dataOffset) + int64(m.dataLen) //nolint:gosec
		if colEnd > blockLen {
			// TOC offset/length out of bounds — skip this column entry.
			continue
		}
		if colEnd > bufSize {
			bufSize = colEnd
		}
	}

	// Assemble sparse buffer: copy header + column metadata, leave column gaps zeroed.
	assembled := make([]byte, bufSize)
	copy(assembled, toc[:tocEnd])

	// Phase 2: targeted range reads for each wanted column.
	for _, m := range metas {
		if _, ok := wantColumns[m.name]; !ok || m.dataLen == 0 {
			continue
		}
		colStart := int64(m.dataOffset) //nolint:gosec
		colLen := int64(m.dataLen)      //nolint:gosec

		// If the column data falls within the already-read TOC buffer, copy from there.
		if colStart+colLen <= int64(len(toc)) {
			copy(assembled[colStart:], toc[colStart:colStart+colLen])
			continue
		}

		// Issue targeted range read from storage.
		dst := assembled[colStart : colStart+colLen]
		if _, err := r.provider.ReadAt(dst, blockOff+colStart, rw.DataTypeBlock); err != nil {
			return nil, fmt.Errorf("readBlockColumnar: column %q: %w", m.name, err)
		}
	}

	return assembled, nil
}
