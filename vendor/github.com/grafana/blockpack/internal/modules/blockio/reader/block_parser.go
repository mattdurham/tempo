package reader

// NOTE: Any changes to this file must be reflected in the corresponding specs.md or NOTES.md.

import (
	"encoding/binary"
	"fmt"
	"log/slog"
	"sync"

	"github.com/golang/snappy"
	"github.com/grafana/blockpack/internal/modules/blockio/shared"
)

// blockHeader holds the parsed block header fields.
type blockHeader struct {
	magic       uint32
	version     uint8
	spanCount   uint32
	columnCount uint32
}

// colMetaEntry holds one parsed column metadata entry.
type colMetaEntry struct {
	name            string
	colType         shared.ColumnType
	dataOffset      uint64
	compressedLen   uint32 // snappy-compressed byte length on disk
	uncompressedLen uint32 // raw byte length after snappy decompress
}

// Block header field offsets (M-28a).
// Wire format: magic[4]+version[1]+reserved[3]+span_count[4]+column_count[4]+reserved2[8] = 24 bytes.
const (
	blkHdrOffVersion     = 4  // uint8 block version field
	blkHdrOffSpanCount   = 8  // uint32 span_count field
	blkHdrOffColumnCount = 12 // uint32 column_count field
)

// parseBlockHeader parses the 24-byte block header from data.
func parseBlockHeader(data []byte) (blockHeader, error) {
	if len(data) < 24 {
		return blockHeader{}, fmt.Errorf("block header: need 24 bytes, have %d", len(data))
	}

	hdr := blockHeader{
		magic:       binary.LittleEndian.Uint32(data[0:]),
		version:     data[blkHdrOffVersion],
		spanCount:   binary.LittleEndian.Uint32(data[blkHdrOffSpanCount:]),
		columnCount: binary.LittleEndian.Uint32(data[blkHdrOffColumnCount:]),
		// bytes 16-23: reserved2 (formerly trace_count + trace_table_len, always zero)
	}

	if hdr.magic != shared.MagicNumber {
		return blockHeader{}, fmt.Errorf("block header: bad magic 0x%08X", hdr.magic)
	}

	// SPEC-ROOT-013: only enc_version=3 (V14 column encoding) is supported by this decoder.
	// V12 blocks use enc_version=2 and are not readable; they must be compacted to V14 first.
	// Accepting V12 here would cause a misleading "unsupported version 2" error deep in column decode.
	if hdr.version != shared.VersionBlockV14 {
		return blockHeader{}, fmt.Errorf(
			"block header: version %d not supported (only V14 supported; V12 files must be compacted to V14 first)",
			hdr.version,
		)
	}

	return hdr, nil
}

// parseColumnMetadataArray parses colCount column metadata entries starting at offset.
// Wire format per entry (SPEC-V14-001):
//
//	name_len[2] + name[name_len] + col_type[1] + data_offset[8] + compressed_len[4] + uncompressed_len[4]
//
// Returns entries and the new offset after the last entry.
func parseColumnMetadataArray(data []byte, offset int, colCount int) ([]colMetaEntry, int, error) {
	entries := make([]colMetaEntry, 0, colCount)
	pos := offset

	for i := range colCount {
		if pos+2 > len(data) {
			return nil, pos, fmt.Errorf("col_meta[%d]: short for name_len", i)
		}

		nameLen := int(binary.LittleEndian.Uint16(data[pos:]))
		pos += 2

		if nameLen > shared.MaxNameLen {
			return nil, pos, fmt.Errorf("col_meta[%d]: name_len %d exceeds MaxNameLen", i, nameLen)
		}

		if pos+nameLen > len(data) {
			return nil, pos, fmt.Errorf("col_meta[%d]: short for name", i)
		}

		name := string(data[pos : pos+nameLen])
		pos += nameLen

		// colMetaFixedSize: col_type[1] + data_offset[8] + compressed_len[4] + uncompressed_len[4] = 17 bytes
		const colMetaFixedSize = 17
		if pos+colMetaFixedSize > len(data) {
			return nil, pos, fmt.Errorf("col_meta[%d]: short for type+offsets", i)
		}

		colType := shared.ColumnType(data[pos])
		pos++

		dataOffset := binary.LittleEndian.Uint64(data[pos:])
		pos += 8

		// SPEC-V14-001: compressed_len[4 LE] + uncompressed_len[4 LE]
		compressedLen := binary.LittleEndian.Uint32(data[pos:])
		pos += 4
		uncompressedLen := binary.LittleEndian.Uint32(data[pos:])
		pos += 4

		entries = append(entries, colMetaEntry{
			name:            name,
			colType:         colType,
			dataOffset:      dataOffset,
			compressedLen:   compressedLen,
			uncompressedLen: uncompressedLen,
		})
	}

	return entries, pos, nil
}

// parseBlockColumnsReuse decodes rawBytes into a Block.
// wantColumns: if non-nil, only decode columns in this set.
// prevBlock: if non-nil and same column set, reuse Column allocations.
// intern is the caller's per-reader string intern table; if nil a new map is used.
func parseBlockColumnsReuse(
	rawBytes []byte,
	wantColumns map[string]struct{},
	prevBlock *Block,
	meta shared.BlockMeta,
	intern map[string]string,
) (*Block, error) {
	if intern == nil {
		intern = make(map[string]string)
	}
	ctx := &decodeCtx{intern: intern}
	hdr, err := parseBlockHeader(rawBytes)
	if err != nil {
		return nil, fmt.Errorf("parseBlock: %w", err)
	}

	spanCount := int(hdr.spanCount)
	colCount := int(hdr.columnCount)

	metas, _, err := parseColumnMetadataArray(rawBytes, 24, colCount)
	if err != nil {
		return nil, fmt.Errorf("parseBlock: column metadata: %w", err)
	}

	var columns map[shared.ColumnKey]*Column
	if prevBlock != nil && prevBlock.columns != nil {
		columns = prevBlock.columns
		// Clear values from existing columns; we will re-populate.
		for _, col := range columns {
			resetColumn(col)
		}
	} else {
		columns = make(map[shared.ColumnKey]*Column, colCount)
	}

	for _, m := range metas {
		if wantColumns != nil {
			if _, ok := wantColumns[m.name]; !ok {
				continue
			}
		}

		// Trace-level columns (compressedLen == 0) are skipped here.
		if m.compressedLen == 0 {
			continue
		}

		start := int(m.dataOffset)          //nolint:gosec // safe: dataOffset bounded by block size < MaxBlockSize
		end := start + int(m.compressedLen) //nolint:gosec // safe: compressedLen bounded by block size < MaxBlockSize
		if start < 0 || end > len(rawBytes) {
			return nil, fmt.Errorf(
				"parseBlock: col %q data offset %d len %d out of range (block %d bytes)",
				m.name, m.dataOffset, m.compressedLen, len(rawBytes),
			)
		}

		colData := rawBytes[start:end]

		// SPEC-V14-001: each column blob is snappy-compressed; decompress before decode.
		// SPEC-ROOT-012: decompressV14ColumnData guards against decompression-bomb OOM.
		var decErr error
		colData, decErr = decompressV14ColumnData(m.name, colData, m.uncompressedLen)
		if decErr != nil {
			return nil, fmt.Errorf("parseBlock: %w", decErr)
		}

		key := shared.ColumnKey{Name: m.name, Type: m.colType}
		var col *Column
		if existing, ok := columns[key]; ok {
			col = existing
		} else {
			col = &Column{}
		}

		col.Name = m.name
		col.Type = m.colType

		decoded, err := readColumnEncoding(colData, spanCount, m.colType, ctx)
		if err != nil {
			return nil, fmt.Errorf("parseBlock: col %q: %w", m.name, err)
		}

		// Copy decoded fields into col (preserving pointer if reusing).
		col.StringDict = decoded.StringDict
		col.StringIdx = decoded.StringIdx
		col.Int64Dict = decoded.Int64Dict
		col.Int64Idx = decoded.Int64Idx
		col.Uint64Dict = decoded.Uint64Dict
		col.Uint64Idx = decoded.Uint64Idx
		col.Float64Dict = decoded.Float64Dict
		col.Float64Idx = decoded.Float64Idx
		col.BoolDict = decoded.BoolDict
		col.BoolIdx = decoded.BoolIdx
		col.BytesDict = decoded.BytesDict
		col.BytesIdx = decoded.BytesIdx
		col.BytesInline = decoded.BytesInline
		col.Present = decoded.Present
		col.SpanCount = decoded.SpanCount
		col.sparseDictIdx = decoded.sparseDictIdx // NOTE-PERF-1: lazy dense expansion
		col.decoded.Store(true)                   // NOTE-CONC-001: mark eagerly decoded so needsDecode() is false

		columns[key] = col
	}

	// NOTE-001: Lazy registration — when wantColumns is non-nil, eagerly-skipped columns
	// are registered with compressedEncoding pointing into rawBytes and NO immediate decode.
	// Both snappy decompression and full column decode are deferred to the first accessor call
	// (ensureDecompressed then decodeNow). IsPresent() also triggers full decode on a lazy
	// column (it calls decodeNow via decodeOnce). SPEC-V14-002: non-wanted columns pay zero
	// CPU/memory cost for decompression until first accessed.
	//
	// NOTE-002: Arena-like pre-allocation — one []Column slice sized to len(metas)
	// replaces N individual *Column heap allocations. Pointers into the slice are stable
	// because capacity is fixed upfront and append never reallocates.
	var lazyStore []Column
	if wantColumns != nil {
		lazyStore = make([]Column, 0, len(metas))
		for _, m := range metas {
			if _, wanted := wantColumns[m.name]; wanted {
				continue // already eagerly decoded
			}

			if m.compressedLen == 0 {
				continue // trace-level column, no data
			}

			key := shared.ColumnKey{Name: m.name, Type: m.colType}
			if _, exists := columns[key]; exists {
				continue // already registered (shouldn't happen, but guard)
			}

			start := int(m.dataOffset)          //nolint:gosec
			end := start + int(m.compressedLen) //nolint:gosec
			if start < 0 || end > len(rawBytes) {
				// SPEC-ROOT-010: log the skip so operators can detect corrupt/malicious data.
				slog.Warn("block_parser: lazy column offset out of range — skipping",
					"column", m.name, "start", start, "end", end,
					"block_size", len(rawBytes))
				continue
			}

			// SPEC-ROOT-012: TOC bomb guard — reject oversized columns at registration time
			// without paying the cost of decompression. Full snappy decode is deferred to
			// first access via ensureDecompressed (SPEC-V14-002).
			if m.uncompressedLen > uint32(shared.MaxBlockSize) { //nolint:gosec
				slog.Warn("block_parser: lazy column uncompressed_len exceeds MaxBlockSize — skipping",
					"column", m.name,
					"uncompressed_len", m.uncompressedLen,
					"max_block_size", shared.MaxBlockSize)
				continue
			}

			lazyStore = append(lazyStore, Column{
				Name:               m.name,
				Type:               m.colType,
				SpanCount:          spanCount,
				compressedEncoding: rawBytes[start:end], // zero-copy sub-slice; decompressed on first access
				uncompressedLen:    m.uncompressedLen,
				internMap:          nil, // nil → internString skips map; safe for concurrent lazy decode
			})
			// Safe: cap was set to len(metas) and we append ≤ len(metas) items, so no realloc.
			columns[key] = &lazyStore[len(lazyStore)-1]
		}
	}

	blk := &Block{
		spanCount:       spanCount,
		columns:         columns,
		lazyColumnStore: lazyStore,
		meta:            meta,
	}
	blk.buildNameIndex()
	blk.BuildIterFields()

	return blk, nil
}

// decompressV14ColumnData applies SPEC-ROOT-012 guards and snappy-decompresses a V14 column blob.
// Returns the decompressed bytes or an error if the TOC length, frame header, or decode fails.
// SPEC-ROOT-012: guards against decompression-bomb OOM via both TOC and snappy frame-header checks.
func decompressV14ColumnData(name string, data []byte, uncompressedLen uint32) ([]byte, error) {
	if uncompressedLen > uint32(shared.MaxBlockSize) { //nolint:gosec
		return nil, fmt.Errorf("col %q: uncompressed_len %d exceeds MaxBlockSize", name, uncompressedLen)
	}
	frameLen, lenErr := snappy.DecodedLen(data)
	if lenErr != nil {
		return nil, fmt.Errorf("col %q: snappy frame header: %w", name, lenErr)
	}
	if frameLen > shared.MaxBlockSize {
		return nil, fmt.Errorf("col %q: snappy frame claims %d bytes, exceeds MaxBlockSize", name, frameLen)
	}
	decompressed, decErr := snappy.Decode(nil, data)
	if decErr != nil {
		return nil, fmt.Errorf("col %q snappy decode: %w", name, decErr)
	}
	if uint32(len(decompressed)) != uncompressedLen { //nolint:gosec
		return nil, fmt.Errorf(
			"col %q: decoded length %d does not match uncompressed_len %d",
			name,
			len(decompressed),
			uncompressedLen,
		)
	}
	return decompressed, nil
}

// resetColumn zeroes a Column's value fields while retaining the allocation.
// IMPORTANT: both decodeOnce and decoded must be reset here because Column instances are
// reused across block parses (ParseBlockFromBytes re-uses prevBlock.columns). decoded.Store(false)
// is the primary guard — needsDecode() reads decoded atomically and would skip decodeNow for
// any column that still has decoded==true from the previous block. Resetting decodeOnce alone
// is not sufficient: decoded.Load() is the outer check, decodeOnce is the inner serializer.
func resetColumn(col *Column) {
	col.StringDict = col.StringDict[:0]
	col.StringIdx = col.StringIdx[:0]
	col.Int64Dict = col.Int64Dict[:0]
	col.Int64Idx = col.Int64Idx[:0]
	col.Uint64Dict = col.Uint64Dict[:0]
	col.Uint64Idx = col.Uint64Idx[:0]
	col.Float64Dict = col.Float64Dict[:0]
	col.Float64Idx = col.Float64Idx[:0]
	col.BoolDict = col.BoolDict[:0]
	col.BoolIdx = col.BoolIdx[:0]
	col.BytesDict = col.BytesDict[:0]
	col.BytesIdx = col.BytesIdx[:0]
	col.BytesInline = nil
	col.Present = nil
	// NOTE-001: clear lazy decode fields so reused columns don't carry stale state.
	col.rawEncoding = nil
	col.compressedEncoding = nil // SPEC-V14-002: clear deferred decompression state
	col.uncompressedLen = 0
	col.internMap = nil
	col.sparseDictIdx = nil // NOTE-PERF-1: clear deferred dense expansion
	col.decodeOnce = sync.Once{}
	col.denseOnce = sync.Once{}
	col.decompressOnce = sync.Once{}
	col.decoded.Store(false) // NOTE-CONC-001: reset atomic so needsDecode() returns true for reused column
}
