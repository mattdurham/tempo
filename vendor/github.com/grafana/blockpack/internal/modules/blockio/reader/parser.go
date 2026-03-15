package reader

// NOTE: Any changes to this file must be reflected in the corresponding specs.md or NOTES.md.

import (
	"encoding/binary"
	"fmt"
	"sync"

	"github.com/golang/snappy"

	"github.com/grafana/blockpack/internal/modules/blockio/shared"
	"github.com/grafana/blockpack/internal/modules/rw"
)

// parsedSketchCache caches fully parsed sketchIndex objects by fileID+"/sketch".
// Since sketch data is immutable (written once per file), entries are never evicted.
// This avoids re-parsing the sketch section on every Reader creation.
var parsedSketchCache sync.Map // key: string → value: *sketchIndex

// parsedIntrinsicCache caches fully decoded IntrinsicColumn objects by fileID+"/intrinsic/"+colName.
// Since intrinsic column data is immutable (written once per file), entries are never evicted.
// This avoids re-running DecodeIntrinsicColumnBlob (snappy + struct build) on every Reader creation.
var parsedIntrinsicCache sync.Map // key: string → value: *shared.IntrinsicColumn

// parsedMetadataCache caches the fully parsed metadata result by fileID.
// This avoids re-reading ~45 MB from bbolt FileCache and re-parsing metadata
// on every Reader creation. File metadata is immutable after compaction.
var parsedMetadataCache sync.Map // key: string → value: *parsedMetadata

// parsedMetadata holds all parsed results from parseV5MetadataLazy.
type parsedMetadata struct {
	metadataBytes []byte
	blockMetas    []shared.BlockMeta
	rangeOffsets  map[string]rangeIndexMeta
	traceIndexRaw []byte
	tsEntries     []tsIndexEntry
	sketchIdx     *sketchIndex
}

// rangeIndexMeta records the byte range within metadataBytes for a
// range column index entry (lazy parsing).
type rangeIndexMeta struct {
	typ    shared.ColumnType
	offset int
	length int
}

// readFooter reads the footer from the end of the file.
// Supports v3 (22 bytes) and v4 (34 bytes) footer formats.
// v4 adds intrinsicIndexOffset[8] + intrinsicIndexLen[4] after the v3 fields.
//
// Detection strategy: try v4 first (read 34 bytes from fileSize-34). If the first 2 bytes
// are version=4, parse v4. Otherwise fall back to v3 (read 22 bytes from fileSize-22).
// This avoids reading extra data before the v3 footer into the version field.
func (r *Reader) readFooter() error {
	if r.fileSize < int64(shared.FooterV3Size) {
		return fmt.Errorf("file too small for footer: %d bytes", r.fileSize)
	}

	cacheKey := r.fileID + "/footer"

	// Try v4 first: only possible if file is large enough.
	if r.fileSize >= int64(shared.FooterV4Size) {
		off := r.fileSize - int64(shared.FooterV4Size)
		buf, err := r.cache.GetOrFetch(cacheKey+"/v4", func() ([]byte, error) {
			b := make([]byte, shared.FooterV4Size)
			n, readErr := r.provider.ReadAt(b, off, rw.DataTypeFooter)
			if readErr != nil {
				return nil, fmt.Errorf("readFooter: %w", readErr)
			}
			if n != int(shared.FooterV4Size) {
				return nil, fmt.Errorf("readFooter: short read: %d bytes", n)
			}
			return b, nil
		})
		if err != nil {
			return fmt.Errorf("readFooter: %w", err)
		}

		if binary.LittleEndian.Uint16(buf[0:]) == shared.FooterV4Version {
			r.footerVersion = shared.FooterV4Version
			r.footerFields.headerOffset = binary.LittleEndian.Uint64(buf[2:])
			r.footerFields.compactOffset = binary.LittleEndian.Uint64(buf[10:])
			r.footerFields.compactLen = binary.LittleEndian.Uint32(buf[18:])
			r.intrinsicIndexOffset = binary.LittleEndian.Uint64(buf[22:])
			r.intrinsicIndexLen = binary.LittleEndian.Uint32(buf[30:])
			if r.footerFields.headerOffset == 0 {
				return fmt.Errorf("readFooter: header_offset is zero")
			}
			r.headerOffset = r.footerFields.headerOffset
			r.compactOffset = r.footerFields.compactOffset
			r.compactLen = r.footerFields.compactLen
			return nil
		}
	}

	// Fall back to v3.
	off := r.fileSize - int64(shared.FooterV3Size)
	buf, err := r.cache.GetOrFetch(cacheKey, func() ([]byte, error) {
		b := make([]byte, shared.FooterV3Size)
		n, readErr := r.provider.ReadAt(b, off, rw.DataTypeFooter)
		if readErr != nil {
			return nil, fmt.Errorf("readFooter: %w", readErr)
		}
		if n != int(shared.FooterV3Size) {
			return nil, fmt.Errorf("readFooter: short read: %d bytes", n)
		}
		return b, nil
	})
	if err != nil {
		return fmt.Errorf("readFooter: %w", err)
	}

	ver := binary.LittleEndian.Uint16(buf[0:])
	if ver != shared.FooterV3Version {
		return fmt.Errorf("readFooter: unsupported footer version %d", ver)
	}

	r.footerVersion = ver
	r.footerFields.headerOffset = binary.LittleEndian.Uint64(buf[2:])
	r.footerFields.compactOffset = binary.LittleEndian.Uint64(buf[10:])
	r.footerFields.compactLen = binary.LittleEndian.Uint32(buf[18:])

	if r.footerFields.headerOffset == 0 {
		return fmt.Errorf("readFooter: header_offset is zero")
	}

	r.headerOffset = r.footerFields.headerOffset
	r.compactOffset = r.footerFields.compactOffset
	r.compactLen = r.footerFields.compactLen
	return nil
}

// readHeader reads the file header at footer.headerOffset.
// For version < 12: 21 bytes (magic[4] + version[1] + metadataOffset[8] + metadataLen[8]).
// For version 12+:  22 bytes (same + signalType[1] at offset 21).
//
// We always read 22 bytes from the provider. For V10/V11 files this reads 1 extra byte
// past the 21-byte header, which is safe because the header is immediately followed by
// other file sections (metadata, compact index, footer), so the extra byte comes from
// subsequent data. This means V12 signal_type (at offset 21) is always present in the
// fixed-size 22-byte buffer.
func (r *Reader) readHeader() error {
	const headerSize = 22
	cacheKey := r.fileID + "/header"

	buf, err := r.cache.GetOrFetch(cacheKey, func() ([]byte, error) {
		b := make([]byte, headerSize)
		n, readErr := r.provider.ReadAt(
			b,
			int64(r.headerOffset), //nolint:gosec // safe: headerOffset is a file offset, fits in int64
			rw.DataTypeHeader,
		)
		if readErr != nil {
			return nil, fmt.Errorf("readHeader: %w", readErr)
		}
		if n != headerSize {
			return nil, fmt.Errorf("readHeader: short read: %d bytes", n)
		}
		return b, nil
	})
	if err != nil {
		return fmt.Errorf("readHeader: %w", err)
	}

	magic := binary.LittleEndian.Uint32(buf[0:])
	if magic != shared.MagicNumber {
		return fmt.Errorf("readHeader: bad magic 0x%08X", magic)
	}

	version := buf[4] //nolint:gosec // safe: buf is headerSize (22) bytes, validated by short-read check above
	if version != shared.VersionV10 && version != shared.VersionV11 &&
		version != shared.VersionV12 && version != shared.VersionV13 {
		return fmt.Errorf("readHeader: unsupported version %d", version)
	}

	r.fileVersion = version
	r.metadataOffset = binary.LittleEndian.Uint64(buf[5:])
	r.metadataLen = binary.LittleEndian.Uint64(buf[13:])

	// V12+ file header has an extra signal_type byte at offset 21.
	// Extracted from the cached buffer — no second provider read needed.
	if r.fileVersion >= shared.VersionV12 {
		r.signalType = buf[21]
	}

	return nil
}

// parseV5MetadataLazy reads the metadata section and eagerly parses:
//   - block index entries → r.blockMetas
//   - range column index byte ranges → r.rangeOffsets (lazy)
//   - trace block index → r.traceIndex
func (r *Reader) parseV5MetadataLazy() error {
	if r.metadataLen == 0 {
		return fmt.Errorf("parseMetadata: metadata_len is zero")
	}

	if r.metadataLen > shared.MaxMetadataSize {
		return fmt.Errorf("parseMetadata: metadata too large: %d bytes", r.metadataLen)
	}

	// Check process-level cache first — avoids re-reading ~45 MB from bbolt
	// and re-parsing metadata on every Reader creation for the same file.
	if r.fileID != "" {
		if cached, ok := parsedMetadataCache.Load(r.fileID); ok {
			pm := cached.(*parsedMetadata)
			r.metadataBytes = pm.metadataBytes
			r.blockMetas = pm.blockMetas
			r.rangeOffsets = pm.rangeOffsets
			r.traceIndexRaw = pm.traceIndexRaw
			r.tsEntries = pm.tsEntries
			r.sketchIdx = pm.sketchIdx
			return nil
		}
	}

	// NOTE-PERF: For snappy-compressed metadata (VersionV12), we cache the *decompressed*
	// bytes under a distinct key ("/metadata/dec") so snappy.Decode runs only on a cache miss,
	// not on every Reader creation. The old "/metadata" key stored compressed bytes and paid
	// full decompression cost every query — profiling showed 23+ s/30s wasted there.
	var (
		cacheKey string
		data     []byte
		err      error
	)
	if r.fileVersion >= shared.VersionV12 {
		cacheKey = r.fileID + "/metadata/dec"
		data, err = r.cache.GetOrFetch(cacheKey, func() ([]byte, error) {
			compressed, readErr := r.readRange(r.metadataOffset, r.metadataLen, rw.DataTypeMetadata)
			if readErr != nil {
				return nil, readErr
			}
			// Guard against decompression bombs before allocating the decode buffer.
			decodedLen, lenErr := snappy.DecodedLen(compressed)
			if lenErr != nil {
				return nil, fmt.Errorf("snappy decoded length: %w", lenErr)
			}
			if uint64(decodedLen) > shared.MaxMetadataSize { //nolint:gosec // safe: decodedLen is non-negative
				return nil, fmt.Errorf(
					"snappy decoded size %d exceeds MaxMetadataSize %d",
					decodedLen, shared.MaxMetadataSize,
				)
			}
			return snappy.Decode(nil, compressed)
		})
	} else {
		cacheKey = r.fileID + "/metadata"
		data, err = r.cache.GetOrFetch(cacheKey, func() ([]byte, error) {
			return r.readRange(r.metadataOffset, r.metadataLen, rw.DataTypeMetadata)
		})
	}
	if err != nil {
		return fmt.Errorf("parseMetadata: %w", err)
	}

	r.metadataBytes = data
	pos := 0

	// Block index: block_count[4] + entries.
	if pos+4 > len(data) {
		return fmt.Errorf("parseMetadata: block_index: short for block_count")
	}

	blockCount := int(binary.LittleEndian.Uint32(data[pos:]))
	pos += 4

	metas, newPos, err := parseBlockIndex(data[pos:pos+len(data)-pos], r.fileVersion, blockCount)
	if err != nil {
		return fmt.Errorf("parseMetadata: block_index: %w", err)
	}

	actualConsumed := newPos
	pos += actualConsumed
	r.blockMetas = metas

	// Range index: range_count[4] + range_count entries.
	if pos+4 > len(data) {
		return fmt.Errorf("parseMetadata: range_index: short for range_count")
	}

	dedCount := int(binary.LittleEndian.Uint32(data[pos:]))
	pos += 4

	rangeStart := pos
	offsets, newPos, err := scanRangeIndexOffsets(data[pos:], dedCount)
	if err != nil {
		return fmt.Errorf("parseMetadata: range_index: %w", err)
	}

	// Adjust offsets to be absolute within data.
	for k, v := range offsets {
		v.offset += rangeStart
		offsets[k] = v
	}

	pos += newPos
	r.rangeOffsets = offsets

	// Column index: block_count × col_count (always 0 in new files; skip without allocating).
	newPos, err = skipColumnIndex(data[pos:], blockCount)
	if err != nil {
		return fmt.Errorf("parseMetadata: column_index: %w", err)
	}

	pos += newPos

	// Trace block index — parsed lazily on first access (search queries never use it).
	// Store raw bytes and skip over the section to reach TS and sketch sections.
	consumed, err := skipTraceBlockIndex(data[pos:])
	if err != nil {
		return fmt.Errorf("parseMetadata: trace_index: %w", err)
	}
	if consumed > 0 {
		r.traceIndexRaw = data[pos : pos+consumed]
	}
	pos += consumed

	// TS index section — optional, present in files written after 2026-03-02.
	// parseTSIndex returns (nil, 0, nil) for old files, enabling graceful degradation.
	if pos < len(data) {
		tsEntries, tsConsumed, tsErr := parseTSIndex(data[pos:])
		if tsErr != nil {
			return fmt.Errorf("parseMetadata: ts_index: %w", tsErr)
		}
		r.tsEntries = tsEntries
		pos += tsConsumed
	}

	// Sketch index section — optional, present in files written after 2026-03-07.
	// Cache the parsed result by fileID to avoid re-parsing on every Reader creation.
	if pos < len(data) && r.fileID != "" {
		skCacheKey := r.fileID + "/sketch"
		if cached, ok := parsedSketchCache.Load(skCacheKey); ok {
			r.sketchIdx = cached.(*sketchIndex)
		} else {
			sketches, skConsumed, skErr := parseSketchIndexSection(data[pos:])
			if skErr != nil {
				return fmt.Errorf("parseMetadata: sketch_index: %w", skErr)
			}
			r.sketchIdx = sketches
			pos += skConsumed
			if sketches != nil {
				parsedSketchCache.Store(skCacheKey, sketches)
			}
		}
	} else if pos < len(data) {
		sketches, skConsumed, skErr := parseSketchIndexSection(data[pos:])
		if skErr != nil {
			return fmt.Errorf("parseMetadata: sketch_index: %w", skErr)
		}
		r.sketchIdx = sketches
		pos += skConsumed
	}

	_ = pos

	// Store in process-level cache for subsequent Reader creations on the same file.
	if r.fileID != "" {
		parsedMetadataCache.Store(r.fileID, &parsedMetadata{
			metadataBytes: r.metadataBytes,
			blockMetas:    r.blockMetas,
			rangeOffsets:  r.rangeOffsets,
			traceIndexRaw: r.traceIndexRaw,
			tsEntries:     r.tsEntries,
			sketchIdx:     r.sketchIdx,
		})
	}
	return nil
}

// parseBlockIndexEntry parses one block index entry for the given version.
// Returns the entry and new position.
func parseBlockIndexEntry(
	data []byte,
	pos int,
	version uint8,
) (shared.BlockMeta, int, error) {
	var meta shared.BlockMeta

	// offset[8] + length[8]
	if pos+16 > len(data) {
		return meta, pos, fmt.Errorf("block_index entry: short for offset/length")
	}

	meta.Offset = binary.LittleEndian.Uint64(data[pos:])
	pos += 8
	meta.Length = binary.LittleEndian.Uint64(data[pos:])
	pos += 8

	// v11+ adds kind[1] before span_count (V12 files also use V11 block layout).
	if version >= shared.VersionV11 {
		if pos+1 > len(data) {
			return meta, pos, fmt.Errorf("block_index entry: short for kind")
		}

		meta.Kind = shared.BlockKind(data[pos])
		pos++
	}

	// span_count[4] + min_start[8] + max_start[8]
	if pos+20 > len(data) {
		return meta, pos, fmt.Errorf("block_index entry: short for span_count/timestamps")
	}

	meta.SpanCount = binary.LittleEndian.Uint32(data[pos:])
	pos += 4
	meta.MinStart = binary.LittleEndian.Uint64(data[pos:])
	pos += 8
	meta.MaxStart = binary.LittleEndian.Uint64(data[pos:])
	pos += 8

	// min_trace_id[16] + max_trace_id[16] — present in V10/V11/V12 only; omitted in V13+.
	if version < shared.VersionV13 {
		if pos+32 > len(data) {
			return meta, pos, fmt.Errorf("block_index entry: short for trace IDs")
		}

		copy(meta.MinTraceID[:], data[pos:pos+16])
		pos += 16
		copy(meta.MaxTraceID[:], data[pos:pos+16])
		pos += 16
	}

	return meta, pos, nil
}

// parseBlockIndex parses block_count block index entries from data.
func parseBlockIndex(data []byte, version uint8, blockCount int) ([]shared.BlockMeta, int, error) {
	metas := make([]shared.BlockMeta, 0, blockCount)
	pos := 0

	for i := range blockCount {
		meta, newPos, err := parseBlockIndexEntry(data, pos, version)
		if err != nil {
			return nil, pos, fmt.Errorf("block[%d]: %w", i, err)
		}

		metas = append(metas, meta)
		pos = newPos
	}

	return metas, pos, nil
}

// skipColumnIndex advances pos past the column index section without allocating.
// For new files, each block has col_count=0 (4 bytes per block).
// For old files, it correctly skips any existing entries.
func skipColumnIndex(data []byte, blockCount int) (int, error) {
	pos := 0

	for b := range blockCount {
		if pos+4 > len(data) {
			return pos, fmt.Errorf("column_index block[%d]: short for col_count", b)
		}

		colCount := int(binary.LittleEndian.Uint32(data[pos:]))
		pos += 4

		for c := range colCount {
			if pos+2 > len(data) {
				return pos, fmt.Errorf("column_index block[%d] col[%d]: short for name_len", b, c)
			}

			nameLen := int(binary.LittleEndian.Uint16(data[pos:]))
			advance := 2 + nameLen + 8 // name_len[2] + name + offset[4] + length[4]
			if advance < 0 || pos+advance > len(data) {
				return pos, fmt.Errorf("column_index block[%d] col[%d]: short for name/offset/length", b, c)
			}

			pos += advance
		}
	}

	return pos, nil
}

// parseTraceBlockIndex parses the trace block index section.
// Supports fmt_version 0x01 (v1: with per-block span indices, discarded on read)
// and fmt_version 0x02 (v2: block IDs only).
// Returns the parsed map and bytes consumed.
func parseTraceBlockIndex(data []byte) (map[[16]byte][]uint16, int, error) {
	if len(data) < 5 {
		return nil, 0, nil
	}

	fmtVersion := data[0]
	if fmtVersion != shared.TraceIndexFmtVersion && fmtVersion != shared.TraceIndexFmtVersion2 {
		return nil, 0, fmt.Errorf("trace_index: unsupported fmt_version %d", fmtVersion)
	}

	traceCount := int(binary.LittleEndian.Uint32(data[1:]))
	pos := 5

	result := make(map[[16]byte][]uint16, traceCount)

	for t := range traceCount {
		if pos+18 > len(data) {
			return nil, pos, fmt.Errorf("trace_index: trace[%d]: short for trace_id+block_count", t)
		}

		var tid [16]byte
		copy(tid[:], data[pos:pos+16])
		pos += 16

		blockRefCount := int(binary.LittleEndian.Uint16(data[pos:]))
		pos += 2

		blockIDs := make([]uint16, 0, blockRefCount)

		if fmtVersion == shared.TraceIndexFmtVersion {
			// v1: block_id[2] + span_count[2] + span_indices[N×2] — discard span indices.
			for b := range blockRefCount {
				if pos+4 > len(data) {
					return nil, pos, fmt.Errorf("trace_index: trace[%d] block[%d]: short for block_id+span_count", t, b)
				}
				blockID := binary.LittleEndian.Uint16(data[pos:])
				pos += 2
				spanCount := int(binary.LittleEndian.Uint16(data[pos:]))
				pos += 2
				if pos+spanCount*2 > len(data) {
					return nil, pos, fmt.Errorf(
						"trace_index: trace[%d] block[%d]: short for span_indices (%d × 2 bytes)",
						t, b, spanCount,
					)
				}
				pos += spanCount * 2
				blockIDs = append(blockIDs, blockID)
			}
		} else {
			// v2: block_id[2] only.
			for b := range blockRefCount {
				if pos+2 > len(data) {
					return nil, pos, fmt.Errorf("trace_index: trace[%d] block[%d]: short for block_id", t, b)
				}
				blockIDs = append(blockIDs, binary.LittleEndian.Uint16(data[pos:]))
				pos += 2
			}
		}

		result[tid] = blockIDs
	}

	return result, pos, nil
}

// skipTraceBlockIndex advances past a trace block index section without building
// the map. This avoids O(N) allocations for search queries that never use the index.
// Returns (consumed, nil) on success; (0, nil) for empty/missing sections.
func skipTraceBlockIndex(data []byte) (int, error) {
	if len(data) < 5 {
		// Treat short sections as empty/missing rather than corrupt. The caller
		// (parseV5MetadataLazy) handles 0-consumed gracefully by skipping the section.
		// A truly corrupt trace index would be caught by fmtVersion validation below.
		return 0, nil
	}
	fmtVersion := data[0]
	if fmtVersion != shared.TraceIndexFmtVersion && fmtVersion != shared.TraceIndexFmtVersion2 {
		return 0, fmt.Errorf("trace_index: unsupported fmt_version %d", fmtVersion)
	}
	traceCount := int(binary.LittleEndian.Uint32(data[1:]))
	pos := 5
	for t := range traceCount {
		if pos+18 > len(data) {
			return pos, fmt.Errorf("trace_index: trace[%d]: short for trace_id+block_count", t)
		}
		pos += 16 // trace_id
		blockRefCount := int(binary.LittleEndian.Uint16(data[pos:]))
		pos += 2
		if fmtVersion == shared.TraceIndexFmtVersion {
			// v1: block_id[2] + span_count[2] + span_indices[N×2]
			for b := range blockRefCount {
				if pos+4 > len(data) {
					return pos, fmt.Errorf("trace_index: trace[%d] block[%d]: short", t, b)
				}
				spanCount := int(binary.LittleEndian.Uint16(data[pos+2:]))
				pos += 4 + spanCount*2
			}
		} else {
			// v2: block_id[2] only
			pos += blockRefCount * 2
		}
	}
	return pos, nil
}

// scanRangeIndexOffsets scans the range index data recording byte offset+length
// for each column entry WITHOUT parsing values.
// data is the slice starting immediately after range_count.
// Returns the offset map and bytes consumed.
func scanRangeIndexOffsets(data []byte, dedCount int) (map[string]rangeIndexMeta, int, error) {
	offsets := make(map[string]rangeIndexMeta, dedCount)
	pos := 0

	for i := range dedCount {
		entryStart := pos

		// name_len[2] + name
		if pos+2 > len(data) {
			return nil, pos, fmt.Errorf("range[%d]: short for name_len", i)
		}

		nameLen := int(binary.LittleEndian.Uint16(data[pos:]))
		pos += 2
		if pos+nameLen > len(data) {
			return nil, pos, fmt.Errorf("range[%d]: short for name", i)
		}

		name := string(data[pos : pos+nameLen])
		pos += nameLen

		// column_type[1]
		if pos+1 > len(data) {
			return nil, pos, fmt.Errorf("range[%d] %q: short for type", i, name)
		}

		colType := shared.ColumnType(data[pos])
		pos++

		// Bucket metadata always present: bucket_min[8] + bucket_max[8] + boundary_count[4]
		if pos+20 > len(data) {
			return nil, pos, fmt.Errorf("range[%d] %q: short for bucket header", i, name)
		}

		pos += 16 // min + max
		boundaryCount := int(binary.LittleEndian.Uint32(data[pos:]))
		pos += 4

		// boundaries[boundary_count × 8]
		if pos+boundaryCount*8 > len(data) {
			return nil, pos, fmt.Errorf("range[%d] %q: short for boundaries", i, name)
		}

		pos += boundaryCount * 8

		// typed_count[4] + typed boundaries
		if pos+4 > len(data) {
			return nil, pos, fmt.Errorf("range[%d] %q: short for typed_count", i, name)
		}

		typedCount := int(binary.LittleEndian.Uint32(data[pos:]))
		pos += 4

		newPos, err := skipTypedBoundaries(data, pos, colType, typedCount)
		if err != nil {
			return nil, pos, fmt.Errorf("range[%d] %q: typed boundaries: %w", i, name, err)
		}

		pos = newPos

		// value_count[4]
		if pos+4 > len(data) {
			return nil, pos, fmt.Errorf("range[%d] %q: short for value_count", i, name)
		}

		valueCount := int(binary.LittleEndian.Uint32(data[pos:]))
		pos += 4

		// Skip value entries to find end.
		for v := range valueCount {
			newPos, err := skipRangeValueEntry(data, pos, colType)
			if err != nil {
				return nil, pos, fmt.Errorf("range[%d] %q value[%d]: %w", i, name, v, err)
			}

			pos = newPos
		}

		offsets[name] = rangeIndexMeta{
			typ:    colType,
			offset: entryStart,
			length: pos - entryStart,
		}
	}

	return offsets, pos, nil
}

// skipTypedBoundaries skips typed boundary data based on column type.
func skipTypedBoundaries(data []byte, pos int, colType shared.ColumnType, count int) (int, error) {
	switch colType {
	case shared.ColumnTypeRangeFloat64:
		// count × float64_bits(8)
		need := count * 8
		if pos+need > len(data) {
			return pos, fmt.Errorf("typed boundaries(float64): need %d bytes at pos %d", need, pos)
		}

		return pos + need, nil

	case shared.ColumnTypeRangeString:
		for i := range count {
			if pos+4 > len(data) {
				return pos, fmt.Errorf("typed boundaries(string)[%d]: short for len", i)
			}

			sLen := int(binary.LittleEndian.Uint32(data[pos:]))
			pos += 4
			if pos+sLen > len(data) {
				return pos, fmt.Errorf("typed boundaries(string)[%d]: short for data", i)
			}

			pos += sLen
		}

		return pos, nil

	case shared.ColumnTypeRangeBytes:
		for i := range count {
			if pos+4 > len(data) {
				return pos, fmt.Errorf("typed boundaries(bytes)[%d]: short for len", i)
			}

			bLen := int(binary.LittleEndian.Uint32(data[pos:]))
			pos += 4
			if pos+bLen > len(data) {
				return pos, fmt.Errorf("typed boundaries(bytes)[%d]: short for data", i)
			}

			pos += bLen
		}

		return pos, nil

	default:
		// RangeInt64/RangeUint64/RangeDuration: typed_count must be 0.
		if count != 0 {
			return pos, fmt.Errorf("typed boundaries: non-zero count %d for type %d", count, colType)
		}

		return pos, nil
	}
}

// skipRangeValueEntry skips one RangeValueEntry for the given column type.
func skipRangeValueEntry(data []byte, pos int, colType shared.ColumnType) (int, error) {
	// value_key depends on type.
	switch colType {
	case shared.ColumnTypeString, shared.ColumnTypeRangeString:
		if pos+4 > len(data) {
			return pos, fmt.Errorf("value_key(string): short for len")
		}

		kLen := int(binary.LittleEndian.Uint32(data[pos:]))
		pos += 4
		if pos+kLen > len(data) {
			return pos, fmt.Errorf("value_key(string): short for data")
		}

		pos += kLen

	case shared.ColumnTypeBytes, shared.ColumnTypeRangeBytes:
		if pos+4 > len(data) {
			return pos, fmt.Errorf("value_key(bytes): short for len")
		}

		kLen := int(binary.LittleEndian.Uint32(data[pos:]))
		pos += 4
		if pos+kLen > len(data) {
			return pos, fmt.Errorf("value_key(bytes): short for data")
		}

		pos += kLen

	case shared.ColumnTypeInt64, shared.ColumnTypeUint64, shared.ColumnTypeFloat64:
		if pos+8 > len(data) {
			return pos, fmt.Errorf("value_key(numeric): short")
		}

		pos += 8

	case shared.ColumnTypeBool:
		if pos+1 > len(data) {
			return pos, fmt.Errorf("value_key(bool): short")
		}

		pos++

	case shared.ColumnTypeRangeInt64,
		shared.ColumnTypeRangeUint64,
		shared.ColumnTypeRangeDuration,
		shared.ColumnTypeRangeFloat64:
		// length_prefix(1 uint8) + key_data where length_prefix is 2 (bucket ID) or 8 (raw value)
		if pos+1 > len(data) {
			return pos, fmt.Errorf("value_key(range numeric): short for length_prefix")
		}

		kLen := int(data[pos])
		pos++
		if pos+kLen > len(data) {
			return pos, fmt.Errorf("value_key(range numeric): short for key_data (len=%d)", kLen)
		}

		pos += kLen

	default:
		return pos, fmt.Errorf("skipRangeValueEntry: unknown column type %d", colType)
	}

	// block_id_count[4] + block_ids[N×4]
	if pos+4 > len(data) {
		return pos, fmt.Errorf("value_entry: short for block_id_count")
	}

	bidCount := int(binary.LittleEndian.Uint32(data[pos:]))
	pos += 4

	need := bidCount * 4
	if pos+need > len(data) {
		return pos, fmt.Errorf("value_entry: short for block_ids (count=%d)", bidCount)
	}

	pos += need
	return pos, nil
}

// readRange reads exactly length bytes at absolute byte offset.
func (r *Reader) readRange(offset, length uint64, dt rw.DataType) ([]byte, error) {
	if length == 0 {
		return nil, nil
	}

	buf := make([]byte, length)
	n, err := r.provider.ReadAt(buf, int64(offset), dt) //nolint:gosec // safe: offset is a file offset, fits in int64
	if err != nil {
		return nil, fmt.Errorf("readRange offset=%d length=%d: %w", offset, length, err)
	}

	if uint64(n) != length { //nolint:gosec // safe: n is bytes read, always non-negative
		return nil, fmt.Errorf("readRange offset=%d: short read %d/%d", offset, n, length)
	}

	return buf, nil
}
