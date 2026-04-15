package reader

// NOTE: Any changes to this file must be reflected in the corresponding specs.md or NOTES.md.

import (
	"encoding/binary"
	"fmt"

	"github.com/grafana/blockpack/internal/modules/blockio/shared"
	"github.com/grafana/blockpack/internal/modules/rw"
)

// Compact block table wire-format constants.
// Each entry is file_offset[8] + file_length[4] = 12 bytes.
const (
	compactBlockEntrySize   = 12 // bytes per block table entry
	compactFileLengthOffset = 8  // byte offset of file_length within an entry
)

// compactBlockEntry holds file location for one block as stored in the compact block table.
type compactBlockEntry struct {
	fileOffset uint64
	fileLength uint32
}

// splitV14CompactSection splits a V14 compact trace section blob into its two logical parts:
// the "header" (magic + version + block_count + optional bloom + block_table) and the
// "trace index" (fmt_version[1] + trace_count[4] + ... to end of blob).
//
// The split point is computed by parsing the fixed-layout fields up through the block table.
// Returns sub-slices of data (no copy); callers that need to retain the header or trace index
// after the source buffer is released must copy them explicitly.
//
// Returns an error if data is too short, has a bad magic number, or has an unsupported version.
// Also returns an error if the trace index portion is less than 5 bytes.
func splitV14CompactSection(data []byte) (header []byte, traceIndex []byte, err error) {
	if len(data) < 9 {
		return nil, nil, fmt.Errorf("v14 compact section: too short (%d bytes)", len(data))
	}

	magic := binary.LittleEndian.Uint32(data[0:])
	if magic != shared.CompactIndexMagic {
		return nil, nil, fmt.Errorf("v14 compact section: bad magic 0x%08X", magic)
	}

	version := data[4]
	if version != shared.CompactIndexVersion && version != shared.CompactIndexVersion2 {
		return nil, nil, fmt.Errorf("v14 compact section: unsupported version %d", version)
	}

	blockCount := int(binary.LittleEndian.Uint32(data[5:])) //nolint:gosec
	if blockCount > shared.MaxBlocks {
		return nil, nil, fmt.Errorf(
			"v14 compact section: block_count %d exceeds maximum %d",
			blockCount,
			shared.MaxBlocks,
		)
	}
	pos := shared.CompactIndexHeaderSize

	if version == shared.CompactIndexVersion2 {
		if pos+4 > len(data) {
			return nil, nil, fmt.Errorf("v14 compact section: short for bloom_bytes")
		}
		bloomBytes := int(binary.LittleEndian.Uint32(data[pos:])) //nolint:gosec
		pos += 4
		if bloomBytes > shared.TraceIDBloomMaxBytes {
			return nil, nil, fmt.Errorf(
				"v14 compact section: bloom_bytes %d exceeds maximum %d",
				bloomBytes,
				shared.TraceIDBloomMaxBytes,
			)
		}
		if pos+bloomBytes > len(data) {
			return nil, nil, fmt.Errorf("v14 compact section: short for bloom_data (need %d bytes)", bloomBytes)
		}
		pos += bloomBytes
	}

	// Skip block table: block_count × 12 bytes.
	need := blockCount * compactBlockEntrySize
	if pos+need > len(data) {
		return nil, nil, fmt.Errorf("v14 compact section: short for block_table (need %d)", need)
	}
	pos += need

	// pos now points at the start of the trace index (fmt_version[1] + ...).
	if len(data)-pos < 5 {
		return nil, nil, fmt.Errorf("v14 compact section: trace_index too short (%d bytes)", len(data)-pos)
	}

	return data[:pos], data[pos:], nil
}

// parseCompactIndexBytesV14Header parses only the header portion of a V14 compact trace section:
// magic + version + block_count + optional bloom filter + block table.
// The trace index bytes are NOT parsed; instead isV14TraceSection is set on compactParsed so
// ensureTraceIndexRaw will fetch and split them lazily on a bloom hit (phase-2 I/O).
//
// Also populates r.blockMetas from the block table if it is not already populated.
//
// The input header bytes must have been produced by splitV14CompactSection (header sub-slice).
func (r *Reader) parseCompactIndexBytesV14Header(header []byte) error {
	if len(header) < 9 {
		return fmt.Errorf("v14 compact header: too short (%d bytes)", len(header))
	}

	magic := binary.LittleEndian.Uint32(header[0:])
	if magic != shared.CompactIndexMagic {
		return fmt.Errorf("v14 compact header: bad magic 0x%08X", magic)
	}

	version := header[4]
	if version != shared.CompactIndexVersion && version != shared.CompactIndexVersion2 {
		return fmt.Errorf("v14 compact header: unsupported version %d", version)
	}

	blockCount := int(binary.LittleEndian.Uint32(header[5:])) //nolint:gosec
	if blockCount > shared.MaxBlocks {
		return fmt.Errorf("v14 compact header: block_count %d exceeds maximum %d", blockCount, shared.MaxBlocks)
	}
	pos := shared.CompactIndexHeaderSize

	var traceIDBloom []byte
	if version == shared.CompactIndexVersion2 {
		if pos+4 > len(header) {
			return fmt.Errorf("v14 compact header: short for bloom_bytes")
		}
		bloomBytes := int(binary.LittleEndian.Uint32(header[pos:])) //nolint:gosec
		pos += 4
		if bloomBytes > shared.TraceIDBloomMaxBytes {
			return fmt.Errorf(
				"v14 compact header: bloom_bytes %d exceeds maximum %d",
				bloomBytes,
				shared.TraceIDBloomMaxBytes,
			)
		}
		if pos+bloomBytes > len(header) {
			return fmt.Errorf("v14 compact header: short for bloom_data (need %d bytes)", bloomBytes)
		}
		traceIDBloom = make([]byte, bloomBytes)
		copy(traceIDBloom, header[pos:pos+bloomBytes])
		pos += bloomBytes
	}

	need := blockCount * compactBlockEntrySize
	if pos+need > len(header) {
		return fmt.Errorf("v14 compact header: short for block_table (need %d)", need)
	}

	blockTable := make([]compactBlockEntry, blockCount)
	for i := range blockCount {
		blockTable[i] = compactBlockEntry{
			fileOffset: binary.LittleEndian.Uint64(header[pos:]),
			fileLength: binary.LittleEndian.Uint32(header[pos+compactFileLengthOffset:]),
		}
		pos += compactBlockEntrySize
	}

	r.compactParsed = &compactTraceIndex{
		blockTable:        blockTable,
		traceIDBloom:      traceIDBloom,
		traceIndexRaw:     nil, // fetched lazily on bloom hit via ensureTraceIndexRaw
		isV14TraceSection: true,
	}

	// Populate blockMetas from the compact block table so ReadBlockRaw,
	// CoalescedGroups, and other block-access methods work for lean readers.
	if len(r.blockMetas) == 0 {
		r.blockMetas = make([]shared.BlockMeta, len(blockTable))
		for i, entry := range blockTable {
			r.blockMetas[i] = shared.BlockMeta{
				Offset: entry.fileOffset,
				Length: uint64(entry.fileLength),
			}
		}
	}

	return nil
}

// BlocksForTraceID returns block indices containing the given trace ID.
// Uses the compact index as a fast path when available. If the compact index
// returns nil (bloom rejected the ID or ID not in hash map), falls through to
// the full trace index — which is the authoritative answer for full readers.
// For lean readers (NewLeanReaderFromProvider), r.traceIndex is always empty so
// the compact index result is final.
func (r *Reader) BlocksForTraceID(traceID [16]byte) []int {
	// For V14 files, compactParsed is populated lazily on first access.
	_ = r.ensureV14TraceSection()
	// For V3/V4 files, compactLen > 0 gates ensureCompactIndexParsed (lazy I/O).
	if r.compactParsed != nil {
		blocks := r.BlocksForTraceIDCompact(traceID)
		if blocks != nil {
			return blocks
		}
	} else if r.compactLen > 0 {
		if err := r.ensureCompactIndexParsed(); err == nil {
			blocks := r.BlocksForTraceIDCompact(traceID)
			if blocks != nil {
				return blocks
			}
		}
	}

	r.ensureTraceIndex()
	blockIDs, ok := r.traceIndex[traceID]
	if !ok {
		return nil
	}

	return blockIDsToInts(blockIDs)
}

// BlocksForTraceIDCompact searches the compact trace index.
// Checks the trace ID bloom filter first — returns nil immediately if the bloom
// reports the ID as definitely absent, avoiding a hash map lookup.
// For lean readers, the trace index bytes are fetched lazily here on a bloom hit
// (phase-2 I/O), so the phase-1 eager read stays at ~15 MB instead of ~700 MB.
func (r *Reader) BlocksForTraceIDCompact(traceID [16]byte) []int {
	if r.compactParsed == nil {
		return nil
	}

	// NOTE-36: bloom filter check before hash map — fast path for absent trace IDs.
	// See writer/NOTES.md § NOTE-36 for the compact index v2 format.
	if !shared.TestTraceIDBloom(r.compactParsed.traceIDBloom, traceID) {
		return nil
	}

	// Bloom hit: ensure the trace index bytes are loaded (lazy for lean readers).
	if err := r.ensureTraceIndexRaw(); err != nil {
		return nil
	}

	blockIDs := scanTraceIndexRaw(r.compactParsed.traceIndexRaw, traceID)
	if blockIDs == nil {
		return nil
	}

	return blockIDsToInts(blockIDs)
}

// blockIDsToInts converts a []uint16 block ID list to a deduplicated []int slice.
// The v2 writer never produces duplicate block IDs, but v1 files could theoretically
// contain them; the dedup map is retained for safety when reading old files.
func blockIDsToInts(blockIDs []uint16) []int {
	blocks := make([]int, 0, len(blockIDs))
	seen := make(map[int]struct{}, len(blockIDs))
	for _, bid := range blockIDs {
		bi := int(bid)
		if _, dup := seen[bi]; !dup {
			seen[bi] = struct{}{}
			blocks = append(blocks, bi)
		}
	}
	return blocks
}

// scanTraceIndexRaw searches raw trace-index bytes for traceID without building a map.
// Supports fmt_version 1 (v1: with per-block span indices) and 2 (v2: block IDs only).
// Returns the block IDs for the matching trace, or nil if not found.
// Zero allocations on miss; one small []uint16 allocation on hit.
// NOTE-PERF-COMPACT: called by BlocksForTraceIDCompact and TraceEntries in place of a
// map lookup. Eliminates O(traceCount) map+slice allocations from parseTraceBlockIndex.
func scanTraceIndexRaw(data []byte, traceID [16]byte) []uint16 {
	if len(data) < 5 {
		return nil
	}

	fmtVersion := data[0]
	if fmtVersion != shared.TraceIndexFmtVersion && fmtVersion != shared.TraceIndexFmtVersion2 {
		return nil
	}

	traceCount := int(binary.LittleEndian.Uint32(data[1:]))
	pos := 5

	for range traceCount {
		if pos+18 > len(data) {
			return nil
		}

		// Compare trace ID in-place — no allocation.
		match := *(*[16]byte)(data[pos : pos+16]) == traceID
		pos += 16

		blockRefCount := int(binary.LittleEndian.Uint16(data[pos:]))
		pos += 2

		if fmtVersion == shared.TraceIndexFmtVersion {
			// v1: block_id[2] + span_count[2] + span_indices[N×2]
			if match {
				blockIDs := make([]uint16, 0, blockRefCount)
				for range blockRefCount {
					if pos+4 > len(data) {
						return nil
					}
					blockID := binary.LittleEndian.Uint16(data[pos:])
					spanCount := int(binary.LittleEndian.Uint16(data[pos+2:]))
					pos += 4
					if pos+spanCount*2 > len(data) {
						return nil
					}
					pos += spanCount * 2
					blockIDs = append(blockIDs, blockID)
				}
				return blockIDs
			}
			// Skip this trace's block entries without allocating.
			for range blockRefCount {
				if pos+4 > len(data) {
					return nil
				}
				spanCount := int(binary.LittleEndian.Uint16(data[pos+2:]))
				pos += 4
				if pos+spanCount*2 > len(data) {
					return nil
				}
				pos += spanCount * 2
			}
		} else {
			// v2: block_id[2] only — fixed stride per block ref.
			need := blockRefCount * 2
			if pos+need > len(data) {
				return nil
			}
			if match {
				blockIDs := make([]uint16, blockRefCount)
				for i := range blockRefCount {
					blockIDs[i] = binary.LittleEndian.Uint16(data[pos+i*2:])
				}
				return blockIDs
			}
			pos += need
		}
	}

	return nil
}

// ensureCompactHeaderParsed reads the compact section header, bloom filter, and block table
// without loading the trace index bytes. This is the phase-1 read for lean readers:
// it keeps the eager I/O at ~15 MB instead of the full ~700 MB compact section.
// The trace index byte range is recorded in compactParsed for later lazy fetching.
//
// Compact section layout (version 2):
//
//	magic[4] + version[1] + block_count[4]  =  9 bytes
//	bloom_bytes[4] + bloom_data[bloom_bytes]
//	block_table: block_count × 12 bytes
//	trace_index: fmt_version[1] + trace_count[4] + …  (rest of compact section)
//
// Compact section layout (version 3, v6 footer — split format):
//
//	compact header (raw, uncompressed): magic[4]+version[1]=3+block_count[4]+bloom_bytes[4]+bloom_data+block_table
//	compact traces (separate, snappy-compressed): snappy(fmt_version[1]+trace_count[4]+traces...)
//	Both sections are located by separate footer fields (compactOffset/compactLen and compactTracesOffset/compactTracesLen).
func (r *Reader) ensureCompactHeaderParsed() error {
	if r.compactParsed != nil {
		return nil
	}

	if r.compactLen == 0 {
		return fmt.Errorf("compact index: not present")
	}

	// V3 split format (footer V6): the compact header section is stored raw (uncompressed).
	// compactTracesOffset/compactTracesLen point to the snappy-compressed trace index section.
	if r.compactTracesLen > 0 {
		return r.ensureCompactHeaderParsedV3()
	}

	// V1/V2 legacy format: read prefix to determine version and bloom size.
	// Step 1: read the fixed 9-byte compact header to determine version and block_count.
	// For version 2 we also need the 4-byte bloom_bytes field, so read 13 bytes total.
	const prefixLen = 13 // magic[4] + version[1] + block_count[4] + bloom_bytes[4]
	var prefixBuf [prefixLen]byte
	readLen := uint64(prefixLen)
	if uint64(r.compactLen) < readLen {
		readLen = uint64(r.compactLen)
	}
	prefix, err := r.readRange(r.compactOffset, readLen, rw.DataTypeTraceBloomFilter)
	if err != nil {
		return fmt.Errorf("compact index: read header: %w", err)
	}
	if len(prefix) < 9 {
		return fmt.Errorf("compact index: too short (%d bytes)", len(prefix))
	}
	copy(prefixBuf[:len(prefix)], prefix)

	magic := binary.LittleEndian.Uint32(prefixBuf[0:])
	if magic != shared.CompactIndexMagic {
		return fmt.Errorf("compact index: bad magic 0x%08X", magic)
	}

	version := prefixBuf[4]
	if version != shared.CompactIndexVersion && version != shared.CompactIndexVersion2 {
		return fmt.Errorf("compact index: unsupported version %d", version)
	}

	blockCount := int(binary.LittleEndian.Uint32(prefixBuf[5:])) //nolint:gosec // validated below before use
	if blockCount > shared.MaxBlocks {
		return fmt.Errorf("compact index: block_count %d exceeds maximum %d", blockCount, shared.MaxBlocks)
	}

	// Step 2: read bloom filter + block table in one range read.
	// pos tracks offset within the compact section (not the file).
	pos := shared.CompactIndexHeaderSize

	var bloomBytes int
	if version == shared.CompactIndexVersion2 {
		if len(prefixBuf) < 13 {
			return fmt.Errorf("compact index: short for bloom_bytes")
		}
		bloomBytes = int(
			binary.LittleEndian.Uint32(prefixBuf[shared.CompactIndexHeaderSize:]),
		) //nolint:gosec // validated below before use
		if bloomBytes > shared.TraceIDBloomMaxBytes {
			return fmt.Errorf(
				"compact index: bloom_bytes %d exceeds maximum %d",
				bloomBytes, shared.TraceIDBloomMaxBytes,
			)
		}
		pos += 4 // account for the bloom_bytes field
	}

	// phase1Len = bloom_bytes_field(4) + bloom_data + block_table
	// For version 1 (no bloom): just the block_table portion after the 9-byte header.
	need := bloomBytes + blockCount*12
	cacheKey := r.fileID + "/compact-header"
	bodyData, err := r.cache.GetOrFetch(cacheKey, func() ([]byte, error) {
		bodyOffset := r.compactOffset + uint64( //nolint:gosec // pos is small, fits in uint64
			pos,
		)
		return r.readRange(bodyOffset, uint64(need), rw.DataTypeTraceBloomFilter) //nolint:gosec // need is bounded
	})
	if err != nil {
		return fmt.Errorf("compact index: read bloom+table: %w", err)
	}
	if len(bodyData) < need {
		return fmt.Errorf("compact index: short for bloom+table (need %d, got %d)", need, len(bodyData))
	}

	bodyPos := 0

	// Parse bloom filter for version 2.
	var traceIDBloom []byte
	if version == shared.CompactIndexVersion2 {
		traceIDBloom = make([]byte, bloomBytes)
		copy(traceIDBloom, bodyData[bodyPos:bodyPos+bloomBytes])
		bodyPos += bloomBytes
	}

	// Parse block table.
	blockTable := make([]compactBlockEntry, blockCount)
	for i := range blockCount {
		blockTable[i] = compactBlockEntry{
			fileOffset: binary.LittleEndian.Uint64(bodyData[bodyPos:]),
			fileLength: binary.LittleEndian.Uint32(bodyData[bodyPos+compactFileLengthOffset:]),
		}
		bodyPos += compactBlockEntrySize
	}

	// Compute where the trace index starts within the file and how many bytes it occupies.
	// The trace index immediately follows the block table in the compact section.
	traceIndexStart := uint64(pos) + uint64(need) //nolint:gosec // both small, fits in uint64
	traceIndexLen := uint64(r.compactLen) - traceIndexStart
	if traceIndexStart > uint64(r.compactLen) {
		return fmt.Errorf("compact index: computed trace_index start %d exceeds compactLen %d",
			traceIndexStart, r.compactLen)
	}
	if traceIndexLen < 5 {
		return fmt.Errorf("compact index: trace_index too short (%d bytes)", traceIndexLen)
	}

	r.compactParsed = &compactTraceIndex{
		blockTable:       blockTable,
		traceIDBloom:     traceIDBloom,
		traceIndexOffset: r.compactOffset + traceIndexStart,
		traceIndexLen:    traceIndexLen,
		// traceIndexRaw intentionally nil — fetched lazily in ensureTraceIndexRaw.
	}

	// Populate blockMetas from the compact block table so ReadBlockRaw,
	// CoalescedGroups, and other block-access methods work for lean readers.
	if len(r.blockMetas) == 0 {
		r.blockMetas = make([]shared.BlockMeta, len(blockTable))
		for i, entry := range blockTable {
			r.blockMetas[i] = shared.BlockMeta{
				Offset: entry.fileOffset,
				Length: uint64(entry.fileLength),
			}
		}
	}

	return nil
}

// ensureCompactHeaderParsedV3 reads the v3 split compact header section (raw, uncompressed).
// Called by ensureCompactHeaderParsed when compactTracesLen > 0 (footer V6).
// The compact header section contains magic+version(3)+block_count+bloom+block_table only.
// The trace index is stored separately and located via compactTracesOffset/compactTracesLen.
func (r *Reader) ensureCompactHeaderParsedV3() error {
	// Read the entire compact header section (raw, uncompressed, small — ~15 MB max).
	cacheKey := r.fileID + "/compact-header"
	data, err := r.cache.GetOrFetch(cacheKey, func() ([]byte, error) {
		return r.readRange(r.compactOffset, uint64(r.compactLen), rw.DataTypeTraceBloomFilter) //nolint:gosec
	})
	if err != nil {
		return fmt.Errorf("compact index v3: read header: %w", err)
	}
	if len(data) < 9 {
		return fmt.Errorf("compact index v3: too short (%d bytes)", len(data))
	}

	magic := binary.LittleEndian.Uint32(data[0:])
	if magic != shared.CompactIndexMagic {
		return fmt.Errorf("compact index v3: bad magic 0x%08X", magic)
	}

	version := data[4]
	if version != shared.CompactIndexVersion3 {
		return fmt.Errorf("compact index v3: unexpected version %d (expected %d)", version, shared.CompactIndexVersion3)
	}

	blockCount := int(binary.LittleEndian.Uint32(data[5:])) //nolint:gosec // validated below before use
	if blockCount > shared.MaxBlocks {
		return fmt.Errorf("compact index v3: block_count %d exceeds maximum %d", blockCount, shared.MaxBlocks)
	}

	pos := shared.CompactIndexHeaderSize

	// Parse bloom filter (always present in v3).
	if pos+4 > len(data) {
		return fmt.Errorf("compact index v3: short for bloom_bytes")
	}
	bloomBytes := int(binary.LittleEndian.Uint32(data[pos:])) //nolint:gosec
	pos += 4
	if bloomBytes > shared.TraceIDBloomMaxBytes {
		return fmt.Errorf(
			"compact index v3: bloom_bytes %d exceeds maximum %d",
			bloomBytes,
			shared.TraceIDBloomMaxBytes,
		)
	}
	if pos+bloomBytes > len(data) {
		return fmt.Errorf("compact index v3: short for bloom_data (need %d bytes)", bloomBytes)
	}
	traceIDBloom := make([]byte, bloomBytes)
	copy(traceIDBloom, data[pos:pos+bloomBytes])
	pos += bloomBytes

	// Parse block table.
	need := blockCount * compactBlockEntrySize
	if pos+need > len(data) {
		return fmt.Errorf("compact index v3: short for block_table (need %d)", need)
	}
	blockTable := make([]compactBlockEntry, blockCount)
	for i := range blockCount {
		blockTable[i] = compactBlockEntry{
			fileOffset: binary.LittleEndian.Uint64(data[pos:]),
			fileLength: binary.LittleEndian.Uint32(data[pos+compactFileLengthOffset:]),
		}
		pos += compactBlockEntrySize
	}

	r.compactParsed = &compactTraceIndex{
		blockTable:       blockTable,
		traceIDBloom:     traceIDBloom,
		traceIndexOffset: r.compactTracesOffset,
		traceIndexLen:    uint64(r.compactTracesLen),
		// traceIndexRaw intentionally nil — fetched lazily in ensureTraceIndexRaw.
	}

	// Populate blockMetas from the compact block table.
	if len(r.blockMetas) == 0 {
		r.blockMetas = make([]shared.BlockMeta, len(blockTable))
		for i, entry := range blockTable {
			r.blockMetas[i] = shared.BlockMeta{
				Offset: entry.fileOffset,
				Length: uint64(entry.fileLength),
			}
		}
	}

	return nil
}

// ensureTraceIndexRaw fetches the raw trace-index bytes for a lean reader (lazy, phase 2).
// Called only after a bloom-filter hit — for the vast majority of lookups (bloom miss)
// this is never invoked, keeping peak RSS at ~15 MB instead of ~700 MB per file open.
//
// For V14 files, the trace index is extracted from the full SectionTraceIndex blob via
// splitV14CompactSection (see isV14TraceSection on compactTraceIndex).
// For v3 split format files (footer V6), the trace index is snappy-compressed at
// compactTracesOffset/compactTracesLen and must be decompressed before use.
// For v1/v2 format files, the trace index bytes are raw (uncompressed).
//
// Thread-safe: uses sync.Once; subsequent calls return immediately with cached result.
// A fetch error is stored in traceIndexFetchErr and returned to callers.
func (r *Reader) ensureTraceIndexRaw() error {
	if r.compactParsed == nil {
		return fmt.Errorf("compact index: not parsed")
	}

	// Fast path: already populated (either by ensureCompactIndexParsed or a prior lazy fetch).
	if r.compactParsed.traceIndexRaw != nil {
		return nil
	}

	// Also fast path if no lazy-load location is recorded (V3/V4 path) and not V14.
	if r.compactParsed.traceIndexLen == 0 && !r.compactParsed.isV14TraceSection {
		return fmt.Errorf("compact index: trace_index location not recorded")
	}

	r.compactParsed.traceIndexOnce.Do(func() {
		cacheKey := r.fileID + "/compact-trace-index"
		data, err := r.cache.GetOrFetch(cacheKey, func() ([]byte, error) {
			if r.compactParsed.isV14TraceSection {
				// V14: re-read the full trace section and extract trace index bytes.
				raw, readErr := r.readV14Section(shared.SectionTraceIndex)
				if readErr != nil {
					return nil, readErr
				}
				_, traceIdx, splitErr := splitV14CompactSection(raw)
				if splitErr != nil {
					return nil, splitErr
				}
				return append([]byte(nil), traceIdx...), nil
			}
			// V3/V4: read from known file offset.
			return r.readRange(
				r.compactParsed.traceIndexOffset,
				r.compactParsed.traceIndexLen,
				rw.DataTypeTraceBloomFilter,
			)
		})
		if err != nil {
			r.compactParsed.traceIndexFetchErr = fmt.Errorf("compact index: trace_index: read: %w", err)
			return
		}
		if len(data) == 0 {
			r.compactParsed.traceIndexFetchErr = fmt.Errorf("compact index: trace_index: empty data")
			return
		}

		// V3 split format: trace index is snappy-compressed.
		// V1/V2 format and V14 (already extracted above): trace index bytes are raw (uncompressed).
		var rawData []byte
		if r.compactTracesLen > 0 {
			// V3: snappy-decompress the compact traces section.
			// SPEC-ROOT-012: use decodeBoundedSnappy — checks snappy.DecodedLen BEFORE
			// allocating, preventing decompression-bomb OOM from malicious frame headers.
			decoded, decErr := decodeBoundedSnappy(data)
			if decErr != nil {
				r.compactParsed.traceIndexFetchErr = fmt.Errorf("compact index: trace_index: snappy decode: %w", decErr)
				return
			}
			rawData = decoded
		} else {
			// V1/V2/V14: raw bytes (V14 bytes were already extracted by splitV14CompactSection).
			rawData = data
		}

		if len(rawData) < 5 {
			r.compactParsed.traceIndexFetchErr = fmt.Errorf(
				"compact index: trace_index: data too short (%d bytes)",
				len(rawData),
			)
			return
		}
		fmtVer := rawData[0]
		if fmtVer != shared.TraceIndexFmtVersion && fmtVer != shared.TraceIndexFmtVersion2 {
			r.compactParsed.traceIndexFetchErr = fmt.Errorf(
				"compact index: trace_index: unsupported fmt_version %d",
				fmtVer,
			)
			return
		}
		// Copy bytes so the slice is owned by this reader, not the cache buffer.
		r.compactParsed.traceIndexRaw = append([]byte(nil), rawData...)
	})

	return r.compactParsed.traceIndexFetchErr
}

// ensureCompactIndexParsed lazily parses the compact trace index.
// Supports version 1 (no bloom), version 2 (with trace ID bloom filter), and
// version 3 (v6 footer split format: bloom header uncompressed, trace index snappy-compressed).
func (r *Reader) ensureCompactIndexParsed() error {
	if r.compactParsed != nil {
		return nil
	}

	if r.compactLen == 0 {
		return fmt.Errorf("compact index: not present")
	}

	// V3 split format (footer V6): parse the compact header section then eagerly fetch
	// and decompress the compact traces section (for full readers that need trace index immediately).
	if r.compactTracesLen > 0 {
		if err := r.ensureCompactHeaderParsedV3(); err != nil {
			return err
		}
		// Eagerly load the trace index so BlocksForTraceID works on the first call.
		return r.ensureTraceIndexRaw()
	}

	// V1/V2 legacy format.
	cacheKey := r.fileID + "/compact"

	data, err := r.cache.GetOrFetch(cacheKey, func() ([]byte, error) {
		return r.readRange(r.compactOffset, uint64(r.compactLen), rw.DataTypeTraceBloomFilter)
	})
	if err != nil {
		return fmt.Errorf("compact index: read: %w", err)
	}

	if len(data) < 9 {
		return fmt.Errorf("compact index: too short (%d bytes)", len(data))
	}

	magic := binary.LittleEndian.Uint32(data[0:])
	if magic != shared.CompactIndexMagic {
		return fmt.Errorf("compact index: bad magic 0x%08X", magic)
	}

	version := data[4]
	if version != shared.CompactIndexVersion && version != shared.CompactIndexVersion2 {
		return fmt.Errorf("compact index: unsupported version %d", version)
	}

	blockCount := int(binary.LittleEndian.Uint32(data[5:])) //nolint:gosec // validated below before use
	if blockCount > shared.MaxBlocks {
		return fmt.Errorf("compact index: block_count %d exceeds maximum %d", blockCount, shared.MaxBlocks)
	}
	pos := shared.CompactIndexHeaderSize

	// Version 2: parse trace ID bloom filter before the block table.
	var traceIDBloom []byte
	if version == shared.CompactIndexVersion2 {
		if pos+4 > len(data) {
			return fmt.Errorf("compact index: short for bloom_bytes")
		}
		bloomBytes := int(binary.LittleEndian.Uint32(data[pos:])) //nolint:gosec // validated below before use
		pos += 4
		if bloomBytes > shared.TraceIDBloomMaxBytes {
			return fmt.Errorf(
				"compact index: bloom_bytes %d exceeds maximum %d",
				bloomBytes, shared.TraceIDBloomMaxBytes,
			)
		}
		if pos+bloomBytes > len(data) {
			return fmt.Errorf("compact index: short for bloom_data (need %d bytes)", bloomBytes)
		}
		traceIDBloom = make([]byte, bloomBytes)
		copy(traceIDBloom, data[pos:pos+bloomBytes])
		pos += bloomBytes
	}

	// Parse block_table: block_count × 12 bytes (file_offset[8] + file_length[4]).
	need := blockCount * compactBlockEntrySize
	if pos+need > len(data) {
		return fmt.Errorf("compact index: short for block_table (need %d)", need)
	}

	blockTable := make([]compactBlockEntry, blockCount)
	for i := range blockCount {
		blockTable[i] = compactBlockEntry{
			fileOffset: binary.LittleEndian.Uint64(data[pos:]),
			fileLength: binary.LittleEndian.Uint32(data[pos+compactFileLengthOffset:]),
		}
		pos += compactBlockEntrySize
	}

	// NOTE-PERF-COMPACT: store raw trace-index bytes in-place instead of parsing into a map.
	// scanTraceIndexRaw scans the bytes linearly on each lookup, eliminating the O(traceCount)
	// allocations from parseTraceBlockIndex (was #1 production allocator: 36.27% alloc_objects).
	// The sub-slice is safe: data is owned by the filecache and outlives the Reader.
	//
	// Validate the trace-index section header before storing raw bytes so that corrupt or
	// unsupported-version indexes fail fast here rather than silently returning no results.
	if pos+5 > len(data) {
		return fmt.Errorf("compact index: trace_index: data too short")
	}
	traceIdxFmtVer := data[pos]
	if traceIdxFmtVer != shared.TraceIndexFmtVersion && traceIdxFmtVer != shared.TraceIndexFmtVersion2 {
		return fmt.Errorf("compact index: trace_index: unsupported fmt_version %d", traceIdxFmtVer)
	}

	r.compactParsed = &compactTraceIndex{
		blockTable:    blockTable,
		traceIndexRaw: append([]byte(nil), data[pos:]...),
		traceIDBloom:  traceIDBloom,
	}

	// Populate blockMetas from the compact block table so ReadBlockRaw,
	// CoalescedGroups, and other block-access methods work for lean readers.
	if len(r.blockMetas) == 0 {
		r.blockMetas = make([]shared.BlockMeta, len(blockTable))
		for i, entry := range blockTable {
			r.blockMetas[i] = shared.BlockMeta{
				Offset: entry.fileOffset,
				Length: uint64(entry.fileLength),
			}
		}
	}

	return nil
}
