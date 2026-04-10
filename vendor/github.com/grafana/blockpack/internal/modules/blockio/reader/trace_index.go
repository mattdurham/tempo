package reader

// NOTE: Any changes to this file must be reflected in the corresponding specs.md or NOTES.md.

import (
	"encoding/binary"
	"fmt"

	"github.com/grafana/blockpack/internal/modules/blockio/shared"
	"github.com/grafana/blockpack/internal/modules/rw"
)

// compactBlockEntry holds file location for one block as stored in the compact block table.
type compactBlockEntry struct {
	fileOffset uint64
	fileLength uint32
}

// BlocksForTraceID returns block indices containing the given trace ID.
// Uses the compact index as a fast path when available. If the compact index
// returns nil (bloom rejected the ID or ID not in hash map), falls through to
// the full trace index — which is the authoritative answer for full readers.
// For lean readers (NewLeanReaderFromProvider), r.traceIndex is always empty so
// the compact index result is final.
func (r *Reader) BlocksForTraceID(traceID [16]byte) []int {
	if r.compactLen > 0 {
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

	// NOTE-36 / SPEC-10.2: bloom filter check before hash map — fast path for absent trace IDs.
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
func (r *Reader) ensureCompactHeaderParsed() error {
	if r.compactParsed != nil {
		return nil
	}

	if r.compactLen == 0 {
		return fmt.Errorf("compact index: not present")
	}

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
	pos := 9

	var bloomBytes int
	if version == shared.CompactIndexVersion2 {
		if len(prefixBuf) < 13 {
			return fmt.Errorf("compact index: short for bloom_bytes")
		}
		bloomBytes = int(binary.LittleEndian.Uint32(prefixBuf[9:])) //nolint:gosec // validated below before use
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
		bodyOffset := r.compactOffset + uint64(pos) //nolint:gosec // pos is small, fits in uint64
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
			fileLength: binary.LittleEndian.Uint32(bodyData[bodyPos+8:]),
		}
		bodyPos += 12
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

// ensureTraceIndexRaw fetches the raw trace-index bytes for a lean reader (lazy, phase 2).
// Called only after a bloom-filter hit — for the vast majority of lookups (bloom miss)
// this is never invoked, keeping peak RSS at ~15 MB instead of ~700 MB per file open.
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

	// Also fast path if no lazy-load location is recorded (shouldn't happen in normal flow).
	if r.compactParsed.traceIndexLen == 0 {
		return fmt.Errorf("compact index: trace_index location not recorded")
	}

	r.compactParsed.traceIndexOnce.Do(func() {
		cacheKey := r.fileID + "/compact-trace-index"
		data, err := r.cache.GetOrFetch(cacheKey, func() ([]byte, error) {
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
		if len(data) < 5 {
			r.compactParsed.traceIndexFetchErr = fmt.Errorf("compact index: trace_index: data too short (%d bytes)", len(data))
			return
		}
		fmtVer := data[0]
		if fmtVer != shared.TraceIndexFmtVersion && fmtVer != shared.TraceIndexFmtVersion2 {
			r.compactParsed.traceIndexFetchErr = fmt.Errorf("compact index: trace_index: unsupported fmt_version %d", fmtVer)
			return
		}
		// Preserve the r20 fix: copy bytes so the slice is owned by this reader, not the cache buffer.
		r.compactParsed.traceIndexRaw = append([]byte(nil), data...)
	})

	return r.compactParsed.traceIndexFetchErr
}

// ensureCompactIndexParsed lazily parses the compact trace index.
// Supports version 1 (no bloom) and version 2 (with trace ID bloom filter).
func (r *Reader) ensureCompactIndexParsed() error {
	if r.compactParsed != nil {
		return nil
	}

	if r.compactLen == 0 {
		return fmt.Errorf("compact index: not present")
	}

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
	pos := 9

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
	need := blockCount * 12
	if pos+need > len(data) {
		return fmt.Errorf("compact index: short for block_table (need %d)", need)
	}

	blockTable := make([]compactBlockEntry, blockCount)
	for i := range blockCount {
		blockTable[i] = compactBlockEntry{
			fileOffset: binary.LittleEndian.Uint64(data[pos:]),
			fileLength: binary.LittleEndian.Uint32(data[pos+8:]),
		}
		pos += 12
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
