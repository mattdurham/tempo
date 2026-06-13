package reader

// NOTE: Any changes to this file must be reflected in the corresponding specs.md or NOTES.md.

import (
	"encoding/binary"
	"fmt"
	"strconv"

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
func splitV14CompactSection(data []byte) (header, traceIndex []byte, err error) {
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

	blockIDs := r.compactParsed.scanTraceIndexRaw(r.fileID, traceID)
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

// traceEntryStride returns the byte length of the trace entry that begins at data[pos]
// (trace_id[16] + block_ref_count[2] + per-ref payload), and whether the entry is fully
// in bounds. fmtVersion selects the per-ref payload layout (v1: block_id[2]+span_count[2]+
// span_indices[N×2]; v2: block_id[2]). Used by both the sparse-index builder and the scan.
func traceEntryStride(data []byte, pos int, fmtVersion uint8) (stride int, ok bool) {
	if pos+18 > len(data) {
		return 0, false
	}
	blockRefCount := int(binary.LittleEndian.Uint16(data[pos+16:]))
	p := pos + 18
	if fmtVersion == shared.TraceIndexFmtVersion {
		for range blockRefCount {
			if p+4 > len(data) {
				return 0, false
			}
			spanCount := int(binary.LittleEndian.Uint16(data[p+2:]))
			p += 4 + spanCount*2
		}
		if p > len(data) {
			return 0, false
		}
	} else {
		p += blockRefCount * 2
		if p > len(data) {
			return 0, false
		}
	}
	return p - pos, true
}

// decodeTraceEntryBlocks decodes the block-ID list for the trace entry that begins at
// data[pos]. fmtVersion selects the per-ref payload layout. Allocates exactly one
// []uint16. Caller has already confirmed the trace ID at pos matches.
func decodeTraceEntryBlocks(data []byte, pos int, fmtVersion uint8) []uint16 {
	blockRefCount := int(binary.LittleEndian.Uint16(data[pos+16:]))
	p := pos + 18
	blockIDs := make([]uint16, 0, blockRefCount)
	if fmtVersion == shared.TraceIndexFmtVersion {
		for range blockRefCount {
			if p+4 > len(data) {
				return blockIDs
			}
			blockIDs = append(blockIDs, binary.LittleEndian.Uint16(data[p:]))
			spanCount := int(binary.LittleEndian.Uint16(data[p+2:]))
			p += 4 + spanCount*2
		}
		return blockIDs
	}
	for range blockRefCount {
		if p+2 > len(data) {
			return blockIDs
		}
		blockIDs = append(blockIDs, binary.LittleEndian.Uint16(data[p:]))
		p += 2
	}
	return blockIDs
}

// scanTraceIndexRaw searches the raw trace-index bytes for traceID without building a map.
// Supports fmt_version 1 (v1: with per-block span indices) and 2 (v2: block IDs only).
// Returns the block IDs for the matching trace, or nil if not found.
// Zero allocations on miss; one small []uint16 allocation on hit.
//
// NOTE-260: the writer sorts trace entries ascending by trace ID, so this builds a sparse
// offset index (traceIdxSamples) on first call and binary-searches it to bound the linear
// binary-search per lookup -- O(log n) ID compares with zero traceEntryStride calls
// instead of O(n). Builds the sparse index once per compactTraceIndex under a sync.Once.
// NOTE-PERF-COMPACT: still does NOT parse the table into a map; the sparse index holds at
// most n/stride samples and all block-list allocation is deferred to a confirmed hit.
func (ci *compactTraceIndex) scanTraceIndexRaw(fileID string, traceID [16]byte) []uint16 {
	data := ci.traceIndexRaw
	if len(data) < 5 {
		return nil
	}

	fmtVersion := data[0]
	if fmtVersion != shared.TraceIndexFmtVersion && fmtVersion != shared.TraceIndexFmtVersion2 {
		return nil
	}

	ci.ensureTraceIdxSamples(fileID, fmtVersion)

	// NOTE-261: pre-decode the 16-byte target as a big-endian uint64 pair.
	targetHi := binary.BigEndian.Uint64(traceID[0:8])
	targetLo := binary.BigEndian.Uint64(traceID[8:16])

	// NOTE-267: with a usable dense offset index, binary-search the entry offsets
	// directly. Each probe reads the candidate trace ID from data at its stored
	// offset -- no traceEntryStride call. The dense index walks the variable-stride
	// layout exactly once, at build time, replacing the per-lookup stride loop with
	// O(log n) ID compares (traceEntryStride was ~2% querier self-time).
	if ci.traceIdxSampleOK && len(ci.traceIdxOffsets) > 0 {
		offs := ci.traceIdxOffsets
		lo, hi := 0, len(offs)
		for lo < hi {
			mid := int(uint(lo+hi) >> 1)
			pos := int(offs[mid])
			entryHi := binary.BigEndian.Uint64(data[pos : pos+8])
			entryLo := binary.BigEndian.Uint64(data[pos+8 : pos+16])
			switch {
			case entryHi < targetHi:
				lo = mid + 1
			case entryHi > targetHi:
				hi = mid
			case entryLo == targetLo:
				return decodeTraceEntryBlocks(data, pos, fmtVersion)
			case entryLo < targetLo:
				lo = mid + 1
			default:
				hi = mid
			}
		}
		return nil
	}

	// Fallback: dense index unavailable (malformed build). Linear scan from the start.
	pos := 5
	for pos < len(data) {
		stride, ok := traceEntryStride(data, pos, fmtVersion)
		if !ok {
			return nil
		}
		entryHi := binary.BigEndian.Uint64(data[pos : pos+8])
		entryLo := binary.BigEndian.Uint64(data[pos+8 : pos+16])
		if entryHi == targetHi {
			if entryLo == targetLo {
				return decodeTraceEntryBlocks(data, pos, fmtVersion)
			}
			if targetLo < entryLo {
				return nil
			}
		} else if targetHi < entryHi {
			return nil
		}
		pos += stride
	}

	return nil
}

// ensureTraceIdxSamples builds the sparse offset index (traceIdxSamples) once per
// compactTraceIndex. On any malformed entry it leaves traceIdxSampleOK false so the caller
// falls back to a full linear scan from the start of the table.
//
// NOTE-265: the built index is also stashed in the process-level parsedTraceSparseCache
// keyed by fileID+length, so a subsequent Reader for the same on-disk trace-index section
// reuses the result instead of re-walking the whole table. A Reader (and thus its
// compactTraceIndex) is created fresh per query, so the O(traceCount) walk — calling
// traceEntryStride on every entry — was rerun on every bloom-hit lookup against the same
// section. The 2026-06-09/13 querier profiles showed traceEntryStride at ~1.77% and this
// build closure at ~0.99% self-time, all of it this repeated rebuild. The samples reference
// only byte OFFSETS into traceIndexRaw plus a copied [16]byte trace ID (no aliasing of the
// raw bytes), and the section's layout is fully determined by its length, so the result is
// safe to share across Readers for the same fileID+length. A failed/malformed build is
// cached too (ok=false) so it is not retried per query.
func (ci *compactTraceIndex) ensureTraceIdxSamples(fileID string, fmtVersion uint8) {
	ci.traceIdxIndexOnce.Do(func() {
		data := ci.traceIndexRaw

		var cacheKey string
		if fileID != "" {
			cacheKey = fileID + "/tracesparse/" + strconv.Itoa(len(data))
			if cached := parsedTraceSparseCache.Get(cacheKey); cached != nil {
				ci.traceIdxOffsets = cached.offsets
				ci.traceIdxSampleOK = cached.ok
				return
			}
		}

		offsets, ok := buildTraceIdxSamples(data, fmtVersion)
		ci.traceIdxOffsets = offsets
		ci.traceIdxSampleOK = ok

		if cacheKey != "" {
			_ = parsedTraceSparseCache.Put(cacheKey, &traceSparseIndex{offsets: offsets, ok: ok})
		}
	})
}

// buildTraceIdxSamples walks the raw trace-index table once, recording the byte offset of
// EVERY trace entry (NOTE-267: a dense offset index). Returns ok=false on a malformed entry
// (the caller then falls back to a full linear scan). The returned offsets are plain int32
// byte positions into data, so they do not alias the raw bytes. Offsets fit in int32: the
// trace-index section is at most ~15 MB (well under 2^31).
func buildTraceIdxSamples(data []byte, fmtVersion uint8) (offsets []int32, ok bool) {
	traceCount := int(binary.LittleEndian.Uint32(data[1:]))
	if traceCount == 0 {
		return nil, true
	}
	// NOTE-268: pre-extend the offsets slice to exactly traceCount and store by index
	// instead of append, and inline the v2 (block-IDs-only) per-entry stride directly
	// into the walk loop rather than calling the non-inlinable traceEntryStride per entry.
	// This build runs once per distinct trace-index section (amortized across Readers by
	// the NOTE-265 process cache) but was still ~2.7% combined querier self-time on the
	// 2026-06-13 profile (buildTraceIdxSamples ~0.91% + the traceEntryStride it called per
	// entry ~1.79%) — a real CPU sink on bloom-hit trace-resolution queries (Q5/Q7/Q9/M6/M9)
	// whenever a section is first touched. The v2 layout (the current writer format) has a
	// fixed-shape entry — trace_id[16] + block_ref_count[2] + block_ref_count×block_id[2] —
	// so its stride is 18+blockRefCount*2 with a single bounds check per entry, with no
	// function-call overhead and the offsets store discharged from the loop bound. v1
	// (legacy variable-stride entries with per-block span indices) keeps the generic
	// traceEntryStride call.
	offsets = make([]int32, traceCount)
	pos := 5
	if fmtVersion == shared.TraceIndexFmtVersion2 {
		n := len(data)
		for i := range offsets {
			// Header (trace_id[16] + block_ref_count[2]) must be fully in bounds before
			// we read block_ref_count, and the entry's refs (block_ref_count×block_id[2])
			// must not overrun the section — the same validation traceEntryStride performed
			// per entry. Checking the full entry extent here keeps every recorded offset
			// pointing at a real, in-bounds entry header (the binary search relies on each
			// offset's 16-byte trace ID being readable and the entries staying sorted).
			if pos+18 > n {
				return nil, false // malformed
			}
			offsets[i] = int32(pos) //nolint:gosec // pos < len(data) <= ~15 MB < 2^31
			blockRefCount := int(binary.LittleEndian.Uint16(data[pos+16:]))
			pos += 18 + blockRefCount*2
			if pos > n {
				return nil, false // entry's refs overran the section
			}
		}
		return offsets, true
	}
	for i := range offsets {
		if pos+18 > len(data) {
			return nil, false // malformed
		}
		offsets[i] = int32(pos) //nolint:gosec // pos < len(data) <= ~15 MB < 2^31
		stride, strideOK := traceEntryStride(data, pos, fmtVersion)
		if !strideOK {
			return nil, false // malformed
		}
		pos += stride
	}
	return offsets, true
}

// ensureCompactHeaderParsedV3 reads the v3 split compact header section (raw, uncompressed).
// Called by ensureCompactHeaderParsed when compactTracesLen > 0 (footer V6).
// The compact header section contains magic+version(3)+block_count+bloom+block_table only.
// The trace index is stored separately and located via compactTracesOffset/compactTracesLen.
func (r *Reader) ensureCompactHeaderParsedV3() error {
	// Read the entire compact header section (raw, uncompressed, small — ~15 MB max).
	data, err := r.cache.GetOrFetchBloom(r.fileID, false, func() ([]byte, error) {
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
		data, err := r.cache.GetOrFetchTraceIndex(r.fileID, false, func() ([]byte, error) {
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
		// NOTE-257: hold the trace-index bytes in place rather than copying into a fresh
		// allocation. rawData is either fresh snappy output (V3 path: decodeBoundedSnappy
		// allocates and owns it) or the cache-owned blob from GetOrFetchTraceIndex
		// (V1/V2/V14), whose fetch closure already produced an independent copy. Both are
		// immutable for the lifetime this reader needs — the cache never mutates a stored
		// blob (MemoryCache returns its backing array under a no-modify contract and only
		// drops the reference on eviction) — and scanTraceIndexRaw only READS the bytes,
		// so aliasing is safe and saves a full memmove of the tens-of-MB trace index.
		r.compactParsed.traceIndexRaw = rawData
	})

	return r.compactParsed.traceIndexFetchErr
}

// ensureCompactIndexParsed lazily parses the compact trace index.
// Supports version 1 (no bloom), version 2 (with trace ID bloom filter), and
// version 3 (v6 footer split format: bloom header uncompressed, trace index snappy-compressed).
// SPEC-ROOT-001: guarded by compactParsedOnce to prevent concurrent write to r.compactParsed.
func (r *Reader) ensureCompactIndexParsed() error {
	if r.compactLen == 0 {
		return fmt.Errorf("compact index: not present")
	}
	r.compactParsedOnce.Do(func() {
		r.compactParsedErr = r.initCompactIndex()
	})
	return r.compactParsedErr
}

// initCompactIndex performs the actual initialization of r.compactParsed.
// Must only be called from inside r.compactParsedOnce.Do.
func (r *Reader) initCompactIndex() error {
	// No-op if already initialized by the constructor path (ensureCompactHeaderParsed,
	// parseCompactIndexBytesV14Header, or ensureCompactHeaderParsedV3).
	if r.compactParsed != nil {
		return nil
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
	data, err := r.cache.GetOrFetchTraceIndex(r.fileID, true, func() ([]byte, error) {
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
