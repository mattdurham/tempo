package valueindex

// NOTE: Any changes to this file must be reflected in the corresponding SPECS.md or NOTES.md.

import (
	"encoding/binary"
	"fmt"

	"github.com/grafana/blockpack/internal/modules/blockio/shared"
)

// QueryResult is one matching entry returned by a Lookup call.
type QueryResult struct {
	SourceRef string
	Value     []byte // canonical-encoded vi:value
	TimeSec   uint64
	TraceID   [16]byte
	BlockID   uint32   // v1: zero-based block index within SourceRef (NOTE-VI-014)
	BlockRef  BlockRef // v2+: direct page-addressed block reference (NOTE-VI-027); zero for v1 files
	SpanID    [8]byte  // v4+: span identity for direct span lookup; zero for v1-v3 files
	RowIdx    uint16   // v4+: row index within block for O(1) span access; zero for v1-v3 files
}

// Reader reads a value index file from an in-memory byte slice.
// After OpenReader succeeds, Meta, HashIndex, and the chunk directory are loaded;
// chunk data is decoded lazily during Lookup.
type Reader struct {
	strTable   *StringTable // v3 files: SourceRef dedup table (NOTE-VI-028)
	data       []byte
	hashIdx    []HashEntry
	chunkDir   []ChunkDirEntry
	meta       Meta
	chunkStart int   // byte offset within data where the raw chunk bytes begin
	entriesVer uint8 // VINX entries version (1=BlockID, 2=BlockRef, 3=BlockRef+strTable)
}

// OpenReader parses the footer, VIMT, VHIX, and VINX chunk directory of a value index file.
// Returns an error if the footer magic is wrong or any section is truncated.
func OpenReader(data []byte) (*Reader, error) {
	if len(data) < shared.ValueIndexFooterSize {
		return nil, fmt.Errorf(
			"valueindex: file too short (%d bytes, need at least %d)",
			len(data),
			shared.ValueIndexFooterSize,
		)
	}

	// Parse footer (last 32 bytes).
	footerStart := len(data) - shared.ValueIndexFooterSize
	footer := data[footerStart:]
	magic := binary.LittleEndian.Uint32(footer[0:4])
	if magic != shared.ValueIndexFileMagic {
		return nil, fmt.Errorf(
			"valueindex: wrong file magic 0x%08X (expected 0x%08X)",
			magic,
			shared.ValueIndexFileMagic,
		)
	}
	if ver := footer[4]; ver != shared.ValueIndexFileVersion {
		return nil, fmt.Errorf("valueindex: unsupported file version %d", ver)
	}
	// Read offsets; validate they are within the int range before converting.
	vimtOffU := binary.LittleEndian.Uint64(footer[8:16])
	vhixOffU := binary.LittleEndian.Uint64(footer[16:24])
	vinxOffU := binary.LittleEndian.Uint64(footer[24:32])
	// NOTE-LINT-407: explicit negative guard before the int → uint64 cast
	// (gosec G115). footerStart = len(data) - ValueIndexFooterSize; the length
	// check above guarantees len(data) >= ValueIndexFooterSize, so this is
	// defense-in-depth against a future refactor breaking that invariant.
	if footerStart < 0 {
		return nil, fmt.Errorf("valueindex: negative footer start %d", footerStart)
	}
	fsU := uint64(footerStart)
	if vimtOffU > fsU || vhixOffU > fsU || vinxOffU > fsU {
		return nil, fmt.Errorf("valueindex: footer offsets exceed file size (%d bytes)", len(data))
	}
	vimtOff := int(vimtOffU) //nolint:gosec // bounds checked above
	vhixOff := int(vhixOffU) //nolint:gosec // bounds checked above
	vinxOff := int(vinxOffU) //nolint:gosec // bounds checked above

	// Validate relative ordering of offsets.
	if vhixOff < vimtOff || vinxOff < vhixOff {
		return nil, fmt.Errorf("valueindex: footer offsets misordered (vimt=%d vhix=%d vinx=%d)",
			vimtOff, vhixOff, vinxOff)
	}

	// Decode VIMT section.
	meta, err := DecodeMeta(data[vimtOff:vhixOff])
	if err != nil {
		return nil, fmt.Errorf("valueindex: VIMT decode: %w", err)
	}

	// Decode VHIX section.
	hashIdx, err := DecodeHashIndex(data[vhixOff:vinxOff])
	if err != nil {
		return nil, fmt.Errorf("valueindex: VHIX decode: %w", err)
	}

	// Decode VINX section header + directory.
	chunkDir, chunkDataStart, entriesVer, strTable, err := decodeVINXSection(data, vinxOff, footerStart)
	if err != nil {
		return nil, fmt.Errorf("valueindex: VINX decode: %w", err)
	}

	return &Reader{
		data:       data,
		meta:       meta,
		hashIdx:    hashIdx,
		strTable:   strTable,
		chunkDir:   chunkDir,
		chunkStart: chunkDataStart,
		entriesVer: entriesVer,
	}, nil
}

// Meta returns the decoded VIMT section for this file.
func (r *Reader) Meta() Meta { return r.meta }

// Lookup returns all entries matching pred and timeRange.
// pred == nil matches all values. timeRange == nil matches all times.
func (r *Reader) Lookup(pred Predicate, timeRange *[2]uint64) ([]QueryResult, error) {
	// Fast time-range skip at file level.
	if timeRange != nil {
		if r.meta.WallMaxTS < timeRange[0] || r.meta.WallMinTS > timeRange[1] {
			return nil, nil
		}
	}

	// Determine time bounds for chunk filtering.
	minTS := uint64(0)
	maxTS := ^uint64(0)
	if timeRange != nil {
		minTS = timeRange[0]
		maxTS = timeRange[1]
	}

	// Determine which chunks to scan.
	// For all-chunks (no pred, or non-equality pred), scan all chunks in [minTS, maxTS].
	chunkData := r.data[r.chunkStart : r.chunkStart+r.chunkDataLen()]
	entries, err := DecodeChunkRangeWithTable(chunkData, r.chunkDir, minTS, maxTS, r.entriesVer, r.strTable)
	if err != nil {
		return nil, fmt.Errorf("valueindex: Lookup decode: %w", err)
	}

	// Apply predicate filter.
	results := make([]QueryResult, 0, len(entries))
	for i := range entries {
		e := &entries[i]
		if pred != nil && !pred.Match(e.Value) {
			continue
		}
		results = append(results, QueryResult{
			Value:     e.Value,
			TraceID:   e.TraceID,
			SourceRef: e.SourceRef,
			TimeSec:   e.TimeSec,
			BlockID:   e.BlockID,
			BlockRef:  e.BlockRef,
			SpanID:    e.SpanID,
			RowIdx:    e.RowIdx,
		})
	}
	return results, nil
}

// chunkDataLen returns the length of the raw chunk data region.
func (r *Reader) chunkDataLen() int {
	// The chunk data starts at chunkStart and ends before the KLL section.
	// We know the total chunk bytes by summing up CompOff+CompLen from the last chunk dir entry.
	if len(r.chunkDir) == 0 {
		return 0
	}
	last := r.chunkDir[len(r.chunkDir)-1]
	return int(last.CompOff) + int(last.CompLen)
}

// decodeVINXSection parses the VINX header and chunk directory from data[vinxOff:footerStart].
// Returns the chunk directory, the absolute byte offset of chunk data, the version byte,
// and the string table (non-nil for v3 files, nil for v1/v2).
// Supports v1 (BlockID), v2 (BlockRef), and v3 (BlockRef + string table).
func decodeVINXSection(data []byte, vinxOff, footerStart int) ([]ChunkDirEntry, int, uint8, *StringTable, error) {
	const vinxHeaderSize = 28
	if vinxOff+vinxHeaderSize > footerStart {
		return nil, 0, 0, nil, fmt.Errorf(
			"valueindex: VINX section truncated at header (off=%d filesize=%d)",
			vinxOff,
			len(data),
		)
	}
	vinx := data[vinxOff:]
	if magic := binary.LittleEndian.Uint32(vinx[0:4]); magic != shared.ValueIndexEntriesMagic {
		return nil, 0, 0, nil, fmt.Errorf("valueindex: VINX wrong magic 0x%08X", magic)
	}
	ver := vinx[4]
	switch ver {
	case shared.ValueIndexEntriesVersionV1, shared.ValueIndexEntriesVersion,
		shared.ValueIndexEntriesVersionV3, shared.ValueIndexEntriesVersionV4:
		// supported
	default:
		return nil, 0, 0, nil, fmt.Errorf("valueindex: VINX unsupported version %d", ver)
	}
	chunkCount := int(binary.LittleEndian.Uint32(vinx[12:16]))
	// v3: read string table length from header[18:22].
	var strTableLen int
	if ver == shared.ValueIndexEntriesVersionV3 || ver == shared.ValueIndexEntriesVersionV4 {
		strTableLen = int(binary.LittleEndian.Uint32(vinx[18:22]))
	}
	// Select dir entry size based on version.
	dirEntrySize := chunkDirEntrySize
	if ver == shared.ValueIndexEntriesVersionV3 || ver == shared.ValueIndexEntriesVersionV4 {
		dirEntrySize = chunkDirEntryV3Size
	}
	ifneeded := vinxHeaderSize + strTableLen + chunkCount*dirEntrySize
	if vinxOff+ifneeded > footerStart {
		return nil, 0, 0, nil, fmt.Errorf(
			"valueindex: VINX section truncated (%d chunks, str_table=%d)",
			chunkCount,
			strTableLen,
		)
	}
	// v3: decode string table.
	var table *StringTable
	pos := vinxOff + vinxHeaderSize
	if strTableLen > 0 {
		var consumed int
		var decErr error
		table, consumed, decErr = DecodeStringTable(data[pos : pos+strTableLen])
		if decErr != nil {
			return nil, 0, 0, nil, fmt.Errorf("valueindex: VINX string table: %w", decErr)
		}
		_ = consumed
		pos += strTableLen
	}
	dir := make([]ChunkDirEntry, chunkCount)
	for i := range dir {
		var de ChunkDirEntry
		copy(de.MinValue[:], data[pos:pos+8])
		de.MinTimeSec = binary.LittleEndian.Uint64(data[pos+8:])
		if ver == shared.ValueIndexEntriesVersionV3 || ver == shared.ValueIndexEntriesVersionV4 {
			de.MaxTimeSec = binary.LittleEndian.Uint64(data[pos+16:])
			de.CompOff = binary.LittleEndian.Uint32(data[pos+24:])
			de.CompLen = binary.LittleEndian.Uint32(data[pos+28:])
		} else {
			de.CompOff = binary.LittleEndian.Uint32(data[pos+16:])
			de.CompLen = binary.LittleEndian.Uint32(data[pos+20:])
		}
		dir[i] = de
		pos += dirEntrySize
	}
	// Chunk data starts immediately after the directory.
	return dir, pos, ver, table, nil
}

// DecodeAllEntries decodes all entries from this reader, resolving SourceRef via
// the string table for v3 files (NOTE-VI-028, issue #432).
func (r *Reader) DecodeAllEntries() ([]Entry, error) {
	dataLen := r.chunkDataLen()
	if dataLen == 0 {
		return nil, nil
	}
	chunkData := r.data[r.chunkStart : r.chunkStart+dataLen]
	return DecodeChunkRangeWithTable(chunkData, r.chunkDir, 0, ^uint64(0), r.entriesVer, r.strTable)
}
