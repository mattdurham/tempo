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
	BlockID   uint32 // zero-based block index within SourceRef; 0 for v1 files (NOTE-VI-014)
}

// Reader reads a value index file from an in-memory byte slice.
// After OpenReader succeeds, Meta, HashIndex, and the chunk directory are loaded;
// chunk data is decoded lazily during Lookup.
type Reader struct {
	data       []byte
	hashIdx    []HashEntry
	chunkDir   []ChunkDirEntry
	meta       Meta
	chunkStart int // byte offset within data where the raw chunk bytes begin
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
	chunkDir, chunkDataStart, _, err := decodeVINXSection(data, vinxOff, footerStart)
	if err != nil {
		return nil, fmt.Errorf("valueindex: VINX decode: %w", err)
	}

	return &Reader{
		data:       data,
		meta:       meta,
		hashIdx:    hashIdx,
		chunkDir:   chunkDir,
		chunkStart: chunkDataStart,
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
	entries, err := DecodeChunkRange(chunkData, r.chunkDir, minTS, maxTS)
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
// Returns the chunk directory, the absolute byte offset of chunk data, and the version byte.
// Supports v1 (no BlockID) and v2 (BlockID per entry); the version is passed through to
// the chunk payload decoder so old files are read correctly.
func decodeVINXSection(data []byte, vinxOff, footerStart int) ([]ChunkDirEntry, int, uint8, error) {
	const vinxHeaderSize = 28
	if vinxOff+vinxHeaderSize > footerStart {
		return nil, 0, 0, fmt.Errorf(
			"valueindex: VINX section truncated at header (off=%d filesize=%d)",
			vinxOff,
			len(data),
		)
	}
	vinx := data[vinxOff:]
	if magic := binary.LittleEndian.Uint32(vinx[0:4]); magic != shared.ValueIndexEntriesMagic {
		return nil, 0, 0, fmt.Errorf("valueindex: VINX wrong magic 0x%08X", magic)
	}
	ver := vinx[4]
	if ver != shared.ValueIndexEntriesVersion {
		return nil, 0, 0, fmt.Errorf("valueindex: VINX unsupported version %d", ver)
	}
	chunkCount := int(binary.LittleEndian.Uint32(vinx[12:16]))
	dirSize := chunkCount * chunkDirEntrySize
	if vinxOff+vinxHeaderSize+dirSize > footerStart {
		return nil, 0, 0, fmt.Errorf("valueindex: VINX chunk directory truncated (%d chunks)", chunkCount)
	}
	dir := make([]ChunkDirEntry, chunkCount)
	pos := vinxOff + vinxHeaderSize
	for i := range dir {
		var de ChunkDirEntry
		copy(de.MinValue[:], data[pos:pos+8])
		de.MinTimeSec = binary.LittleEndian.Uint64(data[pos+8:])
		de.CompOff = binary.LittleEndian.Uint32(data[pos+16:])
		de.CompLen = binary.LittleEndian.Uint32(data[pos+20:])
		dir[i] = de
		pos += chunkDirEntrySize
	}
	// Chunk data starts immediately after the directory.
	return dir, pos, ver, nil
}
