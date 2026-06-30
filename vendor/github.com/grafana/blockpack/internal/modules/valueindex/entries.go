package valueindex

// NOTE: Any changes to this file must be reflected in the corresponding SPECS.md or NOTES.md.

import (
	"encoding/binary"
	"fmt"

	"github.com/golang/snappy"

	"github.com/grafana/blockpack/internal/modules/blockio/shared"
)

// Entry is one posting list row in a value index file.
// Rows are sorted by (Value ASC, TimeSec ASC, TraceID ASC).
//
// NOTE-VI-014: BlockID is the zero-based index of the block within SourceRef that contains
// this span (v1 files). NOTE-VI-027: BlockRef replaces BlockID in v2 files — it encodes
// the block's page number and length in pages for direct S3 ranged GET without a TOC fetch.
// NOTE-VI-028 (#432): SourceRef stored as string table index in v3+ files.
// NOTE-VI-029 (#428): SpanID and RowIdx added in v4 files for span-level addressing.
type Entry struct {
	SourceRef string
	Value     []byte // canonical-encoded column value
	TimeSec   uint64
	TraceID   [16]byte
	BlockID   uint32   // v1: zero-based block index within SourceRef
	BlockRef  BlockRef // v2+: direct page reference (PageNum uint24 + LenPages uint16)
	SpanID    [8]byte  // v4+: span identity for direct span lookup within block
	RowIdx    uint16   // v4+: row index within the block for O(1) span access
}

// ChunkDirEntry is one record in the VINX chunk directory.
// The directory is sorted by MinTimeSec ascending.
//
// v1/v2 wire (24 bytes): min_value[8]+min_time_sec[8]+comp_off[4]+comp_len[4]
// v3 wire (32 bytes): min_value[8]+min_time_sec[8]+max_time_sec[8]+comp_off[4]+comp_len[4]+reserved[4]
type ChunkDirEntry struct {
	MinValue   [8]byte // first 8 bytes of the first entry's Value in this chunk
	MinTimeSec uint64
	MaxTimeSec uint64 // v3 only: maximum TimeSec in this chunk (0 for v1/v2)
	CompOff    uint32
	CompLen    uint32
}

// chunkDirEntrySize is the byte size of one ChunkDirEntry for v1/v2 files.
const chunkDirEntrySize = 24

// chunkDirEntryV3Size is the byte size of one ChunkDirEntry for v3 files.
const chunkDirEntryV3Size = 32

// EncodeEntries encodes a pre-sorted slice of entries into snappy-compressed chunks.
// perChunk is the nominal maximum number of entries per chunk.
// Returns the concatenated raw chunk bytes and the chunk directory.
func EncodeEntries(entries []Entry, perChunk int) ([]byte, []ChunkDirEntry, error) {
	if len(entries) == 0 {
		return []byte{}, nil, nil
	}
	ce := newChunkEncoder(perChunk)
	for i := range entries {
		ce.Add(entries[i])
	}
	allChunks, dir := ce.Finish()
	return allChunks, dir, nil
}

// chunkEncoder incrementally builds snappy-compressed posting-list chunks from a
// stream of entries pushed in sorted order. It is the streaming equivalent of
// EncodeEntries: callers Add one entry at a time and Finish to obtain the
// concatenated chunk bytes and directory. Peak memory is bounded to one chunk's
// worth of entries plus the growing output buffer, never the full posting list.
//
// NOTE-VI-026: introduced for the writer external sort-merge path (issue #413) so
// the chunk/dir building does not require the full []Entry slice in memory.
type chunkEncoder struct {
	allChunks []byte
	dir       []ChunkDirEntry
	pending   []Entry
	perChunk  int
}

// newChunkEncoder creates a streaming chunk encoder with the given nominal chunk size.
func newChunkEncoder(perChunk int) *chunkEncoder {
	if perChunk <= 0 {
		perChunk = shared.ValueIndexEntriesPerChunk
	}
	return &chunkEncoder{
		perChunk: perChunk,
		pending:  make([]Entry, 0, perChunk),
	}
}

// Add appends one entry to the current chunk, flushing the chunk when full.
// Entries must be pushed in the final sorted order.
func (ce *chunkEncoder) Add(e Entry) {
	ce.pending = append(ce.pending, e)
	if len(ce.pending) >= ce.perChunk {
		ce.flushChunk()
	}
}

// flushChunk encodes and compresses the pending entries into a single chunk.
func (ce *chunkEncoder) flushChunk() {
	if len(ce.pending) == 0 {
		return
	}
	raw := encodeChunkPayload(ce.pending)
	compressed := snappy.Encode(nil, raw)

	de := ChunkDirEntry{
		MinTimeSec: ce.pending[0].TimeSec,
		CompOff:    uint32(len(ce.allChunks)), //nolint:gosec // bounded by MaxOutputBytes
		CompLen:    uint32(len(compressed)),   //nolint:gosec // bounded
	}
	copy(de.MinValue[:], ce.pending[0].Value)

	ce.allChunks = append(ce.allChunks, compressed...)
	ce.dir = append(ce.dir, de)
	ce.pending = ce.pending[:0]
}

// Finish flushes any pending entries and returns the chunk bytes and directory.
func (ce *chunkEncoder) Finish() ([]byte, []ChunkDirEntry) {
	ce.flushChunk()
	if ce.allChunks == nil {
		ce.allChunks = []byte{}
	}
	return ce.allChunks, ce.dir
}

// encodeChunkPayload serializes a slice of entries into a raw (pre-snappy) chunk payload.
// It detects whether to use v1 (BlockID uint32) or v2 (BlockRef 5-byte) encoding based
// on whether any entry has a non-zero BlockRef.
//
// v2 wire format per entry (NOTE-VI-027):
//
//	val_len[2] + val[N] + time_sec[8] + trace_id[16] + block_page[3] + block_len_pages[2] + ref_len[2] + ref[N]
//
// v1 wire format per entry (NOTE-VI-014):
//
//	val_len[2] + val[N] + time_sec[8] + trace_id[16] + block_ref[5] + ref_len[2] + ref[N]
//
// block_ref is a 5-byte BlockFileRef (uint24 page + uint16 length), replacing the
// v1 block_id[4] (NOTE-VI-014).
func encodeChunkPayload(entries []Entry) []byte {
	// Detect from first entry: non-zero BlockRef means v2, zero means v1 (BlockID).
	var useBlockRef bool
	if len(entries) > 0 {
		useBlockRef = entries[0].BlockRef.PageNum > 0 || entries[0].BlockRef.LenPages > 0
	}

	// Pre-size estimate: 2 (count) + per entry ~(2+avgValLen + 8 + 16 + 5 + 2 + avgRefLen)
	buf := make([]byte, 0, 2+len(entries)*55)
	buf = binary.LittleEndian.AppendUint16(buf, uint16(len(entries))) //nolint:gosec
	for i := range entries {
		e := &entries[i]
		buf = binary.LittleEndian.AppendUint16(buf, uint16(len(e.Value))) //nolint:gosec
		buf = append(buf, e.Value...)
		buf = binary.LittleEndian.AppendUint64(buf, e.TimeSec)
		buf = append(buf, e.TraceID[:]...)
		if useBlockRef {
			buf = AppendBlockRef(buf, e.BlockRef)
		} else {
			buf = binary.LittleEndian.AppendUint32(buf, e.BlockID)
		}
		buf = binary.LittleEndian.AppendUint16(buf, uint16(len(e.SourceRef))) //nolint:gosec
		buf = append(buf, e.SourceRef...)
	}
	return buf
}

// DecodeAllChunks decompresses and decodes all chunks from data using dir.
// ver is the VINX entries version (ValueIndexEntriesVersionV1 or ValueIndexEntriesVersion).
func DecodeAllChunks(data []byte, dir []ChunkDirEntry, ver uint8) ([]Entry, error) {
	return DecodeChunkRange(data, dir, 0, ^uint64(0), ver)
}

// DecodeChunkRange decodes only chunks where MinTimeSec <= maxTS, then filters
// individual entries to the range [minTS, maxTS].
// ver is the VINX entries version (V1, V2, or V3).
func DecodeChunkRange(data []byte, dir []ChunkDirEntry, minTS, maxTS uint64, ver uint8) ([]Entry, error) {
	return DecodeChunkRangeWithTable(data, dir, minTS, maxTS, ver, nil)
}

// DecodeChunkRangeWithTable is like DecodeChunkRange but accepts a string table for v3.
func DecodeChunkRangeWithTable(
	data []byte,
	dir []ChunkDirEntry,
	minTS, maxTS uint64,
	ver uint8,
	table *StringTable,
) ([]Entry, error) {
	var out []Entry
	for i := range dir {
		de := &dir[i]
		// Skip chunks entirely outside the requested time range.
		if de.MinTimeSec > maxTS {
			continue // chunk starts after the end of the query window
		}
		// v3 files have MaxTimeSec: skip chunks entirely before the start.
		if de.MaxTimeSec > 0 && de.MaxTimeSec < minTS {
			continue // all entries in chunk are too old
		}
		// Bounds check.
		end := int(de.CompOff) + int(de.CompLen)
		if int(de.CompOff) > len(data) || end > len(data) {
			return nil, fmt.Errorf(
				"valueindex: chunk %d out of bounds (off=%d len=%d data=%d)",
				i, de.CompOff, de.CompLen, len(data),
			)
		}
		raw, err := snappy.Decode(nil, data[de.CompOff:end])
		if err != nil {
			return nil, fmt.Errorf("valueindex: chunk %d snappy decode: %w", i, err)
		}
		var entries []Entry
		switch ver {
		case shared.ValueIndexEntriesVersionV1:
			entries, err = decodeChunkPayloadV1(raw)
		case shared.ValueIndexEntriesVersionV3:
			entries, err = decodeChunkPayloadV3(raw, table)
		case shared.ValueIndexEntriesVersionV4:
			entries, err = decodeChunkPayloadV4(raw, table)
		default: // V2
			entries, err = decodeChunkPayload(raw)
		}
		if err != nil {
			return nil, fmt.Errorf("valueindex: chunk %d decode: %w", i, err)
		}
		for j := range entries {
			e := &entries[j]
			if e.TimeSec >= minTS && e.TimeSec <= maxTS {
				out = append(out, *e)
			}
		}
	}
	return out, nil
}

// decodeChunkPayload parses a v2 chunk payload (NOTE-VI-027).
// Wire: val_len[2]+val[N]+time_sec[8]+trace_id[16]+block_page[3]+block_len_pages[2]+ref_len[2]+ref[N]
func decodeChunkPayload(raw []byte) ([]Entry, error) {
	if len(raw) < 2 {
		return nil, fmt.Errorf("valueindex: chunk payload too short (%d bytes)", len(raw))
	}
	count := int(binary.LittleEndian.Uint16(raw[:2]))
	pos := 2
	entries := make([]Entry, 0, count)
	for i := range count {
		if pos+2 > len(raw) {
			return nil, fmt.Errorf("valueindex: entry %d: truncated at value_len", i)
		}
		valLen := int(binary.LittleEndian.Uint16(raw[pos:]))
		pos += 2
		if pos+valLen > len(raw) {
			return nil, fmt.Errorf("valueindex: entry %d: value truncated", i)
		}
		val := make([]byte, valLen)
		copy(val, raw[pos:pos+valLen])
		pos += valLen

		if pos+8 > len(raw) {
			return nil, fmt.Errorf("valueindex: entry %d: truncated at time_sec", i)
		}
		timeSec := binary.LittleEndian.Uint64(raw[pos:])
		pos += 8

		if pos+16 > len(raw) {
			return nil, fmt.Errorf("valueindex: entry %d: truncated at trace_id", i)
		}
		var traceID [16]byte
		copy(traceID[:], raw[pos:pos+16])
		pos += 16

		if pos+BlockRefSize > len(raw) {
			return nil, fmt.Errorf("valueindex: entry %d: truncated at block_ref", i)
		}
		blockRef := DecodeBlockRef(raw, pos)
		pos += BlockRefSize

		if pos+2 > len(raw) {
			return nil, fmt.Errorf("valueindex: entry %d: truncated at ref_len", i)
		}
		refLen := int(binary.LittleEndian.Uint16(raw[pos:]))
		pos += 2
		if pos+refLen > len(raw) {
			return nil, fmt.Errorf("valueindex: entry %d: ref truncated", i)
		}
		ref := string(raw[pos : pos+refLen])
		pos += refLen

		entries = append(entries, Entry{
			Value:     val,
			TimeSec:   timeSec,
			TraceID:   traceID,
			BlockRef:  blockRef,
			SourceRef: ref,
		})
	}
	return entries, nil
}

// decodeChunkPayloadV1 parses a v1 chunk payload (NOTE-VI-014).
// Wire: val_len[2]+val[N]+time_sec[8]+trace_id[16]+block_id[4]+ref_len[2]+ref[N]
func decodeChunkPayloadV1(raw []byte) ([]Entry, error) {
	if len(raw) < 2 {
		return nil, fmt.Errorf("valueindex: v1 chunk payload too short (%d bytes)", len(raw))
	}
	count := int(binary.LittleEndian.Uint16(raw[:2]))
	pos := 2
	entries := make([]Entry, 0, count)
	for i := range count {
		if pos+2 > len(raw) {
			return nil, fmt.Errorf("valueindex: v1 entry %d: truncated at value_len", i)
		}
		valLen := int(binary.LittleEndian.Uint16(raw[pos:]))
		pos += 2
		if pos+valLen > len(raw) {
			return nil, fmt.Errorf("valueindex: v1 entry %d: value truncated", i)
		}
		val := make([]byte, valLen)
		copy(val, raw[pos:pos+valLen])
		pos += valLen

		if pos+8 > len(raw) {
			return nil, fmt.Errorf("valueindex: v1 entry %d: truncated at time_sec", i)
		}
		timeSec := binary.LittleEndian.Uint64(raw[pos:])
		pos += 8

		if pos+16 > len(raw) {
			return nil, fmt.Errorf("valueindex: v1 entry %d: truncated at trace_id", i)
		}
		var traceID [16]byte
		copy(traceID[:], raw[pos:pos+16])
		pos += 16

		if pos+4 > len(raw) {
			return nil, fmt.Errorf("valueindex: v1 entry %d: truncated at block_id", i)
		}
		blockID := binary.LittleEndian.Uint32(raw[pos:])
		pos += 4

		if pos+2 > len(raw) {
			return nil, fmt.Errorf("valueindex: v1 entry %d: truncated at ref_len", i)
		}
		refLen := int(binary.LittleEndian.Uint16(raw[pos:]))
		pos += 2
		if pos+refLen > len(raw) {
			return nil, fmt.Errorf("valueindex: v1 entry %d: ref truncated", i)
		}
		ref := string(raw[pos : pos+refLen])
		pos += refLen

		entries = append(entries, Entry{
			Value:     val,
			TimeSec:   timeSec,
			TraceID:   traceID,
			BlockID:   blockID,
			SourceRef: ref,
		})
	}
	return entries, nil
}

// decodeChunkPayloadV3 parses a v3 chunk payload.
// Wire: val_len[2]+val[N]+time_sec[8]+trace_id[16]+block_page[3]+block_len_pages[2]+str_idx[2]
// The caller resolves str_idx back to SourceRef via table.Lookup.
func decodeChunkPayloadV3(raw []byte, table *StringTable) ([]Entry, error) {
	if len(raw) < 2 {
		return nil, fmt.Errorf("valueindex: v3 chunk payload too short (%d bytes)", len(raw))
	}
	count := int(binary.LittleEndian.Uint16(raw[:2]))
	pos := 2
	out := make([]Entry, 0, count)
	for range count {
		if pos+2 > len(raw) {
			return nil, fmt.Errorf("valueindex: v3 entry truncated at val_len")
		}
		valLen := int(binary.LittleEndian.Uint16(raw[pos : pos+2]))
		pos += 2
		if pos+valLen+8+16+BlockRefSize+2 > len(raw) {
			return nil, fmt.Errorf("valueindex: v3 entry truncated")
		}
		var e Entry
		if valLen > 0 {
			e.Value = make([]byte, valLen)
			copy(e.Value, raw[pos:pos+valLen])
		}
		pos += valLen
		e.TimeSec = binary.LittleEndian.Uint64(raw[pos : pos+8])
		pos += 8
		copy(e.TraceID[:], raw[pos:pos+16])
		pos += 16
		e.BlockRef = DecodeBlockRef(raw, pos)
		pos += BlockRefSize
		// Decode string table index.
		strIdx := binary.LittleEndian.Uint16(raw[pos : pos+2])
		pos += 2
		if table != nil {
			e.SourceRef = table.Lookup(strIdx)
		}
		out = append(out, e)
	}
	if pos != len(raw) {
		return nil, fmt.Errorf("valueindex: v3 chunk payload: %d trailing bytes", len(raw)-pos)
	}
	return out, nil
}

// encodeChunkPayloadV4 encodes entries using v4 format which adds SpanID[8]+RowIdx[2]
// per entry for direct span addressing (NOTE-VI-029, issue #428).
//
// v4 wire format per entry:
//
//	val_len[2]+val[N]+time_sec[8]+trace_id[16]+block_page[3]+block_len_pages[2]+str_idx[2]+span_id[8]+row_idx[2]
func encodeChunkPayloadV4(entries []Entry, table *StringTable) []byte {
	buf := make([]byte, 0, 2+len(entries)*57)
	buf = binary.LittleEndian.AppendUint16(buf, uint16(len(entries))) //nolint:gosec
	for i := range entries {
		e := &entries[i]
		buf = binary.LittleEndian.AppendUint16(buf, uint16(len(e.Value))) //nolint:gosec
		buf = append(buf, e.Value...)
		buf = binary.LittleEndian.AppendUint64(buf, e.TimeSec)
		buf = append(buf, e.TraceID[:]...)
		buf = AppendBlockRef(buf, e.BlockRef)
		var idx uint16
		if table != nil {
			idx, _ = table.Intern(e.SourceRef)
		}
		buf = binary.LittleEndian.AppendUint16(buf, idx)
		buf = append(buf, e.SpanID[:]...)
		buf = binary.LittleEndian.AppendUint16(buf, e.RowIdx)
	}
	return buf
}

// decodeChunkPayloadV4 parses a v4 chunk payload.
func decodeChunkPayloadV4(raw []byte, table *StringTable) ([]Entry, error) {
	if len(raw) < 2 {
		return nil, fmt.Errorf("valueindex: v4 chunk payload too short (%d bytes)", len(raw))
	}
	count := int(binary.LittleEndian.Uint16(raw[:2]))
	pos := 2
	out := make([]Entry, 0, count)
	for range count {
		if pos+2 > len(raw) {
			return nil, fmt.Errorf("valueindex: v4 entry truncated at val_len")
		}
		valLen := int(binary.LittleEndian.Uint16(raw[pos : pos+2]))
		pos += 2
		if pos+valLen+8+16+BlockRefSize+2+8+2 > len(raw) {
			return nil, fmt.Errorf("valueindex: v4 entry truncated")
		}
		var e Entry
		if valLen > 0 {
			e.Value = make([]byte, valLen)
			copy(e.Value, raw[pos:pos+valLen])
		}
		pos += valLen
		e.TimeSec = binary.LittleEndian.Uint64(raw[pos : pos+8])
		pos += 8
		copy(e.TraceID[:], raw[pos:pos+16])
		pos += 16
		e.BlockRef = DecodeBlockRef(raw, pos)
		pos += BlockRefSize
		strIdx := binary.LittleEndian.Uint16(raw[pos : pos+2])
		pos += 2
		if table != nil {
			e.SourceRef = table.Lookup(strIdx)
		}
		copy(e.SpanID[:], raw[pos:pos+8])
		pos += 8
		e.RowIdx = binary.LittleEndian.Uint16(raw[pos : pos+2])
		pos += 2
		out = append(out, e)
	}
	if pos != len(raw) {
		return nil, fmt.Errorf("valueindex: v4 chunk payload: %d trailing bytes", len(raw)-pos)
	}
	return out, nil
}
