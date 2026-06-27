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
// this span. Storing it here eliminates a block-index lookup when resolving results back
// to span data: open SourceRef → seek to block BlockID → decode → done.
type Entry struct {
	SourceRef string
	Value     []byte // canonical-encoded column value
	TimeSec   uint64
	TraceID   [16]byte
	BlockID   uint32 // zero-based block index within SourceRef
}

// ChunkDirEntry is one record in the VINX chunk directory.
// The directory is sorted by MinTimeSec ascending.
//
// Wire: min_value[8]+min_time_sec[8]+comp_off[4]+comp_len[4] = 24 bytes.
type ChunkDirEntry struct {
	MinValue   [8]byte // first 8 bytes of the first entry's Value in this chunk
	MinTimeSec uint64
	CompOff    uint32
	CompLen    uint32
}

// chunkDirEntrySize is the byte size of one ChunkDirEntry on the wire.
const chunkDirEntrySize = 24

// EncodeEntries encodes a pre-sorted slice of entries into snappy-compressed chunks.
// perChunk is the nominal maximum number of entries per chunk.
// Returns the concatenated raw chunk bytes and the chunk directory.
func EncodeEntries(entries []Entry, perChunk int) ([]byte, []ChunkDirEntry, error) {
	if len(entries) == 0 {
		return []byte{}, nil, nil
	}
	if perChunk <= 0 {
		perChunk = shared.ValueIndexEntriesPerChunk
	}

	var (
		allChunks []byte
		dir       []ChunkDirEntry
	)

	for start := 0; start < len(entries); start += perChunk {
		end := start + perChunk
		if end > len(entries) {
			end = len(entries)
		}
		chunk := entries[start:end]

		raw := encodeChunkPayload(chunk)
		compressed := snappy.Encode(nil, raw)

		de := ChunkDirEntry{
			MinTimeSec: chunk[0].TimeSec,
			CompOff:    uint32(len(allChunks)),  //nolint:gosec // bounded by MaxOutputBytes
			CompLen:    uint32(len(compressed)), //nolint:gosec // bounded
		}
		// Store first 8 bytes of the first entry's Value as MinValue.
		n := copy(de.MinValue[:], chunk[0].Value)
		_ = n

		allChunks = append(allChunks, compressed...)
		dir = append(dir, de)
	}
	return allChunks, dir, nil
}

// encodeChunkPayload serializes a slice of entries into a raw (pre-snappy) v2 chunk payload.
//
// Wire format per entry (NOTE-VI-014):
//
//	val_len[2] + val[N] + time_sec[8] + trace_id[16] + block_id[4] + ref_len[2] + ref[N]
func encodeChunkPayload(entries []Entry) []byte {
	// Pre-size estimate: 2 (count) + per entry ~(2+avgValLen + 8 + 16 + 4 + 2 + avgRefLen)
	buf := make([]byte, 0, 2+len(entries)*54)
	buf = binary.LittleEndian.AppendUint16(buf, uint16(len(entries))) //nolint:gosec
	for i := range entries {
		e := &entries[i]
		buf = binary.LittleEndian.AppendUint16(buf, uint16(len(e.Value))) //nolint:gosec
		buf = append(buf, e.Value...)
		buf = binary.LittleEndian.AppendUint64(buf, e.TimeSec)
		buf = append(buf, e.TraceID[:]...)
		buf = binary.LittleEndian.AppendUint32(buf, e.BlockID)
		buf = binary.LittleEndian.AppendUint16(buf, uint16(len(e.SourceRef))) //nolint:gosec
		buf = append(buf, e.SourceRef...)
	}
	return buf
}

// DecodeAllChunks decompresses and decodes all chunks from data using dir.
func DecodeAllChunks(data []byte, dir []ChunkDirEntry) ([]Entry, error) {
	return DecodeChunkRange(data, dir, 0, ^uint64(0))
}

// DecodeChunkRange decodes only chunks where MinTimeSec <= maxTS, then filters
// individual entries to the range [minTS, maxTS].
func DecodeChunkRange(data []byte, dir []ChunkDirEntry, minTS, maxTS uint64) ([]Entry, error) {
	var out []Entry
	for i := range dir {
		de := &dir[i]
		// Skip chunks whose minimum time is already beyond maxTS.
		if de.MinTimeSec > maxTS {
			continue
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
		entries, err := decodeChunkPayload(raw)
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

// decodeChunkPayload parses a chunk payload (NOTE-VI-014).
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

		if pos+4 > len(raw) {
			return nil, fmt.Errorf("valueindex: entry %d: truncated at block_id", i)
		}
		blockID := binary.LittleEndian.Uint32(raw[pos:])
		pos += 4

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
			BlockID:   blockID,
			SourceRef: ref,
		})
	}
	return entries, nil
}
