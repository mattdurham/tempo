package valuecounts

// NOTE: Any changes to this file must be reflected in the corresponding NOTES.md.

import (
	"encoding/binary"
	"fmt"

	"github.com/golang/snappy"
)

// ChunkDirEntry is one record in the VCNT chunk directory, sorted by (MinColumn, MinTimeStart).
// MinColumn is the full column name of the first record in the chunk; MinTimeStart is that
// record's TimeStart. Together they let a time-bounded, single-column lookup skip whole chunks.
type ChunkDirEntry struct {
	MinColumn    string
	MinTimeStart uint64
	CompOff      uint32
	CompLen      uint32
}

// EncodeRecords encodes a pre-sorted slice of records into snappy-compressed chunks.
// records MUST already be sorted in compareRecords order. perChunk is the nominal maximum
// number of records per chunk (<=0 uses shared.ValueCountsRecordsPerChunk).
// Returns the concatenated raw chunk bytes and the chunk directory.
func EncodeRecords(records []Record, perChunk int) ([]byte, []ChunkDirEntry) {
	if len(records) == 0 {
		return []byte{}, nil
	}
	perChunk = recordsPerChunk(perChunk)

	var (
		allChunks []byte
		dir       []ChunkDirEntry
	)
	for start := 0; start < len(records); start += perChunk {
		end := start + perChunk
		if end > len(records) {
			end = len(records)
		}
		chunk := records[start:end]
		raw := encodeChunkPayload(chunk)
		compressed := snappy.Encode(nil, raw)

		dir = append(dir, ChunkDirEntry{
			MinColumn:    chunk[0].ColumnName,
			MinTimeStart: chunk[0].TimeStart,
			CompOff:      uint32(len(allChunks)),  //nolint:gosec // bounded by output size
			CompLen:      uint32(len(compressed)), //nolint:gosec // bounded by output size
		})
		allChunks = append(allChunks, compressed...)
	}
	return allChunks, dir
}

// encodeChunkPayload serializes records into a raw (pre-snappy) chunk payload.
//
// Wire per record:
//
//	col_len[2] + col[N] + time_start[8] + time_end[8] + val_len[4] + val[M] + count[8 signed]
func encodeChunkPayload(records []Record) []byte {
	buf := make([]byte, 0, 2+len(records)*48)
	buf = binary.LittleEndian.AppendUint16(buf, uint16(len(records))) //nolint:gosec // <= perChunk
	for i := range records {
		r := &records[i]
		buf = binary.LittleEndian.AppendUint16(buf, uint16(len(r.ColumnName))) //nolint:gosec // bounded
		buf = append(buf, r.ColumnName...)
		buf = binary.LittleEndian.AppendUint64(buf, r.TimeStart)
		buf = binary.LittleEndian.AppendUint64(buf, r.TimeEnd)
		buf = binary.LittleEndian.AppendUint32(buf, uint32(len(r.Value))) //nolint:gosec // bounded
		buf = append(buf, r.Value...)
		buf = binary.LittleEndian.AppendUint64(buf, uint64(r.Count)) //nolint:gosec // signed round-trip
	}
	return buf
}

// DecodeAll decompresses and decodes every chunk in data using dir.
func DecodeAll(data []byte, dir []ChunkDirEntry) ([]Record, error) {
	return DecodeTimeRange(data, dir, 0, ^uint64(0))
}

// DecodeTimeRange decodes records whose [TimeStart, TimeEnd] window overlaps [minTS, maxTS]
// (NOTE-VC-003).
// Chunks are skipped when MinTimeStart > maxTS — but only after the directory's sorted order
// guarantees later chunks also start later. Because records within a chunk may have differing
// windows, per-record overlap is still checked after decode.
func DecodeTimeRange(data []byte, dir []ChunkDirEntry, minTS, maxTS uint64) ([]Record, error) {
	var out []Record
	for i := range dir {
		de := &dir[i]
		if de.MinTimeStart > maxTS {
			// All records in this chunk start after the query window's end. Later chunks
			// start no earlier (directory is sorted by MinColumn then MinTimeStart), but a
			// different column could reset MinTimeStart, so keep scanning rather than break.
			continue
		}
		end := int(de.CompOff) + int(de.CompLen)
		if int(de.CompOff) > len(data) || end > len(data) {
			return nil, fmt.Errorf(
				"valuecounts: chunk %d out of bounds (off=%d len=%d data=%d)",
				i, de.CompOff, de.CompLen, len(data),
			)
		}
		raw, err := snappy.Decode(nil, data[de.CompOff:end])
		if err != nil {
			return nil, fmt.Errorf("valuecounts: chunk %d snappy decode: %w", i, err)
		}
		recs, err := decodeChunkPayload(raw)
		if err != nil {
			return nil, fmt.Errorf("valuecounts: chunk %d decode: %w", i, err)
		}
		for j := range recs {
			r := &recs[j]
			// Overlap test: window [TimeStart, TimeEnd] intersects [minTS, maxTS].
			if r.TimeStart <= maxTS && r.TimeEnd >= minTS {
				out = append(out, *r)
			}
		}
	}
	return out, nil
}

// decodeChunkPayload parses a raw chunk payload into records.
func decodeChunkPayload(raw []byte) ([]Record, error) {
	if len(raw) < 2 {
		return nil, fmt.Errorf("valuecounts: chunk payload too short (%d bytes)", len(raw))
	}
	count := int(binary.LittleEndian.Uint16(raw[:2]))
	pos := 2
	recs := make([]Record, 0, count)
	for i := range count {
		if pos+2 > len(raw) {
			return nil, fmt.Errorf("valuecounts: record %d: truncated at col_len", i)
		}
		colLen := int(binary.LittleEndian.Uint16(raw[pos:]))
		pos += 2
		if pos+colLen > len(raw) {
			return nil, fmt.Errorf("valuecounts: record %d: column truncated", i)
		}
		col := string(raw[pos : pos+colLen])
		pos += colLen

		if pos+16 > len(raw) {
			return nil, fmt.Errorf("valuecounts: record %d: truncated at time window", i)
		}
		timeStart := binary.LittleEndian.Uint64(raw[pos:])
		pos += 8
		timeEnd := binary.LittleEndian.Uint64(raw[pos:])
		pos += 8

		if pos+4 > len(raw) {
			return nil, fmt.Errorf("valuecounts: record %d: truncated at val_len", i)
		}
		valLen := int(binary.LittleEndian.Uint32(raw[pos:]))
		pos += 4
		if pos+valLen > len(raw) {
			return nil, fmt.Errorf("valuecounts: record %d: value truncated", i)
		}
		val := make([]byte, valLen)
		copy(val, raw[pos:pos+valLen])
		pos += valLen

		if pos+8 > len(raw) {
			return nil, fmt.Errorf("valuecounts: record %d: truncated at count", i)
		}
		cnt := int64(binary.LittleEndian.Uint64(raw[pos:])) //nolint:gosec // signed round-trip
		pos += 8

		recs = append(recs, Record{
			ColumnName: col,
			Value:      val,
			TimeStart:  timeStart,
			TimeEnd:    timeEnd,
			Count:      cnt,
		})
	}
	return recs, nil
}
