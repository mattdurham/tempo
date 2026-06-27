package valueindex

// NOTE: Any changes to this file must be reflected in the corresponding SPECS.md or NOTES.md.

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"slices"

	"github.com/grafana/blockpack/internal/modules/blockio/shared"
	"github.com/grafana/blockpack/internal/modules/blockio/writer"
)

// Writer accumulates entries for one column and flushes to a value index file.
// A Writer is not safe for concurrent use.
type Writer interface {
	// AddEntry records one observation: the span with traceID was at blockID (zero-based block
	// index) within sourceRef at timeSec and had vi:value = value.
	// value must match the col_type given to NewWriter.
	AddEntry(value any, traceID [16]byte, sourceRef string, blockID uint32, timeSec uint64) error

	// Flush sorts, deduplicates, and serializes all buffered entries.
	// level is the compaction level to embed in the VIMT section (0 = block builder output).
	// Returns the sealed file bytes. Resets internal state so the writer may be reused.
	Flush(ctx context.Context, level uint8) ([]byte, error)

	// ColHash returns the 32-char hex column hash for the column this writer indexes.
	ColHash() string

	// Close releases resources. Must be called even if Flush is not called.
	Close()
}

// rawEntry holds one buffered (unsorted) posting list entry.
type rawEntry struct {
	sourceRef      string
	canonicalValue []byte
	timeSec        uint64
	valueHash      [16]byte
	traceID        [16]byte
	blockID        uint32
}

// writerImpl is the concrete Writer implementation.
type writerImpl struct {
	colName string
	colHash string
	entries []rawEntry
	colType shared.ColumnType
}

// NewWriter creates a new value index writer for the named column of the given type.
func NewWriter(colName string, colType shared.ColumnType) Writer {
	return &writerImpl{
		colName: colName,
		colType: colType,
		colHash: ColHash(colName),
	}
}

func (w *writerImpl) ColHash() string { return w.colHash }

func (w *writerImpl) Close() { w.entries = nil }

// AddEntry encodes value to its canonical form and buffers the entry.
func (w *writerImpl) AddEntry(value any, traceID [16]byte, sourceRef string, blockID uint32, timeSec uint64) error {
	cv, err := CanonicalValue(w.colType, value)
	if err != nil {
		return fmt.Errorf("valueindex: AddEntry: %w", err)
	}
	vh := ValueHash16(cv)
	w.entries = append(w.entries, rawEntry{
		canonicalValue: cv,
		valueHash:      vh,
		traceID:        traceID,
		sourceRef:      sourceRef,
		blockID:        blockID,
		timeSec:        timeSec,
	})
	return nil
}

// Flush sorts, deduplicates, and serializes all buffered entries into a value index file.
func (w *writerImpl) Flush(_ context.Context, level uint8) ([]byte, error) {
	sortRawSlice(w.colType, w.entries)
	w.entries = deduplicateEntries(w.entries)
	data, err := w.flushSorted(level)
	if err != nil {
		return nil, err
	}
	w.entries = w.entries[:0]
	return data, nil
}

// flushSorted serializes already-sorted, already-deduplicated entries.
// Called both by Flush (public) and by compaction's flushBatch (internal).
// Does NOT reset w.entries.
func (w *writerImpl) flushSorted(level uint8) ([]byte, error) {
	// Compute wall timestamps.
	var wallMin, wallMax uint64
	for i, re := range w.entries {
		if i == 0 || re.timeSec < wallMin {
			wallMin = re.timeSec
		}
		if re.timeSec > wallMax {
			wallMax = re.timeSec
		}
	}

	// Convert rawEntry → Entry for the entries encoder.
	entries := make([]Entry, len(w.entries))
	for i, re := range w.entries {
		entries[i] = Entry{
			Value:     re.canonicalValue,
			TraceID:   re.traceID,
			SourceRef: re.sourceRef,
			BlockID:   re.blockID,
			TimeSec:   re.timeSec,
		}
	}

	// KLL sketch over vi:value.
	kllBytes, err := w.buildKLL(w.entries)
	if err != nil {
		return nil, fmt.Errorf("valueindex: flushSorted: KLL: %w", err)
	}

	// Encode posting list chunks (VINX content).
	chunkData, chunkDir, err := EncodeEntries(entries, shared.ValueIndexEntriesPerChunk)
	if err != nil {
		return nil, fmt.Errorf("valueindex: flushSorted: EncodeEntries: %w", err)
	}

	// Build VHIX hash index.
	hashEntries := buildHashIndex(w.entries)

	// Encode all sections.
	vimtBytes := EncodeMeta(Meta{
		ColType:         w.colType,
		ColHash:         colHashToBytes(w.colHash),
		ColName:         w.colName,
		WallMinTS:       wallMin,
		WallMaxTS:       wallMax,
		CompactionLevel: level,
	})
	vhixBytes := EncodeHashIndex(hashEntries)
	vinxSection := encodeVINXSection(chunkDir, chunkData)

	// Footer offsets.
	vimtOff := uint64(0)
	vhixOff := vimtOff + uint64(len(vimtBytes))
	vinxOff := vhixOff + uint64(len(vhixBytes))

	// Assemble: VIMT | VHIX | VINX | VKLL | footer.
	out := make([]byte, 0, len(vimtBytes)+len(vhixBytes)+len(vinxSection)+len(kllBytes)+shared.ValueIndexFooterSize)
	out = append(out, vimtBytes...)
	out = append(out, vhixBytes...)
	out = append(out, vinxSection...)
	out = append(out, kllBytes...)

	footer := make([]byte, shared.ValueIndexFooterSize)
	binary.LittleEndian.PutUint32(footer[0:], shared.ValueIndexFileMagic)
	footer[4] = shared.ValueIndexFileVersion
	// footer[5:8] reserved — zero from make
	binary.LittleEndian.PutUint64(footer[8:], vimtOff)
	binary.LittleEndian.PutUint64(footer[16:], vhixOff)
	binary.LittleEndian.PutUint64(footer[24:], vinxOff)
	out = append(out, footer...)
	return out, nil
}

// encodeVINXSection encodes the full VINX section including header, directory, and chunk data.
//
// Layout:
//
//	magic[4]+version[1]+reserved[3]+chunk_data_len[4]+chunk_count[4]+entries_per_chunk[2]+reserved[10]=28
//	chunk_dir[chunk_count × 24]
//	chunk_data[...]
func encodeVINXSection(dir []ChunkDirEntry, chunkData []byte) []byte {
	const headerSize = 28
	dirSize := len(dir) * chunkDirEntrySize
	buf := make([]byte, headerSize+dirSize+len(chunkData))

	binary.LittleEndian.PutUint32(buf[0:], shared.ValueIndexEntriesMagic)
	buf[4] = shared.ValueIndexEntriesVersion
	// buf[5:8] reserved
	binary.LittleEndian.PutUint32(buf[8:], uint32(len(chunkData)))                    //nolint:gosec // bounded
	binary.LittleEndian.PutUint32(buf[12:], uint32(len(dir)))                         //nolint:gosec
	binary.LittleEndian.PutUint16(buf[16:], uint16(shared.ValueIndexEntriesPerChunk)) //nolint:gosec
	// buf[18:28] reserved

	pos := headerSize
	for _, de := range dir {
		copy(buf[pos:], de.MinValue[:])
		binary.LittleEndian.PutUint64(buf[pos+8:], de.MinTimeSec)
		binary.LittleEndian.PutUint32(buf[pos+16:], de.CompOff)
		binary.LittleEndian.PutUint32(buf[pos+20:], de.CompLen)
		pos += chunkDirEntrySize
	}
	copy(buf[pos:], chunkData)
	return buf
}

// buildKLL constructs and encodes the KLL sketch for vi:value.
func (w *writerImpl) buildKLL(entries []rawEntry) ([]byte, error) {
	switch w.colType {
	case shared.ColumnTypeString, shared.ColumnTypeRangeString:
		sk := writer.NewKLLWithK[string](shared.ValueIndexKLLK)
		for i := range entries {
			sk.Add(string(entries[i].canonicalValue))
		}
		return EncodeKLLSection(sk, w.colType)
	case shared.ColumnTypeUint64, shared.ColumnTypeInt64, shared.ColumnTypeFloat64,
		shared.ColumnTypeRangeUint64, shared.ColumnTypeRangeInt64,
		shared.ColumnTypeRangeDuration, shared.ColumnTypeRangeFloat64:
		// NOTE-VI-012: Range* numeric types use the same uint64 bit-pattern KLL as their
		// scalar equivalents; compareCanonical handles type-correct ordering at query time.
		sk := writer.NewKLLWithK[uint64](shared.ValueIndexKLLK)
		for i := range entries {
			if len(entries[i].canonicalValue) >= 8 {
				sk.Add(binary.LittleEndian.Uint64(entries[i].canonicalValue[:8]))
			}
		}
		return EncodeKLLSection(sk, w.colType)
	default:
		sk := writer.NewKLLWithK[string](shared.ValueIndexKLLK)
		return EncodeKLLSection(sk, w.colType)
	}
}

// buildHashIndex returns one HashEntry per distinct valueHash in a sorted rawEntry slice.
func buildHashIndex(entries []rawEntry) []HashEntry {
	if len(entries) == 0 {
		return nil
	}
	var result []HashEntry
	var prevHash [16]byte
	perChunk := shared.ValueIndexEntriesPerChunk

	for i := range entries {
		thisChunk := uint32(i / perChunk) //nolint:gosec // bounded by slice length
		if i == 0 || entries[i].valueHash != prevHash {
			result = append(result, HashEntry{
				ValueHash: entries[i].valueHash,
				ChunkIdx:  thisChunk,
			})
			prevHash = entries[i].valueHash
		}
	}
	return result
}

// deduplicateEntries removes entries with identical (valueHash, traceID, sourceRef, blockID, timeSec).
// Assumes entries are already sorted so duplicates are adjacent.
func deduplicateEntries(entries []rawEntry) []rawEntry {
	if len(entries) == 0 {
		return nil
	}
	out := make([]rawEntry, 0, len(entries))
	out = append(out, entries[0])
	for i := 1; i < len(entries); i++ {
		prev := &out[len(out)-1]
		cur := &entries[i]
		if cur.valueHash == prev.valueHash &&
			cur.traceID == prev.traceID &&
			cur.sourceRef == prev.sourceRef &&
			cur.blockID == prev.blockID &&
			cur.timeSec == prev.timeSec {
			continue
		}
		out = append(out, *cur)
	}
	return out
}

// sortRawSlice sorts rawEntry slices by (canonicalValue ASC, timeSec ASC, traceID ASC).
// colType is required for correct numeric ordering (see NOTE-VI-011 / compareCanonical).
func sortRawSlice(colType shared.ColumnType, entries []rawEntry) {
	slices.SortFunc(entries, func(a, b rawEntry) int {
		if c := compareCanonical(colType, a.canonicalValue, b.canonicalValue); c != 0 {
			return c
		}
		if a.timeSec < b.timeSec {
			return -1
		}
		if a.timeSec > b.timeSec {
			return 1
		}
		return bytes.Compare(a.traceID[:], b.traceID[:])
	})
}

// colHashToBytes converts a 32-char lower-hex col_hash string to [16]byte.
func colHashToBytes(hexHash string) [16]byte {
	var b [16]byte
	if len(hexHash) != 32 {
		return b
	}
	for i := range 16 {
		b[i] = hexNibble(hexHash[i*2])<<4 | hexNibble(hexHash[i*2+1])
	}
	return b
}

func hexNibble(c byte) byte {
	switch {
	case c >= '0' && c <= '9':
		return c - '0'
	case c >= 'a' && c <= 'f':
		return c - 'a' + 10
	case c >= 'A' && c <= 'F':
		return c - 'A' + 10
	default:
		return 0
	}
}
