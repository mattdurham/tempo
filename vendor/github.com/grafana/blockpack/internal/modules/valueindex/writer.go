package valueindex

// NOTE: Any changes to this file must be reflected in the corresponding SPECS.md or NOTES.md.

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"slices"

	"github.com/golang/snappy"

	"github.com/grafana/blockpack/internal/modules/blockio/shared"
)

// Writer accumulates entries for one column and flushes to a value index file.
// A Writer is not safe for concurrent use.
type Writer interface {
	// AddEntry records a v1 observation: blockID is the zero-based block index within sourceRef.
	AddEntry(value any, traceID [16]byte, sourceRef string, blockID uint32, timeSec uint64) error

	// AddEntryV2 records a v2 observation: blockRef is the direct page-addressed block reference
	// (NOTE-VI-027). Use this for entries extracted from v2 blockpack files.
	AddEntryV2(value any, traceID [16]byte, sourceRef string, blockRef BlockRef, timeSec uint64) error

	// AddEntryV4 records a v4 observation with full span identity (NOTE-VI-029, issue #428).
	// spanID is the span's 8-byte identifier; rowIdx is the span's row index within the block.
	// Use this when span identity is available for direct span addressing in query results.
	AddEntryV4(
		value any,
		traceID [16]byte,
		sourceRef string,
		blockRef BlockRef,
		timeSec uint64,
		spanID [8]byte,
		rowIdx uint16,
	) error

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
	blockID        uint32   // v1
	blockRef       BlockRef // v2+ (non-zero when v2+)
	spanID         [8]byte  // v4+ (non-zero when span identity available)
	rowIdx         uint16   // v4+
}

// writerImpl is the concrete Writer implementation.
//
// NOTE-VI-026 (issue #413): entries accumulate in memory only up to
// ValueIndexWriterSpillEntries; beyond that the run is sorted and spilled to a temp
// file (runs) and the in-memory buffer is reset. Flush k-way merges the spilled runs
// with the remaining in-memory tail, bounding peak memory regardless of cardinality.
type writerImpl struct {
	colName string
	colHash string
	entries []rawEntry
	runs    []*runFile
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

func (w *writerImpl) Close() {
	w.entries = nil
	w.discardRuns()
}

// discardRuns closes and deletes all spilled run temp files.
func (w *writerImpl) discardRuns() {
	for _, r := range w.runs {
		r.remove()
	}
	w.runs = nil
}

// AddEntry encodes value to its canonical form and buffers the entry, spilling a
// sorted run to disk once the in-memory buffer reaches ValueIndexWriterSpillEntries
// (NOTE-VI-026, issue #413).
func (w *writerImpl) AddEntry(value any, traceID [16]byte, sourceRef string, blockID uint32, timeSec uint64) error {
	return w.addRaw(value, traceID, sourceRef, blockID, BlockRef{}, timeSec, [8]byte{}, 0, "AddEntry")
}

func (w *writerImpl) AddEntryV2(
	value any,
	traceID [16]byte,
	sourceRef string,
	blockRef BlockRef,
	timeSec uint64,
) error {
	return w.addRaw(value, traceID, sourceRef, 0, blockRef, timeSec, [8]byte{}, 0, "AddEntryV2")
}

func (w *writerImpl) AddEntryV4(
	value any,
	traceID [16]byte,
	sourceRef string,
	blockRef BlockRef,
	timeSec uint64,
	spanID [8]byte,
	rowIdx uint16,
) error {
	return w.addRaw(value, traceID, sourceRef, 0, blockRef, timeSec, spanID, rowIdx, "AddEntryV4")
}

func (w *writerImpl) addRaw(
	value any,
	traceID [16]byte,
	sourceRef string,
	blockID uint32,
	blockRef BlockRef,
	timeSec uint64,
	spanID [8]byte,
	rowIdx uint16,
	caller string,
) error {
	cv, err := CanonicalValue(w.colType, value)
	if err != nil {
		return fmt.Errorf("valueindex: %s: %w", caller, err)
	}
	vh := ValueHash16(cv)
	w.entries = append(w.entries, rawEntry{
		canonicalValue: cv,
		valueHash:      vh,
		traceID:        traceID,
		sourceRef:      sourceRef,
		blockID:        blockID,
		blockRef:       blockRef,
		timeSec:        timeSec,
		spanID:         spanID,
		rowIdx:         rowIdx,
	})
	if len(w.entries) >= shared.ValueIndexWriterSpillEntries {
		if err := w.spillRun(); err != nil {
			return err
		}
	}
	return nil
}

// spillRun sorts the current in-memory buffer and writes it to a new run temp file,
// then resets the buffer.
func (w *writerImpl) spillRun() error {
	if len(w.entries) == 0 {
		return nil
	}
	rf, err := writeRun(w.colType, w.entries)
	if err != nil {
		return fmt.Errorf("valueindex: AddEntry: spill: %w", err)
	}
	w.runs = append(w.runs, rf)
	w.entries = w.entries[:0]
	return nil
}

// Flush sorts, deduplicates, and serializes all buffered entries into a value index file.
// When entries were spilled to disk during AddEntry, the spilled runs are k-way merged
// with the in-memory tail; otherwise the in-memory fast path is taken (NOTE-VI-026).
func (w *writerImpl) Flush(_ context.Context, level uint8) ([]byte, error) {
	if len(w.runs) == 0 {
		// Fast path: everything fit in memory.
		sortRawSlice(w.colType, w.entries)
		w.entries = deduplicateEntries(w.entries)
		data, err := w.flushSorted(level)
		if err != nil {
			return nil, err
		}
		w.entries = w.entries[:0]
		return data, nil
	}

	// External sort-merge path.
	defer w.discardRuns()
	tail := w.entries
	data, err := w.assemble(level, func(yield func(rawEntry) error) error {
		return mergeRuns(w.colType, w.runs, tail, yield)
	})
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
	entries := w.entries
	return w.assemble(level, func(yield func(rawEntry) error) error {
		for i := range entries {
			if err := yield(entries[i]); err != nil {
				return err
			}
		}
		return nil
	})
}

// assemble drives the streaming construction of every value-index section from a
// source that yields rawEntry in final sorted, deduplicated order. Peak memory is
// bounded to one chunk plus the directory/hash-index (which scale with the number
// of distinct values, not entries) regardless of posting-list length.
//
// NOTE-VI-026 (issue #413): this is the shared serialization core used by both the
// in-memory fast path (flushSorted) and the external sort-merge path (flushMerged),
// eliminating the previous full []Entry copy and second sort pass that OOM-killed the
// consumer on high-volume low-cardinality columns.
func (w *writerImpl) assemble(level uint8, source func(yield func(rawEntry) error) error) ([]byte, error) {
	var (
		wallMin, wallMax uint64
		seen             bool
		prevHash         [16]byte
		havePrev         bool
		entryIdx         int
	)
	perChunk := shared.ValueIndexEntriesPerChunk
	ce := newChunkEncoder(perChunk)
	var hashEntries []HashEntry
	var anyBlockRef bool      // true if any entry carries a v2 BlockRef or SpanID
	var sortedEntries []Entry // collected for v4 string table encoding

	err := source(func(re rawEntry) error {
		if re.blockRef.PageNum > 0 || re.blockRef.LenPages > 0 || re.spanID != ([8]byte{}) {
			anyBlockRef = true
		}
		// Wall timestamps.
		if !seen || re.timeSec < wallMin {
			wallMin = re.timeSec
		}
		if !seen || re.timeSec > wallMax {
			wallMax = re.timeSec
		}
		seen = true

		// VHIX hash index: one entry per distinct valueHash, recording the chunk
		// index of its first occurrence. Entries arrive sorted by value, so equal
		// hashes are adjacent.
		chunkIdx := uint32(entryIdx / perChunk) //nolint:gosec // bounded by entry count
		if !havePrev || re.valueHash != prevHash {
			hashEntries = append(hashEntries, HashEntry{
				ValueHash: re.valueHash,
				ChunkIdx:  chunkIdx,
			})
			prevHash = re.valueHash
			havePrev = true
		}

		e := Entry{
			Value:     re.canonicalValue,
			TraceID:   re.traceID,
			SourceRef: re.sourceRef,
			BlockID:   re.blockID,
			BlockRef:  re.blockRef,
			TimeSec:   re.timeSec,
			SpanID:    re.spanID,
			RowIdx:    re.rowIdx,
		}
		ce.Add(e)
		sortedEntries = append(sortedEntries, e)
		entryIdx++
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("valueindex: assemble: %w", err)
	}

	chunkData, chunkDir := ce.Finish()

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
	// NOTE-VI-028 (#432): always use v3 when any BlockRef is present (new files are v2+).
	// v3 adds a string table for SourceRef deduplication.
	var vinxSection []byte
	if anyBlockRef {
		// Build string table for SourceRef deduplication.
		table := NewStringTable()
		// Always use v4 (string table + SpanID + RowIdx). v4 supplants v3 (#432/#428).
		v4ChunkData, v4ChunkDir, v4Err := encodeEntriesV4(sortedEntries, table)
		if v4Err != nil {
			// Fallback to v2 on encode error.
			vinxSection = encodeVINXSectionVer(chunkDir, chunkData, shared.ValueIndexEntriesVersion)
		} else {
			vinxSection = encodeVINXSectionVer4(v4ChunkDir, v4ChunkData, table)
		}
	} else {
		vinxSection = encodeVINXSectionVer(chunkDir, chunkData, shared.ValueIndexEntriesVersionV1)
	}

	// Footer offsets.
	vimtOff := uint64(0)
	vhixOff := vimtOff + uint64(len(vimtBytes))
	vinxOff := vhixOff + uint64(len(vhixBytes))

	// Assemble: VIMT | VHIX | VINX | footer. (VKLL section removed in #435)
	out := make([]byte, 0, len(vimtBytes)+len(vhixBytes)+len(vinxSection)+shared.ValueIndexFooterSize)
	out = append(out, vimtBytes...)
	out = append(out, vhixBytes...)
	out = append(out, vinxSection...)

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

// encodeVINXSectionVer encodes the full VINX section with an explicit version.
func encodeVINXSectionVer(dir []ChunkDirEntry, chunkData []byte, ver uint8) []byte {
	const headerSize = 28
	dirSize := len(dir) * chunkDirEntrySize
	buf := make([]byte, headerSize+dirSize+len(chunkData))

	binary.LittleEndian.PutUint32(buf[0:], shared.ValueIndexEntriesMagic)
	buf[4] = ver
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

// encodeEntriesV4 encodes entries using v4 format (NOTE-VI-029, issue #428).
// encodeEntriesV4 is the only write-side encoder for anyBlockRef=true files.
// v3 is read-only (backward compat); v4 supplants v3 (same layout + SpanID+RowIdx).
func encodeEntriesV4(entries []Entry, table *StringTable) ([]byte, []ChunkDirEntry, error) {
	if len(entries) == 0 {
		return []byte{}, nil, nil
	}
	for i := range entries {
		table.Intern(entries[i].SourceRef)
	}
	perChunk := shared.ValueIndexEntriesPerChunk
	var allChunks []byte
	var dir []ChunkDirEntry
	for start := 0; start < len(entries); start += perChunk {
		end := start + perChunk
		if end > len(entries) {
			end = len(entries)
		}
		chunk := entries[start:end]
		raw := encodeChunkPayloadV4(chunk, table)
		compressed := snappy.Encode(nil, raw)
		var minValue [8]byte
		if len(chunk[0].Value) >= 8 {
			copy(minValue[:], chunk[0].Value)
		}
		maxTimeSec := chunk[0].TimeSec
		for i := range chunk {
			if chunk[i].TimeSec > maxTimeSec {
				maxTimeSec = chunk[i].TimeSec
			}
		}
		dir = append(dir, ChunkDirEntry{
			MinValue:   minValue,
			MinTimeSec: chunk[0].TimeSec,
			MaxTimeSec: maxTimeSec,
			CompOff:    uint32(len(allChunks)),  //nolint:gosec
			CompLen:    uint32(len(compressed)), //nolint:gosec
		})
		allChunks = append(allChunks, compressed...)
	}
	return allChunks, dir, nil
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
			cur.blockRef == prev.blockRef &&
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

// encodeVINXSectionVer4 encodes a v4 VINX section (v3 layout + v4 version byte).
// v4 adds SpanID[8]+RowIdx[2] per entry in addition to v3 features.
func encodeVINXSectionVer4(dir []ChunkDirEntry, chunkData []byte, table *StringTable) []byte {
	// Same layout as V3 but with version byte 0x04.
	const headerSize = 28
	tableBytes := EncodeStringTable(table)
	dirSize := len(dir) * chunkDirEntryV3Size
	buf := make([]byte, headerSize+len(tableBytes)+dirSize+len(chunkData))

	binary.LittleEndian.PutUint32(buf[0:], shared.ValueIndexEntriesMagic)
	buf[4] = shared.ValueIndexEntriesVersionV4
	// buf[5:8] reserved
	binary.LittleEndian.PutUint32(buf[8:], uint32(len(chunkData)))                    //nolint:gosec
	binary.LittleEndian.PutUint32(buf[12:], uint32(len(dir)))                         //nolint:gosec
	binary.LittleEndian.PutUint16(buf[16:], uint16(shared.ValueIndexEntriesPerChunk)) //nolint:gosec
	binary.LittleEndian.PutUint32(buf[18:], uint32(len(tableBytes)))                  //nolint:gosec
	// buf[22:28] reserved

	pos := headerSize
	copy(buf[pos:], tableBytes)
	pos += len(tableBytes)
	for _, de := range dir {
		copy(buf[pos:], de.MinValue[:])
		binary.LittleEndian.PutUint64(buf[pos+8:], de.MinTimeSec)
		binary.LittleEndian.PutUint64(buf[pos+16:], de.MaxTimeSec)
		binary.LittleEndian.PutUint32(buf[pos+24:], de.CompOff)
		binary.LittleEndian.PutUint32(buf[pos+28:], de.CompLen)
		pos += chunkDirEntryV3Size
	}
	copy(buf[pos:], chunkData)
	return buf
}
