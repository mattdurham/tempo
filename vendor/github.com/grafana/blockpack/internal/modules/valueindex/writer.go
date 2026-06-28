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
	// AddEntry records one observation: the span with traceID was at blockRef (the v2
	// page-aligned file locator; NOTE-V2-002) within sourceRef at timeSec and had
	// vi:value = value. value must match the col_type given to NewWriter.
	AddEntry(value any, traceID [16]byte, sourceRef string, blockRef shared.BlockFileRef, timeSec uint64) error

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
	blockRef       shared.BlockFileRef
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
func (w *writerImpl) AddEntry(
	value any,
	traceID [16]byte,
	sourceRef string,
	blockRef shared.BlockFileRef,
	timeSec uint64,
) error {
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
		blockRef:       blockRef,
		timeSec:        timeSec,
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
	kll := newKLLBuilder(w.colType)
	var hashEntries []HashEntry

	err := source(func(re rawEntry) error {
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

		kll.add(re.canonicalValue)
		ce.Add(Entry{
			Value:     re.canonicalValue,
			TraceID:   re.traceID,
			SourceRef: re.sourceRef,
			BlockRef:  re.blockRef,
			TimeSec:   re.timeSec,
		})
		entryIdx++
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("valueindex: assemble: %w", err)
	}

	kllBytes, err := kll.finish()
	if err != nil {
		return nil, fmt.Errorf("valueindex: assemble: KLL: %w", err)
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

// kllBuilder accumulates canonical values into a type-appropriate KLL sketch one
// at a time and encodes the VKLL section on Finish. It lets the writer feed the
// sketch from a streaming merge without materializing the full entry slice
// (NOTE-VI-026, issue #413).
type kllBuilder struct {
	add    func(canonicalValue []byte)
	finish func() ([]byte, error)
}

// newKLLBuilder returns a streaming KLL builder for the given column type, mirroring
// the type dispatch of the old buildKLL.
func newKLLBuilder(colType shared.ColumnType) *kllBuilder {
	switch colType {
	case shared.ColumnTypeString, shared.ColumnTypeRangeString:
		sk := writer.NewKLLWithK[string](shared.ValueIndexKLLK)
		return &kllBuilder{
			add:    func(cv []byte) { sk.Add(string(cv)) },
			finish: func() ([]byte, error) { return EncodeKLLSection(sk, colType) },
		}
	case shared.ColumnTypeUint64, shared.ColumnTypeInt64, shared.ColumnTypeFloat64,
		shared.ColumnTypeRangeUint64, shared.ColumnTypeRangeInt64,
		shared.ColumnTypeRangeDuration, shared.ColumnTypeRangeFloat64:
		// NOTE-VI-012: Range* numeric types use the same uint64 bit-pattern KLL as their
		// scalar equivalents; compareCanonical handles type-correct ordering at query time.
		sk := writer.NewKLLWithK[uint64](shared.ValueIndexKLLK)
		return &kllBuilder{
			add: func(cv []byte) {
				if len(cv) >= 8 {
					sk.Add(binary.LittleEndian.Uint64(cv[:8]))
				}
			},
			finish: func() ([]byte, error) { return EncodeKLLSection(sk, colType) },
		}
	default:
		sk := writer.NewKLLWithK[string](shared.ValueIndexKLLK)
		return &kllBuilder{
			add:    func([]byte) {},
			finish: func() ([]byte, error) { return EncodeKLLSection(sk, colType) },
		}
	}
}

// deduplicateEntries removes entries with identical (valueHash, traceID, sourceRef, blockRef, timeSec).
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
