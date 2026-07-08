package cube

// NOTE: SPEC-CUBE-007, SPEC-CUBE-008 — Chunk format: snappy-compressed 12-byte cells (2048 nominal).
// Directory: per chunk (min_minute[4] + comp_off[4] + comp_len[4]).

import (
	"encoding/binary"
	"fmt"
	"sort"

	"github.com/golang/snappy"
)

// ChunkSizeNominal is the nominal number of cells per chunk before snappy compression.
// SPEC-CUBE-007: matches the value-index 2048-record chunk size for balanced
// decompression cost vs memory footprint.
const ChunkSizeNominal = 2048 // cells per chunk

// recordWidthFor is the SINGLE place the full cell-record-width arithmetic lives (ruling 5's
// single-source-of-truth requirement, E-3): the base 12-byte record plus numAggAttrs 540-byte
// AggAttrValues records. Called by AggCell's codecs (cell.go) and, once wired, the cardinality
// gate's byte-cost formula (E-7) — never an independently-maintained copy of this formula.
func recordWidthFor(numAggAttrs int) int {
	return 12 + numAggAttrs*AggAttrRecordWidth
}

// ChunkDirEntry is one entry in the chunk directory.
type ChunkDirEntry struct {
	MinMinute uint32 // first cell's minute in this chunk
	CompOff   uint32 // byte offset from chunks_section_start
	CompLen   uint32 // snappy-compressed length
}

// EncodeChunk serializes AggCells into a snappy-compressed chunk payload. Cells MUST be sorted by
// (Minute, Dim1ID, Dim2ID). Every cell's len(Aggs) must equal numAggAttrs, which must match the
// owning file's Header.NumAggAttrs; for the plain-count case (numAggAttrs==0, len(Aggs)==0) this
// is byte-identical to the pre-#491 fixed-12-byte format (AggCell is the SOLE cell type — E-3
// APPENDIX 2 — there is no separate plain-Cell-shaped codec to keep in sync with this one).
func EncodeChunk(cells []AggCell, numAggAttrs int) ([]byte, error) {
	if len(cells) == 0 {
		return snappy.Encode(nil, []byte{}), nil
	}
	width := recordWidthFor(numAggAttrs)
	raw := make([]byte, 0, len(cells)*width)
	for i := range cells {
		if len(cells[i].Aggs) != numAggAttrs {
			return nil, fmt.Errorf(
				"cube: cell %d has %d aggAttrs, want %d",
				i,
				len(cells[i].Aggs),
				numAggAttrs,
			)
		}
		raw = append(raw, EncodeAggCell(cells[i])...)
	}
	return snappy.Encode(nil, raw), nil
}

// DecodeChunk decompresses and parses a chunk payload into full-fidelity AggCells (base fields +
// every per-aggAttr record). numAggAttrs must match the owning file's Header.NumAggAttrs.
func DecodeChunk(compressed []byte, numAggAttrs int) ([]AggCell, error) {
	raw, err := snappy.Decode(nil, compressed)
	if err != nil {
		return nil, fmt.Errorf("cube: snappy decode: %w", err)
	}
	width := recordWidthFor(numAggAttrs)
	if len(raw)%width != 0 {
		return nil, fmt.Errorf("cube: chunk size not multiple of %d (%d bytes)", width, len(raw))
	}
	count := len(raw) / width
	if count > 65536 {
		return nil, fmt.Errorf("cube: chunk cell count %d exceeds limit 65536", count)
	}
	cells := make([]AggCell, count)
	for i := 0; i < count; i++ {
		ac, err := DecodeAggCell(raw[i*width:(i+1)*width], numAggAttrs)
		if err != nil {
			return nil, fmt.Errorf("cube: aggCell %d: %w", i, err)
		}
		cells[i] = ac
	}
	return cells, nil
}

// EncodeChunkDirectory serializes a chunk directory to bytes.
// Wire: dir_count[4] + (min_minute[4] + comp_off[4] + comp_len[4])* per entry
func EncodeChunkDirectory(dir []ChunkDirEntry) []byte {
	buf := make([]byte, 0, 4+len(dir)*12)
	buf = binary.LittleEndian.AppendUint32(buf, uint32(len(dir))) //nolint:gosec // dir count bounded by chunk limits
	for i := range dir {
		buf = binary.LittleEndian.AppendUint32(buf, dir[i].MinMinute)
		buf = binary.LittleEndian.AppendUint32(buf, dir[i].CompOff)
		buf = binary.LittleEndian.AppendUint32(buf, dir[i].CompLen)
	}
	return buf
}

// DecodeChunkDirectory parses a chunk directory from bytes.
func DecodeChunkDirectory(buf []byte) ([]ChunkDirEntry, error) {
	if len(buf) < 4 {
		return nil, fmt.Errorf("cube: chunk directory buffer too short")
	}
	count := int(binary.LittleEndian.Uint32(buf[0:4]))
	if count > 65536 {
		return nil, fmt.Errorf("cube: chunk directory count %d exceeds limit 65536", count)
	}
	if len(buf) < 4+count*12 {
		return nil, fmt.Errorf(
			"cube: chunk directory truncated (need %d bytes, have %d)",
			4+count*12,
			len(buf),
		)
	}
	dir := make([]ChunkDirEntry, count)
	pos := 4
	for i := 0; i < count; i++ {
		dir[i] = ChunkDirEntry{
			MinMinute: binary.LittleEndian.Uint32(buf[pos:]),
			CompOff:   binary.LittleEndian.Uint32(buf[pos+4:]),
			CompLen:   binary.LittleEndian.Uint32(buf[pos+8:]),
		}
		pos += 12
	}
	return dir, nil
}

// SplitCellsIntoChunks splits sorted AggCells into chunks of up to chunkSize cells each.
// Returns chunk payloads and directory entries. See EncodeChunk for numAggAttrs semantics.
func SplitCellsIntoChunks(cells []AggCell, chunkSize int, numAggAttrs int) ([][]byte, []ChunkDirEntry, error) {
	if chunkSize <= 0 {
		chunkSize = ChunkSizeNominal
	}
	if len(cells) == 0 {
		return nil, nil, nil
	}

	var chunks [][]byte
	var dir []ChunkDirEntry
	compOff := uint32(0)

	for start := 0; start < len(cells); start += chunkSize {
		end := start + chunkSize
		if end > len(cells) {
			end = len(cells)
		}
		chunk := cells[start:end]
		compressed, err := EncodeChunk(chunk, numAggAttrs)
		if err != nil {
			return nil, nil, fmt.Errorf("cube: encode chunk %d: %w", len(dir), err)
		}
		chunks = append(chunks, compressed)
		dir = append(dir, ChunkDirEntry{
			MinMinute: chunk[0].Minute,
			CompOff:   compOff,
			CompLen:   uint32(len(compressed)), //nolint:gosec // snappy output of <=24KB raw fits uint32
		})
		compOff += uint32(len(compressed)) //nolint:gosec // snappy output of <=24KB raw fits uint32
	}
	return chunks, dir, nil
}

// findChunkForMinute returns the index of the chunk that should contain the target minute.
// Uses binary search on the chunk directory sorted by MinMinute.
// Returns -1 if no suitable chunk is found.
func findChunkForMinute(dir []ChunkDirEntry, targetMinute uint32) int {
	if len(dir) == 0 {
		return -1
	}

	// Binary search for first chunk where MinMinute >= targetMinute
	idx := sort.Search(len(dir), func(i int) bool {
		return dir[i].MinMinute >= targetMinute
	})

	// If exact match or past end, use that chunk
	if idx < len(dir) && dir[idx].MinMinute == targetMinute {
		return idx
	}

	// If search went past end, use last chunk
	if idx >= len(dir) {
		return len(dir) - 1
	}

	// Otherwise, use previous chunk (target might be in it)
	if idx > 0 {
		return idx - 1
	}

	// Target is before first chunk
	return 0
}
