package cube

// NOTE: SPEC-CUBE-009 — Reader provides random access via binary search on sorted cells.
// O(log chunks) directory scan + O(log cells_per_chunk) binary search within chunk.

import (
	"fmt"
	"os"
	"sort"
)

// Reader provides random access to cells by (minute, dim1, dim2).
type Reader struct {
	dict        *Dictionary
	data        []byte
	dir         []ChunkDirEntry
	chunksStart uint64
	header      Header
	bytesRead   int64
}

// BytesRead returns the exact byte count of the encoded cube file this Reader was opened from
// (SPEC-CUBE-030) — used tempo-side to attribute cube-answered query cost (issue #218).
func (r *Reader) BytesRead() int64 { return r.bytesRead }

// NumAggAttrs returns this file's declared per-cell aggAttr count (0 for a pure-count cube).
// E-6b/E-10 use this to cross-check an opened file's actual shape against its RegistryEntry via
// ValidateFileMatchesRegistry (APPENDIX 3's registry-vs-file consistency check).
func (r *Reader) NumAggAttrs() uint8 { return r.header.NumAggAttrs }

// OpenReader opens a cube file for reading.
func OpenReader(path string) (*Reader, error) {
	data, err := os.ReadFile(path) //nolint:gosec // path is an internal, trusted cube file location
	if err != nil {
		return nil, fmt.Errorf("cube: read file: %w", err)
	}
	if len(data) < HeaderSize+FooterSize {
		return nil, fmt.Errorf("cube: file too short (%d bytes)", len(data))
	}

	// Decode header
	header, err := DecodeHeader(data[:HeaderSize])
	if err != nil {
		return nil, fmt.Errorf("cube: decode header: %w", err)
	}

	// Decode footer
	footerStart := len(data) - FooterSize
	footer, err := DecodeFooter(data[footerStart:])
	if err != nil {
		return nil, fmt.Errorf("cube: decode footer: %w", err)
	}

	// Decode dictionary
	dictEnd := footer.ChunksOffset
	if footer.DictOffset >= uint64(len(data)) || dictEnd > uint64(len(data)) {
		return nil, fmt.Errorf("cube: dictionary offsets out of bounds")
	}
	dict, err := DecodeDictionary(data[footer.DictOffset:dictEnd])
	if err != nil {
		return nil, fmt.Errorf("cube: decode dictionary: %w", err)
	}

	// Decode chunk directory
	dirEnd := uint64(footerStart) //nolint:gosec // footerStart = len(data)-FooterSize >= 0 (checked above)
	if footer.DirOffset >= uint64(len(data)) || dirEnd > uint64(len(data)) {
		return nil, fmt.Errorf("cube: chunk directory offsets out of bounds")
	}
	dir, err := DecodeChunkDirectory(data[footer.DirOffset:dirEnd])
	if err != nil {
		return nil, fmt.Errorf("cube: decode chunk directory: %w", err)
	}

	return &Reader{
		data:        data,
		header:      header,
		dict:        dict,
		dir:         dir,
		chunksStart: footer.ChunksOffset,
		bytesRead:   int64(len(data)),
	}, nil
}

// OpenReaderFromBytes opens a cube Reader from in-memory bytes (e.g. downloaded from S3).
func OpenReaderFromBytes(data []byte) (*Reader, error) {
	if len(data) < HeaderSize+FooterSize {
		return nil, fmt.Errorf("cube: file too short (%d bytes)", len(data))
	}
	header, err := DecodeHeader(data[:HeaderSize])
	if err != nil {
		return nil, fmt.Errorf("cube: decode header: %w", err)
	}
	footerStart := len(data) - FooterSize
	footer, err := DecodeFooter(data[footerStart:])
	if err != nil {
		return nil, fmt.Errorf("cube: decode footer: %w", err)
	}
	dictEnd := footer.ChunksOffset
	if footer.DictOffset >= uint64(len(data)) || dictEnd > uint64(len(data)) {
		return nil, fmt.Errorf("cube: dictionary offsets out of bounds")
	}
	dict, err := DecodeDictionary(data[footer.DictOffset:dictEnd])
	if err != nil {
		return nil, fmt.Errorf("cube: decode dictionary: %w", err)
	}
	dirEnd := uint64(footerStart) //nolint:gosec
	if footer.DirOffset >= uint64(len(data)) || dirEnd > uint64(len(data)) {
		return nil, fmt.Errorf("cube: chunk directory offsets out of bounds")
	}
	dir, err := DecodeChunkDirectory(data[footer.DirOffset:dirEnd])
	if err != nil {
		return nil, fmt.Errorf("cube: decode chunk directory: %w", err)
	}
	return &Reader{
		data:        data,
		header:      header,
		dict:        dict,
		dir:         dir,
		chunksStart: footer.ChunksOffset,
		bytesRead:   int64(len(data)),
	}, nil
}

// GetCell returns the count for (minute, dim1, dim2), or (0, false) if not present.
// IMPORTANT: Uses Dictionary reverse maps (O(1)) for dim-ID lookup, not linear scan.
// See GetAggCell for the full-fidelity record (all per-aggAttr values, not just Count).
func (r *Reader) GetCell(minute uint32, dim1, dim2 string) (uint32, bool) {
	ac, ok := r.GetAggCell(minute, dim1, dim2)
	if !ok {
		return 0, false
	}
	return ac.Count, true
}

// GetAggCell returns the complete AggCell (base fields + every per-aggAttr record) for
// (minute, dim1, dim2), or (AggCell{}, false) if not present.
func (r *Reader) GetAggCell(minute uint32, dim1, dim2 string) (AggCell, bool) {
	// Map dim1, dim2 → uint16 IDs via O(1) dictionary lookup
	dim1ID, ok1 := r.lookupDim1ID(dim1)
	dim2ID, ok2 := r.lookupDim2ID(dim2)
	if !ok1 || !ok2 {
		return AggCell{}, false
	}

	// Find chunk that might contain this minute
	chunkIdx := findChunkForMinute(r.dir, minute)
	if chunkIdx < 0 {
		return AggCell{}, false
	}

	// Decompress and search this chunk (and potentially next chunks)
	for i := chunkIdx; i < len(r.dir); i++ {
		cells, err := r.decompressChunk(i)
		if err != nil {
			return AggCell{}, false
		}
		if len(cells) == 0 {
			continue
		}
		// If this chunk starts after target minute, we're done
		if cells[0].Minute > minute {
			break
		}

		// Binary search within chunk for (minute, dim1ID, dim2ID)
		target := AggCell{Minute: minute, Dim1ID: dim1ID, Dim2ID: dim2ID}
		idx := sort.Search(len(cells), func(j int) bool {
			return CompareAggCell(cells[j], target) >= 0
		})
		if idx < len(cells) &&
			cells[idx].Minute == minute &&
			cells[idx].Dim1ID == dim1ID &&
			cells[idx].Dim2ID == dim2ID {
			return cells[idx], true
		}
	}
	return AggCell{}, false
}

// GetCellsInRange returns all AggCells for (dim1, dim2) in [minMinute, maxMinute]. Delegates to
// GetAggCellsInRange — after E-3 APPENDIX 2 flattened AggCell into the sole cell type, these two
// names would otherwise be pointless duplicate accessors with identical signatures; keeping both
// names alive (rather than deleting one) avoids forcing an unrelated, in-flight task's call site
// to change its import surface for a purely mechanical rename.
func (r *Reader) GetCellsInRange(minMinute, maxMinute uint32, dim1, dim2 string) ([]AggCell, error) {
	return r.GetAggCellsInRange(minMinute, maxMinute, dim1, dim2)
}

// GetAggCellsInRange returns all AggCells (base fields + every per-aggAttr record) for
// (dim1, dim2) in [minMinute, maxMinute].
func (r *Reader) GetAggCellsInRange(minMinute, maxMinute uint32, dim1, dim2 string) ([]AggCell, error) {
	// Map dim1, dim2 → uint16 IDs via O(1) dictionary lookup
	dim1ID, ok1 := r.lookupDim1ID(dim1)
	dim2ID, ok2 := r.lookupDim2ID(dim2)
	if !ok1 || !ok2 {
		return nil, nil
	}

	var result []AggCell
	for i := range r.dir {
		// Skip chunks that start after maxMinute
		if r.dir[i].MinMinute > maxMinute {
			continue
		}

		cells, err := r.decompressChunk(i)
		if err != nil {
			return nil, fmt.Errorf("cube: decompress chunk %d: %w", i, err)
		}

		for j := range cells {
			c := &cells[j]
			if c.Minute >= minMinute &&
				c.Minute <= maxMinute &&
				c.Dim1ID == dim1ID &&
				c.Dim2ID == dim2ID {
				result = append(result, cells[j])
			}
		}
	}
	return result, nil
}

// lookupDim1ID uses the Dictionary reverse map (O(1)) to find the ID for a dim1 value.
func (r *Reader) lookupDim1ID(value string) (uint16, bool) {
	return r.dict.LookupID1(value)
}

// lookupDim2ID uses the Dictionary reverse map (O(1)) to find the ID for a dim2 value.
func (r *Reader) lookupDim2ID(value string) (uint16, bool) {
	return r.dict.LookupID2(value)
}

// decompressChunk decodes a chunk into AggCells (base fields + every per-aggAttr record) —
// AggCell is the sole cell type (E-3 APPENDIX 2), so there is no separate narrower decode path.
func (r *Reader) decompressChunk(idx int) ([]AggCell, error) {
	if idx < 0 || idx >= len(r.dir) {
		return nil, fmt.Errorf("cube: chunk index %d out of range", idx)
	}
	de := &r.dir[idx]
	start := r.chunksStart + uint64(de.CompOff)
	end := start + uint64(de.CompLen)
	if start >= uint64(len(r.data)) || end > uint64(len(r.data)) {
		return nil, fmt.Errorf("cube: chunk %d out of bounds", idx)
	}
	return DecodeChunk(r.data[start:end], int(r.header.NumAggAttrs))
}
