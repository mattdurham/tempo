package cube

// NOTE: SPEC-CUBE-010 — Writer accumulates cells, sorts, and flushes to a cube file.

import (
	"fmt"
	"os"
	"sort"
)

// Writer accumulates AggCells and flushes to a cube file. AggCell is the sole cell type (E-3
// APPENDIX 2) — AddCell and AddAggCell are two convenience constructor METHODS over that one
// type (this does not reintroduce type coexistence, only the type duality was vetoed), sharing
// one internal buffer. Every cell added to a given Writer must carry the same aggAttr count —
// per ruling 3's amendment, a cube's attribute set is fixed for its whole life.
type Writer struct {
	dict           *Dictionary
	cells          []AggCell
	cubeID         [16]byte
	numAggAttrs    int
	resolution     uint32
	numAggAttrsSet bool // whether numAggAttrs has been established by the first Add call yet
}

// NewWriter creates a new Writer.
func NewWriter(cubeID [16]byte, resolution uint32) *Writer {
	return &Writer{
		cubeID:     cubeID,
		resolution: resolution,
		cells:      []AggCell{},
		dict:       NewDictionary(),
	}
}

// AddCell increments the count for (minute, dim1, dim2). This is the plain-count entry point,
// unchanged in behavior since before #491 — it stays the only path a numAggAttrs==0 caller ever
// needs.
func (w *Writer) AddCell(minute uint32, dim1, dim2 string, count uint32) error {
	return w.addCell(minute, dim1, dim2, count, nil)
}

// AddAggCell adds one cell with its full per-aggAttr values (Sum/Min/Max/Buckets per attribute),
// in RegistryEntry.AggAttrs order. Every call on a given Writer must pass the same len(aggs) —
// per ruling 3's amendment, a cube's attribute set is fixed for its whole life.
func (w *Writer) AddAggCell(minute uint32, dim1, dim2 string, count uint32, aggs []AggAttrValues) error {
	return w.addCell(minute, dim1, dim2, count, aggs)
}

// addCell is the shared implementation behind AddCell/AddAggCell.
func (w *Writer) addCell(minute uint32, dim1, dim2 string, count uint32, aggs []AggAttrValues) error {
	if !w.numAggAttrsSet {
		w.numAggAttrs = len(aggs)
		w.numAggAttrsSet = true
	} else if len(aggs) != w.numAggAttrs {
		return fmt.Errorf(
			"cube: cell aggAttr count changed mid-writer (got %d, want %d) — a cube's attribute set is fixed for its whole life",
			len(aggs),
			w.numAggAttrs,
		)
	}
	dim1ID, err := w.dict.InternDim1(dim1)
	if err != nil {
		return fmt.Errorf("cube: intern dim1: %w", err)
	}
	dim2ID, err := w.dict.InternDim2(dim2)
	if err != nil {
		return fmt.Errorf("cube: intern dim2: %w", err)
	}
	w.cells = append(w.cells, AggCell{
		Minute: minute,
		Dim1ID: dim1ID,
		Dim2ID: dim2ID,
		Count:  count,
		Aggs:   aggs,
	})
	return nil
}

// AddCellRaw appends a pre-constructed AggCell (for testing).
func (w *Writer) AddCellRaw(c AggCell) {
	w.cells = append(w.cells, c)
}

// InternDim1 pre-interns a dim1 value (for testing).
func (w *Writer) InternDim1(value string) error {
	_, err := w.dict.InternDim1(value)
	return err
}

// InternDim2 pre-interns a dim2 value (for testing).
func (w *Writer) InternDim2(value string) error {
	_, err := w.dict.InternDim2(value)
	return err
}

// Encode serializes the accumulated cells into the in-memory cube file format
// (#442 layout: Header + Dictionary + snappy chunks + ChunkDirectory + Footer).
// Returns an error if no cells have been added. Cells are sorted in place.
func (w *Writer) Encode() ([]byte, error) {
	if len(w.cells) == 0 {
		return nil, fmt.Errorf("cube: cannot encode empty writer")
	}

	// Sort cells by (Minute, Dim1ID, Dim2ID)
	sort.Slice(w.cells, func(i, j int) bool {
		return CompareAggCell(w.cells[i], w.cells[j]) < 0
	})

	// Split cells into chunks
	chunks, dir, err := SplitCellsIntoChunks(w.cells, ChunkSizeNominal, w.numAggAttrs)
	if err != nil {
		return nil, fmt.Errorf("cube: split cells: %w", err)
	}

	// Build file sections
	header := Header{
		Magic:       MagicCube,
		Version:     VersionCube,
		NumAggAttrs: uint8(w.numAggAttrs), //nolint:gosec // G115: numAggAttrs is a small, bounded attribute-set size
		CubeID:      w.cubeID,
		MinMinute:   w.cells[0].Minute,
		MaxMinute:   w.cells[len(w.cells)-1].Minute,
		Resolution:  w.resolution,
	}
	headerBytes := EncodeHeader(header)
	dictBytes := EncodeDictionary(w.dict)

	// Concatenate chunks
	var chunksBytes []byte
	for _, chunk := range chunks {
		chunksBytes = append(chunksBytes, chunk...)
	}

	dirBytes := EncodeChunkDirectory(dir)

	// Build footer
	dictOffset := uint64(len(headerBytes))
	chunksOffset := dictOffset + uint64(len(dictBytes))
	dirOffset := chunksOffset + uint64(len(chunksBytes))

	footer := Footer{
		Magic:        MagicCube,
		Version:      VersionCube,
		CellCount:    uint64(len(w.cells)),
		DictOffset:   dictOffset,
		ChunksOffset: chunksOffset,
		DirOffset:    dirOffset,
	}
	footerBytes := EncodeFooter(footer)

	out := make([]byte, 0, len(headerBytes)+len(dictBytes)+len(chunksBytes)+len(dirBytes)+len(footerBytes))
	out = append(out, headerBytes...)
	out = append(out, dictBytes...)
	out = append(out, chunksBytes...)
	out = append(out, dirBytes...)
	out = append(out, footerBytes...)
	return out, nil
}

// Flush encodes the accumulated cells and writes the cube file atomically
// (temp file + rename).
func (w *Writer) Flush(path string) error {
	data, err := w.Encode()
	if err != nil {
		return err
	}

	// Write to temp file, then rename (atomic)
	tmpPath := path + ".tmp"
	f, err := os.Create(tmpPath) //nolint:gosec // tmpPath derives from an internal, trusted cube file location
	if err != nil {
		return fmt.Errorf("cube: create temp file: %w", err)
	}
	defer func() {
		_ = f.Close()
		_ = os.Remove(tmpPath)
	}()

	if _, err := f.Write(data); err != nil {
		return fmt.Errorf("cube: write file: %w", err)
	}

	if err := f.Sync(); err != nil {
		return fmt.Errorf("cube: sync: %w", err)
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("cube: close: %w", err)
	}

	if err := os.Rename(tmpPath, path); err != nil {
		return fmt.Errorf("cube: rename: %w", err)
	}

	return nil
}
