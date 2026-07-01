package cube

// NOTE: SPEC-CUBE-010 — Writer accumulates cells, sorts, and flushes to a cube file.

import (
	"fmt"
	"os"
	"sort"
)

// Writer accumulates cells and flushes to a cube file.
type Writer struct {
	dict       *Dictionary
	cells      []Cell
	cubeID     [16]byte
	resolution uint32
}

// NewWriter creates a new Writer.
func NewWriter(cubeID [16]byte, resolution uint32) *Writer {
	return &Writer{
		cubeID:     cubeID,
		resolution: resolution,
		cells:      []Cell{},
		dict:       NewDictionary(),
	}
}

// AddCell increments the count for (minute, dim1, dim2).
func (w *Writer) AddCell(minute uint32, dim1, dim2 string, count uint32) error {
	dim1ID, err := w.dict.InternDim1(dim1)
	if err != nil {
		return fmt.Errorf("cube: intern dim1: %w", err)
	}
	dim2ID, err := w.dict.InternDim2(dim2)
	if err != nil {
		return fmt.Errorf("cube: intern dim2: %w", err)
	}
	w.cells = append(w.cells, Cell{
		Minute: minute,
		Dim1ID: dim1ID,
		Dim2ID: dim2ID,
		Count:  count,
	})
	return nil
}

// AddCellRaw appends a pre-constructed Cell (for testing).
func (w *Writer) AddCellRaw(c Cell) {
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
		return CompareCell(w.cells[i], w.cells[j]) < 0
	})

	// Split cells into chunks
	chunks, dir, err := SplitCellsIntoChunks(w.cells, ChunkSizeNominal)
	if err != nil {
		return nil, fmt.Errorf("cube: split cells: %w", err)
	}

	// Build file sections
	header := Header{
		Magic:      MagicCube,
		Version:    VersionCube,
		CubeID:     w.cubeID,
		MinMinute:  w.cells[0].Minute,
		MaxMinute:  w.cells[len(w.cells)-1].Minute,
		Resolution: w.resolution,
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
