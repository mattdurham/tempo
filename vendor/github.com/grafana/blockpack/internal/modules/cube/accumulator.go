package cube

// NOTE: SPEC-CUBE-011 — Accumulator counts spans into per-(dim1,dim2) cells for a single
// minute bucket entirely in memory, then encodes one #442 cube file at flush. No per-span
// I/O. The minute is fixed at construction; the caller rotates accumulators per minute.

import (
	"encoding/hex"
	"fmt"
	"path"

	"github.com/rs/xid"
)

// SpanValues abstracts the per-span field access the accumulator needs, decoupling
// it from any concrete span representation. Implementations return (value, true) when
// the column is present on the span and (_, false) when absent.
type SpanValues interface {
	String(column string) (string, bool)
	Int64(column string) (int64, bool)
}

// Filter reports whether a span should be counted into the cube. A nil Filter
// (or a cube with no filters) counts every span that carries both dimensions.
type Filter func(SpanValues) bool

// FilterOp is the comparison used by NumericFilter.
type FilterOp uint8

// Numeric filter comparison operators.
const (
	FilterOpLess FilterOp = iota
	FilterOpLessEqual
	FilterOpGreater
	FilterOpGreaterEqual
	FilterOpEqual
)

// NumericFilter builds a Filter that keeps a span only when its int64 column
// satisfies (value op threshold). A span missing the column is rejected (the
// filter cannot be satisfied), matching the ticket's "duration < threshold does
// not increment a duration-filtered cube" acceptance case.
func NumericFilter(column string, op FilterOp, threshold int64) Filter {
	return func(s SpanValues) bool {
		v, ok := s.Int64(column)
		if !ok {
			return false
		}
		switch op {
		case FilterOpLess:
			return v < threshold
		case FilterOpLessEqual:
			return v <= threshold
		case FilterOpGreater:
			return v > threshold
		case FilterOpGreaterEqual:
			return v >= threshold
		case FilterOpEqual:
			return v == threshold
		default:
			return false
		}
	}
}

// Definition is the minimal cube description the accumulator needs. The full
// registry (dimensions, lifecycle, cardinality gate) is issue #444; this carries
// only what ingest requires.
type Definition struct {
	Dim1Column string   // first dimension column name (e.g. "service.name")
	Dim2Column string   // second dimension column name (e.g. "status_code")
	Filters    []Filter // span must satisfy ALL filters to be counted
	ID         [16]byte // identifies the cube definition this file belongs to
	Resolution uint32   // minutes per bucket (1 for L0)
}

// ObjectPutter writes an encoded object to backing storage (S3/GCS/etc).
// Mirrors the valueindexconsumer ObjectPutter so the same store doubles can be reused.
type ObjectPutter interface {
	Put(path string, data []byte) error
}

// cellKey identifies one accumulated cell within a single minute bucket.
type cellKey struct {
	dim1 uint16
	dim2 uint16
}

// Accumulator counts spans into cells for one minute bucket. Not safe for
// concurrent use — the ingest loop owns one accumulator per active cube.
type Accumulator struct {
	dict   *Dictionary
	cells  map[cellKey]uint32
	def    Definition
	minute uint32
}

// NewAccumulator creates an accumulator for the given cube definition and minute.
func NewAccumulator(def Definition, minute uint32) *Accumulator {
	return &Accumulator{
		def:    def,
		minute: minute,
		dict:   NewDictionary(),
		cells:  make(map[cellKey]uint32),
	}
}

// CellCount returns the number of distinct (dim1, dim2) cells accumulated so far.
func (a *Accumulator) CellCount() int { return len(a.cells) }

// Add counts one span into its matching cell. The span is skipped (counted=false,
// no error) when it lacks either dimension or fails any filter. It returns an error
// only when a dimension's dictionary is exhausted (>65535 distinct values).
func (a *Accumulator) Add(s SpanValues) (counted bool, err error) {
	dim1, ok := s.String(a.def.Dim1Column)
	if !ok {
		return false, nil
	}
	dim2, ok := s.String(a.def.Dim2Column)
	if !ok {
		return false, nil
	}
	for _, f := range a.def.Filters {
		if f != nil && !f(s) {
			return false, nil
		}
	}

	dim1ID, err := a.dict.InternDim1(dim1)
	if err != nil {
		return false, fmt.Errorf("cube: accumulator intern dim1: %w", err)
	}
	dim2ID, err := a.dict.InternDim2(dim2)
	if err != nil {
		return false, fmt.Errorf("cube: accumulator intern dim2: %w", err)
	}
	a.cells[cellKey{dim1: dim1ID, dim2: dim2ID}]++
	return true, nil
}

// Encode serializes the accumulated cells into a #442 cube file. Every cell is
// stamped with the accumulator's minute, so the file's MinMinute == MaxMinute ==
// the bucket (the partial-minute / shutdown flush carries the correct range).
// Returns an error when no cells were accumulated.
func (a *Accumulator) Encode() ([]byte, error) {
	if len(a.cells) == 0 {
		return nil, fmt.Errorf("cube: accumulator has no cells to encode")
	}
	w := NewWriter(a.def.ID, a.def.Resolution)
	for key, count := range a.cells {
		dim1, ok1 := a.dict.LookupDim1(key.dim1)
		dim2, ok2 := a.dict.LookupDim2(key.dim2)
		if !ok1 || !ok2 {
			return nil, fmt.Errorf("cube: accumulator dict missing id (%d,%d)", key.dim1, key.dim2)
		}
		if err := w.AddCell(a.minute, dim1, dim2, count); err != nil {
			return nil, fmt.Errorf("cube: accumulator add cell: %w", err)
		}
	}
	return w.Encode()
}

// FlushTo encodes the accumulated cells and writes them to store at the standard
// cube key, then resets the accumulator for reuse on the next minute. A flush with
// no cells is a no-op (returns "", nil) so an idle minute writes nothing.
func (a *Accumulator) FlushTo(store ObjectPutter, tenant string) (string, error) {
	if len(a.cells) == 0 {
		return "", nil
	}
	data, err := a.Encode()
	if err != nil {
		return "", err
	}
	key := Filename(tenant, a.def.ID)
	if err := store.Put(key, data); err != nil {
		return "", fmt.Errorf("cube: put %q: %w", key, err)
	}
	a.Reset(a.minute)
	return key, nil
}

// Reset clears all accumulated cells and the dictionary, rebinding the accumulator
// to the given minute. Called after a successful flush so the next minute starts
// clean — no double counting of already-flushed spans.
func (a *Accumulator) Reset(minute uint32) {
	a.minute = minute
	a.dict = NewDictionary()
	a.cells = make(map[cellKey]uint32)
}

// Filename returns the L0 object key for a flushed cube file:
//
//	<tenant>/cubes/<hex cube_id>/L0-<xid>.cube
func Filename(tenant string, cubeID [16]byte) string {
	return path.Join(tenant, "cubes", hex.EncodeToString(cubeID[:]), "L0-"+xid.New().String()+".cube")
}
