package cube

// NOTE: SPEC-CUBE-011 — Accumulator counts spans into per-(dim1,dim2) cells for a single
// minute bucket entirely in memory, then encodes one #442 cube file at flush. No per-span
// I/O. The minute is fixed at construction; the caller rotates accumulators per minute.

import (
	"encoding/hex"
	"fmt"
	"math"
	"path"

	"github.com/rs/xid"

	modules_shared "github.com/grafana/blockpack/internal/modules/blockio/shared"
)

// SpanValues abstracts the per-span field access the accumulator needs, decoupling
// it from any concrete span representation. Implementations return (value, true) when
// the column is present on the span and (_, false) when absent.
//
// Float64 is the single numeric-extraction method every aggAttr computation uses (Sum/Min/Max/Avg
// via the float64 value directly; Buckets via uint64(v) for Int64/Duration-typed attrs only, per
// ruling 1's upstream-imposed scope boundary — AggAttrDef.Type carries the type tag that gates
// this). #490 grants blanket permission for this breaking interface change (E-4, #491); both
// production implementers (tempo's OTLP-span adapter, blockpack's own valueIndexSpanValues in
// backfill.go, E-8) must add this method in the same change.
type SpanValues interface {
	String(column string) (string, bool)
	Int64(column string) (int64, bool)
	Float64(column string) (float64, bool)
}

// DurationColumn is the canonical span-duration column name every v2 cube's AggAttrs MUST
// include (ruling 3 + the third-round clamp) — matches the same canonical value used across the
// rest of this codebase (internal/modules/blockio/shared.SpanDurationColumnName), not an
// independently-invented second spelling.
const DurationColumn = modules_shared.SpanDurationColumnName

// AggAttrType gates whether Buckets[] is computed for a materialized aggregate attribute (ruling
// 1: only Int64/Duration-typed attributes are bucketized — Log2Bucketize has no meaningful
// interpretation for an arbitrary float value; TypeFloat is explicitly unsupported upstream).
type AggAttrType uint8

// Aggregate attribute type tags.
const (
	AggAttrTypeInt64   AggAttrType = iota // includes Duration-typed (nanoseconds as int64)
	AggAttrTypeFloat64                    // Sum/Min/Max/Avg only; Buckets stays all-zero
)

// AggAttrDef describes one materialized aggregate attribute. The SET of these (which columns) is
// identity-changing per ruling 3; the full field set (count+sum+min+max+buckets) is ALWAYS
// materialized for every attribute in the set — there is no function-level selection.
type AggAttrDef struct {
	Column string
	Type   AggAttrType
}

// DefinitionError is returned when a Definition violates a business invariant. Sibling to
// CardinalityError's Reason+Suggestion shape (cardinality.go) per this phase's error-family-
// consistency rule — never a bare fmt.Errorf for a condition callers may want to errors.As against.
type DefinitionError struct {
	Reason     string
	Suggestion string
}

func (e *DefinitionError) Error() string { return e.Reason }

// validateDefinition enforces ruling 3's "duration always in the set" invariant (the third-round
// clamp) — THE SINGLE ENFORCEMENT POINT for this rule (ruling 5(d)), called by NewAccumulator
// (here) AND by CreationTrigger.TryCreate (trigger.go, E-7) — never duplicated at wire-decode
// time (E-3's appendix explicitly removed a redundant wire-layer check in favor of this one).
func validateDefinition(def Definition) error {
	for _, attr := range def.AggAttrs {
		if attr.Column == DurationColumn {
			return nil
		}
	}
	return &DefinitionError{
		Reason:     "cube: definition must include duration in AggAttrs (every v2 cube materializes duration by default)",
		Suggestion: "add {Column: cube.DurationColumn, Type: cube.AggAttrTypeInt64} to Definition.AggAttrs",
	}
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

// StringFilter builds a Filter that keeps a span only when its string column equals value
// exactly. Added (#491 Phase E fix pass, review.md Issues 2/3) alongside NumericFilter as the
// string-equality counterpart needed to apply a cube's baked-in filters at ingest/backfill time
// when the filtered column is not numeric (e.g. service.name = "checkout") — NumericFilter cannot
// express this since it reads exclusively via SpanValues.Int64. A span missing the column is
// rejected, mirroring NumericFilter's "missing column cannot satisfy the filter" convention.
func StringFilter(column, value string) Filter {
	return func(s SpanValues) bool {
		v, ok := s.String(column)
		if !ok {
			return false
		}
		return v == value
	}
}

// Definition is the minimal cube description the accumulator needs. The full
// registry (dimensions, lifecycle, cardinality gate) is issue #444; this carries
// only what ingest requires.
type Definition struct {
	Dim1Column string       // first dimension column name (e.g. "service.name")
	Dim2Column string       // second dimension column name (e.g. "status_code")
	Filters    []Filter     // span must satisfy ALL filters to be counted
	AggAttrs   []AggAttrDef // materialized aggregate attributes; MUST include DurationColumn
	ID         [16]byte     // identifies the cube definition this file belongs to
	Resolution uint32       // minutes per bucket (1 for L0)
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

// cellAggState is one cell's in-progress accumulation: the base count plus one AggAttrValues per
// entry in Definition.AggAttrs (same order, parallel indexing). AggAttrValues is reused directly
// as the accumulation state (not a separate struct) since its shape already matches exactly what
// Add needs to update and what Encode needs to write — no field-by-field translation at flush time.
type cellAggState struct {
	aggs  []AggAttrValues
	count uint32
}

// newCellAggState allocates a cellAggState with every aggAttr's Min/Max sentinel-initialized
// (math.MaxFloat64 / -math.MaxFloat64 — the same convention executor/metrics_trace.go's scan-path
// histogram uses), so the first real sample always wins the initial compare.
func newCellAggState(numAggAttrs int) *cellAggState {
	aggs := make([]AggAttrValues, numAggAttrs)
	for i := range aggs {
		aggs[i].Min = math.MaxFloat64
		aggs[i].Max = -math.MaxFloat64
	}
	return &cellAggState{aggs: aggs}
}

// Accumulator counts spans into cells for one minute bucket. Not safe for
// concurrent use — the ingest loop owns one accumulator per active cube.
type Accumulator struct {
	dict   *Dictionary
	cells  map[cellKey]*cellAggState
	def    Definition
	minute uint32
}

// NewAccumulator creates an accumulator for the given cube definition and minute. Returns
// *DefinitionError if def violates validateDefinition's invariant (every v2 cube's AggAttrs must
// include DurationColumn) — the single enforcement point for this rule (ruling 5(d)).
func NewAccumulator(def Definition, minute uint32) (*Accumulator, error) {
	if err := validateDefinition(def); err != nil {
		return nil, err
	}
	return &Accumulator{
		def:    def,
		minute: minute,
		dict:   NewDictionary(),
		cells:  make(map[cellKey]*cellAggState),
	}, nil
}

// CellCount returns the number of distinct (dim1, dim2) cells accumulated so far.
func (a *Accumulator) CellCount() int { return len(a.cells) }

// Add counts one span into its matching cell, PLUS for each Definition.AggAttrs entry: extracts
// via Float64(col); if absent, that attribute's SampleCount for this call does not increment (the
// base Count still increments as long as both dimensions are present); if present, updates
// Sum/Min/Max/SampleCount and, for AggAttrTypeInt64 attrs, increments the matching Log2Bucketize
// bucket (skipped, per ruling 1, when the value is <2 after the uint64 cast — Log2Bucketize's own
// -1 sentinel — or negative, since a negative value can never validly cast to uint64).
// The span is skipped (counted=false, no error) when it lacks either dimension or fails any
// filter. It returns an error only when a dimension's dictionary is exhausted (>65535 distinct
// values).
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

	key := cellKey{dim1: dim1ID, dim2: dim2ID}
	cs, ok := a.cells[key]
	if !ok {
		cs = newCellAggState(len(a.def.AggAttrs))
		a.cells[key] = cs
	}
	cs.count++
	a.addAggAttrs(s, cs)
	return true, nil
}

// addAggAttrs updates cs.aggs in place for every Definition.AggAttrs entry a span carries a value
// for. Split out of Add to keep Add's own cyclomatic complexity low.
func (a *Accumulator) addAggAttrs(s SpanValues, cs *cellAggState) {
	for i, attr := range a.def.AggAttrs {
		v, ok := s.Float64(attr.Column)
		if !ok {
			continue
		}
		agg := &cs.aggs[i]
		agg.SampleCount++
		agg.Sum += v
		if v < agg.Min {
			agg.Min = v
		}
		if v > agg.Max {
			agg.Max = v
		}
		if attr.Type == AggAttrTypeInt64 && v >= 0 {
			if boundary := Log2Bucketize(uint64(v)); boundary != -1 {
				agg.Buckets[BucketIndex(boundary)]++
			}
		}
	}
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
	for key, cs := range a.cells {
		dim1, ok1 := a.dict.LookupDim1(key.dim1)
		dim2, ok2 := a.dict.LookupDim2(key.dim2)
		if !ok1 || !ok2 {
			return nil, fmt.Errorf("cube: accumulator dict missing id (%d,%d)", key.dim1, key.dim2)
		}
		if err := w.AddAggCell(a.minute, dim1, dim2, cs.count, cs.aggs); err != nil {
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
	a.cells = make(map[cellKey]*cellAggState)
}

// Filename returns the L0 object key for a flushed cube file:
//
//	<tenant>/cubes/<hex cube_id>/L0-<xid>.cube
func Filename(tenant string, cubeID [16]byte) string {
	return path.Join(tenant, "cubes", hex.EncodeToString(cubeID[:]), "L0-"+xid.New().String()+".cube")
}
