package valueindex

// NOTE: Any changes to this file must be reflected in the corresponding SPECS.md or NOTES.md.

import (
	"bytes"
	"cmp"
	"encoding/binary"
	"fmt"
	"math"
	"regexp"

	"github.com/grafana/blockpack/internal/modules/blockio/shared"
)

// Predicate filters posting list entries by their canonical-encoded vi:value.
type Predicate interface {
	// Match returns true if the canonical-encoded value satisfies this predicate.
	Match(canonicalValue []byte) bool
	// ColType returns the column type this predicate operates on.
	ColType() shared.ColumnType
}

// Op is a comparison operator for range predicates.
type Op uint8

const (
	// OpGT matches values strictly greater than the threshold.
	OpGT Op = iota
	// OpGTE matches values greater than or equal to the threshold.
	OpGTE
	// OpLT matches values strictly less than the threshold.
	OpLT
	// OpLTE matches values less than or equal to the threshold.
	OpLTE
)

// eqPredicate matches entries whose canonical value equals a fixed canonical value.
type eqPredicate struct {
	canonical []byte
	colType   shared.ColumnType
}

// neqPredicate matches entries whose canonical value does not equal a fixed canonical value.
type neqPredicate struct {
	canonical []byte
	colType   shared.ColumnType
}

// rangePredicate matches entries by comparing canonical values against a threshold.
// Numeric types (uint64, int64, float64) are decoded before comparison to avoid
// little-endian byte ordering bugs (e.g. 255 > 256 in LE bytes). See NOTE-VI-011.
type rangePredicate struct {
	threshold []byte
	colType   shared.ColumnType
	op        Op
}

// regexPredicate matches string entries against a compiled regexp.
type regexPredicate struct {
	re      *regexp.Regexp
	colType shared.ColumnType
}

// betweenPredicate matches entries in [lo, hi] inclusive.
// Uses compareCanonical for correct numeric ordering. See NOTE-VI-011.
type betweenPredicate struct {
	lo      []byte
	hi      []byte
	colType shared.ColumnType
}

// NewEqPredicate creates a predicate that matches entries equal to value.
func NewEqPredicate(colType shared.ColumnType, value any) (Predicate, error) {
	cv, err := CanonicalValue(colType, value)
	if err != nil {
		return nil, fmt.Errorf("valueindex: NewEqPredicate: %w", err)
	}
	return &eqPredicate{canonical: cv, colType: colType}, nil
}

func (p *eqPredicate) Match(cv []byte) bool       { return bytes.Equal(cv, p.canonical) }
func (p *eqPredicate) ColType() shared.ColumnType { return p.colType }

// NewNEqPredicate creates a predicate that matches entries not equal to value.
func NewNEqPredicate(colType shared.ColumnType, value any) (Predicate, error) {
	cv, err := CanonicalValue(colType, value)
	if err != nil {
		return nil, fmt.Errorf("valueindex: NewNEqPredicate: %w", err)
	}
	return &neqPredicate{canonical: cv, colType: colType}, nil
}

func (p *neqPredicate) Match(cv []byte) bool       { return !bytes.Equal(cv, p.canonical) }
func (p *neqPredicate) ColType() shared.ColumnType { return p.colType }

// NewRangePredicate creates a predicate that compares entries against value using op.
// Numeric types are decoded before comparison; string/bytes use lexicographic order.
func NewRangePredicate(colType shared.ColumnType, value any, op Op) (Predicate, error) {
	cv, err := CanonicalValue(colType, value)
	if err != nil {
		return nil, fmt.Errorf("valueindex: NewRangePredicate: %w", err)
	}
	return &rangePredicate{threshold: cv, colType: colType, op: op}, nil
}

func (p *rangePredicate) Match(cv []byte) bool {
	c := compareCanonical(p.colType, cv, p.threshold)
	switch p.op {
	case OpGT:
		return c > 0
	case OpGTE:
		return c >= 0
	case OpLT:
		return c < 0
	case OpLTE:
		return c <= 0
	default:
		return false
	}
}

func (p *rangePredicate) ColType() shared.ColumnType { return p.colType }

// NewRegexPredicate creates a predicate that matches string entries against a regexp pattern.
// Returns an error if pattern is invalid or colType is not ColumnTypeString.
func NewRegexPredicate(colType shared.ColumnType, pattern string) (Predicate, error) {
	if colType != shared.ColumnTypeString {
		return nil, fmt.Errorf(
			"valueindex: NewRegexPredicate: regex only supported for ColumnTypeString, got %d",
			colType,
		)
	}
	re, err := regexp.Compile(pattern)
	if err != nil {
		return nil, fmt.Errorf("valueindex: NewRegexPredicate: invalid pattern %q: %w", pattern, err)
	}
	return &regexPredicate{re: re, colType: colType}, nil
}

func (p *regexPredicate) Match(cv []byte) bool       { return p.re.Match(cv) }
func (p *regexPredicate) ColType() shared.ColumnType { return p.colType }

// NewBetweenPredicate creates a predicate that matches entries in [lo, hi] inclusive.
func NewBetweenPredicate(colType shared.ColumnType, lo, hi any) (Predicate, error) {
	clo, err := CanonicalValue(colType, lo)
	if err != nil {
		return nil, fmt.Errorf("valueindex: NewBetweenPredicate lo: %w", err)
	}
	chi, err := CanonicalValue(colType, hi)
	if err != nil {
		return nil, fmt.Errorf("valueindex: NewBetweenPredicate hi: %w", err)
	}
	return &betweenPredicate{lo: clo, hi: chi, colType: colType}, nil
}

func (p *betweenPredicate) Match(cv []byte) bool {
	return compareCanonical(p.colType, cv, p.lo) >= 0 && compareCanonical(p.colType, cv, p.hi) <= 0
}

func (p *betweenPredicate) ColType() shared.ColumnType { return p.colType }

// compareCanonical compares two canonical-encoded values of colType.
// Returns -1, 0, or +1.
//
// NOTE-VI-011: Numeric LE encodings do not preserve sort order across byte
// boundaries (e.g. uint64(255) < uint64(256) but LE bytes [255,0,...] > [0,1,...]).
// Decode to native type before comparing.
//
// NOTE-VI-012: Range* types are compared identically to their scalar equivalents.
func compareCanonical(colType shared.ColumnType, a, b []byte) int {
	switch colType {
	case shared.ColumnTypeUint64, shared.ColumnTypeRangeUint64:
		if len(a) < 8 || len(b) < 8 {
			return bytes.Compare(a, b)
		}
		return cmp.Compare(binary.LittleEndian.Uint64(a), binary.LittleEndian.Uint64(b))
	case shared.ColumnTypeInt64, shared.ColumnTypeRangeInt64, shared.ColumnTypeRangeDuration:
		if len(a) < 8 || len(b) < 8 {
			return bytes.Compare(a, b)
		}
		//nolint:gosec // safe: converting canonical LE bytes back to int64
		return cmp.Compare(int64(binary.LittleEndian.Uint64(a)), int64(binary.LittleEndian.Uint64(b)))
	case shared.ColumnTypeFloat64, shared.ColumnTypeRangeFloat64:
		if len(a) < 8 || len(b) < 8 {
			return bytes.Compare(a, b)
		}
		return cmp.Compare(
			math.Float64frombits(binary.LittleEndian.Uint64(a)),
			math.Float64frombits(binary.LittleEndian.Uint64(b)),
		)
	default: // string, bytes, bool, UUID, RangeString, RangeBytes — byte order is correct
		return bytes.Compare(a, b)
	}
}
