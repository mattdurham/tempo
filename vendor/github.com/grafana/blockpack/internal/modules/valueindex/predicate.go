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

// SPEC-VI-9: isNumericColType reports whether colType's canonical little-endian byte encoding
// is NOT lex-order-preserving (NOTE-VI-011: e.g. uint64(255) < uint64(256) numerically, but LE
// bytes [255,0,...] > [0,1,...] lexicographically). Mirrors compareCanonical's numeric branches
// exactly — these are the types blockExcludedByValue must never value-prune by directory bytes
// alone.
func isNumericColType(colType shared.ColumnType) bool {
	switch colType {
	case shared.ColumnTypeUint64, shared.ColumnTypeRangeUint64,
		shared.ColumnTypeInt64, shared.ColumnTypeRangeInt64, shared.ColumnTypeRangeDuration,
		shared.ColumnTypeFloat64, shared.ColumnTypeRangeFloat64:
		return true
	default:
		return false
	}
}

// SPEC-VI-9: blockExcludedByValue reports whether every value in [minValue, maxValue] (a
// BlockDirEntry's per-block value range) is excluded by pred, letting a ranged reader
// (QueryBucketFileRanged, B-4) skip a block's body ReadAt entirely, using only information
// already present in the block directory — no bloom filter is consulted anywhere in the
// matchGroupsInBlock call chain either path uses (NOTE-VI-082).
//
// Ruling 14 (binding — see task-breakdown.md B-3, supersedes the simpler "equality + simple
// range" framing considered earlier): the containment check MUST use compareCanonicalBytes
// (plain lexicographic, with length-prefix tiebreak), NOT compareCanonical (numeric-decode-
// aware). BlockDirEntry.MinValue/MaxValue are themselves computed at write time using
// compareCanonicalBytes (bucketfile.go's ComputeBlockMeta), so checking containment with a
// different comparator than the one that built the bounds risks a false-negative prune —
// silently dropping a block that actually contains a match, a correctness regression.
//
// This makes the prune sound for: (a) equality predicates, any column type — self-consistency
// holds regardless of whether the byte order carries numeric meaning — and (b) range/between
// predicates on non-numeric types (string, bytes, UUID, RangeString, RangeBytes), where
// compareCanonical and compareCanonicalBytes already agree. Range/between predicates on numeric
// types (uint64, int64, float64, and their Range* variants) get NO value-range prune: persisted
// MinValue/MaxValue are lex-byte extremes of LE-encoded numbers, which carry no numeric meaning
// — a genuine capability gap in the current on-disk format, not something to paper over with an
// unsound comparator choice. A future writer-side order-preserving numeric encoding for
// directory bounds could close this gap but is out of scope for #488 (candidate follow-up, not
// attempted here). Numeric range/between predicates fall through to matchGroupsInBlock's
// per-group TimeSec check + pred.Match, exactly matching pre-B-4 behavior — no bloom filter is
// consulted anywhere in this call chain (NOTE-VI-082).
//
// Returns false ("cannot decide, don't prune") for predicate kinds this cheap directory-only
// test can't reason about (neq, regex, nil) — those still fall through to matchGroupsInBlock's
// per-group TimeSec check + pred.Match (NOTE-VI-082).
func blockExcludedByValue(pred Predicate, minValue, maxValue []byte) bool {
	switch p := pred.(type) {
	case *eqPredicate:
		return compareCanonicalBytes(p.canonical, minValue) < 0 ||
			compareCanonicalBytes(p.canonical, maxValue) > 0
	case *rangePredicate:
		if isNumericColType(p.colType) {
			return false
		}
		switch p.op {
		case OpGT:
			return compareCanonicalBytes(maxValue, p.threshold) <= 0
		case OpGTE:
			return compareCanonicalBytes(maxValue, p.threshold) < 0
		case OpLT:
			return compareCanonicalBytes(minValue, p.threshold) >= 0
		case OpLTE:
			return compareCanonicalBytes(minValue, p.threshold) > 0
		default:
			return false
		}
	case *betweenPredicate:
		if isNumericColType(p.colType) {
			return false
		}
		return compareCanonicalBytes(maxValue, p.lo) < 0 ||
			compareCanonicalBytes(minValue, p.hi) > 0
	default: // neqPredicate, regexPredicate, nil predicate
		return false
	}
}
