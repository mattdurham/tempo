package executor_test

import (
	"encoding/binary"
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/grafana/blockpack/internal/logqlparser"
	modules_shared "github.com/grafana/blockpack/internal/modules/blockio/shared"
	"github.com/grafana/blockpack/internal/modules/executor"
	"github.com/grafana/blockpack/internal/vm"
)

// encodeValueExported calls the export shim so executor_test can access encodeValue.
func encodeValueExported(v vm.Value, colType modules_shared.ColumnType) (string, bool) {
	return executor.EncodeValueForTest(v, colType)
}

// TestProgramWantColumns_Nil guards NOTE-018: nil program must return nil
// so ParseBlockFromBytes decodes all columns (safe fallback).
func TestProgramWantColumns_Nil(t *testing.T) {
	assert.Nil(t, executor.ProgramWantColumns(nil))
}

// TestProgramWantColumns_NilPredicates verifies that a program with no
// predicate metadata returns nil (match-all, decode all columns).
func TestProgramWantColumns_NilPredicates(t *testing.T) {
	prog := &vm.Program{Predicates: nil}
	assert.Nil(t, executor.ProgramWantColumns(prog))
}

// TestProgramWantColumns_EmptyPredicates verifies that a program with
// empty predicate maps returns nil (no column filter needed).
func TestProgramWantColumns_EmptyPredicates(t *testing.T) {
	prog := &vm.Program{Predicates: &vm.QueryPredicates{}}
	assert.Nil(t, executor.ProgramWantColumns(prog))
}

// TestProgramWantColumns_Columns verifies user-attribute columns are included
// when listed in Columns (negations, log:body, etc. that have no pruning node).
func TestProgramWantColumns_Columns(t *testing.T) {
	prog := &vm.Program{Predicates: &vm.QueryPredicates{
		Columns: []string{"span.http.method", "resource.service.name"},
	}}
	cols := executor.ProgramWantColumns(prog)
	require.NotNil(t, cols)
	assert.Contains(t, cols, "span.http.method")
	assert.Contains(t, cols, "resource.service.name")
}

// TestProgramWantColumns_NodeLeafColumns verifies built-in equality columns
// (e.g. span:name, span:status) from leaf RangeNodes are included.
func TestProgramWantColumns_NodeLeafColumns(t *testing.T) {
	prog := &vm.Program{Predicates: &vm.QueryPredicates{
		Nodes: []vm.RangeNode{
			{Column: "span:name", Values: []vm.Value{{Type: vm.TypeString, Data: "checkout"}}},
			{Column: "span:status", Values: []vm.Value{{Type: vm.TypeString, Data: "error"}}},
		},
	}}
	cols := executor.ProgramWantColumns(prog)
	require.NotNil(t, cols)
	assert.Contains(t, cols, "span:name")
	assert.Contains(t, cols, "span:status")
}

// TestProgramWantColumns_RangeNode verifies range-predicate columns
// (e.g. span:duration > 5ms) from RangeNodes are included.
func TestProgramWantColumns_RangeNode(t *testing.T) {
	minVal := vm.Value{Type: vm.TypeDuration, Data: int64(5_000_000)}
	prog := &vm.Program{Predicates: &vm.QueryPredicates{
		Nodes: []vm.RangeNode{
			{Column: "span:duration", Min: &minVal},
		},
	}}
	cols := executor.ProgramWantColumns(prog)
	require.NotNil(t, cols)
	assert.Contains(t, cols, "span:duration")
}

// TestProgramWantColumns_PatternNode verifies regex-predicate columns from
// RangeNodes with Pattern are included.
func TestProgramWantColumns_PatternNode(t *testing.T) {
	prog := &vm.Program{Predicates: &vm.QueryPredicates{
		Nodes: []vm.RangeNode{
			{Column: "span:name", Pattern: "checkout.*"},
		},
	}}
	cols := executor.ProgramWantColumns(prog)
	require.NotNil(t, cols)
	assert.Contains(t, cols, "span:name")
}

// TestProgramWantColumns_ExtraColumns verifies that caller-supplied extra columns
// (e.g. identity columns for Execute) are always included.
func TestProgramWantColumns_ExtraColumns(t *testing.T) {
	prog := &vm.Program{Predicates: &vm.QueryPredicates{
		Columns: []string{"span.http.method"},
	}}
	cols := executor.ProgramWantColumns(prog, "trace:id", "span:id")
	require.NotNil(t, cols)
	assert.Contains(t, cols, "span.http.method")
	assert.Contains(t, cols, "trace:id")
	assert.Contains(t, cols, "span:id")
}

// TestProgramWantColumns_UnionAllSources verifies that columns from all predicate
// sources (Nodes tree + Columns list) are unioned into a single set without duplicates.
func TestProgramWantColumns_UnionAllSources(t *testing.T) {
	minVal := vm.Value{Type: vm.TypeInt, Data: int64(400)}
	prog := &vm.Program{Predicates: &vm.QueryPredicates{
		Nodes: []vm.RangeNode{
			{Column: "span:name", Values: []vm.Value{{Type: vm.TypeString, Data: "checkout"}}},
			{Column: "span:duration", Min: &minVal},
			{Column: "span:status", Pattern: "error.*"},
		},
		Columns: []string{"resource.env"},
	}}
	cols := executor.ProgramWantColumns(prog)
	require.NotNil(t, cols)
	assert.Contains(t, cols, "resource.env")
	assert.Contains(t, cols, "span:name")
	assert.Contains(t, cols, "span:duration")
	assert.Contains(t, cols, "span:status")
	assert.Len(t, cols, 4)
}

// TestProgramWantColumns_LogQL_LineFilter is an end-to-end guard for NOTE-018:
// a LogQL query with a line filter must produce a wantColumns set that includes
// log:body, so the two-pass predicate evaluation can match rows in the first pass.
// This is the exact regression that was introduced and fixed during columns_pruning.
func TestProgramWantColumns_LogQL_LineFilter(t *testing.T) {
	sel, err := logqlparser.Parse(`{service.name="checkout"} |= "error"`)
	require.NoError(t, err)

	prog, _, err := logqlparser.CompileAll(sel)
	require.NoError(t, err)

	cols := executor.ProgramWantColumns(prog)
	require.NotNil(t, cols, "query with predicates must not return nil wantColumns")
	assert.Contains(t, cols, "log:body",
		"line filter evaluates log:body inside ColumnPredicate — must be in first-pass column set")
}

// TestProgramWantColumns_LogQL_NoLineFilter verifies that log:body is absent
// when the query has only label matchers and no line filters.
func TestProgramWantColumns_LogQL_NoLineFilter(t *testing.T) {
	sel, err := logqlparser.Parse(`{service.name="checkout"}`)
	require.NoError(t, err)

	prog, _, err := logqlparser.CompileAll(sel)
	require.NoError(t, err)

	cols := executor.ProgramWantColumns(prog)
	require.NotNil(t, cols)
	assert.NotContains(t, cols, "log:body",
		"no line filter means log:body need not be decoded in the first pass")
}

// TestProgramWantColumns_LogQL_PushdownLabelFilter verifies that columns referenced by
// pushed-down pipeline label filters (e.g. | detected_level="error") are included in
// wantColumns. These columns appear as an OR composite node in Nodes (both log.* and resource.*
// variants) and must be loaded for compilePushdownPredicate's ColumnPredicate closures to find them.
// Regression test: | detected_level="error" returned 0 results because log.detected_level
// was absent from wantColumns and ParseBlockFromBytes skipped loading it.
func TestProgramWantColumns_LogQL_PushdownLabelFilter(t *testing.T) {
	sel, err := logqlparser.Parse(`{env="prod"} | detected_level="error"`)
	require.NoError(t, err)

	prog, _, err := logqlparser.CompileAll(sel)
	require.NoError(t, err)

	cols := executor.ProgramWantColumns(prog)
	require.NotNil(t, cols, "query with predicates must not return nil wantColumns")
	assert.Contains(t, cols, "log.detected_level",
		"pushed-down | detected_level filter must include log.detected_level in first-pass columns")
	assert.Contains(t, cols, "resource.detected_level",
		"pushed-down | detected_level filter must include resource.detected_level in first-pass columns")
}

// TestProgramWantColumns_LogQL_PushdownInstanceID verifies that | instance_id="..." filters
// also include log.instance_id and resource.instance_id in wantColumns.
func TestProgramWantColumns_LogQL_PushdownInstanceID(t *testing.T) {
	sel, err := logqlparser.Parse(`{env="prod"} | instance_id="instance-042"`)
	require.NoError(t, err)

	prog, _, err := logqlparser.CompileAll(sel)
	require.NoError(t, err)

	cols := executor.ProgramWantColumns(prog)
	require.NotNil(t, cols)
	assert.Contains(t, cols, "log.instance_id",
		"pushed-down | instance_id filter must include log.instance_id in first-pass columns")
	assert.Contains(t, cols, "resource.instance_id",
		"pushed-down | instance_id filter must include resource.instance_id in first-pass columns")
}

// NOTE-027: verify existing TypeInt path works for ColumnTypeRangeInt64.
func TestEncodeValue_TypeInt_RangeInt64(t *testing.T) {
	v := vm.Value{Type: vm.TypeInt, Data: int64(4500)}
	got, ok := encodeValueExported(v, modules_shared.ColumnTypeRangeInt64)
	require.True(t, ok)
	require.Equal(t, 8, len(got))
	n := int64(binary.LittleEndian.Uint64([]byte(got))) //nolint:gosec // safe: encoding known positive value 4500
	require.Equal(t, int64(4500), n)
}

func TestEncodeValue_TypeDuration_RangeInt64(t *testing.T) {
	v := vm.Value{Type: vm.TypeDuration, Data: int64(5_000_000)}
	got, ok := encodeValueExported(v, modules_shared.ColumnTypeRangeInt64)
	require.True(t, ok)
	require.Equal(t, 8, len(got))
	n := int64(binary.LittleEndian.Uint64([]byte(got))) //nolint:gosec // safe: encoding known positive value
	require.Equal(t, int64(5_000_000), n)
}

func TestEncodeValue_TypeDuration_RangeDuration(t *testing.T) {
	v := vm.Value{Type: vm.TypeDuration, Data: int64(10_000_000)}
	got, ok := encodeValueExported(v, modules_shared.ColumnTypeRangeDuration)
	require.True(t, ok)
	require.Equal(t, 8, len(got))
	n := int64(binary.LittleEndian.Uint64([]byte(got))) //nolint:gosec // safe
	require.Equal(t, int64(10_000_000), n)
}

func TestEncodeValue_TypeDuration_RangeUint64(t *testing.T) {
	// Positive duration: should encode as uint64.
	v := vm.Value{Type: vm.TypeDuration, Data: int64(5_000_000)}
	got, ok := encodeValueExported(v, modules_shared.ColumnTypeRangeUint64)
	require.True(t, ok)
	require.Equal(t, 8, len(got))
	n := binary.LittleEndian.Uint64([]byte(got))
	require.Equal(t, uint64(5_000_000), n)

	// Negative duration: must be rejected (would wrap to huge uint64).
	vNeg := vm.Value{Type: vm.TypeDuration, Data: int64(-1)}
	_, ok2 := encodeValueExported(vNeg, modules_shared.ColumnTypeRangeUint64)
	require.False(t, ok2, "negative duration must be rejected for RangeUint64")
}

func TestEncodeValue_TypeInt_RangeUint64(t *testing.T) {
	// Positive int: should encode as uint64.
	v := vm.Value{Type: vm.TypeInt, Data: int64(42)}
	got, ok := encodeValueExported(v, modules_shared.ColumnTypeRangeUint64)
	require.True(t, ok)
	n := binary.LittleEndian.Uint64([]byte(got))
	require.Equal(t, uint64(42), n)

	// Negative int: must be rejected.
	vNeg := vm.Value{Type: vm.TypeInt, Data: int64(-5)}
	_, ok2 := encodeValueExported(vNeg, modules_shared.ColumnTypeRangeUint64)
	require.False(t, ok2, "negative int must be rejected for RangeUint64")
}

func TestEncodeValue_TypeFloat_RangeFloat64(t *testing.T) {
	v := vm.Value{Type: vm.TypeFloat, Data: float64(3.14)}
	got, ok := encodeValueExported(v, modules_shared.ColumnTypeRangeFloat64)
	require.True(t, ok)
	require.Equal(t, 8, len(got))
	f := math.Float64frombits(binary.LittleEndian.Uint64([]byte(got)))
	require.InDelta(t, 3.14, f, 0.001)
}

func TestEncodeValue_TypeString_RangeString(t *testing.T) {
	v := vm.Value{Type: vm.TypeString, Data: "hello"}
	got, ok := encodeValueExported(v, modules_shared.ColumnTypeRangeString)
	require.True(t, ok)
	require.Equal(t, "hello", got)
}

func TestEncodeValue_UnsupportedType(t *testing.T) {
	// TypeBool against RangeInt64 — no encoding path exists.
	v := vm.Value{Type: vm.TypeBool, Data: true}
	_, ok := encodeValueExported(v, modules_shared.ColumnTypeRangeInt64)
	require.False(t, ok)
}

func TestEncodeValue_UnsupportedColType(t *testing.T) {
	// A column type with no encoding branch.
	v := vm.Value{Type: vm.TypeInt, Data: int64(42)}
	_, ok := encodeValueExported(v, modules_shared.ColumnType(255))
	require.False(t, ok)
}

func TestEncodeValue_TypeString_RangeFloat64_Negative(t *testing.T) {
	// NOTE-040: negative float strings must be rejected.
	v := vm.Value{Type: vm.TypeString, Data: "-3.14"}
	_, ok := encodeValueExported(v, modules_shared.ColumnTypeRangeFloat64)
	require.False(t, ok, "negative float string must be rejected per NOTE-040")
}

func TestEncodeValue_TypeString_RangeInt64(t *testing.T) {
	// Parseable integer string.
	v := vm.Value{Type: vm.TypeString, Data: "4500"}
	got, ok := encodeValueExported(v, modules_shared.ColumnTypeRangeInt64)
	require.True(t, ok, "parseable int string must encode against RangeInt64")
	require.Equal(t, 8, len(got))
	n := int64(binary.LittleEndian.Uint64([]byte(got))) //nolint:gosec // safe: encoding known positive value 4500
	require.Equal(t, int64(4500), n)

	// Non-parseable string must return false.
	v2 := vm.Value{Type: vm.TypeString, Data: "not-a-number"}
	_, ok2 := encodeValueExported(v2, modules_shared.ColumnTypeRangeInt64)
	require.False(t, ok2, "non-parseable string must not encode")
}

func TestEncodeValue_TypeString_RangeFloat64(t *testing.T) {
	v := vm.Value{Type: vm.TypeString, Data: "4500.5"}
	got, ok := encodeValueExported(v, modules_shared.ColumnTypeRangeFloat64)
	require.True(t, ok)
	require.Equal(t, 8, len(got))

	// Verify round-trip.
	f := math.Float64frombits(binary.LittleEndian.Uint64([]byte(got)))
	require.InDelta(t, 4500.5, f, 0.001)

	// Non-parseable.
	v2 := vm.Value{Type: vm.TypeString, Data: "abc"}
	_, ok2 := encodeValueExported(v2, modules_shared.ColumnTypeRangeFloat64)
	require.False(t, ok2)
}

// --- rangeTypeSentinelMin/Max tests ---

func TestRangeTypeSentinelMin_AllTypes(t *testing.T) {
	tests := []struct {
		name    string
		colType modules_shared.ColumnType
		wantOK  bool
	}{
		{"RangeUint64", modules_shared.ColumnTypeRangeUint64, true},
		{"Uint64", modules_shared.ColumnTypeUint64, true},
		{"RangeInt64", modules_shared.ColumnTypeRangeInt64, true},
		{"Int64", modules_shared.ColumnTypeInt64, true},
		{"RangeDuration", modules_shared.ColumnTypeRangeDuration, true},
		{"RangeFloat64", modules_shared.ColumnTypeRangeFloat64, true},
		{"Float64", modules_shared.ColumnTypeFloat64, true},
		{"RangeString", modules_shared.ColumnTypeRangeString, true},
		{"String", modules_shared.ColumnTypeString, true},
		{"Unsupported", modules_shared.ColumnType(255), false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, ok := executor.RangeTypeSentinelMinForTest(tt.colType)
			assert.Equal(t, tt.wantOK, ok)
		})
	}
}

func TestRangeTypeSentinelMax_AllTypes(t *testing.T) {
	tests := []struct {
		name    string
		colType modules_shared.ColumnType
		wantOK  bool
	}{
		{"RangeUint64", modules_shared.ColumnTypeRangeUint64, true},
		{"Uint64", modules_shared.ColumnTypeUint64, true},
		{"RangeInt64", modules_shared.ColumnTypeRangeInt64, true},
		{"Int64", modules_shared.ColumnTypeInt64, true},
		{"RangeDuration", modules_shared.ColumnTypeRangeDuration, true},
		{"RangeFloat64", modules_shared.ColumnTypeRangeFloat64, true},
		{"Float64", modules_shared.ColumnTypeFloat64, true},
		{"RangeString", modules_shared.ColumnTypeRangeString, true},
		{"String", modules_shared.ColumnTypeString, true},
		{"Unsupported", modules_shared.ColumnType(255), false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, ok := executor.RangeTypeSentinelMaxForTest(tt.colType)
			assert.Equal(t, tt.wantOK, ok)
		})
	}
}

func TestRangeTypeSentinelMin_StringIsEmpty(t *testing.T) {
	got, ok := executor.RangeTypeSentinelMinForTest(modules_shared.ColumnTypeRangeString)
	require.True(t, ok)
	assert.Equal(t, "", got, "string min sentinel must be empty string")
}

func TestRangeTypeSentinelMax_Uint64IsMaxUint64(t *testing.T) {
	got, ok := executor.RangeTypeSentinelMaxForTest(modules_shared.ColumnTypeRangeUint64)
	require.True(t, ok)
	n := binary.LittleEndian.Uint64([]byte(got))
	assert.Equal(t, uint64(math.MaxUint64), n)
}

func TestRangeTypeSentinelMax_Float64IsPosInf(t *testing.T) {
	got, ok := executor.RangeTypeSentinelMaxForTest(modules_shared.ColumnTypeRangeFloat64)
	require.True(t, ok)
	f := math.Float64frombits(binary.LittleEndian.Uint64([]byte(got)))
	assert.True(t, math.IsInf(f, +1), "float64 max sentinel must be +Inf")
}

func TestRangeTypeSentinelMin_Float64IsNegInf(t *testing.T) {
	got, ok := executor.RangeTypeSentinelMinForTest(modules_shared.ColumnTypeRangeFloat64)
	require.True(t, ok)
	f := math.Float64frombits(binary.LittleEndian.Uint64([]byte(got)))
	assert.True(t, math.IsInf(f, -1), "float64 min sentinel must be -Inf")
}

// --- inferColTypeFromValues tests ---

func TestInferColType_NilNil(t *testing.T) {
	ct := executor.InferColTypeForTest(nil, nil)
	assert.Equal(t, modules_shared.ColumnTypeRangeInt64, ct, "nil/nil defaults to RangeInt64")
}

func TestInferColType_TypeInt(t *testing.T) {
	v := &vm.Value{Type: vm.TypeInt, Data: int64(42)}
	assert.Equal(t, modules_shared.ColumnTypeRangeInt64, executor.InferColTypeForTest(v, nil))
	assert.Equal(t, modules_shared.ColumnTypeRangeInt64, executor.InferColTypeForTest(nil, v))
}

func TestInferColType_TypeFloat(t *testing.T) {
	v := &vm.Value{Type: vm.TypeFloat, Data: float64(3.14)}
	assert.Equal(t, modules_shared.ColumnTypeRangeFloat64, executor.InferColTypeForTest(v, nil))
}

func TestInferColType_TypeString(t *testing.T) {
	v := &vm.Value{Type: vm.TypeString, Data: "hello"}
	assert.Equal(t, modules_shared.ColumnTypeRangeString, executor.InferColTypeForTest(v, nil))
}

func TestInferColType_TypeDuration(t *testing.T) {
	v := &vm.Value{Type: vm.TypeDuration, Data: int64(5_000_000)}
	assert.Equal(t, modules_shared.ColumnTypeRangeDuration, executor.InferColTypeForTest(v, nil))
}

func TestInferColType_DefaultFallback(t *testing.T) {
	// TypeBool has no explicit case → falls through to default (RangeInt64).
	v := &vm.Value{Type: vm.TypeBool, Data: true}
	assert.Equal(t, modules_shared.ColumnTypeRangeInt64, executor.InferColTypeForTest(v, nil))
}

func TestInferColType_MaxOnly(t *testing.T) {
	// When min is nil, maxVal is used for inference.
	v := &vm.Value{Type: vm.TypeFloat, Data: float64(99.9)}
	assert.Equal(t, modules_shared.ColumnTypeRangeFloat64, executor.InferColTypeForTest(nil, v))
}

// --- isASCII tests ---

func TestIsASCII_True(t *testing.T) {
	assert.True(t, executor.IsASCIIForTest("hello world"))
	assert.True(t, executor.IsASCIIForTest(""))
}

func TestIsASCII_False(t *testing.T) {
	assert.False(t, executor.IsASCIIForTest("héllo"))
	assert.False(t, executor.IsASCIIForTest("日本語"))
}

// --- encodeValue: defensive type-assertion failure branches ---

func TestEncodeValue_TypeInt_WrongData_RangeInt64(t *testing.T) {
	// TypeInt with non-int64 Data — defensive else branch.
	v := vm.Value{Type: vm.TypeInt, Data: "not-an-int"}
	_, ok := encodeValueExported(v, modules_shared.ColumnTypeRangeInt64)
	assert.False(t, ok)
}

func TestEncodeValue_TypeDuration_WrongData_RangeInt64(t *testing.T) {
	v := vm.Value{Type: vm.TypeDuration, Data: "wrong"}
	_, ok := encodeValueExported(v, modules_shared.ColumnTypeRangeInt64)
	assert.False(t, ok)
}

func TestEncodeValue_TypeString_WrongData_RangeInt64(t *testing.T) {
	// TypeString with non-string Data.
	v := vm.Value{Type: vm.TypeString, Data: int64(42)}
	_, ok := encodeValueExported(v, modules_shared.ColumnTypeRangeInt64)
	assert.False(t, ok)
}

func TestEncodeValue_TypeInt_WrongData_RangeUint64(t *testing.T) {
	v := vm.Value{Type: vm.TypeInt, Data: "wrong"}
	_, ok := encodeValueExported(v, modules_shared.ColumnTypeRangeUint64)
	assert.False(t, ok)
}

func TestEncodeValue_TypeDuration_WrongData_RangeUint64(t *testing.T) {
	v := vm.Value{Type: vm.TypeDuration, Data: "wrong"}
	_, ok := encodeValueExported(v, modules_shared.ColumnTypeRangeUint64)
	assert.False(t, ok)
}

func TestEncodeValue_TypeFloat_WrongData_RangeFloat64(t *testing.T) {
	v := vm.Value{Type: vm.TypeFloat, Data: "wrong"}
	_, ok := encodeValueExported(v, modules_shared.ColumnTypeRangeFloat64)
	assert.False(t, ok)
}

func TestEncodeValue_TypeString_WrongData_RangeFloat64(t *testing.T) {
	v := vm.Value{Type: vm.TypeString, Data: int64(42)}
	_, ok := encodeValueExported(v, modules_shared.ColumnTypeRangeFloat64)
	assert.False(t, ok)
}

func TestEncodeValue_TypeString_WrongData_RangeString(t *testing.T) {
	v := vm.Value{Type: vm.TypeString, Data: int64(42)}
	_, ok := encodeValueExported(v, modules_shared.ColumnTypeRangeString)
	assert.False(t, ok)
}

func TestEncodeValue_DefaultType_RangeFloat64(t *testing.T) {
	// TypeBool against RangeFloat64 — hits default case.
	v := vm.Value{Type: vm.TypeBool, Data: true}
	_, ok := encodeValueExported(v, modules_shared.ColumnTypeRangeFloat64)
	assert.False(t, ok)
}

func TestEncodeValue_DefaultType_RangeUint64(t *testing.T) {
	// TypeBool against RangeUint64 — hits default (no matching case in switch).
	v := vm.Value{Type: vm.TypeBool, Data: true}
	_, ok := encodeValueExported(v, modules_shared.ColumnTypeRangeUint64)
	assert.False(t, ok)
}

func TestEncodeValue_TypeInt_NonInt64_RangeString(t *testing.T) {
	// TypeInt against RangeString — no valid encoding path.
	v := vm.Value{Type: vm.TypeInt, Data: int64(42)}
	_, ok := encodeValueExported(v, modules_shared.ColumnTypeRangeString)
	assert.False(t, ok)
}

// --- buildCaseInsensitiveRegexPredicate tests ---

func TestBuildCaseInsensitiveRegexPredicate_NonASCII(t *testing.T) {
	// Non-ASCII prefix falls back to bloom-only.
	analysis := &vm.RegexAnalysis{
		Prefixes:        []string{"débug"},
		CaseInsensitive: true,
	}
	pred := executor.BuildCaseInsensitiveRegexPredicateForTest(
		"resource.service.name", modules_shared.ColumnTypeRangeString, analysis,
	)
	assert.NotNil(t, pred.Columns)
	assert.Empty(t, pred.Values, "non-ASCII prefix must fall back to bloom-only")
}

func TestBuildCaseInsensitiveRegexPredicate_ASCII(t *testing.T) {
	// ASCII prefix generates interval match [UPPER, lower+\xff].
	analysis := &vm.RegexAnalysis{
		Prefixes:        []string{"debug"},
		CaseInsensitive: true,
	}
	pred := executor.BuildCaseInsensitiveRegexPredicateForTest(
		"resource.service.name", modules_shared.ColumnTypeRangeString, analysis,
	)
	assert.True(t, pred.IntervalMatch)
	require.Len(t, pred.Values, 2)
	assert.Equal(t, "DEBUG", pred.Values[0])
	assert.Equal(t, "debug\xff", pred.Values[1])
}

// --- buildCaseSensitiveSinglePrefixPredicate tests ---

func TestBuildCaseSensitiveSinglePrefixPredicate_LiteralAlternation(t *testing.T) {
	// Pattern "foo|bar" → pure literal alternation → point lookups.
	analysis := &vm.RegexAnalysis{
		Prefixes:        []string{""},
		CaseInsensitive: false,
	}
	pred := executor.BuildCaseSensitiveSinglePrefixPredicateForTest(
		"resource.env", modules_shared.ColumnTypeRangeString, "foo|bar", analysis,
	)
	assert.False(t, pred.IntervalMatch)
	require.Len(t, pred.Values, 2)
	assert.Contains(t, pred.Values, "foo")
	assert.Contains(t, pred.Values, "bar")
}

func TestBuildCaseSensitiveSinglePrefixPredicate_Interval(t *testing.T) {
	// Pattern "checkout.*" with prefix "checkout" → interval match.
	analysis := &vm.RegexAnalysis{
		Prefixes:        []string{"checkout"},
		CaseInsensitive: false,
	}
	pred := executor.BuildCaseSensitiveSinglePrefixPredicateForTest(
		"span:name", modules_shared.ColumnTypeRangeString, "checkout.*", analysis,
	)
	assert.True(t, pred.IntervalMatch)
	require.Len(t, pred.Values, 2)
	assert.Equal(t, "checkout", pred.Values[0])
	assert.Equal(t, "checkout\xff", pred.Values[1])
}

func TestBuildCaseSensitiveSinglePrefixPredicate_LiteralAlternation_EncodeFail(t *testing.T) {
	// Literal alternation "foo|bar" against RangeInt64 — strings can't encode as int64.
	// Hits encodedVals == 0 fallback (line 389).
	analysis := &vm.RegexAnalysis{
		Prefixes:        []string{""},
		CaseInsensitive: false,
	}
	pred := executor.BuildCaseSensitiveSinglePrefixPredicateForTest(
		"resource.count", modules_shared.ColumnTypeRangeInt64, "foo|bar", analysis,
	)
	assert.Empty(t, pred.Values, "non-numeric literals against RangeInt64 → bloom-only fallback")
	assert.NotEmpty(t, pred.Columns)
}

func TestBuildCaseSensitiveSinglePrefixPredicate_IntervalEncodeFail(t *testing.T) {
	// Prefix "checkout" against ColumnType(255) — encode fails → line 415 fallback.
	analysis := &vm.RegexAnalysis{
		Prefixes:        []string{"checkout"},
		CaseInsensitive: false,
	}
	pred := executor.BuildCaseSensitiveSinglePrefixPredicateForTest(
		"span:name", modules_shared.ColumnType(255), "checkout.*", analysis,
	)
	assert.Empty(t, pred.Values, "unsupported colType → encode fails → bloom-only")
	assert.NotEmpty(t, pred.Columns)
}

// TestSearchMetaColsDoNotIncludeIntrinsicColumns verifies that searchMetaCols contains
// no column names that are served exclusively by the intrinsic section (traceIntrinsicColumns).
// Trace intrinsic columns do not exist in block payloads, so including them in searchMetaCols
// causes useless nil-column decode attempts in ParseBlockFromBytes.
func TestSearchMetaColsDoNotIncludeIntrinsicColumns(t *testing.T) {
	for col := range executor.SearchMetaCols {
		_, isIntrinsic := executor.TraceIntrinsicColumns[col]
		assert.False(t, isIntrinsic,
			"searchMetaCols must not include trace intrinsic column %q (served exclusively by intrinsic section)", col)
	}
}

// TestOverFetchCap_SingleCondition verifies that for a single-node program the overFetch
// equals limit * totalLeaves (no cap applied).
func TestOverFetchCap_SingleCondition(t *testing.T) {
	// 1 top-level node, 3 leaves, limit=20 → overFetch = 20*3 = 60
	got := executor.ComputeOverFetchForTest(1, 3, 20)
	assert.Equal(t, 60, got, "single-condition overFetch must be limit*leaves")
}

// TestOverFetchCap_MultiCondition verifies that for a multi-node program the overFetch
// is min(limit*10000, 500_000) regardless of leaf count.
// With limit=20 and 3 nodes: min(20*10000, 500_000) = min(200_000, 500_000) = 200_000.
func TestOverFetchCap_MultiCondition(t *testing.T) {
	// 3 top-level nodes (AND), limit=20 → overFetch = min(20*10000, 500_000) = 200_000
	got := executor.ComputeOverFetchForTest(3, 3, 20)
	assert.Equal(t, 200_000, got, "multi-condition overFetch must be min(limit*10000, 500_000)")
}

// TestOverFetchCap_LargeLimitCapped verifies that the cap of 500_000 applies when
// limit*10000 would exceed it.
// With limit=60: 60*10000 = 600_000 → capped at 500_000.
func TestOverFetchCap_LargeLimitCapped(t *testing.T) {
	got := executor.ComputeOverFetchForTest(2, 10, 60)
	assert.Equal(t, 500_000, got, "overFetch must not exceed 500_000")
}

// TestOverFetchCap_ZeroLimit verifies that limit=0 produces overFetch=0 (no limit).
func TestOverFetchCap_ZeroLimit(t *testing.T) {
	got := executor.ComputeOverFetchForTest(3, 5, 0)
	assert.Equal(t, 0, got, "overFetch must be 0 when limit=0")
}
