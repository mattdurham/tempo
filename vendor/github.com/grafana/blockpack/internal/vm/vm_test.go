package vm

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/grafana/blockpack/internal/traceqlparser"
)

// --- valuesEqual ---

func TestValuesEqual_AllTypes(t *testing.T) {
	tests := []struct {
		name  string
		left  Value
		right Value
		want  bool
	}{
		{
			name:  "string equal",
			left:  Value{Type: TypeString, Data: "hello"},
			right: Value{Type: TypeString, Data: "hello"},
			want:  true,
		},
		{
			name:  "string not equal",
			left:  Value{Type: TypeString, Data: "hello"},
			right: Value{Type: TypeString, Data: "world"},
			want:  false,
		},
		{
			name:  "int equal",
			left:  Value{Type: TypeInt, Data: int64(42)},
			right: Value{Type: TypeInt, Data: int64(42)},
			want:  true,
		},
		{
			name:  "int not equal",
			left:  Value{Type: TypeInt, Data: int64(1)},
			right: Value{Type: TypeInt, Data: int64(2)},
			want:  false,
		},
		{
			name:  "float equal",
			left:  Value{Type: TypeFloat, Data: float64(3.14)},
			right: Value{Type: TypeFloat, Data: float64(3.14)},
			want:  true,
		},
		{
			name:  "float not equal",
			left:  Value{Type: TypeFloat, Data: float64(1.0)},
			right: Value{Type: TypeFloat, Data: float64(2.0)},
			want:  false,
		},
		{
			name:  "bool equal true",
			left:  Value{Type: TypeBool, Data: true},
			right: Value{Type: TypeBool, Data: true},
			want:  true,
		},
		{
			name:  "bool not equal",
			left:  Value{Type: TypeBool, Data: true},
			right: Value{Type: TypeBool, Data: false},
			want:  false,
		},
		{
			name:  "bytes equal",
			left:  Value{Type: TypeBytes, Data: []byte{0xDE, 0xAD}},
			right: Value{Type: TypeBytes, Data: []byte{0xDE, 0xAD}},
			want:  true,
		},
		{
			name:  "bytes not equal",
			left:  Value{Type: TypeBytes, Data: []byte{0x01}},
			right: Value{Type: TypeBytes, Data: []byte{0x02}},
			want:  false,
		},
		{
			name:  "type mismatch string vs int",
			left:  Value{Type: TypeString, Data: "42"},
			right: Value{Type: TypeInt, Data: int64(42)},
			want:  false,
		},
		{
			name:  "nil type always false",
			left:  Value{Type: TypeNil},
			right: Value{Type: TypeNil},
			want:  false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := valuesEqual(tc.left, tc.right)
			assert.Equal(t, tc.want, got)
		})
	}
}

// --- compareValues ---

func TestCompareValues_NilHandling(t *testing.T) {
	nilVal := Value{Type: TypeNil}
	intVal := Value{Type: TypeInt, Data: int64(5)}

	// Nil == Nil (same type)
	assert.True(t, compareValues(nilVal, nilVal, traceqlparser.OpEq))
	// Nil != non-nil with OpEq
	assert.False(t, compareValues(nilVal, intVal, traceqlparser.OpEq))
	// Nil != non-nil with OpNeq → true (different types)
	assert.True(t, compareValues(nilVal, intVal, traceqlparser.OpNeq))
	// Nil != Nil with OpNeq → false (same type)
	assert.False(t, compareValues(nilVal, nilVal, traceqlparser.OpNeq))
}

func TestCompareValues_NumericOps(t *testing.T) {
	a := Value{Type: TypeInt, Data: int64(10)}
	b := Value{Type: TypeInt, Data: int64(20)}

	assert.True(t, compareValues(a, b, traceqlparser.OpLt))
	assert.True(t, compareValues(a, b, traceqlparser.OpLte))
	assert.False(t, compareValues(a, b, traceqlparser.OpGt))
	assert.False(t, compareValues(a, b, traceqlparser.OpGte))
	assert.True(t, compareValues(a, a, traceqlparser.OpGte))
	assert.True(t, compareValues(a, a, traceqlparser.OpLte))
	assert.False(t, compareValues(a, b, traceqlparser.OpEq))
	assert.True(t, compareValues(a, b, traceqlparser.OpNeq))
}

func TestCompareValues_DefaultReturnsFalse(t *testing.T) {
	a := Value{Type: TypeInt, Data: int64(1)}
	b := Value{Type: TypeInt, Data: int64(1)}
	// Use an operator not handled by the default switch (OpRegex)
	assert.False(t, compareValues(a, b, traceqlparser.OpRegex))
}

// --- compareNumeric / toFloat64 ---

func TestToFloat64(t *testing.T) {
	assert.Equal(t, float64(42), toFloat64(Value{Type: TypeInt, Data: int64(42)}))
	assert.Equal(t, float64(3.14), toFloat64(Value{Type: TypeFloat, Data: float64(3.14)}))
	assert.Equal(t, float64(1_000_000), toFloat64(Value{Type: TypeDuration, Data: int64(1_000_000)}))
	// Unknown type returns 0
	assert.Equal(t, float64(0), toFloat64(Value{Type: TypeNil}))
	assert.Equal(t, float64(0), toFloat64(Value{Type: TypeString, Data: "x"}))
}

func TestCompareNumeric(t *testing.T) {
	lo := Value{Type: TypeInt, Data: int64(1)}
	hi := Value{Type: TypeInt, Data: int64(2)}

	assert.Equal(t, -1, compareNumeric(lo, hi))
	assert.Equal(t, 1, compareNumeric(hi, lo))
	assert.Equal(t, 0, compareNumeric(lo, lo))
}

// --- spanKindToInt64 / statusCodeToInt64 ---

func TestSpanKindToInt64(t *testing.T) {
	cases := []struct {
		kind string
		want int64
		ok   bool
	}{
		{"unspecified", 0, true},
		{"internal", 1, true},
		{"server", 2, true},
		{"client", 3, true},
		{"producer", 4, true},
		{"consumer", 5, true},
		{"unknown", 0, false},
		{"", 0, false},
	}
	for _, tc := range cases {
		got, ok := spanKindToInt64(tc.kind)
		assert.Equal(t, tc.ok, ok, "ok for %q", tc.kind)
		if ok {
			assert.Equal(t, tc.want, got, "value for %q", tc.kind)
		}
	}
}

func TestStatusCodeToInt64(t *testing.T) {
	cases := []struct {
		status string
		want   int64
		ok     bool
	}{
		{"unset", 0, true},
		{"ok", 1, true},
		{"error", 2, true},
		{"unknown", 0, false},
		{"", 0, false},
	}
	for _, tc := range cases {
		got, ok := statusCodeToInt64(tc.status)
		assert.Equal(t, tc.ok, ok, "ok for %q", tc.status)
		if ok {
			assert.Equal(t, tc.want, got, "value for %q", tc.status)
		}
	}
}

// --- AggBucket.Merge ---

func TestAggBucket_MergeNil(t *testing.T) {
	b := &AggBucket{Count: 5, Sum: 10.0, Min: 1.0, Max: 9.0}
	b.Merge(nil) // must not panic
	assert.Equal(t, int64(5), b.Count)
	assert.Equal(t, float64(10), b.Sum)
}

func TestAggBucket_MergeBasic(t *testing.T) {
	a := &AggBucket{Count: 3, Sum: 6.0, Min: 1.0, Max: 3.0}
	b := &AggBucket{Count: 2, Sum: 4.0, Min: 0.5, Max: 5.0}
	a.Merge(b)
	assert.Equal(t, int64(5), a.Count)
	assert.InDelta(t, 10.0, a.Sum, 1e-9)
	assert.InDelta(t, 0.5, a.Min, 1e-9)
	assert.InDelta(t, 5.0, a.Max, 1e-9)
}

func TestAggBucket_MergeEmptyOther(t *testing.T) {
	a := &AggBucket{Count: 3, Sum: 6.0, Min: 1.0, Max: 3.0}
	b := &AggBucket{Count: 0, Sum: 0}
	a.Merge(b)
	// Other has count 0, so Min/Max of a should remain
	assert.Equal(t, int64(3), a.Count)
	assert.InDelta(t, 6.0, a.Sum, 1e-9)
}

// --- MergeAggregationResults ---

func TestMergeAggregationResults_Empty(t *testing.T) {
	result := MergeAggregationResults()
	assert.NotNil(t, result)
	assert.Empty(t, result)
}

func TestMergeAggregationResults_Single(t *testing.T) {
	input := map[string]*AggBucket{
		"k1": {Count: 3, Sum: 9.0},
	}
	result := MergeAggregationResults(input)
	// Single result passes through unchanged
	assert.Equal(t, input, result)
}

func TestMergeAggregationResults_MultipleDisjoint(t *testing.T) {
	a := map[string]*AggBucket{"k1": {Count: 2, Sum: 4.0}}
	b := map[string]*AggBucket{"k2": {Count: 1, Sum: 1.0}}
	result := MergeAggregationResults(a, b)
	require.Contains(t, result, "k1")
	require.Contains(t, result, "k2")
	assert.Equal(t, int64(2), result["k1"].Count)
	assert.Equal(t, int64(1), result["k2"].Count)
}

func TestMergeAggregationResults_OverlappingKeys(t *testing.T) {
	a := map[string]*AggBucket{"k1": {Count: 2, Sum: 4.0, Min: 1.0, Max: 3.0}}
	b := map[string]*AggBucket{"k1": {Count: 3, Sum: 9.0, Min: 0.5, Max: 5.0}}
	result := MergeAggregationResults(a, b)
	require.Contains(t, result, "k1")
	assert.Equal(t, int64(5), result["k1"].Count)
	assert.InDelta(t, 13.0, result["k1"].Sum, 1e-9)
}

// --- compileMatchAllProgram ---

func TestCompileMatchAllProgram(t *testing.T) {
	prog := compileMatchAllProgram()
	require.NotNil(t, prog)
	require.NotNil(t, prog.ColumnPredicate)
}
