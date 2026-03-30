package executor

import (
	modules_shared "github.com/grafana/blockpack/internal/modules/blockio/shared"
	"github.com/grafana/blockpack/internal/modules/queryplanner"
	"github.com/grafana/blockpack/internal/vm"
)

// EncodeValueForTest exposes encodeValue for black-box tests in package executor_test.
func EncodeValueForTest(v vm.Value, colType modules_shared.ColumnType) (string, bool) {
	return encodeValue(v, colType)
}

// InferColTypeForTest exposes inferColTypeFromValues for black-box tests.
func InferColTypeForTest(minVal, maxVal *vm.Value) modules_shared.ColumnType {
	return inferColTypeFromValues(minVal, maxVal)
}

// RangeTypeSentinelMinForTest exposes rangeTypeSentinelMin for black-box tests.
func RangeTypeSentinelMinForTest(colType modules_shared.ColumnType) (string, bool) {
	return rangeTypeSentinelMin(colType)
}

// RangeTypeSentinelMaxForTest exposes rangeTypeSentinelMax for black-box tests.
func RangeTypeSentinelMaxForTest(colType modules_shared.ColumnType) (string, bool) {
	return rangeTypeSentinelMax(colType)
}

// IsASCIIForTest exposes isASCII for black-box tests.
func IsASCIIForTest(s string) bool {
	return isASCII(s)
}

// BuildCaseInsensitiveRegexPredicateForTest exposes buildCaseInsensitiveRegexPredicate.
func BuildCaseInsensitiveRegexPredicateForTest(
	col string,
	colType modules_shared.ColumnType,
	analysis *vm.RegexAnalysis,
) queryplanner.Predicate {
	return buildCaseInsensitiveRegexPredicate(col, colType, analysis)
}

// BuildCaseSensitiveSinglePrefixPredicateForTest exposes buildCaseSensitiveSinglePrefixPredicate.
func BuildCaseSensitiveSinglePrefixPredicateForTest(
	col string,
	colType modules_shared.ColumnType,
	pattern string,
	analysis *vm.RegexAnalysis,
) queryplanner.Predicate {
	return buildCaseSensitiveSinglePrefixPredicate(col, colType, pattern, analysis)
}

// SearchMetaCols exposes searchMetaCols for invariant tests.
var SearchMetaCols = searchMetaCols

// TraceIntrinsicColumns exposes traceIntrinsicColumns for invariant tests.
var TraceIntrinsicColumns = traceIntrinsicColumns

// ComputeOverFetchForTest exposes the overFetch calculation used in
// BlockRefsFromIntrinsicTOC so that predicates_test.go can verify the cap logic.
// nodeCount is the number of top-level predicate nodes (AND conditions).
// totalLeaves is the sum of all leaf nodes across all top-level nodes.
// limit is the query result limit.
func ComputeOverFetchForTest(nodeCount, totalLeaves, limit int) int {
	return computeOverFetch(limit, nodeCount, totalLeaves)
}
