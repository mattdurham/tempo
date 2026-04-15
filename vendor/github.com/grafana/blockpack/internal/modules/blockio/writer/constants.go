package writer

import "github.com/grafana/blockpack/internal/modules/blockio/shared"

// NOTE: Any changes to this file must be reflected in the corresponding specs.md or NOTES.md.

// Encoding kind constants per SPECS §9 — canonical definitions live in shared.Kind*.
// These aliases are preserved so writer-internal code continues to compile unchanged.
const (
	KindDictionary            = shared.KindDictionary
	KindSparseDictionary      = shared.KindSparseDictionary
	KindInlineBytes           = shared.KindInlineBytes
	KindSparseInlineBytes     = shared.KindSparseInlineBytes
	KindDeltaUint64           = shared.KindDeltaUint64
	KindRLEIndexes            = shared.KindRLEIndexes
	KindSparseRLEIndexes      = shared.KindSparseRLEIndexes
	KindXORBytes              = shared.KindXORBytes
	KindSparseXORBytes        = shared.KindSparseXORBytes
	KindPrefixBytes           = shared.KindPrefixBytes
	KindSparsePrefixBytes     = shared.KindSparsePrefixBytes
	KindDeltaDictionary       = shared.KindDeltaDictionary
	KindSparseDeltaDictionary = shared.KindSparseDeltaDictionary
	KindVectorF32             = shared.KindVectorF32
)

// Trace intrinsic column name constants — aliases to canonical definitions in shared.
const (
	traceIDColumnName       = shared.TraceIDColumnName
	spanIDColumnName        = shared.SpanIDColumnName
	spanParentIDColumnName  = shared.SpanParentIDColumnName
	spanNameColumnName      = shared.SpanNameColumnName
	spanKindColumnName      = shared.SpanKindColumnName
	spanStartColumnName     = shared.SpanStartColumnName
	spanEndColumnName       = shared.SpanEndColumnName
	spanDurationColumnName  = shared.SpanDurationColumnName
	spanStatusColumnName    = shared.SpanStatusColumnName
	spanStatusMsgColumnName = shared.SpanStatusMsgColumnName
	svcNameColumnName       = shared.SvcNameColumnName
)

// Log intrinsic column name constants — aliases to canonical definitions in shared.
const (
	logTimestampColumnName         = shared.LogTimestampColumnName
	logObservedTimestampColumnName = shared.LogObservedTimestampColumnName
	logBodyColumnName              = shared.LogBodyColumnName
	logSeverityNumberColumnName    = shared.LogSeverityNumberColumnName
	logSeverityTextColumnName      = shared.LogSeverityTextColumnName
	logTraceIDColumnName           = shared.LogTraceIDColumnName
	logSpanIDColumnName            = shared.LogSpanIDColumnName
	logFlagsColumnName             = shared.LogFlagsColumnName
)

const (
	defaultMaxBlockSpans      = 2000
	estimatedBytesPerSpan     = 2048
	uuidSampleCount           = 8
	rleCardinalityThreshold   = 3
	sparseNullRatioThreshold  = 0.50
	deltaRangeThreshold16     = 65535
	deltaRangeThreshold32     = 4_294_967_295
	deltaCardinalityThreshold = 3
	defaultRangeBuckets       = 1000
	// exactCardinalityThreshold is the maximum number of distinct values for a range
	// column to use an exact-value index instead of KLL quantile boundaries. When a
	// column has ≤ this many distinct values across all blocks, each value maps directly
	// to its block IDs with zero false positives. Above this threshold, the KLL-based
	// range index is used (bounded false positives, O(1) memory per column).
	// NOTE-38: exact-value index for low-cardinality columns.
	exactCardinalityThreshold = 100
	// rangeBucketKeyMaxLen is the maximum byte length of a RangeString/RangeBytes
	// bucket key. Keys are the lower boundary of the bucket range, truncated to this length.
	// See SPECS §5.2.1.
	rangeBucketKeyMaxLen = 50
)
