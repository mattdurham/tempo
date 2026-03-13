package writer

// NOTE: Any changes to this file must be reflected in the corresponding specs.md or NOTES.md.

// Encoding kind constants per SPECS §9.
const (
	KindDictionary       uint8 = 1
	KindSparseDictionary uint8 = 2
	// KindInlineBytes and KindSparseInlineBytes are reader-only: they were emitted by
	// earlier writer versions and must remain decodable. The current writer never selects
	// these kinds — all bytes columns are encoded as KindXORBytes (8) or KindPrefixBytes (10).
	KindInlineBytes           uint8 = 3
	KindSparseInlineBytes     uint8 = 4
	KindDeltaUint64           uint8 = 5
	KindRLEIndexes            uint8 = 6
	KindSparseRLEIndexes      uint8 = 7
	KindXORBytes              uint8 = 8
	KindSparseXORBytes        uint8 = 9
	KindPrefixBytes           uint8 = 10
	KindSparsePrefixBytes     uint8 = 11
	KindDeltaDictionary       uint8 = 12
	KindSparseDeltaDictionary uint8 = 13
)

// traceIDColumnName is the name of the trace ID column, excluded from the range index.
const traceIDColumnName = "trace:id"

// Log intrinsic column names (SPECS §11 log signal extension).
const (
	logTimestampColumnName         = "log:timestamp"
	logObservedTimestampColumnName = "log:observed_timestamp"
	logBodyColumnName              = "log:body"
	logSeverityNumberColumnName    = "log:severity_number"
	logSeverityTextColumnName      = "log:severity_text"
	logTraceIDColumnName           = "log:trace_id"
	logSpanIDColumnName            = "log:span_id"
	logFlagsColumnName             = "log:flags"
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
