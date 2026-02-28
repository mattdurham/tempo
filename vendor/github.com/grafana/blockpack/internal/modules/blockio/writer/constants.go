package writer

// NOTE: Any changes to this file must be reflected in the corresponding specs.md or NOTES.md.

// Encoding kind constants per SPECS §9.
const (
	KindDictionary            uint8 = 1
	KindSparseDictionary      uint8 = 2
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
	// rangeBucketKeyMaxLen is the maximum byte length of a RangeString/RangeBytes
	// bucket key. Keys are the lower boundary of the bucket range, truncated to this length.
	// See SPECS §5.2.1.
	rangeBucketKeyMaxLen = 50
)
