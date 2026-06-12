package writer

import (
	"sync/atomic"

	"github.com/grafana/blockpack/internal/modules/blockio/shared"
)

// NOTE: Any changes to this file must be reflected in the corresponding specs.md or NOTES.md.

// allPresentEncodingEnabled is the process-level rollout toggle for AllPresent encoding
// kinds (NOTE-AP-001). It defaults to true (compact AllPresent form selected for fully-present
// dense columns). NewWriterWithConfig sets it from Config.DisableAllPresentEncoding. It is an
// atomic.Bool because the encoders (which run on per-block goroutines) read it while a new
// writer construction may write it; the value is a deploy-level constant in practice, so the
// rare write/read overlap is benign — atomic access just removes the data race.
var allPresentEncodingEnabled atomic.Bool //nolint:gochecknoglobals // process-level rollout flag

func init() { //nolint:gochecknoinits // one-time default for the rollout flag
	allPresentEncodingEnabled.Store(true)
}

// setAllPresentEncodingEnabled sets the process-level AllPresent rollout flag.
func setAllPresentEncodingEnabled(v bool) {
	allPresentEncodingEnabled.Store(v)
}

// allPresentEnabled reports whether AllPresent encoding selection is active.
func allPresentEnabled() bool {
	return allPresentEncodingEnabled.Load()
}

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

	// AllPresent encoding kinds — re-exported from shared (NOTE-AP-001).
	KindDictionaryAllPresent      = shared.KindDictionaryAllPresent
	KindInlineBytesAllPresent     = shared.KindInlineBytesAllPresent
	KindDeltaUint64AllPresent     = shared.KindDeltaUint64AllPresent
	KindRLEIndexesAllPresent      = shared.KindRLEIndexesAllPresent
	KindXORBytesAllPresent        = shared.KindXORBytesAllPresent
	KindPrefixBytesAllPresent     = shared.KindPrefixBytesAllPresent
	KindDeltaDictionaryAllPresent = shared.KindDeltaDictionaryAllPresent
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
