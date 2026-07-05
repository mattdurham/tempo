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

// bitPackedDeltaEnabled is the process-level rollout toggle for the bit-packed DeltaUint64
// encoding kind (NOTE-215). It defaults to true. NewWriterWithConfig sets it from
// Config.DisableBitPackedDelta. Atomic for the same reason as allPresentEncodingEnabled:
// a deploy-level constant in practice, atomic access just removes the data race between
// per-block encoder goroutines and a concurrent writer construction.
var bitPackedDeltaEncodingEnabled atomic.Bool //nolint:gochecknoglobals // process-level rollout flag

func init() { //nolint:gochecknoinits // one-time default for the rollout flag
	bitPackedDeltaEncodingEnabled.Store(true)
}

// setBitPackedDeltaEnabled sets the process-level bit-packed DeltaUint64 rollout flag.
func setBitPackedDeltaEnabled(v bool) {
	bitPackedDeltaEncodingEnabled.Store(v)
}

// bitPackedDeltaEnabled reports whether bit-packed DeltaUint64 selection is active.
func bitPackedDeltaEnabled() bool {
	return bitPackedDeltaEncodingEnabled.Load()
}

// pagedDeltaEncodingEnabled is the process-level rollout toggle for the per-page DeltaUint64
// encoding kind (NOTE-218). It defaults to true. NewWriterWithConfig sets it from
// Config.DisablePagedDelta. Atomic for the same reason as bitPackedDeltaEncodingEnabled:
// a deploy-level constant in practice, atomic access just removes the data race between
// per-block encoder goroutines and a concurrent writer construction.
var pagedDeltaEncodingEnabled atomic.Bool //nolint:gochecknoglobals // process-level rollout flag

func init() { //nolint:gochecknoinits // one-time default for the rollout flag
	pagedDeltaEncodingEnabled.Store(true)
}

// setPagedDeltaEnabled sets the process-level per-page DeltaUint64 rollout flag.
func setPagedDeltaEnabled(v bool) {
	pagedDeltaEncodingEnabled.Store(v)
}

// pagedDeltaEnabled reports whether per-page DeltaUint64 selection is active.
func pagedDeltaEnabled() bool {
	return pagedDeltaEncodingEnabled.Load()
}

// uniformBytesEncodingEnabled is the process-level rollout toggle for the uniform-length
// XORBytes encoding kinds (NOTE-217). It defaults to true. NewWriterWithConfig sets it from
// Config.DisableUniformBytes. Atomic for the same reason as allPresentEncodingEnabled.
var uniformBytesEncodingEnabled atomic.Bool //nolint:gochecknoglobals // process-level rollout flag

func init() { //nolint:gochecknoinits // one-time default for the rollout flag
	uniformBytesEncodingEnabled.Store(true)
}

// setUniformBytesEnabled sets the process-level uniform-length XORBytes rollout flag.
func setUniformBytesEnabled(v bool) {
	uniformBytesEncodingEnabled.Store(v)
}

// uniformBytesEnabled reports whether uniform-length XORBytes selection is active.
func uniformBytesEnabled() bool {
	return uniformBytesEncodingEnabled.Load()
}

// gorillaFloat64EncodingEnabled is the process-level rollout toggle for the Gorilla-XOR Float64
// encoding kinds (NOTE-219). It defaults to true. NewWriterWithConfig sets it from
// Config.DisableGorillaFloat64. Atomic for the same reason as allPresentEncodingEnabled:
// a deploy-level constant in practice, atomic access just removes the data race between
// per-block encoder goroutines and a concurrent writer construction.
var gorillaFloat64EncodingEnabled atomic.Bool //nolint:gochecknoglobals // process-level rollout flag

func init() { //nolint:gochecknoinits // one-time default for the rollout flag
	gorillaFloat64EncodingEnabled.Store(true)
}

// setGorillaFloat64Enabled sets the process-level Gorilla-XOR Float64 rollout flag.
func setGorillaFloat64Enabled(v bool) {
	gorillaFloat64EncodingEnabled.Store(v)
}

// gorillaFloat64Enabled reports whether Gorilla-XOR Float64 selection is active.
func gorillaFloat64Enabled() bool {
	return gorillaFloat64EncodingEnabled.Load()
}

// inlineColumnsEnabled is the process-level rollout toggle for V15 inline-column TOC
// entries (NOTE-220). It defaults to false because V15 is a block-format version bump
// that V14-only readers cannot read; a writer must be deployed AFTER readers understand
// V15. When true, NewWriterWithConfig emits VersionBlockV15 blocks and tiny columns whose
// raw blob is strictly smaller inline are stored directly in the TOC entry. Atomic for the
// same reason as allPresentEncodingEnabled: a deploy-level constant in practice, atomic
// access just removes the data race between per-block encoder goroutines and a concurrent
// writer construction.
var inlineColumnsEnabled atomic.Bool //nolint:gochecknoglobals // process-level rollout flag

// setInlineColumnsEnabled sets the process-level V15 inline-column rollout flag.
func setInlineColumnsEnabled(v bool) {
	inlineColumnsEnabled.Store(v)
}

// inlineColumnsActive reports whether V15 inline-column emission is active.
func inlineColumnsActive() bool {
	return inlineColumnsEnabled.Load()
}

// zstdColumnsEnabled is the process-level rollout toggle for per-column zstd compression of
// V15 offset-addressed column blobs (NOTE-405, issue #355). It defaults to false. When true,
// the writer compresses each V15 (non-inline) column blob with BOTH snappy and zstd and keeps
// zstd only when it beats snappy by the benefit margin (zstdBenefitNum/zstdBenefitDen),
// flagging those blobs with shared.ColFlagZstd. Reading is always codec-aware (additive), so
// unlike the V15 version bump itself this is a pure encoder-side choice that any V15 reader
// handles; it is still gated to default-OFF so it can be rolled out deliberately and reverted
// by toggle without a format change. Atomic for the same reason as inlineColumnsEnabled: a
// deploy-level constant in practice, atomic access removes the encoder-goroutine data race.
var zstdColumnsEnabled atomic.Bool //nolint:gochecknoglobals // process-level rollout flag

// setZstdColumnsEnabled sets the process-level per-column zstd rollout flag.
func setZstdColumnsEnabled(v bool) {
	zstdColumnsEnabled.Store(v)
}

// zstdColumnsActive reports whether per-column zstd emission is active.
func zstdColumnsActive() bool {
	return zstdColumnsEnabled.Load()
}

// zstd benefit gate (NOTE-405, issue #355). A column blob switches from snappy to zstd only
// when len(zstd) * zstdBenefitDen < len(snappy) * zstdBenefitNum, i.e. zstd is at least
// (1 - Num/Den) smaller than snappy. With Num=97, Den=100 a blob must be >=3% smaller under
// zstd to switch — enough to comfortably clear noise on already-small blobs and to leave
// incompressible/bit-packed columns (span:start/span:end show ~0% headroom) on snappy, while
// still capturing the large dict/ID-column wins (trace:id ~70% smaller, span:parent_id ~40%).
const (
	zstdBenefitNum = 97
	zstdBenefitDen = 100
)

// emittedBlockVersion returns the block-header version the writer should emit. V15 when the
// inline-column rollout flag is set (NOTE-220), V14 otherwise.
func emittedBlockVersion() uint8 {
	if inlineColumnsActive() {
		return shared.VersionBlockV15
	}
	return shared.VersionBlockV14
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

	// AllPresent encoding kinds — re-exported from shared (NOTE-AP-001).
	KindDictionaryAllPresent      = shared.KindDictionaryAllPresent
	KindInlineBytesAllPresent     = shared.KindInlineBytesAllPresent
	KindDeltaUint64AllPresent     = shared.KindDeltaUint64AllPresent
	KindRLEIndexesAllPresent      = shared.KindRLEIndexesAllPresent
	KindXORBytesAllPresent        = shared.KindXORBytesAllPresent
	KindPrefixBytesAllPresent     = shared.KindPrefixBytesAllPresent
	KindDeltaDictionaryAllPresent = shared.KindDeltaDictionaryAllPresent

	// Bit-packed DeltaUint64 kinds — re-exported from shared (NOTE-215).
	KindDeltaUint64BitPacked           = shared.KindDeltaUint64BitPacked
	KindDeltaUint64BitPackedAllPresent = shared.KindDeltaUint64BitPackedAllPresent

	// Uniform-length byte-column kinds — re-exported from shared (NOTE-217).
	KindXORBytesUniform           = shared.KindXORBytesUniform
	KindSparseXORBytesUniform     = shared.KindSparseXORBytesUniform
	KindXORBytesUniformAllPresent = shared.KindXORBytesUniformAllPresent

	// Per-page DeltaUint64 kind — re-exported from shared (NOTE-218).
	KindDeltaUint64Paged = shared.KindDeltaUint64Paged

	// Gorilla Float64 kinds — re-exported from shared (NOTE-219).
	KindGorillaFloat64           = shared.KindGorillaFloat64
	KindGorillaFloat64AllPresent = shared.KindGorillaFloat64AllPresent
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

const (
	defaultMaxBlockSpans      = 10000
	defaultMinBlockSpans      = 5000 // NOTE-474: flush at boundary only when block is ≥50% full
	estimatedBytesPerSpan     = 2048
	uuidSampleCount           = 8
	rleCardinalityThreshold   = 3
	sparseNullRatioThreshold  = 0.50
	deltaRangeThreshold16     = 65535
	deltaRangeThreshold32     = 4_294_967_295
	deltaCardinalityThreshold = 3
)
