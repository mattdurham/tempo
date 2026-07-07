package writer

// NOTE: Any changes to this file must be reflected in the corresponding specs.md or NOTES.md.

import (
	"io"

	"github.com/grafana/blockpack/internal/modules/blockio/reader"
)

// DedicatedColumn describes one attribute column to be written into the intrinsic section
// in addition to the standard block columns. Dedicated columns enable the zero-block-read
// fast path (NOTE-046) for queries that filter or aggregate on them.
//
// Name must be the full blockpack column name, including prefix (e.g. "span.http.method",
// "resource.deployment.environment"). Only span and resource attributes are supported;
// built-in intrinsic columns (span:start, span:name, etc.) are already always intrinsic.
//
// TYPE LIMITATIONS: the intrinsic accumulator currently only supports STRING, INT64,
// UINT64, and BYTES attribute types. Attributes of other types (notably BOOL and FLOAT64)
// configured as DedicatedColumns are SILENTLY SKIPPED during block writing — no error
// is returned, but the column will not actually be intrinsic and will fall back to the
// generic attribute KV scan path. If you configure a dedicated column and don't see the
// expected speedup, check the attribute's wire type. BOOL/FLOAT64 support is a follow-up.

// Config holds configuration parameters for a blockpack writer instance.
// Field order is optimized for struct alignment (betteralign).
type Config struct {
	OutputStream io.Writer

	// NOTE: EnableV2Format removed (2026-06-29). V2 lean format is now unconditional:
	// all blocks are page-aligned, FooterV9 always, no in-file pruning sections.
	// Querying relies entirely on the value-index pipeline.

	// ValueIndexSink, if non-nil, is called after a successful Flush with a
	// Reader opened over the written bytes. The sink can use it to extract
	// value-index entries and write them inline — replacing the async
	// value-index-consumer pipeline for callers (block builder, compactor)
	// that prefer synchronous in-process indexing.
	//
	// The Reader is owned by the writer; the sink MUST NOT close it.
	// Any error returned by the sink aborts Flush and is returned to the caller.
	ValueIndexSink func(r *reader.Reader) error

	// ScratchDir is a local directory for the writer's on-disk scratch files, currently the
	// per-column intrinsic spill files (NOTE-461, issue #380). When empty, a unique temp
	// directory under os.TempDir() is created and removed automatically at Flush(). When set,
	// the caller owns the directory; the writer creates and removes only its own files inside
	// it. Compaction passes its StagingDir here so spill files land on the same volume as the
	// staged output block, keeping all of compaction's disk I/O on the configured scratch
	// volume rather than the default /tmp.
	ScratchDir string

	// DedicatedColumns lists span and resource attribute columns to be written into
	// the intrinsic section as well as the standard block columns. Writing a column
	// here enables the zero-block-read fast path (NOTE-046) for metrics queries that
	// filter or group-by on that column, at the cost of extra intrinsic section storage.
	//
	// Each Name must be the full blockpack column name including prefix, e.g.:
	//   "span.http.method", "span.http.status_code", "resource.deployment.environment"
	//
	// Built-in intrinsic columns (span:start, span:name, resource.service.name, etc.)
	// are already always written to the intrinsic section and need not be listed here.
	DedicatedColumns []DedicatedColumn

	MaxBlockSpans int

	// MinBlockSpans is the minimum number of spans to accumulate before the writer
	// will flush an inner block at a (service.name, span.name) group boundary.
	//
	// NOTE-474: boundary-aware slicing emits a block at a group boundary when the
	// current block holds ≥ MinBlockSpans spans. This keeps homogeneous groups
	// together (better dictionary/RLE compression) while batching tiny groups into
	// reasonably-sized blocks instead of emitting one block per rare operation.
	// MaxBlockSpans is the hard cap regardless of group boundaries.
	//
	// NOTE-091 caveat: structural queries (>>, <<, >, ~) assume most spans of a
	// trace reside in a single inner block. Boundary-aware slicing splits spans by
	// (svc, op), so multi-service traces will span multiple blocks. Intermediate
	// ancestor spans from services that do not match either structural predicate may
	// reside in pruned blocks, causing false negatives. This is tracked as a known
	// limitation to be fixed in the structural executor (see NOTE-091).
	//
	// 0 means use the default of defaultMinBlockSpans (100).
	MinBlockSpans int

	// MaxBufferedSpans is the maximum number of spans buffered before an automatic
	// flush of completed blocks is performed. When len(w.pending) reaches this limit,
	// spans are sorted, encoded into blocks, and written to the output stream; the
	// pending buffer is then cleared and protoRoots is released to bound RSS.
	//
	// 0 means use the default of 5 × MaxBlockSpans.
	// Set a large value (e.g. math.MaxInt) to effectively disable auto-flush.
	//
	// The auto-flush path preserves all NOTES §17 invariants: metadata, header,
	// and footer are written only at the final Flush().
	MaxBufferedSpans int

	// DisableBitPackedDelta turns off selection of the bit-packed DeltaUint64 encoding kind
	// (NOTE-215). When false (the default), uint64 columns chosen for delta encoding are
	// written using the bit-packed variant whenever it saves a meaningful fraction of the
	// byte-width payload (see SPEC-006). Set to true during rollout to force the legacy
	// byte-width form (kind 5), so a writer can be deployed ahead of readers that understand
	// the new kind.
	//
	// Reading is unaffected by this flag — readers always accept both forms.
	DisableBitPackedDelta bool

	// DisablePagedDelta turns off selection of the per-page DeltaUint64 encoding kind
	// (NOTE-218). When false (the default), uint64 columns chosen for delta encoding whose
	// span_count is large enough to span multiple pages are written using the per-page variant
	// (kind 39) whenever per-page bit-width adaptation saves a meaningful fraction of the
	// column-wide bit-packed payload (see SPEC-006). Set to true during rollout to force the
	// single-page forms (kinds 5/22), so a writer can be deployed ahead of readers that
	// understand the new kind.
	//
	// Reading is unaffected by this flag — readers always accept both forms.
	DisablePagedDelta bool

	// DisableUniformBytes turns off selection of the uniform-length XORBytes encoding kinds
	// (NOTE-217). When false (the default), XOR-encoded byte columns whose present values
	// all share the same byte length are written using the uniform variant that drops the
	// per-row len[4] prefix (kinds 24/25/28). Set to true during rollout to force the legacy
	// variable-length form (kinds 8/9/19), so a writer can be deployed ahead of readers that
	// understand the new kinds.
	//
	// Reading is unaffected by this flag — readers always accept both forms.
	DisableUniformBytes bool

	// DisableGorillaFloat64 turns off selection of the Gorilla-XOR Float64 encoding kinds
	// (NOTE-219). When false (the default), high-cardinality, value-correlated float64 columns
	// are written using the Gorilla-XOR variant (kinds 40/41) instead of the Dictionary path
	// (kinds 1/2) which provides no real deduplication for such columns (see SPEC-006). Low-
	// cardinality float columns stay on Dictionary+RLE regardless of this flag. Set to true
	// during rollout to force the Dictionary form for every float column, so a writer can be
	// deployed ahead of readers that understand the new kinds.
	//
	// Reading is unaffected by this flag — readers always accept both forms.
	DisableGorillaFloat64 bool

	// EnableInlineColumns turns ON V15 inline-column TOC entries (NOTE-220). When true the
	// writer emits VersionBlockV15 blocks and stores tiny columns (whose raw blob is strictly
	// smaller inline than the offset+snappy form) directly in the TOC entry, skipping the
	// offset indirection and the per-column outer snappy. When false (the DEFAULT) the writer
	// emits VersionBlockV14 blocks exactly as before.
	//
	// Unlike the Disable* flags this defaults to OFF because V15 is a block-format version
	// bump: V14-only readers cannot read a V15 block, so a writer emitting V15 must be deployed
	// AFTER readers understand V15. Readers in this codebase accept both V14 and V15.
	EnableInlineColumns bool

	// EnableZstdColumns turns ON per-column zstd compression of V15 offset-addressed column
	// blobs (NOTE-405, issue #355). When true the writer compresses each non-inline V15 column
	// blob with both snappy and zstd and keeps zstd only when it is meaningfully smaller (the
	// blob is flagged with shared.ColFlagZstd; incompressible/bit-packed blobs stay on snappy).
	// When false (the DEFAULT) every column blob uses snappy exactly as before.
	//
	// Requires EnableInlineColumns (V15 blocks): the zstd codec is signaled by a flag bit in
	// the V15 per-column flags byte, which V14 does not have. The reader is always codec-aware,
	// so any V15 reader decodes zstd blobs without a further version bump. Defaults OFF so it
	// can be rolled out deliberately and reverted by toggle without a format change.
	EnableZstdColumns bool
}
