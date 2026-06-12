package writer

// NOTE: Any changes to this file must be reflected in the corresponding specs.md or NOTES.md.

import (
	"io"

	"github.com/grafana/blockpack/internal/modules/blockio/shared"
)

// TextEmbedder is the canonical embedding interface, defined in the shared package.
// Aliased here for backwards compatibility within this package.
type TextEmbedder = shared.TextEmbedder

// EmbeddingFieldConfig describes one span field to include in auto-embedding text.

// Name is the column name (e.g. "span.name", "resource.service.name").

// Weight controls ordering: "primary" (first, plain text), "context" (key=value), "secondary" (last).

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

	// Embedder enables automatic embedding of spans during block building.
	// When non-nil, the writer assembles text from each span's fields (using
	// EmbeddingFields or all fields by default), calls Embedder.Embed(), and
	// stores the vector as the __embedding__ column. VectorDimension is set
	// automatically from the first embedding result.
	//
	// When nil, the writer only stores vectors that are explicitly provided
	// as __embedding__ span attributes (the current behavior).
	Embedder TextEmbedder

	// EmbeddingFields configures which span fields are included in the auto-embedding
	// text. If empty and Embedder is non-nil, all fields are included using the
	// default priority ordering (AssembleAllFields).
	EmbeddingFields []EmbeddingFieldConfig

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

	// MaxBufferedSpans is the maximum number of spans buffered before an automatic
	// flush of completed blocks is performed. When len(w.pending) reaches this limit,
	// spans are sorted, encoded into blocks, and written to the output stream; the
	// pending buffer is then cleared and protoRoots is released to bound RSS.
	//
	// 0 means use the default of 5 × MaxBlockSpans.
	// Set a large value (e.g. math.MaxInt) to effectively disable auto-flush.
	//
	// The auto-flush path preserves all NOTES §17 invariants: the KLL two-pass
	// for range buckets still happens at final Flush() only. The range index
	// accumulates across all flushBlocks() calls and is consumed once at Flush().
	MaxBufferedSpans int

	// VectorDimension is the expected float32 vector dimension for __embedding__ columns.
	// 0 means no vector support — the writer will not build a VectorIndex section.
	// When > 0, vectors encountered during block building are accumulated and a V5 footer
	// is written at Flush time. Typical value: 768 (nomic-embed-text-v1.5).
	VectorDimension int

	// DisableAllPresentEncoding turns off selection of the AllPresent encoding kinds
	// (NOTE-AP-001). When false (the default), fully-present dense columns are written
	// using the compact AllPresent variant that omits the presence_rle segment. Set to
	// true during rollout to force the legacy presence-RLE form for every column, so a
	// writer can be deployed ahead of readers that understand the new kinds.
	//
	// Reading is unaffected by this flag — readers always accept both forms.
	DisableAllPresentEncoding bool

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
}
