package writer

// NOTE: Any changes to this file must be reflected in the corresponding specs.md or NOTES.md.

import "io"

// TextEmbedder converts text to a float32 embedding vector.
// This is a local interface matching vm.TextEmbedder — defined here to avoid
// an import cycle between writer and vm packages. Go structural typing means
// any vm.TextEmbedder or embedder.Embedder satisfies this interface.
type TextEmbedder interface {
	Embed(text string) ([]float32, error)
}

// EmbeddingFieldConfig describes one span field to include in auto-embedding text.
type EmbeddingFieldConfig struct {
	// Name is the column name (e.g. "span.name", "resource.service.name").
	Name string
	// Weight controls ordering: "primary" (first, plain text), "context" (key=value), "secondary" (last).
	Weight string
}

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
}
