package writer

// NOTE: Any changes to this file must be reflected in the corresponding specs.md or NOTES.md.

import "io"

// Config holds configuration parameters for a blockpack writer instance.
type Config struct {
	OutputStream     io.Writer
	MaxBlockSpans    int
	BlockTargetBytes int64
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
}
