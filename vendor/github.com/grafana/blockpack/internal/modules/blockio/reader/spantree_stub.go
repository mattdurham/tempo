package reader

// SpanTree stubs — the SpanTree structural index was removed in issue #434.
// All callers already handle nil/false returns and fall through to block columns
// or the IntrinsicTOC for identity field resolution.

import "github.com/grafana/blockpack/internal/modules/blockio/shared"

// HasSpanTree always returns false. The SpanTree section is no longer written (#434).
func (r *Reader) HasSpanTree() bool { return false }

// SpanTreeForTrace always returns nil. Callers fall through to block-column scan.
func (r *Reader) SpanTreeForTrace(_ [16]byte) ([]shared.SpanTreeRecord, error) { return nil, nil }

// SpanTreeIdentityForBlock always returns nil. Callers handle nil with "if idMap == nil".
func (r *Reader) SpanTreeIdentityForBlock(_ uint16) (map[uint16]shared.SpanTreeRecord, error) {
	return nil, nil
}

// SpanTreeRecordForSpan always returns the zero value and false.
func (r *Reader) SpanTreeRecordForSpan(_ [8]byte) (shared.SpanTreeRecord, bool, error) {
	return shared.SpanTreeRecord{}, false, nil
}

// SpanTreeChunksForSpan always returns nil.
func (r *Reader) SpanTreeChunksForSpan(_ [8]byte) ([]int, error) { return nil, nil }
