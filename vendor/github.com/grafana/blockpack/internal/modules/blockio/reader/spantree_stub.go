package reader

// SpanTree stub — the SpanTree structural index was removed in issue #434.
// HasSpanTree is retained as a regression guard (mirrors HasIntrinsicSection): v2 files
// never carry a SpanTree section, and tests assert this stays false.

// HasSpanTree always returns false. The SpanTree section is no longer written (#434).
func (r *Reader) HasSpanTree() bool { return false }
