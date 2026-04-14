package executor_test

// NOTE: Any changes to this file must be reflected in the corresponding SPECS.md or NOTES.md.

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
	commonv1 "go.opentelemetry.io/proto/otlp/common/v1"
	tracev1 "go.opentelemetry.io/proto/otlp/trace/v1"
)

// intAttr creates an integer OTel KeyValue attribute.
func intAttr(key string, val int64) *commonv1.KeyValue {
	return &commonv1.KeyValue{
		Key:   key,
		Value: &commonv1.AnyValue{Value: &commonv1.AnyValue_IntValue{IntValue: val}},
	}
}

// TestScanRegexFast_NonStringColumn verifies that a regex MATCH predicate on a
// non-string (integer) column produces 0 matches. Without the column-type guard,
// patterns like ".*" would incorrectly match via the empty-string fallback.
func TestScanRegexFast_NonStringColumn(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer
	w := mustNewWriter(t, &buf, 0)
	tid := [16]byte{0x01}
	// Use an integer span attribute (not a string column).
	addSpan(t, w, tid, 0, "op", tracev1.Span_SPAN_KIND_SERVER,
		[]*commonv1.KeyValue{intAttr("status", 500)}, nil)
	mustFlush(t, w)

	r := openReader(t, buf.Bytes())
	// ".*" matches any string; applied to a non-string int column, must match 0 rows.
	rows := runQuery(t, r, `{ span.status =~ ".*" }`)
	assert.Equal(t, 0, len(rows), "regex MATCH on non-string column must return 0 rows")
}

// TestScanRegexFast_PresentEmptyString verifies that a present empty-string attribute
// is correctly distinguished from an absent attribute: ".*" matches "", so the row
// with a present empty-string value should be included; absent rows should not.
func TestScanRegexFast_PresentEmptyString(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer
	w := mustNewWriter(t, &buf, 0)
	tid := [16]byte{0x02}
	// Span 0: present empty-string attribute.
	addSpan(t, w, tid, 0, "op", tracev1.Span_SPAN_KIND_SERVER,
		[]*commonv1.KeyValue{strAttr("tier", "")}, nil)
	// Span 1: absent attribute (no tier).
	addSpan(t, w, tid, 1, "op", tracev1.Span_SPAN_KIND_SERVER, nil, nil)
	mustFlush(t, w)

	r := openReader(t, buf.Bytes())
	// ".*" matches any string including "". Only span 0 (present empty) should match.
	rows := runQuery(t, r, `{ span.tier =~ ".*" }`)
	assert.Equal(t, 1, len(rows), "present empty-string should match .*, absent should not")
}

// TestScanRegexFast_CILiteralBypass verifies that case-insensitive literal patterns
// use the fold-contains path (re==nil) and produce correct results.
func TestScanRegexFast_CILiteralBypass(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer
	w := mustNewWriter(t, &buf, 0)
	tid := [16]byte{0x03}
	addSpan(t, w, tid, 0, "op", tracev1.Span_SPAN_KIND_SERVER,
		[]*commonv1.KeyValue{strAttr("level", "ERROR")}, nil)
	addSpan(t, w, tid, 1, "op", tracev1.Span_SPAN_KIND_SERVER,
		[]*commonv1.KeyValue{strAttr("level", "WARN")}, nil)
	addSpan(t, w, tid, 2, "op", tracev1.Span_SPAN_KIND_SERVER,
		[]*commonv1.KeyValue{strAttr("level", "info")}, nil)
	mustFlush(t, w)

	r := openReader(t, buf.Bytes())
	// "(?i)error" — fold-contains bypass (IsLiteralContains=true). Only span 0 ("ERROR") should match.
	rows := runQuery(t, r, `{ span.level =~ "(?i)error" }`)
	assert.Equal(t, 1, len(rows), "(?i)error should match ERROR case-insensitively")

	// "(?i)error|warn" should match spans 0 and 1.
	rows2 := runQuery(t, r, `{ span.level =~ "(?i)error|warn" }`)
	assert.Equal(t, 2, len(rows2), "(?i)error|warn should match ERROR and WARN")
}
