package executor_test

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	tracev1 "go.opentelemetry.io/proto/otlp/trace/v1"

	"github.com/grafana/blockpack/internal/modules/executor"
	"github.com/grafana/blockpack/internal/traceqlparser"
	"github.com/grafana/blockpack/internal/vm"
)

func TestStatusKindQuery(t *testing.T) {
	var buf bytes.Buffer
	w := mustNewWriter(t, &buf, 0)

	traceID := [16]byte{0x01}
	// Add a span with status=error, kind=client
	span := &tracev1.Span{
		TraceId:           traceID[:],
		SpanId:            []byte{0x01, 0, 0, 0, 0, 0, 0, 0},
		Name:              "test",
		StartTimeUnixNano: 1_000_000_000,
		EndTimeUnixNano:   2_000_000_000,
		Kind:              tracev1.Span_SPAN_KIND_CLIENT,
		Status: &tracev1.Status{
			Code: tracev1.Status_STATUS_CODE_ERROR,
		},
	}
	require.NoError(t, w.AddSpan(traceID[:], span, map[string]any{"service.name": "test-svc"}, "", nil, ""))

	// Add a span with status=ok, kind=server
	span2 := &tracev1.Span{
		TraceId:           traceID[:],
		SpanId:            []byte{0x02, 0, 0, 0, 0, 0, 0, 0},
		Name:              "test2",
		StartTimeUnixNano: 2_000_000_000,
		EndTimeUnixNano:   3_000_000_000,
		Kind:              tracev1.Span_SPAN_KIND_SERVER,
		Status: &tracev1.Status{
			Code: tracev1.Status_STATUS_CODE_OK,
		},
	}
	require.NoError(t, w.AddSpan(traceID[:], span2, map[string]any{"service.name": "test-svc"}, "", nil, ""))

	mustFlush(t, w)
	r := openReader(t, buf.Bytes())

	runQ := func(query string) int {
		t.Helper()
		parsed, err := traceqlparser.ParseTraceQL(query)
		require.NoError(t, err)
		filterExpr, ok := parsed.(*traceqlparser.FilterExpression)
		require.True(t, ok)
		prog, err := vm.CompileTraceQLFilter(filterExpr)
		require.NoError(t, err)
		rows, _, err := executor.Collect(r, prog, executor.CollectOptions{})
		require.NoError(t, err)
		return len(rows)
	}

	assert.Equal(t, 1, runQ(`{ status = error }`), "status=error should match 1 span")
	assert.Equal(t, 1, runQ(`{ kind = client }`), "kind=client should match 1 span")
	assert.Equal(t, 1, runQ(`{ kind = server }`), "kind=server should match 1 span")
	assert.Equal(t, 1, runQ(`{ status = error && kind = client }`), "status=error AND kind=client should match 1 span")
	assert.Equal(t, 0, runQ(`{ status = error && kind = server }`), "status=error AND kind=server should match 0 spans")
}
