package vblockpack

import (
	"context"
	"testing"

	"github.com/grafana/tempo/pkg/tempopb"
	"github.com/grafana/tempo/tempodb/encoding/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

// recordedSpans installs an in-process span recorder as the global tracer provider
// and returns a function that flushes and returns the spans recorded so far. The
// global provider is sticky (it cannot be cleanly restored across tests — see the
// otel SetTracerProvider caveat), so we install once and read the recorder each
// time. Tests that assert on spans should filter by the operation name they expect.
func recordedSpans(t *testing.T) (*tracetest.SpanRecorder, *sdktrace.TracerProvider) {
	t.Helper()
	rec := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(rec))
	otel.SetTracerProvider(tp)
	// version.go's package-level tracer was captured before this call; refresh it so
	// the spans flow into our recorder for the duration of the test.
	tracer = tp.Tracer("tempodb/encoding/vblockpack")
	return rec, tp
}

// spanByName returns the first recorded span with the given operation name.
func spanByName(spans []sdktrace.ReadOnlySpan, name string) (sdktrace.ReadOnlySpan, bool) {
	for _, s := range spans {
		if s.Name() == name {
			return s, true
		}
	}
	return nil, false
}

// attrValue returns the string form of a span attribute by key, if present.
func attrValue(s sdktrace.ReadOnlySpan, key string) (string, bool) {
	for _, kv := range s.Attributes() {
		if string(kv.Key) == key {
			return kv.Value.Emit(), true
		}
	}
	return "", false
}

// TestFetch_EmitsBlockSpan verifies Fetch opens a span carrying the block-
// identifying attributes (blockID, tenantID, blockSize, compactionLevel) so the
// query is visible and comparable in Grafana traces (issue #465).
func TestFetch_EmitsBlockSpan(t *testing.T) {
	rec, tp := recordedSpans(t)
	defer func() { _ = tp.Shutdown(context.Background()) }()

	block, meta := createFetchTestBlock(t)
	_ = collectFetch(t, block, nil, false)

	require.NoError(t, tp.ForceFlush(context.Background()))
	span, ok := spanByName(rec.Ended(), "vblockpack.backendBlock.Fetch")
	require.True(t, ok, "Fetch must emit a vblockpack.backendBlock.Fetch span")

	gotBlockID, ok := attrValue(span, "blockID")
	require.True(t, ok, "span must carry blockID")
	assert.Equal(t, meta.BlockID.String(), gotBlockID)

	gotTenant, ok := attrValue(span, "tenantID")
	require.True(t, ok, "span must carry tenantID")
	assert.Equal(t, meta.TenantID, gotTenant)

	_, ok = attrValue(span, "blockSize")
	assert.True(t, ok, "span must carry blockSize")
	_, ok = attrValue(span, "compactionLevel")
	assert.True(t, ok, "span must carry compactionLevel")
}

// TestSearch_EmitsBlockSpan verifies Search also opens a block span.
func TestSearch_EmitsBlockSpan(t *testing.T) {
	rec, tp := recordedSpans(t)
	defer func() { _ = tp.Shutdown(context.Background()) }()

	block, _ := createFetchTestBlock(t)
	_, err := block.Search(context.Background(), &tempopb.SearchRequest{Limit: 10}, common.SearchOptions{})
	require.NoError(t, err)

	require.NoError(t, tp.ForceFlush(context.Background()))
	_, ok := spanByName(rec.Ended(), "vblockpack.backendBlock.Search")
	assert.True(t, ok, "Search must emit a vblockpack.backendBlock.Search span")
}

// TestFetch_FullScanStatsOnSpan verifies the full-scan path promotes its
// per-step plan/scan I/O onto the span (issue #465). The test block has no
// value index configured, so Fetch always takes the full-scan path and produces
// QueryStats steps.
//
// F-9 (issue #481 parts 2/3, team-lead ruling R8) doc-comment clarification: createFetchTestBlock
// (fetch_test.go) never calls withVIQueryReader, so getValueIndexQueryReader() returns nil here —
// this test exercises the zeroth, config-level VI-DISABLED category (vr == nil), which R8 pins as
// UNCHANGED by Phase F's decline-routing rewrite (F-8): declineOutcome's vr==nil branch is
// deliberately exempt from the boundedAuthorized/ErrSearchNoCoverage backstop declineOutcomeBounded
// implements for every OTHER routine decline. No code or assertion changes were needed here.
func TestFetch_FullScanStatsOnSpan(t *testing.T) {
	rec, tp := recordedSpans(t)
	defer func() { _ = tp.Shutdown(context.Background()) }()

	block, _ := createFetchTestBlock(t)
	_ = collectFetch(t, block, nil, false)

	require.NoError(t, tp.ForceFlush(context.Background()))
	span, ok := spanByName(rec.Ended(), "vblockpack.backendBlock.Fetch")
	require.True(t, ok)

	_, ok = attrValue(span, "scan.execution_path")
	assert.True(t, ok, "full-scan path must record scan.execution_path on the span")
}
