package pipeline

// collector_http_test.go — issue #493 Task 5: TDD coverage for consumeAndCombineResponses'
// best-effort "ShouldQuit fired at job N" summary attribute (R2's own explicit fallback for the
// case where the sharder's own span has already ended by the time ShouldQuit fires -- see
// dispatch_events.go's advancementPoint doc comment, package frontend, for the full
// investigation). Drives the REAL consumeAndCombineResponses entry point (R7) with a small fake
// Combiner reused in spirit from combiner/search_test.go's own ShouldQuit fixtures (a fake here,
// not the real combiner.NewSearch, because asserting an EXACT job number against the real
// worker-pool consumer goroutines would be asserting a genuine data race in the production code
// itself -- see this test's own comment below for why an exact-equality assertion would be
// flaky-by-construction, not a test bug).

import (
	"context"
	"io"
	"net/http"
	"strings"
	"testing"

	"go.opentelemetry.io/otel"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.uber.org/atomic"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/grafana/tempo/modules/frontend/combiner"
)

// recordedSpansPipeline mirrors vblockpack's recordedSpans / frontend's recordedSpansFrontend
// exactly (package-level tracer var reassignment), per R7.
func recordedSpansPipeline(t *testing.T) *tracetest.SpanRecorder {
	t.Helper()
	rec := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(rec))
	otel.SetTracerProvider(tp)
	tracer = tp.Tracer("modules/frontend/pipeline")
	t.Cleanup(func() { _ = tp.Shutdown(context.Background()) })
	return rec
}

// fakeResponses feeds n canned 200-OK PipelineResponses then signals done.
type fakeResponses struct {
	n int
	i int
}

func (f *fakeResponses) Next(context.Context) (combiner.PipelineResponse, bool, error) {
	if f.i >= f.n {
		return nil, true, nil
	}
	f.i++
	resp := pipelineResponse{r: &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader("{}"))}}
	return resp, f.i == f.n, nil
}

// fakeQuitAfterNCombiner is a minimal combiner.Combiner whose ShouldQuit becomes permanently true
// once AddResponse has been called quitAfter times — deterministic and dependency-free, unlike
// driving the real combiner.NewSearch through this same call (which would require asserting an
// exact job number against a genuine, pre-existing data race between the main consumption loop
// and its own worker-pool goroutines — see this file's package doc comment).
type fakeQuitAfterNCombiner struct {
	quitAfter int
	added     atomic.Int64
}

func (f *fakeQuitAfterNCombiner) AddResponse(combiner.PipelineResponse) error {
	f.added.Inc()
	return nil
}

func (f *fakeQuitAfterNCombiner) StatusCode() int { return 200 }

func (f *fakeQuitAfterNCombiner) ShouldQuit() bool {
	return f.added.Load() >= int64(f.quitAfter)
}

func (f *fakeQuitAfterNCombiner) HTTPFinal() (*http.Response, error) {
	return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader("{}"))}, nil
}

var _ combiner.Combiner = (*fakeQuitAfterNCombiner)(nil)

// TestConsumeAndCombineResponses_AttachesShouldQuitSummaryAttribute drives the REAL
// consumeAndCombineResponses entry point (R7) with more responses than the combiner needs before
// quitting, asserting dispatch.should_quit_at_job is attached to whatever span is active on ctx,
// with a value in the sane range [quitAfter, totalResponses] — not an exact equality, since the
// precise job count at which ShouldQuit is observed to flip depends on a genuine, pre-existing
// race between this function's main loop and its own consumer goroutines (respChan is unbuffered
// and a send only guarantees the receiver goroutine has STARTED processing, not that
// AddResponse has RETURNED, before the main loop's very next ShouldQuit() check) — asserting an
// exact number here would be pinning a race, not a deterministic property of the code.
func TestConsumeAndCombineResponses_AttachesShouldQuitSummaryAttribute(t *testing.T) {
	rec := recordedSpansPipeline(t)

	const totalResponses = 10
	const quitAfter = 3

	ctx, span := tracer.Start(context.Background(), "test.caller")
	c := &fakeQuitAfterNCombiner{quitAfter: quitAfter}
	err := consumeAndCombineResponses(ctx, 1, &fakeResponses{n: totalResponses}, c, nil)
	span.End()
	require.NoError(t, err)

	got, ok := frontendSpanByNamePipeline(rec.Ended(), "test.caller")
	require.True(t, ok)

	var found bool
	for _, kv := range got.Attributes() {
		if string(kv.Key) == "dispatch.should_quit_at_job" {
			found = true
			v := kv.Value.AsInt64()
			assert.GreaterOrEqual(t, v, int64(quitAfter))
			assert.LessOrEqual(t, v, int64(totalResponses))
		}
	}
	assert.True(t, found, "dispatch.should_quit_at_job must be attached once ShouldQuit trips")
}

// TestConsumeAndCombineResponses_NoShouldQuitAttributeWhenNeverTriggered proves the attribute is
// absent when every response is consumed without ShouldQuit ever returning true (quitAfter set
// higher than totalResponses).
func TestConsumeAndCombineResponses_NoShouldQuitAttributeWhenNeverTriggered(t *testing.T) {
	rec := recordedSpansPipeline(t)

	const totalResponses = 5

	ctx, span := tracer.Start(context.Background(), "test.caller")
	c := &fakeQuitAfterNCombiner{quitAfter: totalResponses + 1}
	err := consumeAndCombineResponses(ctx, 1, &fakeResponses{n: totalResponses}, c, nil)
	span.End()
	require.NoError(t, err)

	got, ok := frontendSpanByNamePipeline(rec.Ended(), "test.caller")
	require.True(t, ok)
	for _, kv := range got.Attributes() {
		assert.NotEqual(t, "dispatch.should_quit_at_job", string(kv.Key))
	}
}

func frontendSpanByNamePipeline(spans []sdktrace.ReadOnlySpan, name string) (sdktrace.ReadOnlySpan, bool) {
	for _, s := range spans {
		if s.Name() == name {
			return s, true
		}
	}
	return nil, false
}
