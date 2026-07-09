package vblockpack

// decline_reason_test.go — issue #493 Task 2: promotes QueryRange's discarded decline detail
// (indexCovered, cubeWarming, and which of blockpack's 4 metrics decline sentinels fired) onto
// the vblockpack.backendBlock.QueryRange span. TestDeclineReason pins the pure mapping function
// against all 4 sentinels; the TestQueryRange_EmitsDeclineDetail_* cases drive the REAL
// (b *blockpackBlock).QueryRange (per R7) to prove the mapping actually reaches the span.

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	blockpack "github.com/grafana/blockpack"
	"github.com/grafana/tempo/pkg/tempopb"
	"github.com/grafana/tempo/tempodb/backend"
	"github.com/grafana/tempo/tempodb/backend/local"
	"github.com/grafana/tempo/tempodb/encoding/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestDeclineReason pins the pure string mapping for all 4 of blockpack's metrics decline
// sentinels (decline_errors.go), plus the "other" default for any unrecognized error — including
// ErrMetricsValueIndexDisabled, which the pre-existing errors.Is routing chain in QueryRange
// never needed to check (it doesn't remap to ErrSliceIndexCoverageGap/ErrCubeWarming) but which
// this attribute must not silently omit.
func TestDeclineReason(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want string
	}{
		{"value index disabled", blockpack.ErrMetricsValueIndexDisabled, "value_index_disabled"},
		{"shape not answerable", blockpack.ErrMetricsShapeNotAnswerable, "shape_not_answerable"},
		{"no coverage", blockpack.ErrMetricsNoCoverage, "no_coverage"},
		{"legacy time sec zero", blockpack.ErrMetricsLegacyTimeSecZero, "legacy_time_sec_zero"},
		{"wrapped sentinel", errors.New("wrap: " + blockpack.ErrMetricsNoCoverage.Error()), "other"},
		{"unrecognized", errors.New("boom"), "other"},
		{"nil", nil, "other"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, declineReason(tc.err))
		})
	}
	// errors.Is-based wrapping (fmt.Errorf with %w), unlike the plain string-prefixed case
	// above, must still resolve to the same reason.
	wrapped := errWrap(blockpack.ErrMetricsShapeNotAnswerable)
	assert.Equal(t, "shape_not_answerable", declineReason(wrapped))
}

// errWrap wraps err with %w so errors.Is still matches the sentinel underneath.
func errWrap(err error) error {
	return &wrappedErr{err}
}

type wrappedErr struct{ err error }

func (w *wrappedErr) Error() string { return "blockpack QueryRange: " + w.err.Error() }
func (w *wrappedErr) Unwrap() error { return w.err }

// countOverTimeReq builds a simple, VI-answerable (no group-by) count_over_time() request over
// a query window wide enough to cover writeSvcBlock's now()-timestamped fixture spans.
func countOverTimeReq(query string) *tempopb.QueryRangeRequest {
	return &tempopb.QueryRangeRequest{
		Query: query,
		Start: uint64(time.Now().Add(-10 * time.Minute).UnixNano()),
		End:   uint64(time.Now().UnixNano()),
		Step:  uint64(time.Minute.Nanoseconds()),
	}
}

// TestQueryRange_EmitsDeclineDetail_ValueIndexDisabled drives the REAL QueryRange with no
// value-index reader configured (vr == nil) — the config-level, zeroth decline category the
// pre-existing 3-sentinel errors.Is routing chain never checked. Asserts vi.covered=false and
// metrics.decline_reason=="value_index_disabled" on the real, real-write-path-produced span.
func TestQueryRange_EmitsDeclineDetail_ValueIndexDisabled(t *testing.T) {
	rec, tp := recordedSpans(t)
	defer func() { _ = tp.Shutdown(context.Background()) }()

	withVIQueryReader(t, nil, "") // index path disabled entirely -> not covered

	dir := t.TempDir()
	metaA, _ := writeSvcBlock(t, dir, &fakeVISink{}, "test-tenant", uuid.New(), "svc-alpha", 3)
	rawR, _, _, err := local.New(&local.Config{Path: dir})
	require.NoError(t, err)
	block := newBackendBlock(metaA, backend.NewReader(rawR))

	req := countOverTimeReq("{} | count_over_time()")
	_, err = block.QueryRange(context.Background(), req, common.SearchOptions{IndexOnly: false})
	require.Error(t, err)
	require.True(t, errors.Is(err, blockpack.ErrMetricsValueIndexDisabled))

	require.NoError(t, tp.ForceFlush(context.Background()))
	span, ok := spanByName(rec.Ended(), "vblockpack.backendBlock.QueryRange")
	require.True(t, ok, "QueryRange must emit its span")

	covered, ok := attrValue(span, "vi.covered")
	require.True(t, ok, "span must carry vi.covered")
	assert.Equal(t, "false", covered)

	reason, ok := attrValue(span, "metrics.decline_reason")
	require.True(t, ok, "span must carry metrics.decline_reason")
	assert.Equal(t, "value_index_disabled", reason)
}

// TestQueryRange_EmitsDeclineDetail_ShapeNotAnswerable drives the REAL QueryRange with a real,
// covered value index (writeSvcBlock + fakeVISink) but a group-by aggregate shape the VI engine
// cannot answer — mirrors TestFetch_UnsupportedMetricsShape_ProductionDefault_TypedError_NoScan's
// fixture. Asserts vi.covered=true (the leaf itself IS covered) and
// metrics.decline_reason=="shape_not_answerable".
func TestQueryRange_EmitsDeclineDetail_ShapeNotAnswerable(t *testing.T) {
	rec, tp := recordedSpans(t)
	defer func() { _ = tp.Shutdown(context.Background()) }()

	dir := t.TempDir()
	viStore := &fakeVISink{}
	withVISink(t, viStore, "indexes")
	withVIQueryReader(t, viStore, "indexes")

	metaA, _ := writeSvcBlock(t, dir, viStore, "test-tenant", uuid.New(), "svc-alpha", 300)
	rawR, _, _, err := local.New(&local.Config{Path: dir})
	require.NoError(t, err)
	block := newBackendBlock(metaA, backend.NewReader(rawR))

	req := countOverTimeReq(`{ resource.service.name = "svc-alpha" } | count_over_time() by (resource.service.name)`)
	_, err = block.QueryRange(context.Background(), req, common.SearchOptions{IndexOnly: false})
	require.Error(t, err)
	require.True(t, errors.Is(err, blockpack.ErrMetricsShapeNotAnswerable))

	require.NoError(t, tp.ForceFlush(context.Background()))
	span, ok := spanByName(rec.Ended(), "vblockpack.backendBlock.QueryRange")
	require.True(t, ok, "QueryRange must emit its span")

	covered, ok := attrValue(span, "vi.covered")
	require.True(t, ok, "span must carry vi.covered")
	assert.Equal(t, "true", covered)

	reason, ok := attrValue(span, "metrics.decline_reason")
	require.True(t, ok, "span must carry metrics.decline_reason")
	assert.Equal(t, "shape_not_answerable", reason)
}

// TestQueryRange_EmitsDeclineDetail_SuccessPath drives the REAL QueryRange to a genuine success
// (real value-index coverage, VI-answerable shape — no group-by) and asserts vi.covered is still
// attached on the success path (it costs nothing extra and is informative either way), while
// metrics.decline_reason is absent (there is no decline to explain).
func TestQueryRange_EmitsDeclineDetail_SuccessPath(t *testing.T) {
	rec, tp := recordedSpans(t)
	defer func() { _ = tp.Shutdown(context.Background()) }()

	dir := t.TempDir()
	viStore := &fakeVISink{}
	withVISink(t, viStore, "indexes")
	withVIQueryReader(t, viStore, "indexes")

	metaA, _ := writeSvcBlock(t, dir, viStore, "test-tenant", uuid.New(), "svc-alpha", 300)
	rawR, _, _, err := local.New(&local.Config{Path: dir})
	require.NoError(t, err)
	block := newBackendBlock(metaA, backend.NewReader(rawR))

	req := countOverTimeReq(`{ resource.service.name = "svc-alpha" } | count_over_time()`)
	resp, err := block.QueryRange(context.Background(), req, common.SearchOptions{IndexOnly: false})
	require.NoError(t, err, "a VI-answerable, fully-covered count_over_time() query must succeed")
	require.NotNil(t, resp)

	require.NoError(t, tp.ForceFlush(context.Background()))
	span, ok := spanByName(rec.Ended(), "vblockpack.backendBlock.QueryRange")
	require.True(t, ok, "QueryRange must emit its span")

	covered, ok := attrValue(span, "vi.covered")
	require.True(t, ok, "span must carry vi.covered even on success")
	assert.Equal(t, "true", covered)

	_, ok = attrValue(span, "metrics.decline_reason")
	assert.False(t, ok, "metrics.decline_reason must be absent on a success response")
}
