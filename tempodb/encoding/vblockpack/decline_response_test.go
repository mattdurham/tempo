package vblockpack

// decline_response_test.go — F-10 (issue #481 part 3, team-lead ruling R5): table-driven
// coverage of DeclineErrorToHTTPResponse over every recognized decline sentinel from both
// repos, plus the two required regression guards (shape-not-answerable → 4xx actionable;
// index/data inconsistency or unrecognized → 5xx via matched=false).

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"testing"

	"github.com/grafana/blockpack"
	"github.com/grafana/tempo/tempodb/encoding/common"
	"github.com/stretchr/testify/require"
)

// TestDeclineErrorToHTTPResponse_ShapeNotAnswerable_Returns4xxWithActionableMessage is the
// REQUIRED table-driven test over every decline sentinel from both repos: each must map to a
// 4xx status with a non-empty, reason-naming message.
func TestDeclineErrorToHTTPResponse_ShapeNotAnswerable_Returns4xxWithActionableMessage(t *testing.T) {
	cases := []struct {
		name string
		err  error
	}{
		{"MetricsShapeNotAnswerable", blockpack.ErrMetricsShapeNotAnswerable},
		{"MetricsNoCoverage", blockpack.ErrMetricsNoCoverage},
		{"MetricsLegacyTimeSecZero", blockpack.ErrMetricsLegacyTimeSecZero},
		{"MetricsValueIndexDisabled", blockpack.ErrMetricsValueIndexDisabled},
		{"CubeWarming", ErrCubeWarming},
		{"MaterializedIndexBuilding", ErrMaterializedIndexBuilding},
		{"SearchNoCoverage", ErrSearchNoCoverage},
		{"SliceIndexCoverageGap", ErrSliceIndexCoverageGap},
		{"StructuralIndexCoverageGap", blockpack.ErrStructuralIndexCoverageGap},
		{"TraceByIDIndexNotConfigured", blockpack.ErrTraceByIDIndexNotConfigured},
		{"TraceByIDCoverageGap", blockpack.ErrTraceByIDCoverageGap},
	}
	seenMessages := make(map[string]bool, len(cases))
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// Real-conversion-path: wrap with fmt.Errorf("...: %w", ...) exactly as the
			// production call sites do (backend_block.go), proving errors.Is-based matching
			// survives real error wrapping, not just a bare sentinel comparison.
			wrapped := fmt.Errorf("vblockpack QueryRange: %w", tc.err)
			status, message, matched := DeclineErrorToHTTPResponse(wrapped)
			require.True(t, matched, "%s must be recognized", tc.name)
			require.True(t, status >= 400 && status < 500, "%s: status = %d, want 4xx", tc.name, status)
			require.NotEmpty(t, message, "%s: message must be non-empty/actionable", tc.name)
			seenMessages[message] = true
		})
	}
	require.Len(t, seenMessages, len(cases), "every sentinel must produce a DISTINCT message — a shared/generic message would not be actionable")
}

// TestDeclineErrorToHTTPResponse_IndexDataInconsistency_Returns5xx is the REQUIRED companion:
// an unrecognized error (standing in for index/data inconsistency, which has no single typed
// sentinel — NOTE-VI-078 propagates the underlying object-store/decode error as-is) must NOT be
// matched — the caller's existing default-500 classification must apply, never a 4xx.
func TestDeclineErrorToHTTPResponse_IndexDataInconsistency_Returns5xx(t *testing.T) {
	cases := []struct {
		name string
		err  error
	}{
		{"NilError", nil},
		{"GenericIndexDataInconsistency", errors.New("index named a block/page the data file cannot resolve")},
		{"WrappedGenericError", fmt.Errorf("vblockpack Fetch: %w", errors.New("s3: object decode failed"))},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			status, message, matched := DeclineErrorToHTTPResponse(tc.err)
			require.False(t, matched, "%s must NOT be recognized as a decline sentinel", tc.name)
			require.Equal(t, 0, status)
			require.Empty(t, message)
		})
	}
}

// TestDeclineErrorToHTTPResponse_UsesStatusUnprocessableEntity pins the specific status code
// chosen (422, matching the existing ErrTraceTooLarge convention in modules/querier/http.go's
// handleError) rather than an arbitrary 4xx — a regression guard against silently drifting to a
// different 4xx code that could confuse client-side error handling expecting 422 consistently.
func TestDeclineErrorToHTTPResponse_UsesStatusUnprocessableEntity(t *testing.T) {
	status, _, matched := DeclineErrorToHTTPResponse(blockpack.ErrMetricsShapeNotAnswerable)
	require.True(t, matched)
	require.Equal(t, http.StatusUnprocessableEntity, status)
}

// TestFindTraceByID_VIDisabled_SentinelPropagatesToHTTPResponse is the Phase 0 required
// end-to-end regression guard: a REAL blockpackBlock.FindTraceByID call (via the actual write
// path, no hand-built fixtures) with no value-index reader configured at all (vr == nil) must
// surface blockpack.ErrTraceByIDIndexNotConfigured, wrapped exactly as the production call site
// wraps it (backend_block.go's "GetTraceByID: %w"), and that wrapped error must still be
// recognized by DeclineErrorToHTTPResponse end to end -- proving the two new sentinels this
// phase added to blockpack's public API actually propagate through tempo's decline mapper, not
// just that the mapper's switch statement compiles.
func TestFindTraceByID_VIDisabled_SentinelPropagatesToHTTPResponse(t *testing.T) {
	withVIQueryReader(t, nil, "") // vr == nil: index-driven path disabled entirely

	block, meta := createFetchTestBlock(t)
	traceIDA := []byte{1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1}
	require.NotNil(t, meta)

	_, err := block.FindTraceByID(context.Background(), traceIDA, common.SearchOptions{})
	require.Error(t, err, "vr == nil must hard-error, there is no scan fallback (NOTE-VI-073)")
	require.True(t, errors.Is(err, blockpack.ErrTraceByIDIndexNotConfigured),
		"err = %v, want ErrTraceByIDIndexNotConfigured", err)

	status, message, matched := DeclineErrorToHTTPResponse(err)
	require.True(t, matched, "the wrapped sentinel must still be recognized end to end")
	require.Equal(t, http.StatusUnprocessableEntity, status)
	require.NotEmpty(t, message)
}
