package vblockpack

// slice_partial_response.go — issue #217, Phase 1 (tasks 1.1/1.2): turns an indexOnly=true slice
// job's coverage-gap decline into a normal, valid, EMPTY response instead of an HTTP error.
//
// Per the plan's own "Simplification findings": the combiner (modules/frontend/combiner/
// common.go) needs ZERO changes for this — shouldQuit()/AddResponse/erroredResponse() are
// generic machinery reused by every combiner, and a query-shape-specific "tolerable 4xx" concept
// would be new, real complexity spread across code with nothing to do with #217. The actual fix
// belongs one layer down, at the point a slice job's response is constructed: for QueryRange
// (metrics), that is this file's queryRangePartialCoverageGapResponse, called directly from
// backend_block.go's QueryRange in place of returning ErrSliceIndexCoverageGap. For Fetch
// (search/structural), the fix is subtler: Fetch's return type (traceql.FetchSpansResponse) is a
// shared, backend-agnostic executor type with no PartialStatus/Message fields of its own (adding
// them would mean touching pkg/traceql, used by every storage backend, not just vblockpack) — so
// Fetch instead returns an ordinary EMPTY FetchSpansResponse (mirroring the existing
// "no blocks overlap the window" empty-result pattern already used elsewhere in this file) and
// separately records the partial-coverage signal on a context-carried side channel
// (sliceCoveragePartialSignal below) that SearchBlock (querier.go) reads back AFTER
// engine.ExecuteSearch returns, to set the wire-level tempopb.SearchResponse.Status/Message
// itself. This keeps pkg/traceql (shared across backends) completely untouched.
import (
	"context"
	"sync"

	"github.com/grafana/tempo/pkg/tempopb"
)

// queryRangePartialCoverageGapResponse builds the normal, empty, PARTIAL QueryRangeResponse an
// indexOnly=true slice job returns in place of a hard error when its own coverage-gap decline
// fires (#217 task 1.1). From the combiner's point of view this is an ordinary 200 — Phase 1's
// entire point is that no combiner code needs to know this ever happened.
func queryRangePartialCoverageGapResponse(message string) *tempopb.QueryRangeResponse {
	return &tempopb.QueryRangeResponse{
		Status:  tempopb.PartialStatus_PARTIAL,
		Message: message,
	}
}

// SliceCoveragePartialSignal is a mutable, context-carried side channel (issue #217 task 1.2)
// letting Fetch report "I tolerated a coverage-gap decline for this indexOnly slice job and
// returned an empty result instead of erroring" back to SearchBlock (modules/querier/
// querier.go), which is the only caller with access to the wire-level *tempopb.SearchResponse
// Fetch's own return type cannot carry this signal on directly (see this file's package doc
// comment for why). Exported (and its constructor/accessor below) solely because SearchBlock
// lives in a different package (modules/querier) — this is cross-package wiring plumbing, not
// new caller-facing API surface. Safe for the (uncommon but possible) case of multiple
// Fetch/tryIndexFetch calls sharing one ctx: the FIRST tolerated decline's message wins,
// matching Phase 1.3's combiner-side "dedupe identical messages, don't blow up unboundedly"
// intent applied at the single-block level.
type SliceCoveragePartialSignal struct {
	mu      sync.Mutex
	partial bool
	message string
}

type sliceCoveragePartialSignalKey struct{}

// WithSliceCoveragePartialSignal installs a fresh signal on ctx and returns both the derived
// context and the signal itself, so the installer (SearchBlock) can read it back after the
// downstream Fetch call(s) complete.
func WithSliceCoveragePartialSignal(ctx context.Context) (context.Context, *SliceCoveragePartialSignal) {
	sig := &SliceCoveragePartialSignal{}
	return context.WithValue(ctx, sliceCoveragePartialSignalKey{}, sig), sig
}

// markSliceCoveragePartial records message on ctx's signal, if one was installed. A no-op
// (never panics, never errors) when no signal is present — callers that never went through
// WithSliceCoveragePartialSignal (e.g. a non-slice-job Fetch, or a direct unit test) simply have
// nothing observe the mark, matching this mechanism's "purely additive side channel" design.
func markSliceCoveragePartial(ctx context.Context, message string) {
	sig, ok := ctx.Value(sliceCoveragePartialSignalKey{}).(*SliceCoveragePartialSignal)
	if !ok || sig == nil {
		return
	}
	sig.mu.Lock()
	defer sig.mu.Unlock()
	if !sig.partial {
		sig.partial = true
		sig.message = message
	}
}

// State reports whether markSliceCoveragePartial was ever called on this signal, and the first
// recorded message.
func (s *SliceCoveragePartialSignal) State() (bool, string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.partial, s.message
}
