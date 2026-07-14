package combiner

// partial_message.go — issue #217 task 1.3: shared accumulator for propagating an incoming
// shard/job's PartialStatus=PARTIAL (Phase 1's per-slice coverage-gap tolerance,
// tempodb/encoding/vblockpack/slice_partial_response.go) into the combiner's own final Status
// instead of discarding it. Deliberately NOT part of genericCombiner itself (see #217's plan
// doc "Simplification findings": shouldQuit()/AddResponse/erroredResponse() need zero changes —
// a coverage-gap decline is, from the combiner's point of view, an ordinary 200) — this is a
// small helper the per-response-type combiners (metrics_query_range.go, search.go) each own one
// instance of, mirroring how each already owns its own metricsCombiner/completionTracker.
import (
	"strings"

	"github.com/grafana/tempo/pkg/tempopb"
)

// maxPartialMessageLen caps the accumulated message's total length defensively — a
// pathologically wide query with many uncovered slices could otherwise accumulate an
// unboundedly long message string as more and more jobs report a (deduplicated, but still
// growing) reason.
const maxPartialMessageLen = 2048

// partialMessageAccumulator collects distinct partial-coverage messages across many incoming
// shard responses, deduplicating identical messages and capping the combined length. Not
// concurrency-safe on its own — callers (the combine closure) already run under genericCombiner's
// own mutex.
type partialMessageAccumulator struct {
	partial  bool
	seen     map[string]struct{}
	messages []string
	total    int
}

// add records that a shard reported PartialStatus_PARTIAL with the given message (message may
// be empty). Cheap no-op for an exact-duplicate message; silently stops accumulating once
// maxPartialMessageLen is reached (still marks partial=true).
func (a *partialMessageAccumulator) add(message string) {
	a.partial = true
	if message == "" {
		return
	}
	if a.seen == nil {
		a.seen = make(map[string]struct{})
	}
	if _, dup := a.seen[message]; dup {
		return
	}
	if a.total >= maxPartialMessageLen {
		return
	}
	a.seen[message] = struct{}{}
	a.messages = append(a.messages, message)
	a.total += len(message)
}

// message joins the accumulated distinct messages into a single string, truncated to
// maxPartialMessageLen.
func (a *partialMessageAccumulator) message() string {
	if len(a.messages) == 0 {
		return ""
	}
	joined := strings.Join(a.messages, "; ")
	if len(joined) > maxPartialMessageLen {
		joined = joined[:maxPartialMessageLen]
	}
	return joined
}

// applyPartialCoverageGap (#217 task 1.3) merges an accumulated per-slice coverage-gap partial
// signal into resp, which may ALREADY carry Status=PARTIAL/Message from an unrelated cause
// (e.g. metrics_query_range.go's max-series-truncation branch, applied before this call) — in
// that case both reasons are preserved by appending rather than overwriting. A no-op when
// nothing was ever accumulated.
func applyPartialCoverageGap(resp *tempopb.QueryRangeResponse, acc *partialMessageAccumulator) {
	if acc == nil || !acc.partial {
		return
	}
	resp.Status = tempopb.PartialStatus_PARTIAL
	msg := acc.message()
	if msg == "" {
		return
	}
	if resp.Message == "" {
		resp.Message = msg
	} else {
		resp.Message = resp.Message + "; " + msg
	}
}

// applyPartialCoverageGapSearch is applyPartialCoverageGap's *tempopb.SearchResponse sibling
// (#217 task 1.2/1.3, item A's new SearchResponse.Status/Message fields). Search has no
// analogous "unrelated cause" that could already have set Status/Message before this call, but
// the append-not-overwrite behavior is kept identical for consistency and future-proofing.
func applyPartialCoverageGapSearch(resp *tempopb.SearchResponse, acc *partialMessageAccumulator) {
	if acc == nil || !acc.partial {
		return
	}
	resp.Status = tempopb.PartialStatus_PARTIAL
	msg := acc.message()
	if msg == "" {
		return
	}
	if resp.Message == "" {
		resp.Message = msg
	} else {
		resp.Message = resp.Message + "; " + msg
	}
}
