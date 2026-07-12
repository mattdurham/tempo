package executor

// metrics_trace_bounded.go — Phase 4 (plan-scan-fallback.md, "anchor+confirm") multi-leaf
// AND early-stopping.
//
// The landmine: independently early-stopping each AND leaf to its own newest-N and then
// intersecting (viIntersectSorted) can silently under-report -- a true intersection match
// can lie outside one leaf's own truncated top-N when that leaf is materially less
// selective than the intersection as a whole. viIntersectNewestFirstAnchored instead
// resolves ONE leaf (the anchor, chosen by the caller -- vibuilder's BuildSourceBounded --
// for being cheap to over-fetch) newest-first and CONFIRMS each candidate against the
// remaining leaf(s) via confirmFn, widening the anchor batch (the caller's job, not this
// function's) whenever too few confirmed matches come back. This function itself is a pure
// walk-and-confirm primitive with zero I/O of its own.
//
// viIntersectOrdered is the OTHER half: a viEvalNodes-time (query-eval, not resolution-time)
// order-preserving multi-set intersection, mirroring ViUnionNewestFirst's existing purpose
// for OR (Phase 3) -- viIntersectSorted's key-sorted merge-join is correct in CONTENT for
// any already-independently-correct per-leaf inputs, but destroys whatever newest-first
// order BuildSourceBounded established. viIntersectOrdered preserves the driver set's own
// order instead, so a flat multi-leaf AND (isOR=false, every sibling itself a leaf) built
// via the early-stopping path stays newest-first end to end.
import (
	"strings"
)

// ViSpanIdentityKey is the cross-package span-identity key VILookupResult results share
// (SourceRef ++ TraceID ++ BlockPage ++ RowIdx, matching viSpanCmp's own identity rule).
// Exported (like ViUnionNewestFirst) so vibuilder -- a different package -- can build its
// own confirm-membership sets using the SAME identity rule this package's AND/OR
// combination logic already relies on, rather than re-deriving a second, possibly
// divergent one.
func ViSpanIdentityKey(s VILookupResult) string {
	k := viSpanKey(s)
	var sb strings.Builder
	sb.Grow(len(s.SourceRef) + len(k))
	sb.WriteString(s.SourceRef)
	sb.Write(k[:])
	return sb.String()
}

// ViMembershipSet builds a set of ViSpanIdentityKey values from set, for O(1) confirm-time
// membership checks -- the mechanism vibuilder's anchor+confirm design uses instead of a
// fresh per-candidate network round-trip against each remaining leaf (see this file's
// package doc comment for the full rationale: the "remaining" leaves are resolved FULLY,
// once, by the caller, so a targeted per-candidate lookup would cost more round-trips than
// it saves here).
func ViMembershipSet(set []VILookupResult) map[string]struct{} {
	m := make(map[string]struct{}, len(set))
	for _, s := range set {
		m[ViSpanIdentityKey(s)] = struct{}{}
	}
	return m
}

// ViConfirmAllSets returns a confirmFn (viIntersectNewestFirstAnchored's parameter shape)
// that reports true only when a candidate's identity key is present in EVERY set in sets --
// the AND-across-remaining-leaves membership check vibuilder's anchor+confirm design needs.
// An empty sets (a query with only one non-anchor leaf... i.e. exactly 2 leaves total) still
// works correctly: the loop body simply never executes, so every candidate confirms true,
// which is correct since there is nothing left to confirm against beyond the anchor itself.
func ViConfirmAllSets(sets []map[string]struct{}) func(VILookupResult) (bool, error) {
	return func(cand VILookupResult) (bool, error) {
		key := ViSpanIdentityKey(cand)
		for _, s := range sets {
			if _, ok := s[key]; !ok {
				return false, nil
			}
		}
		return true, nil
	}
}

// ViIntersectNewestFirstAnchored walks anchorResults (already newest-first ordered and
// already over-fetched by the caller) and confirms each candidate via confirmFn, collecting
// up to limit confirmed matches. Stops calling confirmFn entirely once limit is reached
// (the whole point of early-stopping: no wasted confirm work past the point of no further
// benefit). needMore reports whether anchorResults was exhausted before limit confirmed
// matches were found -- the caller's signal to pull a wider anchor batch and retry, per the
// plan's own step 4 (this function has no knowledge of, or ability to, pull more data
// itself -- that requires disc/store I/O this function deliberately does not have).
func ViIntersectNewestFirstAnchored(
	anchorResults []VILookupResult, limit int, confirmFn func(VILookupResult) (bool, error),
) (confirmed []VILookupResult, needMore bool, err error) {
	if limit <= 0 {
		return nil, false, nil
	}
	seen := make(map[string]struct{}, min(limit, len(anchorResults)))
	confirmed = make([]VILookupResult, 0, min(limit, len(anchorResults)))
	for _, cand := range anchorResults {
		if len(confirmed) >= limit {
			return confirmed, false, nil
		}
		key := ViSpanIdentityKey(cand)
		if _, dup := seen[key]; dup {
			continue
		}
		ok, cerr := confirmFn(cand)
		if cerr != nil {
			return nil, false, cerr
		}
		if ok {
			seen[key] = struct{}{}
			confirmed = append(confirmed, cand)
		}
	}
	return confirmed, len(confirmed) < limit, nil
}

// viIntersectOrdered intersects N already-resolved, independently-correct per-leaf sets,
// preserving sets[0]'s own order -- unlike viIntersectSorted's key-sorted merge-join, which
// discards whatever order the inputs arrived in. Correctness does not depend on any
// particular leaf being the anchor or on how the sets were produced (BuildSourceBounded's
// early-stopping path or the ordinary unbounded BuildSource path both produce sets that are
// already independently correct for their own leaf) -- this function only ever narrows
// sets[0], it never adds anything, so it cannot manufacture a wrong answer regardless of
// input order or content. Mirrors ViUnionNewestFirst's identical "order-preserving
// combination over already-resolved sets" role for OR.
func viIntersectOrdered(sets [][]VILookupResult) []VILookupResult {
	if len(sets) == 0 {
		return nil
	}
	if len(sets) == 1 {
		return viDedupOrdered(sets[0])
	}
	memberSets := make([]map[string]struct{}, len(sets)-1)
	for i := 1; i < len(sets); i++ {
		memberSets[i-1] = ViMembershipSet(sets[i])
	}
	confirmFn := ViConfirmAllSets(memberSets)
	seen := make(map[string]struct{}, len(sets[0]))
	out := make([]VILookupResult, 0, len(sets[0]))
	for _, cand := range sets[0] {
		key := ViSpanIdentityKey(cand)
		if _, dup := seen[key]; dup {
			continue
		}
		ok, _ := confirmFn(cand) // ViConfirmAllSets never returns a non-nil error
		if ok {
			seen[key] = struct{}{}
			out = append(out, cand)
		}
	}
	return out
}

// viDedupOrdered removes consecutive-or-not duplicate identity keys from set while
// preserving its own order -- the order-preserving analog of viSortDedup (which key-sorts
// first, discarding order). Used by viIntersectOrdered's single-node case, and by
// ViIntersectNewestFirstAnchored's caller-facing contract implicitly (anchorResults is
// assumed to already be per-key-unique within one leaf's own newest-first resolution,
// matching lookupColumnNewestFirst's existing per-leaf dedup-free contract today -- exposed
// here in case a future caller needs it explicitly).
func viDedupOrdered(set []VILookupResult) []VILookupResult {
	if len(set) < 2 {
		return set
	}
	seen := make(map[string]struct{}, len(set))
	out := make([]VILookupResult, 0, len(set))
	for _, s := range set {
		key := ViSpanIdentityKey(s)
		if _, dup := seen[key]; dup {
			continue
		}
		seen[key] = struct{}{}
		out = append(out, s)
	}
	return out
}
