package vibuilder

// builder_bounded_and.go — Phase 4 (plan-scan-fallback.md, "anchor+confirm") multi-leaf AND
// early-stopping resolution, called from BuildSourceBounded (builder.go) whenever a query's
// predicate tree is a pure AND of 2+ leaves (no OR anywhere -- see hasORNode).
//
// Design deviation from the plan's literal wording, documented rather than silently
// applied (both are correctness-preserving by the plan's own stated invariants):
//
//  1. Anchor selection: the plan's own text asks to reuse blockpack's VCNT-based
//     cost-estimation machinery (queryplan.ClassifyProgramVCNT/VCNTColumnTotalFunc) to pick
//     the most selective leaf. Verified by reading queryplan/vcnt_cost.go directly: that
//     machinery needs VCNT byte content as an input, and VCNT bytes are fetched EXCLUSIVELY
//     by tempo's frontend (modules/frontend/vcnt_fetch.go) at plan time -- they never reach
//     the querier's per-block BuildSourceBounded call at all, so there is no clean, existing
//     wiring to reuse it from this package's position (flagged as an open design question in
//     the plan itself; resolved here rather than guessed). Anchor selection instead uses a
//     cheap, zero-extra-I/O heuristic: the leaf whose FileDiscoverer reports the FEWEST
//     candidate files for the window (file count is already being computed as part of
//     discovery regardless, so this costs nothing extra). This is correctness-preserving
//     because the plan's OWN invariant already establishes anchor choice affects only
//     PERFORMANCE, never correctness (see viIntersectNewestFirstAnchored's mutation-test
//     requirement in the test plan: a wrong anchor choice must never produce a wrong answer,
//     only a slower one).
//  2. Confirm mechanism: the plan's sketch describes a "targeted, per-candidate membership
//     check" against the remaining leaf(s), e.g. a narrowed QueryBucketFileRanged-style
//     lookup per anchor candidate. This function instead resolves each remaining leaf FULLY
//     (once, via the existing unbounded lookupColumn -- exactly Phase 2's own stopgap cost
//     for those leaves, never MORE expensive) and confirms via an O(1) set-membership check
//     (modules_executor.ViMembershipSet/ViConfirmAllSets) instead of a fresh per-candidate
//     network round-trip per confirm. This trades "possibly more bytes fetched for the
//     remaining leaves" for "far fewer round-trips" (one full download per remaining leaf
//     instead of one targeted lookup per anchor candidate), and is never LESS correct: the
//     remaining leaves are never truncated, so confirming an anchor candidate against their
//     full resolution can never silently miss a true intersection member -- the exact
//     landmine this whole phase exists to close.
import (
	"context"
	"math"

	modules_executor "github.com/grafana/blockpack/internal/modules/executor"
	"github.com/grafana/blockpack/internal/modules/valueindex"
)

// anchorOverFetchFactor is the anchor leaf's initial over-fetch margin (requested limit =
// query limit * this factor) before the first confirm pass. A tuning constant, not a
// correctness-critical value per the plan's own framing: too small just means more widening
// rounds (step 4), never a wrong answer.
const anchorOverFetchFactor = 3

// selectAnchorLeaf picks the leaf with the fewest discovered files for timeRange (a cheap,
// zero-extra-I/O selectivity proxy -- see this file's package doc comment for why the
// plan's literal VCNT-cost-model ask is unreachable from here). Returns the index into work,
// or ok=false if no leaf could be evaluated at all (every leaf's discovery failed to even
// resolve a colTypeName -- extremely unlikely given work was already filtered by
// buildPredicate, but handled defensively rather than assumed impossible).
func selectAnchorLeaf(
	ctx context.Context, disc FileDiscovererNewestFirst, work []leafWork, timeRange *[2]uint64,
) (idx int, err error) {
	bestIdx := -1
	bestCount := -1
	for i, w := range work {
		colHash := valueindex.ColHash(w.col)
		colTypeName := valueindex.ColTypeName(w.colType)
		if colTypeName == "" {
			continue
		}
		keys, derr := disc.FilesForTimeRangeNewestFirst(ctx, colHash, colTypeName, timeRange[0], timeRange[1])
		if derr != nil {
			return -1, derr
		}
		if bestIdx == -1 || len(keys) < bestCount {
			bestIdx = i
			bestCount = len(keys)
		}
	}
	if bestIdx == -1 {
		return -1, nil
	}
	return bestIdx, nil
}

// buildSourceBoundedMultiLeafAND resolves a multi-leaf, pure-AND query (BuildSourceBounded's
// caller already confirmed no OR appears anywhere in the tree) via the anchor+confirm
// design: pick an anchor leaf, resolve it newest-first with an over-fetch margin, confirm
// each candidate against the other leaf(s)' full resolution, widening the anchor batch if
// under-confirmed, until either limit confirmed matches are found or the anchor is
// genuinely exhausted (in which case the confirmed count is simply whatever the true
// intersection actually contains -- never a silently-truncated wrong answer, per this
// phase's whole purpose).
func buildSourceBoundedMultiLeafAND(
	ctx context.Context,
	discNewestFirst FileDiscovererNewestFirst,
	disc FileDiscoverer,
	store FileStore,
	work []leafWork,
	timeRange *[2]uint64,
	minSec, maxSec uint64,
	watermarks map[string]ColumnWatermark,
	limit int,
	src *modules_executor.SliceValueIndexSource,
) (added bool, err error) {
	anchorIdx, err := selectAnchorLeaf(ctx, discNewestFirst, work, timeRange)
	if err != nil {
		return false, err
	}
	if anchorIdx == -1 {
		// No leaf could be evaluated at all (buildPredicate already filtered work down to
		// leaves with a valid colTypeName, so this is effectively unreachable) -- report no
		// coverage rather than guessing.
		return false, nil
	}
	anchor := work[anchorIdx]

	// Resolve every OTHER leaf FULLY (unbounded, exactly Phase 2's own stopgap cost for
	// them) -- see this file's package doc comment for why this must never be truncated.
	otherResults := make(map[int][]modules_executor.VILookupResult, len(work)-1)
	memberSets := make([]map[string]struct{}, 0, len(work)-1)
	var confirmFilesRead int
	var confirmBytesRead int64
	for i, w := range work {
		if i == anchorIdx {
			continue
		}
		results, filesRead, bytesRead, lerr := lookupColumn(ctx, disc, store, w.col, w.colType, w.pred, timeRange)
		if lerr != nil {
			return false, lerr
		}
		confirmFilesRead += filesRead
		confirmBytesRead += bytesRead
		otherResults[i] = results
		memberSets = append(memberSets, modules_executor.ViMembershipSet(results))
	}
	confirmFn := modules_executor.ViConfirmAllSets(memberSets)

	overFetch := limit * anchorOverFetchFactor
	if overFetch < limit {
		overFetch = limit // overflow guard; unreachable at any realistic limit value
	}
	var confirmed []modules_executor.VILookupResult
	var anchorFilesRead int
	var anchorBytesRead int64
	for {
		anchorResults, filesRead, bytesRead, lerr := lookupColumnNewestFirst(
			ctx, discNewestFirst, store, anchor.col, anchor.colType, anchor.pred, timeRange, overFetch,
		)
		if lerr != nil {
			return false, lerr
		}
		anchorFilesRead = filesRead
		anchorBytesRead = bytesRead
		var needMore bool
		confirmed, needMore, err = modules_executor.ViIntersectNewestFirstAnchored(anchorResults, limit, confirmFn)
		if err != nil {
			return false, err
		}
		if !needMore || len(anchorResults) < overFetch {
			// Either satisfied (needMore is false), or the anchor's own true candidate set
			// is smaller than what we asked for (len(anchorResults) < overFetch) -- widening
			// further cannot produce more, so stop rather than loop forever.
			break
		}
		overFetch *= 2
		if overFetch <= 0 || overFetch < limit {
			overFetch = math.MaxInt // overflow guard; unreachable at any realistic limit value
		}
	}

	src.RecordFileIO(anchorFilesRead+confirmFilesRead, anchorBytesRead+confirmBytesRead)

	// AddLeaf, not Add (issue #206): a pure multi-leaf AND on the SAME column (e.g.
	// `score >= 100 && score <= 150`) is exactly this function's own scope -- two leaves in
	// work sharing one column name, one of them the anchor and the rest "other" leaves. Add's
	// column-name-only keying would merge the anchor's confirmed subset and an other leaf's
	// full resolution into the SAME bucket when they share a column, so viEvalAND downstream
	// would intersect that merged set with itself instead of the anchor's confirmed answer
	// against the other leaf's own set. AddLeaf keys each leaf by w.idx/anchor.idx instead --
	// the leaf's DFS leaf-slot STRUCTURAL position (assigned by collectLeaves, re-derived
	// identically by viEvalNode at eval time for the same query text), not a *vm.RangeNode
	// pointer: a pointer-identity scheme was tried first and discarded, because the public
	// blockpack.QueryTraceQLFromIndex recompiles the query string independently rather than
	// reusing this package's *vm.Program, so the node pointers seen at build time and at
	// eval time are, in general, two different tree instances even for identical query text.
	// idx keeps leaves disambiguated regardless of column-name collisions and survives that
	// independent recompile.
	// Issue #536: keyed by (col, colType), not colName alone -- see ColumnWatermark's own
	// doc comment for why a colName-only key silently collides two same-name,
	// different-type columns.
	anchorWmKey := ColumnWatermarkKey(anchor.col, valueindex.ColTypeName(anchor.colType))
	if wm, ok := watermarks[anchorWmKey]; !ok || wm.CoversRange(minSec, maxSec) {
		src.AddLeaf(anchor.idx, anchor.col, anchor.colType, confirmed)
		added = true
	}
	for i, w := range work {
		if i == anchorIdx {
			continue
		}
		wmKey := ColumnWatermarkKey(w.col, valueindex.ColTypeName(w.colType))
		if wm, ok := watermarks[wmKey]; ok && !wm.CoversRange(minSec, maxSec) {
			continue
		}
		src.AddLeaf(w.idx, w.col, w.colType, otherResults[i])
		added = true
	}
	return added, nil
}
