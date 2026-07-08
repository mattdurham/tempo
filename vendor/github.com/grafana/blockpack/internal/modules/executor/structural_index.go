package executor

// structural_index.go — D4 (plan-d.md, issue #489): ExecuteStructuralFromIndex, the index-driven
// trace-level join engine for 2-node positive-operator structural queries (>>, >, ~, <<, <).
//
// Lead/confirm design (team-lead checkpoint ruling, 2026-07-07, provisional pending
// brainstormer-d's adversarial check): the plan's original "pick whichever side is cheaper as
// lead, confirm the non-lead side via D3B's verifyCandidateSpans" only composes correctly when
// the non-lead side is the operator's terminal/output side (R, per applyStructuralOp's hardwired
// node1-is-output convention, stream_structural.go) — confirming L would need to check candidate
// ANCESTOR/sibling spans, not a span's own row, which verifyCandidateSpans cannot do. Resolution:
// two DECOUPLED concepts instead of one "lead":
//   - Walk anchor: ALWAYS L. L's exact VI matches (leftSource) feed nodeMatch bit0 for every
//     candidate trace's structural walk; R is treated as provisionally match-all (bit1 always
//     set, mirroring compileStructuralPair's own nil-filter-matches-all convention) so the walk
//     finds every structurally-valid R-candidate, then D3B confirms R's REAL filter against
//     exactly those candidates (verifyCandidateSpans' actual contract — this is what D3B was
//     built for).
//   - Discovery seed: which side's VI TraceID set drives the INITIAL candidate-trace list (before
//     any TraceGroup lookups). This is a pure efficiency choice — a trace with no L match AND no R
//     match anywhere cannot be a true answer for any of these operators, so seeding from EITHER
//     side's exact-match TraceID set is a sound, non-lossy restriction. RULING LANDED (team-lead,
//     validator-d2 adversarial check confirmed all three edges — both-sides-must-match airtight
//     including ~'s self-exclusion case; the provisional-R-then-confirm walk is algebraically
//     identical regardless of which side seeded discovery, since bit1 is only ever read as a
//     candidate gate, never as a source of truth; either-side seeding is non-lossy): seed from
//     whichever side the caller-injected isSelective classifies as Selective while the OTHER side
//     is not — L remains the zero-extra-query default whenever cost is tied or unknown, since L is
//     already resolved as the walk anchor regardless. See ExecuteStructuralFromIndex's own
//     seed-selection block below.
//
// The intersection prefilter (issue step 4: when BOTH sides classify Selective, also resolve R
// and intersect TraceID sets before doing ANY TraceGroup lookups) is implemented below — it is
// independent of which side seeds discovery, and per the rulings log is complementary to, never a
// substitute for, D3B's confirmation (D3B always runs on the walk's R-candidates regardless).
//
// Selectivity classification injection (forced, not a design choice): ruling 2's "internal import"
// premise ("executor and root already both live inside internal/... or import
// internal/modules/queryplan directly today") turned out false for executor specifically, and
// worse than merely inaccurate — internal/modules/queryplan imports internal/modules/vibuilder
// (indexable.go, LeafIndexable) which itself imports internal/modules/executor (builder.go), so
// executor importing queryplan is a real, unavoidable Go import cycle (verified by attempting the
// build, not assumed). Applying the SAME injection pattern team-lead's own D3 ruling already
// established for exactly this class of problem (branch (b): "ONLY if it has root-inseparable
// dependencies, use injection: executor defines a Func type, root supplies the implementation"):
// isSelective is a caller-injected classifier — root's own D5 wrapper (which safely imports
// queryplan today, timeslice.go) builds it by closing over
// queryplan.ClassifyProgramVCNT(prog, vcntData, vcntDir, minTS, maxTS) == queryplan.Selective.
// Reported to team-lead as an applied-precedent fix, not a new open question.

import (
	"bytes"
	"context"
	"fmt"
	"sort"

	modules_reader "github.com/grafana/blockpack/internal/modules/blockio/reader"
	modules_shared "github.com/grafana/blockpack/internal/modules/blockio/shared"
	"github.com/grafana/blockpack/internal/modules/valueindex"
	"github.com/grafana/blockpack/internal/traceqlparser"
	"github.com/grafana/blockpack/internal/vm"
)

// StructuralSelectivityClassifier reports whether prog classifies as queryplan.Selective over the
// caller's already-decoded VCNT section and query window. Injected because executor cannot import
// internal/modules/queryplan (see this file's package doc comment for the import-cycle reason).
type StructuralSelectivityClassifier func(prog *vm.Program) bool

// structuralSpanAddr is the join key between a VILookupResult (an attribute value-index match)
// and a TraceGroup's SpanEntry (task #12, FIX-D4-CRITICAL). VILookupResult.SpanID is NOT a valid
// join key here — but not because of a per-column write-time routing split (attribute columns DO
// carry a real, non-zero SpanID through extraction and the AddEntryV4/V2 routing, verified by
// live instrumentation during #12's adjudication; there is no AddEntryV4-vs-V2-by-column-identity
// story). The zeroing is ARCHITECTURAL: `WriteValueIndexL0` always calls `FlushBucket`
// (`valueindex_l0write.go`), which always builds the BucketGroup on-disk format, and that
// format's `SpanRef` (`bucketfile.go:71-74`) has NO SpanID field at all by design — only
// `SpanIndexes []uint16` (row indexes) and `TraceID`, matching NOTE-VI-045/#429's own
// (SourceID/page, RowIdx)+TraceID addressing scheme. `assembleBucket` (`writer.go:313`) reads
// `rawEntry.spanID` but never copies it into `SpanRef` — there is nowhere to put it — and the
// query side (`matchGroupsInBlock`, `bucketquery.go:154-182`) constructs every `LookupResult`
// without ever setting `SpanID`, so it zero-fills for EVERY result, including a hypothetical
// direct query against the `span:id` sentinel column's own BucketGroup file. Joining on SpanID
// therefore NEVER matched a real attribute-column VI entry against a real SpanEntry, silently
// returning zero candidates (ok=true) for every positive structural operator against genuine
// production-written VI data, no matter how selective the query — caught by coder-d3's first
// genuine end-to-end test (real WriteValueIndexL0 -> search-VI -> QueryStructuralFromIndex round
// trip); every prior test in this phase hand-constructed its VILookupResult fixtures with an
// artificially-correct SpanID, masking the defect completely.
//
// The identity the write path actually guarantees for EVERY entry is (SourceRef, BlockPage,
// RowIdx) — NOTE-VI-045's own "a span is identified by (SourceRef, BlockPage, RowIdx) rather
// than SpanID" contract, already the documented convention for the BucketGroup write path.
// TraceGroup.Spans[] carries the same identity as SourceRef + BlockRef.PageNum + RowIdx, so this
// is the correct, write-path-guaranteed join key.
type structuralSpanAddr struct {
	sourceRef string
	blockPage uint32
	rowIdx    uint16
}

// ExecuteStructuralFromIndex answers a 2-node structural query using the value index for
// candidate-trace discovery and TraceGroup for whole-trace ancestor/sibling resolution — no
// full-block scan. Mirrors QueryTraceQLFromIndex's (results, ok, err) decline contract so tempo's
// Fetch dispatch (DT1) can try it before falling back to ExecuteStructural.
//
// ok=false, err=nil (routine decline, caller falls back to ExecuteStructural):
//   - q flattens to other than a 2-node chain (compileStructuralPair's own decline).
//   - op is a negated operator (!>>, !>, !~) — routed to D6's separate function instead.
//   - leftSource has no coverage for the window (viMatchSpans ok=false), AND indexOnly is false.
//   - the trace-by-id index has zero candidate files for the window, AND indexOnly is false.
//
// ok=false, err!=nil: index/data inconsistency (skew) surfaces the SAME way
// MaterializeTraceGroupMultiFile / GetTraceByID already do — never silently masked. Also returned
// (as ErrStructuralIndexCoverageGap) when indexOnly is true and either coverage gap above occurs,
// or when a candidate's assembled TraceGroup is Partial (ruling 5).
func ExecuteStructuralFromIndex(
	ctx context.Context,
	q *traceqlparser.StructuralQuery,
	leftSource, rightSource ValueIndexSource,
	isSelective StructuralSelectivityClassifier,
	traceGroupStore valueindex.LookupStore,
	tenant, indexPrefix string,
	readerFor StructuralReaderProvider,
	minTS, maxTS uint64,
	indexOnly bool,
	opts Options,
) (*StructuralResult, bool, error) {
	if ctx == nil {
		ctx = context.Background()
	}

	leftProg, rightProg, op, ok, err := compileStructuralPair(q)
	if err != nil {
		return nil, false, err
	}
	if !ok {
		// Not a 2-node chain — routine decline, caller falls back to ExecuteStructural.
		return nil, false, nil
	}
	if isNegationOp(op) {
		// Negated operators are D6's responsibility, not this function's.
		return nil, false, nil
	}
	if leftSource == nil {
		// SPEC-ROOT-001: leftSource is the walk anchor and REQUIRED -- guarded here at the point
		// of use (not only by the root wrapper, structural.go's QueryStructuralFromIndex), since
		// viMatchSpans calls a nil-interface method unconditionally on a match-all left leg
		// (NOTE-VI-087) with no nil-check of its own. Mirrors the root wrapper's own "no index
		// coverage supplied" decline convention.
		return nil, false, nil
	}

	// Walk anchor: L is ALWAYS resolved exactly via VI (see package doc comment above).
	leftResults, leftOK := viMatchSpans(leftSource, leftProg)
	if !leftOK {
		if indexOnly {
			return nil, false, ErrStructuralIndexCoverageGap
		}
		return nil, false, nil
	}
	leftSpansByTrace := groupVILookupResultsByTrace(leftResults)

	candidateTraceIDs := chooseCandidateTraceIDs(rightSource, rightProg, isSelective, leftProg, leftSpansByTrace)

	if len(candidateTraceIDs) == 0 {
		return &StructuralResult{}, true, nil
	}

	colHash := valueindex.ColHash(modules_shared.TraceIDColumnName)
	colTypeName := valueindex.ColTypeName(modules_shared.ColumnTypeUUID)
	keys, discoverErr := valueindex.DiscoverIndexFiles(ctx, traceGroupStore, tenant, indexPrefix, colHash, colTypeName, minTS, maxTS)
	if discoverErr != nil {
		return nil, false, fmt.Errorf("ExecuteStructuralFromIndex: discover index files: %w", discoverErr)
	}
	if len(keys) == 0 {
		if indexOnly {
			return nil, false, ErrStructuralIndexCoverageGap
		}
		return nil, false, nil
	}

	result := &StructuralResult{}
	for _, traceID := range candidateTraceIDs {
		if ctx.Err() != nil {
			return nil, false, ctx.Err()
		}
		done, err := evalOneStructuralCandidateTrace(
			ctx, traceID, keys, traceGroupStore, readerFor, minTS, maxTS,
			leftSpansByTrace[traceID], op, rightProg, opts, result,
		)
		if err != nil {
			return nil, false, err
		}
		if done {
			break
		}
	}
	return result, true, nil
}

// evalOneStructuralCandidateTrace resolves ONE candidate trace's TraceGroup, checks Partial
// (ruling 5), materializes its spans (D3), runs the structural walk (L exact + R
// provisional-all-match) and D3B confirmation, and appends surviving matches to result. Returns
// done=true once opts.Limit is reached (caller stops iterating further candidates). Split out of
// ExecuteStructuralFromIndex to keep that function's own cyclomatic complexity down.
func evalOneStructuralCandidateTrace(
	ctx context.Context,
	traceID [16]byte,
	keys []string,
	traceGroupStore valueindex.LookupStore,
	readerFor StructuralReaderProvider,
	minTS, maxTS uint64,
	leftMatchAddrs map[structuralSpanAddr]struct{},
	op traceqlparser.StructuralOp,
	rightProg *vm.Program,
	opts Options,
	result *StructuralResult,
) (bool, error) {
	group, found, findErr := FindTraceGroupInCandidates(ctx, traceGroupStore, keys, traceID, minTS, maxTS)
	if findErr != nil {
		return false, fmt.Errorf("ExecuteStructuralFromIndex: %w", findErr)
	}
	if !found {
		// Legitimate VI/TraceGroup skew at the trace level (the search VI named a trace the
		// trace-by-id index doesn't cover for this window) — not an error, mirrors
		// GetTraceByID's own miss semantics.
		return false, nil
	}

	assembled := valueindex.AssembleTrace(group)
	if assembled.Partial {
		// Ruling 5: a partial tree can produce false negatives here — never label a
		// narrower-than-true answer as authoritatively successful. Wrapped with the trace ID
		// (MEDIUM finding, go-presubmit.md) so an operator debugging a production coverage-gap
		// error can identify which trace triggered it -- %w preserves errors.Is compatibility
		// (DT2's contract), the sentinel identity is unchanged.
		return false, fmt.Errorf("trace %x: %w", traceID, ErrStructuralIndexCoverageGap)
	}

	resolvedSpans, resolveErr := MaterializeTraceGroupMultiFile(ctx, readerFor, group, traceID, 0)
	if resolveErr != nil {
		return false, fmt.Errorf("ExecuteStructuralFromIndex: %w", resolveErr)
	}
	if len(resolvedSpans) == 0 {
		return false, nil
	}

	recs := resolvedSpansToStructuralRecs(resolvedSpans, leftMatchAddrs)
	resolved := resolveStructuralParentIndices([][]structuralSpanRec{recs}, nil)
	if len(resolved) == 0 {
		return false, nil
	}
	recs = resolved[0]

	candidateIdx := applyStructuralOp(recs, op, nil)
	if len(candidateIdx) == 0 {
		return false, nil
	}
	candidates := make([]ResolvedSpan, 0, len(candidateIdx))
	for _, idx := range candidateIdx {
		candidates = append(candidates, resolvedSpans[idx])
	}

	confirmed, verifyErr := verifyCandidateSpans(ctx, rightProg, candidates)
	if verifyErr != nil {
		return false, fmt.Errorf("ExecuteStructuralFromIndex: %w", verifyErr)
	}

	blockByKey, blockErr := materializeConfirmedSpanBlocks(confirmed)
	if blockErr != nil {
		return false, fmt.Errorf("ExecuteStructuralFromIndex: %w", blockErr)
	}

	for _, sp := range confirmed {
		key := verifyBlockKey{reader: sp.Reader, sourceRef: sp.SourceRef, blockIdx: sp.BlockIdx}
		result.Matches = append(result.Matches, SpanMatch{
			Block:    blockByKey[key],
			TraceID:  traceID,
			SpanID:   append([]byte(nil), sp.Span.SpanID[:]...),
			BlockIdx: sp.BlockIdx,
			RowIdx:   int(sp.RowIdx),
		})
		if opts.Limit > 0 && len(result.Matches) >= opts.Limit {
			return true, nil
		}
	}
	return false, nil
}

// groupVILookupResultsByTrace groups a ValueIndexSource lookup's results by TraceID, keeping only
// the set of matching span addresses per trace (identity only — no field materialization needed
// for the structural walk). Keyed by structuralSpanAddr (SourceRef, BlockPage, RowIdx), NOT
// SpanID — see structuralSpanAddr's own doc comment (task #12, FIX-D4-CRITICAL) for why SpanID is
// not a valid join key for ordinary attribute-column VI entries.
func groupVILookupResultsByTrace(results []VILookupResult) map[[16]byte]map[structuralSpanAddr]struct{} {
	byTrace := make(map[[16]byte]map[structuralSpanAddr]struct{}, len(results))
	for _, r := range results {
		set, ok := byTrace[r.TraceID]
		if !ok {
			set = make(map[structuralSpanAddr]struct{})
			byTrace[r.TraceID] = set
		}
		set[structuralSpanAddr{sourceRef: r.SourceRef, blockPage: r.BlockPage, rowIdx: r.RowIdx}] = struct{}{}
	}
	return byTrace
}

// chooseDiscoverySeed returns the sorted candidate TraceID list from spansByTrace (either side's
// grouped VI matches — see chooseCandidateTraceIDs, which decides WHICH side to pass here).
func chooseDiscoverySeed(spansByTrace map[[16]byte]map[structuralSpanAddr]struct{}) [][16]byte {
	ids := make([][16]byte, 0, len(spansByTrace))
	for traceID := range spansByTrace {
		ids = append(ids, traceID)
	}
	sort.Slice(ids, func(i, j int) bool { return bytes.Compare(ids[i][:], ids[j][:]) < 0 })
	return ids
}

// chooseCandidateTraceIDs decides which side's exact-match TraceID set seeds discovery, then
// applies the step-4 intersection prefilter, resolving rightSource's VI matches AT MOST ONCE
// regardless of which of those two uses triggers it (single-source-of-truth: one memoized
// resolution, not two independently-computed calls that could drift or double the VI query cost).
//
// Seed choice (RULING, see package doc comment): seed from R instead of L only when R classifies
// Selective and L does NOT — L remains the default on a tie or when isSelective is nil/uncertain,
// since L is already resolved as the walk anchor regardless and costs nothing extra. Seeding from
// EITHER side's exact matches is equally sound (non-lossy): a trace with zero true matches on
// either side cannot produce a confirmed answer for any of these operators.
//
// The intersection prefilter (issue step 4) additionally narrows the seed by the OTHER side's
// exact matches whenever that other side is ALSO Selective — this branch is only reachable when L
// seeded (seeding from R already requires L to be non-Selective, so "both Selective" cannot hold
// there). It remains complementary to, never a substitute for, D3B's confirmation of R's real
// filter later.
func chooseCandidateTraceIDs(
	rightSource ValueIndexSource,
	rightProg *vm.Program,
	isSelective StructuralSelectivityClassifier,
	leftProg *vm.Program,
	leftSpansByTrace map[[16]byte]map[structuralSpanAddr]struct{},
) [][16]byte {
	leftSelective := isSelective != nil && isSelective(leftProg)
	rightSelective := isSelective != nil && rightSource != nil && isSelective(rightProg)

	var rightSpansByTrace map[[16]byte]map[structuralSpanAddr]struct{}
	rightAttempted := false
	resolveRight := func() map[[16]byte]map[structuralSpanAddr]struct{} {
		if !rightAttempted {
			rightAttempted = true
			if rightResults, rightOK := viMatchSpans(rightSource, rightProg); rightOK {
				rightSpansByTrace = groupVILookupResultsByTrace(rightResults)
			}
		}
		return rightSpansByTrace
	}

	var candidateTraceIDs [][16]byte
	if rightSelective && !leftSelective {
		if rs := resolveRight(); rs != nil {
			candidateTraceIDs = chooseDiscoverySeed(rs)
		}
	}
	if candidateTraceIDs == nil {
		candidateTraceIDs = chooseDiscoverySeed(leftSpansByTrace)
	}

	if leftSelective && rightSelective {
		if rs := resolveRight(); rs != nil {
			candidateTraceIDs = intersectTraceIDSets(candidateTraceIDs, rs)
		}
		// rs==nil: no coverage for R specifically — skip the prefilter silently (no double-VI-
		// query cost paid for nothing); D3B still confirms R later regardless.
	}
	return candidateTraceIDs
}

// intersectTraceIDSets keeps only the TraceIDs in ids that also appear in other — the step-4
// prefilter (both sides Selective). Order is preserved from ids.
func intersectTraceIDSets(ids [][16]byte, other map[[16]byte]map[structuralSpanAddr]struct{}) [][16]byte {
	out := ids[:0:0]
	for _, id := range ids {
		if _, ok := other[id]; ok {
			out = append(out, id)
		}
	}
	return out
}

// materializeConfirmedSpanBlocks fetches and parses (WantAll(), matching the scan path's
// ExecuteStructural and QueryTraceQLFromIndex's own field-materialization convention) each
// DISTINCT (reader, block) pair among confirmed's spans exactly once, so
// ExecuteStructuralFromIndex's final SpanMatch.Block is always a real, ready-to-use parsed Block
// — never nil — for D5's SpanFieldsProvider conversion. D3B's own verifyCandidateSpans
// deliberately fetches blocks restricted to the predicate's own columns (evaluateProgramAgainstBlock,
// structural_verify.go) for confirmation only; that parse is NOT reused here since a final match
// must expose every column, not just the ones the confirming predicate happened to need. Reuses
// groupResolvedSpansByBlock (D3B) for the (reader, block) grouping rather than re-deriving it.
//
// I/O invariant (HIGH finding, go-presubmit.md): blockIdxs are batched per reader into ONE
// ReadBlocks call before any parsing happens, mirroring ResolveTraceGroupSourceRef
// (structural_traceresolve.go) and QueryTraceQLFromIndex's own coalesced multi-block fetch — never
// one ReadBlocks([]int{idx}) call per distinct block, which would throw away
// modules_reader.Reader's adjacent-block coalescing (reader.go: "Adjacent block ranges are merged
// into as few I/O operations as possible").
func materializeConfirmedSpanBlocks(confirmed []ResolvedSpan) (map[verifyBlockKey]*modules_reader.Block, error) {
	_, order := groupResolvedSpansByBlock(confirmed)

	byReader := make(map[*modules_reader.Reader][]int)
	for _, key := range order {
		byReader[key.reader] = append(byReader[key.reader], key.blockIdx)
	}
	rawByReader := make(map[*modules_reader.Reader]map[int][]byte, len(byReader))
	for reader, idxs := range byReader {
		raw, err := reader.ReadBlocks(idxs)
		if err != nil {
			return nil, fmt.Errorf("materialize blocks: %w", err)
		}
		rawByReader[reader] = raw
	}

	blocks := make(map[verifyBlockKey]*modules_reader.Block, len(order))
	for _, key := range order {
		raw, ok := rawByReader[key.reader][key.blockIdx]
		if !ok {
			return nil, fmt.Errorf("index/data skew: block %d missing from read result", key.blockIdx)
		}
		bwb, parseErr := key.reader.ParseBlockFromBytes(raw, modules_reader.WantAll(), key.reader.BlockMeta(key.blockIdx))
		if parseErr != nil {
			return nil, fmt.Errorf("parse block %d: %w", key.blockIdx, parseErr)
		}
		blocks[key] = bwb.Block
	}
	return blocks, nil
}

// resolvedSpansToStructuralRecs converts D3's []ResolvedSpan (one trace's whole materialized
// tree) into stream_structural.go's structuralSpanRec shape, so the EXACT existing
// evalOpDescendantStruct/evalOpChildStruct/evalOpSiblingStruct/evalOpAncestorStruct/
// evalOpParentStruct semantics can be reused unchanged (parity-critical — D1's golden table
// verifies against these exact functions). nodeMatch bit0 (0x01) is set only for spans whose
// (SourceRef, BlockPage, RowIdx) address appears in leftMatchAddrs (L's REAL VI matches, keyed by
// structuralSpanAddr — task #12, FIX-D4-CRITICAL: NOT by SpanID, which is zero for ordinary
// attribute VI entries in production); bit1 (0x02) is set for EVERY span (R is provisionally
// treated as match-all, mirroring compileStructuralPair's own nil-filter convention) — D3B's
// verifyCandidateSpans confirms R's real filter afterward against exactly the walk's survivors.
func resolvedSpansToStructuralRecs(spans []ResolvedSpan, leftMatchAddrs map[structuralSpanAddr]struct{}) []structuralSpanRec {
	recs := make([]structuralSpanRec, len(spans))
	for i, sp := range spans {
		rec := structuralSpanRec{
			spanID:    sp.Span.SpanID,
			parentIdx: -1,
			nodeMatch: 0x02, // R: provisionally match-all, confirmed later via D3B.
			present:   structuralSpanIDPresent,
		}
		addr := structuralSpanAddr{sourceRef: sp.SourceRef, blockPage: sp.Span.BlockRef.PageNum, rowIdx: sp.RowIdx}
		if _, ok := leftMatchAddrs[addr]; ok {
			rec.nodeMatch |= 0x01
		}
		if !sp.Span.IsRoot() {
			rec.parentID = sp.Span.ParentSpanID
			rec.present |= structuralParentIDPresent
		}
		recs[i] = rec
	}
	return recs
}
