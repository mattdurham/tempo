package executor

// structural_index_negated.go — D6 (plan-d.md, issue #489): ExecuteNegatedStructuralFromIndex,
// the index-driven trace-level join engine for 2-node NEGATED structural queries (!>>, !>, !~).
//
// Architecturally distinct from D4's ExecuteStructuralFromIndex, not a parameter flip (per the
// audit, brainstorm §3): the candidate-trace set here is driven EXCLUSIVELY by the RIGHT (tested)
// operand's VI resolution. A trace with ZERO left-filter matches trivially satisfies !>>/!>/!~ for
// every right-match span (vacuous truth), so the left (negated) side cannot be scored/led via VI
// the way the positive path assumes — the value index has no fast negation path (the same
// documented limitation as negated FILTER predicates, SPEC-ROOT-019). The left side is therefore
// confirmed ONLY after the full TraceGroup tree is fetched, via D3B's verifyCandidateSpans applied
// UNCONDITIONALLY to every span in the tree (never a selectivity-led subset, unlike D4's
// conditional use of the same primitive) — strictly more expensive per candidate than the positive
// path, proportional to trace size rather than to the negated side's own VI selectivity. Own cost
// accounting (ruling 3): callers must not assume this path's cost model matches D4's.
//
// Both sides' structuralSpanRec bits are therefore REAL ground truth here (unlike D4's
// provisional-R-then-confirm convention): bit0 (nodeMatch&0x01) is set only for spans D3B confirms
// against the real left filter; bit1 (nodeMatch&0x02) is set only for spans the right VI source
// exactly matched. applyStructuralOp's existing evalOpNotDescendantStruct/evalOpNotChildStruct/
// evalOpNotSiblingStruct (stream_structural.go) are reused completely unchanged — this is exactly
// the parity-critical reuse D1's golden table exists to verify.

import (
	"context"
	"fmt"

	modules_shared "github.com/grafana/blockpack/internal/modules/blockio/shared"
	"github.com/grafana/blockpack/internal/modules/valueindex"
	"github.com/grafana/blockpack/internal/traceqlparser"
	"github.com/grafana/blockpack/internal/vm"
)

// ExecuteNegatedStructuralFromIndex answers a 2-node NEGATED structural query (!>>, !>, !~) using
// the value index for RIGHT-side candidate-trace discovery and TraceGroup for whole-trace
// ancestor/sibling resolution — no full-block scan. Mirrors ExecuteStructuralFromIndex's
// (result, ok, err) decline contract.
//
// ok=false, err=nil (routine decline, caller falls back to ExecuteStructural):
//   - q flattens to other than a 2-node chain (compileStructuralPair's own decline).
//   - op is a positive operator (>>, >, ~, <<, <) — routed to D4's separate function instead.
//   - rightSource has no coverage for the window (viMatchSpans ok=false), AND indexOnly is false.
//   - the trace-by-id index has zero candidate files for the window, AND indexOnly is false.
//
// ok=false, err!=nil: index/data inconsistency (skew) surfaces the SAME way
// MaterializeTraceGroupMultiFile / GetTraceByID already do — never silently masked. Also returned
// (as ErrStructuralIndexCoverageGap) when indexOnly is true and either coverage gap above occurs,
// or when a candidate's assembled TraceGroup is Partial (ruling 5 — identical rule to D4: a
// partial tree can produce false POSITIVES for negated operators, the mirror image of D4's
// false-negative concern, so it is never treated as an authoritatively complete answer either).
func ExecuteNegatedStructuralFromIndex(
	ctx context.Context,
	q *traceqlparser.StructuralQuery,
	rightSource ValueIndexSource,
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
	if !isNegationOp(op) {
		// Positive operators are D4's responsibility, not this function's.
		return nil, false, nil
	}
	if rightSource == nil {
		// SPEC-ROOT-001: rightSource is D6's ONLY VI-resolved side and REQUIRED -- guarded here at
		// the point of use, mirroring D4's leftSource guard (structural_index.go) and the same
		// defect class task #10 fixed for D4's own rightSource at the wrapper layer. Without this,
		// viMatchSpans calls a nil-interface method unconditionally on a match-all right leg
		// (NOTE-VI-087), and this engine has no root wrapper today to catch it one layer up.
		return nil, false, nil
	}

	// Candidate discovery driven EXCLUSIVELY by the right (tested) side (see package doc comment
	// above) — the left/negated side is never VI-resolved at all, by construction: there is no
	// leftSource parameter for any caller to wire one in, even by mistake.
	rightResults, rightOK := viMatchSpans(rightSource, rightProg)
	if !rightOK {
		if indexOnly {
			return nil, false, ErrStructuralIndexCoverageGap
		}
		return nil, false, nil
	}
	rightSpansByTrace := groupVILookupResultsByTrace(rightResults)
	candidateTraceIDs := chooseDiscoverySeed(rightSpansByTrace)

	if len(candidateTraceIDs) == 0 {
		return &StructuralResult{}, true, nil
	}

	colHash := valueindex.ColHash(modules_shared.TraceIDColumnName)
	colTypeName := valueindex.ColTypeName(modules_shared.ColumnTypeUUID)
	keys, discoverErr := valueindex.DiscoverIndexFiles(ctx, traceGroupStore, tenant, indexPrefix, colHash, colTypeName, minTS, maxTS)
	if discoverErr != nil {
		return nil, false, fmt.Errorf("ExecuteNegatedStructuralFromIndex: discover index files: %w", discoverErr)
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
		done, evalErr := evalOneNegatedStructuralCandidateTrace(
			ctx, traceID, keys, traceGroupStore, readerFor, minTS, maxTS,
			rightSpansByTrace[traceID], op, leftProg, opts, result,
		)
		if evalErr != nil {
			return nil, false, evalErr
		}
		if done {
			break
		}
	}
	return result, true, nil
}

// evalOneNegatedStructuralCandidateTrace resolves ONE candidate trace's TraceGroup, checks
// Partial (ruling 5), materializes its spans (D3), confirms the left filter UNCONDITIONALLY
// against every span via D3B (never a narrowed subset — ruling 3's own cost accounting), runs the
// negated structural walk, and appends surviving matches to result. Returns done=true once
// opts.Limit is reached. Split out of ExecuteNegatedStructuralFromIndex to keep that function's
// own cyclomatic complexity down (mirrors D4's evalOneStructuralCandidateTrace split).
func evalOneNegatedStructuralCandidateTrace(
	ctx context.Context,
	traceID [16]byte,
	keys []string,
	traceGroupStore valueindex.LookupStore,
	readerFor StructuralReaderProvider,
	minTS, maxTS uint64,
	rightMatchAddrs map[structuralSpanAddr]struct{},
	op traceqlparser.StructuralOp,
	leftProg *vm.Program,
	opts Options,
	result *StructuralResult,
) (bool, error) {
	group, found, findErr := FindTraceGroupInCandidates(ctx, traceGroupStore, keys, traceID, minTS, maxTS)
	if findErr != nil {
		return false, fmt.Errorf("ExecuteNegatedStructuralFromIndex: %w", findErr)
	}
	if !found {
		// Legitimate VI/TraceGroup skew at the trace level — not an error, mirrors D4/
		// GetTraceByID's own miss semantics.
		return false, nil
	}

	assembled := valueindex.AssembleTrace(group)
	if assembled.Partial {
		// Wrapped with the trace ID (MEDIUM finding, go-presubmit.md; mirrors D4's identical fix)
		// so an operator debugging a production coverage-gap error can identify which trace
		// triggered it -- %w preserves errors.Is compatibility (DT2's contract).
		return false, fmt.Errorf("trace %x: %w", traceID, ErrStructuralIndexCoverageGap)
	}

	resolvedSpans, resolveErr := MaterializeTraceGroupMultiFile(ctx, readerFor, group, traceID, 0)
	if resolveErr != nil {
		return false, fmt.Errorf("ExecuteNegatedStructuralFromIndex: %w", resolveErr)
	}
	if len(resolvedSpans) == 0 {
		return false, nil
	}

	// UNCONDITIONAL D3B confirmation (ruling 3): every span in the whole tree is checked against
	// the real left filter, never a selectivity-narrowed subset — this is what makes the left side
	// safe to treat as ground truth below, despite never having been VI-resolved.
	confirmedLeft, verifyErr := verifyCandidateSpans(ctx, leftProg, resolvedSpans)
	if verifyErr != nil {
		return false, fmt.Errorf("ExecuteNegatedStructuralFromIndex: %w", verifyErr)
	}
	leftMatchSpanIDs := make(map[[8]byte]struct{}, len(confirmedLeft))
	for _, sp := range confirmedLeft {
		leftMatchSpanIDs[sp.Span.SpanID] = struct{}{}
	}

	recs := resolvedSpansToNegatedStructuralRecs(resolvedSpans, leftMatchSpanIDs, rightMatchAddrs)
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

	blockByKey, blockErr := materializeConfirmedSpanBlocks(candidates)
	if blockErr != nil {
		return false, fmt.Errorf("ExecuteNegatedStructuralFromIndex: %w", blockErr)
	}

	for _, sp := range candidates {
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

// resolvedSpansToNegatedStructuralRecs converts D3's []ResolvedSpan (one trace's whole
// materialized tree) into stream_structural.go's structuralSpanRec shape, exactly like D4's
// resolvedSpansToStructuralRecs, EXCEPT both nodeMatch bits are REAL ground truth here (never a
// provisional-match-all bit). leftMatchSpanIDs is D3B's unconditional confirmation output, keyed
// by SpanID -- this side is safe to join on SpanID because BOTH sides of that comparison
// originate from real per-row data (confirmedLeft's spans come from the SAME resolvedSpans this
// function also iterates, whose SpanID is always the TraceGroup's own real, non-zero identity —
// never a raw attribute-column VILookupResult). rightMatchAddrs, by contrast, comes from the
// right VI source's exact matches and MUST be keyed by structuralSpanAddr, not SpanID (task #12,
// FIX-D4-CRITICAL — see structuralSpanAddr's own doc comment): an ordinary attribute-column VI
// entry carries a zero SpanID in production, so joining it against a real SpanEntry by SpanID
// never matches.
func resolvedSpansToNegatedStructuralRecs(
	spans []ResolvedSpan, leftMatchSpanIDs map[[8]byte]struct{}, rightMatchAddrs map[structuralSpanAddr]struct{},
) []structuralSpanRec {
	recs := make([]structuralSpanRec, len(spans))
	for i, sp := range spans {
		rec := structuralSpanRec{
			spanID:    sp.Span.SpanID,
			parentIdx: -1,
			present:   structuralSpanIDPresent,
		}
		if _, ok := leftMatchSpanIDs[sp.Span.SpanID]; ok {
			rec.nodeMatch |= 0x01
		}
		addr := structuralSpanAddr{sourceRef: sp.SourceRef, blockPage: sp.Span.BlockRef.PageNum, rowIdx: sp.RowIdx}
		if _, ok := rightMatchAddrs[addr]; ok {
			rec.nodeMatch |= 0x02
		}
		if !sp.Span.IsRoot() {
			rec.parentID = sp.Span.ParentSpanID
			rec.present |= structuralParentIDPresent
		}
		recs[i] = rec
	}
	return recs
}
