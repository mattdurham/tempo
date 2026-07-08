package blockpack

// structural.go — D5 (plan-d.md, issue #489): root-level public API for the index-driven
// structural-query path, QueryStructuralFromIndex. Mirrors QueryTraceQLFromIndex's shape and
// doc-comment conventions (api.go) — this is a thin wrapper with no heavy lifting of its own;
// modules_executor.ExecuteStructuralFromIndex (plan-d.md D4) does the actual work. Per the Rulings
// log (plan-d.md, ruling 2): this stays in its own new file rather than vcnt.go/api.go, and does
// not depend on the concurrent #481 session's uncommitted vcnt.go re-exports.
//
// D5b (task #11, surfaced by DT1 integration): ErrStructuralIndexCoverageGap and
// CompileStructuralLegs below let tempo build BOTH ValueIndexSources (left AND right) and
// classify a query's operator before calling QueryStructuralFromIndex — a permanently-nil
// rightSource would disable D4's intersection prefilter and the discovery-seed cost mechanism
// entirely (D4-SEED), defeating the seed-split ruling on the actual production path.

import (
	"context"
	"encoding/hex"
	"fmt"

	modules_blockio "github.com/grafana/blockpack/internal/modules/blockio"
	modules_executor "github.com/grafana/blockpack/internal/modules/executor"
	"github.com/grafana/blockpack/internal/modules/queryplan"
	"github.com/grafana/blockpack/internal/traceqlparser"
)

// ErrStructuralIndexCoverageGap is a public re-export of
// modules_executor.ErrStructuralIndexCoverageGap (mirrors the ErrValueIndexFileNotFound
// convention, valueindex_query.go) — without it, callers cannot errors.Is() against the typed
// coverage-gap error QueryStructuralFromIndex returns (indexOnly=true coverage gaps, Partial
// TraceGroups).
var ErrStructuralIndexCoverageGap = modules_executor.ErrStructuralIndexCoverageGap

// StructuralOp identifies which structural operator (>>, >, ~, <<, <, !~, !>>, !>) a structural
// query uses — a plain type alias for traceqlparser.StructuralOp, returned by
// CompileStructuralLegs so a caller can classify a query without importing internal/traceqlparser
// directly.
type StructuralOp = traceqlparser.StructuralOp

// Structural operator constants, re-exported from traceqlparser for callers that need to classify
// a StructuralOp (e.g. deciding whether a query is negated, to choose which execution path to
// try) without importing internal/traceqlparser directly.
const (
	OpDescendant    = traceqlparser.OpDescendant
	OpChild         = traceqlparser.OpChild
	OpSibling       = traceqlparser.OpSibling
	OpAncestor      = traceqlparser.OpAncestor
	OpParent        = traceqlparser.OpParent
	OpNotSibling    = traceqlparser.OpNotSibling
	OpNotDescendant = traceqlparser.OpNotDescendant
	OpNotChild      = traceqlparser.OpNotChild
)

// CompileStructuralLegs parses traceqlQuery and, if it is a 2-node structural query, compiles its
// two filter legs to their own *Program — mirrors QueryStructuralFromIndex's own parse+decline
// convention, but returns the compiled legs/op directly instead of executing the query. A nil
// filter leg (match-all, "{}") compiles to a nil *Program.
//
// ok=false, err=nil: traceqlQuery is not a structural query, or flattens to other than exactly 2
// filter nodes — caller falls back to QueryTraceQL (the scan path).
// ok=false, err!=nil: a parse error, or a genuine compile error on one of the two legs.
func CompileStructuralLegs(traceqlQuery string) (leftProg, rightProg *Program, op StructuralOp, ok bool, err error) {
	parsed, parseErr := traceqlparser.ParseTraceQL(traceqlQuery)
	if parseErr != nil {
		return nil, nil, 0, false, fmt.Errorf("parse TraceQL: %w", parseErr)
	}
	q, isStructural := parsed.(*traceqlparser.StructuralQuery)
	if !isStructural {
		return nil, nil, 0, false, nil
	}
	return modules_executor.CompileStructuralLegs(q)
}

// StructuralReaderProvider resolves a SourceRef (the S3 object key stamped on each SpanEntry,
// NOTE-VI-076) to an already-open *Reader for that file — Option A's (team-lead ruling,
// 2026-07-07) multi-file trace materialization primitive (plan-d.md D3). This is a zero-cost type
// alias, not an adapter: Reader is itself a plain alias for modules_reader.Reader (verified
// directly, reader.go:44), so root callers pass their own *blockpack.Reader-returning functions
// here unchanged.
type StructuralReaderProvider = modules_executor.StructuralReaderProvider

// StructuralSelectivityClassifier is a public re-export of
// modules_executor.StructuralSelectivityClassifier — a plain type alias, since Program is itself
// an alias for vm.Program (api.go). QueryStructuralFromIndex builds this closure internally over
// queryplan.ClassifyProgramVCNT; it is exported only so advanced callers/tests can construct their
// own classifier if needed.
type StructuralSelectivityClassifier = modules_executor.StructuralSelectivityClassifier

// QueryStructuralFromIndex executes a 2-node structural TraceQL query (`{A} OP {B}`, one of
// >>, >, ~, <<, <) using the value index for candidate-trace discovery and the trace-by-id index
// (TraceGroup) for whole-trace ancestor/sibling resolution — no full-block scan. See
// modules_executor.ExecuteStructuralFromIndex for the full contract; this is a thin,
// no-internal-imports-leaked wrapper mirroring QueryTraceQLFromIndex's existing convention
// (api.go).
//
// Scope (plan-d.md, "2-node structural queries only for v1"): a chain flattening to other than
// exactly 2 filter nodes, or a negated operator (!>>, !>, !~ — routed to QueryNegatedStructuralFromIndex,
// this file's D6 root wrapper, instead), is a routine decline (ok=false, err=nil); the caller falls
// back to QueryTraceQL (the scan path), which already handles the general case and is not removed
// by this function's existence.
//
// leftSource/rightSource are the caller's already-populated ValueIndexSource for the query's two
// filter legs (mirrors QueryTraceQLFromIndex's own source parameter — discovery/download of the
// underlying VI files is the caller's job). leftSource is required: L is unconditionally the
// structural walk's anchor (plan-d.md D4), so a nil leftSource can never be worked around and is
// treated the same as QueryTraceQLFromIndex's own "no index coverage supplied" decline.
// rightSource may legitimately be nil (e.g. a match-all right leg, or a caller that chooses not to
// pre-resolve it); this function only ever consults rightSource for the OPTIONAL cost-based
// intersection prefilter (plan-d.md D4 step 4), which is automatically disabled — never attempted
// — whenever rightSource or vcntData is nil, rather than risk a nil-source panic deep in that
// path.
//
// vcntData/vcntDir is one decoded VCNT section (both legs scored over the SAME window, mirroring
// queryplan.ClassifyProgramVCNT's single-window composition); pass nil/empty to always skip the
// cost-based intersection prefilter (the query still executes correctly, just without that
// optimization).
//
// traceGroupStore/tenant/indexPrefix/minTS/maxTS drive trace-by-id index discovery — see
// GetTraceByID's own doc comment (reader.go) for the identical authoritative-index contract;
// traceGroupStore and tenant are REQUIRED for the same reason (NOTE-VI-073): there is no scan
// fallback for a caller that omits them. readerFor resolves each candidate trace's
// SpanEntry.SourceRef to an open Reader for Option A's multi-file materialization (plan-d.md D3)
// — the caller owns reader caching/pooling.
//
// indexOnly (mirrors issue #487's IndexOnly / ErrSliceIndexCoverageGap pattern): when true, a
// coverage gap that would otherwise be a routine decline instead returns
// ErrStructuralIndexCoverageGap — a time-sliced structural job has no safe scan fallback across
// slice boundaries, so it must fail loudly rather than silently narrow.
//
// Each returned SpanMatch.Fields is materialized and safe to retain after return.
func QueryStructuralFromIndex(
	ctx context.Context,
	traceqlQuery string,
	leftSource, rightSource ValueIndexSource,
	vcntData []byte, vcntDir []VCNTChunkDirEntry,
	traceGroupStore LookupStore,
	tenant, indexPrefix string,
	readerFor StructuralReaderProvider,
	minTS, maxTS uint64,
	indexOnly bool,
	opts QueryOptions,
) (results []SpanMatch, ok bool, err error) {
	if ctx == nil {
		ctx = context.Background()
	}
	defer func() {
		if rec := recover(); rec != nil {
			results = nil
			ok = false
			err = fmt.Errorf("internal error in QueryStructuralFromIndex: %v", rec)
		}
	}()

	if leftSource == nil {
		// No index coverage supplied for the walk anchor: caller must fall back to a full scan.
		return nil, false, nil
	}
	if traceGroupStore == nil || tenant == "" {
		return nil, false, fmt.Errorf(
			"QueryStructuralFromIndex: traceGroupStore and tenant are required (NOTE-VI-073) -- there is no scan fallback",
		)
	}
	if shardErr := validateQueryOptions(opts); shardErr != nil {
		return nil, false, fmt.Errorf("QueryStructuralFromIndex: %w", shardErr)
	}

	parsed, parseErr := traceqlparser.ParseTraceQL(traceqlQuery)
	if parseErr != nil {
		return nil, false, fmt.Errorf("parse TraceQL: %w", parseErr)
	}
	q, isStructural := parsed.(*traceqlparser.StructuralQuery)
	if !isStructural {
		// Only structural queries use this path; filter/metrics queries have their own.
		return nil, false, nil
	}

	// The cost-based intersection prefilter (plan-d.md D4 step 4) is only ever attempted when
	// BOTH a VCNT section and a rightSource are available -- leaving isSelective nil disables it
	// deterministically at the executor layer (structural_index.go's own `isSelective != nil`
	// guard), rather than relying on rightSource happening to never be dereferenced.
	var isSelective StructuralSelectivityClassifier
	if len(vcntData) > 0 && rightSource != nil {
		isSelective = func(prog *Program) bool {
			return queryplan.ClassifyProgramVCNT(prog, vcntData, vcntDir, minTS, maxTS) == queryplan.Selective
		}
	}

	execOpts := modules_executor.Options{
		Limit:      opts.Limit,
		TimeRange:  normalizeTimeRange(opts.StartNano, opts.EndNano),
		StartBlock: opts.StartBlock,
		BlockCount: opts.BlockCount,
	}

	execResult, indexOK, execErr := modules_executor.ExecuteStructuralFromIndex(
		ctx, q, leftSource, rightSource, isSelective,
		traceGroupStore, tenant, indexPrefix, readerFor,
		minTS, maxTS, indexOnly, execOpts,
	)
	if execErr != nil {
		return nil, false, execErr
	}
	if !indexOK {
		return nil, false, nil
	}

	// Convert executor SpanMatch → public SpanMatch, materializing fields from the already-parsed
	// Block (same conversion pattern QueryTraceQLFromIndex and QueryTraceQL's structural case
	// use). The reader parameter is unused by NewSpanFieldsAdapterWithReader (it reads directly
	// from the already-decoded Block), and a genuinely multi-file structural result may not even
	// have one single reader to offer here.
	wantCols := modules_executor.ComputeSecondPassCols(nil, opts.SelectColumns)
	results = make([]SpanMatch, 0, len(execResult.Matches))
	for i := range execResult.Matches {
		m := &execResult.Matches[i]
		rawAdapter := modules_blockio.NewSpanFieldsAdapterWithReader(m.Block, nil, m.BlockIdx, m.RowIdx, wantCols)
		fields := rawAdapter
		if len(opts.SelectColumns) > 0 {
			fields = newFilteredSpanFields(rawAdapter, opts.SelectColumns)
		}
		match := SpanMatch{
			TraceID: hex.EncodeToString(m.TraceID[:]),
			SpanID:  hex.EncodeToString(m.SpanID),
			Fields:  fields,
		}
		results = append(results, match.Clone())
		// NOTE-ALLOC-4: release after Clone materializes the fields.
		modules_blockio.ReleaseSpanFieldsAdapter(rawAdapter)
	}
	return results, true, nil
}

// QueryNegatedStructuralFromIndex executes a 2-node NEGATED structural TraceQL query (`{A} OP {B}`,
// one of !>>, !>, !~) using the value index for RIGHT-side candidate-trace discovery and the
// trace-by-id index (TraceGroup) for whole-trace ancestor/sibling resolution — no full-block scan.
// See modules_executor.ExecuteNegatedStructuralFromIndex (SPEC-STRUCT-10) for the full contract;
// this is a thin, no-internal-imports-leaked wrapper mirroring QueryStructuralFromIndex's own
// convention (this file), the D4/D6 sibling split.
//
// D6 is architecturally distinct from D4, not a parameter flip: candidate-trace discovery here is
// driven EXCLUSIVELY by the RIGHT (tested) operand's VI resolution — the left (negated) side has
// no fast negation path via the value index (the same documented limitation as negated FILTER
// predicates, SPEC-ROOT-019) and is instead confirmed only after the full TraceGroup tree is
// fetched, via D3B's verifyCandidateSpans applied unconditionally to every span in the tree. This
// is strictly more expensive per candidate than QueryStructuralFromIndex's positive-operator path,
// proportional to trace size rather than to the negated side's own VI selectivity — callers must
// not assume this path's cost model matches QueryStructuralFromIndex's.
//
// Scope (plan-d.md, "2-node structural queries only for v1"): a chain flattening to other than
// exactly 2 filter nodes, or a positive operator (>>, >, ~, <<, < — routed to
// QueryStructuralFromIndex instead), is a routine decline (ok=false, err=nil); the caller falls
// back to QueryTraceQL (the scan path).
//
// rightSource is the caller's already-populated ValueIndexSource for the query's right (tested)
// filter leg — the ONLY side this function ever VI-resolves. rightSource is required: unlike
// QueryStructuralFromIndex's leftSource/rightSource split, D6 has no optional side — a nil
// rightSource can never be worked around and is treated the same as QueryStructuralFromIndex's own
// "no index coverage supplied" decline. There is no vcntData/vcntDir/selectivity-classifier
// parameter here: D6's cost model has no cost-based intersection prefilter equivalent to D4's
// step 4 (see modules_executor.ExecuteNegatedStructuralFromIndex's own doc comment).
//
// traceGroupStore/tenant/indexPrefix/minTS/maxTS drive trace-by-id index discovery — see
// GetTraceByID's own doc comment (reader.go) for the identical authoritative-index contract;
// traceGroupStore and tenant are REQUIRED for the same reason (NOTE-VI-073): there is no scan
// fallback for a caller that omits them. readerFor resolves each candidate trace's
// SpanEntry.SourceRef to an open Reader for Option A's multi-file materialization (plan-d.md D3)
// — the caller owns reader caching/pooling.
//
// indexOnly (mirrors issue #487's IndexOnly / ErrSliceIndexCoverageGap pattern, and
// QueryStructuralFromIndex's own identical parameter): when true, a coverage gap that would
// otherwise be a routine decline instead returns ErrStructuralIndexCoverageGap — a time-sliced
// structural job has no safe scan fallback across slice boundaries, so it must fail loudly rather
// than silently narrow.
//
// Each returned SpanMatch.Fields is materialized and safe to retain after return.
func QueryNegatedStructuralFromIndex(
	ctx context.Context,
	traceqlQuery string,
	rightSource ValueIndexSource,
	traceGroupStore LookupStore,
	tenant, indexPrefix string,
	readerFor StructuralReaderProvider,
	minTS, maxTS uint64,
	indexOnly bool,
	opts QueryOptions,
) (results []SpanMatch, ok bool, err error) {
	if ctx == nil {
		ctx = context.Background()
	}
	defer func() {
		if rec := recover(); rec != nil {
			results = nil
			ok = false
			err = fmt.Errorf("internal error in QueryNegatedStructuralFromIndex: %v", rec)
		}
	}()

	if rightSource == nil {
		// D6's ONLY VI-resolved side has no coverage supplied: caller must fall back to a full
		// scan. Mirrors QueryStructuralFromIndex's leftSource == nil decline.
		return nil, false, nil
	}
	if traceGroupStore == nil || tenant == "" {
		return nil, false, fmt.Errorf(
			"QueryNegatedStructuralFromIndex: traceGroupStore and tenant are required (NOTE-VI-073) -- there is no scan fallback",
		)
	}
	if shardErr := validateQueryOptions(opts); shardErr != nil {
		return nil, false, fmt.Errorf("QueryNegatedStructuralFromIndex: %w", shardErr)
	}

	parsed, parseErr := traceqlparser.ParseTraceQL(traceqlQuery)
	if parseErr != nil {
		return nil, false, fmt.Errorf("parse TraceQL: %w", parseErr)
	}
	q, isStructural := parsed.(*traceqlparser.StructuralQuery)
	if !isStructural {
		// Only structural queries use this path; filter/metrics queries have their own.
		return nil, false, nil
	}

	execOpts := modules_executor.Options{
		Limit:      opts.Limit,
		TimeRange:  normalizeTimeRange(opts.StartNano, opts.EndNano),
		StartBlock: opts.StartBlock,
		BlockCount: opts.BlockCount,
	}

	execResult, indexOK, execErr := modules_executor.ExecuteNegatedStructuralFromIndex(
		ctx, q, rightSource,
		traceGroupStore, tenant, indexPrefix, readerFor,
		minTS, maxTS, indexOnly, execOpts,
	)
	if execErr != nil {
		return nil, false, execErr
	}
	if !indexOK {
		return nil, false, nil
	}

	// Convert executor SpanMatch → public SpanMatch — identical conversion to
	// QueryStructuralFromIndex's own tail (this file).
	wantCols := modules_executor.ComputeSecondPassCols(nil, opts.SelectColumns)
	results = make([]SpanMatch, 0, len(execResult.Matches))
	for i := range execResult.Matches {
		m := &execResult.Matches[i]
		rawAdapter := modules_blockio.NewSpanFieldsAdapterWithReader(m.Block, nil, m.BlockIdx, m.RowIdx, wantCols)
		fields := rawAdapter
		if len(opts.SelectColumns) > 0 {
			fields = newFilteredSpanFields(rawAdapter, opts.SelectColumns)
		}
		match := SpanMatch{
			TraceID: hex.EncodeToString(m.TraceID[:]),
			SpanID:  hex.EncodeToString(m.SpanID),
			Fields:  fields,
		}
		results = append(results, match.Clone())
		// NOTE-ALLOC-4: release after Clone materializes the fields.
		modules_blockio.ReleaseSpanFieldsAdapter(rawAdapter)
	}
	return results, true, nil
}
