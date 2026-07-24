// Package vibuilder orchestrates building an executor.ValueIndexSource for a
// query from object storage (NOTE-VI-036, issue #461).
//
// The value-index search and metrics paths (NOTE-VI-033/035) both consume a
// pre-populated ValueIndexSource: the querier is responsible for discovering the
// relevant value-index files, downloading them, applying the per-leaf predicate,
// and assembling the source. That orchestration lived only as prose in the
// executor NOTES until now; this package implements it so tempo's querier
// (blockpackBlock) can wire the index query path with a single call.
//
// Flow per query:
//
//  1. Walk the compiled program's predicate tree (vm.Program.Predicates).
//  2. For each leaf column with a buildable predicate: hash the column, discover
//     the overlapping VI files (via an IndexFileCache to avoid an S3 LIST per
//     query — issue #462), download them, run valueindex.QueryFiles with the leaf
//     predicate, map the results to executor.VILookupResult, and Add them to the
//     source. A covered-but-empty column Adds an empty slice (coverage, not
//     fallback — see NOTE-VI-033).
//  3. A genuine match-all query (`{}`, no filter at all) compiles with
//     prog.Predicates == nil entirely (vm.compileMatchAllProgram never populates
//     Predicates) and is left for the caller's own full-scan fallback — the value
//     index is partitioned per column, so there is no column list to enumerate
//     without at least one referenced column.
//
// A column whose leaf predicate cannot be expressed against the value index (e.g.
// a vector predicate, or an unindexable column type) is simply not Added, so the
// source reports no coverage for it and the caller falls back to a full block
// scan. This is the same fail-safe contract the executor already documents.
//
// Task #211 (NOTE-492 addendum, internal/vm/NOTES.md): a compiled program whose
// Predicates.Nodes is empty but Predicates.Columns is non-empty is NEVER a genuine
// match-all in practice — every real compiler path that produces this exact shape
// (task #210's own "either side of an AND/OR unconstrained" decline guard, or a
// standalone `!~`/built-in-field `!=` with nothing else in the query) is a decline,
// tracking column names purely for row-level decode bookkeeping, not a "fetch
// everything" request. internal/modules/executor/metrics_trace.go's viMatchSpans
// (the ONLY consumer of a source this package builds) already declines
// unconditionally whenever Predicates.Nodes is empty, regardless of Columns — so a
// source built for this shape could never be used by any real caller. BuildSource
// and BuildSourceBounded therefore decline this shape immediately too, without
// ever calling lookupColumnAll, instead of wastefully downloading every listed
// column's full VI file set for a source no caller can consume.
package vibuilder

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sync/atomic"

	"golang.org/x/sync/errgroup"

	modules_shared "github.com/grafana/blockpack/internal/modules/blockio/shared"
	modules_executor "github.com/grafana/blockpack/internal/modules/executor"
	"github.com/grafana/blockpack/internal/modules/valueindex"
	"github.com/grafana/blockpack/internal/vm"
)

// ErrFileNotFound is the sentinel a FileStore returns (wrapped or bare) when the
// requested value-index object does not exist in object storage — i.e. an S3 404 /
// NoSuchKey. The querier treats a not-found file as an empty miss and skips it
// rather than aborting the index build (NOTE-VI-041, issue #399 point 5).
//
// This is the last line of defense for the compactor's write-then-delete cycle:
// the file-listing cache can hand the querier a key the compactor has just
// deleted. A genuinely absent file holds no postings, so skipping it cannot
// under-count results — unlike a transient network/auth error, which must still
// abort the build so the caller falls back to a correct full scan.
//
// FileStore implementations signal a 404 by returning an error that satisfies
// errors.Is(err, ErrFileNotFound) from either Size or ReadAt.
var ErrFileNotFound = errors.New("vibuilder: value-index file not found")

// downloadConcurrency bounds how many value-index files queryKeysRanged fetches in
// parallel for one column's file set. The original 16-32 range was sized only
// for S3/minio connection pressure, on the assumption this work was I/O-wait
// bound. A live CPU profile on the dev test cluster after deploying that version
// showed otherwise: each downloaded file is immediately snappy-decoded and
// merge-sorted in the same goroutine (valueindex.DecodeBucketFile,
// executor.viSortDedup/viMatchSpans), so downloadConcurrency actually bounds
// concurrent CPU-bound work, not just concurrent sockets. The querier runs
// under a 5-core CPU limit; the original value of 24 (worst case 96 with
// leafConcurrency) oversubscribed that by roughly 19x and caused queries to
// time out on CPU contention even though each individual block's work was
// fast in isolation. Lowered to roughly track available cores instead
// (NOTE-VI, dev-cluster TraceQL search timeout incident, CPU-bound
// follow-up).
const downloadConcurrency = 4

// leafConcurrency bounds how many predicate leaf columns BuildSource downloads
// in parallel. Kept smaller than downloadConcurrency because each leaf's own
// queryKeysRanged can itself fan out up to downloadConcurrency downloads — worst
// case simultaneous CPU-bound goroutines for one query is
// leafConcurrency * downloadConcurrency. See downloadConcurrency's comment for
// why this bounds CPU work, not just I/O.
const leafConcurrency = 2

// FileStore downloads a single value-index file by its full object key. It is the
// read half of the storage backend the querier already holds (tempo's S3 reader,
// the local folder wrapper in tests). Size + ReadAt is the same minimal contract
// as blockpack.Storage; vibuilder reads the whole object into memory because
// QueryFiles is a pure in-memory pass over the file bytes.
type FileStore interface {
	// Size returns the byte length of the object at key.
	Size(key string) (int64, error)
	// ReadAt fills p from the object at key starting at off, following
	// io.ReaderAt semantics (a short read returns a non-nil error; EOF is io.EOF).
	ReadAt(key string, p []byte, off int64) (int, error)
}

// FileDiscoverer returns the value-index file keys for one column directory that
// overlap [minSec, maxSec]. *valueindex.IndexFileCache satisfies this via
// FilesForTimeRange; a direct DiscoverIndexFiles wrapper satisfies it too. Keeping
// it an interface lets tests inject a fake and lets the caller choose cached vs
// uncached discovery.
type FileDiscoverer interface {
	FilesForTimeRange(ctx context.Context, colHash, colTypeName string, minSec, maxSec uint64) ([]string, error)
}

// FileDiscovererNewestFirst is FileDiscoverer's newest-first sibling: returns the same
// column's file keys but ordered by valueindex.SortFileMetasNewestFirst instead of
// SortFileMetas (Phase 1.4), letting an early-stopping consumer ask for newest-first
// candidates directly instead of discovering ascending and re-sorting itself. Kept as a
// SEPARATE interface (not a new method appended to FileDiscoverer) so
// *valueindex.IndexFileCache -- which now satisfies BOTH FilesForTimeRange and
// FilesForTimeRangeNewestFirst -- continues to satisfy the existing FileDiscoverer
// contract with no breaking signature change for its many other callers.
// BuildSourceBounded type-asserts a FileDiscoverer parameter against this interface and
// only takes the early-stopping path when it succeeds, falling back to the ordinary
// unbounded resolution otherwise (never a correctness violation, just no early-stop
// optimization for a discoverer that doesn't support it).
type FileDiscovererNewestFirst interface {
	FilesForTimeRangeNewestFirst(
		ctx context.Context,
		colHash, colTypeName string,
		minSec, maxSec uint64,
	) ([]string, error)
}

// BuildSource assembles an executor.ValueIndexSource for prog over [minSec, maxSec]
// using disc for file discovery and store for downloads.
//
// It returns (nil, false, nil) when the source would have no coverage at all — no
// leaf column resolved and the query is not a coverable match-all — so the caller
// can skip the index path entirely and go straight to a full scan. Otherwise it
// returns a populated source and true; the executor's own coverage gate then
// decides per-column whether to fall back.
//
// A download or discovery error is returned; the caller should fall back to a full
// scan on error rather than fail the query.
//
// watermarks (#496 R7, plan.md Section 4.7) gates coverage for non-dedicated,
// usage-triggered columns mid-backfill: keyed by ColumnWatermarkKey(colName, colTypeName), NOT
// colName alone (issue #536 -- see ColumnWatermark's own doc comment for why a colName-only key
// silently collides two same-name, different-type columns). nil (or a column simply absent from
// the map -- the common case for dedicated columns, which are never usage-tracked) means no
// gating, exactly today's behavior. A column present in watermarks whose CoversRange(minSec, maxSec) is false is
// left un-Added, so the executor's existing decline/fallback path fires --
// identical to "no VI files discovered at all." This is the single most
// important correctness gate in the whole #496 feature: VI's index is
// documented as authoritative (NOTE-VI-096, issue #474), so a query must
// NEVER assemble a "complete" answer from a column whose historical backfill
// is still in progress for the queried time range.
func BuildSource(
	ctx context.Context,
	disc FileDiscoverer,
	store FileStore,
	prog *vm.Program,
	minSec, maxSec uint64,
	watermarks map[string]ColumnWatermark,
) (*modules_executor.SliceValueIndexSource, bool, error) {
	if prog == nil || disc == nil || store == nil {
		return nil, false, nil
	}
	preds := prog.Predicates
	src := modules_executor.NewSliceValueIndexSource()
	timeRange := &[2]uint64{minSec, maxSec}

	// Task #211: decline immediately whenever preds.Nodes is empty, regardless of whether
	// preds.Columns is also empty. Nodes==0 && Columns==0 is a genuine "nothing referenced"
	// query (e.g. `{}`); Nodes==0 && Columns>0 is ALWAYS a compile-time decline in practice
	// (task #210's zero-node-operand AND/OR guard, a standalone `!~`, etc. — see this file's
	// own package doc comment for the full argument for why there is no genuine "match-all
	// with an explicit column list" shape reachable from the real compiler). Either way,
	// viMatchSpans (executor/metrics_trace.go, the only consumer of this source) declines
	// unconditionally whenever Nodes is empty, so building a source for this shape via
	// lookupColumnAll below was always wasted I/O that no caller could ever use.
	if preds == nil || len(preds.Nodes) == 0 {
		return nil, false, nil
	}

	added := false

	// Leaf predicates: each contributes a constrained per-column result set.
	// buildPredicate is pure CPU (no I/O) and stays synchronous; only the
	// downstream discovery+download+query work fans out.
	leaves := collectLeaves(preds.Nodes)
	var work []leafWork
	for i := range leaves {
		pred, colType, ok := buildPredicate(&leaves[i])
		if !ok {
			// Unindexable predicate for this leaf — leave the column uncovered so
			// the executor falls back. Task #212 (NOTE-VI-107 addendum), corrected by task
			// #213 (CRITICAL regression fix): a RequirePresent-shaped leaf from the SCOPED
			// `attr != V` rewrite's own existence-only sibling (NOTE-453/454,
			// vm.RangeNode.NeqPairedRange) is marked so LookupLeaf can safely substitute
			// ITS EXACT paired sibling leaf(s)' own data — never a bare column-name
			// aggregate, which task #213 found could silently borrow an UNRELATED leaf's
			// data whenever an independent leaf elsewhere in the query happened to
			// reference the identical expanded column name (exactly what the UNSCOPED `!=`
			// rewrite's two RequirePresent leaves target). pairedLeafIdxs is only non-empty
			// for that scoped shape (collectLeaves' own doc comment); the unscoped rewrite's
			// RequirePresent leaves, and every other decline reason (unsupported value type,
			// multi-value OR, a genuine same-column decidability decline), are left unmarked
			// and still decline exactly as before.
			if leaves[i].node.RequirePresent && len(leaves[i].pairedLeafIdxs) > 0 {
				src.MarkRequirePresentLeaf(leaves[i].idx, leaves[i].pairedLeafIdxs)
			}
			continue
		}
		work = append(work, leafWork{col: leaves[i].col, colType: colType, pred: pred, idx: leaves[i].idx})
	}
	if len(work) > 0 {
		g, gctx := errgroup.WithContext(ctx)
		g.SetLimit(leafConcurrency)
		var anyLeafAdded atomic.Bool
		for _, w := range work {
			g.Go(func() error {
				results, filesRead, bytesRead, err := lookupColumn(
					gctx,
					disc,
					store,
					w.col,
					w.colType,
					w.pred,
					timeRange,
				)
				if err != nil {
					return err
				}
				// Record the download I/O for this leaf so the querier can report it
				// on its OTel span (issue #465); a covered-but-empty column still
				// counts the bytes of any files we read deciding it was empty. src is
				// mutex-protected, safe from concurrent leaves.
				src.RecordFileIO(filesRead, bytesRead)
				// #496 R7: a column mid-backfill whose watermark does not yet cover
				// this query's window must be left un-Added -- see BuildSource's own
				// doc comment for why this is the feature's single most important
				// correctness gate.
				// Issue #536: keyed by (w.col, w.colType), not w.col alone -- see BuildSource's own
				// comment and ColumnWatermark's own doc comment for the rationale.
				wmKey := ColumnWatermarkKey(w.col, valueindex.ColTypeName(w.colType))
				if wm, ok := watermarks[wmKey]; ok && !wm.CoversRange(minSec, maxSec) {
					return nil
				}
				// Add even when empty: a covered-but-empty column is coverage, not
				// fallback (NOTE-VI-033). AddLeaf (issue #206) additionally keys this
				// leaf's own results by w.idx -- the leaf's DFS leaf-slot STRUCTURAL
				// position, not a *vm.RangeNode pointer (pointer identity was tried first
				// and discarded: QueryTraceQLFromIndex recompiles the query string
				// independently, producing different node pointers than build-time saw,
				// but the same idx numbering for identical query text) -- so a same-column
				// sibling leaf (e.g. a second range bound) never gets merged into the
				// same bucket -- see SliceValueIndexSource.AddLeaf's doc comment.
				src.AddLeaf(w.idx, w.col, w.colType, results)
				anyLeafAdded.Store(true)
				return nil
			})
		}
		if err := g.Wait(); err != nil {
			return nil, false, err
		}
		// #496 R7: added must reflect whether a leaf was ACTUALLY Added, not merely
		// whether a leaf had a buildable predicate (len(work) > 0) -- the watermark
		// gate above can now skip src.Add for every leaf in work, and reporting
		// added=true anyway would silently reintroduce a "false complete" answer:
		// the caller sees ok=true and a source with zero covered columns, mistaking
		// "the index has no idea" for "the index confirms zero matches."
		if anyLeafAdded.Load() {
			added = true
		}
	}

	if !added {
		return nil, false, nil
	}
	return src, true, nil
}

// BuildSourceBounded mirrors BuildSource but threads limit through to an early-stopping,
// newest-first resolution for the SINGLE-LEAF case (Phase 2 scope, plan-scan-fallback.md).
// When the query has more than one leaf (collectLeaves flattens both AND and OR
// combinators, so this also covers multi-leaf OR), early-stopping is not yet implemented
// for that shape (Phase 3/4 replace this fallback) -- BuildSourceBounded falls through to
// the ordinary unbounded lookupColumn per leaf instead of silently under-reporting, so a
// multi-leaf query still gets a CORRECT, just not yet early-stopped, answer.
//
// limit <= 0 is treated as unbounded and delegates to BuildSource directly (identical
// behavior, no early-stopping code path exercised at all).
//
// disc is type-asserted against FileDiscovererNewestFirst; a discoverer that doesn't
// support newest-first discovery falls back to the unbounded path too (never a
// correctness violation, just no early-stop optimization).
//
// Watermark gating (#496 R7) and the match-all path are unchanged from BuildSource --
// neither is in scope for early-stopping (a match-all query has no selectivity concept to
// bound; watermarks describe backfill coverage, not recency).
func BuildSourceBounded(
	ctx context.Context,
	disc FileDiscoverer,
	store FileStore,
	prog *vm.Program,
	minSec, maxSec uint64,
	watermarks map[string]ColumnWatermark,
	limit int,
) (*modules_executor.SliceValueIndexSource, bool, error) {
	if limit <= 0 {
		return BuildSource(ctx, disc, store, prog, minSec, maxSec, watermarks)
	}
	if prog == nil || disc == nil || store == nil {
		return nil, false, nil
	}
	preds := prog.Predicates
	src := modules_executor.NewSliceValueIndexSource()
	timeRange := &[2]uint64{minSec, maxSec}

	// Task #211: mirrors BuildSource's own bail-out — decline immediately whenever
	// preds.Nodes is empty, regardless of Columns (see BuildSource's own comment and this
	// file's package doc comment for the full argument for why Nodes==0/Columns>0 is always
	// a compile-time decline in practice, never a genuine match-all).
	if preds == nil || len(preds.Nodes) == 0 {
		return nil, false, nil
	}

	added := false

	leaves := collectLeaves(preds.Nodes)
	var work []leafWork
	for i := range leaves {
		pred, colType, ok := buildPredicate(&leaves[i])
		if !ok {
			// Task #212 (NOTE-VI-107 addendum), corrected by task #213: mirrors
			// BuildSource's own leaf loop -- mark a RequirePresent-shaped leaf ONLY when it
			// carries its exact paired sibling leaf indices (the scoped `!=` rewrite's
			// shape), never a bare column name. See BuildSource's identical comment for the
			// full rationale.
			if leaves[i].node.RequirePresent && len(leaves[i].pairedLeafIdxs) > 0 {
				src.MarkRequirePresentLeaf(leaves[i].idx, leaves[i].pairedLeafIdxs)
			}
			continue
		}
		work = append(work, leafWork{col: leaves[i].col, colType: colType, pred: pred, idx: leaves[i].idx})
	}

	discNewestFirst, canEarlyStop := disc.(FileDiscovererNewestFirst)
	// Phase 2 scope: only a genuine single leaf gets the newest-first, early-stopping
	// resolution below.
	singleLeaf := len(work) == 1 && canEarlyStop
	// Phase 4 scope: a multi-leaf query with NO OR anywhere in the predicate tree (a pure
	// AND of 2+ leaves) gets the anchor+confirm early-stopping resolution instead of falling
	// through to the unbounded per-leaf path. A query containing any OR (whether pure OR or
	// a mixed AND-of-ORs shape) is not yet handled here -- Phase 3 covers the pure-OR case
	// separately; a mixed shape still falls through to the safe, unbounded stopgap below.
	multiLeafAND := len(work) > 1 && canEarlyStop && !hasORNode(preds.Nodes)
	// Phase 3 scope: a genuine flat OR of leaves (isFlatORQuery) gets the newest-first
	// per-leaf lookup too -- unlike Phase 4's AND case, OR needs NO special merge routing
	// here: the executor's viEvalNodes/viEvalOR (metrics_trace.go) already merges each
	// leaf's newest-first results via ViUnionNewestFirst at query-evaluation time, so
	// using lookupColumnNewestFirst per leaf (same call the singleLeaf branch below
	// already makes) is the ONLY change this shape needs.
	flatOR := len(work) > 1 && canEarlyStop && isFlatORQuery(preds.Nodes)

	if multiLeafAND {
		andAdded, err := buildSourceBoundedMultiLeafAND(
			ctx,
			discNewestFirst,
			disc,
			store,
			work,
			timeRange,
			minSec,
			maxSec,
			watermarks,
			limit,
			src,
		)
		if err != nil {
			return nil, false, err
		}
		if andAdded {
			added = true
			// Reviewer-2-6 MEDIUM fix: mark the source only when a genuine early-stopping
			// path was actually taken for EVERY leaf in this query (see SliceValueIndexSource
			// .MarkNewestFirst's own doc comment for why this must be all-or-nothing) -- lets
			// viMatchSpans opt this source into viEvalAND/viEvalOR's order-preserving merge at
			// query-eval time instead of the default key-sorted chain.
			src.MarkNewestFirst()
		}
	} else if len(work) > 0 {
		perLeafAdded, err := buildSourceBoundedPerLeaf(
			ctx, discNewestFirst, disc, store, work, timeRange, minSec, maxSec, watermarks, limit, singleLeaf || flatOR, src,
		)
		if err != nil {
			return nil, false, err
		}
		if perLeafAdded {
			added = true
			if singleLeaf || flatOR {
				src.MarkNewestFirst()
			}
		}
	}

	if !added {
		return nil, false, nil
	}
	return src, true, nil
}

// buildSourceBoundedPerLeaf resolves each leaf in work independently and concurrently
// (mirrors BuildSource's own leaf loop shape), choosing newestFirst's resolution path per
// leaf: lookupColumnNewestFirst when true (Phase 2's single-leaf case, or Phase 3's flat-OR
// case -- the executor's own viEvalOR/ViUnionNewestFirst merges the per-leaf newest-first
// results at query-eval time, so no special merge routing is needed here), or the ordinary
// unbounded lookupColumn otherwise (the safe stopgap for any shape not yet covered by an
// early-stopping design -- a mixed AND-of-ORs, or a discoverer without newest-first support).
func buildSourceBoundedPerLeaf(
	ctx context.Context,
	discNewestFirst FileDiscovererNewestFirst,
	disc FileDiscoverer,
	store FileStore,
	work []leafWork,
	timeRange *[2]uint64,
	minSec, maxSec uint64,
	watermarks map[string]ColumnWatermark,
	limit int,
	newestFirst bool,
	src *modules_executor.SliceValueIndexSource,
) (added bool, err error) {
	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(leafConcurrency)
	var anyLeafAdded atomic.Bool
	for _, w := range work {
		g.Go(func() error {
			var results []modules_executor.VILookupResult
			var filesRead int
			var bytesRead int64
			var lerr error
			if newestFirst {
				results, filesRead, bytesRead, lerr = lookupColumnNewestFirst(
					gctx, discNewestFirst, store, w.col, w.colType, w.pred, timeRange, limit,
				)
			} else {
				results, filesRead, bytesRead, lerr = lookupColumn(
					gctx, disc, store, w.col, w.colType, w.pred, timeRange,
				)
			}
			if lerr != nil {
				return lerr
			}
			src.RecordFileIO(filesRead, bytesRead)
			// Issue #536: keyed by (w.col, w.colType), not w.col alone -- see BuildSource's
			// identical comment and ColumnWatermark's own doc comment for the rationale.
			wmKey := ColumnWatermarkKey(w.col, valueindex.ColTypeName(w.colType))
			if wm, ok := watermarks[wmKey]; ok && !wm.CoversRange(minSec, maxSec) {
				return nil
			}
			// AddLeaf, not Add (issue #206): this stopgap path also resolves a mixed
			// AND-of-ORs shape (e.g. `a >= 100 && a <= 150 && (b = "x" || b = "y")`), which
			// can still contain two same-column AND-sibling leaves (the `a` bounds here)
			// even though the overall query fell through to this per-leaf resolution rather
			// than buildSourceBoundedMultiLeafAND -- disambiguation by leaf identity is
			// needed here for exactly the same reason as BuildSource's own leaf loop.
			src.AddLeaf(w.idx, w.col, w.colType, results)
			anyLeafAdded.Store(true)
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return false, err
	}
	return anyLeafAdded.Load(), nil
}

// leaf is a flattened leaf RangeNode: a column plus its predicate description.
//
// idx (issue #206) is this leaf's DFS leaf-slot position, assigned by collectLeaves in the
// exact same pre-order numbering the executor's viEvalNode uses at query-eval time (see
// leafWork.idx's own doc comment for the full rationale). idx defaults to -1 for a leaf
// value never produced by collectLeaves (e.g. a bare struct literal in a test) -- see
// LeafIndexable/LeafColumns' throwaway probe leaf below, which never threads idx onward to
// AddLeaf so its zero-value default is inert.
type leaf struct {
	node *vm.RangeNode
	col  string
	// pairedLeafIdxs (task #213, CRITICAL regression fix) holds the DFS leaf-slot indices of
	// this leaf's SAME-rewrite value-bearing sibling(s), set ONLY when node.RequirePresent &&
	// node.NeqPairedRange (the scoped `!=` rewrite's RequirePresent leaf — see
	// vm.RangeNode.NeqPairedRange's own doc comment). nil for every other leaf, including the
	// unscoped `!=` rewrite's OR-of-two-RequirePresent leaves, which must never be paired with
	// anything. See collectLeaves' own doc comment for how these indices are computed.
	pairedLeafIdxs []int
	idx            int
}

// leafWork is one leaf's resolved (column, type, predicate) triple, ready to pass to
// lookupColumn/lookupColumnNewestFirst. Package-level (not BuildSourceBounded-local) so
// Phase 4's buildSourceBoundedMultiLeafAND can share the exact same shape.
//
// idx (issue #206) is the same DFS leaf-slot position leaf.idx carries, threaded through to
// SliceValueIndexSource.AddLeaf so a same-column sibling leaf (e.g. `col >= 100` and
// `col <= 150`, two separate leaves per traceql_compiler's own AND flattening) gets its own
// disambiguated results bucket instead of being merged into the same column-keyed slot as
// its sibling.
//
// A *vm.RangeNode POINTER would NOT be safe here: the public blockpack.QueryTraceQLFromIndex
// takes the query as a string and recompiles it internally rather than reusing the caller's
// own *vm.Program, so the pointers vibuilder sees while building the source and the ones the
// executor sees while evaluating it are, in general, different tree instances for the
// identical query text (verified directly -- see AddLeaf's doc comment in metrics_trace.go).
// idx is instead the leaf's STRUCTURAL position, which two independent compiles of the same
// query text always agree on (compilation is a deterministic, purely syntax-driven walk).
//
// idx < 0 means "no identity to offer" -- AddLeaf then falls back to the legacy column-only
// Add behavior. Every leafWork built from a real leaf (via collectLeaves) always carries a
// valid, non-negative idx; the only literals that omit it are direct test constructions
// exercising the different-column case, which was never ambiguous and is unaffected either
// way -- but those call sites must NOT rely on the zero-value default (0 is itself a valid
// leaf-slot number and would collide with a real leaf 0 elsewhere), so any such construction
// explicitly sets idx: -1 or pulls idx from its own leaf.idx.
type leafWork struct {
	pred    valueindex.Predicate
	col     string
	colType modules_shared.ColumnType
	idx     int
}

// hasORNode reports whether nodes (or anything nested under it) contains an OR composite
// (IsOR=true, len(Children) > 0) anywhere in the tree. Used by BuildSourceBounded to detect
// the "pure AND, no nested OR" shape Phase 4's anchor+confirm design requires -- a query
// with ANY OR anywhere (a pure OR, or a mixed AND-of-ORs) is not this phase's scope and
// falls through to the existing unbounded stopgap instead.
func hasORNode(nodes []vm.RangeNode) bool {
	for i := range nodes {
		n := &nodes[i]
		if len(n.Children) == 0 {
			continue
		}
		if n.IsOR {
			return true
		}
		if hasORNode(n.Children) {
			return true
		}
	}
	return false
}

// isFlatORQuery reports whether nodes is a GENUINE flat OR of leaves (Phase 3,
// plan-scan-fallback.md): exactly one top-level node, itself an OR composite
// (IsOR=true) with 2+ children, every one of which is itself a leaf (no further
// Children — "absence of nested AND", per the plan's own scope note). This is
// stricter than hasORNode's "any OR anywhere" check above: a query like
// `a OR (b AND c)` has an OR node but is NOT a flat OR of leaves (one child is
// itself a composite) and correctly falls through to the unbounded stopgap, same
// as today, until a future phase extends this. Early-stopping for this shape does
// not actually need vibuilder to route it specially beyond using the newest-first
// per-leaf lookup below (see BuildSourceBounded's own call site) — the executor's
// viEvalNodes/viEvalOR (metrics_trace.go) already merges per-leaf newest-first
// results via ViUnionNewestFirst at query-evaluation time, so this check exists
// purely to decide which lookup function (newest-first vs. ordinary) to call per
// leaf here.
func isFlatORQuery(nodes []vm.RangeNode) bool {
	if len(nodes) != 1 || !nodes[0].IsOR {
		return false
	}
	children := nodes[0].Children
	if len(children) < 2 {
		return false
	}
	for i := range children {
		if len(children[i].Children) > 0 {
			return false
		}
	}
	return true
}

// collectLeaves flattens the predicate tree to its leaf nodes (those naming a
// column). Composite AND/OR nodes are descended into; the boolean combination is
// re-applied by the executor's viEvalNodes walk, so here we only need the set of
// columns + their leaf predicates. Duplicate columns are kept separate: each leaf
// carries its own predicate and the source appends per (col, type).
//
// issue #206: idx numbers every leaf SLOT (a node with no Children) in DFS pre-order,
// incrementing once per slot visited regardless of whether that slot's Column is empty --
// this MUST be the exact same rule and traversal order the executor's viEvalNode uses at
// eval time (its own leafIdx counter walks node.Children the same way, incrementing once
// per len(Children)==0 node before checking Column), so a leaf's idx here always matches
// the idx viEvalNode assigns it when evaluating an independently-recompiled copy of the
// identical query text. Skipping the counter increment for an empty-Column node would
// desync the two numbering rules the moment such a node is followed by a real leaf.
//
// task #213 (CRITICAL regression fix): when a leaf is the scoped `!=` rewrite's
// RequirePresent leaf (n.RequirePresent && n.NeqPairedRange — see RangeNode.NeqPairedRange's
// own doc comment), the very next element of ns (the SAME slice, one level of nesting) is
// ALWAYS the value-bearing range-OR composite extractNeqNode/extractNeqNumericNode emitted
// alongside it in the SAME return statement — never reordered, since nothing between here and
// there ever re-sorts a Children/Nodes slice. neqRangeSiblingLeaves both validates that shape
// (rather than assuming any 2-element adjacency is automatically this rewrite's pair) and
// predicts the DFS leaf-slot indices its two children will be assigned: idx has already been
// incremented past the RequirePresent leaf itself, and a composite node never consumes a leaf
// slot on its own (only genuine leaves, i.e. len(Children)==0, do) — so the sibling's own two
// leaf children, visited on the very next loop iteration below, are guaranteed to land at idx
// and idx+1 respectively.
func collectLeaves(nodes []vm.RangeNode) []leaf {
	var out []leaf
	idx := 0
	var walk func(ns []vm.RangeNode)
	walk = func(ns []vm.RangeNode) {
		for i := range ns {
			n := &ns[i]
			if len(n.Children) > 0 {
				walk(n.Children)
				continue
			}
			leafIdx := idx
			idx++
			if n.Column == "" {
				continue
			}
			l := leaf{node: n, col: n.Column, idx: leafIdx}
			if n.RequirePresent && n.NeqPairedRange && i+1 < len(ns) {
				if p0, p1, ok := neqRangeSiblingLeaves(&ns[i+1], n.Column, idx); ok {
					l.pairedLeafIdxs = []int{p0, p1}
				}
			}
			out = append(out, l)
		}
	}
	walk(nodes)
	return out
}

// neqRangeSiblingLeaves validates that sib is EXACTLY the scoped `!=` rewrite's
// value-bearing range-OR composite (extractNeqNode/extractNeqNumericNode's own
// construction: an OR of exactly two direct leaf children, both on col) and, if so, returns
// the DFS leaf-slot indices its two children will be assigned: nextIdx and nextIdx+1 — see
// collectLeaves' own doc comment for why this prediction is safe. Returns ok=false for any
// other shape (defensive: a future refactor changing this rewrite's output shape degrades to
// "not paired" rather than mis-recording indices, since the caller never marks a pairing it
// cannot verify).
func neqRangeSiblingLeaves(sib *vm.RangeNode, col string, nextIdx int) (p0, p1 int, ok bool) {
	if !sib.IsOR || len(sib.Children) != 2 {
		return 0, 0, false
	}
	c0, c1 := &sib.Children[0], &sib.Children[1]
	if len(c0.Children) != 0 || len(c1.Children) != 0 {
		return 0, 0, false
	}
	if c0.Column != col || c1.Column != col {
		return 0, 0, false
	}
	return nextIdx, nextIdx + 1, true
}

// buildPredicate turns a leaf RangeNode into a valueindex.Predicate plus the
// column type it operates on. Returns ok=false when the leaf cannot be expressed
// against the value index (present-only or an unsupported value type), in which
// case the caller leaves the column uncovered.
func buildPredicate(l *leaf) (valueindex.Predicate, modules_shared.ColumnType, bool) {
	n := l.node
	switch {
	case len(n.Values) > 0:
		// Equality / point lookup. Multiple values are OR'd; the value index
		// supports a single equality predicate per file pass, so we only build the
		// index path when there is exactly one value. Multi-value OR is left to the
		// block scan (the executor would otherwise need per-value passes).
		if len(n.Values) != 1 {
			return nil, 0, false
		}
		colType, val, ok := valueAsColType(l.col, n.Values[0])
		if !ok {
			return nil, 0, false
		}
		pred, err := valueindex.NewEqPredicate(colType, val)
		if err != nil {
			return nil, 0, false
		}
		return pred, colType, true

	case n.Min != nil || n.Max != nil:
		return buildRangePredicate(n)

	case n.Pattern != "":
		pred, err := valueindex.NewRegexPredicate(modules_shared.ColumnTypeString, n.Pattern)
		if err != nil {
			return nil, 0, false
		}
		return pred, modules_shared.ColumnTypeString, true

	default:
		// RequirePresent or empty leaf: existence-only, not a value predicate.
		return nil, 0, false
	}
}

// LeafIndexable reports whether a single leaf RangeNode has a shape the value index can
// represent at all (single-value equality, range, or regex — see buildPredicate's own switch),
// with NO discovery/download I/O and independent of whether any value-index files currently
// exist for it (issue #487, T5b). It is the exact same decision buildPredicate makes for real
// index-source construction, exposed so a caller outside this package (blockpack's public
// AllLeavesIndexable, queryplan.AllLeavesIndexable) can determine per-leaf shape resolvability
// ahead of time — e.g. to require ALL of a program's leaves to be indexable, not just the "at
// least one" BuildSource itself qualifies on — without re-deriving buildPredicate's rules a
// second time and risking drift between the two.
//
// SPEC-VB-3, NOTE-VI-085. col must be set to n.Column (task #203): buildPredicate's
// equality-leaf branch resolves valueAsColType's column-name-aware override
// (dedicatedNumericColumnTypes) from leaf.col, not from n directly — an empty col here
// would silently skip that override and drift from what buildPredicate actually decides
// for the SAME leaf when driven through collectLeaves (which always sets col: n.Column),
// violating this function's own "exact same decision" guarantee.
func LeafIndexable(n *vm.RangeNode) bool {
	if n == nil {
		return false
	}
	_, _, ok := buildPredicate(&leaf{node: n, col: n.Column})
	return ok
}

// LeafColumnInfo describes one leaf's column name, its resolved value-index column
// type (when Indexable), and whether its predicate shape is representable against
// the value index at all (LeafIndexable's exact per-leaf decision, reused verbatim
// here — never independently re-derived). ColType is the zero modules_shared.ColumnType
// when Indexable is false (buildPredicate never resolves a type for a shape it
// rejects) or for a match-all leaf (no predicate to type-check against).
type LeafColumnInfo struct {
	Column    string
	ColType   modules_shared.ColumnType
	Indexable bool
}

// LeafColumns enumerates every leaf's {Column, ColType, Indexable} in prog's predicate
// tree, one entry per LEAF occurrence — NOT deduplicated by column name. A column
// referenced by multiple leaves (e.g. two comparisons against the same attribute)
// produces one entry per leaf, mirroring collectLeaves' own per-leaf granularity, since
// that is exactly the leaf set LeafIndexable/buildPredicate themselves operate over.
// Callers needing a per-query, per-column decision (e.g. #496's usage-recording hook,
// which must count "one distinct query referenced column X" rather than "one leaf
// referenced column X") must de-duplicate by Column themselves.
//
// ColType is exposed (not just the Indexable bool) because #496's usage registry keys
// entries by (Tenant, ColumnHash, ColumnType) — a caller recording a use needs the same
// resolved type buildPredicate uses, not just a shape verdict.
//
// A program with Nodes empty and Columns populated returns one entry per listed column
// with Indexable=true and a zero ColType (mirrors AllLeavesIndexable's own handling of
// this shape). Task #211 (NOTE-492 addendum): in practice, per the real compiler, this
// shape is ALWAYS a compile-time decline (task #210's zero-node AND/OR guard, a standalone
// `!~`, etc.), never a genuine `{} | rate()` match-all — that compiles with
// prog.Predicates == nil entirely, handled by the guard above. This function still
// reports Indexable=true for every listed column regardless, since its own caller (#496's
// usage-recording hook) tracks "this query referenced column X" for backfill-triggering
// purposes independent of whether BuildSource/BuildSourceBounded can actually resolve the
// query via the value index — a deliberately different, narrower question than "can this
// be answered from the index," which is unaffected by BuildSource's own decline for this
// shape. A program referencing nothing at all (no Nodes, no Columns) returns nil.
//
// SPEC-VB-5.
func LeafColumns(prog *vm.Program) []LeafColumnInfo {
	if prog == nil || prog.Predicates == nil {
		return nil
	}
	preds := prog.Predicates
	if len(preds.Nodes) == 0 && len(preds.Columns) == 0 {
		return nil
	}
	if len(preds.Nodes) == 0 {
		out := make([]LeafColumnInfo, len(preds.Columns))
		for i, c := range preds.Columns {
			out[i] = LeafColumnInfo{Column: c, Indexable: true}
		}
		return out
	}
	leaves := collectLeaves(preds.Nodes)
	out := make([]LeafColumnInfo, len(leaves))
	for i := range leaves {
		_, colType, ok := buildPredicate(&leaves[i])
		out[i] = LeafColumnInfo{Column: leaves[i].col, ColType: colType, Indexable: ok}
	}
	return out
}

// buildRangePredicate builds a between/range predicate from a leaf's Min/Max
// bounds. A two-sided bound becomes a between predicate; a one-sided bound becomes
// a range predicate with the matching operator.
//
// task #204: each bound's exact comparison operator (GT vs GTE, LT vs LTE — derived
// from MinInclusive/MaxInclusive) is resolved and threaded down to
// valueAsRangeColType/intOrDedicatedColType BEFORE the predicate is built, because for
// the two millisecond-truncated dedicated time columns (span:start/span:duration) the
// operator itself determines whether the comparison is decidable at all
// (decidableTimeBucketThreshold's doc comment has the full derivation). Every other
// column/value shape is unaffected — op is simply unused there, identical behavior to
// before this fix.
func buildRangePredicate(n *vm.RangeNode) (valueindex.Predicate, modules_shared.ColumnType, bool) {
	switch {
	// SPEC-VB-7 (task #206 correction): DEAD CODE for every real TraceQL query today.
	// traceql_compiler's extractTraceQLNodes always decomposes an AND of two range bounds on
	// the same column into two SEPARATE leaf RangeNodes (one Min-only, one Max-only) — never a
	// single node with both Min and Max set. Kept (not deleted) as defensive coverage for a
	// combined-bound shape no current compiler path produces; untested (no unit or end-to-end
	// test exercises it) — see SPEC-VB-7's own "Reachability" note for the full writeup,
	// including a correction to this branch's own decidable-alignment description.
	case n.Min != nil && n.Max != nil:
		// between never carries an explicit valueindex.Op — NewBetweenPredicate is
		// always inclusive-inclusive — so only the timeCompareOp (used for the
		// dedicated-time-column decidability gate below) is needed here.
		minTimeOp := timeOpGT
		if n.MinInclusive {
			minTimeOp = timeOpGTE
		}
		maxTimeOp := timeOpLT
		if n.MaxInclusive {
			maxTimeOp = timeOpLTE
		}

		colType, lo, ok := valueAsRangeColType(n.Column, *n.Min, minTimeOp)
		if !ok {
			return nil, 0, false
		}
		hiColType, hi, ok := valueAsRangeColType(n.Column, *n.Max, maxTimeOp)
		if !ok {
			return nil, 0, false
		}

		if override, isTime := dedicatedNumericColumnTypes[n.Column]; isTime &&
			override.truncateMillis &&
			colType == modules_shared.ColumnTypeUint64 &&
			hiColType == modules_shared.ColumnTypeUint64 {
			// NewBetweenPredicate has no concept of an exclusive bound (it is always
			// inclusive-inclusive) — normalize each already-decidable bucket bound into
			// that shape (task #204; betweenTimeBucketBounds' own doc explains the +1/-1
			// bucket-step adjustment).
			loBucket, hiBucket, normOK := betweenTimeBucketBounds(
				lo.(uint64),
				minTimeOp,
				hi.(uint64),
				maxTimeOp,
			) //nolint:forcetypeassert // guarded by colType==Uint64 check above
			if !normOK {
				return nil, 0, false
			}
			lo, hi = loBucket, hiBucket
		}

		pred, err := valueindex.NewBetweenPredicate(colType, lo, hi)
		if err != nil {
			return nil, 0, false
		}
		return pred, colType, true

	case n.Min != nil:
		op, timeOp := valueindex.OpGT, timeOpGT
		if n.MinInclusive {
			op, timeOp = valueindex.OpGTE, timeOpGTE
		}
		colType, lo, ok := valueAsRangeColType(n.Column, *n.Min, timeOp)
		if !ok {
			return nil, 0, false
		}
		pred, err := valueindex.NewRangePredicate(colType, lo, op)
		if err != nil {
			return nil, 0, false
		}
		return pred, colType, true

	default: // n.Max != nil
		op, timeOp := valueindex.OpLT, timeOpLT
		if n.MaxInclusive {
			op, timeOp = valueindex.OpLTE, timeOpLTE
		}
		colType, hi, ok := valueAsRangeColType(n.Column, *n.Max, timeOp)
		if !ok {
			return nil, 0, false
		}
		pred, err := valueindex.NewRangePredicate(colType, hi, op)
		if err != nil {
			return nil, 0, false
		}
		return pred, colType, true
	}
}

// betweenTimeBucketBounds converts a decidable Min/Max bucket pair — each already
// resolved by decidableTimeBucketThreshold under its own operator — into the
// inclusive-inclusive [lo, hi] shape valueindex.NewBetweenPredicate requires (it has no
// concept of an exclusive bound). A GT lo-bound is bumped up one bucket step (B > q is
// exactly equivalent to B >= q+1 for integer buckets); an LT hi-bound is bumped down one
// bucket step (B < q is exactly equivalent to B <= q-1). Returns ok=false only at the
// uint64 domain's extreme edges, where the adjustment would overflow/underflow — values
// no real span start/duration could ever reach anyway, so declining there costs nothing.
//
// If the resulting lo > hi, the range is genuinely empty (e.g. a contradictory
// `1.6ms <= duration <= 1.4ms`) — NewBetweenPredicate(lo, hi) with lo > hi already
// matches nothing (Match requires v>=lo AND v<=hi, never simultaneously true), so this
// is correctly representable as a real "definitely zero matches" answer, not a decline.
func betweenTimeBucketBounds(
	loBucket uint64, minOp timeCompareOp,
	hiBucket uint64, maxOp timeCompareOp,
) (lo, hi uint64, ok bool) {
	lo = loBucket
	if minOp == timeOpGT {
		if loBucket == math.MaxUint64 {
			return 0, 0, false
		}
		lo = loBucket + 1
	}
	hi = hiBucket
	if maxOp == timeOpLT {
		if hiBucket == 0 {
			return 0, 0, false
		}
		hi = hiBucket - 1
	}
	return lo, hi, true
}

// timeCompareOp identifies the exact comparison a query literal will be evaluated
// with, threaded down from buildPredicate/buildRangePredicate specifically so
// intOrDedicatedColType can apply decidableTimeBucketThreshold's op-aware
// millisecond-granularity decidability gate (task #204). It mirrors valueindex.Op
// (OpGT/OpGTE/OpLT/OpLTE) for range bounds, plus a fifth value, timeOpEQ, for the
// point-value/equality leaf shape — a shape valueindex.Op itself has no member for,
// since equality goes through NewEqPredicate, never NewRangePredicate.
type timeCompareOp uint8

const (
	timeOpGT timeCompareOp = iota
	timeOpGTE
	timeOpLT
	timeOpLTE
	timeOpEQ
)

// decidableTimeBucketThreshold is the exact decidability rule for comparing a raw-
// nanosecond query literal against a millisecond-floor-truncated dedicated time column
// (span:start/span:duration, NOTE-VI-027) via the value index — derived for task #204
// (CRITICAL: #203's own millisecond-truncation fix produced wrong answers at bucket
// boundaries in BOTH directions — a silent false negative AND a silent false positive,
// found independently by two reviewers with real reproduction tests. See NOTE-VI-107
// for the corrected spec writeup this function implements).
//
// THE PROBLEM: the write side stores only a bucket B = floor(real_ns / 1_000_000) for
// these two columns — the exact sub-millisecond real value is permanently, irreversibly
// lost (a deliberate ~1000x cardinality reduction, NOTE-VI-027). A stored bucket B
// therefore represents an UNKNOWN real nanosecond value somewhere in the half-open
// interval [B*1e6, (B+1)*1e6 - 1] — every integer nanosecond in that one millisecond is
// an equally possible real value. #203's fix floor-truncated the query threshold to the
// SAME bucket domain and reused the original comparison operator unchanged; that is only
// mathematically sound for two of five operators at exactly one alignment each — for
// every other (operator, threshold) combination it silently produces a wrong answer
// (see the two worked bug reports below).
//
// Given a query threshold T (raw nanoseconds, NEVER pre-truncated — truncating both
// sides and reusing the operator is exactly #203's bug), whether "real_value <op> T"
// holds for the unknown real value inside bucket B is one of three things:
//   - DEFINITELY TRUE  if EVERY real value in B's interval satisfies the comparison.
//   - DEFINITELY FALSE if NO real value in B's interval satisfies the comparison.
//   - UNDECIDABLE      if SOME real values in B's interval satisfy it and some don't —
//     the bucket alone cannot distinguish them, and the write side has already
//     discarded the information that would.
//
// Let Tq = T div 1_000_000 (the bucket T's own value would floor to) and
// Tr = T mod 1_000_000 (T's position within that bucket, 0..999_999).
//
// Per-operator derivation, worked from the interval above (B ranges over every bucket):
//
//	>=  (V >= T): true region is B*1e6 >= T; false region is (B+1)*1e6-1 < T. These two
//	    regions partition EVERY integer bucket with NO gap if and only if Tr == 0 (T
//	    sits exactly on a bucket's lower edge) — then true iff B >= Tq, false iff B < Tq.
//	    For Tr > 0, bucket B == Tq is always ambiguous (it contains values both < T and
//	    >= T) — UNDECIDABLE.
//
//	<   (V < T): the exact logical complement of >=, so it partitions cleanly under the
//	    IDENTICAL condition, Tr == 0 — true iff B < Tq, false iff B >= Tq. Same
//	    single-bucket (B == Tq) gap for Tr > 0.
//
//	>   (V > T): true region is B*1e6 > T, i.e. B >= Tq+1; false region is
//	    (B+1)*1e6-1 <= T. These partition cleanly with NO gap if and only if
//	    Tr == 999_999 (T sits exactly on a bucket's UPPER edge, one nanosecond below the
//	    next millisecond) — an extreme threshold essentially never authored by hand. For
//	    every OTHER Tr (0..999_998 — which includes every "round" millisecond-aligned
//	    threshold like "1ms" itself, Tr == 0) bucket B == Tq is ambiguous: it contains
//	    the single value B*1e6 (which FAILS V > T when Tr == 0) alongside values that
//	    pass. THIS IS EXACTLY THE TASK #204 FALSE-NEGATIVE BUG REPORT: `{duration > 1ms}`
//	    silently dropped a real 1.5ms span, because a genuinely-1.0ms span (fails the
//	    comparison) and a genuinely-1.5ms span (passes it) land in the very SAME bucket
//	    (B=1) for threshold T=1_000_000ns — the bucket cannot be resolved either way.
//	    UNDECIDABLE; must decline, not "fixed" by flooring both sides identically (that
//	    is precisely #203's bug).
//
//	<=  (V <= T): the exact logical complement of >, so it partitions cleanly under the
//	    IDENTICAL condition, Tr == 999_999. Same single-bucket (B == Tq) gap otherwise —
//	    this is exactly the shape of the task #204 FALSE-POSITIVE bug report:
//	    `{duration >= 1.6ms}` incorrectly matching a real 1.5ms span (equivalently framed
//	    as `<=`'s complement ambiguity at the same boundary).
//
//	==  (V == T): the true region is a single exact point, which a 1,000,000-wide bucket
//	    interval can never represent (unless the interval had width 1, which it never
//	    does here); the false region is every bucket B != Tq. There is NO value of Tr
//	    that eliminates the B == Tq gap for equality — it is UNDECIDABLE unconditionally,
//	    for every possible T. Whether some span in bucket Tq happens to have real value
//	    exactly T can never be determined from the bucket alone, regardless of alignment.
//
// Truth table (Tr = T mod 1_000_000; "decidable" means every bucket resolves cleanly —
// gap-free — and, when decidable, comparing the ordinary op directly against Tq is
// EXACTLY correct with no further adjustment):
//
//	op   | decidable iff  | decidable comparison | practical frequency
//	---- | -------------- | --------------------- | --------------------------------
//	>=   | Tr == 0        | B >= Tq               | common (round-ms thresholds)
//	<    | Tr == 0        | B <  Tq               | common (round-ms thresholds)
//	>    | Tr == 999_999  | B >  Tq               | vanishingly rare in practice
//	<=   | Tr == 999_999  | B <= Tq               | vanishingly rare in practice
//	==   | never          | (always decline)      | never
//
// Rounding direction cannot fix this in general — adjusting which edge is "inclusive"
// only swaps which operator becomes decidable at Tr==0 vs Tr==999_999; it never removes
// the single-bucket gap for the OTHER operators. This is a genuine, permanent
// information loss from the write-side truncation (NOTE-VI-027), not a rounding bug.
// Per this whole session's index-authority philosophy (NOTE-VI-096, issue #474/#481:
// "never give a wrong answer, decline instead of guessing"), the only correct behavior
// for an undecidable (op, T) pair is to report this leaf unindexable (ok=false) so the
// caller falls back to a full, correct block scan — the same "leave the column
// uncovered" convention LeafIndexable/buildPredicate already use for every other
// unsupported predicate shape.
func decidableTimeBucketThreshold(nanos uint64, op timeCompareOp) (bucket uint64, ok bool) {
	q := nanos / 1_000_000
	r := nanos % 1_000_000
	switch op {
	case timeOpGTE, timeOpLT:
		if r != 0 {
			return 0, false
		}
		return q, true
	case timeOpGT, timeOpLTE:
		if r != 999_999 {
			return 0, false
		}
		return q, true
	default: // timeOpEQ (or any unrecognized op): never decidable, see doc above.
		return 0, false
	}
}

// dedicatedColumnOverride is the value-index column type a dedicated/intrinsic column
// really uses on disk, plus whether a query literal for that column must be evaluated
// through the millisecond-decidability gate (NOTE-VI-107, decidableTimeBucketThreshold)
// before it can be compared against the column's stored canonical value — never a plain,
// unconditional floor-truncation of both sides (that was task #203's bug, corrected by
// task #204).
type dedicatedColumnOverride struct {
	colType        modules_shared.ColumnType
	truncateMillis bool
}

// dedicatedNumericColumnTypes maps a leaf's column NAME to the value-index column type
// (and, where relevant, unit) that column's real on-disk storage uses, when either
// diverges from valueAsColType's literal-type-based default (task #203, CRITICAL — TWO
// independent, stacked bugs, the second only discovered after fixing the first unmasked
// it).
//
// Bug 1 (type-bucket mismatch): valueAsColType historically mapped EVERY TraceQL
// Duration/Int literal to ColumnTypeInt64 unconditionally, with no awareness of which
// column the literal was being compared against. span:start and span:duration are real
// Uint64 columns on disk (internal/modules/blockio/writer/writer_block.go's
// feedSpanTiming/applySpanStart/applySpanDuration all call
// feedIntrinsicUint64/addPresent with shared.ColumnTypeUint64, never Int64 — confirmed
// against a real CreateBlock+Reader round trip, not assumed), so a `{duration > Xms}` or
// `{start > X}` leaf resolved against the Int64 value-index type-bucket directory, which
// never has any files for these columns — the query always found zero matches there,
// even when real, correctly-typed Uint64 VI files existed for the very same column right
// next to it. Because BuildSource's "Add even when empty" contract (NOTE-VI-036) treats
// a covered-but-empty column identically to "the index confirms zero real matches," this
// was a silent, permanent wrong-answer bug: never an error, never the real matches, for
// every value-index-backed duration/start comparison in production.
//
// Bug 2 (unit mismatch, found while mutation-verifying bug 1's fix against a REAL
// write+read round trip through tempo's Fetch path, not the type fix's own synthetic
// unit tests): span:start/span:duration's real on-disk VALUE-INDEX canonical value is
// millisecond-truncated at write time (root valueindex_extract.go's
// truncateTimeValueToMillis, NOTE-VI-027/issue #415 — a deliberate ~1000x cardinality
// reduction), but the TraceQL compiler always compiles a `duration`/`start`/`end`
// literal to raw NANOSECONDS (internal/vm's Duration/Int literal handling). Comparing a
// raw-nanosecond query threshold against a millisecond-scale stored value is off by
// 10^6 — for realistic sub-second durations this makes virtually every VI-backed
// duration/start range query silently return the wrong (almost always empty) answer.
// Bug 1's fix alone was not sufficient to make span:duration/span:start VI queries
// correct — fixing only the type-bucket mismatch would have "fixed" the reproduction
// test for the wrong reason (its fixture happened to use exact-millisecond nanosecond
// values) while leaving production duration queries just as wrong. Both bugs share the
// same two columns and the same fix location, so they are fixed together here rather
// than as two separate changes.
//
// Every other intrinsic/dedicated numeric column was audited against its real write-path
// type (writer_block.go, viusage/dedicated_columns.go) and found to already agree with
// the Int64 default: span:kind and span:status are genuinely Int64
// (feedSpanKind/feedSpanStatus), and every int-valued attribute (including #496's
// DefaultDedicatedColumns entries such as span.http.response.status_code) is stored as
// Int64 because OTLP's AnyValue always represents integer attribute values as int64 —
// there is no write path that stores an attribute as Uint64, and none of these columns
// go through truncateTimeValueToMillis (name-gated to span:start/span:end/span:duration
// only) either. span:end has NO real on-disk column at all (NOTE-399: synthesized from
// span:start + span:duration on read, only its range-index min/max is fed) — it is
// never written to the value index in any type bucket, so it carries no type- or
// unit-mismatch risk, though it remains subject to the separate, already-documented "no
// VI files ever written for this column" gap (NOTE-VI-036) unrelated to this fix.
var dedicatedNumericColumnTypes = map[string]dedicatedColumnOverride{ //nolint:gochecknoglobals
	modules_shared.SpanStartColumnName:    {colType: modules_shared.ColumnTypeUint64, truncateMillis: true},
	modules_shared.SpanDurationColumnName: {colType: modules_shared.ColumnTypeUint64, truncateMillis: true},
}

// valueAsColType maps a vm.Value to the value-index column type and the concrete
// Go value the predicate constructors expect, for a POINT-VALUE (equality) context.
// Returns ok=false for nil/bool/bytes values that the value-index predicate
// constructors do not accept directly.
//
// col is the leaf's column NAME (n.Column / leaf.col) — required so an Int/Duration
// literal resolves against the CORRECT real on-disk type for the specific column being
// queried, not a blind Int64 default (task #203; see dedicatedNumericColumnTypes' own
// doc comment for the full root-cause writeup).
//
// Equality against a millisecond-truncated dedicated time column (span:start/
// span:duration) is UNCONDITIONALLY undecidable (decidableTimeBucketThreshold's doc,
// task #204) — passing timeOpEQ here always resolves ok=false for those two columns,
// regardless of the literal's value.
func valueAsColType(col string, v vm.Value) (modules_shared.ColumnType, any, bool) {
	switch v.Type {
	case vm.TypeString:
		s, ok := v.Data.(string)
		if !ok {
			return 0, nil, false
		}
		return modules_shared.ColumnTypeString, s, true
	case vm.TypeInt, vm.TypeDuration:
		switch d := v.Data.(type) {
		case int64:
			return intOrDedicatedColType(col, d, timeOpEQ)
		case int:
			return intOrDedicatedColType(col, int64(d), timeOpEQ)
		default:
			return 0, nil, false
		}
	case vm.TypeFloat:
		f, ok := v.Data.(float64)
		if !ok {
			return 0, nil, false
		}
		return modules_shared.ColumnTypeFloat64, f, true
	default:
		return 0, nil, false
	}
}

// valueAsRangeColType mirrors valueAsColType but is used only for a RANGE BOUND
// (buildRangePredicate's Min/Max/between branches), where the caller already knows the
// exact comparison operator (op) that bound will be evaluated with. For every
// column/value shape except an Int/Duration literal against a millisecond-truncated
// dedicated time column this is identical to valueAsColType — op is simply unused. op
// only changes behavior for span:start/span:duration, where the operator determines
// millisecond-granularity decidability (decidableTimeBucketThreshold; task #204).
func valueAsRangeColType(col string, v vm.Value, op timeCompareOp) (modules_shared.ColumnType, any, bool) {
	switch d := v.Data.(type) {
	case int64:
		if v.Type == vm.TypeInt || v.Type == vm.TypeDuration {
			return intOrDedicatedColType(col, d, op)
		}
	case int:
		if v.Type == vm.TypeInt || v.Type == vm.TypeDuration {
			return intOrDedicatedColType(col, int64(d), op)
		}
	}
	return valueAsColType(col, v)
}

// intOrDedicatedColType resolves an Int/Duration literal's value-index column type and
// value: the column-specific override from dedicatedNumericColumnTypes when col has one,
// otherwise the ordinary ColumnTypeInt64 default (unchanged behavior for every column
// not listed there). A negative literal against a Uint64-backed column is left
// unindexable (ok=false) rather than silently wrapping around via uint64(d) — the real
// column can never hold a negative value, so no real row could ever satisfy such a
// comparison against a wrapped, huge threshold; the caller falls back to a full block
// scan instead of risking a wrong predicate.
//
// op (task #204, correcting NOTE-VI-107's original — buggy — "just floor both sides"
// approach): when the override requests truncateMillis, d (a raw-nanosecond TraceQL
// literal) is passed to decidableTimeBucketThreshold together with the caller's own
// comparison operator. That function is the single source of truth for whether this
// specific (op, d) pair can be answered correctly at millisecond bucket granularity at
// all — see its doc comment for the full per-operator derivation and truth table. A
// decidable pair resolves to the exact bucket-domain threshold to compare the STORED
// value against (using the SAME operator, unchanged); an undecidable pair resolves
// ok=false, and this leaf is left unindexable rather than risk a silently wrong answer.
func intOrDedicatedColType(col string, d int64, op timeCompareOp) (modules_shared.ColumnType, any, bool) {
	override, ok := dedicatedNumericColumnTypes[col]
	if !ok || override.colType != modules_shared.ColumnTypeUint64 {
		return modules_shared.ColumnTypeInt64, d, true
	}
	if d < 0 {
		return 0, nil, false
	}
	v := uint64(d) //nolint:gosec // d >= 0 checked above
	if !override.truncateMillis {
		return modules_shared.ColumnTypeUint64, v, true
	}
	bucket, decidable := decidableTimeBucketThreshold(v, op)
	if !decidable {
		return 0, nil, false
	}
	return modules_shared.ColumnTypeUint64, bucket, true
}

// SPEC-VB-2: lookupColumn discovers and ranged-queries the value-index files for one column,
// returning the matched spans as executor.VILookupResult. An empty (non-nil-error)
// return means the column is indexed but had no matches — the caller Adds it as
// covered-but-empty.
//
// Ranged read path (B-5, issue #488): rewired from whole-file downloadAll +
// valueindex.QueryBucketFiles onto per-key valueindex.QueryBucketFileRanged, so a
// column whose predicate/time-window prunes most of a value-index file's blocks
// never pays for the pruned blocks' bytes over the network — only the footer,
// directory, and surviving block bodies are fetched.
func lookupColumn(
	ctx context.Context,
	disc FileDiscoverer,
	store FileStore,
	col string,
	colType modules_shared.ColumnType,
	pred valueindex.Predicate,
	timeRange *[2]uint64,
) ([]modules_executor.VILookupResult, int, int64, error) {
	colHash := valueindex.ColHash(col)
	colTypeName := valueindex.ColTypeName(colType)
	if colTypeName == "" {
		return nil, 0, 0, nil
	}
	keys, err := disc.FilesForTimeRange(ctx, colHash, colTypeName, timeRange[0], timeRange[1])
	if err != nil {
		return nil, 0, 0, fmt.Errorf("vibuilder: discover %s: %w", col, err)
	}
	lrs, filesRead, bytesRead, err := queryKeysRanged(ctx, store, keys, pred, timeRange)
	if err != nil {
		return nil, 0, 0, fmt.Errorf("vibuilder: query %s: %w", col, err)
	}
	return toVILookupResults(lrs), filesRead, bytesRead, nil
}

// lookupColumnNewestFirst mirrors lookupColumn but discovers via
// FileDiscovererNewestFirst.FilesForTimeRangeNewestFirst and resolves via
// queryKeysRangedNewestFirst instead of the ascending pair, so the returned entries are an
// early-stopped, newest-first prefix instead of the full ascending set (Phase 2,
// plan-scan-fallback.md). limit <= 0 means unbounded (identical result to lookupColumn,
// modulo ordering).
func lookupColumnNewestFirst(
	ctx context.Context,
	disc FileDiscovererNewestFirst,
	store FileStore,
	col string,
	colType modules_shared.ColumnType,
	pred valueindex.Predicate,
	timeRange *[2]uint64,
	limit int,
) ([]modules_executor.VILookupResult, int, int64, error) {
	colHash := valueindex.ColHash(col)
	colTypeName := valueindex.ColTypeName(colType)
	if colTypeName == "" {
		return nil, 0, 0, nil
	}
	keys, err := disc.FilesForTimeRangeNewestFirst(ctx, colHash, colTypeName, timeRange[0], timeRange[1])
	if err != nil {
		return nil, 0, 0, fmt.Errorf("vibuilder: discover %s: %w", col, err)
	}
	lrs, filesRead, bytesRead, err := queryKeysRangedNewestFirst(ctx, store, keys, pred, timeRange, limit)
	if err != nil {
		return nil, 0, 0, fmt.Errorf("vibuilder: query %s: %w", col, err)
	}
	return toVILookupResults(lrs), filesRead, bytesRead, nil
}

// SPEC-VB-2: lookupColumnAll discovers and ranged-queries every value-index file for a column
// (across all of its candidate type buckets) and returns all entries — used by the
// match-all path. It tries each plausible type bucket because a column name can
// appear under more than one type prefix.
func lookupColumnAll(
	ctx context.Context,
	disc FileDiscoverer,
	store FileStore,
	col string,
	timeRange *[2]uint64,
) ([]modules_executor.VILookupResult, modules_shared.ColumnType, int, int64, error) {
	colHash := valueindex.ColHash(col)
	buckets := allTypeBuckets()
	type bucketResult struct {
		results    []modules_executor.VILookupResult
		filesRead  int
		bytesRead  int64
		hasResults bool
	}
	slots := make([]bucketResult, len(buckets))
	g, gctx := errgroup.WithContext(ctx)
	// Bounded by leafConcurrency, not len(buckets) (7) — each bucket goroutine
	// itself calls queryKeysRanged, which fans out up to downloadConcurrency more
	// CPU-bound work (decode+sort), so the effective worst case here is
	// leafConcurrency * downloadConcurrency, matching BuildSource's leaf loop.
	g.SetLimit(leafConcurrency)
	for i, colType := range buckets {
		g.Go(func() error {
			colTypeName := valueindex.ColTypeName(colType)
			keys, err := disc.FilesForTimeRange(gctx, colHash, colTypeName, timeRange[0], timeRange[1])
			if err != nil {
				return fmt.Errorf("vibuilder: discover-all %s: %w", col, err)
			}
			if len(keys) == 0 {
				return nil
			}
			// A nil predicate matches every entry (QueryBucketFileRanged treats nil as
			// match-all, same as the underlying Predicate contract), so the universe of
			// indexed spans for this column is returned.
			lrs, filesRead, bytesRead, err := queryKeysRanged(gctx, store, keys, nil, timeRange)
			if err != nil {
				return fmt.Errorf("vibuilder: query-all %s: %w", col, err)
			}
			slots[i] = bucketResult{
				results:    toVILookupResults(lrs),
				filesRead:  filesRead,
				bytesRead:  bytesRead,
				hasResults: len(lrs) > 0,
			}
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return nil, 0, 0, 0, err
	}
	var all []modules_executor.VILookupResult
	var firstType modules_shared.ColumnType
	var totalFiles int
	var totalBytes int64
	firstSet := false
	for i, s := range slots {
		totalFiles += s.filesRead
		totalBytes += s.bytesRead
		if !s.hasResults {
			continue
		}
		if !firstSet {
			// Deterministic: first bucket in allTypeBuckets() order with
			// results, matching original serial semantics exactly — not
			// goroutine completion order.
			firstType = buckets[i]
			firstSet = true
		}
		all = append(all, s.results...)
	}
	return all, firstType, totalFiles, totalBytes, nil
}

// allTypeBuckets lists the distinct value-index type-bucket representatives, one
// per ColTypeName output. Used by the match-all path to probe every prefix a
// column might live under.
func allTypeBuckets() []modules_shared.ColumnType {
	return []modules_shared.ColumnType{
		modules_shared.ColumnTypeString,
		modules_shared.ColumnTypeInt64,
		modules_shared.ColumnTypeUint64,
		modules_shared.ColumnTypeFloat64,
		modules_shared.ColumnTypeBool,
		modules_shared.ColumnTypeBytes,
		modules_shared.ColumnTypeUUID,
	}
}

// SPEC-VB-1: storeRangedSource adapts vibuilder's FileStore (keyed by string) to
// valueindex.RangedSource (no key parameter) for one specific key, so
// QueryBucketFileRanged can be handed a plain RangedSource without valueindex ever
// needing to know about vibuilder's FileStore shape (B-5, issue #488).
//
// Size is cached after its first successful call — a pointer receiver, not a plain
// immutable value — because both QueryBucketFileRanged's own footer read and
// queryKeysRanged's separate FilesRead/BytesRead stats bookkeeping (issue #465)
// need the object's size, and an object-storage backend's Size is typically its own
// network round trip (an S3 HEAD). Answering it twice per file would double that
// specific cost, working against the very I/O reduction #488 exists to deliver.
type storeRangedSource struct {
	store FileStore
	key   string
	size  int64
	sized bool
}

func (s *storeRangedSource) Size() (int64, error) {
	if !s.sized {
		sz, err := s.store.Size(s.key)
		if err != nil {
			return 0, err
		}
		s.size = sz
		s.sized = true
	}
	return s.size, nil
}

func (s *storeRangedSource) ReadAt(p []byte, off int64) (int, error) {
	return s.store.ReadAt(s.key, p, off)
}

// SPEC-VB-2: queryKeysRanged runs valueindex.QueryBucketFileRanged against every key in
// parallel, bounded by downloadConcurrency (mirroring the pre-ranged-read
// downloadAll's fan-out exactly), accumulating matched results and I/O stats.
//
// A per-key ErrFileNotFound (from either Size or ReadAt, surfaced through
// QueryBucketFileRanged's wrapped error chain via errors.Is) is treated as an empty
// miss and skipped: the compactor's write-then-delete cycle plus a stale listing
// cache can name a key that no longer exists, and an absent file holds no postings,
// so dropping it cannot under-count (NOTE-VI-041, issue #399 point 5). Any other
// per-key error aborts the whole query so the caller falls back to a full scan.
//
// filesRead/bytesRead are observability counters (issue #465): bytesRead is each
// surviving key's full object size (via storeRangedSource's cached Size), matching
// the pre-ranged-read contract exactly — not the (smaller) number of bytes actually
// transferred by QueryBucketFileRanged's ReadAt calls beneath. Reducing the latter
// without changing what gets reported here is the whole point of the ranged read
// path.
func queryKeysRanged(
	ctx context.Context,
	store FileStore,
	keys []string,
	pred valueindex.Predicate,
	timeRange *[2]uint64,
) ([]valueindex.LookupResult, int, int64, error) {
	if len(keys) == 0 {
		return nil, 0, 0, nil
	}
	type keySlot struct {
		results []valueindex.LookupResult
		size    int64
		keep    bool // false for a skipped (404) key; zero-value default
	}
	slots := make([]keySlot, len(keys))
	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(downloadConcurrency)
	for i, key := range keys {
		g.Go(func() error {
			if gctx.Err() != nil {
				// Another key already hit a real (non-404) error; do not start new
				// work, but do not report a spurious error either — the goroutine
				// that found the real error reports it.
				return nil //nolint:nilerr // intentional: gctx.Err() belongs to a sibling goroutine's failure, not this one's
			}
			src := &storeRangedSource{store: store, key: key}
			results, err := valueindex.QueryBucketFileRanged(gctx, src, pred, timeRange)
			if err != nil {
				if errors.Is(err, ErrFileNotFound) {
					// Retention/compaction deleted this file out from under a
					// stale listing — treat as an empty miss and skip it.
					return nil
				}
				return fmt.Errorf("vibuilder: query %s: %w", key, err)
			}
			// Cached by src's own Size() call above (via QueryBucketFileRanged's
			// footer read) — this does not issue a second network round trip.
			size, serr := src.Size()
			if serr != nil {
				if errors.Is(serr, ErrFileNotFound) {
					return nil
				}
				return fmt.Errorf("vibuilder: size %s: %w", key, serr)
			}
			slots[i] = keySlot{results: results, size: size, keep: true}
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return nil, 0, 0, err
	}
	var out []valueindex.LookupResult
	var filesRead int
	var bytesRead int64
	for _, s := range slots {
		if !s.keep {
			continue
		}
		filesRead++
		bytesRead += s.size
		out = append(out, s.results...)
	}
	return out, filesRead, bytesRead, nil
}

// queryKeysRangedNewestFirst mirrors queryKeysRanged but processes keys (already
// newest-first ordered by the caller, via FileDiscovererNewestFirst) in newest-first
// BATCHES of downloadConcurrency instead of one all-at-once fan-out. Within a batch, keys
// are queried concurrently exactly as queryKeysRanged does today (preserving its existing
// latency-hiding benefit); only the BETWEEN-batch behavior is new: sequential, and
// early-stoppable once len(out) >= limit after a batch completes. limit is also threaded
// per-key into valueindex.QueryBucketFileRangedNewestFirst, so a single wide file can
// itself satisfy the whole limit without waiting for its batch siblings to finish
// downloading (each key still gets the FULL limit, an intentional over-fetch-safe MVP
// choice per plan-scan-fallback.md Phase 2 -- tightening this to the REMAINING limit
// across a batch's siblings is a documented follow-up optimization, not required for
// correctness: it can only return MORE matches per key than strictly needed, never fewer
// or wrong ones).
//
// limit <= 0 means unbounded: every key is queried across every batch, and the
// early-stop check below never fires (byte-identical exhaustive resolution to
// queryKeysRanged, modulo the newest-first per-key ordering already established by the
// caller's key discovery order).
func queryKeysRangedNewestFirst(
	ctx context.Context,
	store FileStore,
	keys []string,
	pred valueindex.Predicate,
	timeRange *[2]uint64,
	limit int,
) ([]valueindex.LookupResult, int, int64, error) {
	if len(keys) == 0 {
		return nil, 0, 0, nil
	}

	var out []valueindex.LookupResult
	var filesRead int
	var bytesRead int64

	for batchStart := 0; batchStart < len(keys); batchStart += downloadConcurrency {
		batchEnd := batchStart + downloadConcurrency
		if batchEnd > len(keys) {
			batchEnd = len(keys)
		}
		batch := keys[batchStart:batchEnd]

		type keySlot struct {
			results []valueindex.LookupResult
			size    int64
			keep    bool
		}
		slots := make([]keySlot, len(batch))
		g, gctx := errgroup.WithContext(ctx)
		g.SetLimit(downloadConcurrency)
		for i, key := range batch {
			g.Go(func() error {
				if gctx.Err() != nil {
					return nil //nolint:nilerr // intentional: gctx.Err() belongs to a sibling goroutine's failure, not this one's
				}
				src := &storeRangedSource{store: store, key: key}
				results, err := valueindex.QueryBucketFileRangedNewestFirst(gctx, src, pred, timeRange, limit)
				if err != nil {
					if errors.Is(err, ErrFileNotFound) {
						// Retention/compaction deleted this file out from under a
						// stale listing — treat as an empty miss and skip it.
						return nil
					}
					return fmt.Errorf("vibuilder: query %s: %w", key, err)
				}
				size, serr := src.Size()
				if serr != nil {
					if errors.Is(serr, ErrFileNotFound) {
						return nil
					}
					return fmt.Errorf("vibuilder: size %s: %w", key, serr)
				}
				slots[i] = keySlot{results: results, size: size, keep: true}
				return nil
			})
		}
		if err := g.Wait(); err != nil {
			return nil, 0, 0, err
		}
		for _, s := range slots {
			if !s.keep {
				continue
			}
			filesRead++
			bytesRead += s.size
			out = append(out, s.results...)
		}
		if limit > 0 && len(out) >= limit {
			return out, filesRead, bytesRead, nil
		}
	}
	return out, filesRead, bytesRead, nil
}

// toVILookupResults maps valueindex.LookupResult to executor.VILookupResult,
// carrying the per-row addressing (BlockID, RowIdx) the search path needs.
func toVILookupResults(lrs []valueindex.LookupResult) []modules_executor.VILookupResult {
	if len(lrs) == 0 {
		return nil
	}
	out := make([]modules_executor.VILookupResult, len(lrs))
	for i := range lrs {
		out[i] = modules_executor.VILookupResult{
			SourceRef: lrs[i].SourceRef,
			TimeSec:   lrs[i].TimeSec,
			BlockID:   lrs[i].BlockID,
			BlockPage: lrs[i].BlockRef.PageNum,
			BlockLen:  lrs[i].BlockRef.LenPages,
			RowIdx:    lrs[i].RowIdx,
			TraceID:   lrs[i].TraceID,
			SpanID:    lrs[i].SpanID,
		}
	}
	return out
}
