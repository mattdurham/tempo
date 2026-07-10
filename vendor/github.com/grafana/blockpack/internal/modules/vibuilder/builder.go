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
//  3. Match-all queries ({} | rate()) enumerate every column the program touches
//     and Add all of their entries so AllResults can enumerate the span universe.
//
// A column whose leaf predicate cannot be expressed against the value index (e.g.
// a vector predicate, or an unindexable column type) is simply not Added, so the
// source reports no coverage for it and the caller falls back to a full block
// scan. This is the same fail-safe contract the executor already documents.
package vibuilder

import (
	"context"
	"errors"
	"fmt"
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
// usage-triggered columns mid-backfill: keyed by column name, nil (or a
// column simply absent from the map -- the common case for dedicated columns,
// which are never usage-tracked) means no gating, exactly today's behavior. A
// column present in watermarks whose CoversRange(minSec, maxSec) is false is
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

	// Collect the distinct (column, predicate) leaves the query needs. A match-all
	// query has no nodes but may list Columns; we Add every listed column so
	// AllResults can enumerate the universe.
	if preds == nil || (len(preds.Nodes) == 0 && len(preds.Columns) == 0) {
		// Truly nothing referenced (e.g. `{}`): nothing to discover. The caller
		// falls back; we cannot enumerate "all spans across all columns" without a
		// column list (the value index is partitioned per column).
		return nil, false, nil
	}

	added := false

	// Leaf predicates: each contributes a constrained per-column result set.
	// buildPredicate is pure CPU (no I/O) and stays synchronous; only the
	// downstream discovery+download+query work fans out.
	leaves := collectLeaves(preds.Nodes)
	type leafWork struct {
		pred    valueindex.Predicate
		col     string
		colType modules_shared.ColumnType
	}
	var work []leafWork
	for i := range leaves {
		pred, colType, ok := buildPredicate(&leaves[i])
		if !ok {
			// Unindexable predicate for this leaf — leave the column uncovered so
			// the executor falls back.
			continue
		}
		work = append(work, leafWork{col: leaves[i].col, colType: colType, pred: pred})
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
				if wm, ok := watermarks[w.col]; ok && !wm.CoversRange(minSec, maxSec) {
					return nil
				}
				// Add even when empty: a covered-but-empty column is coverage, not
				// fallback (NOTE-VI-033).
				src.Add(w.col, w.colType, results)
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

	// Match-all over an explicit column list (`{} | rate()` compiles with Columns
	// populated and no Nodes): Add every column's full entry set with an
	// always-true predicate so AllResults can enumerate spans.
	if len(preds.Nodes) == 0 && len(preds.Columns) > 0 {
		for _, col := range preds.Columns {
			results, colType, filesRead, bytesRead, err := lookupColumnAll(ctx, disc, store, col, timeRange)
			if err != nil {
				return nil, false, err
			}
			src.RecordFileIO(filesRead, bytesRead)
			// #496 R7: a match-all query (`{} | rate()`) over a partially-backfilled
			// column has the identical partial-coverage risk as a leaf predicate --
			// same gate as above, second (and last) src.Add call site in this file.
			if wm, ok := watermarks[col]; ok && !wm.CoversRange(minSec, maxSec) {
				continue
			}
			src.Add(col, colType, results)
			added = true
		}
	}

	if !added {
		return nil, false, nil
	}
	return src, true, nil
}

// leaf is a flattened leaf RangeNode: a column plus its predicate description.
type leaf struct {
	node *vm.RangeNode
	col  string
}

// collectLeaves flattens the predicate tree to its leaf nodes (those naming a
// column). Composite AND/OR nodes are descended into; the boolean combination is
// re-applied by the executor's viEvalNodes walk, so here we only need the set of
// columns + their leaf predicates. Duplicate columns are kept separate: each leaf
// carries its own predicate and the source appends per (col, type).
func collectLeaves(nodes []vm.RangeNode) []leaf {
	var out []leaf
	var walk func(ns []vm.RangeNode)
	walk = func(ns []vm.RangeNode) {
		for i := range ns {
			n := &ns[i]
			if len(n.Children) > 0 {
				walk(n.Children)
				continue
			}
			if n.Column == "" {
				continue
			}
			out = append(out, leaf{node: n, col: n.Column})
		}
	}
	walk(nodes)
	return out
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
		colType, val, ok := valueAsColType(n.Values[0])
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
// SPEC-VB-3, NOTE-VI-085.
func LeafIndexable(n *vm.RangeNode) bool {
	if n == nil {
		return false
	}
	_, _, ok := buildPredicate(&leaf{node: n})
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
// A match-all query (prog.Predicates.Nodes empty, Columns populated — e.g. `{} |
// rate()`) returns one entry per listed column with Indexable=true and a zero ColType
// (mirrors AllLeavesIndexable's own match-all handling: BuildSource's lookupColumnAll
// path never rejects a column's shape in this case, but a match-all leaf has no
// predicate to resolve a type from). A program referencing nothing at all (no Nodes,
// no Columns) returns nil.
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
func buildRangePredicate(n *vm.RangeNode) (valueindex.Predicate, modules_shared.ColumnType, bool) {
	switch {
	case n.Min != nil && n.Max != nil:
		colType, lo, ok := valueAsColType(*n.Min)
		if !ok {
			return nil, 0, false
		}
		_, hi, ok := valueAsColType(*n.Max)
		if !ok {
			return nil, 0, false
		}
		pred, err := valueindex.NewBetweenPredicate(colType, lo, hi)
		if err != nil {
			return nil, 0, false
		}
		return pred, colType, true

	case n.Min != nil:
		colType, lo, ok := valueAsColType(*n.Min)
		if !ok {
			return nil, 0, false
		}
		op := valueindex.OpGT
		if n.MinInclusive {
			op = valueindex.OpGTE
		}
		pred, err := valueindex.NewRangePredicate(colType, lo, op)
		if err != nil {
			return nil, 0, false
		}
		return pred, colType, true

	default: // n.Max != nil
		colType, hi, ok := valueAsColType(*n.Max)
		if !ok {
			return nil, 0, false
		}
		op := valueindex.OpLT
		if n.MaxInclusive {
			op = valueindex.OpLTE
		}
		pred, err := valueindex.NewRangePredicate(colType, hi, op)
		if err != nil {
			return nil, 0, false
		}
		return pred, colType, true
	}
}

// valueAsColType maps a vm.Value to the value-index column type and the concrete
// Go value the predicate constructors expect. Returns ok=false for nil/bool/bytes
// values that the value-index predicate constructors do not accept directly.
func valueAsColType(v vm.Value) (modules_shared.ColumnType, any, bool) {
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
			return modules_shared.ColumnTypeInt64, d, true
		case int:
			return modules_shared.ColumnTypeInt64, int64(d), true
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
