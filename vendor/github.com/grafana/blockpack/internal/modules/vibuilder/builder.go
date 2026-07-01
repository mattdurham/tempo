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
	"io"

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
func BuildSource(
	ctx context.Context,
	disc FileDiscoverer,
	store FileStore,
	prog *vm.Program,
	minSec, maxSec uint64,
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
	leaves := collectLeaves(preds.Nodes)
	for i := range leaves {
		col := leaves[i].col
		pred, colType, ok := buildPredicate(&leaves[i])
		if !ok {
			// Unindexable predicate for this leaf — leave the column uncovered so
			// the executor falls back.
			continue
		}
		results, filesRead, bytesRead, err := lookupColumn(ctx, disc, store, col, colType, pred, timeRange)
		if err != nil {
			return nil, false, err
		}
		// Record the download I/O for this leaf so the querier can report it on its
		// OTel span (issue #465); a covered-but-empty column still counts the bytes
		// of any files we read deciding it was empty.
		src.RecordFileIO(filesRead, bytesRead)
		// Add even when empty: a covered-but-empty column is coverage, not
		// fallback (NOTE-VI-033).
		src.Add(col, colType, results)
		added = true
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
// against the value index (vector predicate, present-only, or an unsupported
// value type), in which case the caller leaves the column uncovered.
func buildPredicate(l *leaf) (valueindex.Predicate, modules_shared.ColumnType, bool) {
	n := l.node
	switch {
	case len(n.QueryVector) > 0:
		// Vector similarity is not a value-index predicate.
		return nil, 0, false

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

// lookupColumn discovers, downloads, and predicate-filters the value-index files
// for one column, returning the matched spans as executor.VILookupResult. An empty
// (non-nil-error) return means the column is indexed but had no matches — the
// caller Adds it as covered-but-empty.
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
	files, bytesRead, err := downloadAll(store, keys)
	if err != nil {
		return nil, 0, 0, err
	}
	lrs, err := valueindex.QueryBucketFiles(pred, timeRange, files...)
	if err != nil {
		return nil, 0, 0, fmt.Errorf("vibuilder: query %s: %w", col, err)
	}
	return toVILookupResults(lrs), len(files), bytesRead, nil
}

// lookupColumnAll discovers and downloads every value-index file for a column
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
	var all []modules_executor.VILookupResult
	var firstType modules_shared.ColumnType
	var totalFiles int
	var totalBytes int64
	for _, colType := range allTypeBuckets() {
		colTypeName := valueindex.ColTypeName(colType)
		keys, err := disc.FilesForTimeRange(ctx, colHash, colTypeName, timeRange[0], timeRange[1])
		if err != nil {
			return nil, 0, 0, 0, fmt.Errorf("vibuilder: discover-all %s: %w", col, err)
		}
		if len(keys) == 0 {
			continue
		}
		files, bytesRead, err := downloadAll(store, keys)
		if err != nil {
			return nil, 0, 0, 0, err
		}
		totalFiles += len(files)
		totalBytes += bytesRead
		// A nil predicate matches every entry (Reader.Lookup treats nil as
		// match-all), so the universe of indexed spans for this column is returned.
		lrs, err := valueindex.QueryBucketFiles(nil, timeRange, files...)
		if err != nil {
			return nil, 0, 0, 0, fmt.Errorf("vibuilder: query-all %s: %w", col, err)
		}
		if len(all) == 0 {
			firstType = colType
		}
		all = append(all, toVILookupResults(lrs)...)
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

// downloadAll reads every key fully into memory via store. A per-file read error
// aborts the whole build (the caller falls back to a full scan) rather than
// silently dropping a file, which would produce a wrong (under-counted) result —
// EXCEPT a not-found (ErrFileNotFound) file, which is skipped: the compactor's
// write-then-delete cycle plus a stale listing cache can name a key that no longer
// exists, and an absent file holds no postings, so dropping it cannot under-count
// (NOTE-VI-041, issue #399 point 5). It also returns the total bytes downloaded so
// the builder can record I/O stats (issue #465).
func downloadAll(store FileStore, keys []string) ([][]byte, int64, error) {
	if len(keys) == 0 {
		return nil, 0, nil
	}
	files := make([][]byte, 0, len(keys))
	var totalBytes int64
	for _, key := range keys {
		data, err := readWhole(store, key)
		if err != nil {
			if errors.Is(err, ErrFileNotFound) {
				// Retention/compaction deleted this file out from under a stale
				// listing — treat as an empty miss and skip it.
				continue
			}
			return nil, 0, fmt.Errorf("vibuilder: download %s: %w", key, err)
		}
		totalBytes += int64(len(data))
		files = append(files, data)
	}
	return files, totalBytes, nil
}

// readWhole reads the entire object at key into a single buffer. A not-found error
// from either Size or ReadAt is returned verbatim so downloadAll can recognize it
// via errors.Is(err, ErrFileNotFound) and skip the file.
func readWhole(store FileStore, key string) ([]byte, error) {
	size, err := store.Size(key)
	if err != nil {
		return nil, err
	}
	if size <= 0 {
		return nil, nil
	}
	buf := make([]byte, size)
	n, err := store.ReadAt(key, buf, 0)
	if err != nil && err != io.EOF {
		return nil, err
	}
	return buf[:n], nil
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
