package valueindexcompactor

// traceindex_dispatch.go — Stage 3, traceindex.go wiring plan: compactor
// format-dispatch for TraceGroup files under the trace:id column directory.
//
// Finding 2: compactColumn's existing vbg2Magic Peek/purge loop runs BEFORE
// mergeLevel and deletes anything that doesn't match the BucketGroup magic
// header. TraceGroup files (valueindex.EncodeTraceGroups) carry no such
// magic, so the dispatch decision below MUST be made in compactColumn before
// that loop runs -- routing trace-index colDirs to mergeTraceLevel and
// skipping the magic-purge loop entirely -- not only inside mergeLevel,
// which would be too late.

import (
	"context"
	"fmt"
	"log/slog"
	"path"
	"sort"

	"github.com/grafana/blockpack/internal/modules/blockio/shared"
	"github.com/grafana/blockpack/internal/modules/valueindex"
)

// traceIDColHash is the precomputed column-directory hash for the trace:id
// column, used to detect trace-index colDirs in compactColumn's dispatch
// check. Computed once at package init: ColHash is a pure function of the
// fixed "trace:id" string.
var traceIDColHash = valueindex.ColHash(shared.TraceIDColumnName)

// isTraceIndexColDir reports whether colDir (shape ".../<colHash>/<colTypeName>",
// per buildWorkList) is the trace:id column directory.
func isTraceIndexColDir(colDir string) bool {
	return path.Base(path.Dir(colDir)) == traceIDColHash
}

// mergeTraceLevel reads all TraceGroup files at one level, merges + dedups +
// retention-filters them via valueindex.MergeTraceGroups, writes the merged
// output at level+1, then deletes only the inputs it successfully decoded and
// incorporated. Mirrors mergeLevel's write-then-delete crash-safety ordering.
//
// Scope boundary (plan.md Stage 3, brainstorm Key Decision): MergeTraceGroups
// is in-memory only for v1 -- no disk-streaming k-way-merge iterator for
// TraceGroup files, unlike the BucketGroup path's StreamCompactBucketFiles.
// TraceGroup files are expected to stay far smaller than BucketGroup files
// (compact per-trace pointers vs. full value postings); revisit only if
// post-deployment telemetry shows otherwise.
//
// A file that fails valueindex.DecodeTraceGroups is skipped, not treated as
// fatal -- the merge proceeds with the remaining valid files. Unlike a
// successfully-decoded file (always deleted once incorporated, even if every
// span it carried was later dropped by retention), a corrupt file is
// deliberately left in place rather than deleted: retrying can't fix a
// genuinely malformed payload, but deleting it would destroy the only copy of
// data that might be recoverable, or might reveal a producer-side bug worth
// investigating. It simply sits at its level, excluded from future batches
// once the level's file count drops below CompactThresholdFiles, without
// blocking progress on the rest of the column.
func (s *Service) mergeTraceLevel(ctx context.Context, colDir string, files []levelFile) error {
	mergeStart := s.now()

	sort.Slice(files, func(i, j int) bool { return files[i].key < files[j].key })
	outputLevel := files[0].level + 1

	var checker valueindex.RefChecker
	if s.exister != nil {
		checker = newCachingRefChecker(s.exister)
	}

	allGroups := make([][]valueindex.TraceGroup, 0, len(files))
	validFiles := make([]levelFile, 0, len(files))
	var corrupted int
	for _, f := range files {
		if err := ctx.Err(); err != nil {
			return err
		}
		data, err := s.store.Get(ctx, f.key)
		if err != nil {
			s.metrics.incError(compactorOpGet)
			return fmt.Errorf("valueindexcompactor: get %q: %w", f.key, err)
		}
		groups, derr := valueindex.DecodeTraceGroups(data)
		if derr != nil {
			corrupted++
			s.metrics.incError(compactorOpDecode)
			slog.Warn("valueindexcompactor: skipping corrupt trace index input",
				"key", f.key, "err", derr)
			continue
		}
		allGroups = append(allGroups, groups)
		validFiles = append(validFiles, f)
	}

	merged, err := valueindex.MergeTraceGroups(ctx, checker, allGroups...)
	if err != nil {
		s.metrics.incError(compactorOpGet)
		return fmt.Errorf("valueindexcompactor: merge trace groups %q: %w", colDir, err)
	}

	var written int
	if len(merged) > 0 {
		data, eerr := valueindex.EncodeTraceGroups(merged)
		if eerr != nil {
			s.metrics.incError(compactorOpPut)
			return fmt.Errorf("valueindexcompactor: encode trace groups %q: %w", colDir, eerr)
		}
		wallMinSec, wallMaxSec := traceGroupWallRange(merged)
		key := path.Join(colDir, valueindex.FormatFilenameV2(outputLevel, wallMinSec, wallMaxSec, valueindex.NewID()))
		if err := s.store.Put(ctx, key, data); err != nil {
			s.metrics.incError(compactorOpPut)
			return fmt.Errorf("valueindexcompactor: put %q: %w", key, err)
		}
		written = 1
	}

	// Delete only the inputs that were successfully decoded and incorporated
	// into the merge -- write-then-delete crash safety, identical ordering
	// guarantee to mergeLevel. Corrupt files (see doc comment above) are left
	// in place.
	var firstErr error
	var deleted int
	for _, f := range validFiles {
		if err := s.store.Delete(ctx, f.key); err != nil {
			s.metrics.incError(compactorOpDelete)
			if firstErr == nil {
				firstErr = fmt.Errorf("valueindexcompactor: delete %q: %w", f.key, err)
			}
			continue
		}
		deleted++
	}

	s.metrics.observeMerge(s.now().Sub(mergeStart))
	s.metrics.addMergeCounts(len(validFiles), written, deleted, 0, 0)
	if corrupted > 0 {
		s.metrics.incSkipped(corrupted)
	}
	return firstErr
}

// traceGroupWallRange returns the min/max TimeSec across groups, embedded in
// the merged output's filename (mirrors mergeLevel's NOTE-VI-037 use of the
// BucketGroup footer's MinTimeSec/MaxTimeSec) so DiscoverIndexFiles can prune
// compacted trace-index files by time. TraceGroup files carry no footer
// (Finding 2), so the range is computed directly from the in-memory groups.
func traceGroupWallRange(groups []valueindex.TraceGroup) (minSec, maxSec uint64) {
	for i := range groups {
		t := groups[i].TimeSec
		if i == 0 || t < minSec {
			minSec = t
		}
		if i == 0 || t > maxSec {
			maxSec = t
		}
	}
	return minSec, maxSec
}
