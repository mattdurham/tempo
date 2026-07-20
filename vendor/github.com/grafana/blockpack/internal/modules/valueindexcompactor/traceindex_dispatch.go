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
	"os"
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
// retention-filters them via valueindex.StreamCompactTraceGroups, writes the
// merged output at level+1, then deletes only the inputs it successfully
// decoded and incorporated. Mirrors mergeLevel's write-then-delete
// crash-safety ordering and its disk-streaming input-staging design
// (NOTE-VI-109, issue #500): inputs are staged to local disk one at a time as
// they are fetched, and each input's decoded representation is bounded to one
// block at a time via a disk-backed lazy iterator
// (valueindex.NewDiskTraceGroupFileIterator) -- peak decoded memory is bounded
// by the number of concurrently-open iterators times one block, not by the
// number or total size of input files. This replaces the prior fully
// in-memory MergeTraceGroups call site (still used directly by other/small-
// input callers; see NOTE-VI-109).
//
// A file that fails to construct a valid iterator (bad magic, or a genuine
// decode error in its footer/string table/block directory) is skipped, not
// treated as fatal -- the merge proceeds with the remaining valid files.
// Unlike a successfully-decoded file (always deleted once incorporated, even
// if every span it carried was later dropped by retention), such a file is
// deliberately left in place rather than deleted: retrying can't fix a
// genuinely malformed payload, but deleting it would destroy the only copy of
// data that might be recoverable, or might reveal a producer-side bug worth
// investigating. It simply sits at its level, excluded from future batches
// once the level's file count drops below CompactThresholdFiles, without
// blocking progress on the rest of the column. A hard valueindexcompactor.IndexStore.Get
// failure (as opposed to a decode failure) is fatal and aborts the whole merge, matching
// mergeLevel's own Get-failure posture.
func (s *Service) mergeTraceLevel(ctx context.Context, colDir string, files []levelFile) error {
	mergeStart := s.now()

	sort.Slice(files, func(i, j int) bool { return files[i].key < files[j].key })
	outputLevel := files[0].level + 1

	var checker valueindex.RefChecker
	if s.exister != nil {
		checker = newCachingRefChecker(s.exister)
	}

	// tmpDir is the single source of truth for where this merge stages local files, threaded
	// into both the input-side writeLocalTempInput calls below and the output-side
	// StreamCompactTraceGroups call -- os.TempDir() today, but kept as one local variable
	// (rather than each call site independently hardcoding os.TempDir()) so it is trivially
	// swappable for a config-driven mount path once one exists (deploy-side disk provisioning
	// for value-index-compactor, e.g. a PVC via volumeClaimTemplate, is being handled
	// separately -- an emptyDir is unsafe here given this StatefulSet's 20 replicas can be
	// co-scheduled on the same nodes).
	tmpDir := os.TempDir()

	iterators := make([]valueindex.TraceGroupIterator, 0, len(files))
	validFiles := make([]levelFile, 0, len(files))
	var corrupted int
	defer func() {
		for _, it := range iterators {
			_ = it.Close()
		}
	}()
	for _, f := range files {
		if err := ctx.Err(); err != nil {
			return err
		}
		data, err := s.store.Get(ctx, f.key)
		if err != nil {
			s.metrics.incError(compactorOpGet)
			return fmt.Errorf("valueindexcompactor: get %q: %w", f.key, err)
		}
		tmpPath, err := writeLocalTempInput(tmpDir, data)
		if err != nil {
			return fmt.Errorf("valueindexcompactor: stage %q locally: %w", f.key, err)
		}
		it, ierr := valueindex.NewDiskTraceGroupFileIterator(ctx, tmpPath, checker)
		if ierr != nil {
			// A genuinely corrupt trace-index file (footer/string-table/block-directory
			// decode failure) is skipped, not fatal -- mergeTraceLevel's own long-standing
			// contract (unlike mergeLevel's BucketGroup sibling, which used to abort the
			// whole merge on the analogous NewDiskBucketFileIterator error; both now skip
			// consistently, see markFileCorrupted). Marked "<key>.corrupted" so it's
			// preserved for inspection but never retried again (tempo-dev-test-03 incident
			// follow-up, 2026-07-15) -- if marking itself fails, fall back to the ORIGINAL
			// leave-in-place behavior (retried next cycle) rather than making a single
			// corrupt file newly fatal, since that was never this function's contract.
			_ = os.Remove(tmpPath)
			corrupted++
			s.metrics.incError(compactorOpDecode)
			if merr := s.markFileCorrupted(ctx, f.key, data); merr != nil {
				slog.Warn("valueindexcompactor: skipping corrupt trace index input (failed to mark, left in place)",
					"key", f.key, "err", ierr, "markErr", merr)
			} else {
				s.metrics.incFilesCorrupted()
				slog.Warn("valueindexcompactor: marked corrupt trace index input as .corrupted and skipped",
					"key", f.key, "err", ierr)
			}
			continue
		}
		if it == nil {
			// Bad magic -- not a v2 "VTG2" file at all. Same skip-not-delete treatment as a
			// genuine decode error: the v1 flat-blob rollover window has closed (NOTE-VI-079),
			// so there is no longer a "safe to discard, known-legacy" case for this format.
			_ = os.Remove(tmpPath)
			corrupted++
			s.metrics.incError(compactorOpDecode)
			slog.Warn("valueindexcompactor: skipping non-v2 trace index input", "key", f.key)
			continue
		}
		iterators = append(iterators, it)
		validFiles = append(validFiles, f)
	}

	var written int
	err := valueindex.StreamCompactTraceGroups(ctx, iterators, 0, s.cfg.MaxOutputBytes, tmpDir, func(outPath string) error {
		//nolint:gosec // G304: outPath is StreamCompactTraceGroups' own local temp output file, not user input
		data, rerr := os.ReadFile(outPath)
		if rerr != nil {
			return fmt.Errorf("valueindexcompactor: read local output %q: %w", outPath, rerr)
		}
		// Footer-only decode for the wall-clock time range -- mirrors mergeLevel's
		// NOTE-VI-037 use of the BucketGroup footer, avoiding a second full decode of the
		// just-written output.
		var wallMinSec, wallMaxSec uint64
		if ft, ferr := valueindex.DecodeTraceFooter(data); ferr == nil {
			wallMinSec, wallMaxSec = ft.MinTimeSec, ft.MaxTimeSec
		}
		key := path.Join(colDir, valueindex.FormatFilenameV2(outputLevel, wallMinSec, wallMaxSec, valueindex.NewID()))
		if perr := s.store.Put(ctx, key, data); perr != nil {
			s.metrics.incError(compactorOpPut)
			return fmt.Errorf("valueindexcompactor: put %q: %w", key, perr)
		}
		written++
		return nil
	})
	if err != nil {
		return fmt.Errorf("valueindexcompactor: merge trace groups %q: %w", colDir, err)
	}

	// Every iterator is fully drained by a successful StreamCompactTraceGroups call, so each
	// disk-backed iterator's cumulative per-file retention stats now reflect a complete total
	// -- mirrors mergeLevel's identical post-hoc StatsProvider aggregation.
	var stats valueindex.CompactStats
	for _, it := range iterators {
		if sp, ok := it.(valueindex.StatsProvider); ok {
			fstats := sp.Stats()
			stats.Retained += fstats.Retained
			stats.Dropped += fstats.Dropped
			stats.Corrupt += fstats.Corrupt
		}
	}
	if stats.Corrupt > 0 {
		slog.Warn("valueindexcompactor: dropped refs with unresolvable SourceID (data corruption, not routine retention)",
			"colDir", colDir, "corrupt", stats.Corrupt)
	}

	// Delete only the inputs that were successfully decoded and incorporated
	// into the merge -- write-then-delete crash safety, identical ordering
	// guarantee to mergeLevel. Corrupt/non-v2 files (see doc comment above) are
	// left in place.
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
	s.metrics.addMergeCounts(len(validFiles), written, deleted, stats.Retained, stats.Dropped, stats.Corrupt)
	if corrupted > 0 {
		s.metrics.incSkipped(corrupted)
	}
	return firstErr
}
