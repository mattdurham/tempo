package valueindex

// NOTE: Any changes to this file must be reflected in the corresponding NOTES.md.
//
// temp_cleanup.go — startup sweep for orphaned local merge temp files (plan.md Decision 4).
// A process that crashes mid-merge can leave vi-merge-{in,out}-*.tmp files behind in the local
// temp directory; this sweep removes them once, at service construction time. It never
// touches runspill.go's vi-run-*.tmp files (a distinct, non-overlapping naming convention).

import (
	"fmt"
	"os"
	"path/filepath"
	"time"
)

// staleMergeTempFileAge is the minimum age a vi-merge-*.tmp file must reach before the
// startup sweep treats it as orphaned rather than possibly belonging to a merge that is
// still legitimately in flight. Decision 4's original design assumed exactly one process
// owns a given local temp directory across its own crash/restart lifecycle, with the sweep
// running once at that process's own startup — under that assumption alone, no in-flight
// file could ever be present at sweep time. That assumption is violated in practice by (a)
// this task's own stated future-work motivation (parallelizing Run() would mean multiple
// concurrent mergeLevel calls within one process, any of which could still be writing a
// temp file when another goroutine's — hypothetically — restarted sweep runs) and (b) this
// repo's shared multi-process test/dev environment, where independent `go test` invocations
// against the same host can observe each other's temp files through the shared os.TempDir().
// A same-process merge is expected to complete in well under a minute even for large
// batches; 10 minutes is a generous margin that only ever excludes a file that is either
// still in active use or was orphaned so recently it will be caught by the next sweep.
const staleMergeTempFileAge = 10 * time.Minute

// sweepOrphanedMergeTempFilesIn removes every vi-merge-*.tmp file directly under dir that is
// at least staleMergeTempFileAge old. Both the input-side (vi-merge-in-*, valueindexcompactor)
// and output-side (vi-merge-out-*, this package) naming conventions share this prefix, so one
// glob catches both. Best-effort: a missing or unreadable dir yields (0, nil), not an error —
// a leftover orphaned file is a disk-hygiene concern, not a correctness blocker. Individual
// stat/removal failures are collected (the first is returned) but every match is still
// attempted.
func sweepOrphanedMergeTempFilesIn(dir string) (int, error) {
	matches, err := filepath.Glob(filepath.Join(dir, "vi-merge-*.tmp"))
	if err != nil {
		return 0, fmt.Errorf("valueindex: sweepOrphanedMergeTempFilesIn: glob: %w", err)
	}
	var removed int
	var firstErr error
	now := time.Now()
	for _, m := range matches {
		if info, statErr := os.Stat(m); statErr == nil && now.Sub(info.ModTime()) < staleMergeTempFileAge {
			// Too fresh to be orphaned — may belong to a merge still in flight.
			continue
		}
		if rerr := os.Remove(m); rerr != nil {
			if firstErr == nil {
				firstErr = fmt.Errorf("valueindex: sweepOrphanedMergeTempFilesIn: remove %q: %w", m, rerr)
			}
			continue
		}
		removed++
	}
	return removed, firstErr
}

// SweepOrphanedMergeTempFiles removes every leftover vi-merge-*.tmp file in the process's
// local temp directory (os.TempDir()). Intended to be called once, at service construction
// (valueindexcompactor.NewService), to clean up files left behind by a prior process that
// crashed mid-merge. Callers should treat a non-nil error as best-effort/logged, never fatal.
func SweepOrphanedMergeTempFiles() (int, error) {
	return sweepOrphanedMergeTempFilesIn(os.TempDir())
}
