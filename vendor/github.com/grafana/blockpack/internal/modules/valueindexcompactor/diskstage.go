package valueindexcompactor

// NOTE: Any changes to this file must be reflected in the corresponding NOTES.md.
//
// diskstage.go implements plan.md Decision 2's input-side local disk staging: mergeLevel
// writes each input's raw bytes (already fetched via one s.store.Get) to a local temp file
// before handing it to valueindex.NewDiskBucketFileIterator, instead of decoding the raw
// bytes into a fully in-memory *valueindex.BucketFile. The "vi-merge-in-" prefix (shared with
// valueindex's "vi-merge-out-" output-side naming, both under the common "vi-merge-" glob) is
// deliberately distinct from valueindex/runspill.go's "vi-run-*.tmp" spill files, so the
// startup sweep (valueindex.SweepOrphanedMergeTempFiles) never touches runspill's in-flight
// files or vice versa.

import (
	"fmt"
	"os"
)

// writeLocalTempInput writes data to a new local temp file under dir (os.CreateTemp(dir,
// "vi-merge-in-*.tmp")), returning its path. On any failure, no path is returned and no
// partial file is left behind (os.CreateTemp either fails outright or its returned handle is
// closed before returning). The caller owns removal of the returned path on every code path
// that does not hand ownership to a valueindex.GroupIterator (plan.md Decision 2's ownership
// table) — writeLocalTempInput itself never removes the file it successfully wrote.
func writeLocalTempInput(dir string, data []byte) (string, error) {
	f, err := os.CreateTemp(dir, "vi-merge-in-*.tmp")
	if err != nil {
		return "", fmt.Errorf("valueindexcompactor: writeLocalTempInput: create temp file: %w", err)
	}
	path := f.Name()

	if _, err := f.Write(data); err != nil {
		_ = f.Close()
		_ = os.Remove(path) //nolint:gosec // G703: path comes from os.CreateTemp, not user input
		return "", fmt.Errorf("valueindexcompactor: writeLocalTempInput: write %q: %w", path, err)
	}
	if err := f.Close(); err != nil {
		_ = os.Remove(path) //nolint:gosec // G703: path comes from os.CreateTemp, not user input
		return "", fmt.Errorf("valueindexcompactor: writeLocalTempInput: close %q: %w", path, err)
	}
	return path, nil
}
