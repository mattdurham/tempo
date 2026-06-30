package valueindex

// NOTE: Any changes to this file must be reflected in the corresponding SPECS.md or NOTES.md.

import (
	"context"
	"fmt"

	"github.com/grafana/blockpack/internal/modules/blockio/shared"
)

// RefChecker reports whether a source_ref is still live in object storage.
// Implementations may cache results for the duration of one compaction run.
type RefChecker interface {
	IsLive(ctx context.Context, sourceRef string) (bool, error)
}

// CompactConfig controls compaction behavior.
type CompactConfig struct {
	// Checker, if non-nil, drops entries whose SourceRef is confirmed deleted.
	Checker RefChecker
	// MaxOutputBytes is the approximate max serialized file size before splitting.
	// 0 means no limit — produce a single output file.
	MaxOutputBytes int64
}

// CompactStats reports the entry-level outcome of one CompactFiles call so the
// caller can drive observability metrics. Retained counts entries kept (source
// still live, or no Checker configured); Dropped counts entries discarded
// because their source blockpack was confirmed deleted by the Checker. Both are
// pre-dedup counts (each input occurrence is counted once), so Retained is the
// number of live entries that flowed into the merge, not the deduped output
// cardinality.
type CompactStats struct {
	Retained int
	Dropped  int
}

// CompactFiles merges and deduplicates the given readers into one or more output files.
// For each output file it calls output with the serialized bytes. It returns
// per-entry CompactStats so callers can observe retained vs dropped counts.
// All input readers must share the same CompactionLevel (VI-012). Output level = input level + 1.
func CompactFiles(
	ctx context.Context,
	readers []*Reader,
	cfg CompactConfig,
	output func([]byte) error,
) (CompactStats, error) {
	if len(readers) == 0 {
		return CompactStats{}, nil
	}

	// VI-012: all inputs must be at the same compaction level.
	inputLevel := readers[0].meta.CompactionLevel
	for _, r := range readers[1:] {
		if r.meta.CompactionLevel != inputLevel {
			return CompactStats{}, fmt.Errorf(
				"valueindex: CompactFiles requires same-level inputs: expected level %d, got level %d",
				inputLevel, r.meta.CompactionLevel,
			)
		}
	}

	outputLevel := inputLevel + 1

	colName := readers[0].meta.ColName
	colType := readers[0].meta.ColType

	// Gather all entries, optionally filtering dead refs.
	allEntries, stats, err := gatherEntries(ctx, readers, cfg.Checker)
	if err != nil {
		return CompactStats{}, fmt.Errorf("valueindex: CompactFiles: gather: %w", err)
	}

	// Sort and deduplicate (sortRawSlice / deduplicateEntries defined in writer.go).
	sortRawSlice(colType, allEntries)
	allEntries = deduplicateEntries(allEntries)

	// Write output file(s).
	if err := writeCompacted(ctx, allEntries, colName, colType, outputLevel, cfg.MaxOutputBytes, output); err != nil {
		return CompactStats{}, err
	}
	return stats, nil
}

// gatherEntries reads all entries from all readers, optionally checking liveness.
// It returns CompactStats counting retained vs dropped (dead-ref) entries.
func gatherEntries(ctx context.Context, readers []*Reader, checker RefChecker) ([]rawEntry, CompactStats, error) {
	var all []rawEntry
	var stats CompactStats
	for _, r := range readers {
		entries, err := r.DecodeAllEntries()
		if err != nil {
			return nil, CompactStats{}, err
		}
		for i := range entries {
			e := &entries[i]
			if checker != nil {
				live, err := checker.IsLive(ctx, e.SourceRef)
				if err != nil {
					return nil, CompactStats{}, fmt.Errorf("valueindex: RefChecker.IsLive(%q): %w", e.SourceRef, err)
				}
				if !live {
					stats.Dropped++
					continue
				}
			}
			stats.Retained++
			all = append(all, rawEntry{
				canonicalValue: e.Value,
				valueHash:      ValueHash16(e.Value),
				traceID:        e.TraceID,
				sourceRef:      e.SourceRef,
				blockID:        e.BlockID,
				blockRef:       e.BlockRef,
				timeSec:        e.TimeSec,
			})
		}
	}
	return all, stats, nil
}

// writeCompacted serializes entries into one or more output files.
// When maxOutputBytes > 0 it splits the output when the estimated size is exceeded.
func writeCompacted(
	ctx context.Context,
	entries []rawEntry,
	colName string,
	colType shared.ColumnType,
	outputLevel uint8,
	maxOutputBytes int64,
	output func([]byte) error,
) error {
	if maxOutputBytes <= 0 {
		// Single output file.
		data, err := flushBatch(ctx, entries, colName, colType, outputLevel)
		if err != nil {
			return err
		}
		return output(data)
	}

	// Split into batches by approximate entry count.
	// We use a rough heuristic: flush when accumulated entry count × avgEntrySize > maxOutputBytes.
	const avgEntryBytes = 64
	batchSize := int(maxOutputBytes / avgEntryBytes)
	if batchSize < 1 {
		batchSize = 1
	}

	for start := 0; start < len(entries); start += batchSize {
		end := start + batchSize
		if end > len(entries) {
			end = len(entries)
		}
		data, err := flushBatch(ctx, entries[start:end], colName, colType, outputLevel)
		if err != nil {
			return err
		}
		if err := output(data); err != nil {
			return err
		}
	}

	// If entries is empty, still produce one output file.
	if len(entries) == 0 {
		data, err := flushBatch(ctx, nil, colName, colType, outputLevel)
		if err != nil {
			return err
		}
		return output(data)
	}
	return nil
}

// flushBatch writes a batch of rawEntry to a new value index file and returns the bytes.
// Entries must already be sorted and deduplicated.
func flushBatch(
	_ context.Context,
	entries []rawEntry,
	colName string,
	colType shared.ColumnType,
	outputLevel uint8,
) ([]byte, error) {
	// Directly populate writerImpl.entries (already canonical, sorted, deduped).
	wi := &writerImpl{
		colName: colName,
		colType: colType,
		colHash: ColHash(colName),
		entries: entries,
	}
	return wi.flushSorted(outputLevel)
}
