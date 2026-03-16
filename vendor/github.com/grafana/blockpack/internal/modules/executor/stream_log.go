package executor

// NOTE: Any changes to this file must be reflected in the corresponding SPECS.md or NOTES.md.

import (
	"fmt"
	"maps"
	"strings"

	"github.com/grafana/blockpack/internal/logqlparser"
	modules_reader "github.com/grafana/blockpack/internal/modules/blockio/reader"
	modules_shared "github.com/grafana/blockpack/internal/modules/blockio/shared"
	"github.com/grafana/blockpack/internal/modules/queryplanner"
	"github.com/grafana/blockpack/internal/vm"
)

// LogAttrs holds log.* ColumnTypeString column values collected for a single log row.
// Names and Values are parallel slices; Names[i] is the full column name (e.g.
// "log.detected_level") and Values[i] is the corresponding string value.
// Both slices are nil when no log.* attributes are present for the row.
type LogAttrs struct {
	Names  []string
	Values []string
}

// Len returns the number of log.* attributes.
func (a LogAttrs) Len() int { return len(a.Names) }

// LogEntry is one matched log row returned by StreamLogs.
// SPEC-SL-5: Pipeline.Process is called per matched row and may drop it.
// LokiLabels holds the raw value of the resource.__loki_labels__ column (the Loki
// stream selector string, e.g. `{service_name="api", env="prod"}`). It is the only
// resource-label field callers need; building a full map[string]string per row was
// wasteful because consumers only ever read this one key.
type LogEntry struct {
	// LokiLabels is the value of resource.__loki_labels__ for this row.
	LokiLabels string
	Line       string
	// LogAttrs holds log.* ColumnTypeString column values as parallel name/value slices
	// (e.g. Names=["log.detected_level"], Values=["info"]). These are original LogRecord
	// attributes and must be exposed with the "log." prefix intact so callers (e.g.
	// extractStructuredMetadata) can distinguish them from pipeline-derived labels.
	// Both slices are nil when none present.
	LogAttrs       LogAttrs
	TimestampNanos uint64
}

// StreamLogs scans a blockpack log file using the given vm.Program and optional
// Pipeline, collecting all matched rows into a slice.
//
// SPEC-SL-1: Nil reader returns nil error; result is empty.
// SPEC-SL-2: Nil pipeline is valid; rows are delivered without pipeline transformation.
// Program provides the block-level column predicate; pipeline applies per-row transformations.
//
// Blocks are fetched lazily in ~8 MB coalesced batches.
func StreamLogs(
	r *modules_reader.Reader,
	program *vm.Program,
	pipeline *logqlparser.Pipeline,
) ([]LogEntry, error) {
	if r == nil {
		return nil, nil
	}
	if program == nil {
		return nil, fmt.Errorf("StreamLogs: program cannot be nil")
	}

	wantColumns := ProgramWantColumns(program)
	// NOTE-021: ensure log:timestamp is always in the first-pass block for time pre-filtering.
	// Also include resource.__loki_labels__ so stream-label metadata is always decoded.
	if wantColumns != nil {
		wantCopy := make(map[string]struct{}, len(wantColumns)+2)
		maps.Copy(wantCopy, wantColumns)
		wantCopy["log:timestamp"] = struct{}{}
		wantCopy["resource.__loki_labels__"] = struct{}{}
		wantColumns = wantCopy
	}

	plan := planBlocks(r, program, queryplanner.TimeRange{}, queryplanner.PlanOptions{})

	if len(plan.SelectedBlocks) == 0 {
		return nil, nil
	}

	// Pre-allocate with a capacity hint: min(totalSpanCount, 4096) to avoid repeated
	// slice growth copies while not over-allocating for selective queries.
	var totalRows int
	for _, blockIdx := range plan.SelectedBlocks {
		totalRows += int(r.BlockMeta(blockIdx).SpanCount)
	}
	if totalRows > 4096 {
		totalRows = 4096
	}
	results := make([]LogEntry, 0, totalRows)

	// Partition selected blocks into ~8 MB coalesced groups for lazy batched I/O.
	groups := r.CoalescedGroups(plan.SelectedBlocks)

	blockToGroup := make(map[int]int, len(plan.SelectedBlocks))
	for gi, g := range groups {
		for _, bi := range g.BlockIDs {
			blockToGroup[bi] = gi
		}
	}

	fetched := make(map[int][]byte)
	fetchedGroupsSeen := make(map[int]bool)

	for _, blockIdx := range plan.SelectedBlocks {
		gi, ok := blockToGroup[blockIdx]
		if !ok {
			continue
		}

		if !fetchedGroupsSeen[gi] {
			groupRaw, fetchErr := r.ReadGroup(groups[gi])
			if fetchErr != nil {
				return nil, fmt.Errorf("StreamLogs ReadGroup: %w", fetchErr)
			}
			maps.Copy(fetched, groupRaw)
			fetchedGroupsSeen[gi] = true
		}

		raw, rawOK := fetched[blockIdx]
		if !rawOK {
			continue
		}
		delete(fetched, blockIdx)

		meta := r.BlockMeta(blockIdx)
		r.ResetInternStrings()
		bwb, parseErr := r.ParseBlockFromBytes(raw, wantColumns, meta)
		if parseErr != nil {
			return nil, fmt.Errorf("StreamLogs ParseBlockFromBytes block %d: %w", blockIdx, parseErr)
		}

		colProvider := NewColumnProvider(bwb.Block)
		rowSet, evalErr := program.ColumnPredicate(colProvider)
		if evalErr != nil {
			return nil, fmt.Errorf("StreamLogs ColumnPredicate block %d: %w", blockIdx, evalErr)
		}

		if rowSet.Size() == 0 {
			continue
		}

		// NOTE-001: Lazy registration in ParseBlockFromBytes registers all columns with
		// presence-only decode. Full decode is triggered on first value access — no AddColumnsToBlock needed.

		// Cache column pointers for the row loop.
		tsCol := bwb.Block.GetColumn("log:timestamp")
		bodyCol := bwb.Block.GetColumn("log:body")
		colNames, colMap, colCols, logStrNames, logStrCols := buildBlockColMapsWithLogCache(bwb.Block)
		skipParsers := pipeline != nil && blockHasBodyParsed(bwb.Block)
		for _, rowIdx := range rowSet.ToSlice() {
			var tsNanos uint64
			if tsCol != nil {
				if v, ok := tsCol.Uint64Value(rowIdx); ok {
					tsNanos = v
				}
			}
			var line string
			if bodyCol != nil {
				if v, ok := bodyCol.StringValue(rowIdx); ok {
					line = v
				}
			}

			// NOTE-SL-016: acquire from pool — zero map allocation for dropped rows.
			bls := acquireBlockLabelSet(bwb.Block, rowIdx, colNames, colMap, colCols)
			if pipeline != nil {
				var keep bool
				if skipParsers {
					line, _, keep = pipeline.ProcessSkipParsers(tsNanos, line, bls)
				} else {
					line, _, keep = pipeline.Process(tsNanos, line, bls)
				}
				if !keep {
					releaseBlockLabelSet(bls)
					continue // pipeline dropped the row
				}
			}

			// Read __loki_labels__ via the post-pipeline LabelSet so that mutations
			// (drop/keep/label_format) are respected if they affect this field.
			lokiLabels := bls.Get("__loki_labels__")
			logAttrs := collectLogStringAttrs(logStrNames, logStrCols, rowIdx)
			releaseBlockLabelSet(bls)
			results = append(
				results,
				LogEntry{TimestampNanos: tsNanos, Line: line, LokiLabels: lokiLabels, LogAttrs: logAttrs},
			)
		}
	}

	return results, nil
}

// blockHasBodyParsed reports whether the block contains any body-auto-parsed log.* columns.
// Body-auto-parsed columns are written by the blockpack writer when parseLogBody succeeds
// at ingest time; they always use ColumnTypeRangeString.
// Explicit OTLP LogRecord attributes (e.g. log.level, log.detected_level, log.instance_id)
// use ColumnTypeString and are NOT counted here.
// When this returns true, body fields are already stored as block columns and will be
// read lazily via blockLabelSet, so the pipeline's logfmt/JSON parser stages are pure no-ops.
func blockHasBodyParsed(block *modules_reader.Block) bool {
	for key := range block.Columns() {
		if strings.HasPrefix(key.Name, "log.") && key.Type == modules_shared.ColumnTypeRangeString {
			return true
		}
	}
	return false
}
