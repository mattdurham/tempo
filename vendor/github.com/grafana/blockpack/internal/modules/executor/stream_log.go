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
	wantColumns = injectLogRequiredColumns(wantColumns)

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

	// Delegate coalesced I/O, parse, and per-row pipeline to iterateLogRows.
	// CollectOptions{} zero value means no time range filtering (MinNano==MaxNano==0
	// is treated as open-ended by filterRowsByTimeRange).
	_, _, _, err := iterateLogRows(r, program, wantColumns, pipeline, CollectOptions{}, plan,
		nil, nil,
		func(_ uint64, entry LogEntry) bool {
			results = append(results, entry)
			return true
		},
	)
	if err != nil {
		return nil, err
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

// injectLogRequiredColumns returns a copy of wantColumns with log:timestamp and
// resource.__loki_labels__ added. Both columns are required for log queries regardless
// of predicates: log:timestamp for time pre-filtering (NOTE-021) and
// resource.__loki_labels__ so stream-label metadata is always decoded.
// Returns wantColumns unchanged when it is nil (nil means "want all columns").
func injectLogRequiredColumns(wantColumns map[string]struct{}) map[string]struct{} {
	if wantColumns == nil {
		return nil
	}
	wantCopy := make(map[string]struct{}, len(wantColumns)+2)
	maps.Copy(wantCopy, wantColumns)
	wantCopy["log:timestamp"] = struct{}{}
	wantCopy["resource.__loki_labels__"] = struct{}{}
	return wantCopy
}
