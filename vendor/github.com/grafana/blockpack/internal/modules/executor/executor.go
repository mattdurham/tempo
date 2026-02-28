// Package executor executes TraceQL filter queries against modules blockpack files.
//
// Responsibility boundary:
//   - executor owns: block scanning, span-level predicate evaluation, result collection
//   - queryplanner owns: which blocks to read (bloom filter, range index pruning)
//   - blockio/reader owns: how to read them (coalescing, wire parsing)
//
// # Usage
//
//	exec := executor.New()
//	result, err := exec.Execute(r, program, executor.Options{})
//	// result.Matches contains matching spans; result.Plan contains block selection stats
package executor

// NOTE: Any changes to this file must be reflected in the corresponding specs.md or NOTES.md.

import (
	"fmt"

	modules_reader "github.com/grafana/blockpack/internal/modules/blockio/reader"
	"github.com/grafana/blockpack/internal/modules/queryplanner"
	"github.com/grafana/blockpack/internal/vm"
)

// SpanMatch is a span that matched the query.
type SpanMatch struct {
	SpanID   []byte   // 8-byte span ID
	BlockIdx int      // block index within the file
	RowIdx   int      // row (span) index within the block
	TraceID  [16]byte // 16-byte trace ID
}

// Result is the output of Execute.
type Result struct {
	Plan          *queryplanner.Plan // block selection plan (pruning stats)
	Matches       []SpanMatch        // matching spans
	BytesRead     int64              // total raw bytes read from the provider
	BlocksScanned int                // blocks that were actually read and evaluated
}

// Options controls query execution behavior.
type Options struct {
	// Limit caps the number of returned matches. 0 means no limit.
	Limit int
}

// Executor runs TraceQL filter queries against a modules blockpack file.
// It is stateless and safe for concurrent use.
type Executor struct{}

// New returns a new Executor.
func New() *Executor { return &Executor{} }

// Execute selects candidate blocks via queryplanner, fetches them in bulk,
// then evaluates program.ColumnPredicate against each block's spans.
//
// r is a *reader.Reader. program must be compiled with vm.CompileTraceQLFilter.
func (e *Executor) Execute(
	r *modules_reader.Reader,
	program *vm.Program,
	opts Options,
) (*Result, error) {
	if r == nil {
		return &Result{}, nil
	}

	planner := queryplanner.NewPlanner(r)
	plan := planner.Plan(buildPredicates(r, program))

	result := &Result{Plan: plan}
	if len(plan.SelectedBlocks) == 0 {
		return result, nil
	}

	rawBlocks, err := planner.FetchBlocks(plan)
	if err != nil {
		return nil, fmt.Errorf("FetchBlocks: %w", err)
	}

	for _, blockIdx := range plan.SelectedBlocks {
		raw, ok := rawBlocks[blockIdx]
		if !ok {
			continue
		}
		result.BytesRead += int64(len(raw)) //nolint:gosec

		meta := r.BlockMeta(blockIdx)
		bwb, parseErr := r.ParseBlockFromBytes(raw, nil, meta)
		if parseErr != nil {
			return nil, fmt.Errorf("ParseBlockFromBytes block %d: %w", blockIdx, parseErr)
		}

		provider := newBlockColumnProvider(bwb.Block)
		rowSet, evalErr := program.ColumnPredicate(provider)
		if evalErr != nil {
			return nil, fmt.Errorf("ColumnPredicate block %d: %w", blockIdx, evalErr)
		}

		result.BlocksScanned++

		for _, rowIdx := range rowSet.ToSlice() {
			match := spanMatchFromBlock(bwb.Block, blockIdx, rowIdx)
			result.Matches = append(result.Matches, match)
			if opts.Limit > 0 && len(result.Matches) >= opts.Limit {
				return result, nil
			}
		}
	}

	return result, nil
}

// spanMatchFromBlock extracts a SpanMatch from a block row.
func spanMatchFromBlock(block *modules_reader.Block, blockIdx, rowIdx int) SpanMatch {
	m := SpanMatch{BlockIdx: blockIdx, RowIdx: rowIdx}

	if col := block.GetColumn("trace:id"); col != nil {
		if v, ok := col.BytesValue(rowIdx); ok && len(v) == 16 {
			copy(m.TraceID[:], v)
		}
	}
	if col := block.GetColumn("span:id"); col != nil {
		if v, ok := col.BytesValue(rowIdx); ok {
			m.SpanID = make([]byte, len(v))
			copy(m.SpanID, v)
		}
	}
	return m
}
