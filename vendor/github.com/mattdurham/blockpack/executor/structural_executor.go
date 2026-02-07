package executor

import (
	"fmt"

	"github.com/mattdurham/blockpack/sql"
	"github.com/mattdurham/blockpack/traceql"
	"github.com/mattdurham/blockpack/vm"
)

// ExecuteStructuralQuery executes a structural query with two-pass execution:
// 1. Execute LHS filter -> collect spans
// 2. Execute RHS filter -> collect spans
// 3. Build trace graphs
// 4. Apply structural operator
// 5. Return filtered results
func ExecuteStructuralQuery(
	executor *BlockpackExecutor,
	path string,
	plan *sql.StructuralQueryPlan,
	opts QueryOptions,
) (*BlockpackResult, error) {
	// For structural queries, we need ALL span fields to build the complete graph,
	// not just the fields mentioned in the filters
	structuralOpts := opts
	structuralOpts.MaterializeAllFields = true

	// PASS 1: Execute LHS filter
	lhsResult, err := executor.ExecuteQuery(path, plan.LHSProgram, nil, structuralOpts)
	if err != nil {
		return nil, fmt.Errorf("execute LHS: %w", err)
	}

	// PASS 2: Execute RHS filter
	rhsResult, err := executor.ExecuteQuery(path, plan.RHSProgram, nil, structuralOpts)
	if err != nil {
		return nil, fmt.Errorf("execute RHS: %w", err)
	}

	// If either side is empty, result is empty
	if len(lhsResult.Matches) == 0 || len(rhsResult.Matches) == 0 {
		return &BlockpackResult{
			Matches:       []BlockpackSpanMatch{},
			BlocksScanned: lhsResult.BlocksScanned + rhsResult.BlocksScanned,
			BytesRead:     lhsResult.BytesRead + rhsResult.BytesRead,
			IOOperations:  lhsResult.IOOperations + rhsResult.IOOperations,
		}, nil
	}

	// PASS 3: Build trace graphs
	// For transitive operators (>>, <<), we need ALL spans in each trace to build
	// complete graphs, not just LHS and RHS matches. Otherwise intermediate spans
	// won't be in the graph and transitive traversal will fail.
	// For non-transitive operators (>, <, ~, !~), we only need LHS and RHS spans.
	var allSpans []BlockpackSpanMatch
	var graphLoadResult *BlockpackResult // Track metrics from graph loading
	if plan.Operator == traceql.OpDescendant || plan.Operator == traceql.OpAncestor {
		// Load all spans from affected traces to build complete graphs
		// Get unique trace IDs from LHS and RHS
		traceIDs := make(map[string]struct{})
		for _, match := range lhsResult.Matches {
			traceIDs[match.TraceID] = struct{}{}
		}
		for _, match := range rhsResult.Matches {
			traceIDs[match.TraceID] = struct{}{}
		}

		// Load all spans for these traces using a match-all filter
		// TODO(performance): Use vm.CompileTraceIDFilter(traceIDList) to scan only affected traces
		//   Currently does full scan + in-memory filter, which is expensive for large datasets
		matchAllProgram, err := vm.CompileTraceQLFilter(nil) // nil filter matches all
		if err != nil {
			return nil, fmt.Errorf("compile match-all filter: %w", err)
		}
		allSpansResult, err := executor.ExecuteQuery(path, matchAllProgram, nil, QueryOptions{
			MaterializeAllFields: true,
		})
		if err != nil {
			return nil, fmt.Errorf("load all spans for graph: %w", err)
		}
		graphLoadResult = allSpansResult // Save for metrics

		// Filter to only spans from affected traces
		for _, span := range allSpansResult.Matches {
			if _, ok := traceIDs[span.TraceID]; ok {
				allSpans = append(allSpans, span)
			}
		}
	} else {
		// For non-transitive operators, only need LHS and RHS spans
		allSpans = append(append([]BlockpackSpanMatch{}, lhsResult.Matches...), rhsResult.Matches...)
	}
	graphs := BuildTraceGraph(allSpans)

	// PASS 4: Apply structural operator
	finalMatches := make([]BlockpackSpanMatch, 0)

	for traceID, graph := range graphs {
		// Get LHS and RHS nodes for this trace
		lhsNodes := getNodesForMatches(lhsResult.Matches, traceID, graph)
		rhsNodes := getNodesForMatches(rhsResult.Matches, traceID, graph)

		if len(lhsNodes) == 0 || len(rhsNodes) == 0 {
			continue
		}

		// Apply relationship operator
		var resultNodes []*SpanNode
		switch plan.Operator {
		case traceql.OpDescendant:
			resultNodes = DescendantOf(lhsNodes, rhsNodes, graph, false)
		case traceql.OpChild:
			resultNodes = ChildOf(lhsNodes, rhsNodes, graph, false)
		case traceql.OpSibling:
			resultNodes = SiblingOf(lhsNodes, rhsNodes, graph, false)
		case traceql.OpAncestor:
			resultNodes = AncestorOf(lhsNodes, rhsNodes, graph, false)
		case traceql.OpParent:
			resultNodes = ParentOf(lhsNodes, rhsNodes, graph, false)
		case traceql.OpNotSibling:
			resultNodes = SiblingOf(lhsNodes, rhsNodes, graph, true) // negated
		default:
			return nil, fmt.Errorf("unknown operator: %v", plan.Operator)
		}

		// Convert nodes back to matches
		for _, node := range resultNodes {
			if node.Span != nil {
				finalMatches = append(finalMatches, *node.Span)
			}
		}
	}

	// Apply LIMIT if specified
	if opts.Limit > 0 && len(finalMatches) > opts.Limit {
		finalMatches = finalMatches[:opts.Limit]
	}

	// Aggregate metrics from all query passes
	blocksScanned := lhsResult.BlocksScanned + rhsResult.BlocksScanned
	bytesRead := lhsResult.BytesRead + rhsResult.BytesRead
	ioOperations := lhsResult.IOOperations + rhsResult.IOOperations

	// Include graph loading metrics for transitive operators
	if graphLoadResult != nil {
		blocksScanned += graphLoadResult.BlocksScanned
		bytesRead += graphLoadResult.BytesRead
		ioOperations += graphLoadResult.IOOperations
	}

	return &BlockpackResult{
		Matches:       finalMatches,
		BlocksScanned: blocksScanned,
		BytesRead:     bytesRead,
		IOOperations:  ioOperations,
	}, nil
}

// getNodesForMatches converts matches to nodes for a specific trace
func getNodesForMatches(matches []BlockpackSpanMatch, traceID string, graph *TraceSpanGraph) []*SpanNode {
	nodes := make([]*SpanNode, 0)
	for i := range matches {
		match := &matches[i]
		if match.TraceID == traceID {
			if node := graph.SpansByID[match.SpanID]; node != nil {
				nodes = append(nodes, node)
			}
		}
	}
	return nodes
}
