package executor

import (
	"fmt"

	"github.com/mattdurham/blockpack/sql"
)

// ExecuteTraceQLStructural is the top-level API for executing structural TraceQL queries.
// It integrates the parser, compiler, and executor into a single function call.
//
// Workflow:
// 1. Parse TraceQL string → AST (traceql.StructuralQuery)
// 2. Compile AST → StructuralQueryPlan (LHS/RHS programs + operator)
// 3. Execute plan → BlockpackResult (filtered spans based on relationships)
//
// Example:
//
//	executor := NewBlockpackExecutor(storage)
//	result, err := ExecuteTraceQLStructural(
//	    executor,
//	    path,
//	    "{ .root = true } >> { .leaf = true }",
//	    QueryOptions{Limit: 100},
//	)
//
// Supported operators:
//   - >> : descendant (all descendants in tree)
//   - >  : child (direct children only)
//   - ~  : sibling (shares same parent)
//   - << : ancestor (all ancestors in tree)
//   - <  : parent (direct parent only)
//   - !~ : not sibling (negated sibling)
//
// Returns:
//   - BlockpackResult with spans matching the structural relationship
//   - Error if parsing, compilation, or execution fails
func ExecuteTraceQLStructural(
	executor *BlockpackExecutor,
	path string,
	traceqlQuery string,
	opts QueryOptions,
) (*BlockpackResult, error) {
	// Step 1: Compile TraceQL to execution plan
	plan, err := sql.CompileTraceQLStructural(traceqlQuery)
	if err != nil {
		return nil, fmt.Errorf("compile structural query: %w", err)
	}

	// Step 2: Execute the plan
	result, err := ExecuteStructuralQuery(executor, path, plan, opts)
	if err != nil {
		return nil, fmt.Errorf("execute structural query: %w", err)
	}

	return result, nil
}
