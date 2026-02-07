package sql

import (
	"fmt"

	"github.com/mattdurham/blockpack/traceql"
	"github.com/mattdurham/blockpack/vm"
)

// StructuralQueryPlan represents a compiled structural query
type StructuralQueryPlan struct {
	LHSProgram *vm.Program          // Compiled LHS filter
	RHSProgram *vm.Program          // Compiled RHS filter
	Operator   traceql.StructuralOp // Structural operator
}

// CompileStructuralQuery compiles a structural query to executable plan
func CompileStructuralQuery(sq *traceql.StructuralQuery) (*StructuralQueryPlan, error) {
	if sq == nil {
		return nil, fmt.Errorf("structural query is nil")
	}

	// Compile LHS filter
	lhsProgram, err := compileFilter(sq.Left)
	if err != nil {
		return nil, fmt.Errorf("compile LHS: %w", err)
	}

	// Compile RHS filter
	rhsProgram, err := compileFilter(sq.Right)
	if err != nil {
		return nil, fmt.Errorf("compile RHS: %w", err)
	}

	return &StructuralQueryPlan{
		LHSProgram: lhsProgram,
		RHSProgram: rhsProgram,
		Operator:   sq.Op,
	}, nil
}

// compileFilter compiles a filter expression to a VM program using direct TraceQL compilation.
// This bypasses SQL entirely, allowing full TraceQL expressiveness including shorthand syntax.
func compileFilter(expr *traceql.FilterExpression) (*vm.Program, error) {
	// Compile directly from TraceQL AST to VM bytecode
	program, err := vm.CompileTraceQLFilter(expr)
	if err != nil {
		return nil, fmt.Errorf("compile TraceQL filter: %w", err)
	}

	return program, nil
}
