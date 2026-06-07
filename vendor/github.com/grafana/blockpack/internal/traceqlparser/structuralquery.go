package traceqlparser

// StructuralQuery is a blockpack data type.
type StructuralQuery struct {
	Left  *FilterExpression
	Right Expr
	Op    StructuralOp
}
