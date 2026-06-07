package traceqlparser

// BinaryExpr is a blockpack data type.
type BinaryExpr struct {
	Left  Expr
	Right Expr
	Op    BinaryOp
}
