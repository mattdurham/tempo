package traceqlparser

// VectorExpr is a blockpack data type.
type VectorExpr struct {
	KVPairs   map[string]string
	QueryText string
	Mode      VectorMode
}
