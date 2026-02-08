package blockio

// IsIDColumn reports whether a column name should use XOR encoding.
func IsIDColumn(name string) bool {
	return isIDColumn(name)
}

// IsURLColumn reports whether a column name should use prefix encoding.
func IsURLColumn(name string) bool {
	return isURLColumn(name)
}

// IsArrayColumn reports whether a column stores array data.
func IsArrayColumn(name string) bool {
	return isArrayColumn(name)
}

// NewReusableBlock returns a block instance prepared for column reuse.
func NewReusableBlock() *Block {
	return &Block{columns: make(map[string]*Column)}
}
