package sketch

import "github.com/FastFilter/xorfilter"

// BinaryFuse8 is a blockpack data type.
type BinaryFuse8 struct {
	inner *xorfilter.BinaryFuse8
}
