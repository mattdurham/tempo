package sketch

// HyperLogLog is a blockpack data type.
type HyperLogLog struct {
	regs [hllM]uint8
}
