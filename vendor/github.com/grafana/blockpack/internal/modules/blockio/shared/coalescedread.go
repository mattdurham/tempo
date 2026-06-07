package shared

// CoalescedRead is a blockpack data type.
type CoalescedRead struct {
	BlockIDs     []int
	BlockOffsets []int64
	BlockLengths []int64
	Offset       int64
	Length       int64
}
