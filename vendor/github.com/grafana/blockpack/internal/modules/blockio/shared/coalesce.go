package shared

// NOTE: Any changes to this file must be reflected in the corresponding specs.md or NOTES.md.

// CoalesceConfig controls how adjacent block reads are merged.
type CoalesceConfig struct {
	MaxGapBytes   int64
	MaxWasteRatio float64
	// MaxReadBytes caps the total bytes in a single coalesced request.
	// A block that is itself larger than MaxReadBytes is still read whole;
	// the limit only prevents additional blocks from being merged in.
	// Zero means no limit.
	MaxReadBytes int64
}

// AggressiveCoalesceConfig merges within 4 MB, capped at 8 MB per request.
var AggressiveCoalesceConfig = CoalesceConfig{
	MaxGapBytes:   4 * 1024 * 1024,
	MaxWasteRatio: 1.0,
	MaxReadBytes:  8 * 1024 * 1024,
}

// CoalescedRead describes a single merged I/O request.
type CoalescedRead struct {
	BlockIDs     []int
	BlockOffsets []int64
	BlockLengths []int64
	Offset       int64
	Length       int64
}
