package shared

// NOTE: Any changes to this file must be reflected in the corresponding specs.md or NOTES.md.

// CoalesceConfig controls how adjacent block reads are merged.

// MaxReadBytes caps the total bytes in a single coalesced request.
// A block that is itself larger than MaxReadBytes is still read whole;
// the limit only prevents additional blocks from being merged in.
// Zero means no limit.

// AggressiveCoalesceConfig merges within 4 MB, capped at 8 MB per request.
var AggressiveCoalesceConfig = CoalesceConfig{
	MaxGapBytes:   4 * 1024 * 1024,
	MaxWasteRatio: 1.0,
	MaxReadBytes:  8 * 1024 * 1024,
}

// CoalescedRead describes a single merged I/O request.
