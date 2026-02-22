package shared

import (
	"fmt"
)

// Allocation limit constants for validateAllocationSize checks.
// These are more permissive upper bounds for DoS prevention during parsing.
// See also limits.go for stricter validation constants.
const (
	MaxBlockSpans          = 10_000_000  // 10M spans per block
	MaxAllocColumns        = 10_000      // 10K columns
	MaxAllocTraces         = 100_000_000 // 100M traces
	MaxAllocDictionarySize = 1_000_000   // 1M dictionary entries
	MaxAllocStringLen      = 10_000_000  // 10MB max string
)

// SafeUint32ToInt converts uint32 to int with overflow check.
// Returns error if value exceeds int capacity.
func SafeUint32ToInt(val uint32) (int, error) {
	const maxInt = int(^uint(0) >> 1)

	// On 32-bit systems, check if value exceeds MaxInt32
	if int64(val) > int64(maxInt) {
		return 0, fmt.Errorf("uint32 value %d exceeds maximum int value %d", val, maxInt)
	}

	return int(val), nil
}

// SafeUint64ToInt converts uint64 to int with overflow check.
// Returns error if value exceeds int capacity.
func SafeUint64ToInt(val uint64) (int, error) {
	const maxInt = int(^uint(0) >> 1)

	if val > uint64(maxInt) {
		return 0, fmt.Errorf("uint64 value %d exceeds maximum int value %d", val, maxInt)
	}

	return int(val), nil
}

// ValidateAllocationSize checks if allocation size is reasonable.
// Returns error if size is negative or exceeds maximum.
func ValidateAllocationSize(size, max int, context string) error { //nolint:revive
	if size < 0 {
		return fmt.Errorf("%s: negative allocation size %d", context, size)
	}
	if size > max {
		return fmt.Errorf("%s: allocation size %d exceeds maximum %d", context, size, max)
	}
	return nil
}
