package shared

import (
	"errors"
	"fmt"
	"math"
)

// Security limits to prevent denial-of-service attacks from maliciously crafted files.
// These constants define maximum values for various data structures to prevent
// memory exhaustion and integer overflow attacks.

const (
	// MaxSpans is the maximum number of spans allowed in a single block.
	// Limit: 1 million spans = reasonable for large blocks
	// At ~1KB per span, this is ~1GB of uncompressed data per block
	MaxSpans = 1_000_000

	// MaxBlocks is the maximum number of blocks allowed in a file.
	// Limit: 100k blocks × 1M spans = 100B spans total (unrealistic but safe)
	MaxBlocks = 100_000

	// MaxColumns is the maximum number of columns per block.
	// Limit: 10k columns handles extreme wide data
	// This is far beyond typical use cases (usually <100 columns)
	MaxColumns = 10_000

	// MaxDictionarySize is the maximum size of a dictionary.
	// Limit: 1M entries for dictionary encoding
	// At 100 bytes per entry average, this is ~100MB
	MaxDictionarySize = 1_000_000

	// MaxStringLen is the maximum length of a single string value.
	// Limit: 10MB string prevents memory exhaustion
	// Note: Typical strings are <1KB, but some use cases need larger
	MaxStringLen = 10 * 1024 * 1024 // 10MB

	// MaxBytesLen is the maximum length of a single bytes value.
	// Limit: 10MB for bytes (same as strings)
	MaxBytesLen = 10 * 1024 * 1024 // 10MB

	// MaxBlockSize is the maximum uncompressed size of a block.
	// Limit: 1GB uncompressed block (compressed will be much smaller)
	MaxBlockSize = 1024 * 1024 * 1024 // 1GB

	// MaxMetadataSize is the maximum size of metadata section.
	// Limit: 100MB for all metadata (block index + dedicated index + etc)
	MaxMetadataSize = 100 * 1024 * 1024 // 100MB

	// MaxTraceCount is the maximum number of unique traces in a block.
	// Limit: 1M traces per block
	MaxTraceCount = 1_000_000

	// MaxNameLen is the maximum length of a column or field name.
	// Limit: 1KB for names (excessive names can cause memory issues)
	MaxNameLen = 1024

	// MaxGroupByCardinality is the maximum number of distinct GROUP BY keys allowed.
	// This prevents memory exhaustion from high-cardinality GROUP BY queries
	// (e.g., GROUP BY trace.id could generate millions of groups).
	MaxGroupByCardinality = 1_000_000

	// FooterVersion is the current footer format version (streaming-only)
	FooterVersion uint16 = 2
)

// ValidateSpanCount checks if a span count is within limits.
func ValidateSpanCount(count uint32) error {
	if count > MaxSpans {
		return fmt.Errorf("span count %d exceeds limit %d", count, MaxSpans)
	}
	return nil
}

// ValidateBlockCount checks if a block count is within limits.
func ValidateBlockCount(count uint32) error {
	if count > MaxBlocks {
		return fmt.Errorf("block count %d exceeds limit %d", count, MaxBlocks)
	}
	return nil
}

// ValidateColumnCount checks if a column count is within limits.
func ValidateColumnCount(count uint32) error {
	if count > MaxColumns {
		return fmt.Errorf("column count %d exceeds limit %d", count, MaxColumns)
	}
	return nil
}

// ValidateStringLen checks if a string length is within limits.
func ValidateStringLen(length uint32) error {
	if length > MaxStringLen {
		return fmt.Errorf(
			"string length %d exceeds limit %d (max %d MB)",
			length,
			MaxStringLen,
			MaxStringLen/(1024*1024),
		)
	}
	return nil
}

// ValidateBytesLen checks if a bytes length is within limits.
func ValidateBytesLen(length uint32) error {
	if length > MaxBytesLen {
		return fmt.Errorf("bytes length %d exceeds limit %d (max %d MB)", length, MaxBytesLen, MaxBytesLen/(1024*1024))
	}
	return nil
}

// ValidateDictionarySize checks if a dictionary size is within limits.
func ValidateDictionarySize(size int) error {
	if size > MaxDictionarySize {
		return fmt.Errorf("dictionary size %d exceeds limit %d", size, MaxDictionarySize)
	}
	return nil
}

// ValidateBlockSize checks if a block size is within limits.
func ValidateBlockSize(size uint64) error {
	if size > MaxBlockSize {
		return fmt.Errorf(
			"block size %d exceeds limit %d (max %d GB)",
			size,
			MaxBlockSize,
			MaxBlockSize/(1024*1024*1024),
		)
	}
	return nil
}

// ValidateMetadataSize checks if metadata size is within limits.
func ValidateMetadataSize(size uint64) error {
	if size > MaxMetadataSize {
		return fmt.Errorf(
			"metadata size %d exceeds limit %d (max %d MB)",
			size,
			MaxMetadataSize,
			MaxMetadataSize/(1024*1024),
		)
	}
	return nil
}

// ValidateTraceCount checks if trace count is within limits.
func ValidateTraceCount(count int) error {
	if count > MaxTraceCount {
		return fmt.Errorf("trace count %d exceeds limit %d", count, MaxTraceCount)
	}
	return nil
}

// ValidateNameLen checks if a name length is within limits.
func ValidateNameLen(length int) error {
	if length < 0 || length > MaxNameLen {
		return fmt.Errorf("name length %d exceeds limit %d", length, MaxNameLen)
	}
	return nil
}

// ValidateColumnType checks if a column type value is valid.
// Valid types are 0-9 (String, Int64, Uint64, Float64, Bool, Bytes, RangeInt64, RangeUint64, RangeDuration, reserved)
func ValidateColumnType(typ uint8) error {
	if typ > 9 {
		return fmt.Errorf("invalid column type %d (valid range: 0-9)", typ)
	}
	return nil
}

// Integer overflow protection
//
// NOTE: These SafeAdd*/SafeMul* functions provide overflow-safe arithmetic operations.
// They are currently used in tests and available for future integration into critical
// code paths where integer overflow is a concern (e.g., buffer size calculations,
// offset arithmetic). The validation functions above (ValidateSpanCount, etc.) provide
// the primary defense against overflow by rejecting invalid input early.

// ErrIntegerOverflow is returned when an arithmetic operation would overflow.
var ErrIntegerOverflow = errors.New("integer overflow in arithmetic operation")

// SafeAddInt adds two int values with overflow checking.
// Returns error if the addition would overflow.
func SafeAddInt(a, b int) (int, error) {
	if b > 0 {
		if a > math.MaxInt-b {
			return 0, fmt.Errorf("%w: %d + %d", ErrIntegerOverflow, a, b)
		}
	} else if b < 0 {
		if a < math.MinInt-b {
			return 0, fmt.Errorf("%w: %d + %d", ErrIntegerOverflow, a, b)
		}
	}
	return a + b, nil
}

// SafeAddInt64 adds two int64 values with overflow checking.
// Returns error if the addition would overflow.
func SafeAddInt64(a, b int64) (int64, error) {
	if b > 0 {
		if a > math.MaxInt64-b {
			return 0, fmt.Errorf("%w: %d + %d", ErrIntegerOverflow, a, b)
		}
	} else if b < 0 {
		if a < math.MinInt64-b {
			return 0, fmt.Errorf("%w: %d + %d", ErrIntegerOverflow, a, b)
		}
	}
	return a + b, nil
}

// SafeAddUint64 adds two uint64 values with overflow checking.
// Returns error if the addition would overflow.
func SafeAddUint64(a, b uint64) (uint64, error) {
	if a > math.MaxUint64-b {
		return 0, fmt.Errorf("%w: %d + %d", ErrIntegerOverflow, a, b)
	}
	return a + b, nil
}
