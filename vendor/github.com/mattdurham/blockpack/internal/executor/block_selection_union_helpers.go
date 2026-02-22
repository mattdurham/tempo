package executor

import (
	"encoding/hex"
	"regexp"

	"github.com/mattdurham/blockpack/internal/arena"
	blockpackio "github.com/mattdurham/blockpack/internal/blockio"
)

// handleTraceIDUnion processes trace:id predicates using the trace block index for fast lookups.
// Returns true if the trace block index was successfully used.
func handleTraceIDUnion(
	reader *blockpackio.Reader,
	predicates *QueryPredicates,
	current map[int]struct{},
	matched map[int]struct{},
) bool {
	traceIDValues, ok := predicates.DedicatedColumns[IntrinsicTraceID]
	if !ok || len(traceIDValues) == 0 {
		return false
	}

	traceIDUsedIndex := false
	for _, val := range traceIDValues {
		// Convert value to [16]byte trace ID
		var traceID [16]byte
		decoded := false

		if hexStr, ok := val.Data.(string); ok {
			// Hex string format: "0102030405060708090a0b0c0d0e0f10"
			decodedBytes, err := hex.DecodeString(hexStr)
			if err == nil && len(decodedBytes) == 16 {
				copy(traceID[:], decodedBytes)
				decoded = true
			}
		} else if bytes, ok := val.Data.([]byte); ok && len(bytes) == 16 {
			// Raw bytes format
			copy(traceID[:], bytes)
			decoded = true
		}

		if decoded {
			blockIDs := reader.BlocksForTraceID(traceID)
			if blockIDs != nil {
				// Trace block index available - use it
				traceIDUsedIndex = true
				for _, id := range blockIDs {
					if _, allowed := current[id]; allowed {
						matched[id] = struct{}{}
					}
				}
			}
		}
	}

	return traceIDUsedIndex
}

// processDedicatedColumnsUnion processes dedicated column predicates for union logic.
// Returns true if all predicates were successfully represented via dedicated index lookups,
// false otherwise (column not indexed, value not found, type conversion failure, etc.).
func processDedicatedColumnsUnion(
	reader *blockpackio.Reader,
	predicates *QueryPredicates,
	current map[int]struct{},
	matched map[int]struct{},
	traceIDUsedIndex bool,
	arena *arena.Arena,
) bool {
	allValuesIndexed := true

	for column, values := range predicates.DedicatedColumns {
		// Skip trace:id if we successfully used the trace block index
		if column == IntrinsicTraceID && traceIDUsedIndex {
			continue
		}

		scopedColumns := expandPredicateColumns(predicates, column)
		if len(scopedColumns) == 0 {
			// Column doesn't exist - can't use dedicated index
			allValuesIndexed = false
			continue
		}

		for _, scoped := range scopedColumns {
			if len(reader.DedicatedValues(scoped)) == 0 {
				// No dedicated index for this column - can't guarantee we have all values
				allValuesIndexed = false
				continue
			}

			// Use type from file's dedicated index (supports both explicit and auto-detected columns)
			colType, ok := reader.DedicatedColumnTypeFromFile(scoped)
			if !ok {
				// Can't determine column type - can't use dedicated index
				allValuesIndexed = false
				continue
			}

			for _, val := range values {
				key, ok := valueToDedicatedKey(colType, val, scoped)
				if !ok {
					// Can't convert value to dedicated key - can't use index for this value
					allValuesIndexed = false
					continue
				}

				blocks := reader.BlocksForDedicated(scoped, key, arena)
				if len(blocks) == 0 {
					// CRITICAL: Value not found in dedicated index!
					// This could mean:
					// 1. The value truly doesn't exist anywhere (rare)
					// 2. The value exists but wasn't sampled (sparse indexing - COMMON)
					//
					// We can't distinguish between these cases, so we must fall back
					// to scanning all blocks to avoid missing data.
					allValuesIndexed = false
				}

				for _, id := range blocks {
					if _, allowed := current[id]; allowed {
						matched[id] = struct{}{}
					}
				}
			}
		}
	}

	return allValuesIndexed
}

// handleDedicatedRegexUnion processes regex predicates on dedicated columns for union logic.
func handleDedicatedRegexUnion(
	reader *blockpackio.Reader,
	predicates *QueryPredicates,
	current map[int]struct{},
	matched map[int]struct{},
	arena *arena.Arena,
) {
	for column, pattern := range predicates.DedicatedColumnsRegex {
		re, err := regexp.Compile(pattern)
		if err != nil {
			continue
		}

		scopedColumns := expandPredicateColumns(predicates, column)
		if len(scopedColumns) == 0 {
			continue
		}

		for _, scoped := range scopedColumns {
			if len(reader.DedicatedValues(scoped)) == 0 {
				continue
			}

			matchingBlocks := reader.BlocksForDedicatedRegex(scoped, re, arena)
			for _, blockID := range matchingBlocks {
				if _, allowed := current[blockID]; allowed {
					matched[blockID] = struct{}{}
				}
			}
		}
	}
}
