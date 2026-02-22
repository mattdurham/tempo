package executor

import (
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"hash/fnv"
	"math"
	"regexp"
	"sort"
	"strings"

	"github.com/mattdurham/blockpack/internal/arena"
	aslice "github.com/mattdurham/blockpack/internal/arena/slice"
	blockpackio "github.com/mattdurham/blockpack/internal/blockio"
	blockpack "github.com/mattdurham/blockpack/internal/types"
	"github.com/mattdurham/blockpack/internal/vm"
)

func checkColumnBloom(bloom *[blockpack.ColumnNameBloomBits / 8]byte, columnName string) bool {
	// Helper to check if a specific column name exists in bloom filter
	checkName := func(name string) bool {
		h1 := fnv.New32a()
		_, _ = h1.Write([]byte(name))
		hash1 := h1.Sum32()

		h2 := fnv.New32()
		_, _ = h2.Write([]byte(name))
		hash2 := h2.Sum32()

		pos1 := hash1 % blockpack.ColumnNameBloomBits
		pos2 := hash2 % blockpack.ColumnNameBloomBits

		byteIdx1 := pos1 / 8
		bit1 := pos1 % 8
		byteIdx2 := pos2 / 8
		bit2 := pos2 % 8

		// Both bits must be set for the column to potentially exist
		return (bloom[byteIdx1]&(1<<bit1)) != 0 && (bloom[byteIdx2]&(1<<bit2)) != 0
	}

	// First try exact match (for backwards compatibility)
	if checkName(columnName) {
		return true
	}

	// Check all type variants (columnName:0, columnName:1, etc.)
	// ColumnType is currently 0-8 (9 types), but check up to 15 for future expansion
	for typ := 0; typ <= 15; typ++ {
		suffixedName := fmt.Sprintf("%s:%d", columnName, typ)
		if checkName(suffixedName) {
			return true
		}
	}

	return false
}

func expandPredicateColumns(predicates *QueryPredicates, column string) []string {
	if strings.HasPrefix(column, vm.UnscopedColumnPrefix) {
		if predicates == nil {
			return nil
		}
		return predicates.UnscopedColumnNames[column]
	}
	return []string{column}
}

func hasDedicatedIndexInFile(reader *blockpackio.Reader, predicates *QueryPredicates) bool {
	for col := range predicates.DedicatedColumns {
		for _, scoped := range expandPredicateColumns(predicates, col) {
			if len(reader.DedicatedValues(scoped)) > 0 {
				return true
			}
		}
	}
	for col := range predicates.DedicatedColumnsRegex {
		for _, scoped := range expandPredicateColumns(predicates, col) {
			if len(reader.DedicatedValues(scoped)) > 0 {
				return true
			}
		}
	}
	for col := range predicates.DedicatedRanges {
		for _, scoped := range expandPredicateColumns(predicates, col) {
			if len(reader.DedicatedValues(scoped)) > 0 {
				return true
			}
		}
	}
	return false
}

// valueMinMaxAnyMatch checks if any of the given values could exist in the column
// based on its min/max statistics. This provides precise pruning for equality predicates.
//
//nolint:dupl // Block selection strategy; duplication is intentional for different criteria
func valueMinMaxAnyMatch(col *blockpack.Column, values []vm.Value) bool {
	// If we can't determine a match from min/max, conservatively return true
	supportedFound := false

	for _, v := range values {
		switch col.Type {
		case blockpack.ColumnTypeString:
			if v.Type == vm.TypeString {
				if s, ok := v.Data.(string); ok {
					supportedFound = true
					// Check if value is in range [StringMin, StringMax]
					if s >= col.Stats.StringMin && s <= col.Stats.StringMax {
						return true
					}
				}
			}
		case blockpack.ColumnTypeInt64:
			if v.Type == vm.TypeInt || v.Type == vm.TypeDuration {
				if val, ok := valueToInt64(v.Data); ok {
					supportedFound = true
					// Check if value is in range [IntMin, IntMax]
					if val >= col.Stats.IntMin && val <= col.Stats.IntMax {
						return true
					}
				}
			}
		case blockpack.ColumnTypeUint64:
			if v.Type == vm.TypeInt || v.Type == vm.TypeDuration {
				if val, ok := valueToInt64(v.Data); ok && val >= 0 {
					uval := uint64(val)
					supportedFound = true
					// Check if value is in range [UintMin, UintMax]
					if uval >= col.Stats.UintMin && uval <= col.Stats.UintMax {
						return true
					}
				}
			}
		case blockpack.ColumnTypeFloat64:
			if v.Type == vm.TypeFloat {
				if f, ok := v.Data.(float64); ok {
					supportedFound = true
					// Check if value is in range [FloatMin, FloatMax]
					if f >= col.Stats.FloatMin && f <= col.Stats.FloatMax {
						return true
					}
				}
			}
		case blockpack.ColumnTypeBool:
			if v.Type == vm.TypeBool {
				if b, ok := v.Data.(bool); ok {
					supportedFound = true
					// Check if value matches either BoolMin or BoolMax
					if b == col.Stats.BoolMin || b == col.Stats.BoolMax {
						return true
					}
				}
			}
		case blockpack.ColumnTypeBytes:
			// Bytes comparison is more expensive, skip min/max check
			// Fall through to scanning
			continue
		}
	}

	// If none of the values had a supported type, conservatively return true
	// to fall back to actual scanning
	return !supportedFound
}

func blockMightMatchValuePredicates(block *blockpackio.Block, predicates *QueryPredicates) bool {
	// INTERSECTION logic: block must match ALL predicates (AND query).
	// Note: This function is only called when !predicates.HasOROperations (lines 894, 1033),
	// so we only implement AND logic here. OR queries are handled elsewhere.
	for columnName, values := range predicates.AttributeEquals {
		// For unscoped attributes, check all possible column names
		possibleColumns := GetPossibleColumnNames(columnName)
		found := false

		for _, possibleColumn := range possibleColumns {
			col := block.GetColumn(possibleColumn)
			if col == nil {
				continue
			}
			if !col.Stats.HasValues {
				continue
			}
			minMaxMatch := valueMinMaxAnyMatch(col, values)
			if minMaxMatch {
				found = true
				break
			}
		}

		if !found {
			return false
		}
	}
	return true
}

// ValueBloomResult describes why a block passed or failed value min/max checks
// Note: Name retained for compatibility, but bloom filters have been removed.
// This now only checks min/max statistics, not bloom filters.
//
//nolint:govet,betteralign // Struct field order optimized for readability, not memory
type ValueBloomResult struct {
	PruneReason BlockPruningReason // Specific reason if !Matches
	Matches     bool               // True if block might contain matching values
}

func blockMatchesValueBloom(
	reader *blockpackio.Reader,
	blockIdx int,
	predicates map[string][]vm.Value,
	columnNames []string,
) ValueBloomResult {
	// Build a list of all possible column names to check for stats
	// (handling unscoped attributes that can map to multiple column names)
	allColumnNames := make([]string, 0, len(columnNames)*2)
	for _, colName := range columnNames {
		possibleNames := GetPossibleColumnNames(colName)
		allColumnNames = append(allColumnNames, possibleNames...)
	}

	stats, err := reader.BlockColumnStats(blockIdx, allColumnNames)
	if err != nil {
		return ValueBloomResult{Matches: true} // fall back to scanning the block
	}

	for columnName, values := range predicates {
		// For unscoped attributes, check all possible column names
		possibleColumns := GetPossibleColumnNames(columnName)
		foundColumn := false
		minMaxFailed := false

		for _, possibleColumn := range possibleColumns {
			entry, ok := stats[possibleColumn]
			if !ok {
				continue
			}
			if !entry.Stats.HasValues {
				continue
			}

			foundColumn = true

			// Check min/max for value range pruning
			minMaxMatch := valueMinMaxAnyMatchStats(entry, values)
			if !minMaxMatch {
				minMaxFailed = true
				continue
			}
			// Found a matching column with matching values
			foundColumn = true
			minMaxFailed = false
			break
		}

		if !foundColumn || minMaxFailed {
			// Determine most specific pruning reason
			if minMaxFailed {
				return ValueBloomResult{Matches: false, PruneReason: PruneValueMinMaxMiss}
			}
			// Column not found at all
			return ValueBloomResult{Matches: false, PruneReason: PruneColumnBloomMiss}
		}
	}
	return ValueBloomResult{Matches: true}
}

// valueMinMaxAnyMatchStats checks if any of the given values could exist in the column
// based on its min/max statistics. This provides precise pruning for equality predicates.
//
//nolint:dupl // Block selection strategy; duplication is intentional for different criteria
func valueMinMaxAnyMatchStats(entry blockpack.ColumnStatsWithType, values []vm.Value) bool {
	//nolint:dupl // Block selection strategy; duplication is intentional for different criteria
	// If we can't determine a match from min/max, conservatively return true
	supportedFound := false

	for _, v := range values {
		switch entry.Type {
		case blockpack.ColumnTypeString:
			if v.Type == vm.TypeString {
				if s, ok := v.Data.(string); ok {
					supportedFound = true
					// Check if value is in range [StringMin, StringMax]
					if s >= entry.Stats.StringMin && s <= entry.Stats.StringMax {
						return true
					}
				}
			}
		case blockpack.ColumnTypeInt64:
			if v.Type == vm.TypeInt || v.Type == vm.TypeDuration {
				if val, ok := valueToInt64(v.Data); ok {
					supportedFound = true
					// Check if value is in range [IntMin, IntMax]
					if val >= entry.Stats.IntMin && val <= entry.Stats.IntMax {
						return true
					}
				}
			}
		case blockpack.ColumnTypeUint64:
			if v.Type == vm.TypeInt || v.Type == vm.TypeDuration {
				if val, ok := valueToInt64(v.Data); ok && val >= 0 {
					uval := uint64(val)
					supportedFound = true
					// Check if value is in range [UintMin, UintMax]
					if uval >= entry.Stats.UintMin && uval <= entry.Stats.UintMax {
						return true
					}
				}
			}
		case blockpack.ColumnTypeFloat64:
			if v.Type == vm.TypeFloat {
				if f, ok := v.Data.(float64); ok {
					supportedFound = true
					// Check if value is in range [FloatMin, FloatMax]
					if f >= entry.Stats.FloatMin && f <= entry.Stats.FloatMax {
						return true
					}
				}
			}
		case blockpack.ColumnTypeBool:
			if v.Type == vm.TypeBool {
				if b, ok := v.Data.(bool); ok {
					supportedFound = true
					// Check if value matches either BoolMin or BoolMax
					if b == entry.Stats.BoolMin || b == entry.Stats.BoolMax {
						return true
					}
				}
			}
		case blockpack.ColumnTypeBytes:
			// Bytes comparison is more expensive, skip min/max check for now
			// Fall through to bloom filter check
			continue
		}
	}

	// If none of the values had a supported type, conservatively return true
	// to fall back to bloom filter and actual scanning
	return !supportedFound
}

func valueToInt64(v any) (int64, bool) {
	switch val := v.(type) {
	case int64:
		return val, true
	case int:
		return int64(val), true
	}
	return 0, false
}

// pruneBlocksByTimeRange filters blocks based on time range, updating decisions and returning candidates.
func pruneBlocksByTimeRange(
	blocks []*blockpackio.Block,
	opts QueryOptions,
	decisions []BlockPruningDecision,
	arena *arena.Arena,
) aslice.Slice[int] {
	candidate := aslice.Make[int](arena, 0)
	for idx, block := range blocks {
		startsBeforeRange := opts.StartTime > 0 && block.MaxStart < uint64(opts.StartTime)
		endsAfterRange := opts.EndTime > 0 && block.MinStart > uint64(opts.EndTime)
		if startsBeforeRange {
			decisions[idx].Included = false
			decisions[idx].PrunedReason = PruneTimeRangeBeforeQuery
			continue
		}
		if endsAfterRange {
			decisions[idx].Included = false
			decisions[idx].PrunedReason = PruneTimeRangeAfterQuery
			continue
		}
		candidate = candidate.AppendOne(arena, idx)
	}
	return candidate
}

// pruneBlocksByValueStats prunes blocks using per-block value statistics (v10 feature).
// Checks if attribute values in predicates could exist in blocks based on bloom filters and min/max ranges.
// Returns the filtered candidate list.
func pruneBlocksByValueStats(
	reader *blockpackio.Reader,
	predicates *QueryPredicates,
	candidate aslice.Slice[int],
	decisions []BlockPruningDecision,
	arena *arena.Arena,
) aslice.Slice[int] {
	// Only apply for AttributeEquals predicates (non-OR queries)
	if predicates == nil || len(predicates.AttributeEquals) == 0 || predicates.HasOROperations {
		return candidate
	}

	filtered := aslice.Make[int](arena, 0)

	for _, blockIdx := range candidate.Raw() {
		// Get value statistics for this block
		valueStats := reader.BlockValueStats(blockIdx)
		if len(valueStats) == 0 {
			// No value stats available (v9 file or stats not tracked) - include block
			filtered = filtered.AppendOne(arena, blockIdx)
			continue
		}

		// Check if all required attributes match
		matchesAll := true
		for attrName, values := range predicates.AttributeEquals {
			stats, hasStats := valueStats[attrName]
			if !hasStats {
				// Attribute not tracked in stats - include block (conservative)
				continue
			}

			// Check if ANY of the query values matches the block stats
			matchesAny := false
			for _, queryValue := range values {
				// Use the Data field directly (already contains the Go value)
				if stats.MayContainValue(queryValue.Data) {
					matchesAny = true
					break
				}
			}

			if !matchesAny {
				// None of the query values match this attribute's stats - prune block
				matchesAll = false
				break
			}
		}

		if matchesAll {
			filtered = filtered.AppendOne(arena, blockIdx)
		} else {
			decisions[blockIdx].Included = false
			decisions[blockIdx].PrunedReason = PruneBlockValuePredicate
		}
	}

	return filtered
}

// pruneBlocksByBloomFilter applies column bloom filter and value range pruning for AttributeEquals predicates.
// Returns the filtered candidate list.
func pruneBlocksByBloomFilter(
	reader *blockpackio.Reader,
	predicates *QueryPredicates,
	candidate aslice.Slice[int],
	decisions []BlockPruningDecision,
	arena *arena.Arena,
) aslice.Slice[int] {
	if len(predicates.AttributeEquals) == 0 || predicates.HasOROperations {
		return candidate
	}

	blocks := reader.Blocks()
	filtered := aslice.Make[int](arena, 0)
	columnNames := aslice.Make[string](arena, 0)
	for columnName := range predicates.AttributeEquals {
		columnNames = columnNames.AppendOne(arena, columnName)
	}

	for _, blockIdx := range candidate.Raw() {
		block := blocks[blockIdx]

		// Check column bloom first
		hasAllColumns := true
		for columnName := range predicates.AttributeEquals {
			possibleColumns := GetPossibleColumnNames(columnName)
			found := false
			for _, possibleColumn := range possibleColumns {
				bloomResult := checkColumnBloom(
					(*[blockpack.ColumnNameBloomBits / 8]byte)(&block.ColumnNameBloom),
					possibleColumn,
				)
				if bloomResult {
					found = true
					break
				}
			}
			if !found {
				hasAllColumns = false
				break
			}
		}

		if !hasAllColumns {
			decisions[blockIdx].Included = false
			decisions[blockIdx].PrunedReason = PruneColumnBloomMiss
			continue
		}

		// Check value min/max
		valueBloomResult := blockMatchesValueBloom(reader, blockIdx, predicates.AttributeEquals, columnNames.Raw())
		if !valueBloomResult.Matches {
			decisions[blockIdx].Included = false
			decisions[blockIdx].PrunedReason = valueBloomResult.PruneReason
			continue
		}

		filtered = filtered.AppendOne(arena, blockIdx)
	}
	return filtered
}

// pruneBlocksByRegexPredicates applies regex predicates on dedicated columns (AND logic only).
// Returns the filtered current set (as map for efficient intersection).
func pruneBlocksByRegexPredicates(
	reader *blockpackio.Reader,
	predicates *QueryPredicates,
	current map[int]struct{},
	decisions []BlockPruningDecision,
	arena *arena.Arena,
) (map[int]struct{}, bool) {
	if predicates.HasOROperations {
		return current, false
	}

	appliedDedicatedRegex := false
	for column, pattern := range predicates.DedicatedColumnsRegex {
		re, err := regexp.Compile(pattern)
		if err != nil {
			continue
		}

		scopedColumns := expandPredicateColumns(predicates, column)
		if len(scopedColumns) == 0 {
			continue
		}

		filtered := make(map[int]struct{})
		appliedPredicate := false
		for _, scoped := range scopedColumns {
			if len(reader.DedicatedValues(scoped)) == 0 {
				continue
			}

			appliedDedicatedRegex = true
			appliedPredicate = true
			matchingBlocks := reader.BlocksForDedicatedRegex(scoped, re, arena)
			for _, blockID := range matchingBlocks {
				if _, allowed := current[blockID]; allowed {
					filtered[blockID] = struct{}{}
				}
			}
		}
		if !appliedPredicate {
			continue
		}

		// Mark pruned blocks - only if not already pruned
		for id := range current {
			if _, kept := filtered[id]; !kept {
				if decisions[id].Included {
					decisions[id].Included = false
					decisions[id].PrunedReason = PruneDedicatedIndexMiss
				}
			}
		}
		current = filtered
	}

	return current, appliedDedicatedRegex
}

// checkFloatInRange checks if a float value is within the specified range.
func checkFloatInRange(value float64, rangePred *vm.RangePredicate) bool {
	var minVal, maxVal *float64
	if rangePred.MinValue != nil {
		if v, ok := rangePred.MinValue.Data.(float64); ok {
			minVal = &v
		} else if v, ok := rangePred.MinValue.Data.(int64); ok {
			fv := float64(v)
			minVal = &fv
		}
	}
	if rangePred.MaxValue != nil {
		if v, ok := rangePred.MaxValue.Data.(float64); ok {
			maxVal = &v
		} else if v, ok := rangePred.MaxValue.Data.(int64); ok {
			fv := float64(v)
			maxVal = &fv
		}
	}

	if minVal != nil {
		if rangePred.MinInclusive {
			if value < *minVal {
				return false
			}
		} else {
			if value <= *minVal {
				return false
			}
		}
	}
	if maxVal != nil {
		if rangePred.MaxInclusive {
			if value > *maxVal {
				return false
			}
		} else {
			if value >= *maxVal {
				return false
			}
		}
	}

	return true
}

// checkIntInRange checks if an int64 value is within the specified range.
func checkIntInRange(value int64, rangePred *vm.RangePredicate) bool {
	var minVal, maxVal *int64
	if rangePred.MinValue != nil {
		if v, ok := rangePred.MinValue.Data.(int64); ok {
			minVal = &v
		} else if v, ok := rangePred.MinValue.Data.(uint64); ok {
			// Check for overflow - uint64 values > MaxInt64 cannot be satisfied by any int64
			if v > math.MaxInt64 {
				// This range is unsatisfiable for int64 values
				return false
			}
			iv := int64(v)
			minVal = &iv
		}
	}
	if rangePred.MaxValue != nil {
		if v, ok := rangePred.MaxValue.Data.(int64); ok {
			maxVal = &v
		} else if v, ok := rangePred.MaxValue.Data.(uint64); ok {
			// Check for overflow - uint64 values > MaxInt64 cannot be safely converted
			// Skip this bound if it cannot be safely converted to int64
			if v <= math.MaxInt64 {
				iv := int64(v)
				maxVal = &iv
			}
		}
	}

	if minVal != nil {
		if rangePred.MinInclusive {
			if value < *minVal {
				return false
			}
		} else {
			if value <= *minVal {
				return false
			}
		}
	}
	if maxVal != nil {
		if rangePred.MaxInclusive {
			if value > *maxVal {
				return false
			}
		} else {
			if value >= *maxVal {
				return false
			}
		}
	}

	return true
}

// addBlocksForDedicatedRangesUnion adds blocks matching range predicates to the matched set (union logic).
func addBlocksForDedicatedRangesUnion(
	reader *blockpackio.Reader,
	predicates *QueryPredicates,
	current map[int]struct{},
	matched map[int]struct{},
	arena *arena.Arena,
) {
	for column, rangePred := range predicates.DedicatedRanges {
		scopedColumns := expandPredicateColumns(predicates, column)
		if len(scopedColumns) == 0 {
			continue
		}

		for _, scoped := range scopedColumns {
			if len(reader.DedicatedValues(scoped)) == 0 {
				continue
			}
			colType, ok := reader.DedicatedColumnTypeFromFile(scoped)
			if !ok {
				continue
			}

			// Only handle range-bucketed column types or low-cardinality numeric columns
			if colType != blockpack.ColumnTypeRangeDuration && colType != blockpack.ColumnTypeRangeInt64 &&
				colType != blockpack.ColumnTypeRangeUint64 && colType != blockpack.ColumnTypeInt64 &&
				colType != blockpack.ColumnTypeUint64 && colType != blockpack.ColumnTypeFloat64 {
				continue
			}

			// Get blocks for matching values
			colBlocks := make(map[int]struct{})

			// Get all indexed values for this column
			dedicatedValues := reader.DedicatedValues(scoped)

			// For float columns, handle float comparisons
			if colType == blockpack.ColumnTypeFloat64 {
				// For each indexed value, check if it's in range
				for _, key := range dedicatedValues {
					// Extract float64 value from key data
					if len(key.Data()) < 8 {
						continue
					}
					value := math.Float64frombits(binary.LittleEndian.Uint64(key.Data()))

					if checkFloatInRange(value, rangePred) {
						// Get blocks for this value
						for _, blockID := range reader.BlocksForDedicated(scoped, key, arena) {
							if _, allowed := current[blockID]; allowed {
								colBlocks[blockID] = struct{}{}
							}
						}
					}
				}
			} else {
				// For int columns, handle int comparisons
				// For each indexed value, check if it's in range
				for _, key := range dedicatedValues {
					// Extract int64 value from key data
					if len(key.Data()) < 8 {
						continue
					}
					value := int64(binary.LittleEndian.Uint64(key.Data())) //nolint:gosec

					if checkIntInRange(value, rangePred) {
						// Get blocks for this value
						for _, blockID := range reader.BlocksForDedicated(scoped, key, arena) {
							if _, allowed := current[blockID]; allowed {
								colBlocks[blockID] = struct{}{}
							}
						}
					}
				}
			}

			// Union with matched set
			for id := range colBlocks {
				matched[id] = struct{}{}
			}
		}
	}
}

// selectBlocksWithUnionLogic applies union logic for OR operations or multiple dedicated columns.
// Returns non-nil plan if we should return early, nil if we should continue to intersection logic.
func selectBlocksWithUnionLogic(
	reader *blockpackio.Reader,
	predicates *QueryPredicates,
	current map[int]struct{},
	decisions []BlockPruningDecision,
	stages aslice.Slice[PruningStageStats],
	beforeDedicated int,
	totalBlocks int,
	summary PredicateSummary,
	arena *arena.Arena,
) ([]int, *QueryPlan) {
	useUnion := predicates.HasOROperations || len(predicates.DedicatedColumns) > 1
	if !useUnion {
		return nil, nil
	}

	matched := make(map[int]struct{})

	// Special handling for trace:id using trace block index (v8+)
	traceIDUsedIndex := handleTraceIDUnion(reader, predicates, current, matched)

	// Add blocks from DedicatedColumns
	allValuesIndexed := processDedicatedColumnsUnion(reader, predicates, current, matched, traceIDUsedIndex, arena)

	// Add blocks from DedicatedRanges
	addBlocksForDedicatedRangesUnion(reader, predicates, current, matched, arena)

	// Add blocks from DedicatedColumnsRegex
	handleDedicatedRegexUnion(reader, predicates, current, matched, arena)

	// Note: We don't apply bloom filter checks to dedicated columns here
	// See explanation in intersection logic above

	// CRITICAL BUG FIX: Fall back to scanning all blocks if:
	// 1. No blocks were matched by dedicated indexes (len(matched) == 0), OR
	// 2. Not all values were found in dedicated indexes (allValuesIndexed == false)
	//
	// Case 1: The columns don't have dedicated indexes at all
	// Case 2: Sparse sampling - some values weren't indexed but might exist in unsampled blocks
	//
	// Example of Case 2 bug (sparse sampling):
	//   Query: { status_code = 404 || status_code = 500 || status_code = 503 }
	//   - 404 is in sparse index → finds 10 blocks
	//   - 500, 503 not in sparse index (only ~30% sampled) → finds 0 blocks
	//   - Old code: returns 10 blocks (WRONG - misses 500/503 in unsampled blocks)
	//   - New code: falls back to scan all blocks (CORRECT)
	//
	// Example of complete index (test scenario):
	//   Query: { service = 'frontend' }
	//   - Complete dedicated index exists with all values
	//   - 'frontend' found → 1 block matched
	//   - allValuesIndexed == true → trust the index
	//
	// If not all OR-branch values could be represented via dedicated indexes, do not
	// use the partially populated `matched` set for pruning. Fall back to the caller's
	// normal (safe) block selection logic instead.
	if len(matched) == 0 || !allValuesIndexed {
		// No blocks matched by dedicated indexes - return all current blocks
		// and let the predicate evaluation during scanning do the filtering
		out := aslice.Make[int](arena, 0)
		for id := range current {
			out = out.AppendOne(arena, id)
		}
		outRaw := out.Raw()
		sort.Ints(outRaw)

		// Don't add a dedicated_index stage since we didn't actually use it
		plan := &QueryPlan{
			TotalBlocks:       totalBlocks,
			BlocksSelected:    len(outRaw),
			BlocksPruned:      totalBlocks - len(outRaw),
			PruningStages:     stages.Raw(),
			BlockDecisions:    decisions,
			PredicatesSummary: summary,
		}
		return outRaw, plan
	}

	// Mark pruned blocks - only if not already pruned
	for id := range current {
		if _, kept := matched[id]; !kept {
			if decisions[id].Included {
				decisions[id].Included = false
				decisions[id].PrunedReason = PruneDedicatedIndexMiss
			}
		}
	}

	out := aslice.Make[int](arena, 0)
	for id := range matched {
		out = out.AppendOne(arena, id)
	}
	outRaw := out.Raw()
	sort.Ints(outRaw)

	// Protect against integer underflow in pruning stats
	blocksPruned := 0
	if beforeDedicated >= len(outRaw) {
		blocksPruned = beforeDedicated - len(outRaw)
	}

	stages = stages.AppendOne(arena, PruningStageStats{
		StageName:       "dedicated_index",
		BlocksRemaining: len(outRaw),
		BlocksPruned:    blocksPruned,
	})

	plan := &QueryPlan{
		TotalBlocks:       totalBlocks,
		BlocksSelected:    len(outRaw),
		BlocksPruned:      totalBlocks - len(outRaw),
		PruningStages:     stages.Raw(),
		BlockDecisions:    decisions,
		PredicatesSummary: summary,
	}
	return outRaw, plan
}

// pruneBlocksByDedicatedIntersection applies intersection logic for dedicated columns (AND logic).
// Modifies current in place and returns flags for what was applied.
// extractInt64FromValue extracts an int64 value from a vm.Value for range comparisons
func extractInt64FromValue(val *vm.Value) (int64, bool) {
	switch val.Type {
	case vm.TypeInt, vm.TypeDuration:
		switch v := val.Data.(type) {
		case int64:
			return v, true
		case int:
			return int64(v), true
		}
	case vm.TypeBytes:
		// For bytes (trace:id, span:id), extract first 8 bytes as int64
		if bytes, ok := val.Data.([]byte); ok && len(bytes) >= 8 {
			return int64(binary.LittleEndian.Uint64(bytes[:8])), true //nolint:gosec
		}
	}
	return 0, false
}

// checkInt64InRange checks if a value is within the specified range
func checkInt64InRange(value int64, minVal, maxVal *int64, minInclusive, maxInclusive bool) bool {
	if minVal != nil {
		if minInclusive {
			if value < *minVal {
				return false
			}
		} else {
			if value <= *minVal {
				return false
			}
		}
	}
	if maxVal != nil {
		if maxInclusive {
			if value > *maxVal {
				return false
			}
		} else {
			if value >= *maxVal {
				return false
			}
		}
	}
	return true
}

// extractRangeBounds extracts min/max values from a vm.RangePredicate.
// Returns nil pointers if the range is unsatisfiable for int64 values.
func extractRangeBounds(rangePred *vm.RangePredicate) (minVal, maxVal *int64) {
	if rangePred.MinValue != nil {
		if v, ok := rangePred.MinValue.Data.(int64); ok {
			minVal = &v
		} else if v, ok := rangePred.MinValue.Data.(uint64); ok {
			// Check for overflow - uint64 values > MaxInt64 cannot be satisfied by any int64
			if v > math.MaxInt64 {
				// Return nil to signal unsatisfiable range
				// Callers should treat this as "no matches possible"
				return nil, nil
			}
			iv := int64(v)
			minVal = &iv
		}
	}
	if rangePred.MaxValue != nil {
		if v, ok := rangePred.MaxValue.Data.(int64); ok {
			maxVal = &v
		} else if v, ok := rangePred.MaxValue.Data.(uint64); ok {
			// Check for overflow - uint64 values > MaxInt64 cannot be safely converted
			// Skip this bound if it cannot be safely converted to int64
			if v <= math.MaxInt64 {
				iv := int64(v)
				maxVal = &iv
			}
		}
	}
	return minVal, maxVal
}

// processTraceIDDedicated handles the special fast path for trace:id using trace block index
func processTraceIDDedicated(
	reader *blockpackio.Reader,
	values []vm.Value,
	current map[int]struct{},
	decisions []BlockPruningDecision,
) (filtered map[int]struct{}, usedIndex bool) {
	// Check if trace block index is available at all
	if !reader.HasTraceBlockIndex() {
		return nil, false
	}

	colBlocks := make(map[int]struct{})
	foundValidTraceID := false

	for _, val := range values {
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
			foundValidTraceID = true
			blockIDs := reader.BlocksForTraceID(traceID)
			// blockIDs == nil means trace not found (which is valid - prune all blocks)
			// Empty slice means trace exists but in zero blocks (rare, but valid)
			for _, id := range blockIDs {
				if _, allowed := current[id]; allowed {
					colBlocks[id] = struct{}{}
				}
			}
		}
	}

	// If no valid trace IDs were provided, can't use the index
	if !foundValidTraceID {
		return nil, false
	}

	// Successfully used trace block index - apply intersection
	filtered = make(map[int]struct{})
	for id := range colBlocks {
		if _, allowed := current[id]; allowed {
			filtered[id] = struct{}{}
		}
	}

	// Mark pruned blocks
	for id := range current {
		if _, kept := filtered[id]; !kept {
			if decisions[id].Included {
				decisions[id].Included = false
				decisions[id].PrunedReason = PruneDedicatedIndexMiss
			}
		}
	}

	return filtered, true
}

// collectBlocksForRangeBuckets collects blocks for range-bucketed columns with exact values
func collectBlocksForRangeBuckets(
	reader *blockpackio.Reader,
	column string,
	colType blockpack.ColumnType,
	values []vm.Value,
	current map[int]struct{},
	arena *arena.Arena,
) map[int]struct{} {
	colBlocks := make(map[int]struct{})

	bucketMeta, err := reader.GetRangeBucketMetadata(column)
	if err != nil || bucketMeta == nil || len(bucketMeta.Boundaries) == 0 {
		// No bucket metadata - try direct value lookup
		for _, val := range values {
			key, ok := valueToDedicatedKey(colType, val, column)
			if !ok {
				continue
			}
			for _, id := range reader.BlocksForDedicated(column, key, arena) {
				if _, allowed := current[id]; allowed {
					colBlocks[id] = struct{}{}
				}
			}
		}
		return colBlocks
	}

	// Has bucket metadata - convert values to bucket IDs
	for _, val := range values {
		int64Val, found := extractInt64FromValue(&val)
		if !found {
			continue
		}

		// Find the bucket ID for this value
		bucketID := blockpack.GetBucketID(int64Val, bucketMeta.Boundaries)

		// Create bucket key
		key := blockpack.RangeBucketValueKey(bucketID, colType)

		// Get blocks for this bucket
		for _, id := range reader.BlocksForDedicated(column, key, arena) {
			if _, allowed := current[id]; allowed {
				colBlocks[id] = struct{}{}
			}
		}
	}

	return colBlocks
}

// collectBlocksForDirectValues collects blocks for non-range-bucketed columns
func collectBlocksForDirectValues(
	reader *blockpackio.Reader,
	column string,
	colType blockpack.ColumnType,
	values []vm.Value,
	current map[int]struct{},
	arena *arena.Arena,
) map[int]struct{} {
	colBlocks := make(map[int]struct{})

	for _, val := range values {
		key, ok := valueToDedicatedKey(colType, val, column)
		if !ok {
			continue
		}
		for _, id := range reader.BlocksForDedicated(column, key, arena) {
			if _, allowed := current[id]; allowed {
				colBlocks[id] = struct{}{}
			}
		}
	}

	return colBlocks
}

// intersectAndPruneBlocks intersects colBlocks with current and updates decisions
func intersectAndPruneBlocks(
	current map[int]struct{},
	colBlocks map[int]struct{},
	decisions []BlockPruningDecision,
) {
	for id := range current {
		if _, ok := colBlocks[id]; !ok {
			if decisions[id].Included {
				decisions[id].Included = false
				decisions[id].PrunedReason = PruneDedicatedIndexMiss
			}
			delete(current, id)
		}
	}
}

// collectBlocksForRangeWithBuckets collects blocks for range predicates on range-bucketed columns with metadata
func collectBlocksForRangeWithBuckets(
	reader *blockpackio.Reader,
	column string,
	colType blockpack.ColumnType,
	rangePred *vm.RangePredicate,
	bucketMeta *blockpackio.RangeBucketMetadata,
	current map[int]struct{},
	arena *arena.Arena,
) map[int]struct{} {
	colBlocks := make(map[int]struct{})

	minVal, maxVal := extractRangeBounds(rangePred)

	// Get bucket IDs that intersect with the range
	bucketIDs := blockpack.GetBucketsForRange(
		minVal,
		maxVal,
		rangePred.MinInclusive,
		rangePred.MaxInclusive,
		bucketMeta.Boundaries,
	)

	// Get blocks for each matching bucket
	for _, bucketID := range bucketIDs {
		key := blockpack.RangeBucketValueKey(bucketID, colType)
		for _, blockID := range reader.BlocksForDedicated(column, key, arena) {
			if _, allowed := current[blockID]; allowed {
				colBlocks[blockID] = struct{}{}
			}
		}
	}

	return colBlocks
}

// collectBlocksForRangeWithoutBuckets collects blocks for range predicates on range-bucketed columns without metadata
func collectBlocksForRangeWithoutBuckets(
	reader *blockpackio.Reader,
	column string,
	rangePred *vm.RangePredicate,
	current map[int]struct{},
	arena *arena.Arena,
) map[int]struct{} {
	// For range columns without bucket metadata, use direct value indexing.
	// When we have < 10 blocks, we collect ALL values per block (not just samples),
	// so the dedicated index is reliable. Scan each value in the range.
	colBlocks := make(map[int]struct{})
	minVal, maxVal := extractRangeBounds(rangePred)

	dedicatedValues := reader.DedicatedValues(column)
	for _, key := range dedicatedValues {
		if len(key.Data()) < 8 {
			continue
		}
		value := int64(binary.LittleEndian.Uint64(key.Data())) //nolint:gosec

		if checkInt64InRange(value, minVal, maxVal, rangePred.MinInclusive, rangePred.MaxInclusive) {
			for _, blockID := range reader.BlocksForDedicated(column, key, arena) {
				if _, allowed := current[blockID]; allowed {
					colBlocks[blockID] = struct{}{}
				}
			}
		}
	}

	return colBlocks
}

// collectBlocksForNonRangeColumnWithRange collects blocks for range predicates on non-range column types
func collectBlocksForNonRangeColumnWithRange(
	reader *blockpackio.Reader,
	column string,
	colType blockpack.ColumnType,
	rangePred *vm.RangePredicate,
	current map[int]struct{},
	arena *arena.Arena,
) map[int]struct{} {
	// For non-range-bucketed columns, the dedicated index only stores ONE representative value per block.
	// This makes it unreliable for range queries - absence of a value doesn't mean the block doesn't contain
	// matching spans. Therefore, return ALL current blocks and let scan-phase filtering handle value matching.
	return current
}

func pruneBlocksByDedicatedIntersection(
	reader *blockpackio.Reader,
	predicates *QueryPredicates,
	current map[int]struct{},
	decisions []BlockPruningDecision,
	arena *arena.Arena,
) (appliedDedicated, appliedDedicatedRanges bool) {
	appliedDedicated = false
	for column, values := range predicates.DedicatedColumns {
		// Skip array columns - they need element-wise matching during scan phase
		if isArrayColumn(column) {
			continue
		}

		// Special fast path for trace:id using trace block index (v8+)
		if column == IntrinsicTraceID && len(values) > 0 {
			filtered, usedIndex := processTraceIDDedicated(reader, values, current, decisions)
			if usedIndex {
				appliedDedicated = true
				current = filtered
				continue // Skip regular dedicated column handling
			}
		}

		scopedColumns := expandPredicateColumns(predicates, column)
		if len(scopedColumns) == 0 {
			continue
		}

		colBlocksUnion := make(map[int]struct{})
		appliedPredicate := false
		usedRangeColumn := false
		for _, scoped := range scopedColumns {
			// Skip array columns - they need element-wise matching during scan phase
			if isArrayColumn(scoped) {
				continue
			}
			// Regular dedicated column handling
			if len(reader.DedicatedValues(scoped)) == 0 {
				continue
			}
			// Use type from file's dedicated index (supports both explicit and auto-detected columns)
			colType, ok := reader.DedicatedColumnTypeFromFile(scoped)
			if !ok {
				continue
			}
			appliedDedicated = true
			appliedPredicate = true

			var colBlocks map[int]struct{}
			if blockpack.IsRangeColumnType(colType) {
				colBlocks = collectBlocksForRangeBuckets(reader, scoped, colType, values, current, arena)
				usedRangeColumn = true
			} else {
				colBlocks = collectBlocksForDirectValues(reader, scoped, colType, values, current, arena)
			}

			for id := range colBlocks {
				colBlocksUnion[id] = struct{}{}
			}
		}
		if !appliedPredicate {
			continue
		}

		// RANGE COLUMN FIX: Range-bucketed columns (int64, uint64, duration) build their
		// dedicated index from a value store limited to maxStoredValues=10000 entries.
		// At scale (many blocks), this means only a fraction of blocks get indexed,
		// making the index incomplete. Using intersection logic with an incomplete index
		// causes false negatives (blocks containing matching values get pruned).
		// Skip intersection for range columns and let scanning handle filtering.
		if usedRangeColumn {
			continue
		}

		// Intersection logic for non-range dedicated columns (strings, bools):
		// These use complete (non-sampled) indexes, so intersection is safe.
		intersectAndPruneBlocks(current, colBlocksUnion, decisions)
	}

	// Handle DedicatedRanges for intersection logic
	appliedDedicatedRanges = false
	for column, rangePred := range predicates.DedicatedRanges {
		scopedColumns := expandPredicateColumns(predicates, column)
		if len(scopedColumns) == 0 {
			continue
		}

		colBlocksUnion := make(map[int]struct{})
		appliedPredicate := false
		for _, scoped := range scopedColumns {
			dedicatedValues := reader.DedicatedValues(scoped)
			if len(dedicatedValues) == 0 {
				continue
			}
			colType, ok := reader.DedicatedColumnTypeFromFile(scoped)
			if !ok {
				continue
			}

			appliedDedicatedRanges = true
			appliedPredicate = true

			var colBlocks map[int]struct{}
			if blockpack.IsRangeColumnType(colType) {
				bucketMeta, err := reader.GetRangeBucketMetadata(scoped)
				if err != nil || bucketMeta == nil || len(bucketMeta.Boundaries) == 0 {
					colBlocks = collectBlocksForRangeWithoutBuckets(reader, scoped, rangePred, current, arena)
				} else {
					colBlocks = collectBlocksForRangeWithBuckets(reader, scoped, colType, rangePred, bucketMeta, current, arena)
				}
			} else {
				colBlocks = collectBlocksForNonRangeColumnWithRange(reader, scoped, colType, rangePred, current, arena)
			}

			for id := range colBlocks {
				colBlocksUnion[id] = struct{}{}
			}
		}

		if !appliedPredicate {
			continue
		}

		// CRITICAL BUG FIX: Dedicated range indexes use sparse sampling and are unreliable
		// for intersection logic. Same issue as DedicatedColumns above.
		//
		// Skip dedicated range index intersection to avoid under-counting.
		if len(colBlocksUnion) == 0 {
			// Dedicated range index didn't help - skip intersection, keep all blocks
			continue
		}

		// SPARSE SAMPLING FIX: Don't use dedicated range indexes for intersection!
		// Same reasoning as DedicatedColumns - sparse sampling causes under-counting.
		// Let scanning handle filtering correctly.
		continue

		// The code below is DISABLED because it causes under-counting:
		//
		// // Intersect with current candidate set
		// intersectAndPruneBlocks(current, colBlocksUnion, decisions)
	}

	return appliedDedicated, appliedDedicatedRanges
}

// selectBlocksWithPlan is like selectBlocks but also tracks pruning decisions for query plan
func selectBlocksWithPlan(
	reader *blockpackio.Reader,
	predicates *QueryPredicates,
	opts QueryOptions,
	arena *arena.Arena,
) ([]int, *QueryPlan) {
	blocks := reader.Blocks()
	totalBlocks := len(blocks)

	// Track pruning decisions for each block (arena allocation)
	decisions := aslice.Make[BlockPruningDecision](arena, totalBlocks).Raw()
	for i, block := range blocks {
		decisions[i] = BlockPruningDecision{
			BlockID:   i,
			Included:  true, // Start optimistic
			SpanCount: block.SpanCount(),
		}
	}

	// Build predicates summary
	hasColumnFilters := false
	hasDedicatedIndex := false
	if predicates != nil {
		hasColumnFilters = len(predicates.AttributeEquals) > 0 || len(predicates.DedicatedColumns) > 0 ||
			len(predicates.DedicatedColumnsRegex) > 0 ||
			len(predicates.DedicatedRanges) > 0
		hasDedicatedIndex = len(predicates.DedicatedColumns) > 0 || len(predicates.DedicatedColumnsRegex) > 0 ||
			len(predicates.DedicatedRanges) > 0
	}

	summary := PredicateSummary{
		HasTimeFilter:     opts.StartTime > 0 || opts.EndTime > 0,
		HasColumnFilters:  hasColumnFilters,
		HasDedicatedIndex: hasDedicatedIndex,
		HasOROperations:   predicates != nil && predicates.HasOROperations,
	}

	if predicates != nil {
		for col := range predicates.AttributeEquals {
			summary.ColumnFilters = append(summary.ColumnFilters, col)
		}
		for col := range predicates.DedicatedColumns {
			summary.ColumnFilters = append(summary.ColumnFilters, col)
			summary.DedicatedColumns = append(summary.DedicatedColumns, col)
		}
		for col := range predicates.DedicatedColumnsRegex {
			summary.ColumnFilters = append(summary.ColumnFilters, col)
			summary.DedicatedColumns = append(summary.DedicatedColumns, col)
		}
		for col := range predicates.DedicatedRanges {
			summary.ColumnFilters = append(summary.ColumnFilters, col)
			summary.DedicatedColumns = append(summary.DedicatedColumns, col)
		}
	}

	stages := aslice.Make[PruningStageStats](arena, 0)

	// Step 1: Time range filtering
	candidate := pruneBlocksByTimeRange(blocks, opts, decisions, arena)
	stages = stages.AppendOne(arena, PruningStageStats{
		StageName:       "time_range",
		BlocksRemaining: candidate.Len(),
		BlocksPruned:    totalBlocks - candidate.Len(),
	})

	if predicates == nil {
		plan := &QueryPlan{
			TotalBlocks:       totalBlocks,
			BlocksSelected:    candidate.Len(),
			BlocksPruned:      totalBlocks - candidate.Len(),
			PruningStages:     stages.Raw(),
			BlockDecisions:    decisions,
			PredicatesSummary: summary,
		}
		return candidate.Raw(), plan
	}

	// Step 2: Value statistics pruning (v10 feature)
	beforeValueStats := candidate.Len()
	candidate = pruneBlocksByValueStats(reader, predicates, candidate, decisions, arena)

	// Only add stage if we actually did some pruning
	if beforeValueStats != candidate.Len() {
		stages = stages.AppendOne(arena, PruningStageStats{
			StageName:       "value_statistics",
			BlocksRemaining: candidate.Len(),
			BlocksPruned:    beforeValueStats - candidate.Len(),
		})
	}

	// Step 3: Column name bloom filter and value range pruning for AttributeEquals predicates
	beforeColumnBloom := candidate.Len()
	candidate = pruneBlocksByBloomFilter(reader, predicates, candidate, decisions, arena)

	// Only add stage if we actually did some pruning
	if beforeColumnBloom != candidate.Len() || len(predicates.AttributeEquals) > 0 {
		stages = stages.AppendOne(arena, PruningStageStats{
			StageName:       "column_bloom_and_value_filters",
			BlocksRemaining: candidate.Len(),
			BlocksPruned:    beforeColumnBloom - candidate.Len(),
		})
	}

	return selectBlocksWithDedicatedIndexPlan(
		reader,
		predicates,
		candidate,
		decisions,
		stages,
		totalBlocks,
		summary,
		arena,
	)
}

func selectBlocksWithDedicatedIndexPlan(
	reader *blockpackio.Reader,
	predicates *QueryPredicates,
	candidate aslice.Slice[int],
	decisions []BlockPruningDecision,
	stages aslice.Slice[PruningStageStats],
	totalBlocks int,
	summary PredicateSummary,
	arena *arena.Arena,
) ([]int, *QueryPlan) {
	// Step 4: Dedicated column index filtering
	if len(predicates.DedicatedColumns) == 0 && len(predicates.DedicatedColumnsRegex) == 0 &&
		len(predicates.DedicatedRanges) == 0 {
		plan := &QueryPlan{
			TotalBlocks:       totalBlocks,
			BlocksSelected:    candidate.Len(),
			BlocksPruned:      totalBlocks - candidate.Len(),
			PruningStages:     stages.Raw(),
			BlockDecisions:    decisions,
			PredicatesSummary: summary,
		}
		return candidate.Raw(), plan
	}

	// Check if ANY columns actually have dedicated indexes in the file
	// If none do, skip the dedicated index optimization entirely
	if !hasDedicatedIndexInFile(reader, predicates) {
		plan := &QueryPlan{
			TotalBlocks:       totalBlocks,
			BlocksSelected:    candidate.Len(),
			BlocksPruned:      totalBlocks - candidate.Len(),
			PruningStages:     stages.Raw(),
			BlockDecisions:    decisions,
			PredicatesSummary: summary,
		}
		return candidate.Raw(), plan
	}

	beforeDedicated := candidate.Len()
	current := make(map[int]struct{}, candidate.Len())
	for _, id := range candidate.Raw() {
		current[id] = struct{}{}
	}

	// Step 4a: Regex predicates on dedicated columns
	// Skip if using union logic (OR operations) - regex blocks will be handled in Step 4b
	current, appliedDedicatedRegex := pruneBlocksByRegexPredicates(reader, predicates, current, decisions, arena)

	// Step 4b: Exact value predicates on dedicated columns
	// Use union logic if query has OR operations or multiple dedicated columns
	if selected, plan := selectBlocksWithUnionLogic(reader, predicates, current, decisions, stages, beforeDedicated, totalBlocks, summary, arena); plan != nil {
		return selected, plan
	}

	// Step 4c: Intersection logic for dedicated columns
	appliedDedicated, appliedDedicatedRanges := pruneBlocksByDedicatedIntersection(
		reader,
		predicates,
		current,
		decisions,
		arena,
	)

	if !appliedDedicated && !appliedDedicatedRegex && !appliedDedicatedRanges {
		plan := &QueryPlan{
			TotalBlocks:       totalBlocks,
			BlocksSelected:    candidate.Len(),
			BlocksPruned:      totalBlocks - candidate.Len(),
			PruningStages:     stages.Raw(),
			BlockDecisions:    decisions,
			PredicatesSummary: summary,
		}
		return candidate.Raw(), plan
	}

	out := aslice.Make[int](arena, 0)
	for id := range current {
		out = out.AppendOne(arena, id)
	}
	outRaw := out.Raw()
	sort.Ints(outRaw)

	stages = stages.AppendOne(arena, PruningStageStats{
		StageName:       "dedicated_index",
		BlocksRemaining: len(outRaw),
		BlocksPruned:    beforeDedicated - len(outRaw),
	})

	plan := &QueryPlan{
		TotalBlocks:       totalBlocks,
		BlocksSelected:    len(outRaw),
		BlocksPruned:      totalBlocks - len(outRaw),
		PruningStages:     stages.Raw(),
		BlockDecisions:    decisions,
		PredicatesSummary: summary,
	}
	return outRaw, plan
}

// isArrayColumn checks if a column name represents an array column that stores multiple values.
// Array columns need element-wise matching during the scan phase and cannot be used for
// dedicated index intersection pruning.
// Uses an explicit hardcoded list for intrinsic array columns to avoid misclassifying
// non-array intrinsics like span:kind, span:status, span:name, etc.
func isArrayColumn(columnName string) bool {
	// Intrinsic array columns (explicit hardcoded list — must match writer/column_builder.go)
	switch columnName {
	case "event:name", "event:time_since_start", "event:dropped_attributes_count",
		"link:trace_id", "link:span_id", "link:trace_state", "link:dropped_attributes_count",
		"instrumentation:name", "instrumentation:version", "instrumentation:dropped_attributes_count":
		return true
	}
	// Array attribute columns (event., link., instrumentation. prefix attributes)
	if strings.HasPrefix(columnName, "event.") ||
		strings.HasPrefix(columnName, "link.") ||
		strings.HasPrefix(columnName, "instrumentation.") {
		return true
	}
	// Explicit array columns (resource/span arrays with -array suffix)
	if strings.HasSuffix(columnName, "-array") {
		return true
	}
	return false
}

// valueToStringKey converts a VM string value to a dedicated string key.
func valueToStringKey(v vm.Value) (blockpack.DedicatedValueKey, bool) {
	if v.Type == vm.TypeString {
		if s, ok := v.Data.(string); ok {
			return blockpack.StringValueKey(s), true
		}
	}
	return blockpack.DedicatedValueKey{}, false
}

// valueToUint64Key converts a VM int/duration value to a dedicated uint64 key.
func valueToUint64Key(v vm.Value) (blockpack.DedicatedValueKey, bool) {
	if v.Type == vm.TypeInt || v.Type == vm.TypeDuration {
		switch val := v.Data.(type) {
		case int64:
			if val < 0 {
				return blockpack.DedicatedValueKey{}, false
			}
			return blockpack.UintValueKey(uint64(val)), true
		case int:
			if val < 0 {
				return blockpack.DedicatedValueKey{}, false
			}
			return blockpack.UintValueKey(uint64(val)), true
		}
	}
	return blockpack.DedicatedValueKey{}, false
}

// valueToInt64Key converts a VM int value to a dedicated int64 key.
func valueToInt64Key(v vm.Value) (blockpack.DedicatedValueKey, bool) {
	if v.Type == vm.TypeInt {
		switch val := v.Data.(type) {
		case int64:
			return blockpack.IntValueKey(val), true
		case int:
			return blockpack.IntValueKey(int64(val)), true
		}
	}
	return blockpack.DedicatedValueKey{}, false
}

// valueToRangeInt64Key converts a VM int/duration value to a range-bucketed int64 key.
func valueToRangeInt64Key(v vm.Value, colType blockpack.ColumnType) (blockpack.DedicatedValueKey, bool) {
	if v.Type == vm.TypeInt || v.Type == vm.TypeDuration {
		switch val := v.Data.(type) {
		case int64:
			return blockpack.RangeInt64ValueKey(val, colType), true
		case int:
			return blockpack.RangeInt64ValueKey(int64(val), colType), true
		}
	}
	return blockpack.DedicatedValueKey{}, false
}

// valueToRangeUint64Key converts a VM int/duration value to a range-bucketed uint64 key.
func valueToRangeUint64Key(v vm.Value, colType blockpack.ColumnType) (blockpack.DedicatedValueKey, bool) {
	if v.Type == vm.TypeInt || v.Type == vm.TypeDuration {
		switch val := v.Data.(type) {
		case int64:
			if val < 0 {
				return blockpack.DedicatedValueKey{}, false
			}
			return blockpack.RangeInt64ValueKey(val, colType), true
		case int:
			if val < 0 {
				return blockpack.DedicatedValueKey{}, false
			}
			return blockpack.RangeInt64ValueKey(int64(val), colType), true
		}
	}
	return blockpack.DedicatedValueKey{}, false
}

// valueToBoolKey converts a VM bool value to a dedicated bool key.
func valueToBoolKey(v vm.Value) (blockpack.DedicatedValueKey, bool) {
	if v.Type == vm.TypeBool {
		if b, ok := v.Data.(bool); ok {
			return blockpack.BoolValueKey(b), true
		}
	}
	return blockpack.DedicatedValueKey{}, false
}

// valueToFloat64Key converts a VM float value to a dedicated float64 key.
func valueToFloat64Key(v vm.Value) (blockpack.DedicatedValueKey, bool) {
	if v.Type == vm.TypeFloat {
		if f, ok := v.Data.(float64); ok {
			return blockpack.FloatValueKey(f), true
		}
	}
	return blockpack.DedicatedValueKey{}, false
}

// valueToBytesKey converts a VM bytes value to a dedicated bytes key.
func valueToBytesKey(v vm.Value) (blockpack.DedicatedValueKey, bool) {
	if v.Type == vm.TypeBytes {
		if b, ok := v.Data.([]byte); ok {
			return blockpack.BytesValueKey(b), true
		}
	}
	return blockpack.DedicatedValueKey{}, false
}

func valueToDedicatedKey(
	colType blockpack.ColumnType,
	v vm.Value,
	columnName string,
) (blockpack.DedicatedValueKey, bool) {
	// Special handling for trace:id and span:id with RangeInt64 type
	// These are byte fields but stored as range-bucketed int64 (first 8 bytes)
	if (columnName == IntrinsicTraceID || columnName == IntrinsicSpanID) && colType == blockpack.ColumnTypeRangeInt64 {
		if v.Type == vm.TypeBytes {
			if bytes, ok := v.Data.([]byte); ok && len(bytes) >= 8 {
				// Extract first 8 bytes as int64
				val := int64(binary.LittleEndian.Uint64(bytes[:8])) //nolint:gosec
				return blockpack.RangeInt64ValueKey(val, colType), true
			}
		}
	}

	switch colType {
	case blockpack.ColumnTypeString:
		return valueToStringKey(v)
	case blockpack.ColumnTypeUint64:
		return valueToUint64Key(v)
	case blockpack.ColumnTypeRangeUint64:
		return valueToRangeUint64Key(v, colType)
	case blockpack.ColumnTypeInt64:
		return valueToInt64Key(v)
	case blockpack.ColumnTypeRangeInt64:
		return valueToRangeInt64Key(v, colType)
	case blockpack.ColumnTypeRangeDuration:
		return valueToRangeUint64Key(v, colType)
	case blockpack.ColumnTypeBool:
		return valueToBoolKey(v)
	case blockpack.ColumnTypeFloat64:
		return valueToFloat64Key(v)
	case blockpack.ColumnTypeBytes:
		return valueToBytesKey(v)
	}

	return blockpack.DedicatedValueKey{}, false
}
