package executor

// NOTE-036: planBlocks unifies the block-selection pipeline across all query paths.
// See NOTES.md §NOTE-036.

import (
	"bytes"
	"math"
	"strings"

	modules_reader "github.com/grafana/blockpack/internal/modules/blockio/reader"
	modules_shared "github.com/grafana/blockpack/internal/modules/blockio/shared"
	"github.com/grafana/blockpack/internal/modules/queryplanner"
	"github.com/grafana/blockpack/internal/vm"
)

// planBlocks runs the full block-selection pipeline for a query:
//  1. BuildPredicates — converts vm.Program predicates into planner predicates
//  2. PlanWithOptions — applies bloom/range-index pruning and time range filtering
//  3. fileLevelReject — fast-reject the entire file when bucketMin/bucketMax guarantees no match
//  4. BlocksFromIntrinsicTOC intersection — intrinsic-column fast reject (when available)
//
// NOTE-036: All query paths (Collect, ExecuteTraceMetrics, ExecuteLogMetrics,
// StreamLogs, CollectLogs) must use planBlocks to ensure intrinsic TOC pruning
// is active everywhere, not just in Collect.
func planBlocks(
	r *modules_reader.Reader,
	program *vm.Program,
	tr queryplanner.TimeRange,
	opts queryplanner.PlanOptions,
) *queryplanner.Plan {
	predicates := BuildPredicates(r, program)
	planner := queryplanner.NewPlanner(r)
	plan := planner.PlanWithOptions(predicates, tr, opts)

	// File-level fast reject: if the query value is guaranteed outside the
	// file's [bucketMin, bucketMax] range, skip all remaining pruning.
	if program != nil && program.Predicates != nil {
		if fileLevelReject(r, program.Predicates.Nodes) {
			plan.SelectedBlocks = nil
			plan.Explain = "file-level reject: query value outside column [bucketMin, bucketMax]"
			plan.PrunedByIndex = 0
			plan.PrunedByTime = 0
			plan.PrunedByFuse = 0
			return plan
		}
	}

	// File-level bloom reject: Fuse8 for service.name, compact bloom for trace:id.
	// NOTE-045: Checks equality predicates via FileBloom (Fuse8) and compact trace bloom.
	if program != nil && program.Predicates != nil {
		if fileLevelBloomReject(r, program.Predicates.Nodes) {
			plan.SelectedBlocks = nil
			plan.Explain = "file-level reject: bloom filter absence for equality predicate"
			plan.PrunedByIndex = 0
			plan.PrunedByTime = 0
			plan.PrunedByFuse = 0
			return plan
		}
	}

	// File-level and block-level vector centroid reject (VECTOR() predicates only).
	// If the file centroid is too distant from the query vector, skip the entire file.
	// If only some blocks are distant, prune those blocks.
	if program != nil && program.HasVector {
		plan.SelectedBlocks = fileLevelVectorPrune(r, program, plan.SelectedBlocks)
		if len(plan.SelectedBlocks) == 0 {
			plan.Explain = "file-level reject: vector centroid too distant"
			return plan
		}
	}

	// Intersect with intrinsic-column TOC when available.
	// Returns nil when no pruning is possible (no intrinsic section, no intrinsic
	// predicates, or all blocks survive), so we skip the intersection step in that case.
	if intrinsicBlocks := BlocksFromIntrinsicTOC(r, program); intrinsicBlocks != nil {
		keepSet := make(map[int]struct{}, len(intrinsicBlocks))
		for _, bi := range intrinsicBlocks {
			keepSet[bi] = struct{}{}
		}
		filtered := plan.SelectedBlocks[:0]
		for _, bi := range plan.SelectedBlocks {
			if _, ok := keepSet[bi]; ok {
				filtered = append(filtered, bi)
			}
		}
		plan.SelectedBlocks = filtered
	}

	// ColStats block pruning (NOTE-446, issue #364): skip blocks where a predicate column
	// is wholly absent (present_count == 0) or whose per-block numeric range cannot satisfy
	// the predicate's bound. Runs last so it refines the already-selected set with no extra
	// I/O beyond the lazily-fetched ColStats section.
	if program != nil && program.Predicates != nil && r.HasColStats() {
		plan.SelectedBlocks = pruneByColStats(r, program.Predicates.Nodes, plan.SelectedBlocks)
	}

	return plan
}

// pruneByColStats removes blocks from selected that cannot match any top-level AND
// predicate, using the per-block ColStats section. Only top-level AND leaves (and OR
// composites where every arm rejects) are considered; this is conservative — when a
// predicate cannot be evaluated against ColStats the block is kept.
func pruneByColStats(r *modules_reader.Reader, nodes []vm.RangeNode, selected []int) []int {
	if len(selected) == 0 || len(nodes) == 0 {
		return selected
	}
	out := selected[:0]
	for _, bi := range selected {
		cs := r.ColStats(bi)
		if cs == nil {
			out = append(out, bi)
			continue
		}
		reject := false
		for i := range nodes {
			if colStatsRejects(cs, &nodes[i]) {
				reject = true
				break
			}
		}
		if !reject {
			out = append(out, bi)
		}
	}
	return out
}

// colStatsRejects reports whether the block's column statistics guarantee that no row can
// satisfy node. AND composites reject if ANY child rejects; OR composites reject only if
// ALL children reject. Leaves reject on column absence (RequirePresent / equality / range
// against an absent column) or on a numeric range that cannot intersect the predicate.
func colStatsRejects(cs *modules_shared.BlockColStats, node *vm.RangeNode) bool {
	if len(node.Children) > 0 {
		if node.IsOR {
			for i := range node.Children {
				if !colStatsRejects(cs, &node.Children[i]) {
					return false
				}
			}
			return true
		}
		for i := range node.Children {
			if colStatsRejects(cs, &node.Children[i]) {
				return true
			}
		}
		return false
	}

	if node.Column == "" {
		return false
	}
	stat := cs.Lookup(node.Column)

	// A leaf that requires the column to be present, match a value, fall in a range, or
	// match a pattern can never match a block where the column is wholly absent
	// (present_count == 0 or no recorded stat).
	requiresPresence := node.RequirePresent ||
		len(node.Values) > 0 || node.Min != nil || node.Max != nil || node.Pattern != ""
	if requiresPresence {
		if stat == nil || stat.PresentCount == 0 {
			return true
		}
	}
	if stat == nil {
		return false
	}

	// Numeric range pruning: if the predicate has a numeric bound and the block's recorded
	// [min, max] cannot intersect it, the block cannot match. Only applied when ColStats
	// carries a numeric range for this column.
	//
	// NOTE-448: Dispatch numeric range pruning to a type-aware helper based on the value type
	// of the predicate bounds. This avoids applying uint64 bit comparison to signed int64
	// values (wrong for negatives) or float64 values.
	if stat.HasNumRange {
		if node.Min != nil || node.Max != nil {
			bound := node.Min
			if bound == nil {
				bound = node.Max
			}
			switch bound.Type {
			case vm.TypeFloat:
				if colStatsRejectsFloat64(stat, node) {
					return true
				}
			case vm.TypeInt:
				// NOTE-448: Int64 path uses signed comparison. Old files (HasNumRange=false
				// for Int64) are unaffected — this branch only fires when HasNumRange=true.
				if colStatsRejectsInt64(stat, node) {
					return true
				}
			default:
				// uint64, duration, and other unsigned types use the existing uint64 path.
				if node.Min != nil && numNodeBoundExceedsMax(node.Min, node.MinInclusive, stat.MaxNum) {
					return true
				}
				if node.Max != nil && numNodeBoundBelowMin(node.Max, node.MaxInclusive, stat.MinNum) {
					return true
				}
			}
		}
	}
	return false
}

// numNodeBoundExceedsMax reports whether a lower-bound predicate (Min) excludes the whole
// block: the smallest value the predicate admits is strictly greater than the block max.
func numNodeBoundExceedsMax(minVal *vm.Value, inclusive bool, blockMax uint64) bool {
	v, ok := valueAsUint64(minVal)
	if !ok {
		return false
	}
	if inclusive {
		return v > blockMax
	}
	return v >= blockMax
}

// numNodeBoundBelowMin reports whether an upper-bound predicate (Max) excludes the whole
// block: the largest value the predicate admits is strictly less than the block min.
func numNodeBoundBelowMin(maxVal *vm.Value, inclusive bool, blockMin uint64) bool {
	v, ok := valueAsUint64(maxVal)
	if !ok {
		return false
	}
	if inclusive {
		return v < blockMin
	}
	return v <= blockMin
}

// valueAsUint64 returns the uint64 representation of a numeric Value for comparison against
// ColStats numeric ranges, matching the writer's little-endian range-key encoding (the raw
// bits of an int64/uint64). Returns false for non-numeric values.
func valueAsUint64(v *vm.Value) (uint64, bool) {
	switch v.Type {
	case vm.TypeInt, vm.TypeDuration:
		if iv, ok := v.Data.(int64); ok {
			return uint64(iv), true //nolint:gosec
		}
		return 0, false
	default:
		return 0, false
	}
}

// fileLevelBloomReject returns true if file-level bloom filters guarantee that no span
// in the file can match the equality predicates in nodes.
// NOTE-045: Checks resource.service.name via FileBloom (Fuse8) and trace:id via compact bloom.
// AND semantics: reject if ANY leaf rejects. OR semantics: reject only if ALL children reject.
func fileLevelBloomReject(r *modules_reader.Reader, nodes []vm.RangeNode) bool {
	fb := r.FileBloom()
	for i := range nodes {
		if bloomRejectByEquality(r, fb, &nodes[i]) {
			return true
		}
	}
	return false
}

// bloomRejectByEquality returns true if the equality predicate tree guarantees no match
// via file-level bloom filters.
func bloomRejectByEquality(r *modules_reader.Reader, fb *modules_reader.FileBloom, node *vm.RangeNode) bool {
	if len(node.Children) > 0 {
		if node.IsOR {
			// OR: reject only if ALL children reject.
			for i := range node.Children {
				if !bloomRejectByEquality(r, fb, &node.Children[i]) {
					return false
				}
			}
			return true
		}
		// AND: reject if ANY child rejects.
		for i := range node.Children {
			if bloomRejectByEquality(r, fb, &node.Children[i]) {
				return true
			}
		}
		return false
	}
	// Leaf node — only handle equality (Values non-empty, no range/pattern).
	// SPEC-ROOT-006: complex boolean extracted to named predicate.
	if !nodeIsEqualityLeaf(node) {
		return false
	}
	if node.Column == "" {
		return false
	}
	// trace:id: compact bloom.
	if node.Column == "trace:id" {
		return bloomRejectTraceID(r, node.Values)
	}
	// String columns: FileBloom Fuse8.
	return bloomRejectString(fb, node.Column, node.Values)
}

// nodeIsEqualityLeaf returns true when node is an equality-only leaf: it has at
// least one value and no range or pattern predicate. Bloom rejection is only
// applicable to pure equality nodes.
func nodeIsEqualityLeaf(node *vm.RangeNode) bool {
	return len(node.Values) > 0 && node.Min == nil && node.Max == nil && node.Pattern == ""
}

// bloomRejectTraceID returns true if ALL trace:id values are definitely absent (compact bloom).
func bloomRejectTraceID(r *modules_reader.Reader, values []vm.Value) bool {
	for _, v := range values {
		b, ok := v.Data.([]byte)
		if !ok || len(b) != 16 {
			return false
		}
		var tid [16]byte
		copy(tid[:], b)
		if r.MayContainTraceID(tid) {
			return false
		}
	}
	return len(values) > 0
}

// bloomRejectString returns true if ALL string values are definitely absent (FileBloom Fuse8).
func bloomRejectString(fb *modules_reader.FileBloom, col string, values []vm.Value) bool {
	if fb == nil {
		return false
	}
	for _, v := range values {
		s, ok := v.Data.(string)
		if !ok {
			return false
		}
		if fb.MayContainString(col, s) {
			return false
		}
	}
	return len(values) > 0
}

// fileLevelReject returns true if the AND-combined predicates in nodes guarantee
// that no span in the file can match — i.e. the file should be entirely skipped.
// NOTE-045: Uses RangeColumnBoundaries (bucketMin/bucketMax) for O(1) file rejection.
// It is conservative: it only rejects when a leaf node's range predicate is entirely
// outside the column's [bucketMin, bucketMax] range.
//
// AND semantics: reject if ANY leaf rejects.
// OR semantics: reject only if ALL children reject.
func fileLevelReject(r *modules_reader.Reader, nodes []vm.RangeNode) bool {
	for i := range nodes {
		if rejectByBoundary(r, &nodes[i]) {
			return true
		}
	}
	return false
}

// rejectByBoundary returns true if the node guarantees no match based on
// file-level bucket boundaries.
func rejectByBoundary(r *modules_reader.Reader, node *vm.RangeNode) bool {
	if len(node.Children) > 0 {
		// Composite node.
		if node.IsOR {
			// OR: reject only if ALL children reject.
			for i := range node.Children {
				if !rejectByBoundary(r, &node.Children[i]) {
					return false
				}
			}
			return true
		}
		// AND: reject if ANY child rejects.
		for i := range node.Children {
			if rejectByBoundary(r, &node.Children[i]) {
				return true
			}
		}
		return false
	}

	// Leaf node — only handle range predicates (Min/Max) for numeric columns.
	if node.Min == nil && node.Max == nil {
		// NOTE-448: For anchored regex patterns on string columns, attempt prefix rejection.
		if node.Pattern != "" && node.Column != "" {
			bounds := r.RangeColumnBoundaries(node.Column)
			if bounds != nil {
				return rejectRegexByStringBounds(bounds, node)
			}
		}
		return false // equality or regex without extractable bounds — defer to block-level
	}
	if node.Column == "" {
		return false
	}

	bounds := r.RangeColumnBoundaries(node.Column)
	if bounds == nil {
		return false
	}

	return rangeRejectsFile(bounds, node)
}

// rangeRejectsFile returns true when the predicate's interval is entirely outside
// the file's [bucketMin, bucketMax] range.
func rangeRejectsFile(bounds *modules_reader.RangeBoundaries, node *vm.RangeNode) bool {
	switch bounds.ColType {
	case modules_shared.ColumnTypeRangeInt64, modules_shared.ColumnTypeRangeDuration:
		return rejectInt64Range(bounds.BucketMin, bounds.BucketMax, node)
	case modules_shared.ColumnTypeRangeUint64:
		return rejectUint64Range(uint64(bounds.BucketMin), uint64(bounds.BucketMax), node) //nolint:gosec
	case modules_shared.ColumnTypeRangeFloat64:
		return rejectFloat64Range(bounds, node)
	// NOTE-448: String/Bytes range predicates now participate in file-level pruning.
	// StringBounds[0]/[last] are the exact global min/max fed to the KLL sketch by the writer.
	case modules_shared.ColumnTypeRangeString:
		return rejectStringRange(bounds.StringBounds, node)
	case modules_shared.ColumnTypeRangeBytes:
		return rejectBytesRange(bounds.BytesBounds, node)
	}
	return false
}

// rejectInt64Range checks if a range predicate can be rejected for an int64 column.
func rejectInt64Range(fileMin, fileMax int64, node *vm.RangeNode) bool {
	if node.Min != nil && node.Max == nil {
		if queryMin, ok := ptrValueToInt64(node.Min); ok {
			return queryMin > fileMax
		}
	}
	if node.Max != nil && node.Min == nil {
		if queryMax, ok := ptrValueToInt64(node.Max); ok {
			return queryMax < fileMin
		}
	}
	if node.Min != nil && node.Max != nil {
		queryMin, okMin := ptrValueToInt64(node.Min)
		queryMax, okMax := ptrValueToInt64(node.Max)
		if okMin && okMax {
			return queryMin > fileMax || queryMax < fileMin
		}
	}
	return false
}

// rejectUint64Range checks if a range predicate can be rejected for a uint64 column.
func rejectUint64Range(fileMin, fileMax uint64, node *vm.RangeNode) bool {
	if node.Min != nil && node.Max == nil {
		if queryMin, ok := ptrValueToUint64(node.Min); ok {
			return queryMin > fileMax
		}
	}
	if node.Max != nil && node.Min == nil {
		if queryMax, ok := ptrValueToUint64(node.Max); ok {
			return queryMax < fileMin
		}
	}
	if node.Min != nil && node.Max != nil {
		queryMin, okMin := ptrValueToUint64(node.Min)
		queryMax, okMax := ptrValueToUint64(node.Max)
		if okMin && okMax {
			return queryMin > fileMax || queryMax < fileMin
		}
	}
	return false
}

// rejectFloat64Range checks if a range predicate can be rejected for a float64 column.
func rejectFloat64Range(bounds *modules_reader.RangeBoundaries, node *vm.RangeNode) bool {
	fileMin := math.Float64frombits(uint64(bounds.BucketMin)) //nolint:gosec
	fileMax := math.Float64frombits(uint64(bounds.BucketMax)) //nolint:gosec
	if math.IsNaN(fileMin) || math.IsNaN(fileMax) {
		return false
	}
	if node.Min != nil && node.Max == nil {
		if queryMin, ok := ptrValueToFloat64(node.Min); ok && !math.IsNaN(queryMin) {
			return queryMin > fileMax
		}
	}
	if node.Max != nil && node.Min == nil {
		if queryMax, ok := ptrValueToFloat64(node.Max); ok && !math.IsNaN(queryMax) {
			return queryMax < fileMin
		}
	}
	if node.Min != nil && node.Max != nil {
		queryMin, okMin := ptrValueToFloat64(node.Min)
		queryMax, okMax := ptrValueToFloat64(node.Max)
		if okMin && okMax && !math.IsNaN(queryMin) && !math.IsNaN(queryMax) {
			return queryMin > fileMax || queryMax < fileMin
		}
	}
	return false
}

// ptrValueToInt64 converts a *vm.Value to int64 for file-level boundary comparison.
// Returns false if the pointer is nil or the type cannot be converted.
func ptrValueToInt64(v *vm.Value) (int64, bool) {
	if v == nil {
		return 0, false
	}
	switch v.Type {
	case vm.TypeInt:
		if i, ok := v.Data.(int64); ok {
			return i, true
		}
	case vm.TypeDuration:
		if i, ok := v.Data.(int64); ok {
			return i, true
		}
	}
	return 0, false
}

// ptrValueToUint64 converts a *vm.Value to uint64 for file-level boundary comparison.
// Returns false if the pointer is nil or the type cannot be converted.
func ptrValueToUint64(v *vm.Value) (uint64, bool) {
	if v == nil {
		return 0, false
	}
	switch v.Type {
	case vm.TypeInt:
		if i, ok := v.Data.(int64); ok {
			if i >= 0 {
				return uint64(i), true //nolint:gosec // safe: i >= 0
			}
		}
	case vm.TypeDuration:
		if i, ok := v.Data.(int64); ok {
			if i >= 0 {
				return uint64(i), true //nolint:gosec // safe: i >= 0
			}
		}
	}
	return 0, false
}

// ptrValueToFloat64 converts a *vm.Value to float64 for file-level boundary comparison.
// Returns false if the pointer is nil or the type cannot be converted.
func ptrValueToFloat64(v *vm.Value) (float64, bool) {
	if v == nil {
		return 0, false
	}
	if f, ok := v.Data.(float64); ok {
		return f, true
	}
	return 0, false
}

// ptrValueToString converts a *vm.Value to string for file-level boundary comparison.
// Returns false if the pointer is nil or the type is not TypeString.
func ptrValueToString(v *vm.Value) (string, bool) {
	if v == nil {
		return "", false
	}
	if s, ok := v.Data.(string); ok {
		return s, true
	}
	return "", false
}

// ptrValueToBytes converts a *vm.Value to []byte for file-level boundary comparison.
// Returns false if the pointer is nil or the type is not TypeBytes.
func ptrValueToBytes(v *vm.Value) ([]byte, bool) {
	if v == nil {
		return nil, false
	}
	if b, ok := v.Data.([]byte); ok {
		return b, true
	}
	return nil, false
}

// rejectStringRange checks if a range predicate can be rejected for a string column.
// fileMin = stringBounds[0], fileMax = stringBounds[last] (KLL sketch exact extrema).
// NOTE-448: StringBounds[0] and StringBounds[last] are the exact file-wide min and max
// strings fed to the KLL sketch during write.
func rejectStringRange(bounds []string, node *vm.RangeNode) bool {
	if len(bounds) < 2 {
		return false
	}
	fileMin := bounds[0]
	fileMax := bounds[len(bounds)-1]
	if node.Min != nil && node.Max == nil {
		if queryMin, ok := ptrValueToString(node.Min); ok {
			return queryMin > fileMax
		}
	}
	if node.Max != nil && node.Min == nil {
		if queryMax, ok := ptrValueToString(node.Max); ok {
			return queryMax < fileMin
		}
	}
	if node.Min != nil && node.Max != nil {
		queryMin, okMin := ptrValueToString(node.Min)
		queryMax, okMax := ptrValueToString(node.Max)
		if okMin && okMax {
			return queryMin > fileMax || queryMax < fileMin
		}
	}
	return false
}

// rejectBytesRange checks if a range predicate can be rejected for a bytes column.
// Uses lexicographic byte order, matching the writer's bytes bounds encoding.
// NOTE-448: In practice TraceQL does not emit byte-typed range predicates; added for
// completeness and consistency with the string path.
func rejectBytesRange(bounds [][]byte, node *vm.RangeNode) bool {
	if len(bounds) < 2 {
		return false
	}
	fileMin := bounds[0]
	fileMax := bounds[len(bounds)-1]
	if node.Min != nil && node.Max == nil {
		if queryMin, ok := ptrValueToBytes(node.Min); ok {
			return bytes.Compare(queryMin, fileMax) > 0
		}
	}
	if node.Max != nil && node.Min == nil {
		if queryMax, ok := ptrValueToBytes(node.Max); ok {
			return bytes.Compare(queryMax, fileMin) < 0
		}
	}
	if node.Min != nil && node.Max != nil {
		queryMin, okMin := ptrValueToBytes(node.Min)
		queryMax, okMax := ptrValueToBytes(node.Max)
		if okMin && okMax {
			return bytes.Compare(queryMin, fileMax) > 0 || bytes.Compare(queryMax, fileMin) < 0
		}
	}
	return false
}

// extractAnchoredLiteralPrefix extracts a guaranteed literal prefix from an anchored regex
// pattern for conservative range pruning. Returns "" when no useful prefix can be extracted.
//
// Rules:
//   - Pattern must start with "^" — unanchored patterns could match anywhere.
//   - Scans bytes after "^" until the first regex metacharacter: [ ( . * + ? { \ $ |
//   - The literal run (possibly empty) is the prefix.
//
// NOTE-448: Only the upper bound direction is exploited (prefix > fileMax). The lower
// bound direction requires computing nextPrefix(prefix) — omitted to stay conservative.
func extractAnchoredLiteralPrefix(pattern string) string {
	if !strings.HasPrefix(pattern, "^") {
		return ""
	}
	rest := pattern[1:]
	// Alternation anywhere in the pattern makes any extracted prefix unsafe:
	// "^abc.*|def" also matches "def" which may be below the extracted "abc".
	if strings.ContainsRune(rest, '|') {
		return ""
	}
	for i := 0; i < len(rest); i++ {
		switch rest[i] {
		case '[', '(', '.', '*', '+', '?', '{', '\\', '$':
			return rest[:i]
		}
	}
	return rest
}

// rejectRegexByStringBounds attempts to reject a regex-only predicate node by comparing
// the pattern's anchored literal prefix against the file's string bounds.
// Only rejects when prefix > fileMax. NOTE-448: Only applies to ColumnTypeRangeString.
func rejectRegexByStringBounds(bounds *modules_reader.RangeBoundaries, node *vm.RangeNode) bool {
	if bounds.ColType != modules_shared.ColumnTypeRangeString {
		return false
	}
	if len(bounds.StringBounds) < 2 {
		return false
	}
	prefix := extractAnchoredLiteralPrefix(node.Pattern)
	if prefix == "" {
		return false
	}
	fileMax := bounds.StringBounds[len(bounds.StringBounds)-1]
	return prefix > fileMax
}

// colStatsRejectsInt64 applies signed int64 range comparison against ColStats numeric bounds.
// stat.MinNum and stat.MaxNum store int64 bit patterns as uint64 (writer emits raw int64 LE).
// Casting back to int64 reconstructs the signed value correctly, including negative numbers.
//
// NOTE-448: Replaces the previously incorrect path where numNodeBoundExceedsMax would have
// used valueAsUint64 (uint64 cast) on int64 values — wrong for negative values. Old files
// (HasNumRange=false for Int64) are unaffected.
func colStatsRejectsInt64(stat *modules_shared.ColStat, node *vm.RangeNode) bool {
	blockMin := int64(stat.MinNum) //nolint:gosec
	blockMax := int64(stat.MaxNum) //nolint:gosec
	if node.Min != nil {
		if queryMin, ok := ptrValueToInt64(node.Min); ok {
			if node.MinInclusive {
				if queryMin > blockMax {
					return true
				}
			} else {
				if queryMin >= blockMax {
					return true
				}
			}
		}
	}
	if node.Max != nil {
		if queryMax, ok := ptrValueToInt64(node.Max); ok {
			if node.MaxInclusive {
				if queryMax < blockMin {
					return true
				}
			} else {
				if queryMax <= blockMin {
					return true
				}
			}
		}
	}
	return false
}

// colStatsRejectsFloat64 applies float64 range comparison against ColStats numeric bounds.
// stat.MinNum and stat.MaxNum store math.Float64bits representations.
// NaN in either the stat or query bound is treated conservatively (no rejection).
//
// NOTE-448: numMinKey/numMaxKey in the writer are set via math.Float64bits. Add an
// explicit NaN guard on the executor side for safety.
func colStatsRejectsFloat64(stat *modules_shared.ColStat, node *vm.RangeNode) bool {
	blockMin := math.Float64frombits(stat.MinNum)
	blockMax := math.Float64frombits(stat.MaxNum)
	if math.IsNaN(blockMin) || math.IsNaN(blockMax) {
		return false
	}
	if node.Min != nil {
		if queryMin, ok := ptrValueToFloat64(node.Min); ok && !math.IsNaN(queryMin) {
			if node.MinInclusive {
				if queryMin > blockMax {
					return true
				}
			} else {
				if queryMin >= blockMax {
					return true
				}
			}
		}
	}
	if node.Max != nil {
		if queryMax, ok := ptrValueToFloat64(node.Max); ok && !math.IsNaN(queryMax) {
			if node.MaxInclusive {
				if queryMax < blockMin {
					return true
				}
			} else {
				if queryMax <= blockMin {
					return true
				}
			}
		}
	}
	return false
}

// fileLevelVectorPrune prunes blocks using VECTOR() centroid distances.
// If the file centroid is too distant (similarity < threshold), all blocks are pruned.
// Otherwise, blocks whose centroid is too distant are pruned individually.
// Returns the surviving block indices.
func fileLevelVectorPrune(r *modules_reader.Reader, program *vm.Program, selectedBlocks []int) []int {
	if !program.HasVector || len(program.QueryVector) == 0 {
		return selectedBlocks
	}

	vi, err := r.VectorIndex()
	if err != nil || vi == nil {
		// No vector index — cannot prune; keep all blocks.
		return selectedBlocks
	}

	// Determine threshold: use the minimum VectorThreshold from VECTOR() nodes.
	threshold := findVectorThreshold(program.Predicates)

	// File-level check: if the file centroid is too distant, skip all blocks.
	fileSim := float32(1.0) - vi.FileCentroidDistance(program.QueryVector)
	if fileSim < threshold {
		return nil
	}

	// Block-level check: prune blocks whose centroid is too distant.
	if len(vi.BlockCentroids) == 0 {
		return selectedBlocks
	}
	filtered := make([]int, 0, len(selectedBlocks))
	for _, bi := range selectedBlocks {
		if bi >= len(vi.BlockCentroids) {
			// No centroid for this block index — keep it.
			filtered = append(filtered, bi)
			continue
		}
		blockSim := float32(1.0) - vi.BlockCentroidDistance(bi, program.QueryVector)
		if blockSim >= threshold {
			filtered = append(filtered, bi)
		}
	}
	return filtered
}

// findVectorThreshold returns the minimum VectorThreshold from VECTOR() RangeNodes.
// Falls back to DefaultVectorThreshold if no node is found.
func findVectorThreshold(preds *vm.QueryPredicates) float32 {
	if preds == nil {
		return vm.DefaultVectorThreshold
	}
	threshold := findVectorThresholdNodes(preds.Nodes)
	if threshold == 0 {
		return vm.DefaultVectorThreshold
	}
	return threshold
}

func findVectorThresholdNodes(nodes []vm.RangeNode) float32 {
	for _, n := range nodes {
		if len(n.Children) > 0 {
			if t := findVectorThresholdNodes(n.Children); t > 0 {
				return t
			}
			continue
		}
		if len(n.QueryVector) > 0 && n.VectorThreshold > 0 {
			return n.VectorThreshold
		}
	}
	return 0
}
