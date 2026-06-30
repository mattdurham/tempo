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
// NOTE-036: All query paths (Collect, ExecuteTraceMetrics) must use planBlocks to
// ensure intrinsic TOC pruning is active everywhere, not just in Collect.
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
			plan.PrunedByFileBounds = len(plan.SelectedBlocks) // NOTE-456
			plan.SelectedBlocks = nil
			plan.Explain = "file-level reject: query value outside column [bucketMin, bucketMax]"
			plan.PrunedByIndex = 0
			plan.PrunedByTime = 0
			return plan
		}
	}

	// NOTE: File-level bloom reject removed (2026-06-29, in-file block pruning removal).
	// Value index is now the authoritative source for pruning.

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
		beforeIntrinsic := len(plan.SelectedBlocks)
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
		plan.PrunedByIntrinsicTOC = beforeIntrinsic - len(plan.SelectedBlocks) // NOTE-449
	}

	// NOTE: ColStats block pruning removed (2026-06-29, in-file block pruning removal).
	// Value index is now the authoritative source for pruning.

	return plan
}

// NOTE: pruneByColStats removed (2026-06-29, in-file block pruning removal).
// Value index is now the authoritative source for pruning.

// NOTE: colStatsRejects, colStatsRejectsNumeric, helper functions removed (2026-06-29).
// Value index is now the authoritative source for pruning.

// nodeIsEqualityLeaf returns true when node is an equality-only leaf: it has at
// least one value and no range or pattern predicate.
func nodeIsEqualityLeaf(node *vm.RangeNode) bool {
	return len(node.Values) > 0 && node.Min == nil && node.Max == nil && node.Pattern == ""
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
		// NOTE-451: A numeric equality predicate ("attr = V") carries only Values, never
		// Min/Max — so range-indexed numeric columns without a bloom filter (Int64, Uint64,
		// Float64, Duration) previously got no file-level pruning. Treat the equality value
		// set as a degenerate inclusive range [min(values), max(values)] and feed it through
		// the same range-rejection path. String/bytes equality is handled by bloom (and KLL
		// range bounds elsewhere); this branch only synthesizes a range for numeric values.
		if eq, ok := numericEqualityAsRange(node); ok && node.Column != "" {
			if bounds := r.RangeColumnBoundaries(node.Column); bounds != nil {
				return rangeRejectsFile(bounds, &eq)
			}
		}
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

// numericEqualityAsRange synthesizes a degenerate inclusive range node from an equality
// leaf whose Values are all numeric (TypeInt, TypeFloat, or TypeDuration). For a single
// value V it produces [V, V]; for a merged value set (e.g. "attr = A || attr = B" folded
// into one leaf) it produces [min, max] so the block/file is rejected only when its
// boundaries cannot intersect ANY of the equality values.
//
// Returns ok=false when the node is not a pure equality leaf (has Min/Max/Pattern), has no
// values, or contains any non-numeric value (string/bytes equality is handled by bloom and
// KLL bounds, not numeric range comparison). All values must share one numeric kind so the
// resulting bound type is well-defined for the range-rejection helpers.
func numericEqualityAsRange(node *vm.RangeNode) (vm.RangeNode, bool) {
	if !nodeIsEqualityLeaf(node) {
		return vm.RangeNode{}, false
	}
	var minV, maxV vm.Value
	wantType := node.Values[0].Type
	switch wantType {
	case vm.TypeInt, vm.TypeFloat, vm.TypeDuration:
	// NOTE-452 (issue #373): bool equality ("attr = true"/"= false") is a degenerate
	// numeric range [v,v] over the {0,1} ColStats range, prunes blocks where every span
	// has the opposite value (blockMax == 0 → no true; blockMin == 1 → no false).
	case vm.TypeBool:
	default:
		return vm.RangeNode{}, false
	}
	for i, v := range node.Values {
		if v.Type != wantType {
			return vm.RangeNode{}, false
		}
		if i == 0 {
			minV, maxV = v, v
			continue
		}
		if numericValueLess(v, minV) {
			minV = v
		}
		if numericValueLess(maxV, v) {
			maxV = v
		}
	}
	lo, hi := minV, maxV
	return vm.RangeNode{
		Column:       node.Column,
		Min:          &lo,
		Max:          &hi,
		MinInclusive: true,
		MaxInclusive: true,
	}, true
}

// numericValueLess reports whether a < b for two numeric Values of the same type
// (TypeInt, TypeFloat, or TypeDuration). Behavior is undefined for differing or
// non-numeric types; callers guarantee same-type numeric values.
func numericValueLess(a, b vm.Value) bool {
	switch a.Type {
	case vm.TypeFloat:
		af, _ := a.Data.(float64)
		bf, _ := b.Data.(float64)
		return af < bf
	case vm.TypeBool:
		// NOTE-452 (issue #373): false(0) < true(1).
		ab, _ := a.Data.(bool)
		bb, _ := b.Data.(bool)
		return !ab && bb
	default: // TypeInt, TypeDuration
		ai, _ := a.Data.(int64)
		bi, _ := b.Data.(int64)
		return ai < bi
	}
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
//
// NOTE-450: Honor node.MinInclusive/MaxInclusive (mirrors colStatsRejectsInt64). An
// exclusive lower bound (queryMin, e.g. "> V") rejects the file when queryMin >= fileMax,
// because the only file value that could possibly satisfy the predicate would be fileMax
// and that value is excluded. Likewise an exclusive upper bound rejects when
// queryMax <= fileMin. Inclusive bounds keep the original strict comparison.
func rejectInt64Range(fileMin, fileMax int64, node *vm.RangeNode) bool {
	if node.Min != nil {
		if queryMin, ok := ptrValueToInt64(node.Min); ok {
			if node.MinInclusive {
				if queryMin > fileMax {
					return true
				}
			} else if queryMin >= fileMax {
				return true
			}
		}
	}
	if node.Max != nil {
		if queryMax, ok := ptrValueToInt64(node.Max); ok {
			if node.MaxInclusive {
				if queryMax < fileMin {
					return true
				}
			} else if queryMax <= fileMin {
				return true
			}
		}
	}
	return false
}

// rejectUint64Range checks if a range predicate can be rejected for a uint64 column.
//
// NOTE-450: Honor node.MinInclusive/MaxInclusive (see rejectInt64Range).
func rejectUint64Range(fileMin, fileMax uint64, node *vm.RangeNode) bool {
	if node.Min != nil {
		if queryMin, ok := ptrValueToUint64(node.Min); ok {
			if node.MinInclusive {
				if queryMin > fileMax {
					return true
				}
			} else if queryMin >= fileMax {
				return true
			}
		}
	}
	if node.Max != nil {
		if queryMax, ok := ptrValueToUint64(node.Max); ok {
			if node.MaxInclusive {
				if queryMax < fileMin {
					return true
				}
			} else if queryMax <= fileMin {
				return true
			}
		}
	}
	return false
}

// rejectFloat64Range checks if a range predicate can be rejected for a float64 column.
//
// NOTE-450: Honor node.MinInclusive/MaxInclusive (see rejectInt64Range). NaN guards on
// both the file bounds and the query bounds remain conservative (no rejection).
func rejectFloat64Range(bounds *modules_reader.RangeBoundaries, node *vm.RangeNode) bool {
	fileMin := math.Float64frombits(uint64(bounds.BucketMin)) //nolint:gosec
	fileMax := math.Float64frombits(uint64(bounds.BucketMax)) //nolint:gosec
	if math.IsNaN(fileMin) || math.IsNaN(fileMax) {
		return false
	}
	if node.Min != nil {
		if queryMin, ok := ptrValueToFloat64(node.Min); ok && !math.IsNaN(queryMin) {
			if node.MinInclusive {
				if queryMin > fileMax {
					return true
				}
			} else if queryMin >= fileMax {
				return true
			}
		}
	}
	if node.Max != nil {
		if queryMax, ok := ptrValueToFloat64(node.Max); ok && !math.IsNaN(queryMax) {
			if node.MaxInclusive {
				if queryMax < fileMin {
					return true
				}
			} else if queryMax <= fileMin {
				return true
			}
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
//
// NOTE-453 (issue #369): honor node.MinInclusive / node.MaxInclusive. An exclusive lower
// bound (queryMin, e.g. "> V") rejects the file when queryMin >= fileMax: the only file
// value that could satisfy the predicate would be exactly fileMax, and that value is
// excluded by the strict ">". Likewise an exclusive upper bound rejects when
// queryMax <= fileMin. Inclusive bounds keep the original strict-only comparison. This is
// what lets the `!= V` rewrite OR(> V, < V) prune blocks where fileMin == fileMax == V.
func rejectStringRange(bounds []string, node *vm.RangeNode) bool {
	if len(bounds) < 2 {
		return false
	}
	fileMin := bounds[0]
	fileMax := bounds[len(bounds)-1]
	if node.Min != nil {
		if queryMin, ok := ptrValueToString(node.Min); ok {
			if node.MinInclusive {
				if queryMin > fileMax {
					return true
				}
			} else if queryMin >= fileMax {
				return true
			}
		}
	}
	if node.Max != nil {
		if queryMax, ok := ptrValueToString(node.Max); ok {
			if node.MaxInclusive {
				if queryMax < fileMin {
					return true
				}
			} else if queryMax <= fileMin {
				return true
			}
		}
	}
	return false
}

// rejectBytesRange checks if a range predicate can be rejected for a bytes column.
// Uses lexicographic byte order, matching the writer's bytes bounds encoding.
// NOTE-448: In practice TraceQL does not emit byte-typed range predicates; added for
// completeness and consistency with the string path.
//
// NOTE-453 (issue #369): honor node.MinInclusive / node.MaxInclusive — same exclusive-bound
// semantics as rejectStringRange, using lexicographic byte comparison.
func rejectBytesRange(bounds [][]byte, node *vm.RangeNode) bool {
	if len(bounds) < 2 {
		return false
	}
	fileMin := bounds[0]
	fileMax := bounds[len(bounds)-1]
	if node.Min != nil {
		if queryMin, ok := ptrValueToBytes(node.Min); ok {
			cmp := bytes.Compare(queryMin, fileMax)
			if node.MinInclusive {
				if cmp > 0 {
					return true
				}
			} else if cmp >= 0 {
				return true
			}
		}
	}
	if node.Max != nil {
		if queryMax, ok := ptrValueToBytes(node.Max); ok {
			cmp := bytes.Compare(queryMax, fileMin)
			if node.MaxInclusive {
				if cmp < 0 {
					return true
				}
			} else if cmp <= 0 {
				return true
			}
		}
	}
	return false
}

// nextStringPrefix returns the smallest string strictly greater than every string that has
// `s` as a prefix — i.e. the exclusive upper bound of the prefix's lexicographic range.
// It increments the last byte; if that byte is 0xFF it is dropped and the next byte is
// incremented (carry). Returns ok=false only when the whole string is 0xFF bytes (no finite
// upper bound exists) or s is empty. Pure byte arithmetic; no UTF-8 awareness needed since
// lexicographic byte order matches the string comparison used against the bounds.
func nextStringPrefix(s string) (string, bool) {
	b := []byte(s)
	for i := len(b) - 1; i >= 0; i-- {
		if b[i] != 0xFF {
			b[i]++
			return string(b[:i+1]), true
		}
	}
	return "", false
}

// extractAnchoredBounds extracts a conservative lexicographic interval [lower, upper) that
// contains every string the anchored regex can match. Returns ok=false when no useful bound
// can be derived.
//
// NOTE-455 (issue #374): generalizes the previous literal-prefix-only extraction in two ways:
//  1. Both directions are exploited. A literal prefix P gives lower=P (inclusive) and
//     upper=nextStringPrefix(P) (exclusive) — every match starts with P, so it lies in
//     [P, nextPrefix(P)).
//  2. A trailing simple character class `[X-Y]` (single ASCII range, no negation, no POSIX
//     classes, no extra members) following the literal run is folded in: lower=P+X,
//     upper=P+(Y+1). Scanning stops after the class because the upper bound cannot be
//     tightened further without more parsing.
//
// Conservatism: when no class is present we still return the prefix interval; when the
// prefix is empty AND there is no class, ok=false (no bound). hasUpper is false when the
// upper bound overflowed past 0xFF (no finite ceiling) — callers then use only the lower
// bound. Alternation ('|') anywhere makes any bound unsafe → ok=false.
func extractAnchoredBounds(pattern string) (lower, upper string, hasUpper, ok bool) {
	if !strings.HasPrefix(pattern, "^") {
		return "", "", false, false
	}
	rest := pattern[1:]
	if strings.ContainsRune(rest, '|') {
		return "", "", false, false
	}
	i := 0
	for i < len(rest) {
		switch rest[i] {
		case '[', '(', '.', '*', '+', '?', '{', '\\', '$':
			goto stop
		}
		i++
	}
stop:
	prefix := rest[:i]
	// Try to fold a trailing simple character class `[X-Y]` immediately after the literal.
	if i < len(rest) && rest[i] == '[' {
		if lo, hi, classOK := parseSimpleCharClass(rest[i:]); classOK {
			lower = prefix + string(lo)
			if hi == 0xFF {
				return lower, "", false, true
			}
			upper = prefix + string(hi+1)
			return lower, upper, true, true
		}
	}
	if prefix == "" {
		return "", "", false, false
	}
	lower = prefix
	up, hasUp := nextStringPrefix(prefix)
	return lower, up, hasUp, true
}

// parseSimpleCharClass recognizes ONLY a single-range ASCII character class of the exact
// form `[X-Y]` where X <= Y, both are printable ASCII bytes, and there is nothing else
// inside the brackets. Returns ok=false for negation (`[^...]`), POSIX classes
// (`[[:alpha:]]`), multi-range/multi-member classes (`[a-cx-z]`, `[abc]`), escapes, or any
// non-ASCII byte. This deliberately narrow recognizer keeps extraction conservative: an
// unrecognized class is simply treated as a wildcard stop (no bound from it).
func parseSimpleCharClass(s string) (lo, hi byte, ok bool) {
	// Minimal form is "[X-Y]" = 5 bytes.
	if len(s) < 5 || s[0] != '[' {
		return 0, 0, false
	}
	if s[1] == '^' {
		return 0, 0, false
	}
	lo = s[1]
	if s[2] != '-' {
		return 0, 0, false
	}
	hi = s[3]
	if s[4] != ']' {
		return 0, 0, false
	}
	// Require printable ASCII and a well-ordered range. Excludes non-ASCII (>=0x80) so the
	// hi+1 increment stays within a single byte and lexicographic byte order is unambiguous.
	if lo < 0x20 || lo > 0x7E || hi < 0x20 || hi > 0x7E || lo > hi {
		return 0, 0, false
	}
	return lo, hi, true
}

// rejectRegexByStringBounds attempts to reject a regex-only predicate node by comparing the
// pattern's anchored [lower, upper) interval against the file's string bounds.
//
// NOTE-455 (issue #374): rejects when EITHER (a) lower > fileMax — every match sorts above
// the file's largest value — OR (b) upper <= fileMin — every match sorts below the file's
// smallest value (upper is exclusive, so a match equal to fileMin is impossible when
// upper <= fileMin). When hasUpper is false (prefix had no finite ceiling) only the lower
// bound test applies. NOTE-448: Only applies to ColumnTypeRangeString.
func rejectRegexByStringBounds(bounds *modules_reader.RangeBoundaries, node *vm.RangeNode) bool {
	if bounds.ColType != modules_shared.ColumnTypeRangeString {
		return false
	}
	if len(bounds.StringBounds) < 2 {
		return false
	}
	lower, upper, hasUpper, ok := extractAnchoredBounds(node.Pattern)
	if !ok {
		return false
	}
	fileMin := bounds.StringBounds[0]
	fileMax := bounds.StringBounds[len(bounds.StringBounds)-1]
	if lower != "" && lower > fileMax {
		return true
	}
	if hasUpper && upper <= fileMin {
		return true
	}
	return false
}

// colStatsRejectsInt64 applies signed int64 range comparison against ColStats numeric bounds.
// stat.MinNum and stat.MaxNum store int64 bit patterns as uint64 (writer emits raw int64 LE).
// Casting back to int64 reconstructs the signed value correctly, including negative numbers.
//
// NOTE: colStatsRejectsInt64 removed (2026-06-29, in-file block pruning removal).

// NOTE: colStatsRejectsFloat64 removed (2026-06-29, in-file block pruning removal).

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
