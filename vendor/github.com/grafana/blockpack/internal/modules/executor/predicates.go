package executor

// NOTE: Any changes to this file must be reflected in the corresponding specs.md or NOTES.md.

import (
	"encoding/binary"
	"math"
	"math/bits"
	"regexp"
	"slices"
	"strconv"
	"strings"

	modules_reader "github.com/grafana/blockpack/internal/modules/blockio/reader"
	modules_shared "github.com/grafana/blockpack/internal/modules/blockio/shared"
	"github.com/grafana/blockpack/internal/modules/queryplanner"
	"github.com/grafana/blockpack/internal/vm"
)

// buildPredicates converts a compiled vm.Program into queryplanner.Predicate values
// for bloom-filter and range-index block pruning.
//
// Each top-level RangeNode in program.Predicates.Nodes is translated to a
// queryplanner.Predicate via translateNode; the planner AND-combines the result.
//
// Tree structure is preserved:
//   - OR composites (IsOR:true) → queryplanner.Predicate{Op:LogicalOR, Children:...}
//   - AND composites (IsOR:false) → queryplanner.Predicate{Op:LogicalAND, Children:...}
//   - Leaf with Values → bloom + range-index point-lookup predicate
//   - Leaf with Min/Max → bloom + range-index interval predicate
//   - Leaf with Pattern → bloom + regex-prefix range predicate (see translateRegexNode)
//   - Leaf without range constraint → bloom-only predicate
//
// NOTE-030: replaces the old flat-map approach (DedicatedColumns/DedicatedRanges/
// UnscopedColumnNames/HasOROperations). See executor/NOTES.md §NOTE-030.
func buildPredicates(r *modules_reader.Reader, program *vm.Program) []queryplanner.Predicate {
	if program == nil || program.Predicates == nil {
		return nil
	}
	preds := program.Predicates
	if len(preds.Nodes) == 0 && len(preds.Columns) == 0 {
		return nil
	}

	result := make([]queryplanner.Predicate, 0, len(preds.Nodes))
	for _, node := range preds.Nodes {
		p := translateNode(r, node)
		result = append(result, p)
	}

	return result
}

// translateNode converts a single RangeNode into a queryplanner.Predicate.
func translateNode(r *modules_reader.Reader, node vm.RangeNode) queryplanner.Predicate {
	// Composite node: recursively translate children, combine with AND or OR.
	if len(node.Children) > 0 {
		children := make([]queryplanner.Predicate, 0, len(node.Children))
		for _, child := range node.Children {
			children = append(children, translateNode(r, child))
		}
		op := queryplanner.LogicalAND
		if node.IsOR {
			op = queryplanner.LogicalOR
		}
		return queryplanner.Predicate{Op: op, Children: children}
	}

	// Leaf node: single column with Values, Min/Max, or Pattern.
	col := node.Column

	// Values: equality / point-lookup (bloom + range-index point match).
	if len(node.Values) > 0 {
		colType, hasIndex := r.RangeColumnType(col)
		if !hasIndex {
			return queryplanner.Predicate{Columns: []string{col}}
		}
		encodedVals := make([]string, 0, len(node.Values))
		for _, v := range node.Values {
			if enc, ok := encodeValue(v, colType); ok {
				encodedVals = append(encodedVals, enc)
			}
		}
		return queryplanner.Predicate{
			Columns: []string{col},
			Values:  encodedVals,
			ColType: colType,
		}
	}

	// Min/Max: interval lookup (bloom + range-index interval match).
	if node.Min != nil || node.Max != nil {
		colType, _ := r.RangeColumnType(col)
		if colType == 0 {
			colType = inferColTypeFromValues(node.Min, node.Max)
		}

		minEnc, minOK := rangeTypeSentinelMin(colType)
		maxEnc, maxOK := rangeTypeSentinelMax(colType)

		if node.Min != nil {
			if enc, ok := encodeValue(*node.Min, colType); ok {
				minEnc, minOK = enc, true
			}
		}
		if node.Max != nil {
			if enc, ok := encodeValue(*node.Max, colType); ok {
				maxEnc, maxOK = enc, true
			}
		}

		if !minOK || !maxOK {
			return queryplanner.Predicate{Columns: []string{col}}
		}

		return queryplanner.Predicate{
			Columns:       []string{col},
			Values:        []string{minEnc, maxEnc},
			ColType:       colType,
			IntervalMatch: true,
		}
	}

	// Pattern: regex — extract prefix for range-index pruning.
	if node.Pattern != "" {
		return translateRegexNode(r, col, node.Pattern)
	}

	// Bloom-only predicate (no range constraint specified).
	return queryplanner.Predicate{Columns: []string{col}}
}

// translateRegexNode builds a queryplanner.Predicate for a regex leaf node.
// NOTE-011: Interval match for case-insensitive regex prefix lookups.
// NOTE-024: Pure literal alternations use point lookups instead of interval match.
// NOTE-029: Partial Go-factored prefixes fall back to bloom-only.
func translateRegexNode(r *modules_reader.Reader, col, pattern string) queryplanner.Predicate {
	analysis := vm.AnalyzeRegex(pattern)
	if analysis == nil || len(analysis.Prefixes) == 0 {
		return queryplanner.Predicate{Columns: []string{col}}
	}

	colType, _ := r.RangeColumnType(col)
	if colType == 0 {
		colType = modules_shared.ColumnTypeRangeString
	}

	if analysis.CaseInsensitive {
		if len(analysis.Prefixes) > 1 {
			// Case-insensitive alternation: multiple prefixes span non-overlapping
			// ranges. Fall back to bloom-only to avoid false negatives.
			return queryplanner.Predicate{Columns: []string{col}}
		}
		return buildCaseInsensitiveRegexPredicate(col, colType, analysis)
	}

	// NOTE-024: single-prefix case-sensitive regex — delegate to helper.
	if len(analysis.Prefixes) == 1 {
		return buildCaseSensitiveSinglePrefixPredicate(col, colType, pattern, analysis)
	}

	// Multiple extracted prefixes from Go's regex parser.
	// NOTE-029: check if original pattern is a pure OR of complete literals.
	lits := extractLiteralAlternatives(pattern)
	if len(lits) == 0 {
		return queryplanner.Predicate{Columns: []string{col}}
	}
	encodedVals := make([]string, 0, len(lits))
	for _, lit := range lits {
		v := vm.Value{Type: vm.TypeString, Data: lit}
		if enc, ok := encodeValue(v, colType); ok {
			encodedVals = append(encodedVals, enc)
		}
	}
	if len(encodedVals) == 0 {
		return queryplanner.Predicate{Columns: []string{col}}
	}
	return queryplanner.Predicate{
		Columns: []string{col},
		Values:  encodedVals,
		ColType: colType,
	}
}

// BuildPredicates is an exported thin wrapper around buildPredicates.
// Used by api.go's StreamTraceQLModules inline execute loop.
func BuildPredicates(r *modules_reader.Reader, program *vm.Program) []queryplanner.Predicate {
	return buildPredicates(r, program)
}

// traceIntrinsicColumns is the set of column names served by the intrinsic section
// for trace files. Includes "practically intrinsic" columns (resource.service.name).
var traceIntrinsicColumns = map[string]struct{}{
	"trace:id":              {},
	"span:id":               {},
	"span:parent_id":        {},
	"span:name":             {},
	"span:kind":             {},
	"span:start":            {},
	"span:end":              {},
	"span:duration":         {},
	"span:status":           {},
	"span:status_message":   {},
	"resource.service.name": {},
}

// logIntrinsicColumns is the set of column names served by the intrinsic section
// for log files.
var logIntrinsicColumns = map[string]struct{}{
	"log:timestamp":          {},
	"log:observed_timestamp": {},
	"log:severity_number":    {},
	"log:severity_text":      {},
	"log:trace_id":           {},
	"log:span_id":            {},
	"log:flags":              {},
	"resource.service.name":  {},
}

// ProgramIsIntrinsicOnly reports whether all column references in program can be
// served from the intrinsic columns section without reading any block data.
// Returns false if program is nil, references any dynamic attribute column, or
// has no predicates (match-all queries are not intrinsic-only).
func ProgramIsIntrinsicOnly(program *vm.Program) bool {
	wantCols := ProgramWantColumns(program)
	if wantCols == nil {
		return false // match-all queries are not intrinsic-only
	}
	for col := range wantCols {
		_, inTrace := traceIntrinsicColumns[col]
		_, inLog := logIntrinsicColumns[col]
		if !inTrace && !inLog {
			return false
		}
	}
	return true
}

// BlocksFromIntrinsicTOC returns the set of block indices that may contain rows
// matching the given predicate program, using the intrinsic column section.
//
// Pruning strategy (applied for each predicate leaf that references an intrinsic column):
//  1. TOC min/max check (no I/O): if the query range does not overlap the column's
//     global min/max, the entire file has no matches — return an empty block set.
//  2. Dict column (span:name, span:status, span:kind, resource.service.name): load the
//     column blob once, find matching entries by exact equality, collect their BlockRefs.
//  3. Flat column (span:duration, span:start, span:end): load the column blob once,
//     binary-search the sorted uint64 values for the predicate range, collect BlockRefs.
//
// Block sets across predicates are intersected (AND semantics). Returns nil if no
// intrinsic column is referenced by any predicate, or if the file has no intrinsic section.
//
// This is an additional pruning layer before bloom filter / range index checks:
// it can eliminate blocks for ANY query that references intrinsic columns, not only
// "intrinsic-only" queries. A mixed query like { resource.service.name="grafana" &&
// span.http.method="GET" } still benefits from service.name pruning here.
func BlocksFromIntrinsicTOC(r *modules_reader.Reader, program *vm.Program) []int {
	if !r.HasIntrinsicSection() {
		return nil
	}
	if program == nil || program.Predicates == nil {
		return nil
	}
	if len(program.Predicates.Nodes) == 0 {
		return nil
	}

	total := r.BlockCount()
	if total == 0 {
		return nil
	}

	// Collect intrinsic-column leaf nodes from the top-level AND list.
	// We only handle top-level AND leaves for simplicity; composite OR nodes are skipped
	// (conservative: don't prune when OR semantics are involved).
	var intrinsicLeaves []vm.RangeNode
	for _, node := range program.Predicates.Nodes {
		collectIntrinsicLeaves(node, &intrinsicLeaves)
	}
	if len(intrinsicLeaves) == 0 {
		return nil
	}

	// Start with all blocks selected; intersect per predicate.
	selected := allBlockBitset(total)

	for _, leaf := range intrinsicLeaves {
		colName := leaf.Column
		meta, hasMeta := r.IntrinsicColumnMeta(colName)
		if !hasMeta {
			// Column not in intrinsic section — skip (don't prune based on it).
			continue
		}

		// Step 1: TOC min/max check — no I/O.
		if !intrinsicTOCOverlaps(meta, leaf) {
			// No overlap — entire file pruned.
			return []int{}
		}

		// Step 2 / 3: Load column blob and collect matching block indices.
		col, err := r.GetIntrinsicColumn(colName)
		if err != nil || col == nil {
			// Cannot load — skip pruning for this column.
			continue
		}

		var matchBlocks []int
		if col.Format == modules_shared.IntrinsicFormatDict {
			matchBlocks = intrinsicDictMatches(col, leaf)
		} else {
			matchBlocks = intrinsicFlatMatches(col, leaf)
		}

		// nil matchBlocks means "could not evaluate" — skip intersection for this column.
		if matchBlocks == nil {
			continue
		}

		// Intersect: keep only blocks that are in matchBlocks.
		keepSet := makeBlockBitset(total)
		for _, bi := range matchBlocks {
			if bi < total {
				keepSet.set(bi)
			}
		}
		selected.intersect(keepSet, total)
	}

	// If nothing was actually pruned (all blocks still selected), return nil so the
	// caller falls through to the regular planner without redundant work.
	if selected.count() == total {
		return nil
	}

	return selected.toSortedSlice(total)
}

// collectIntrinsicLeaves walks a RangeNode tree and appends leaf nodes whose column
// is in the intrinsic column set to dst. OR composite nodes are skipped (conservative).
func collectIntrinsicLeaves(node vm.RangeNode, dst *[]vm.RangeNode) {
	if len(node.Children) == 0 {
		// Leaf node — include if it references an intrinsic column.
		if node.Column == "" {
			return
		}
		_, inTrace := traceIntrinsicColumns[node.Column]
		_, inLog := logIntrinsicColumns[node.Column]
		if inTrace || inLog {
			*dst = append(*dst, node)
		}
		return
	}
	// Composite: only recurse into AND nodes; skip OR (too complex to intersect safely).
	if node.IsOR {
		return
	}
	for _, child := range node.Children {
		collectIntrinsicLeaves(child, dst)
	}
}

// intrinsicTOCOverlaps reports whether a predicate leaf can possibly match given the
// column's global min/max from the TOC. Returns true (no prune) if the predicate
// type cannot be evaluated against min/max, or when the ranges overlap.
//
// Numeric columns (uint64, int64) use numeric comparison because 8-byte LE encoding
// is not lexicographically ordered. String columns use direct string comparison.
func intrinsicTOCOverlaps(meta modules_shared.IntrinsicColMeta, leaf vm.RangeNode) bool {
	if meta.Min == "" && meta.Max == "" {
		return true // no range info — assume overlap
	}

	isNumeric := isNumericColType(meta.Type)

	switch {
	case len(leaf.Values) > 0:
		// Equality: at least one value must fall within [tocMin, tocMax].
		if !isNumeric {
			// String column: use direct lexicographic comparison.
			for _, v := range leaf.Values {
				qEnc, ok := encodeValue(v, meta.Type)
				if !ok {
					return true
				}
				if qEnc >= meta.Min && qEnc <= meta.Max {
					return true
				}
			}
			return false
		}
		tocMin, tocMax, okTOC := decodeTOCRange(meta, isNumeric)
		if !okTOC {
			return true
		}
		for _, v := range leaf.Values {
			qEnc, ok := encodeValue(v, meta.Type)
			if !ok {
				return true // cannot encode — assume overlap
			}
			qVal, okQ := decodeNumericKey(qEnc, isNumeric)
			if !okQ {
				return true
			}
			if qVal >= tocMin && qVal <= tocMax {
				return true
			}
		}
		return false

	case leaf.Min != nil || leaf.Max != nil:
		// Range predicate: check if [queryMin, queryMax] overlaps [tocMin, tocMax].
		tocMin, tocMax, okTOC := decodeTOCRange(meta, isNumeric)
		if !okTOC {
			return true
		}
		// queryMin defaults to tocMin (no lower bound means start of file).
		queryMin := tocMin
		queryMax := tocMax
		if leaf.Min != nil {
			if enc, ok := encodeValue(*leaf.Min, meta.Type); ok {
				if qv, okQ := decodeNumericKey(enc, isNumeric); okQ {
					queryMin = qv
				}
			}
		}
		if leaf.Max != nil {
			if enc, ok := encodeValue(*leaf.Max, meta.Type); ok {
				if qv, okQ := decodeNumericKey(enc, isNumeric); okQ {
					queryMax = qv
				}
			}
		}
		// Overlap condition: queryMin <= tocMax AND queryMax >= tocMin.
		return queryMin <= tocMax && queryMax >= tocMin

	default:
		// Pattern or no constraint — assume overlap (regex not evaluated here).
		return true
	}
}

// isNumericColType reports whether a ColumnType stores values as 8-byte LE uint64.
func isNumericColType(ct modules_shared.ColumnType) bool {
	switch ct {
	case modules_shared.ColumnTypeUint64, modules_shared.ColumnTypeRangeUint64,
		modules_shared.ColumnTypeInt64, modules_shared.ColumnTypeRangeInt64,
		modules_shared.ColumnTypeRangeDuration:
		return true
	}
	return false
}

// decodeTOCRange decodes the TOC min/max into comparable uint64 values.
// For numeric columns, treats the 8-byte LE encoding as uint64 bits.
// For string columns, returns (0, maxUint64, true) since string comparison
// is handled separately. Returns (0, 0, false) if decoding fails.
func decodeTOCRange(meta modules_shared.IntrinsicColMeta, isNumeric bool) (lo, hi uint64, ok bool) {
	if !isNumeric {
		// String columns: caller uses string comparison — return sentinel values.
		return 0, ^uint64(0), true
	}
	if len(meta.Min) != 8 || len(meta.Max) != 8 {
		return 0, 0, false
	}
	minV := binary.LittleEndian.Uint64([]byte(meta.Min))
	maxV := binary.LittleEndian.Uint64([]byte(meta.Max))
	return minV, maxV, true
}

// decodeNumericKey decodes an 8-byte LE wire-encoded key to uint64 for numeric comparison.
// For non-numeric (string) columns, returns (0, false).
func decodeNumericKey(enc string, isNumeric bool) (uint64, bool) {
	if !isNumeric {
		return 0, false
	}
	if len(enc) != 8 {
		return 0, false
	}
	return binary.LittleEndian.Uint64([]byte(enc)), true
}

// intrinsicDictMatches finds all distinct block indices in a dict column that match
// the predicate leaf. Equality predicates (Values) and regex (Pattern) are supported;
// range predicates (Min/Max with no Values) return nil (no pruning — fall through to bloom/range).
func intrinsicDictMatches(col *modules_shared.IntrinsicColumn, leaf vm.RangeNode) []int {
	if len(leaf.Values) == 0 && leaf.Pattern == "" {
		// Range predicate on a dict column — not handled; skip pruning.
		return nil
	}

	// Regex predicate: compile and match each dict entry by value.
	if len(leaf.Values) == 0 && leaf.Pattern != "" {
		re, err := regexp.Compile(leaf.Pattern)
		if err != nil {
			return nil // invalid regex — skip pruning
		}
		blockSet := make(map[int]struct{})
		for _, entry := range col.DictEntries {
			// Dict entries use Value=="" to signal int64 type (wire format stores int64
			// with vLen=0; the encoder writes vLen>0 even for empty strings in string
			// columns). Int64 entries are not string-matchable — skip them.
			if entry.Value == "" {
				continue
			}
			if !re.MatchString(entry.Value) {
				continue
			}
			for _, ref := range entry.BlockRefs {
				blockSet[int(ref.BlockIdx)] = struct{}{}
			}
		}
		if len(blockSet) == 0 {
			return []int{} // empty: no blocks match
		}
		result := make([]int, 0, len(blockSet))
		for bi := range blockSet {
			result = append(result, bi)
		}
		slices.Sort(result)
		return result
	}

	// Build a set of query values for O(1) lookup.
	wantStr := make(map[string]struct{}, len(leaf.Values))
	wantInt := make(map[int64]struct{}, len(leaf.Values))
	for _, v := range leaf.Values {
		switch v.Type {
		case vm.TypeString:
			if s, ok := v.Data.(string); ok {
				wantStr[s] = struct{}{}
			}
		case vm.TypeInt:
			if i, ok := v.Data.(int64); ok {
				wantInt[i] = struct{}{}
			}
		case vm.TypeDuration:
			if i, ok := v.Data.(int64); ok {
				wantInt[i] = struct{}{}
			}
		}
	}

	blockSet := make(map[int]struct{})
	for _, entry := range col.DictEntries {
		var match bool
		// Dict entries use Value=="" to signal int64 type (wire format stores int64 with vLen=0).
		// Empty-string string values cannot reach here — dict columns are typed, so a string-typed
		// column never produces entries with Value=="" (the encoder writes vLen>0 for empty strings).
		if entry.Value != "" {
			_, match = wantStr[entry.Value]
		} else {
			_, match = wantInt[entry.Int64Val]
		}
		if !match {
			continue
		}
		for _, ref := range entry.BlockRefs {
			blockSet[int(ref.BlockIdx)] = struct{}{}
		}
	}

	if len(blockSet) == 0 {
		return []int{} // empty: no blocks match
	}
	result := make([]int, 0, len(blockSet))
	for bi := range blockSet {
		result = append(result, bi)
	}
	slices.Sort(result)
	return result
}

// intrinsicFlatMatches finds block indices in a flat (uint64-sorted) column whose
// values fall within the predicate range. Values predicates (equality) are also
// supported via exact match. Returns nil if the predicate cannot be evaluated.
func intrinsicFlatMatches(col *modules_shared.IntrinsicColumn, leaf vm.RangeNode) []int {
	if len(col.Uint64Values) == 0 {
		// Bytes flat column (trace:id, span:id) — not range-searchable; skip.
		return nil
	}

	var lo, hi uint64
	var hasLo, hasHi bool

	switch {
	case len(leaf.Values) > 0:
		// Equality: treat each value as both lo and hi (exact match).
		blockSet := make(map[int]struct{})
		for _, v := range leaf.Values {
			target, ok := valueToUint64(v)
			if !ok {
				return nil // cannot encode — skip pruning
			}
			// Binary search in sorted Uint64Values.
			i := sortSearchUint64(col.Uint64Values, target)
			for i < len(col.Uint64Values) && col.Uint64Values[i] == target {
				blockSet[int(col.BlockRefs[i].BlockIdx)] = struct{}{}
				i++
			}
		}
		if len(blockSet) == 0 {
			return []int{}
		}
		result := make([]int, 0, len(blockSet))
		for bi := range blockSet {
			result = append(result, bi)
		}
		slices.Sort(result)
		return result

	case leaf.Min != nil || leaf.Max != nil:
		if leaf.Min != nil {
			if v, ok := valueToUint64(*leaf.Min); ok {
				lo, hasLo = v, true
			} else {
				return nil
			}
		}
		if leaf.Max != nil {
			if v, ok := valueToUint64(*leaf.Max); ok {
				hi, hasHi = v, true
			} else {
				return nil
			}
		}

	default:
		return nil // pattern or no constraint — skip
	}

	// Find range [lo, hi] in sorted Uint64Values.
	vals := col.Uint64Values
	start := 0
	if hasLo {
		start = sortSearchUint64(vals, lo)
	}
	end := len(vals)
	if hasHi {
		// Find first index where value > hi.
		end = sortSearchUint64(vals, hi+1)
		if hi == ^uint64(0) {
			end = len(vals) // overflow guard: hi+1 wraps to 0
		}
	}

	if start >= end {
		return []int{}
	}

	blockSet := make(map[int]struct{}, end-start)
	for i := start; i < end; i++ {
		blockSet[int(col.BlockRefs[i].BlockIdx)] = struct{}{}
	}
	result := make([]int, 0, len(blockSet))
	for bi := range blockSet {
		result = append(result, bi)
	}
	slices.Sort(result)
	return result
}

// valueToUint64 converts a vm.Value to uint64 for flat-column range lookup.
// Returns (0, false) if the value type cannot be converted.
func valueToUint64(v vm.Value) (uint64, bool) {
	switch v.Type {
	case vm.TypeInt:
		if i, ok := v.Data.(int64); ok {
			if i < 0 {
				return 0, false
			}
			return uint64(i), true //nolint:gosec // safe: i >= 0
		}
	case vm.TypeDuration:
		if i, ok := v.Data.(int64); ok {
			if i < 0 {
				return 0, false
			}
			return uint64(i), true //nolint:gosec // safe: i >= 0
		}
	case vm.TypeFloat:
		if f, ok := v.Data.(float64); ok {
			if f < 0 {
				return 0, false
			}
			return uint64(f), true //nolint:gosec // safe: f >= 0
		}
	}
	return 0, false
}

// sortSearchUint64 returns the smallest index i in [0, n) such that vals[i] >= target.
// Equivalent to sort.Search(len(vals), func(i int) bool { return vals[i] >= target }).
func sortSearchUint64(vals []uint64, target uint64) int {
	lo, hi := 0, len(vals)
	for lo < hi {
		mid := int(uint(lo+hi) >> 1) //nolint:gosec // safe: lo+hi bounded by slice len
		if vals[mid] < target {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	return lo
}

// blockBitset is a compact bitset for up to n block indices.
type blockBitset []uint64

// makeBlockBitset creates a zero-initialized (all-clear) bitset for n blocks.
func makeBlockBitset(n int) blockBitset {
	words := (n + 63) / 64
	return make(blockBitset, words)
}

// allBlockBitset creates an all-ones bitset for exactly n blocks.
// Bits beyond index n-1 are cleared to avoid spurious block indices.
func allBlockBitset(n int) blockBitset {
	words := (n + 63) / 64
	b := make(blockBitset, words)
	for i := range b {
		b[i] = ^uint64(0)
	}
	// Clear trailing bits in the last word that are beyond n-1.
	if rem := n % 64; rem != 0 {
		b[words-1] = (uint64(1) << uint(rem)) - 1 //nolint:gosec // safe: rem = n%64, always in [1,63]
	}
	return b
}

func (b blockBitset) set(i int) {
	b[i/64] |= 1 << uint(i%64) //nolint:gosec // safe: i%64 always in [0,63]
}

func (b blockBitset) test(i int) bool {
	return b[i/64]>>uint(i%64)&1 != 0 //nolint:gosec // safe: i%64 always in [0,63]
}

func (b blockBitset) count() int {
	n := 0
	for _, w := range b {
		n += bits.OnesCount64(w)
	}
	return n
}

func (b blockBitset) intersect(other blockBitset, total int) {
	_ = total
	for i := range b {
		if i < len(other) {
			b[i] &= other[i]
		} else {
			b[i] = 0
		}
	}
}

func (b blockBitset) toSortedSlice(total int) []int {
	out := make([]int, 0, b.count())
	for i := range total {
		if b.test(i) {
			out = append(out, i)
		}
	}
	return out
}

// searchMetaColumns returns the minimal set of blockpack column names needed to
// construct a Tempo search result from a matched span.
//
// NOTE-028: Mirrors Tempo's SearchMetaConditions() (pkg/traceql/storage.go), translated
// to blockpack column names. Tempo pre-computes RootSpanName, RootServiceName,
// TraceDuration, and TraceStartTime as trace-level parquet columns; blockpack stores
// everything per-span so root span detection requires span:parent_id, and root name/
// service are derived from span:name and resource.service.name of the root span.
// span:end is included for duration fallback when no root span is present in the result set.
func searchMetaColumns() map[string]struct{} {
	return map[string]struct{}{
		"trace:id":              {},
		"span:id":               {},
		"span:start":            {},
		"span:end":              {},
		"span:duration":         {},
		"span:name":             {},
		"span:parent_id":        {},
		"resource.service.name": {},
	}
}

// ProgramWantColumns returns the minimal set of column names needed to evaluate program.
//
// Sources (unioned):
//  1. Leaf Column values from the RangeNode tree (preds.Nodes) — collected recursively.
//  2. preds.Columns — explicit column list for attributes that need decode but not pruning:
//     negations (!=, !~), log:body for line filters, pushdown label-filter columns.
//  3. extra — caller-supplied columns (e.g. identity columns like trace:id, span:id).
//
// Returns nil if program has no predicates, which ParseBlockFromBytes treats as "all columns".
// NOTE-018: used by all executor code paths for two-pass column decode.
// NOTE-030: preds.Columns replaces the old AttributesAccessed / UnscopedColumnNames fields.
func ProgramWantColumns(program *vm.Program, extra ...string) map[string]struct{} {
	if program == nil || program.Predicates == nil {
		return nil
	}
	p := program.Predicates
	if len(p.Nodes) == 0 && len(p.Columns) == 0 && len(extra) == 0 {
		return nil
	}

	cols := make(map[string]struct{})
	// Collect all leaf column names from the Nodes tree.
	collectNodeColumns(p.Nodes, cols)
	// Add explicit Columns (negations, log:body, etc. that have no pruning node).
	for _, c := range p.Columns {
		cols[c] = struct{}{}
	}
	for _, c := range extra {
		cols[c] = struct{}{}
	}
	if len(cols) == 0 {
		return nil
	}
	return cols
}

// collectNodeColumns recursively walks a RangeNode slice and adds all leaf Column
// values to the cols set.
func collectNodeColumns(nodes []vm.RangeNode, cols map[string]struct{}) {
	for _, n := range nodes {
		if len(n.Children) > 0 {
			collectNodeColumns(n.Children, cols)
		} else if n.Column != "" {
			cols[n.Column] = struct{}{}
		}
	}
}

// inferColTypeFromValues infers the best ColumnType for encoding a range predicate
// when no range index exists for the column.
func inferColTypeFromValues(minVal, maxVal *vm.Value) modules_shared.ColumnType {
	v := minVal
	if v == nil {
		v = maxVal
	}
	if v == nil {
		return modules_shared.ColumnTypeRangeInt64
	}
	switch v.Type {
	case vm.TypeInt:
		return modules_shared.ColumnTypeRangeInt64
	case vm.TypeFloat:
		return modules_shared.ColumnTypeRangeFloat64
	case vm.TypeString:
		return modules_shared.ColumnTypeRangeString
	case vm.TypeDuration:
		return modules_shared.ColumnTypeRangeDuration
	default:
		return modules_shared.ColumnTypeRangeInt64
	}
}

// isASCII reports whether s contains only ASCII bytes.
func isASCII(s string) bool {
	for i := range len(s) {
		if s[i] > 127 {
			return false
		}
	}
	return true
}

// extractLiteralAlternatives reports whether pattern is a pure OR of complete
// literal strings with no regex metacharacters. If so, it returns the individual
// alternatives for use as point lookups in the range index. Returns nil if any
// alternative contains a metacharacter or if the pattern is empty.
//
// NOTE-024: This detects the case where Go's regex parser factors a common prefix
// from an alternation (e.g. "cluster-0|cluster-1" → single prefix "cluster-"),
// causing the single-prefix interval path to emit an overly wide range match.
// By operating on the original pattern string before regex parsing, this function
// recovers the individual literals and enables point lookups instead.
func extractLiteralAlternatives(pattern string) []string {
	if pattern == "" {
		return nil
	}
	parts := strings.Split(pattern, "|")
	const metachars = `.*+?[]{}()^$\`
	for _, p := range parts {
		if p == "" {
			return nil
		}
		if strings.ContainsAny(p, metachars) {
			return nil
		}
	}
	return parts
}

// buildCaseInsensitiveRegexPredicate builds an interval-match predicate for a
// case-insensitive regex pattern with a single ASCII prefix. The all-uppercase
// prefix is the min key; the all-lowercase prefix + "\xff" is the max key.
// The "\xff" suffix ensures buckets whose lower boundary extends beyond the
// prefix (e.g., "debug-service" for prefix "debug") are included in the
// interval. All buckets whose range overlaps [UPPER, lower\xff] are kept.
//
// Non-ASCII prefixes fall back to bloom-only because Unicode case mapping can
// change byte length/ordering, making the [UPPER, lower] interval unsafe under
// bytewise lexicographic comparison.
// NOTE-011: interval matching for case-insensitive regex prefix lookups.
func buildCaseInsensitiveRegexPredicate(
	col string,
	colType modules_shared.ColumnType,
	analysis *vm.RegexAnalysis,
) queryplanner.Predicate {
	prefix := analysis.Prefixes[0]
	// Non-ASCII: Unicode case mapping can change byte length/ordering.
	// Fall back to bloom-only to avoid false negatives.
	if !isASCII(prefix) {
		return queryplanner.Predicate{Columns: []string{col}}
	}
	upper := strings.ToUpper(prefix)
	// Append \xff so the interval captures buckets with lower boundaries that
	// extend beyond the prefix (e.g., "debug-service" > "debug" but < "debug\xff").
	lower := strings.ToLower(prefix) + "\xff"

	upperVal := vm.Value{Type: vm.TypeString, Data: upper}
	lowerVal := vm.Value{Type: vm.TypeString, Data: lower}

	var vals []string
	if encMin, ok := encodeValue(upperVal, colType); ok {
		if encMax, ok := encodeValue(lowerVal, colType); ok {
			vals = []string{encMin, encMax}
		}
	}

	if len(vals) < 2 {
		// Encoding failed — fall back to bloom-only.
		return queryplanner.Predicate{Columns: []string{col}}
	}

	return queryplanner.Predicate{
		Columns:       []string{col},
		Values:        vals,
		ColType:       colType,
		IntervalMatch: true,
	}
}

// buildCaseSensitiveSinglePrefixPredicate builds the predicate for a case-sensitive
// regex with exactly one extracted prefix. It checks whether the raw pattern is a pure
// OR of complete literals (NOTE-024) and uses point lookups if so; otherwise it falls
// back to interval matching on the extracted prefix (NOTE-011).
func buildCaseSensitiveSinglePrefixPredicate(
	col string,
	colType modules_shared.ColumnType,
	pattern string,
	analysis *vm.RegexAnalysis,
) queryplanner.Predicate {
	// NOTE-024: Before falling back to interval matching on the common prefix,
	// check whether the original pattern is a pure OR of complete literals.
	if lits := extractLiteralAlternatives(pattern); len(lits) > 1 {
		encodedVals := make([]string, 0, len(lits))
		for _, lit := range lits {
			v := vm.Value{Type: vm.TypeString, Data: lit}
			if enc, ok := encodeValue(v, colType); ok {
				encodedVals = append(encodedVals, enc)
			}
		}
		if len(encodedVals) == 0 {
			return queryplanner.Predicate{Columns: []string{col}}
		}
		return queryplanner.Predicate{
			Columns: []string{col},
			Values:  encodedVals,
			ColType: colType,
		}
	}
	// Single literal or non-pure-literal pattern: use interval matching
	// [prefix, prefix+"\xff"] to find all buckets whose lower boundary
	// starts with the extracted common prefix.
	// NOTE-011: single-prefix case-sensitive regex uses interval matching like (?i) patterns.
	prefix := analysis.Prefixes[0]
	minVal := vm.Value{Type: vm.TypeString, Data: prefix}
	maxVal := vm.Value{Type: vm.TypeString, Data: prefix + "\xff"}
	encMin, okMin := encodeValue(minVal, colType)
	encMax, okMax := encodeValue(maxVal, colType)
	if okMin && okMax {
		return queryplanner.Predicate{
			Columns:       []string{col},
			Values:        []string{encMin, encMax},
			ColType:       colType,
			IntervalMatch: true,
		}
	}
	return queryplanner.Predicate{Columns: []string{col}}
}

// rangeTypeSentinelMin returns the wire-encoded minimum sentinel for the given column type
// and whether encoding succeeded. Used to express open-ended upper range predicates
// (e.g. duration < Y) where no explicit lower bound is provided.
// The sentinel covers the full storable range without false negatives.
// For ColumnTypeRangeString the minimum sentinel is "" (empty string), which is a valid
// value — callers must use the bool return to distinguish success from unsupported types.
func rangeTypeSentinelMin(colType modules_shared.ColumnType) (string, bool) {
	var buf [8]byte
	switch colType {
	case modules_shared.ColumnTypeRangeUint64, modules_shared.ColumnTypeUint64:
		binary.LittleEndian.PutUint64(buf[:], 0)
		return string(buf[:]), true
	case modules_shared.ColumnTypeRangeInt64, modules_shared.ColumnTypeRangeDuration, modules_shared.ColumnTypeInt64:
		const minInt64AsUint64 = 1 << 63 // bit pattern of math.MinInt64 as uint64
		binary.LittleEndian.PutUint64(buf[:], minInt64AsUint64)
		return string(buf[:]), true
	case modules_shared.ColumnTypeRangeFloat64, modules_shared.ColumnTypeFloat64:
		binary.LittleEndian.PutUint64(buf[:], math.Float64bits(math.Inf(-1)))
		return string(buf[:]), true
	case modules_shared.ColumnTypeRangeString, modules_shared.ColumnTypeString:
		return "", true // empty string is the lexicographic minimum
	default:
		return "", false
	}
}

// rangeTypeSentinelMax returns the wire-encoded maximum sentinel for the given column type
// and whether encoding succeeded. Used to express open-ended lower range predicates
// (e.g. duration > X) where no explicit upper bound is provided.
// The sentinel covers the full storable range without false negatives.
func rangeTypeSentinelMax(colType modules_shared.ColumnType) (string, bool) {
	var buf [8]byte
	switch colType {
	case modules_shared.ColumnTypeRangeUint64, modules_shared.ColumnTypeUint64:
		binary.LittleEndian.PutUint64(buf[:], math.MaxUint64)
		return string(buf[:]), true
	case modules_shared.ColumnTypeRangeInt64, modules_shared.ColumnTypeRangeDuration, modules_shared.ColumnTypeInt64:
		binary.LittleEndian.PutUint64(
			buf[:],
			uint64(math.MaxInt64),
		) //nolint:gosec // safe: storing MaxInt64 bits as uint64
		return string(buf[:]), true
	case modules_shared.ColumnTypeRangeFloat64, modules_shared.ColumnTypeFloat64:
		binary.LittleEndian.PutUint64(buf[:], math.Float64bits(math.Inf(+1)))
		return string(buf[:]), true
	case modules_shared.ColumnTypeRangeString, modules_shared.ColumnTypeString:
		return "\xff\xff\xff\xff\xff\xff\xff\xff", true // high sentinel beyond realistic string values
	default:
		return "", false
	}
}

// encodeValue encodes a vm.Value to the range-index wire format (SPECS §5.2.1):
//   - String / RangeString: raw string
//   - Int64 / RangeDuration: 8-byte little-endian int64 bits
//   - Uint64 / RangeUint64: 8-byte little-endian uint64 bits
//   - Float64 / RangeFloat64: 8-byte little-endian IEEE-754 float64 bits
func encodeValue(v vm.Value, colType modules_shared.ColumnType) (string, bool) {
	switch colType {
	case modules_shared.ColumnTypeRangeString, modules_shared.ColumnTypeString:
		if v.Type == vm.TypeString {
			if s, ok := v.Data.(string); ok {
				return s, true
			}
		}

	case modules_shared.ColumnTypeRangeInt64, modules_shared.ColumnTypeRangeDuration,
		modules_shared.ColumnTypeInt64:
		var n int64
		switch v.Type {
		case vm.TypeInt:
			if i, ok := v.Data.(int64); ok {
				n = i
			} else {
				return "", false
			}
		case vm.TypeDuration:
			if i, ok := v.Data.(int64); ok {
				n = i
			} else {
				return "", false
			}
		case vm.TypeString: // NOTE-027: cross-encoding string literals against numeric index
			if s, ok := v.Data.(string); ok {
				parsed, err := strconv.ParseInt(s, 10, 64)
				if err != nil {
					return "", false
				}
				n = parsed
			} else {
				return "", false
			}
		default:
			return "", false
		}
		var buf [8]byte
		binary.LittleEndian.PutUint64(buf[:], uint64(n)) //nolint:gosec
		return string(buf[:]), true

	case modules_shared.ColumnTypeRangeUint64, modules_shared.ColumnTypeUint64:
		// TypeDuration values are int64 nanoseconds; span:duration is stored as
		// ColumnTypeUint64 (→ ColumnTypeRangeUint64 in the range index).
		// Reject negative values: TraceQL accepts negative duration literals (e.g. -5ms),
		// and casting a negative int64 to uint64 would wrap to a huge value, causing
		// incorrect pruning (false negatives / data loss).
		switch v.Type {
		case vm.TypeInt:
			if i, ok := v.Data.(int64); ok {
				if i < 0 {
					return "", false
				}
				var buf [8]byte
				binary.LittleEndian.PutUint64(buf[:], uint64(i)) //nolint:gosec // safe: i >= 0
				return string(buf[:]), true
			}
		case vm.TypeDuration:
			if i, ok := v.Data.(int64); ok {
				if i < 0 {
					return "", false
				}
				var buf [8]byte
				binary.LittleEndian.PutUint64(buf[:], uint64(i)) //nolint:gosec // safe: i >= 0
				return string(buf[:]), true
			}
		}

	case modules_shared.ColumnTypeRangeFloat64, modules_shared.ColumnTypeFloat64:
		var f float64
		switch v.Type {
		case vm.TypeFloat:
			if fv, ok := v.Data.(float64); ok {
				f = fv
			} else {
				return "", false
			}
		case vm.TypeString: // NOTE-027: cross-encoding string literals against float64 index
			if s, ok := v.Data.(string); ok {
				parsed, err := strconv.ParseFloat(s, 64)
				// NOTE-040: writer excludes negative floats from the float range index
				// (negative IEEE-754 bit patterns sort in reverse under LE comparison).
				// Reject negative thresholds here to match — no false negatives.
				if err != nil || parsed < 0 {
					return "", false
				}
				f = parsed
			} else {
				return "", false
			}
		default:
			return "", false
		}
		{
			var buf [8]byte
			binary.LittleEndian.PutUint64(buf[:], math.Float64bits(f))
			return string(buf[:]), true
		}
	}

	return "", false
}

// countIntrinsicLeaves counts all leaf nodes recursively in a RangeNode tree.
// Used to compute the overFetch multiplier for BlockRefsFromIntrinsicTOC.
func countIntrinsicLeaves(node vm.RangeNode) int {
	if len(node.Children) == 0 {
		if node.Column == "" {
			return 0
		}
		return 1
	}
	n := 0
	for _, child := range node.Children {
		n += countIntrinsicLeaves(child)
	}
	return n
}

// unionBlockRefs merges two BlockRef slices, deduplicating by (BlockIdx, RowIdx).
func unionBlockRefs(a, b []modules_shared.BlockRef) []modules_shared.BlockRef {
	type refKey struct{ blockIdx, rowIdx uint16 }
	seen := make(map[refKey]struct{}, len(a)+len(b))
	result := make([]modules_shared.BlockRef, 0, len(a)+len(b))
	for _, ref := range a {
		k := refKey{ref.BlockIdx, ref.RowIdx}
		if _, ok := seen[k]; !ok {
			seen[k] = struct{}{}
			result = append(result, ref)
		}
	}
	for _, ref := range b {
		k := refKey{ref.BlockIdx, ref.RowIdx}
		if _, ok := seen[k]; !ok {
			seen[k] = struct{}{}
			result = append(result, ref)
		}
	}
	return result
}

// intersectBlockRefSets intersects multiple BlockRef slices, returning only refs
// present in all sets. Uses smallest-first strategy for efficiency.
func intersectBlockRefSets(sets [][]modules_shared.BlockRef, limit int) []modules_shared.BlockRef {
	if len(sets) == 0 {
		return nil
	}
	if len(sets) == 1 {
		if limit > 0 && len(sets[0]) > limit {
			return sets[0][:limit]
		}
		return sets[0]
	}

	// Intersect: start from smallest set, check membership in others.
	// Sort sets by size ascending for efficiency.
	slices.SortFunc(sets, func(a, b []modules_shared.BlockRef) int {
		return len(a) - len(b)
	})

	// Build lookup sets for all but the first (smallest).
	type refKey struct{ blockIdx, rowIdx uint16 }
	lookups := make([]map[refKey]struct{}, len(sets)-1)
	for i, s := range sets[1:] {
		m := make(map[refKey]struct{}, len(s))
		for _, ref := range s {
			m[refKey{ref.BlockIdx, ref.RowIdx}] = struct{}{}
		}
		lookups[i] = m
	}

	var result []modules_shared.BlockRef
	for _, ref := range sets[0] {
		k := refKey{ref.BlockIdx, ref.RowIdx}
		inAll := true
		for _, lk := range lookups {
			if _, ok := lk[k]; !ok {
				inAll = false
				break
			}
		}
		if inAll {
			result = append(result, ref)
			if limit > 0 && len(result) >= limit {
				break
			}
		}
	}
	return result
}

// evalNodeBlockRefs recursively evaluates a RangeNode tree against intrinsic column blobs.
// Returns (refs, true) when the node is fully evaluable; (nil, false) when not.
// OR nodes: union all children refs; fail if any child is not evaluable.
// AND nodes: intersect all children refs; fail if any child is not evaluable.
//
// NOTE-039: recursive OR/AND evaluation for intrinsic fast path. Both OR and AND must
// fail fast on unevaluable children because refs are returned directly as results without
// VM re-evaluation — skipping any child would return rows that don't satisfy the full predicate.
func evalNodeBlockRefs(
	r *modules_reader.Reader,
	node vm.RangeNode,
	overFetch int,
) ([]modules_shared.BlockRef, bool) {
	// Leaf node.
	if len(node.Children) == 0 {
		if node.Column == "" {
			return nil, false
		}
		refs := scanIntrinsicLeafRefs(r, node.Column, node, overFetch)
		return refs, refs != nil
	}

	if node.IsOR {
		// OR: union — all children must be evaluable.
		var union []modules_shared.BlockRef
		for _, child := range node.Children {
			childRefs, ok := evalNodeBlockRefs(r, child, overFetch)
			if !ok {
				return nil, false // any unevaluable child makes OR unevaluable
			}
			if union == nil {
				union = childRefs
			} else {
				union = unionBlockRefs(union, childRefs)
			}
		}
		if union == nil {
			union = []modules_shared.BlockRef{} // evaluable but empty
		}
		return union, true
	}

	// AND: intersect — all children must be evaluable.
	// Refs are returned directly as results without VM re-evaluation, so skipping
	// any child would return rows that do not satisfy the full predicate.
	var evaluableSets [][]modules_shared.BlockRef
	for _, child := range node.Children {
		childRefs, ok := evalNodeBlockRefs(r, child, overFetch)
		if !ok {
			return nil, false // any unevaluable child makes AND unevaluable
		}
		evaluableSets = append(evaluableSets, childRefs)
	}
	if len(evaluableSets) == 0 {
		return nil, false
	}
	return intersectBlockRefSets(evaluableSets, overFetch), true
}

// BlockRefsFromIntrinsicTOC returns up to limit matching BlockRefs for an intrinsic-only
// query, reading only the intrinsic column section (no full block I/O).
//
// Returns nil when:
//   - The file has no intrinsic section
//   - No top-level nodes are evaluable from intrinsic data
//
// Note: this function does NOT check whether the program is intrinsic-only.
// Callers must verify that separately (see ProgramIsIntrinsicOnly).
//
// NOTE-039: Uses recursive evalNodeBlockRefs to support OR and regex predicates.
// Top-level nodes are AND-combined (each node in program.Predicates.Nodes is intersected).
// If any top-level node is not evaluable, returns nil (fast path not applicable).
func BlockRefsFromIntrinsicTOC(r *modules_reader.Reader, program *vm.Program, limit int) []modules_shared.BlockRef {
	if !r.HasIntrinsicSection() || program == nil || program.Predicates == nil {
		return nil
	}
	if len(program.Predicates.Nodes) == 0 {
		return nil
	}
	// limit == 0 means no limit (return all matching refs).
	// limit < 0 is invalid — return nil.
	if limit < 0 {
		return nil
	}

	// Count total leaves for overFetch calculation.
	totalLeaves := 0
	for _, node := range program.Predicates.Nodes {
		totalLeaves += countIntrinsicLeaves(node)
	}
	if totalLeaves == 0 {
		return nil
	}
	overFetch := 0
	if limit > 0 {
		overFetch = limit * totalLeaves
	}

	// Evaluate each top-level node and intersect (top-level = AND-combined).
	var topSets [][]modules_shared.BlockRef
	for _, node := range program.Predicates.Nodes {
		refs, ok := evalNodeBlockRefs(r, node, overFetch)
		if !ok {
			return nil // any unevaluable top-level node → fast path not applicable
		}
		topSets = append(topSets, refs)
	}

	if len(topSets) == 0 {
		return nil
	}

	result := intersectBlockRefSets(topSets, limit)
	return result
}

// scanIntrinsicLeafRefs loads the raw column blob and scans it directly for matching refs,
// avoiding full struct materialization. Falls back to the full-decode path if raw scanning
// is not applicable (e.g., bytes flat columns, regex patterns).
func scanIntrinsicLeafRefs(
	r *modules_reader.Reader,
	colName string,
	leaf vm.RangeNode,
	maxRefs int,
) []modules_shared.BlockRef {
	meta, ok := r.IntrinsicColumnMeta(colName)
	if !ok {
		return nil
	}

	blob, err := r.GetIntrinsicColumnBlob(colName)
	if err != nil || blob == nil {
		return nil
	}

	if meta.Format == modules_shared.IntrinsicFormatDict {
		if len(leaf.Values) == 0 && leaf.Pattern == "" {
			return nil // range predicate on dict — not supported in raw scan
		}
		// Regex predicate: compile pattern and scan dict entries.
		// NOTE-039: regex on dict columns uses ScanDictColumnRefsWithBloom with nil bloom keys
		// (no bloom pruning possible for regex — any page could have matching entries).
		if len(leaf.Values) == 0 && leaf.Pattern != "" {
			re, err := regexp.Compile(leaf.Pattern)
			if err != nil {
				return nil // invalid regex — skip fast path
			}
			return modules_shared.ScanDictColumnRefsWithBloom(blob, func(value string, _ int64, isInt64 bool) bool {
				if isInt64 {
					return false // int64 dict entries are not string-matchable
				}
				return re.MatchString(value)
			}, nil, maxRefs)
		}
		// Build match sets.
		wantStr := make(map[string]struct{}, len(leaf.Values))
		wantInt := make(map[int64]struct{}, len(leaf.Values))
		for _, v := range leaf.Values {
			switch v.Type {
			case vm.TypeString:
				if s, ok := v.Data.(string); ok {
					wantStr[s] = struct{}{}
				}
			case vm.TypeInt:
				if i, ok := v.Data.(int64); ok {
					wantInt[i] = struct{}{}
				}
			case vm.TypeDuration:
				if i, ok := v.Data.(int64); ok {
					wantInt[i] = struct{}{}
				}
			}
		}
		// Build bloom keys for page-level pruning on v2 paged blobs.
		bloomKeys := make([][]byte, 0, len(leaf.Values))
		for _, v := range leaf.Values {
			switch v.Type {
			case vm.TypeString:
				if s, ok := v.Data.(string); ok {
					bloomKeys = append(bloomKeys, []byte(s))
				}
			case vm.TypeInt:
				if i, ok := v.Data.(int64); ok {
					var buf [8]byte
					binary.LittleEndian.PutUint64(buf[:], uint64(i)) //nolint:gosec
					bloomKeys = append(bloomKeys, buf[:])
				}
			case vm.TypeDuration:
				if i, ok := v.Data.(int64); ok {
					var buf [8]byte
					binary.LittleEndian.PutUint64(buf[:], uint64(i)) //nolint:gosec
					bloomKeys = append(bloomKeys, buf[:])
				}
			}
		}
		return modules_shared.ScanDictColumnRefsWithBloom(blob, func(value string, int64Val int64, isInt64 bool) bool {
			if isInt64 {
				_, ok := wantInt[int64Val]
				return ok
			}
			_, ok := wantStr[value]
			return ok
		}, bloomKeys, maxRefs)
	}

	// Flat column — extract range bounds.
	var lo, hi uint64
	var hasLo, hasHi bool

	switch {
	case len(leaf.Values) > 0:
		// Equality: scan for each value. Use full-decode fallback for simplicity
		// since equality on flat columns is rare and may have multiple values.
		col, err := r.GetIntrinsicColumn(colName)
		if err != nil || col == nil {
			return nil
		}
		return intrinsicFlatMatchRefs(col, leaf, maxRefs)

	case leaf.Min != nil || leaf.Max != nil:
		if leaf.Min != nil {
			v, ok := valueToUint64(*leaf.Min)
			if !ok {
				return nil
			}
			lo, hasLo = v, true
		}
		if leaf.Max != nil {
			v, ok := valueToUint64(*leaf.Max)
			if !ok {
				return nil
			}
			hi, hasHi = v, true
		}

	default:
		return nil // pattern or no constraint
	}

	return modules_shared.ScanFlatColumnRefs(blob, lo, hi, hasLo, hasHi, maxRefs)
}

// intrinsicFlatMatchRefs returns up to max BlockRefs from a flat (uint64-sorted) column
// matching the predicate range. Returns nil when the predicate cannot be evaluated.
func intrinsicFlatMatchRefs(
	col *modules_shared.IntrinsicColumn,
	leaf vm.RangeNode,
	limit int,
) []modules_shared.BlockRef {
	if len(col.Uint64Values) == 0 {
		return nil // bytes flat column — not range-searchable
	}

	vals := col.Uint64Values
	var start, end int

	switch {
	case len(leaf.Values) > 0:
		// Equality match.
		var result []modules_shared.BlockRef
		for _, v := range leaf.Values {
			target, ok := valueToUint64(v)
			if !ok {
				return nil
			}
			i := sortSearchUint64(vals, target)
			for i < len(vals) && vals[i] == target {
				result = append(result, col.BlockRefs[i])
				if limit > 0 && len(result) >= limit {
					return result
				}
				i++
			}
		}
		return result

	case leaf.Min != nil || leaf.Max != nil:
		start = 0
		end = len(vals)
		if leaf.Min != nil {
			lo, ok := valueToUint64(*leaf.Min)
			if !ok {
				return nil
			}
			start = sortSearchUint64(vals, lo)
		}
		if leaf.Max != nil {
			hi, ok := valueToUint64(*leaf.Max)
			if !ok {
				return nil
			}
			end = sortSearchUint64(vals, hi+1)
			if hi == ^uint64(0) {
				end = len(vals)
			}
		}

	default:
		return nil
	}

	if start >= end {
		return []modules_shared.BlockRef{} // no matches but evaluable
	}
	count := end - start
	if limit > 0 && count > limit {
		count = limit
	}
	result := make([]modules_shared.BlockRef, count)
	copy(result, col.BlockRefs[start:start+count])
	return result
}
