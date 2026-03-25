# Discovery: Blockpack Regex and OR Bug in Intrinsic Column Evaluation

## Summary

Found **TWO CRITICAL BUGS** in the intrinsic column evaluation path that cause regex (`=~`) and OR (`||`) predicates on resource attributes to return 0 results when they should return matches:

1. **OR nodes are skipped entirely** in `collectIntrinsicLeaves()` — OR branches are never collected/evaluated
2. **Dict columns reject regex patterns** in `scanIntrinsicLeafRefs()` — patterns are not supported for dict columns (which hold resource attributes)

These bugs only manifest in the **intrinsic fast path** (when `ProgramIsIntrinsicOnly() == true` and the file has an intrinsic section). When combined with span-level attributes, the query falls back to block scanning, which works correctly.

---

## Observed Behavior

- `{resource.service.name="loki-querier"}` → ✓ returns spans (equality, dict column fast path)
- `{resource.service.name=~"loki-querier"}` → ✗ returns 0 spans (regex on dict column, fast path fails)
- `{resource.service.name=~"loki-.*"}` → ✗ returns 0 spans (regex on dict column, fast path fails)
- `{resource.service.name="CockroachDB" || resource.service.name="loki-querier"}` → ✗ returns 0 spans (OR node skipped in fast path)
- `{resource.service.name=~"loki-.*" && span.rpc.system="grpc"}` → ✓ returns spans (mixed predicate falls back to block scan)

---

## Root Cause Analysis

### Bug #1: OR Nodes Are Completely Skipped in Intrinsic Fast Path

**File:** `/home/matt/source/tempo-mrd/vendor/github.com/grafana/blockpack/internal/modules/executor/predicates.go`

**Function:** `collectIntrinsicLeaves()` (lines 340-360)

```go
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
		return  // ← PROBLEM: OR nodes silently return without collecting any children
	}
	for _, child := range node.Children {
		collectIntrinsicLeaves(child, dst)
	}
}
```

**The Problem:**
- When an OR node is encountered (e.g., `{A || B}`), the function returns immediately without recursing into children
- No leaves are collected from either branch of the OR
- Result: `BlockRefsFromIntrinsicTOC()` returns `nil` (line 1157-1158), which signals "predicate not evaluable"
- The intrinsic fast path is abandoned, falling back to block scanning

**Why It's Wrong:**
- OR predicates CAN be evaluated in the intrinsic path: collect leaves from each branch, union the results instead of intersecting
- The current comment says "too complex to intersect safely" — but that's the wrong operation. OR should UNION, not INTERSECT

---

### Bug #2: Dict Columns Don't Support Regex Patterns

**File:** `/home/matt/source/tempo-mrd/vendor/github.com/grafana/blockpack/internal/modules/executor/predicates.go`

**Function:** `scanIntrinsicLeafRefs()` (lines 1244-1297)

```go
if meta.Format == modules_shared.IntrinsicFormatDict {
	if len(leaf.Values) == 0 {
		return nil // range/pattern on dict — not supported in raw scan
	}
	// Build match sets for equality lookup...
	wantStr := make(map[string]struct{}, len(leaf.Values))
	// ... (only handles leaf.Values, not patterns)
	return modules_shared.ScanDictColumnRefsWithBloom(blob, func(value string, int64Val int64, isInt64 bool) bool {
		if isInt64 {
			_, ok := wantInt[int64Val]
			return ok
		}
		_, ok := wantStr[value]
		return ok
	}, bloomKeys, maxRefs)
}
```

**The Problem:**
- Line 1245-1246: when `len(leaf.Values) == 0` (i.e., a pattern like `=~"loki-.*"`), the function returns `nil`
- This happens even though the pattern **could** be evaluated against dict entries
- Signals "predicate not evaluable", causing `BlockRefsFromIntrinsicTOC()` to return `nil` (line 1172-1173)
- Falls back to block scanning

**Why It's Wrong:**
- Resource attributes (like `service.name`) are stored as **dict columns** in the intrinsic section
- Dict columns contain a small set of distinct values (typically 3-50) with BlockRef pointers
- A regex can easily be matched against these few distinct values without full decode
- The intrinsic evaluator **should** iterate dict entries and match the pattern, collecting all matching refs

**Where This Matters:**
- Any regex query on a dict-based intrinsic column returns 0 from the fast path
- Resource attributes are almost always dict columns (high cardinality attributes are filtered out of intrinsic)
- Trace intrinsic columns may also be dict columns

---

## Specific Code Locations to Fix

### Issue #1: OR Node Handling

**File:** `vendor/github.com/grafana/blockpack/internal/modules/executor/predicates.go`
**Function:** `collectIntrinsicLeaves()` (lines 340-360)
**Current behavior:** Line 354-355 returns early for OR nodes
**Fix needed:** Recurse into OR children and union the leaf sets

**Affected functions that call this:**
- `BlockRefsFromIntrinsicTOC()` (line 1155)
- `buildPredicateMatchSet()` (line 432)

### Issue #2: Dict Pattern Support

**File:** `vendor/github.com/grafana/blockpack/internal/modules/executor/predicates.go`
**Function:** `scanIntrinsicLeafRefs()` (lines 1244-1247)
**Current behavior:** Returns `nil` when `len(leaf.Values) == 0` for dict columns
**Fix needed:** Add pattern matching logic for dict columns

**Implementation approach:**
1. Check if `leaf.Pattern != ""` for dict columns
2. Compile the regex pattern
3. Iterate `blob.DictEntries` and match each value against the pattern
4. Collect refs for all matching entries

---

## Why Mixing Works (Partial Explanation)

Query: `{resource.service.name=~"loki-.*" && span.rpc.system="grpc"}` → ✓ Works

- When the predicate tree includes **both** intrinsic AND span-level attributes, `ProgramIsIntrinsicOnly()` returns `false`
- The executor skips the intrinsic fast paths (`collectIntrinsicTopK`, `collectIntrinsicPlain`)
- Falls back to full block scanning via `modeBlockPlain`
- During block scanning, the full `ColumnPredicate` evaluator handles all predicate types correctly (including regex on dict columns)
- Regular VM-based predicate evaluation is complete and correct; the bug only affects the **intrinsic shortcut path**

---

## Data Structure Context

### resource.service.name Storage

- Stored as an **intrinsic column** (if present in the file)
- Format: **Dict** (string values with distinct count typically < 100)
- Structure: `IntrinsicColumn.DictEntries[]` → each entry has:
  - `Value`: the string value (e.g., "loki-querier", "CockroachDB")
  - `BlockRefs`: slice of BlockRef pointers for rows with this value

### Why Dict Columns Use Dict Format

- Resource attributes have low cardinality (typically same 10-50 values across all spans)
- Dict encoding is space-efficient: store value once, point to all occurrences
- Intrinsic section summary data uses dict for fast block-level pruning
- Full block reading uses regular columnar format (still dict-encoded in some cases, but with different layout)

---

## Test Cases That Should Pass (After Fix)

```
1. {resource.service.name=~"loki-querier"} — Exact regex on resource attribute
2. {resource.service.name=~"loki-.*"} — Prefix regex on resource attribute
3. {resource.service.name="svc1" || resource.service.name="svc2"} — OR of two resource conditions
4. {resource.service.name=~"svc.*" || resource.service.name="special"} — OR with mixed equality/regex
5. {resource.a="x" || resource.b="y" || resource.c="z"} — Multi-branch OR
```

---

## Severity & Impact

- **Severity:** HIGH — Breaks entire feature class
- **Scope:** Queries using:
  - Regex (`=~`) on resource/log attributes
  - OR (`||`) combining multiple resource/log conditions
  - Only when file has intrinsic section AND predicate is intrinsic-only
- **Workaround:** Add a span-level condition to force block scanning (e.g., `&&  span:id != ""``)
- **Performance Impact:** Regex/OR queries take full-block-scan path (~1000x slower) instead of intrinsic-only path (~100ms)

---

## Files to Examine

| File | Lines | Purpose |
|------|-------|---------|
| `predicates.go` | 340-360 | `collectIntrinsicLeaves()` — OR node handler |
| `predicates.go` | 1228-1297 | `scanIntrinsicLeafRefs()` — dict pattern support needed here |
| `predicates.go` | 1140-1223 | `BlockRefsFromIntrinsicTOC()` — calls both above functions |
| `predicates.go` | 1289-1296 | `ScanDictColumnRefsWithBloom()` call — needs pattern variant |
| `executor.go` | 422-480 | `buildPredicateMatchSet()` — calls `collectIntrinsicLeaves()` |

---

## Related Code References

### How Intrinsic Columns Are Structured

See `vendor/github.com/grafana/blockpack/internal/modules/blockio/shared/intrinsic.go` for:
- `IntrinsicColumn` struct definition
- `IntrinsicFormatDict` vs `IntrinsicFormatFlat`
- Dict entry structure

### How Regular Column Predicates Work (For Reference)

When NOT in the intrinsic fast path:
- `executor.go` line 304: `program.ColumnPredicate(provider)` handles all predicate types
- Block-level `blockLabelSet` (in `block_label_set.go`) handles predicate evaluation per row
- Full regex and OR support works correctly here

### How Dict Columns Are Encoded

Writer side: `vendor/github.com/grafana/blockpack/internal/modules/blockio/writer/` — dict encoding
Reader side: `vendor/github.com/grafana/blockpack/internal/modules/blockio/reader/parser.go` — dict decoding

---

## Next Steps

1. Fix `collectIntrinsicLeaves()` to recurse OR children and union leaf sets
2. Extend `scanIntrinsicLeafRefs()` to handle regex patterns on dict columns
3. Add unit tests for:
   - Regex on resource attributes
   - OR of multiple resource conditions
   - Mixed regex + OR on intrinsic columns
4. Verify no performance regression (intrinsic path should still be fast)
5. Test against real traces with actual resource attribute values
