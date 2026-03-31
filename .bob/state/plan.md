# Implementation Plan: Intrinsic Section Optimization

## Overview

Remove identity columns (trace:id, span:id, span:parent_id, span:status_message) from the
intrinsic accumulator, switch range-predicate field population from O(N) intrinsic scan to
O(M) block reads, and remove the RefBloom per-page filter (256 bytes/page, 100% FPR at 10K
entries). All three changes land together because they are tightly coupled and share a single
backward-compat story.

The vendor copy at `/home/matt/source/tempo-mrd/vendor/github.com/grafana/blockpack/` is the
source of truth. All changes apply there first, then are mirrored to the upstream source at
`/home/matt/source/blockpack-tempo/`.

---

## Spec-Driven Modules in Scope

| Module | Files with spec docs |
|--------|---------------------|
| `internal/modules/blockio/shared/` | NOTES.md |
| `internal/modules/blockio/writer/` | NOTES.md |
| `internal/modules/executor/` | SPECS.md, NOTES.md, TESTS.md, BENCHMARKS.md |

All three modules carry the `// NOTE: Any changes to this file must be reflected in the
corresponding specs.md or NOTES.md.` invariant. Doc updates are paired with code changes.

---

## Files to Modify (vendor copy — apply first)

1. `internal/modules/blockio/shared/constants.go` — remove `IntrinsicRefBloomBytes`, `IntrinsicRefBloomK`
2. `internal/modules/blockio/shared/types.go` — remove `RefBloom []byte` from `PageMeta`; remove `RefIndexEntry` type and `refIndex`/`refIndexOnce` from `IntrinsicColumn`
3. `internal/modules/blockio/shared/intrinsic_ref_filter.go` — delete file (all functions become dead)
4. `internal/modules/blockio/shared/intrinsic_codec.go` — remove RefBloom from `EncodePageTOC`; update `DecodePageTOC` to read-and-discard RefBloom bytes from old v0x02 files without storing them
5. `internal/modules/blockio/shared/NOTES.md` — add dated entry for RefBloom removal
6. `internal/modules/blockio/writer/intrinsic_accum.go` — remove `computePageRefRange`, `collectDictPageRefs`; remove `RefBloom`/`MinRef`/`MaxRef` from `PageMeta` literals in `encodePagedFlatColumn` and `encodePagedDictColumn`
7. `internal/modules/blockio/writer/writer_block.go` — remove `feedIntrinsicBytes` calls for `trace:id`, `span:id`, `span:parent_id` in `addRowFromProto`, `addRowFromTempoProto`, `applyTraceID`, `applySpanID`, `applySpanParentID`; remove `feedIntrinsicString` call for `span:status_message` in same functions; remove `feedIntrinsicBytes("trace:id", ...)` in `applyTraceID`; fix `feedIntrinsicsFromIndex` to skip the four identity columns
8. `internal/modules/blockio/writer/NOTES.md` — add dated entry documenting removal of identity columns from intrinsic accumulator
9. `internal/modules/executor/stream.go` — remove `useIntrinsicLookup` branch from `collectIntrinsicPlain`; always use `forEachBlockInGroups`; remove `useIntrinsicLookup bool` parameter from `collectIntrinsicPlain`; update call site at line 648
10. `internal/modules/executor/SPECS.md` — update field-population invariant: block reads are always used for field population in Case A
11. `internal/modules/executor/NOTES.md` — add dated entry for the change
12. `internal/modules/executor/TESTS.md` — update test plan for `execution_path_test.go` changes
13. `internal/modules/executor/execution_path_test.go` — update EP-01 and EP-03 to assert `Block` populated (not `IntrinsicFields`); update EP-02 description to match unified behavior; remove EP-01/EP-03 `IntrinsicFields != nil` assertions

## Files to Mirror (upstream source — apply after vendor)

Same list but rooted at `/home/matt/source/blockpack-tempo/`. Note that `RefBloom`/`MinRef`/`MaxRef` and `IntrinsicPageTOCVersion2` may not exist yet in the upstream source — adapt as needed (the changes may be no-ops for those symbols if the upstream is at an older version).

---

## Implementation Steps

### Phase 1: Update Tests First (TDD)

**Step 1.1: Update `execution_path_test.go` to reflect new behavior**

File: `vendor/github.com/grafana/blockpack/internal/modules/executor/execution_path_test.go`

After the change, `collectIntrinsicPlain` always uses `forEachBlockInGroups`. This means:
- Range predicates now return `MatchedRow.Block` populated (not `IntrinsicFields`)
- The comment at the top of the file must be updated to remove the distinction

Actions:
- [ ] Update the file-header comment: remove "Range/regex predicates → IntrinsicFields"; state "all collectIntrinsicPlain results return Block populated"
- [ ] Update `TestExecutionPath_RangePredicate_IntrinsicFields` (EP-01):
  - Rename to `TestExecutionPath_RangePredicate_BlockPopulated`
  - Change assertions: `row.Block != nil` (not `IntrinsicFields`)
  - Change assertions: `row.IntrinsicFields == nil` (not Block)
  - Keep result count assertions unchanged
- [ ] Update EP-03 `TestExecutionPath_RangeAndEquality_IntrinsicFields`:
  - Rename to `TestExecutionPath_RangeAndEquality_BlockPopulated`
  - Same assertion swap: Block not nil, IntrinsicFields nil
- [ ] Update EP-02 `TestExecutionPath_EqualityPredicate_BlockPopulated`:
  - Comment update only (the assertions are already correct: Block != nil, IntrinsicFields nil)
  - Remove note about "different from range predicate path" since they now use the same path

**Step 1.2: Verify tests fail before implementation**

```bash
cd /home/matt/source/tempo-mrd && go test ./vendor/github.com/grafana/blockpack/internal/modules/executor/ -run TestExecutionPath_RangePredicate_BlockPopulated -v 2>&1 | head -30
```

Expected: compilation error (function renamed) or test failure (IntrinsicFields still populated by old code).

---

### Phase 2: Remove RefBloom from `shared/`

**Step 2.1: Remove RefBloom constants**

File: `vendor/github.com/grafana/blockpack/internal/modules/blockio/shared/constants.go`

Remove these two constants (lines ~83-94):
```go
// IntrinsicRefBloomBytes is the fixed size of the per-page ref bloom filter in bytes.
IntrinsicRefBloomBytes = 256
// IntrinsicRefBloomK is the number of hash functions for the ref bloom filter.
IntrinsicRefBloomK = 3
```

Keep `IntrinsicPageTOCVersion2` — still needed by the codec to detect old files.

Verify:
```bash
cd /home/matt/source/tempo-mrd && go build ./vendor/github.com/grafana/blockpack/internal/modules/blockio/shared/ 2>&1
```
Expected: errors for `IntrinsicRefBloomBytes` references (in `intrinsic_accum.go`, `intrinsic_ref_filter.go`). That is correct — those will be fixed in subsequent steps.

**Step 2.2: Remove RefBloom from `PageMeta` and `IntrinsicColumn` types**

File: `vendor/github.com/grafana/blockpack/internal/modules/blockio/shared/types.go`

Changes to `PageMeta`:
- Remove `RefBloom []byte` field
- Remove `MinRef uint32` and `MaxRef uint32` fields
- Remove comments for those fields

Changes to `IntrinsicColumn`:
- Remove `refIndex []RefIndexEntry` field
- Remove `refIndexOnce sync.Once` field
- Remove `sync` import if no longer used

Remove type `RefIndexEntry` entirely (was used only by `refIndex`).

After removal, `PageMeta` becomes:
```go
type PageMeta struct {
    Min      string
    Max      string
    Bloom    []byte
    Offset   uint32
    Length   uint32
    RowCount uint32
}
```

And `IntrinsicColumn` no longer has `refIndex`/`refIndexOnce`/`RefIndexEntry` fields.

**Step 2.3: Delete `intrinsic_ref_filter.go`**

File to delete: `vendor/github.com/grafana/blockpack/internal/modules/blockio/shared/intrinsic_ref_filter.go`

This file contains: `matchesRefFilter`, `refFilterRange`, `DecodePagedColumnBlobFiltered`, `EnsureRefIndex`, `LookupRefFast`, `LookupRef`, `uint32ToLE`.

Before deleting, grep to confirm no other callers outside this file:
```bash
cd /home/matt/source/tempo-mrd && grep -r "DecodePagedColumnBlobFiltered\|EnsureRefIndex\|LookupRefFast\|matchesRefFilter" vendor/github.com/grafana/blockpack/ --include="*.go" | grep -v "_test.go" | grep -v "intrinsic_ref_filter.go"
```
Expected: matches in `internal/modules/blockio/reader/intrinsic_reader.go` and possibly `executor/`. Find all callers.

Check if `LookupRef` has callers:
```bash
cd /home/matt/source/tempo-mrd && grep -rn "\.LookupRef\b" vendor/github.com/grafana/blockpack/ --include="*.go"
```

If `LookupRef` (non-Fast) has callers outside this file, move it to `intrinsic_codec.go` before deleting. If it has no callers, delete.

To delete the file: replace its content with an empty package declaration or use `rm`. Since we cannot use Bash rm, overwrite with a redirect comment marking it for deletion. Actually — the Write tool can overwrite; but we cannot delete. Instead, remove all exported functions and make the file compile-clean with just the package declaration. The linter (`deadcode`) will flag unused unexported helpers. Best approach: keep the file but remove all functions except `LookupRef` if it has callers; otherwise overwrite with just `package shared`.

**Step 2.4: Update `intrinsic_codec.go` — EncodePageTOC**

File: `vendor/github.com/grafana/blockpack/internal/modules/blockio/shared/intrinsic_codec.go`

In `EncodePageTOC`, remove the v0x02 block that writes `MinRef`, `MaxRef`, `RefBloom`:
```go
// REMOVE these lines:
binary.LittleEndian.PutUint32(tmp4[:], p.MinRef)
buf.Write(tmp4[:])
binary.LittleEndian.PutUint32(tmp4[:], p.MaxRef)
buf.Write(tmp4[:])
binary.LittleEndian.PutUint16(tmp2[:], uint16(len(p.RefBloom)))
buf.Write(tmp2[:])
buf.Write(p.RefBloom)
```

Change `EncodePageTOC` to write `IntrinsicPageTOCVersion2` still (to avoid changing the version byte, which keeps the wire format stable for new files). Actually — since we're removing the ref-range fields, we should write `0x01` or a new version. Best decision: write version `0x01` for new files (no ref-range fields). Old v0x02 files can still be read. This avoids creating a third version.

Update `EncodePageTOC`:
- Change `buf.WriteByte(IntrinsicPageTOCVersion2)` to `buf.WriteByte(0x01)` — new files are v0x01, no ref-range fields.

Update `DecodePageTOC`:
- Keep v0x01 decode path unchanged (already reads min/max/bloom, sets conservative MinRef/MaxRef defaults)
- Keep v0x02 decode path: read MinRef/MaxRef/RefBloom bytes but discard them (do not store in `PageMeta` since those fields are removed). This preserves backward compat with existing files.

The v0x02 decode path becomes:
```go
if version == IntrinsicPageTOCVersion2 {
    // Read and discard ref-range index fields from legacy v0x02 files.
    if pos+10 > len(raw) {
        return PagedIntrinsicTOC{}, fmt.Errorf("DecodePageTOC: truncated at ref-range fields")
    }
    pos += 4 // MinRef — discard
    pos += 4 // MaxRef — discard
    refBloomLen := int(binary.LittleEndian.Uint16(raw[pos:]))
    pos += 2
    if refBloomLen > 0 {
        if pos+refBloomLen > len(raw) {
            return PagedIntrinsicTOC{}, fmt.Errorf("DecodePageTOC: truncated at ref_bloom")
        }
        pos += refBloomLen // RefBloom — discard
    }
}
```

**Step 2.5: Update `shared/NOTES.md`**

Append a new dated entry documenting the removal. The entry must be assigned the next sequential ID (check existing entries — current last is NOTE-006).

New entry:
```markdown
## NOTE-007: RefBloom Removed from Page TOC (2026-03-29)
*Added: 2026-03-29*

**Decision:** Removed `RefBloom []byte`, `MinRef uint32`, and `MaxRef uint32` from
`PageMeta`. Removed `IntrinsicRefBloomBytes` (256) and `IntrinsicRefBloomK` (3) constants.
Removed `RefIndexEntry` type and `refIndex`/`refIndexOnce` fields from `IntrinsicColumn`.
Deleted (emptied) `intrinsic_ref_filter.go`. `EncodePageTOC` now writes version 0x01 (no
ref-range fields). `DecodePageTOC` reads and discards the ref-range bytes in v0x02 files for
backward compatibility.

**Rationale:** RefBloom was designed to skip pages during reverse-lookup (lookupIntrinsicFields).
After the companion change that switches field population entirely to `forEachBlockInGroups`
(block reads), there are no remaining reverse-lookup callers. The ref-bloom provided zero
pruning benefit at 10K entries/page with 256 bytes (FPR ≈ 100% when full). Removal saves
256 bytes/page of storage and eliminates the bloom maintenance cost at write time.

**Backward compat:** v0x02 files decode correctly — the ref-range bytes are read and
discarded. New files write v0x01 (no ref-range fields).

Back-ref: `shared/constants.go`, `shared/types.go`, `shared/intrinsic_codec.go`,
`writer/intrinsic_accum.go`
```

---

### Phase 3: Remove Identity Columns from Writer

**Step 3.1: Update `writer/intrinsic_accum.go`**

File: `vendor/github.com/grafana/blockpack/internal/modules/blockio/writer/intrinsic_accum.go`

Remove functions `computePageRefRange` and `collectDictPageRefs` (lines 702–737).

In `encodePagedFlatColumn`, update the `PageMeta` literal (lines ~502–514):
```go
// BEFORE:
minRef, maxRef, refBloom := computePageRefRange(c.refs[start:end])
pages = append(pages, shared.PageMeta{
    Offset:   offset,
    Length:   uint32(len(blob)),
    RowCount: uint32(end - start),
    Min:      minVal,
    Max:      maxVal,
    MinRef:   minRef,
    MaxRef:   maxRef,
    RefBloom: refBloom,
})

// AFTER:
pages = append(pages, shared.PageMeta{
    Offset:   offset,
    Length:   uint32(len(blob)),   //nolint:gosec
    RowCount: uint32(end - start), //nolint:gosec
    Min:      minVal,
    Max:      maxVal,
})
```

In `encodePagedDictColumn`, update the `PageMeta` literal (lines ~668–681):
```go
// BEFORE:
pageRefs := collectDictPageRefs(c.entries, entryRanges)
minRef, maxRef, refBloom := computePageRefRange(pageRefs)
pages = append(pages, shared.PageMeta{
    ...
    Bloom:    bloom,
    MinRef:   minRef,
    MaxRef:   maxRef,
    RefBloom: refBloom,
})

// AFTER:
pages = append(pages, shared.PageMeta{
    ...
    Bloom:    bloom,
})
```

Also update the comment at the top of `encodeColumn` (line ~188-189) which references "RefBloom" in the v2 format description.

**Step 3.2: Remove identity columns from `writer_block.go`**

File: `vendor/github.com/grafana/blockpack/internal/modules/blockio/writer/writer_block.go`

The four identity columns to remove from the intrinsic accumulator:
- `trace:id` (traceIDColumnName) — `feedIntrinsicBytes` calls
- `span:id` (spanIDColumnName) — `feedIntrinsicBytes` calls
- `span:parent_id` (spanParentIDColumnName) — `feedIntrinsicBytes` calls
- `span:status_message` (spanStatusMsgColumnName) — `feedIntrinsicString` calls

The `addPresent` calls for all four columns STAY (block column payloads still need these values for field population via `forEachBlockInGroups`).

Locations to change:

**In `addRowFromProto` (~lines 314–402):**
- Line 314: remove `b.feedIntrinsicBytes(traceIDColumnName, ...)`; keep `b.addPresent` on line 315
- Lines 320–321: remove `b.feedIntrinsicBytes(spanIDColumnName, ...)`; keep `b.addPresent`
- Lines 327–328: remove `b.feedIntrinsicBytes(spanParentIDColumnName, ...)`; keep `b.addPresent`
- Line 400: remove `b.feedIntrinsicString(spanStatusMsgColumnName, ...)`; keep `b.addPresent`

**In `addRowFromTempoProto` (~lines 504–577):**
- Line 504: remove `b.feedIntrinsicBytes(traceIDColumnName, ...)`; keep `b.addPresent`
- Lines 509–510: remove `b.feedIntrinsicBytes(spanIDColumnName, ...)`; keep `b.addPresent`
- Lines 514–516: remove `b.feedIntrinsicBytes(spanParentIDColumnName, ...)`; keep `b.addPresent`
- Line 576: remove `b.feedIntrinsicString(spanStatusMsgColumnName, ...)`; keep `b.addPresent`

**In `applyTraceID` (~line 742):**
- Remove `b.feedIntrinsicBytes("trace:id", ...)` call; keep `b.addPresent`

**In `applySpanID` (~line 753):**
- Remove `b.feedIntrinsicBytes(spanIDColumnName, ...)` call; keep `b.addPresent`

**In `applySpanParentID` (~line 762):**
- Remove `b.feedIntrinsicBytes(spanParentIDColumnName, ...)` call; keep `b.addPresent`

**In `applySpanStatusMsg` (grep for spanStatusMsgColumnName in addRowFromBlock path, ~line 898):**
- Remove `b.feedIntrinsicString(spanStatusMsgColumnName, ...)`; keep `b.addPresent`

**In `feedIntrinsicsFromIndex` (~lines 1057–1132):**
- Remove the `"trace:id"`, `spanIDColumnName`, `spanParentIDColumnName`, `spanStatusMsgColumnName` cases from the switch/if chain
- These are the compaction path — since source files may have these in their intrinsic section (old files), `buildIntrinsicBlockIndex` would still load them, but `feedIntrinsicsFromIndex` should skip them to avoid re-adding to the new file's intrinsic accumulator

**Step 3.3: Update `writer/NOTES.md`**

Append a new dated entry:

```markdown
## NOTE-005: Identity Columns Removed from Intrinsic Accumulator (2026-03-29)
*Added: 2026-03-29*

**Decision:** `feedIntrinsicBytes` calls for `trace:id`, `span:id`, `span:parent_id` and
`feedIntrinsicString` for `span:status_message` are removed from `addRowFromProto`,
`addRowFromTempoProto`, `applyTraceID`, `applySpanID`, `applySpanParentID`, and
`applySpanStatusMsg`. The corresponding `addPresent` calls are retained — block column
payloads still store these values. `feedIntrinsicsFromIndex` now skips these four columns
during compaction to avoid carrying them forward into new intrinsic accumulators.

**Rationale:** These are identity/display-only columns — they are never used as predicate
targets in the intrinsic predicate-evaluation path (`scanIntrinsicLeafRefs`). They only
appeared in the intrinsic section to support `lookupIntrinsicFields` reverse lookups during
field population. Since field population for Case A (plain) now uses `forEachBlockInGroups`
(block reads), the intrinsic section entries are redundant.

**Storage savings:** Removing these 4 columns shrinks the intrinsic section from 11 columns
to 7, approximately 48% reduction in intrinsic section size (~125 MB per large file). The
block column payloads retain these values unchanged.

**Back-ref:** `writer/writer_block.go:addRowFromProto`, `writer/writer_block.go:addRowFromTempoProto`,
`writer/writer_block.go:applyTraceID`, `writer/writer_block.go:feedIntrinsicsFromIndex`
```

---

### Phase 4: Update Executor — Remove `useIntrinsicLookup` Branch

**Step 4.1: Update `executor/stream.go`**

File: `vendor/github.com/grafana/blockpack/internal/modules/executor/stream.go`

**Change 1: Remove `useIntrinsicLookup` parameter from `collectIntrinsicPlain`**

Current signature (line ~782):
```go
func collectIntrinsicPlain(
    r *modules_reader.Reader,
    refs []modules_shared.BlockRef,
    opts CollectOptions,
    wantColumns map[string]struct{},
    secondPassCols map[string]struct{},
    stats *CollectStats,
    useIntrinsicLookup bool,
) ([]MatchedRow, error) {
```

New signature (remove `useIntrinsicLookup bool` parameter):
```go
func collectIntrinsicPlain(
    r *modules_reader.Reader,
    refs []modules_shared.BlockRef,
    opts CollectOptions,
    wantColumns map[string]struct{},
    secondPassCols map[string]struct{},
    stats *CollectStats,
) ([]MatchedRow, error) {
```

**Change 2: Remove the `useIntrinsicLookup` branch inside `collectIntrinsicPlain`**

Current code (~lines 807–820):
```go
if useIntrinsicLookup {
    // Range predicate path: resolve fields from cached intrinsic blobs, zero block reads.
    fieldMaps := lookupIntrinsicFields(r, refs, secondPassCols)
    results := make([]MatchedRow, 0, len(refs))
    for i, ref := range refs {
        results = append(results, MatchedRow{
            IntrinsicFields: &intrinsicFieldsProvider{fields: fieldMaps[i]},
            BlockIdx:        int(ref.BlockIdx),
            RowIdx:          int(ref.RowIdx),
        })
    }
    return results, nil
}
```

Delete this entire `if` block. The function body becomes the current `else` path (equality path via `forEachBlockInGroups`) for all cases.

Update the function comment to reflect the new behavior:
```go
// collectIntrinsicPlain handles Case A: pure intrinsic + no sort.
// All results use forEachBlockInGroups to populate MatchedRow.Block.
// This is correct for both equality predicates (status=error, kind=server) and
// range predicates (duration>100ms, svc=~".*"). Block reads are O(M) where M is
// the result count — far cheaper than the previous O(N) intrinsic column scan
// for range predicates across 3.3M entries × 7 columns per file.
```

**Change 3: Update call site**

Current call (line ~648):
```go
return collectIntrinsicPlain(r, refs, opts, wantColumns, secondPassCols, stats, hasRangePredicate(program))
```

New call:
```go
return collectIntrinsicPlain(r, refs, opts, wantColumns, secondPassCols, stats)
```

**Step 4.2: Update `executor/SPECS.md`**

Find the section describing Case A / field population (search for "lookupIntrinsicFields" or "Case A"). Update to say:

```
Case A (pure intrinsic, no sort): uses forEachBlockInGroups for field population.
MatchedRow.Block is populated; MatchedRow.IntrinsicFields is nil.
This applies to both equality predicates and range predicates.
```

**Step 4.3: Update `executor/NOTES.md`**

Append a new dated entry. Current last note is around NOTE-050 or later — grep the file to find the last sequential ID before appending:

```bash
grep -n "## NOTE-0" /home/matt/source/tempo-mrd/vendor/github.com/grafana/blockpack/internal/modules/executor/NOTES.md | tail -5
```

New entry (assign next sequential ID after checking):
```markdown
## NOTE-0XX: collectIntrinsicPlain Always Uses Block Reads (2026-03-29)
*Added: 2026-03-29*

**Decision:** Removed the `useIntrinsicLookup` branch from `collectIntrinsicPlain`. Range
predicates (duration>X, svc=~".*") now use `forEachBlockInGroups` for field population,
the same path as equality predicates. The `useIntrinsicLookup bool` parameter is removed.

**Rationale:** The `useIntrinsicLookup` branch called `lookupIntrinsicFields` which scanned
all N entries in all intrinsic columns to find the M result refs. For a 3.3M-span file with
7 remaining intrinsic columns, this is O(3.3M × 7) = O(23M) operations even for a 10-result
query. `forEachBlockInGroups` groups refs by block and reads only the blocks containing
matched spans — O(M × block_read_cost). For M=10 results across a few blocks, this is
O(500K) bytes read vs O(286MB) of intrinsic scans: a 500x improvement.

**Impact on execution paths:** Case A results now always populate `MatchedRow.Block` (not
`IntrinsicFields`). `lookupIntrinsicFields` is retained — it is still used by Case B
(`collectIntrinsicTopK`) for timestamp-sorted top-K queries where ordering requires the
intrinsic timestamp column, and by `stream_structural.go` for identity field resolution.

**Test changes:** `execution_path_test.go` EP-01 and EP-03 assertions updated from
IntrinsicFields→populated/Block→nil to Block→populated/IntrinsicFields→nil.

Back-ref: `executor/stream.go:collectIntrinsicPlain`
```

**Step 4.4: Update `executor/TESTS.md`**

Append or update the test plan section for EP tests:

```markdown
## EP-01 (updated 2026-03-29)
Range predicates now use forEachBlockInGroups. EP-01 verifies Block is populated and
IntrinsicFields is nil (was reversed before 2026-03-29).

## EP-03 (updated 2026-03-29)
Range+equality combination also uses forEachBlockInGroups. EP-03 verifies Block populated.
```

---

### Phase 5: Test and Fix Compilation

**Step 5.1: Build vendor copy**

```bash
cd /home/matt/source/tempo-mrd && go build ./vendor/github.com/grafana/blockpack/... 2>&1
```

Expected: clean build. Fix any remaining references to removed symbols.

Common issues to watch for:
- Tests in `shared/intrinsic_ref_filter_test.go` — these test the deleted functions. They must be deleted or emptied.
- Tests in `executor/intrinsic_pruning_test.go` around `TestLookupIntrinsicFields_PageSkipping` — this calls `lookupIntrinsicFields` directly; it should still compile since `lookupIntrinsicFields` is retained.
- Tests in `executor/execution_path_test.go` — updated in Step 1.1.

**Step 5.2: Run tests for affected packages**

```bash
cd /home/matt/source/tempo-mrd && go test ./vendor/github.com/grafana/blockpack/internal/modules/blockio/shared/... -v -count=1 2>&1 | tail -30
```

```bash
cd /home/matt/source/tempo-mrd && go test ./vendor/github.com/grafana/blockpack/internal/modules/blockio/writer/... -v -count=1 2>&1 | tail -30
```

```bash
cd /home/matt/source/tempo-mrd && go test ./vendor/github.com/grafana/blockpack/internal/modules/executor/... -v -count=1 2>&1 | tail -40
```

Expected: all pass.

**Step 5.3: Build Tempo**

```bash
cd /home/matt/source/tempo-mrd && go build ./tempodb/... 2>&1
```

Expected: clean build (Tempo code calls blockpack public API which is unchanged).

**Step 5.4: Run Tempo tests**

```bash
cd /home/matt/source/tempo-mrd && go test ./tempodb/encoding/vblockpack/... -v -count=1 2>&1 | tail -30
```

---

### Phase 6: Mirror Changes to Upstream Source

**Step 6.1: Identify divergence between vendor and upstream**

```bash
diff -u /home/matt/source/blockpack-tempo/internal/modules/blockio/shared/constants.go \
         /home/matt/source/tempo-mrd/vendor/github.com/grafana/blockpack/internal/modules/blockio/shared/constants.go
```

If the upstream does not have `IntrinsicRefBloomBytes`/`IntrinsicRefBloomK`, there is nothing to remove. Apply only the changes that exist in both.

**Step 6.2: Apply the same changes to upstream**

For each file modified in vendor:
1. Read the current upstream file
2. Apply the equivalent changes (adapting for any version differences)
3. Write the updated file

**Step 6.3: Build upstream**

```bash
cd /home/matt/source/blockpack-tempo && go build ./... 2>&1
```

Expected: clean build.

**Step 6.4: Run upstream tests (optional but recommended)**

```bash
cd /home/matt/source/blockpack-tempo && go test ./internal/modules/... -count=1 2>&1 | tail -30
```

---

### Phase 7: Final Verification

**Step 7.1: Full build and test**

```bash
cd /home/matt/source/tempo-mrd && go build ./tempodb/... && echo "BUILD OK"
cd /home/matt/source/blockpack-tempo && go build ./... && echo "BUILD OK"
```

**Step 7.2: Run the parity smoke test (if available)**

```bash
cd /home/matt/source/tempo-mrd && go test ./vendor/github.com/grafana/blockpack/internal/parity/... -v -count=1 2>&1 | tail -20
```

---

## Spec-Driven Verification Tests

### Module: `internal/modules/executor/`

Source: SPECS.md and NOTES.md invariants

| Invariant | Test to Verify | Test File |
|-----------|---------------|-----------|
| Case A always populates `MatchedRow.Block` | `TestExecutionPath_RangePredicate_BlockPopulated` | execution_path_test.go |
| Case A never populates `MatchedRow.IntrinsicFields` | `TestExecutionPath_RangePredicate_BlockPopulated` | execution_path_test.go |
| Range+equality combination uses Block path | `TestExecutionPath_RangeAndEquality_BlockPopulated` | execution_path_test.go |
| Result counts unchanged (correctness invariant) | `TestExecutionPath_Correctness` (EP-05, no change needed) | execution_path_test.go |
| `lookupIntrinsicFields` still works for TopK | `TestIntrinsicTopK_*` (no change) | stream_topk_test.go |

### Module: `internal/modules/blockio/shared/`

| Invariant | Test to Verify | Test File |
|-----------|---------------|-----------|
| Old v0x02 files still decode without error | New test: `TestDecodePageTOC_V2LegacyBackwardCompat` | shared/intrinsic_ref_filter_test.go or codec test |
| New files encode as v0x01 (no ref-range fields) | New test: `TestEncodeDecodePageTOC_NoRefBloom` | shared test |

---

## Spec-Driven Module Updates

### Module: `internal/modules/blockio/shared/`

**Spec files present:** NOTES.md

**Required updates:**
- [ ] Add NOTES.md entry NOTE-007: RefBloom removal decision and backward compat story (Step 2.5)

### Module: `internal/modules/blockio/writer/`

**Spec files present:** NOTES.md

**Required updates:**
- [ ] Add NOTES.md entry NOTE-005: identity columns removed from intrinsic accumulator (Step 3.3)

### Module: `internal/modules/executor/`

**Spec files present:** SPECS.md, NOTES.md, TESTS.md, BENCHMARKS.md

**Required updates:**
- [ ] Update SPECS.md: Case A field population section — block reads always used (Step 4.2)
- [ ] Add NOTES.md entry NOTE-0XX: collectIntrinsicPlain change rationale (Step 4.3)
- [ ] Update TESTS.md: EP-01 and EP-03 test scenario description (Step 4.4)
- [ ] Update BENCHMARKS.md: Case A intrinsic query I/O target reduced (O(N) → O(M)) — update Metric Targets table if it has rows for range-predicate I/O cost

---

## Edge Cases to Handle

### Edge Case 1: Old v0x02 Files on Decode
**Scenario:** Existing blockpack files in production have RefBloom bytes in the page TOC.
**Expected:** `DecodePageTOC` reads and discards the bytes; no panic, no data corruption.
**Test:** `TestDecodePageTOC_V2LegacyBackwardCompat` — craft a v0x02 blob by hand and decode it.

### Edge Case 2: Compaction of Old Files (feedIntrinsicsFromIndex)
**Scenario:** Compaction source files (old format) have `trace:id`, `span:id`, `span:parent_id`, `span:status_message` in their intrinsic section. `buildIntrinsicBlockIndex` reads them; `feedIntrinsicsFromIndex` previously re-fed them.
**Expected:** After the change, `feedIntrinsicsFromIndex` skips these four columns. New compacted files do NOT include them in the intrinsic section.
**Test:** Existing compaction tests should cover this; verify they still pass.

### Edge Case 3: lookupIntrinsicFields Still Used by TopK
**Scenario:** Caller accidentally removes `lookupIntrinsicFields` thinking it's unused.
**Expected:** TopK path (`collectIntrinsicTopK`) calls `lookupIntrinsicFields` after selecting refs. If removed, compilation error.
**Mitigation:** Explicitly verify `lookupIntrinsicFields` has callers in `stream.go` and `stream_structural.go` after the change.

### Edge Case 4: intrinsic_ref_filter_test.go References Deleted Types
**Scenario:** `TestLookupRefFast`, `TestEnsureRefIndex`, etc. reference deleted functions.
**Expected:** Compilation error if not updated.
**Action:** Check test file content; either delete those specific test functions or replace them with backward-compat decode tests (see verification tests above).

---

## Risks

### Risk 1: Test `TestExecutionPath_RangePredicate_IntrinsicFields` fails before code change
**Risk:** If the test is updated before the implementation, it will fail for the right reason.
**Impact:** Expected — this is the TDD pattern. The test should fail until `collectIntrinsicPlain` is changed.
**Mitigation:** Plan explicitly calls for test update first (Phase 1), then implementation (Phases 2-4).

### Risk 2: `lookupIntrinsicFields` Still Correct After Removing Identity Columns
**Risk:** `collectIntrinsicTopK` calls `lookupIntrinsicFields(r, selected, secondPassCols)`. If `secondPassCols` contains `trace:id` or `span:id` and those are no longer in the intrinsic section, lookups return nil/zero.
**Impact:** TopK results might have empty trace:id/span:id fields.
**Mitigation:** After the change, `trace:id`/`span:id` are still in block column payloads (via `addPresent`). `lookupIntrinsicFields` reads from intrinsic columns — it will find nil for those names. `SpanMatchFromRow` reads from block columns when `lookupIntrinsicFields` returns nil. Verify `SpanMatchFromRow` fallback path handles nil correctly. Check `executor_test.go` EX-09 (SpanMatch.TraceID / SpanMatch.SpanID field population) passes.

### Risk 3: `stream_structural.go` lookupIntrinsicFields for identity fields
**Risk:** `stream_structural.go` line 235 calls `lookupIntrinsicFields(r, allRefs, intrinsicWant)` for identity columns (trace:id, span:id). After removing these from intrinsic section, this lookup returns empty maps.
**Impact:** Structural queries might lose trace/span ID values.
**Mitigation:** Check what `intrinsicWant` contains in `stream_structural.go`. If it requests trace:id/span:id, the lookup will return empty, and the code must fall back to block reads. This is a real risk — verify `stream_structural_test.go` still passes.

### Risk 4: `execution_path_test.go` comment references `hasRangePredicate`
**Risk:** The EP-01 test comment mentions `hasRangePredicate=true → lookupIntrinsicFields`. After the change this is stale.
**Impact:** Misleading documentation only; no runtime impact.
**Mitigation:** Update the comment in Step 1.1.

---

## Dependencies

### Internal Dependencies
- `shared/` is imported by `writer/`, `reader/`, and `executor/` — changes propagate
- `executor/stream.go` imports `modules_reader` and `modules_shared` — unchanged
- No new external imports needed

### External Dependencies
- None added

---

## Complexity Analysis

### Modified Functions
- `collectIntrinsicPlain`: complexity decreases (removes `if useIntrinsicLookup` branch)
- `encodePagedFlatColumn`: complexity unchanged (removes 3 lines, not a branch)
- `encodePagedDictColumn`: complexity unchanged
- `addRowFromProto`: complexity unchanged (removes 4 `feedIntrinsic*` calls, no branches)
- `feedIntrinsicsFromIndex`: complexity slightly decreases (fewer cases)

---

## Success Criteria

- [ ] `go build ./tempodb/...` passes cleanly
- [ ] `go build ./...` in blockpack-tempo passes cleanly
- [ ] All executor tests pass (`go test ./vendor/github.com/grafana/blockpack/internal/modules/executor/...`)
- [ ] All shared tests pass
- [ ] All writer tests pass
- [ ] EP-01 and EP-03 now assert `Block != nil` and `IntrinsicFields == nil` for range predicates
- [ ] EP-05 correctness tests unchanged (result counts unaffected)
- [ ] TopK tests (`stream_topk_test.go`) unchanged and passing
- [ ] Structural query tests (`stream_structural_test.go`) passing
- [ ] Executor EX-09 (TraceID/SpanID field population) passing
- [ ] NOTES.md updated in all three spec-driven modules
- [ ] SPECS.md updated in executor
- [ ] TESTS.md updated in executor
- [ ] Old v0x02 files decode without error (backward compat test)

---

## Notes

- The `hasRangePredicate(program)` call at the `collectIntrinsicPlain` call site (line 648) is no longer needed as an argument. The function itself may still be useful elsewhere; check whether it has other callers before removing it.
- `intrinsicFieldsProvider` struct and `IntrinsicFields` field on `MatchedRow` remain in place — they are still used by `collectIntrinsicTopK` (Case B). Only Case A's use of `IntrinsicFields` is removed.
- The `NOTE-050` comment at stream.go line 185 (about trace intrinsic columns in `secondPassCols`) remains correct — `lookupIntrinsicFields` is still used by Case B and structural queries and still needs these columns in `secondPassCols`.
- For the upstream mirror: if the upstream at `/home/matt/source/blockpack-tempo/` does not have `IntrinsicPageTOCVersion2`, `IntrinsicRefBloomBytes`, `IntrinsicRefBloomK`, `MinRef`, `MaxRef`, `RefBloom`, or `computePageRefRange`, those removals are no-ops. Apply only the changes that are applicable to the upstream's current state.

## Questions/Uncertainties

- Does `stream_structural.go`'s `lookupIntrinsicFields` call need to be replaced with a block-read for trace/span identity fields after removing them from the intrinsic section? Examine `intrinsicWant` construction in that function before finalizing.
- Does `TestLookupIntrinsicFields_PageSkipping` in `intrinsic_pruning_test.go` use `DecodePagedColumnBlobFiltered`? If so, that test becomes invalid and needs to be removed or replaced.
