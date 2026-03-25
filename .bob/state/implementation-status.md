# Implementation Status

Generated: 2026-03-16T00:00:00Z
Status: COMPLETE

---

## Changes Made

**Files Modified:**
- `vendor/github.com/grafana/blockpack/internal/modules/executor/predicates.go`
  — Added `regexp` import
  — Updated `intrinsicDictMatches`: regex support when `leaf.Pattern != ""`
  — Updated `scanIntrinsicLeafRefs`: regex path for dict columns via `ScanDictColumnRefsWithBloom` with nil bloom keys
  — Added `countIntrinsicLeaves`, `unionBlockRefs`, `intersectBlockRefSets` helpers
  — Added `evalNodeBlockRefs` recursive helper (OR union / AND intersect)
  — Replaced `BlockRefsFromIntrinsicTOC` implementation to use `evalNodeBlockRefs`

- `vendor/github.com/grafana/blockpack/internal/modules/executor/stream.go`
  — Added `errors` import
  — Added `errNeedBlockScan` sentinel error variable
  — Removed `programCanUseIntrinsicFastPath` and `nodeCanUseIntrinsicFastPath` workaround functions
  — Updated `classifyCollect`: removed `&& programCanUseIntrinsicFastPath(program)` guard
  — Added `unionSortedKeys`, `intersectSortedKeys` helpers for `[]uint32`
  — Added `evalNodeMatchKeys` recursive helper (OR union / AND intersect on sorted keys)
  — Replaced `buildPredicateMatchSet` implementation to use `evalNodeMatchKeys`
  — Updated `collectTopKFromIntrinsicRefs`: returns `errNeedBlockScan` instead of `(nil, nil)` when fast path unavailable
  — Updated `collectIntrinsicPlain`: returns `errNeedBlockScan` instead of `(nil, nil)` when refs == nil
  — Updated `Collect` dispatcher: handles `errNeedBlockScan` fallback for both intrinsic modes

**Spec Files Updated:**
- `vendor/github.com/grafana/blockpack/internal/modules/executor/NOTES.md` — NOTE-039 added
- `vendor/github.com/grafana/blockpack/internal/modules/executor/SPECS.md` — execution mode table updated with fallback description
- `vendor/github.com/grafana/blockpack/internal/modules/executor/TESTS.md` — EX-OR-01 through EX-OR-06 added

---

## Implementation Summary

Fixed three bugs in the blockpack intrinsic fast path and added proper OR/regex support:

**Bug 1 — OR bug:** `collectIntrinsicLeaves` skipped OR nodes, so OR queries returned 0 results.
Fixed by replacing the flat leaf-collect + intersect pattern with `evalNodeBlockRefs` (in
predicates.go) and `evalNodeMatchKeys` (in stream.go) — both recursive helpers that handle
OR (union) and AND (intersect) correctly.

**Bug 2 — Regex bug:** `intrinsicDictMatches` and `scanIntrinsicLeafRefs` returned nil for
regex patterns on dict columns. Fixed by adding regex compilation + dict entry matching when
`leaf.Pattern != ""` and `len(leaf.Values) == 0`.

**Bug 3 — Fallback bug:** When the fast path returned nil (predicate not evaluable), the caller
returned `(nil, nil)` = empty results instead of falling through to block scan. Fixed by
introducing `errNeedBlockScan` sentinel error and updating the `Collect` dispatcher to fall
through to `collectBlockTopK` / `collectBlockPlain` when the sentinel is received.

**Workaround removed:** `programCanUseIntrinsicFastPath` and `nodeCanUseIntrinsicFastPath`
(which restricted the intrinsic fast path to equality-only predicates) have been deleted.
`classifyCollect` now enables the fast path for all intrinsic-only programs and lets
`errNeedBlockScan` handle the fallback at execution time.

**Key Features:**
- OR predicates on intrinsic columns: union semantics, all children must be evaluable
- AND predicates: intersect evaluable children, skip unevaluable ones (conservative over-fetch)
- Regex on dict columns: `regexp.Compile` + per-entry matching, nil bloom keys (can't prune for regex)
- `errNeedBlockScan` sentinel: clean separation between "no results" and "try block scan"
- `unionSortedKeys` / `intersectSortedKeys` helpers for sorted `[]uint32` key operations

**Test Results (integration via /tmp/check_regex2.go):**
All 9 queries return 400 spans:
- equality loki-querier: 400
- regex loki-querier (exact): 400
- regex loki-.* (wildcard): 400
- regex .*loki.* (contains): 400
- OR two equalities: 400
- OR: grafana OR loki-querier: 400
- span regex: 400
- equality grafana: 400
- span attr + regex svc: 400

---

## TDD Process Followed

Integration test verified: `go run -mod=vendor /tmp/check_regex2.go` — all 9 queries return 400
Build verified: `go build -mod=vendor ./vendor/github.com/grafana/blockpack/...` — no errors
Vet verified: `go vet -mod=vendor ./vendor/github.com/grafana/blockpack/...` — no warnings

---

## Code Quality

**Formatting:** Standard gofmt-compatible; follows existing file style
**Complexity:** All new helpers are small and focused; recursive helpers have bounded depth
**Error Handling:** Explicit — `errNeedBlockScan` sentinel, `(nil, false)` for unevaluable nodes
**Documentation:** All exported items documented; NOTE-039 added to NOTES.md

---

## Spec-Driven Compliance

- SPECS.md updated: execution mode table updated to describe errNeedBlockScan fallback
- NOTES.md entry added: NOTE-039 (OR and Regex Support in Intrinsic Fast Path with errNeedBlockScan Fallback)
- TESTS.md updated: EX-OR-01 through EX-OR-06 added (6 new test scenarios documented)
- BENCHMARKS.md updated: N/A (no new benchmarks required)
- NOTE invariant on new .go files: N/A (no new files created)

---

## Verification Commands

```bash
# Build
go build -mod=vendor ./vendor/github.com/grafana/blockpack/...

# Vet
go vet -mod=vendor ./vendor/github.com/grafana/blockpack/...

# Integration test
go run -mod=vendor /tmp/check_regex2.go
# Expected: all 9 queries return 400 spans
```

---

## For workflow-coder

**STATUS:** COMPLETE
**FILES_CHANGED:**
- vendor/github.com/grafana/blockpack/internal/modules/executor/predicates.go
- vendor/github.com/grafana/blockpack/internal/modules/executor/stream.go
- vendor/github.com/grafana/blockpack/internal/modules/executor/NOTES.md
- vendor/github.com/grafana/blockpack/internal/modules/executor/SPECS.md
- vendor/github.com/grafana/blockpack/internal/modules/executor/TESTS.md
**TESTS_ADDED:** 6 scenario entries in TESTS.md; integration verified via /tmp/check_regex2.go
**READY_FOR_REVIEW:** true
