# Code Quality Review - workflow-reviewer

## Review Scope
Files reviewed:
- tempodb/encoding/vblockpack/roundtrip_test.go
- go.mod (dependency classification change)

---

### Issue 1: mockIterator type is duplicated across test files
**Severity:** MEDIUM
**Category:** code
**Files:** tempodb/encoding/vblockpack/roundtrip_test.go:48, tempodb/encoding/vblockpack/create_test.go:18

**WHAT:** `roundtrip_test.go` instantiates a `mockIterator` struct (line 48-51) which is defined in `create_test.go` (line 18-35). The `roundtrip_test.go` uses this type without defining it, relying on the same-package test sharing. This is not a bug but it creates tight coupling between test files. If `create_test.go` is removed or the struct is renamed, `roundtrip_test.go` silently breaks.

**WHY:** Test isolation is poor. `TestRoundTrip_WriteAndReadBlock` in `roundtrip_test.go` depends on `mockIterator` defined in a completely different test file (`create_test.go`). There is also a parallel `testIterator` defined in `integration_test.go` doing essentially the same thing. This cross-file dependency makes tests harder to understand and maintain.

**WHERE:** roundtrip_test.go:48 (usage), create_test.go:18 (definition)

---

### Issue 2: Test only verifies first span in the first ResourceSpan/ScopeSpan
**Severity:** LOW
**Category:** code
**Files:** tempodb/encoding/vblockpack/roundtrip_test.go:98-99

**WHAT:** The test writes one trace with one span, then only reads back `ResourceSpans[0].ScopeSpans[0].Spans[0]` and checks `foundSpan.Name`. It does not verify the `TraceId` on the returned span, the `SpanId`, or any timing fields.

**WHY:** The roundtrip test is intended to prove data fidelity end-to-end. By only checking the span name, it misses potential corruption in TraceId, SpanId, or timestamps during the blockpack encode/decode cycle. A more complete assertion would also check `foundSpan.TraceId` and `foundSpan.SpanId`.

**WHERE:** roundtrip_test.go:98-101

---

### Issue 3: go.mod direct/indirect dependency reclassification is correct
**Severity:** LOW
**Category:** code
**Files:** go.mod:89

**WHAT:** `github.com/mattdurham/blockpack` moved from indirect to direct. This is correct given the package is directly imported in `tempodb/encoding/vblockpack/*.go`.

**WHY:** The change is appropriate and has no functional impact. The version `v0.0.0-20260219155254-414f47287d07` is unchanged. However, noting it is a pseudo-version pointing to a specific commit on a fork/personal repo — there is no semantic versioning guaranteeing API stability.

**WHERE:** go.mod:89

---

### Issue 4: Test logs use t.Log for progress but no cleanup logging
**Severity:** LOW
**Category:** code
**Files:** tempodb/encoding/vblockpack/roundtrip_test.go:18, 65, 77, 84, 87, 101, 102

**WHAT:** The test uses several `t.Log` and `t.Logf` calls which is appropriate for test observability. However, `t.TempDir()` already handles cleanup automatically — the test doesn't need explicit cleanup. No issues here; this is informational.

**WHY:** The logging is acceptable for a new test file but is somewhat verbose compared to the existing test files in the same package (integration_test.go and create_test.go also use t.Fatalf style without as many log statements).

**WHERE:** roundtrip_test.go:18-102 (various)

---

## Summary

| Severity | Count |
|----------|-------|
| CRITICAL | 0     |
| HIGH     | 0     |
| MEDIUM   | 1     |
| LOW      | 3     |

Total issues found: 4
