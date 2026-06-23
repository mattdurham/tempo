# Fix executor/TESTS.md: duplicate spec IDs and undocumented test

**Status:** Pending
**task_type:** cleanup
**cleanup_type:** spec-docs
**source:** discover-docs

## Location

`internal/modules/executor/TESTS.md`, lines 204, 215, 228, 547
`internal/modules/executor/executor_log_test.go`, `TestBuildPredicates_LogRegexCaseInsensitiveAlternation`

## Issue

### Problem 1: Duplicate EX-17

Line 204: `## EX-17: TestBuildPredicates_LogRegexPrefix`
Line 228: `## EX-17: TestBuildPredicates_LogRegexCommonPrefix`

Two scenarios share `EX-17`. The second was appended after EX-18 without getting a new ID.

### Problem 2: Duplicate EX-18

Line 215: `## EX-18: TestBuildPredicates_LogRegexCaseInsensitive`
Line 547: `## EX-18: TestExtractLiteralAlternatives`

Two scenarios also share `EX-18`. The sequence EX-14 through EX-18 was defined, then EX-17 and EX-18 were reused again for additional log-path tests appended later.

So the full collision list is:
- Second EX-17 (line 228) needs renaming to EX-19
- Second EX-18 (line 547) needs renaming to EX-20

### Problem 3: Undocumented test function

`internal/modules/executor/executor_log_test.go` contains `TestBuildPredicates_LogRegexCaseInsensitiveAlternation` (line 360) with no corresponding TESTS.md entry.

## Fix

This is a CLEANUP task. Do NOT add new functionality.

**Fix 1:** Renumber second EX-17 at line 228:
```
## EX-17: TestBuildPredicates_LogRegexCommonPrefix
```
→
```
## EX-19: TestBuildPredicates_LogRegexCommonPrefix
```

**Fix 2:** Renumber second EX-18 at line 547:
```
## EX-18: TestExtractLiteralAlternatives
```
→
```
## EX-20: TestExtractLiteralAlternatives
```

**Fix 3:** Add a new entry `EX-21: TestBuildPredicates_LogRegexCaseInsensitiveAlternation` to TESTS.md documenting the existing test. Read `executor_log_test.go` at line 360 to capture the scenario and assertions accurately before writing the entry.

## Acceptance criteria

- No two entries in TESTS.md share the same EX-* flat ID
- `TestBuildPredicates_LogRegexCommonPrefix` → EX-19
- `TestExtractLiteralAlternatives` → EX-20
- `TestBuildPredicates_LogRegexCaseInsensitiveAlternation` has a TESTS.md entry (EX-21)
- No code changes needed (doc fix only)
