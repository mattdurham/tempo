# Blockpack Dependency Update Verification Results

## Executive Summary

**Overall Status:** FAILED

**Critical Issue:** Memory alignment error detected in blockpack library when running with race detector. The test `TestBackendBlockFindTraceByID` fails with:
```
fatal error: checkptr: converted pointer straddles multiple allocations
```

This is a **critical bug in the upstream blockpack library** at commit `18e794dfb242bc1a548e907d7295244365c30ef2`.

## Phase 1: Fetch Latest Blockpack Code

**Status:** SUCCESS

**Blockpack Repository:** `/home/matt/source/blockpack`

**Blockpack Commit:** `18e794dfb242bc1a548e907d7295244365c30ef2`

**Commit Message:** "feat: Add range column support for Float64, Bytes, and String (#82)"

**Details:**
- Repository was already up to date with origin/main
- Clean pull completed successfully
- Working directory is clean

## Phase 2: Verify Compilation

**Status:** SUCCESS

**Module Verification:** `all modules verified`

**Go Version:** Downloaded go1.25.6 (linux/amd64)

**Build Result:** SUCCESS

**Details:**
- Module cache cleaned successfully
- All dependencies downloaded and verified
- Full codebase compilation successful with `go build ./...`
- No compilation errors or warnings

## Phase 3: Run Unit Tests

**Status:** FAILED

### vblockpack Tests with Race Detector

**Command:** `go test -v -race ./tempodb/encoding/vblockpack/...`

**Result:** FAILED

**Test Results Summary:**
- PASS: TestNewCompactor
- SKIP: TestCompactorCompact (requires complete blockpack implementation)
- PASS: TestTraceIDsEqual (all subtests)
- PASS: TestCountSpansInTrace (all subtests)
- PASS: TestCompareIDs (all subtests)
- PASS: TestIsTraceConnected (all subtests)
- PASS: TestMergeTraces (all subtests)
- PASS: TestWALBlockBasicOperations
- PASS: TestCreateBlockBasicOperations
- **FAIL: TestBackendBlockFindTraceByID** (CRITICAL)

**Critical Error:**
```
fatal error: checkptr: converted pointer straddles multiple allocations

goroutine 59 gp=0xc0004a0c40 m=15 mp=0xc000581808 [running]:
runtime.throw({0x219e45c?, 0x24da440?})
	/home/matt/go/pkg/mod/golang.org/toolchain@v0.0.1-go1.25.6.linux-amd64/src/runtime/panic.go:1094 +0x48
runtime.checkptrAlignment(0xc0004faaa0?, 0x0?, 0x1cab701?)
	/home/matt/go/pkg/mod/golang.org/toolchain@v0.0.1-go1.25.6.linux-amd64/src/runtime/checkptr.go:26 +0x5b
github.com/mattdurham/blockpack/internal/xunsafe.Cast[...](...)
	/home/matt/source/tempo-mrd-worktrees/blockpack-integration/vendor/github.com/mattdurham/blockpack/internal/xunsafe/pointer.go:25
github.com/mattdurham/blockpack/internal/arena/slice.Make[...](0xc0004faaa0, 0x0)
	/home/matt/source/tempo-mrd-worktrees/blockpack-integration/vendor/github.com/mattdurham/blockpack/internal/arena/slice/slice.go:58
```

**Root Cause Analysis:**

The error originates from blockpack's internal unsafe pointer casting code:
- Location: `blockpack/internal/xunsafe/pointer.go:25`
- Function: `xunsafe.Cast`
- Called by: `arena/slice.Make` at line 58
- Called by: `executor.selectBlocksWithPlan` at line 2558
- Triggered during: `ExecuteTraceQL` query execution

The race detector's checkptr verification is catching an unsafe pointer conversion that violates Go's pointer alignment rules. This indicates the pointer is pointing across memory allocation boundaries, which is undefined behavior.

**Race Conditions:** YES - Critical memory safety issue detected

**Coverage:** Not measured due to test failure

**Failed Tests:**
1. TestBackendBlockFindTraceByID - Memory alignment violation in blockpack

### Config Tests

**Command:** `go test -v ./tempodb/encoding/common/... -run Blockpack`

**Result:** SUCCESS

**Test Results:**
- PASS: TestBlockpackConfigDefaults
- PASS: TestBlockpackConfigValidation (all subtests)
- PASS: TestBlockConfigWithBlockpack
- PASS: TestBlockConfigBlockpackValidationFailure

**Duration:** 0.007s

**Details:** All blockpack configuration validation tests pass successfully. Configuration parsing and validation logic is working correctly.

### Broader tempodb Tests

**Status:** NOT RUN (due to critical failure in Phase 3.1)

## Phase 4: Run Integration Tests

**Status:** NOT RUN (blocked by Phase 3 failure)

**Reason:** Critical memory safety issue must be resolved before proceeding with integration tests.

## Phase 5: Verify Query Functionality

**Status:** NOT RUN (blocked by Phase 3 failure)

**Reason:** The ExecuteTraceQL function is the source of the memory alignment error.

## Phase 6: Static Analysis

**Status:** PARTIAL

### go vet

**Command:** `go vet ./tempodb/encoding/vblockpack/...`

**Result:** SUCCESS

**Details:** No issues found by go vet. The issue is only detectable with race detector enabled.

### dataType Parameter Verification

**Command:** `grep -n "dataType" .../backend_block.go`

**Result:** SUCCESS

**Locations Found:**
- Line 39: `func (s *tempoStorage) ReadAt(path string, p []byte, off int64, dataType blockpack.DataType) (int, error)`
- Line 41: Comment explaining dataType is for future caching optimization
- Line 598: `func (p *bytesReaderProvider) ReadAt(b []byte, off int64, dataType blockpack.DataType) (int, error)`
- Line 599: Comment explaining dataType is ignored for in-memory data

**Details:** DataType parameter is properly implemented in both storage adapters with appropriate documentation.

## Phase 7: Performance Verification

**Status:** NOT RUN (blocked by Phase 3 failure)

## Phase 8: Documentation Review

**Status:** NOT RUN (blocked by Phase 3 failure)

## Overall Summary

**Overall Status:** FAILED

### Success Criteria Met

- [x] Blockpack updated to latest main (commit 18e794dfb2)
- [x] Compilation successful
- [ ] **All unit tests pass** - FAILED
- [ ] **No race conditions** - CRITICAL RACE/ALIGNMENT ISSUE
- [ ] Integration tests pass - NOT RUN
- [ ] Query functionality verified - NOT RUN
- [x] Static analysis clean (go vet passes)
- [x] DataType parameter properly documented

### Issues Found

**CRITICAL:**

1. **Memory Alignment Violation in Blockpack (BLOCKER)**
   - **Severity:** CRITICAL
   - **Location:** `blockpack/internal/xunsafe/pointer.go:25` (upstream library)
   - **Test:** TestBackendBlockFindTraceByID
   - **Error:** `fatal error: checkptr: converted pointer straddles multiple allocations`
   - **Impact:** Undefined behavior, potential data corruption, crashes in production
   - **Detection:** Only with race detector enabled (`-race` flag)
   - **Affected Code Path:** ExecuteTraceQL → executeStreamingQuery → selectBlocksWithPlan → arena/slice.Make → xunsafe.Cast
   - **Root Cause:** Unsafe pointer casting violates Go memory alignment rules
   - **Source:** Upstream blockpack library commit 18e794dfb2 (latest main)

**HIGH:**

2. **Test Suite Incomplete**
   - TestCompactorCompact is skipped (requires complete blockpack implementation)
   - Integration tests not run due to blocking issue
   - Query functionality tests not run due to blocking issue

### Recommendation

**CANNOT PROCEED - UPSTREAM BUG BLOCKING**

**Action Required:**

1. **Report bug to blockpack maintainer** - This is a critical memory safety issue in the blockpack library
2. **Document issue details:**
   - Commit: 18e794dfb242bc1a548e907d7295244365c30ef2
   - File: `internal/xunsafe/pointer.go:25`
   - Function: `xunsafe.Cast`
   - Trigger: Any TraceQL query execution with race detector enabled
   - Error: Pointer alignment violation detected by Go's checkptr
3. **Possible workarounds:**
   - Revert blockpack to previous commit before PR #82 ("Add range column support")
   - Wait for blockpack fix
   - Disable race detector (NOT RECOMMENDED - masks real memory safety issues)
4. **Investigate further:**
   - Check if the issue exists in PR #82 specifically (range column support)
   - Test with blockpack commit before PR #82 to confirm regression

**Risk Assessment:**

- **Production Risk:** HIGH - Memory alignment violations can cause crashes, data corruption, or silent failures
- **Race Detector Dependency:** The issue is only detected with `-race` flag, but the underlying problem exists regardless
- **Upstream Dependency:** This is a blockpack bug, not a Tempo integration issue
- **DataType Parameter:** The DataType parameter itself is correctly implemented; the bug is unrelated

**Next Steps:**

1. Identify the last known-good blockpack commit
2. Test with that commit to verify Tempo integration works
3. Report the bug upstream with full reproduction steps
4. Wait for blockpack fix or implement workaround
5. Re-run verification workflow after blockpack is fixed

## Technical Details

### Error Stack Trace Context

The error occurs in blockpack's arena allocator when creating slices with custom memory layout:

```
xunsafe.Cast (pointer.go:25)
  ↓
arena/slice.Make (slice.go:58)
  ↓
executor.selectBlocksWithPlan (blockpack_executor.go:2558)
  ↓
executor.executeStreamingQuery (blockpack_executor.go:677)
  ↓
executor.ExecuteQuery (blockpack_executor.go:609)
  ↓
blockpack.ExecuteTraceQL (api.go:111)
  ↓
Tempo's backend_block.go integration
```

### Unsafe Pointer Issue

The `xunsafe.Cast` function is performing unsafe pointer conversions that violate Go's pointer alignment guarantees. The Go runtime's checkptr verification (enabled with `-race`) detects that a converted pointer crosses allocation boundaries, which is undefined behavior per the Go memory model.

### Affected Blockpack Commit

**Commit:** 18e794dfb242bc1a548e907d7295244365c30ef2
**PR:** #82 - "feat: Add range column support for Float64, Bytes, and String"
**Date:** Latest main branch (as of 2026-02-16)

This may be a regression introduced in PR #82. Testing with the commit before this PR would help isolate the issue.

## Conclusion

The Tempo codebase integration with blockpack is **correctly implemented**. The DataType parameter is properly added to both storage adapters with appropriate documentation. Configuration validation works correctly. Compilation succeeds without errors.

However, the latest blockpack library (commit 18e794dfb2) contains a **critical memory safety bug** in its unsafe pointer handling code that causes test failures when race detection is enabled. This is a **blocking issue** that must be resolved in the upstream blockpack library before this integration can be considered production-ready.

**Status:** BLOCKED on upstream blockpack bug fix.
