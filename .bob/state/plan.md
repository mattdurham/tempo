# Implementation Plan: Fix blockpack Integration and Compilation Error

## Overview

The `tempodb/encoding/vblockpack/roundtrip_test.go` file has a compile error at line 88 where
`block.Iterator()` is called on a `common.BackendBlock`, which does not have that method.
`Iterator()` is defined only on `common.WALBlock`. The fix replaces that call with
`block.FindTraceByID()` to properly verify the round-trip write-then-read. Additionally,
`github.com/mattdurham/blockpack` is incorrectly marked `// indirect` in `go.mod` even though
it is directly imported by Tempo source code, so that marker will be removed.

## Files to Modify

1. `/home/mdurham/source/tempo-mrd/tempodb/encoding/vblockpack/roundtrip_test.go`
   - Remove the `block.Iterator()` block (lines 88-110)
   - Replace with `block.FindTraceByID()` to verify the trace was written and is readable
   - Remove the now-unused `"io"` import

2. `/home/mdurham/source/tempo-mrd/go.mod`
   - Line 272: Remove the `// indirect` comment from the `github.com/mattdurham/blockpack` entry
   - Move the entry from the indirect `require` block (line 129+) to the direct `require` block
     (lines 74-127) in alphabetical order

## Files NOT to Modify

- `vendor/modules.txt` - Already has `## explicit; go 1.25.1` which is the correct marker for a
  direct dependency. No change needed.
- `vendor/github.com/mattdurham/blockpack/` - Vendor code is current and correct.
- `tempodb/encoding/vblockpack/backend_block.go` - Correct; `BackendBlock` should not have
  `Iterator()`.
- `tempodb/encoding/vblockpack/wal_block.go` - Correct; `WALBlock.Iterator()` is already there.
- `go.sum` - No version change; no update needed.

## Implementation Steps

### Phase 1: Fix the Compile Error in roundtrip_test.go

**Step 1.1: Read the current file (already done in planning)**

Current state of `roundtrip_test.go`:
- Line 5: `"io"` is imported (used only by the `blockIter.Next()` call that is being removed)
- Lines 88-110: The problematic `block.Iterator()` block

**Step 1.2: Rewrite roundtrip_test.go**

File: `/home/mdurham/source/tempo-mrd/tempodb/encoding/vblockpack/roundtrip_test.go`

Remove `"io"` from the import block (it will no longer be used after removing the iterator code).

Replace lines 87-112 (the Iterator block through end of test):

**Current code (lines 87-113):**
```go
	// Try to iterate through the block to verify traces
	blockIter, err := block.Iterator()
	if err == nil {
		defer blockIter.Close()

		foundTraces := 0
		for {
			id, tr, err := blockIter.Next(ctx)
			if err == io.EOF {
				break
			}
			if err != nil {
				t.Logf("Iterator error (may be expected if not fully implemented): %v", err)
				break
			}
			if tr != nil {
				foundTraces++
				t.Logf("Found trace: %x", id)
			}
		}
		t.Logf("Successfully iterated through block, found %d traces", foundTraces)
	} else {
		t.Logf("Iterator not yet implemented: %v", err)
	}

	t.Log("Roundtrip test completed!")
}
```

**Replacement code:**
```go
	// Verify the trace can be read back using FindTraceByID
	t.Log("Verifying trace can be retrieved by ID...")
	response, err := block.FindTraceByID(ctx, traceID, common.SearchOptions{})
	require.NoError(t, err)
	require.NotNil(t, response, "expected to find trace by ID after roundtrip")
	require.NotNil(t, response.Trace, "expected trace in response after roundtrip")

	// Verify the trace has the expected structure
	require.Greater(t, len(response.Trace.ResourceSpans), 0, "expected at least one ResourceSpan")
	require.Greater(t, len(response.Trace.ResourceSpans[0].ScopeSpans), 0, "expected at least one ScopeSpan")
	require.Greater(t, len(response.Trace.ResourceSpans[0].ScopeSpans[0].Spans), 0, "expected at least one Span")

	foundSpan := response.Trace.ResourceSpans[0].ScopeSpans[0].Spans[0]
	require.Equal(t, "test-span", foundSpan.Name, "expected span name to match after roundtrip")

	t.Logf("Roundtrip verified: found trace with span name '%s'", foundSpan.Name)
	t.Log("Roundtrip test completed!")
}
```

The full updated file should look like the following. Note the removal of `"io"` from imports:

```go
package vblockpack

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/grafana/tempo/pkg/tempopb"
	tempotrace "github.com/grafana/tempo/pkg/tempopb/trace/v1"
	"github.com/grafana/tempo/tempodb/backend"
	"github.com/grafana/tempo/tempodb/backend/local"
	"github.com/grafana/tempo/tempodb/encoding/common"
	"github.com/stretchr/testify/require"
)

func TestRoundTrip_WriteAndReadBlock(t *testing.T) {
	t.Log("Testing write then read roundtrip")

	// Setup
	ctx := context.Background()
	cfg := &common.BlockConfig{
		RowGroupSizeBytes: 100 * 1024 * 1024,
	}

	// Create test trace with known data
	traceID := []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}
	trace := &tempopb.Trace{
		ResourceSpans: []*tempotrace.ResourceSpans{
			{
				ScopeSpans: []*tempotrace.ScopeSpans{
					{
						Spans: []*tempotrace.Span{
							{
								TraceId:           traceID,
								SpanId:            []byte{1, 0, 0, 0, 0, 0, 0, 1},
								Name:              "test-span",
								StartTimeUnixNano: uint64(time.Now().UnixNano()),
								EndTimeUnixNano:   uint64(time.Now().Add(time.Millisecond * 100).UnixNano()),
							},
						},
					},
				},
			},
		},
	}

	iter := &mockIterator{
		traces: []*tempopb.Trace{trace},
		ids:    [][]byte{traceID},
	}

	// Create temporary backend
	tempDir := t.TempDir()
	rawR, rawW, _, err := local.New(&local.Config{
		Path: tempDir,
	})
	require.NoError(t, err)

	r := backend.NewReader(rawR)
	w := backend.NewWriter(rawW)

	meta := backend.NewBlockMeta("test-tenant", uuid.New(), VersionString)

	t.Log("Writing block...")

	// Write the block
	resultMeta, err := CreateBlock(ctx, cfg, meta, iter, r, w)
	require.NoError(t, err)
	require.NotNil(t, resultMeta)
	require.Equal(t, int64(1), resultMeta.TotalObjects)
	require.Greater(t, resultMeta.Size_, uint64(0))

	t.Logf("Block written: %d traces, %d bytes", resultMeta.TotalObjects, resultMeta.Size_)

	// Now read the block back
	t.Log("Reading block back...")

	enc := Encoding{}
	block, err := enc.OpenBlock(resultMeta, r)
	require.NoError(t, err)
	require.NotNil(t, block)

	t.Log("Block opened successfully!")

	// Verify the trace can be read back using FindTraceByID
	t.Log("Verifying trace can be retrieved by ID...")
	response, err := block.FindTraceByID(ctx, traceID, common.SearchOptions{})
	require.NoError(t, err)
	require.NotNil(t, response, "expected to find trace by ID after roundtrip")
	require.NotNil(t, response.Trace, "expected trace in response after roundtrip")

	// Verify the trace has the expected structure
	require.Greater(t, len(response.Trace.ResourceSpans), 0, "expected at least one ResourceSpan")
	require.Greater(t, len(response.Trace.ResourceSpans[0].ScopeSpans), 0, "expected at least one ScopeSpan")
	require.Greater(t, len(response.Trace.ResourceSpans[0].ScopeSpans[0].Spans), 0, "expected at least one Span")

	foundSpan := response.Trace.ResourceSpans[0].ScopeSpans[0].Spans[0]
	require.Equal(t, "test-span", foundSpan.Name, "expected span name to match after roundtrip")

	t.Logf("Roundtrip verified: found trace with span name '%s'", foundSpan.Name)
	t.Log("Roundtrip test completed!")
}
```

**Step 1.3: Check for mockIterator definition**

The `roundtrip_test.go` references a `mockIterator` struct (lines 49-52) but the struct definition
is not visible in the file. Before writing the fix, confirm whether `mockIterator` is defined in
another file in the same package or needs to be added.

Run: `grep -r "mockIterator" /home/mdurham/source/tempo-mrd/tempodb/encoding/vblockpack/`

If `mockIterator` is not found in another file, a definition must be added to `roundtrip_test.go`.
The struct needs:
- `traces []*tempopb.Trace`
- `ids [][]byte`
- `index int`
- `Next(ctx) (common.ID, *tempopb.Trace, error)` - returns entries by index
- `Close()` - no-op

### Phase 2: Fix go.mod Direct Dependency Marker

**Step 2.1: Move blockpack from indirect to direct require block**

File: `/home/mdurham/source/tempo-mrd/go.mod`

**Current state (line 272 in the second `require` block starting at line 129):**
```
	github.com/mattdurham/blockpack v0.0.0-20260219155254-414f47287d07 // indirect
```

**Action:** Remove this line from the indirect block AND add the entry without `// indirect`
to the first direct `require` block (lines 74-127), placing it alphabetically between
`github.com/mark3labs/mcp-go` and `github.com/minio/minio-go/v7`.

The direct require block currently has entries like:
```
	github.com/mark3labs/mcp-go v0.43.2
	...
	github.com/minio/minio-go/v7 v7.0.98
```

Insert after `github.com/mark3labs/mcp-go`:
```
	github.com/mattdurham/blockpack v0.0.0-20260219155254-414f47287d07
```

**Note on go mod tidy risk:** Simply removing `// indirect` and moving to the direct block is
a safe, minimal change. Running `go mod tidy` afterward would achieve the same effect but could
also reorder or update other dependencies unexpectedly. The manual edit is preferred here.
If `go mod tidy` is run as part of verification, review its diff carefully before committing.

### Phase 3: Verification Steps

**Step 3.1: Check mockIterator exists**

```
grep -r "mockIterator" /home/mdurham/source/tempo-mrd/tempodb/encoding/vblockpack/
```

If not found, add a `mockIterator` definition to `roundtrip_test.go` (see Step 1.3 above).

**Step 3.2: Compile check**

```
cd /home/mdurham/source/tempo-mrd && go build ./tempodb/encoding/vblockpack/...
```

Expected: No errors. The `block.Iterator undefined` error should be gone.

**Step 3.3: Run the roundtrip test**

```
cd /home/mdurham/source/tempo-mrd && go test -v -run TestRoundTrip_WriteAndReadBlock ./tempodb/encoding/vblockpack/
```

Expected: `PASS`

**Step 3.4: Run all vblockpack tests**

```
cd /home/mdurham/source/tempo-mrd && go test -v ./tempodb/encoding/vblockpack/...
```

Expected: All tests pass.

**Step 3.5: Run with race detector**

```
cd /home/mdurham/source/tempo-mrd && go test -race ./tempodb/encoding/vblockpack/...
```

Expected: No race conditions detected.

**Step 3.6: Verify go.mod consistency (optional)**

```
cd /home/mdurham/source/tempo-mrd && go mod verify
```

Expected: All modules verified. This confirms the vendor directory matches go.mod without
running `go mod tidy` (which would require network access).

**Step 3.7: Broader build check**

```
cd /home/mdurham/source/tempo-mrd && go build ./tempodb/...
```

Expected: No errors across the entire tempodb package tree.

## Edge Cases to Handle

### Edge Case 1: mockIterator Missing
**Scenario:** The `mockIterator` type used at lines 49-52 of `roundtrip_test.go` is not defined
in the package. The `testIterator` type (defined in `integration_test.go`) has a different
structure.
**Expected:** Add a `mockIterator` definition to `roundtrip_test.go` if it does not exist
elsewhere in the package.
**Detection:** Run `grep -r "type mockIterator" /home/mdurham/source/tempo-mrd/tempodb/encoding/vblockpack/`
before writing the file.

### Edge Case 2: Unused Import After Removing io.EOF
**Scenario:** The `"io"` import in `roundtrip_test.go` is only used for `io.EOF` inside the
iterator block that is being removed.
**Expected:** Remove `"io"` from the import block to prevent a "imported and not used" compile
error.
**Handled:** The replacement file shown in Step 1.2 already omits `"io"`.

### Edge Case 3: go.mod Alphabetical Ordering
**Scenario:** Inserting `github.com/mattdurham/blockpack` into the direct require block must
maintain alphabetical order to pass `go mod tidy` checks.
**Expected:** Insert between `github.com/mark3labs/mcp-go` and `github.com/minio/minio-go/v7`.
**Verification:** `go mod tidy` (if run) will reorder if misplaced; the manual edit should
get it right.

### Edge Case 4: vendor/modules.txt Already Has `explicit` Marker
**Scenario:** `vendor/modules.txt` line 1076 already reads `## explicit; go 1.25.1` which is
correct for a direct dependency.
**Expected:** No change to `vendor/modules.txt` is needed.
**Verification:** The `explicit` keyword in modules.txt is what controls vendoring behavior,
not the `// indirect` comment in go.mod.

## Risks/Concerns

### Risk 1: mockIterator Not Defined
**Risk:** `roundtrip_test.go` references `mockIterator` which may not be defined anywhere in
the `vblockpack` package (it is not in `integration_test.go` which uses `testIterator`).
**Impact:** Additional compile error unrelated to Iterator fix.
**Mitigation:** Grep for the type before writing. If missing, define it in `roundtrip_test.go`
alongside the test. The definition is straightforward (index-based iterator over a slice).

### Risk 2: FindTraceByID Behavior for traceID Format
**Risk:** The `traceID` in `roundtrip_test.go` is a raw `[]byte` (not hex-encoded), while
`blockpack.FindTraceByID` in `backend_block.go` converts to hex string internally. The
`common.ID` type wraps `[]byte`. Ensure the lookup uses the correct form.
**Impact:** Test could fail with "trace not found" if ID encoding mismatches.
**Mitigation:** The existing `TestBackendBlockFindTraceByID` in `integration_test.go` (line 250)
uses the same pattern (`common.ID(traceIDBytes)`) successfully. The pattern is proven.

### Risk 3: go mod tidy Reorders go.mod
**Risk:** Running `go mod tidy` to clean up after removing `// indirect` may reorder entries
or update unrelated dependency versions if the network is available.
**Impact:** Unintended changes to go.mod and go.sum.
**Mitigation:** Use manual edit for go.mod only. Do NOT run `go mod tidy` as part of this
change. Run `go mod verify` instead to confirm vendor integrity without network changes.

## Dependencies

### Internal Dependencies Used in the Fix
- `github.com/grafana/tempo/tempodb/encoding/common` - `common.SearchOptions`, `common.BackendBlock.FindTraceByID`
- `github.com/stretchr/testify/require` - already imported in the file

### External Dependencies
No new external dependencies. The fix removes a call that was broken and replaces it with an
existing method call that is already tested and working.

## Test Coverage Goals

- `TestRoundTrip_WriteAndReadBlock`: After fix, this test will verify the full write-then-read
  roundtrip by asserting that `FindTraceByID` returns the written trace with correct span data.
- Coverage improves vs. the current state (test previously compiled but the iterator block was
  entirely guarded by `if err == nil` which would have been skipped).

## Success Criteria

- [ ] `go build ./tempodb/encoding/vblockpack/...` compiles with no errors
- [ ] `block.Iterator undefined` error is gone
- [ ] `go test -v -run TestRoundTrip_WriteAndReadBlock ./tempodb/encoding/vblockpack/` passes
- [ ] `go test ./tempodb/encoding/vblockpack/...` all tests pass
- [ ] `go test -race ./tempodb/encoding/vblockpack/...` no race conditions
- [ ] `go.mod` no longer has `// indirect` on the blockpack entry
- [ ] `go mod verify` succeeds confirming vendor integrity
- [ ] No unused imports in the modified file (`"io"` removed)
- [ ] No changes to vendor directory contents required

## Notes

- The brainstorm confirmed there are NO blockpack API compatibility issues. The vendor directory
  at `v0.0.0-20260219155254-414f47287d07` is consistent with all usages in `backend_block.go`,
  `create.go`, and `wal_block.go`.
- The `vendor/modules.txt` note `## explicit; go 1.25.1` (note: go version in modules.txt is
  `1.25.1` not `1.24.2` as the brainstorm mentioned - the brainstorm had a minor discrepancy).
  This is already correct for a direct dependency. No changes needed there.
- The `blockpackIterator` stub in `wal_block.go` is unrelated to this fix. It is used for WAL
  iteration during flush and is separate from the backend block scenario.

## Questions/Uncertainties

- **mockIterator definition**: Must verify before writing whether `mockIterator` is defined
  somewhere in the package (e.g., a `helpers_test.go` or similar). If it is missing, it must
  be added or the test will have a second compile error.
- **Latest blockpack version**: The brainstorm noted the current pseudo-version
  `v0.0.0-20260219155254-414f47287d07` appears to be the latest available (no tagged releases).
  No version upgrade is planned as part of this fix since the vendor is already up to date.
