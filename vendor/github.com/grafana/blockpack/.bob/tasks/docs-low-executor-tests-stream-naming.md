# Fix executor/TESTS.md: EX-S-05 and EX-SLK series stale test function names

**Type:** cleanup
**Cleanup-type:** spec-docs
**Severity:** LOW
**Source:** discover-docs (round 2)

## Problem

### EX-S-05 (Setup description)
TESTS.md EX-S-05 setup says: "call `Stream(nil, program, StreamOptions{}, callback)`"
Actual `stream_test.go:164`: `executor.Collect(nil, program, executor.CollectOptions{})`

### EX-SLK series: 5 test function names do not match code
| TESTS.md entry | Documented name | Actual function |
|---|---|---|
| EX-SLK-01 | `TestStreamLogsTopK_BasicTopK` | `TestStreamLogsTopK_GlobalOrder_Backward` |
| EX-SLK-02 | `TestStreamLogsTopK_PipelineFiltering` | `TestStreamLogsTopK_PipelineFilters` |
| EX-SLK-05 | `TestStreamLogsTopK_UnlimitedCollectAll` | `TestStreamLogsTopK_LimitZeroDeliversAll` |
| EX-SLK-07 | `TestStreamLogsTopK_NilPipeline` | (no separate test; covered by `TestStreamLogsTopK_LimitZeroDeliversAll`) |
| EX-SLK-08 | `TestStreamLogsTopK_DirectionForward` | `TestStreamLogsTopK_GlobalOrder_Forward` |

## Fix

This is a CLEANUP task. Do NOT change any code.

In `internal/modules/executor/TESTS.md`:
1. Update EX-S-05 Setup to: "call `Collect(nil, program, CollectOptions{})`."
2. Update EX-SLK-01 name to `TestStreamLogsTopK_GlobalOrder_Backward`.
3. Update EX-SLK-02 name to `TestStreamLogsTopK_PipelineFilters`.
4. Update EX-SLK-05 name to `TestStreamLogsTopK_LimitZeroDeliversAll`.
5. Update EX-SLK-07 note to say: covered by `TestStreamLogsTopK_LimitZeroDeliversAll` (no separate test).
6. Update EX-SLK-08 name to `TestStreamLogsTopK_GlobalOrder_Forward`.

## Acceptance criteria
- EX-S-05 Setup says `Collect(nil, program, CollectOptions{})`.
- EX-SLK-01, 02, 05, 07, 08 names match actual test functions.
