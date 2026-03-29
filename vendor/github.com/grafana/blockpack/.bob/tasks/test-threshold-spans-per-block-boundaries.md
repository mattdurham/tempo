# Test: Span count limits per block

**Status:** Pending
**task_type:** test-coverage
**cleanup_type:** test-coverage
**severity:** MEDIUM
**source:** coverage-gaps

## Gap

Blocks are split based on MaxSpansPerBlock, but no explicit test verifies span distribution at exact boundaries. No test for MaxSpansPerBlock=0 (unbounded), 1 (each span a block), or off-by-one at exact boundary.

## Risk

Span distribution logic might over/under-pack blocks at boundaries, causing uneven distribution or unused blocks.

## Location

File: `internal/modules/blockio/writer/writer.go`
Function: Block splitting logic

## Test Design

Write 100 spans with MaxSpansPerBlock set to 10, 33, 99, 100, and 101. Verify blocks split correctly — e.g., with MaxSpansPerBlock=10, expect ~10 blocks with ~10 spans each. Assert no under/over-packing.

## Notes

Set MaxSpansPerBlock via WriterOptions or similar before writing. Count resulting blocks and spans per block. Verify distribution is fair and respects the limit.
