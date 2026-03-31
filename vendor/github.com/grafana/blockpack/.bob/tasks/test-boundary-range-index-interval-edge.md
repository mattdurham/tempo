# Test: Range index interval edge case

**Status:** Pending
**task_type:** test-coverage
**cleanup_type:** test-coverage
**severity:** MEDIUM
**source:** coverage-gaps

## Gap

Interval matching logic handles [minBound, maxBound] inclusive range and open-ended ranges via sentinels, but no test exercises minBound=maxBound (point query via interval) or minBound>maxBound (invalid interval).

## Risk

Silent data loss if interval logic has off-by-one in boundary check.

## Location

File: `internal/modules/blockio/reader/reader.go:266-309`
Function: `BlocksForRangeInterval`

## Test Design

Query range index with interval [100, 100] (point query). Verify correct blocks returned. Query with [100, 99] (inverted); verify error or empty result.

## Notes

Test multiple boundary cases: point query [X, X], inverted [max, min], single-element range.
