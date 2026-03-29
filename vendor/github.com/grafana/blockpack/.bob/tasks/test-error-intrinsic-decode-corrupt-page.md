# Test: Intrinsic column decode failure

**Status:** Pending
**task_type:** test-coverage
**cleanup_type:** test-coverage
**severity:** MEDIUM
**source:** coverage-gaps

## Gap

GetIntrinsicColumn decodes columns with possible errors (corrupt pages, bad indices), but no test corrupts a page bloom filter, truncates page TOC, or sets invalid ref indices. Error paths are untested.

## Risk

Panic or data corruption from malformed intrinsic blob.

## Location

File: `internal/modules/blockio/reader/intrinsic_reader.go:100-110`
Function: `GetIntrinsicColumn`

## Test Design

Write a dict intrinsic column, corrupt the page TOC length field or page bloom filter, and call GetIntrinsicColumn. Verify error is returned (not panic). Test multiple corruption points.

## Notes

Manually corrupt intrinsic blob bytes. Inspect binary format to find TOC location. Test both page-level and dict-level corruption.
