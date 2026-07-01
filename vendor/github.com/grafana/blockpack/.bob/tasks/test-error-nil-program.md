# Test: Nil program parameter

**Status:** Pending
**task_type:** test-coverage
**cleanup_type:** test-coverage
**severity:** HIGH
**source:** coverage-gaps

## Gap

Nil check exists in stream.go lines 110-111 but no test calls Collect(r, nil, opts) to verify it's enforced.

## Risk

Check is ineffective if test never exercises it.

## Location

File: `internal/modules/executor/stream.go:110-111`
Function: `Collect`

## Test Design

Call Collect with program=nil. Verify error is returned with clear message "program must not be nil".

## Notes

Simple test but important for completeness. Verify error message is exact match.
