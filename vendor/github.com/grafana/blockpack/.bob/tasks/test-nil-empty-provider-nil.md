# Test: Nil ReaderProvider

**Status:** Pending
**task_type:** test-coverage
**cleanup_type:** test-coverage
**severity:** MEDIUM
**source:** coverage-gaps

## Gap

NewReaderFromProvider expects non-nil provider but doesn't validate input nil explicitly. Nil dereference could occur on first access.

## Risk

Nil pointer dereference causing panic.

## Location

File: `internal/modules/blockio/reader/reader.go:115-123`
Function: `NewReaderFromProvider`

## Test Design

Call NewReaderFromProvider(nil, ...). Verify error is returned or panic with clear message.

## Notes

Simple boundary test. Check error message is informative (doesn't just say "nil pointer dereference").
