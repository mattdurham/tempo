# Fix executor/NOTES.md: stale Execute/Stream references in Note 7 and NOTE-029

**Status:** Pending
**task_type:** cleanup
**cleanup_type:** spec-docs
**source:** discover-docs

## Location

`internal/modules/executor/NOTES.md`:
- Line 106 (Note 7: Integration Coverage)
- Line 110 (Note 7: Integration Coverage)
- Line 973 (NOTE-029 back-ref)

## Issue

Three more occurrences of the old `Execute` and `Stream` function names persist in NOTES.md after the Execute→Collect rename (NOTE-035, 2026-03-11):

**Line 106:**
```
`modules_reader.NewReaderFromProvider`) → this executor (`New().Execute`).
```
`Execute` no longer exists; the primary method is `Collect`.

**Line 110:**
```
- `Execute` empty-file short-circuit (EX-05)
```
Should reference `Collect`.

**Line 973 (NOTE-029 back-ref):**
```
**Back-ref:** `internal/modules/executor/stream.go:Stream`,
```
`Stream` no longer exists in stream.go; the function is `Collect`.

## Fix

Line 106: Change `New().Execute` → `New().Collect`

Line 110: Change `` `Execute` empty-file short-circuit (EX-05)`` → `` `Collect` empty-file short-circuit (EX-05)``

Line 973: Change `stream.go:Stream` → `stream.go:Collect`

## Acceptance criteria

- No remaining occurrences of `New().Execute`, `Execute empty-file short-circuit`, or `stream.go:Stream` in NOTES.md (outside of NOTE-035 which documents the rename itself and NOTE-012 which has an Addendum)
- No code changes needed (doc fix only)
