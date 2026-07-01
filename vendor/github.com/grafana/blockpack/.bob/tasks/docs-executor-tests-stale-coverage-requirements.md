# Fix executor/TESTS.md: stale function names in Coverage Requirements

**Status:** Pending
**task_type:** cleanup
**cleanup_type:** spec-docs
**source:** discover-docs

## Location

`internal/modules/executor/TESTS.md`, line 113 (Coverage Requirements section)

## Issue

The Coverage Requirements section lists the old public API names:

```
- All public functions (`New`, `Execute`, `Stream`) must be exercised.
```

After the Execute→Collect rename (NOTE-035, 2026-03-11):
- `Execute` → `Collect`
- `Stream` → merged into `Collect` (no separate Stream function)
- `CollectTopK` was added (globally-sorted variant)

The actual test functions correctly exercise `Collect` and `CollectTopK`. Only the spec text is stale.

## Fix

Update line 113 to:

```
- All public functions (`New`, `Collect`, `CollectTopK`) must be exercised.
```

## Acceptance criteria

- Coverage Requirements correctly lists `Collect` and `CollectTopK` instead of `Execute` and `Stream`
- No code changes needed (doc fix only)
