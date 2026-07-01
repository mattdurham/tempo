# Area Complexity: Utility Packages

**Status:** Reference
**Created:** 2026-02-15
**Complexity Tier:** 4 — LOW

## Locations

| Package | Files | Lines | Description |
|---------|-------|-------|-------------|
| `internal/xunsafe` | 8 | ~564 | Unsafe Go operations for performance |
| `internal/inspect` | 5 | ~860 | File inspection utilities |
| `internal/quantile` | 1 | 361 | KLL quantile sketch |
| `internal/benchsummary` | 1 | 339 | Benchmark result parsing |
| `internal/xsync` | 4 | ~223 | Concurrent data structures |
| `internal/debug` | 2 | 138 | Debug toggles |
| `internal/testutil` | 1 | 108 | Test helpers |
| `internal/xflag` | 1 | 54 | Feature flags |

**Combined:** ~2,650 lines

## Description

Small, focused utility packages supporting the core modules. Generally well-contained with minimal dependencies.

## Key Risks

- `xunsafe` and `quantile` are correctness-critical despite small size
- `xunsafe` uses unsafe pointers — same risks as arena package

## Refactoring Opportunities

- Low priority — small and well-scoped
