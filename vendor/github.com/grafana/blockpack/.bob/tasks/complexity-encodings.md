# Area Complexity: Encodings

**Status:** Reference
**Created:** 2026-02-15
**Complexity Tier:** 3 — MODERATE

## Location

`internal/encodings/` — 8 source files, 6 test files, ~1,249 lines

## Description

Columnar encoding algorithms for efficient compression. Implements delta, dictionary, RLE, XOR, and prefix encoding strategies.

## Complexity Indicators

- Bit-level operations throughout
- Sparse array handling
- Compression integration (zstd)
- Algorithmic complexity — correctness-critical for data integrity

## Key Files

| File | Lines | Notes |
|------|-------|-------|
| `dictionary.go` | 459 | Dictionary encoding |
| `helpers.go` | 274 | Encoding utilities |

## Key Risks

- Encoding/decoding bugs silently corrupt data
- Bit-level operations are error-prone
- Performance characteristics vary by data distribution

## Refactoring Opportunities

1. Add property-based testing (encode → decode round-trip)
2. Consider single-pass RLE encoding (see remaining-high-priority-issues.md MEDIUM-1)
