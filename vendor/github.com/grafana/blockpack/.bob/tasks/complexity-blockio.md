# Area Complexity: BlockIO

**Status:** Reference
**Created:** 2026-02-15
**Complexity Tier:** 1 — VERY HIGH

## Location

`internal/blockio/` — 82 source files, 66 test files, ~15,623 lines

## Description

Low-level I/O for reading and writing blockpack format. Handles columnar encoding/decoding, compression, caching, metadata parsing, and format versioning. Highest total code volume in the project.

## Subpackages

- `reader/` — 17 files: parsing, decoding, decompression
- `writer/` — 26 files: column builders, bloom filters, metadata, serialization
- `shared/` — caching providers, tracking readers, bitset operations
- `format/` — format constants, version definitions
- `streams/` — streaming I/O abstractions

## Complexity Indicators

- **26 files use sync primitives** (Mutex, RWMutex, Pool) — heaviest concurrency in the project
- Complex encoding strategies: delta, dictionary, RLE, XOR, prefix
- State machine patterns for progressive I/O
- Multiple caching layers: LRU, sharded LRU, memcache providers

## Key Files

### Writer
| File | Size | Notes |
|------|------|-------|
| `writer_blocks.go` | ~1,024 lines | Serialization logic |
| `writer_aggregate.go` | ~21KB | Aggregate column writing |
| `writer_span.go` | ~13KB | Span column writing |

### Reader
| File | Size | Notes |
|------|------|-------|
| `column.go` | ~37KB | Complex column decoding — largest reader file |
| `parser.go` | ~24KB | Format parsing |
| `metadata.go` | ~13KB | Metadata handling |

## Key Risks

- Distributed complexity — no single mega-file but many interacting pieces
- Concurrency with 26 sync-primitive files increases race condition risk
- Encoding/decoding correctness is critical for data integrity
- Format versioning adds ongoing maintenance burden

## Refactoring Opportunities

1. Audit sync primitive usage for potential simplification
2. Consider consolidating encoding strategy selection logic
3. Improve separation between format parsing and data decoding in reader
