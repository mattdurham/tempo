# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.1.0] - 2026-01-05

Initial release of the high-performance Aho-Corasick library for Go.

### Features

- **Builder pattern** for fluent automaton construction
  - `AddPattern`, `AddPatterns`, `AddStrings` for adding patterns
  - `SetMatchKind` for choosing match semantics
  - `SetByteClasses` for memory optimization control

- **Automaton API** for pattern matching
  - `Find` — first match from position
  - `FindAt` — match at exact position (anchored)
  - `FindAll` — all non-overlapping matches
  - `FindAllOverlapping` — all matches including overlaps
  - `IsMatch` — existence check (zero allocation)
  - `Count` — count non-overlapping matches

- **Match semantics**
  - `LeftmostFirst` — Perl-compatible (first pattern wins)
  - `LeftmostLongest` — POSIX-compatible (longest pattern wins)

- **Optimizations**
  - Dense array transitions for O(1) state lookup
  - Byte class compression (256 → N equivalence classes)
  - Precomputed root transitions (no failure link following for root)
  - Zero-allocation `IsMatch()` hot path

### Performance

Benchmarks on Intel i7-1255U (64KB haystack, 4 patterns):

| Method | Throughput | Allocations |
|--------|------------|-------------|
| `IsMatch` (with match) | 1.6 GB/s | 0 |
| `Find` | 1.1 GB/s | 1 |
| `IsMatch` (no match) | 780 MB/s | 0 |

### Testing

- 27 unit tests covering core functionality
- 2 fuzz tests verifying correctness against `bytes.Contains`/`bytes.Index`
- 93% code coverage
- CI on Linux, Windows, macOS with race detector

[Unreleased]: https://github.com/coregx/ahocorasick/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/coregx/ahocorasick/releases/tag/v0.1.0
