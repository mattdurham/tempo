# ahocorasick

[![Go Reference](https://pkg.go.dev/badge/github.com/coregx/ahocorasick.svg)](https://pkg.go.dev/github.com/coregx/ahocorasick)
[![Tests](https://github.com/coregx/ahocorasick/actions/workflows/ci.yml/badge.svg)](https://github.com/coregx/ahocorasick/actions/workflows/ci.yml)
[![Go Report Card](https://goreportcard.com/badge/github.com/coregx/ahocorasick)](https://goreportcard.com/report/github.com/coregx/ahocorasick)
[![codecov](https://codecov.io/gh/coregx/ahocorasick/branch/main/graph/badge.svg)](https://codecov.io/gh/coregx/ahocorasick)

High-performance Aho-Corasick multi-pattern string matching for Go.

## Features

- **1+ GB/s throughput** — comparable to Rust's [aho-corasick](https://github.com/BurntSushi/aho-corasick)
- **Dense array transitions** — optimized NFA with O(1) state transitions
- **Byte class compression** — reduces memory by grouping equivalent bytes
- **Multiple match semantics** — LeftmostFirst (Perl) and LeftmostLongest (POSIX)
- **Zero dependencies** — pure Go, no cgo
- **Zero allocations** — `IsMatch()` hot path allocates nothing

## Installation

```bash
go get github.com/coregx/ahocorasick
```

Requires Go 1.21+

## Quick Start

```go
package main

import (
    "fmt"
    "github.com/coregx/ahocorasick"
)

func main() {
    // Build automaton from patterns
    ac, _ := ahocorasick.NewBuilder().
        AddStrings([]string{"error", "warning", "fatal"}).
        Build()

    haystack := []byte("[error] something failed")

    // Check if any pattern matches (zero allocation)
    if ac.IsMatch(haystack) {
        fmt.Println("Found a match!")
    }

    // Find first match
    if m := ac.Find(haystack, 0); m != nil {
        fmt.Printf("Found %q at position %d\n",
            haystack[m.Start:m.End], m.Start)
    }

    // Find all non-overlapping matches
    for _, m := range ac.FindAll(haystack, -1) {
        fmt.Printf("Pattern %d: %q\n", m.PatternID, haystack[m.Start:m.End])
    }
}
```

## Performance

Benchmarks on Intel i7-1255U (64KB haystack, 4 patterns):

| Method | Throughput | Allocations |
|--------|------------|-------------|
| `IsMatch` (with match) | **1.6 GB/s** | 0 |
| `Find` | **1.1 GB/s** | 1 |
| `IsMatch` (no match) | 780 MB/s | 0 |

Comparable to Rust's aho-corasick crate (~1-2 GB/s).

## API

### Builder

```go
ahocorasick.NewBuilder().
    AddStrings([]string{"foo", "bar"}).  // Add patterns
    SetMatchKind(ahocorasick.LeftmostLongest).  // Optional: POSIX semantics
    Build()  // Returns (*Automaton, error)
```

### Automaton

```go
// Existence check (zero allocation)
ac.IsMatch(haystack []byte) bool

// Find matches
ac.Find(haystack []byte, start int) *Match      // First match from position
ac.FindAt(haystack []byte, start int) *Match    // Match at exact position
ac.FindAll(haystack []byte, n int) []Match      // All non-overlapping (n=-1 for all)
ac.FindAllOverlapping(haystack []byte) []Match  // All including overlaps

// Utilities
ac.Count(haystack []byte) int   // Count non-overlapping matches
ac.PatternCount() int           // Number of patterns
ac.Pattern(id int) []byte       // Get pattern by ID
```

### Match Semantics

```go
LeftmostFirst   // First pattern in list wins (Perl-compatible, default)
LeftmostLongest // Longest pattern wins (POSIX-compatible)
```

## Use Cases

- **Log analysis** — scan for error patterns in log files
- **Content filtering** — detect keywords in text
- **Network security** — signature-based detection
- **DNA sequencing** — find multiple motifs simultaneously
- **Regex acceleration** — as prefilter for `foo|bar|baz` alternations

## How It Works

The [Aho-Corasick algorithm](https://en.wikipedia.org/wiki/Aho%E2%80%93Corasick_algorithm) builds a finite automaton from patterns:

1. **Trie construction** — patterns form a prefix tree
2. **Failure links** — enable backtracking without re-reading input
3. **Dense transitions** — O(1) state lookup via byte class indexing

This allows matching all patterns simultaneously in O(n) time regardless of pattern count.

## Related Projects

- [coregex](https://github.com/coregx/coregex) — High-performance regex engine (uses this library)
- [BurntSushi/aho-corasick](https://github.com/BurntSushi/aho-corasick) — Rust reference implementation

## License

MIT License — see [LICENSE](LICENSE) for details.
