# vm — Test Specifications

This document defines the required tests for the `vm` package's regex optimization module.
Each test is described with its scenario, expected behavior, and edge cases.

---

## 1. Regex Prefix Extraction

### VM-T-01: TestAnalyzeRegex_LiteralPrefix

**Scenario:** Simple patterns with literal prefixes are optimized to prefix-based range lookups.

**Setup:** Table-driven cases:
- `"foo.*"` → `["foo"]`
- `"^error"` → `["error"]`
- `"^error.*$"` → `["error"]`
- `"debug"` → `["debug"]`
- `"^GET /api/.*"` → `["GET /api/"]`

**Assertions:** `AnalyzeRegex` returns non-nil `RegexAnalysis` with expected `Prefixes`.
`CaseInsensitive` is false.

---

### VM-T-02: TestAnalyzeRegex_CaseInsensitive

**Scenario:** Patterns with `(?i)` flag are detected as case-insensitive and prefixes
are lowercased.

**Setup:** Table-driven cases:
- `"(?i)debug"` → `["debug"]`, `CaseInsensitive: true`
- `"(?i)^error.*"` → `["error"]`, `CaseInsensitive: true`
- `"(?i)DEBUG"` → `["debug"]`, `CaseInsensitive: true`

**Assertions:** `CaseInsensitive` is true. Prefixes are lowercased.

---

### VM-T-03: TestAnalyzeRegex_Alternation

**Scenario:** Alternation patterns extract a prefix per branch.

**Setup:** Table-driven cases:
- `"error|warn|info"` → `["error", "warn", "info"]`
- `"^error|^warn"` → `["error", "warn"]`
- `"error.*|warn.*"` → `["error", "warn"]`
- `"f.o|b.r"` → `["f", "b"]` (dot stops collection but partial prefix is kept)
- `"foo(?:bar|baz)qux"` → `["fooba"]` (Simplify() factors common prefix "fooba" from "foobar|foobaz")

**Assertions:** All branches produce prefixes; alternation returns nil if any branch
is not optimizable.

---

### VM-T-04: TestAnalyzeRegex_NotOptimizable

**Scenario:** Patterns too complex for prefix extraction return nil.

**Setup:** Table-driven cases:
- `".*foo"` — suffix match (leading wildcard)
- `"[a-z]+"` — character class
- `".+"` — leading wildcard
- `""` — empty pattern
- `".*"` — match-all wildcard

**Assertions:** `AnalyzeRegex` returns nil for all cases.

---

Back-ref: `internal/vm/regex_optimize.go:AnalyzeRegex`, NOTE-011 (executor/NOTES.md)
