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

---

## 2. RegexFastKind Shape Classification (issue #513)

### VM-T-05: TestAnalyzeRegex_RequiresTrailingChar

**Scenario:** A literal followed by `OpPlus` (`"bob.+"`) classifies to `RequiresTrailingChar`.

**Assertions:** `AnalyzeRegex("bob.+")` is non-nil; `RequiresTrailingChar` is true;
`IsLiteralContains`, `AnchoredPrefix`, `AnchoredExact` are all false.

---

### VM-T-06: TestAnalyzeRegex_AnchoredPrefix

**Scenario:** A leading-anchored literal, with or without a trailing `.*` (no `$`), classifies
to `AnchoredPrefix`.

**Setup:** Table-driven cases: `"^bob"`, `"^bob.*"`.

**Assertions:** For each pattern, `AnchoredPrefix` is true; `IsLiteralContains`,
`RequiresTrailingChar`, `AnchoredExact` are all false.

---

### VM-T-07: TestAnalyzeRegex_AnchoredExact

**Scenario:** A leading-and-trailing-anchored literal (`"^bob$"`) classifies to `AnchoredExact`.

**Assertions:** `AnchoredExact` is true; `IsLiteralContains`, `RequiresTrailingChar`,
`AnchoredPrefix` are all false.

---

### VM-T-08: TestAnalyzeRegex_DeferredShapesFallBackSafely

**Scenario:** Deliberately deferred shapes (see SPEC-VM-2's "Deferred shapes" list) resolve to
`RegexFastNone` (all four booleans false) — correct via full-regex-engine fallback, never a
partially-correct fast path.

**Setup:** Table-driven cases: `"^bob.*$"` (anchor+wildcard+end-anchor together), `"bob?"`
(`OpQuest`), `"bob|baz.+"` (mixed-shape alternation), `"bob.*$"` (the `"foo.*$"` bug shape,
unanchored), `"bob$"` (suffix-anchor-only, no leading anchor), `"^bob.+"`, `"^bob.+$"`,
`"bob.+$"` (leading-anchor/end-anchor combinations with `OpPlus`, no benchmark evidence).

**Assertions:** For each pattern, `AnalyzeRegex` returns non-nil (prefixes still usable for
scan pre-filtering) but all four shape booleans (`IsLiteralContains`, `RequiresTrailingChar`,
`AnchoredPrefix`, `AnchoredExact`) are false.

---

### VM-T-09: TestAnalyzeRegex_UnanchoredStarEndAnchor_Fixed

**Scenario:** `"foo.*$"` must NOT be classified as pure contains — an embedded `\n` between the
literal and end-of-string blocks `.*` from reaching `$`, so `"foo.*$"` is not equivalent to
`strings.Contains("foo")`. Regression guard for the NOTE-514 (`vm/NOTES.md`) latent bug: prior to
the Phase 1 fix, `extractPrefixFromConcat` returned on `OpStar` before ever inspecting the
trailing end anchor.

**Assertions:** `AnalyzeRegex("foo.*$")` is non-nil; all four shape booleans are false.

---

### VM-T-10: TestAnalyzeRegex_CaseInsensitiveAnchoredOrTrailingFallsBackSafely

**Scenario:** `CaseInsensitive` combined with the anchored/trailing shapes must force
`RegexFastNone` (all four shape booleans false) rather than the wrong, byte-comparison fast
path — regression test for the CRITICAL bug found in review (task #119, NOTE-515,
`vm/NOTES.md`): `hasAnyPrefix`/`anyPrefixHasTrailingChar`/`equalsAny` compare byte-for-byte and
never fold case.

**Setup:** Table-driven cases: `"(?i)^bob"`, `"(?i)bob.+"`, `"(?i)^bob$"`.

**Assertions:** For each pattern, `CaseInsensitive` is true, but all four shape booleans
(`IsLiteralContains`, `RequiresTrailingChar`, `AnchoredPrefix`, `AnchoredExact`) are false.

Back-ref: `internal/vm/regex_optimize_shapes_test.go`. See `SPECS.md` SPEC-VM-2,
`NOTES.md` NOTE-514/NOTE-515. Issue #513, task #119.
