# Structural TraceQL Query Correctness

This document is the canonical specification for the 6 structural TraceQL operators
(`>>`, `>`, `~`, `<<`, `<`, `!~`) implemented in `internal/executor/structural.go`.

All tests in `internal/executor/structural_test.go` and
`internal/executor/structural_tempo_test.go` are derived from the tables and rules below.

---

## Canonical 6-Span Fixture

```
Trace T1 (6 spans):

spanID  parentID  label
  A       nil      root
  B       A
  C       A
  D       B
  E       B
  F       C
```

Tree structure:
```
A (root)
├── B
│   ├── D
│   └── E
└── C
    └── F
```

spanID bytes (padded to 8 bytes): A=0x01, B=0x02, C=0x03, D=0x04, E=0x05, F=0x06.
traceID: 16 bytes, first byte = 0x42.
Each span has attribute `span.label` = "A".."F" for filtering.

---

## Operator Result Tables

For each operator, the query is `{ span.label = "X" } OP {}` where the left side
matches span X and the right side matches all spans.

Results show which spans are returned (right-side span that satisfies the relationship).

### `>>` (Descendant): Right span is a descendant of left span

| Left span | Right matches (descendants) |
|-----------|----------------------------|
| A         | B, C, D, E, F              |
| B         | D, E                       |
| C         | F                          |
| D         | (none — D is a leaf)       |
| E         | (none — E is a leaf)       |
| F         | (none — F is a leaf)       |

### `>` (Child): Right span is a direct child of left span

| Left span | Right matches (direct children) |
|-----------|---------------------------------|
| A         | B, C                            |
| B         | D, E                            |
| C         | F                               |
| D         | (none — no children)            |
| E         | (none — no children)            |
| F         | (none — no children)            |

### `~` (Sibling): Right span shares the same parentID as left span

| Left span | Right matches (siblings)              |
|-----------|---------------------------------------|
| A         | (none — A is root, parentID = nil)    |
| B         | C (shares parent A with B)            |
| C         | B (shares parent A with C)            |
| D         | E (shares parent B with D)            |
| E         | D (shares parent B with E)            |
| F         | (none — F is the only child of C)     |

Note: A span cannot be its own sibling (self-match excluded).

### `<<` (Ancestor): Right span is an ancestor of left span

| Left span | Right matches (ancestors)          |
|-----------|------------------------------------|
| A         | (none — A is root, no ancestors)   |
| B         | A                                  |
| C         | A                                  |
| D         | B, A                               |
| E         | B, A                               |
| F         | C, A                               |

### `<` (Parent): Right span is the direct parent of left span

| Left span | Right match (direct parent)        |
|-----------|------------------------------------|
| A         | (none — A is root, no parent)      |
| B         | A                                  |
| C         | A                                  |
| D         | B                                  |
| E         | B                                  |
| F         | C                                  |

### `!~` (Not Sibling): Right span has NO left-side sibling

Semantics: returns right-side spans R such that there is NO left-side span L where
`L.parentID == R.parentID AND L.parentID != nil AND L.spanID != R.spanID`.

| Left span | Right matches (not-sibling spans)                 |
|-----------|---------------------------------------------------|
| A         | B, C, D, E, F  (A is root; no left sibling exists)|
| B         | A, D, E, F     (C is excluded: shares parent A with B) |
| C         | A, D, E, F     (B is excluded: shares parent A with C) |
| D         | A, B, C, F     (E is excluded: shares parent B with D) |
| E         | A, B, C, F     (D is excluded: shares parent B with E) |
| F         | A, B, C, D, E  (F is only child of C; no left sibling) |

---

## Edge Cases

### Root span (parentID = nil)
- A span with `parentID = nil` is a root span.
- Root spans CANNOT be returned by `<` or `<<` (they have no parent to walk to).
- Root spans do NOT have siblings via `~` (nil parentID implies no structural siblings).
- For `!~`: nil-parentID right-side spans match (they have no structural parent group,
  so no left-side sibling can exist for them) UNLESS the span is also the left-match span
  (self-match prevention applies before the nil-parentID fast path).
- The root span CAN be returned by `>>`, `>`, `~` as a left-side span emits its
  descendants/children/siblings respectively.

### Orphan span (parentID set but parent not present)
- An orphan has a non-nil parentID but that parent is absent from the accumulated spans.
- The parent chain terminates at the orphan.
- An orphan is treated similarly to a root when walking chains: the walk stops.
- An orphan CAN still be returned as a direct child if its parentID matches a left span.

### Self-match prevention
- A span cannot be its own ancestor, descendant, sibling, parent, or child.
- In `evalDescendant`/`evalAncestor`: check `bytes.Equal(R.spanID, L.spanID)` → skip.
- In `evalSibling`/`evalNotSibling`: spans with the same parentID but same spanID are excluded.
- In `evalNotSibling` specifically: if R is itself a left-match span (`r.leftMatch == true`),
  R is excluded regardless of its parentID. This applies even when R has a nil parentID
  (which would otherwise pass the nil-parentID fast path). The general rule — a span cannot
  be emitted as a right-side result when it is also the left-matching span — takes precedence.

### Infinite loop prevention
- Parent chain walks are capped at `len(spans)` iterations to prevent cycles in
  malformed trace data.

### Duplicate spans (same spanID in multiple blocks)
- If the same span appears in multiple blocks, merge `leftMatch`/`rightMatch` flags (OR them).
- Only one record per spanID is kept per trace in the accumulator.

### Empty trace
- If a traceID key exists in the accumulator but has 0 spans, skip without panic.

### Nil FilterExpression (wildcard `{}`)
- Both left and right sides can be nil (matching all spans).
- `vm.CompileTraceQLFilter(nil)` returns a "match all" program.
- `{} >> {}` returns all non-root spans (every span that has a parent).

### Single-span trace
- A trace with only 1 span has no structural relationships.
- All 6 operators return empty for a single-span trace.

### Multi-block trace
- A single trace can span multiple blocks.
- The accumulator collects spans across ALL blocks before Phase 2 evaluation.
- Structural relationships are correctly computed even when spans are in different blocks.

---

## Implementation Notes

### Memory model
Structural queries accumulate ALL spans from selected blocks (not just matching ones).
This is necessary to correctly evaluate ancestor/descendant relationships which require
the complete parent tree.

Block selection pruning ensures we only scan blocks likely to contain relevant spans,
but within selected blocks, all spans are recorded.

This means memory usage scales with the total span count across selected blocks, not
just the matching spans. This is a known characteristic; future optimizations could
process one trace at a time if trace-block locality metadata is available.

### Which span is emitted
For all 6 operators, the returned span is the one on the RIGHT side of the structural
relationship:
- `>>`: emit the descendant (R)
- `>`: emit the child (R)
- `~`: emit the sibling (R)
- `<<`: emit the ancestor (R, which is the right-filter match)
- `<`: emit the parent (R, which is the right-filter match)
- `!~`: emit the not-sibling right-side span (R)

### SpanFields for structural matches
Structural matches emit a `*SpanFields` populated with the matched span's intrinsic
fields (TraceID, SpanID, etc.) from the stored spanRecord. Dynamic attributes are
not available after the block scan phase. The `Fields` field is a valid `*SpanFields`
to satisfy `Clone()` requirements.

---

## Verification Commands

```bash
# Run all structural tests
go test ./internal/executor/... -run TestStructural -v

# Run with race detector
go test ./internal/executor/... -race -run TestStructural

# Full precommit check
make precommit
```
