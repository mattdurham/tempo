# Fix queryplanner/NOTES.md §8: OR composite description contradicts code and NOTE-012

**Type:** cleanup
**Cleanup-type:** spec-docs
**Severity:** MEDIUM
**Source:** discover-docs (round 2)

## Problem

`internal/modules/queryplanner/NOTES.md` §8 "OR composite pruning" paragraph (lines 152-155) says:

> In `blockSetForPred`, if any OR child is unconstrained (nil), the entire OR returns nil —
> no blocks can be pruned when any alternative is unindexed. Only when all children are
> indexed does the OR return a union of their block sets. See NOTE-012.

This directly contradicts three authoritative sources:

1. **The actual code** (`selection.go:blockSetForPred` comment): "Returns nil only when ALL children are unconstrained."
2. **NOTE-012** (same NOTES.md, lines ~249): "Union constrained children, skip unconstrained (nil) ones. A nil child means the column has no range index... Skipping a nil OR child is safe."
3. **SPECS.md §3.2**: "Returns nil (unconstrained) only when ALL children are unconstrained."

The §8 text describes an older, discarded semantics. NOTE-012 added 2026-03-06 (verified 2026-03-14) superseded it.

## Fix

This is a CLEANUP task. Do NOT change any code.

In `internal/modules/queryplanner/NOTES.md` §8, update the "OR composite pruning" paragraph to:

> **OR composite pruning (updated 2026-03-14):** OR composites carry per-child `Values`.
> In `blockSetForPred`, unconstrained (nil) OR children are **skipped** — they represent
> columns absent from the file entirely (writer invariant). The OR returns the union of
> constrained children's block sets. Returns nil only when ALL children are unconstrained.
> See NOTE-012.

## Acceptance criteria
- The §8 OR description says nil children are "skipped" and OR returns nil "only when ALL children are unconstrained."
- The description matches NOTE-012 and the code.
