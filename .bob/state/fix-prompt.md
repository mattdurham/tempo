# Fix Review Issues (Iteration 1)

## Issues to Fix

Read the full reviews at .bob/state/review.md AND .bob/state/go-presubmit.md.

All issues are comment/documentation accuracy — no code logic changes needed.

### HIGH (fix first)

1. **stream.go:217-219** — NOTE-050 comment says "intrinsic columns are no longer in block payloads." This is wrong — dual storage is intact (addPresent calls kept). Fix the comment to say the intrinsic fast path is used because it provides efficient pre-filtering, not because block payloads lack intrinsic columns.

2. **stream.go:1216-1218** — lookupIntrinsicFields docstring claims page-skipping via MinRef/MaxRef/RefBloom and references GetIntrinsicColumnForRefs. Those fields were removed (NOTE-007) and the code uses GetIntrinsicColumn. Update the docstring to match reality.

### MEDIUM

3. **executor/NOTES.md** — Entry titled "NOTE-NNN" was never assigned a real sequential ID. Grep for the last NOTE-0XX and assign the next number.

4. **stream.go:185-193** — secondPassCols comment says identity values "must come from lookupIntrinsicFields" but after NOTE-005 (identity columns removed from intrinsic section), they come from MatchedRow.Block via forEachBlockInGroups. Update the comment.

5. **executor/NOTES.md NOTE-050 addendum** — The addendum (dated 2026-03-25) states addPresent calls were removed. This is wrong — they were retained. Fix or remove the addendum.

6. **shared/NOTES.md NOTE-006** — Still documents RefBloom/MinRef/MaxRef as active without noting supersession by NOTE-007. Add a forward cross-reference.

### LOW (fix if easy)

7. **feedBytes docstring** — still mentions removed columns. Update.
8. **DecodePagedColumnBlobFiltered** — dead refFilter parameter. Add _ = refFilter or remove param if no callers pass non-nil.

## Constraints
- Do NOT rewrite code that is not related to a reported issue
- Do NOT introduce new functionality
- Only fix comments, docstrings, and NOTES.md entries
- After fixes: go build ./tempodb/... must still pass

## Changed Files (for context)
See .bob/state/review-prompt.md for the full file list.
