# Commit Instructions

Commit all changes from the intrinsic section optimization work.

## Context
- Design: .bob/state/brainstorm.md
- Plan: .bob/state/plan.md
- Review findings: .bob/state/review.md, .bob/state/go-presubmit.md
- Fix details: .bob/state/implementation-status.md

## What changed (3 coupled changes)

1. **Removed identity columns from intrinsic accumulator** — trace:id, span:id, span:parent_id, span:status_message no longer written to intrinsic section (saves ~48% storage). Block columns retain them via addPresent.

2. **Switched field population to block reads** — collectIntrinsicPlain always uses forEachBlockInGroups instead of lookupIntrinsicFields O(N) scan. Scales with M (result count) not N (total spans).

3. **Removed RefBloom from page TOC** — 256-byte bloom per page was 100% FPR at 10K entries. Value bloom kept. EncodePageTOC writes v0x01; DecodePageTOC reads-and-discards v0x02 for backward compat.

## Commit message

```
feat(blockpack): optimize intrinsic section — remove identity columns, switch to block reads

Remove trace:id, span:id, span:parent_id, span:status_message from intrinsic
accumulator (saves ~48% intrinsic section storage). Switch collectIntrinsicPlain
field population from O(N) intrinsic scan to O(M) block reads via
forEachBlockInGroups. Remove RefBloom (100% FPR) from page TOC; keep value bloom
and MinRef/MaxRef backward compat in DecodePageTOC.

Block columns retain all intrinsic values (dual storage). lookupIntrinsicFields
retained for Case B (TopK) and structural queries.
```

## Instructions

1. Do NOT commit .bob/ directory or any state files
2. Do NOT commit .docker/ changes or test infrastructure
3. Commit only the vendor/github.com/grafana/blockpack/ changes + tempodb/ changes
4. Do NOT push or create a PR — just commit locally
