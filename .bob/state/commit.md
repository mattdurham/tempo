# Commit Status

Generated: 2026-03-29T00:00:00Z
Status: SUCCESS

---

## Commit Details

**Branch:** blockpack-integration
**Commit SHA:** 0d645af18df2e936c2ef046ef1e349e809148790
**Commit Message:**
```
feat(blockpack): optimize intrinsic section — remove identity columns, switch to block reads

Remove trace:id, span:id, span:parent_id, span:status_message from intrinsic
accumulator (saves ~48% intrinsic section storage). Switch collectIntrinsicPlain
field population from O(N) intrinsic scan to O(M) block reads via
forEachBlockInGroups. Remove RefBloom (100% FPR) from page TOC; keep value bloom
and MinRef/MaxRef backward compat in DecodePageTOC.

Block columns retain all intrinsic values (dual storage). lookupIntrinsicFields
retained for Case B (TopK) and structural queries.

Co-Authored-By: Claude Sonnet 4.6 <noreply@anthropic.com>
```

**Files Committed:** 132 files changed, 30404 insertions(+), 1578 deletions(-)

Key files:
- tempodb/encoding/vblockpack/backend_block.go
- tempodb/encoding/vblockpack/compactor.go
- tempodb/encoding/vblockpack/create.go
- tempodb/encoding/vblockpack/fetch_test.go
- tempodb/tempodb.go
- vendor/github.com/grafana/blockpack/api.go
- vendor/github.com/grafana/blockpack/reader.go
- vendor/github.com/grafana/blockpack/internal/modules/blockio/writer/intrinsic_accum.go
- vendor/github.com/grafana/blockpack/internal/modules/blockio/writer/file_bloom.go
- vendor/github.com/grafana/blockpack/internal/modules/blockio/reader/intrinsic_reader.go
- vendor/github.com/grafana/blockpack/internal/modules/blockio/shared/intrinsic_ref_filter.go
- (+ all other blockpack vendor updates)

**Excluded (per instructions):**
- .bob/ state files
- .docker/ changes
- modules/frontend/search_sharder_test.go
- modules/querier/querier_query_range.go
- .claude/scheduled_tasks.lock

---

## Pull Request

Not created (local commit only, per instructions).

---

## Summary

Commit created successfully on branch blockpack-integration.
No push performed, no PR created (per explicit instructions).

---

## For Orchestrator

**STATUS:** SUCCESS
**COMMIT_SHA:** 0d645af18df2e936c2ef046ef1e349e809148790
**BRANCH:** blockpack-integration
**NEXT_PHASE:** MONITOR
