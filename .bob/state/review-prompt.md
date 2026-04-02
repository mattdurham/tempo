## Review Scope

This review covers the CMS removal from blockpack sketch system on the drop-cms-sketch branch.

### Changed Files

Production:
- vendor/.../blockio/reader/file_sketch_summary.go
- vendor/.../blockio/reader/layout.go
- vendor/.../blockio/reader/sketch_index.go
- vendor/.../blockio/writer/sketch_index.go
- vendor/.../blockio/writer/writer_block.go
- vendor/.../executor/plan_blocks.go
- vendor/.../executor/stream.go
- vendor/.../executor/stream_log_topk.go
- vendor/.../queryplanner/column_sketch.go
- vendor/.../queryplanner/explain.go
- vendor/.../queryplanner/planner.go
- vendor/.../queryplanner/scoring.go

Tests: file_sketch_summary_test.go, layout_test.go, plan_blocks_test.go, pruning_bench_test.go, scoring_test.go, sketch_integration_test.go

### What Changed (CMS removal)

1. **Removed identity columns from intrinsic accumulator** (writer/writer_block.go, writer/intrinsic_accum.go):
   - Removed feedIntrinsic* calls for trace:id, span:id, span:parent_id, span:status_message
   - Kept addPresent calls (block columns still store these values)
   - Removed computePageRefRange and collectDictPageRefs

2. **Switched field population from intrinsic scan to block reads** (executor/stream.go):
   - Removed useIntrinsicLookup branch from collectIntrinsicPlain
   - All Case A results now use forEachBlockInGroups (both range and equality predicates)
   - lookupIntrinsicFields retained for Case B (collectIntrinsicTopK) and structural queries

3. **Removed RefBloom from page TOC** (shared/constants.go, shared/types.go, shared/intrinsic_codec.go):
   - Removed IntrinsicRefBloomBytes, IntrinsicRefBloomK constants
   - Removed RefBloom/MinRef/MaxRef from PageMeta
   - EncodePageTOC writes v0x01; DecodePageTOC reads-and-discards v0x02 RefBloom for backward compat
   - Value bloom KEPT (helps dict predicate evaluation)

### Key Files to Review

Focus on these files (the core changes):
- `vendor/github.com/grafana/blockpack/internal/modules/blockio/shared/constants.go`
- `vendor/github.com/grafana/blockpack/internal/modules/blockio/shared/types.go`
- `vendor/github.com/grafana/blockpack/internal/modules/blockio/shared/intrinsic_codec.go`
- `vendor/github.com/grafana/blockpack/internal/modules/blockio/shared/intrinsic_ref_filter.go`
- `vendor/github.com/grafana/blockpack/internal/modules/blockio/writer/intrinsic_accum.go`
- `vendor/github.com/grafana/blockpack/internal/modules/blockio/writer/writer_block.go`
- `vendor/github.com/grafana/blockpack/internal/modules/executor/stream.go`
- `vendor/github.com/grafana/blockpack/internal/modules/executor/predicates.go`
- `vendor/github.com/grafana/blockpack/internal/modules/executor/execution_path_test.go`

### Critical Review Criteria

- **Backward compat**: v0x02 files must decode correctly (DecodePageTOC reads-and-discards)
- **Dual storage**: block columns must retain ALL intrinsic values (addPresent calls preserved)
- **lookupIntrinsicFields retained**: still used by collectIntrinsicTopK (Case B) and stream_structural.go
- **feedIntrinsic removed ONLY for**: trace:id, span:id, span:parent_id, span:status_message
- **feedIntrinsic KEPT for**: span:duration, span:start, span:status, span:kind, span:name, resource.service.name
- **SPECS.md/NOTES.md/TESTS.md updated** alongside code changes

### Context

- Design: .bob/state/brainstorm.md
- Plan: .bob/state/plan.md
