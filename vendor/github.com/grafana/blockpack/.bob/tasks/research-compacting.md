# Research: Block Compaction Strategy

**Status:** TODO
**Created:** 2026-02-15
**Priority:** HIGH
**Type:** Research

## Overview

Research and design a block compaction strategy for blockpack. Compaction is critical for long-term storage efficiency, query performance, and data accuracy. References in the codebase indicate compaction should handle:

1. Recomputing aggregates during block compaction
2. Merging overlapping time ranges
3. Dropping old metric stream blocks after retention period
4. Handling late-arriving spans (>5 minutes late)

## Context

**Current State:**
- Compaction is mentioned in docs but not implemented
- `BLOCKPACK_FORMAT.md:1713` references: "Re-compute aggregates during compaction"
- `PRECOMPUTED_METRIC_STREAMS_REPORT.md:346-349` outlines compaction strategy
- `01-RESEARCH.md:792` mentions CompactionLevel in BlockMeta fields (unclear if critical)

**Why This Matters:**
- Late-arriving data not currently included in aggregates
- No mechanism to merge overlapping time ranges
- No retention/cleanup strategy
- Query performance degrades with many small blocks
- Storage efficiency suffers without consolidation

## Research Questions

### 1. Compaction Triggers
- When should compaction run? (time-based, size-based, count-based)
- Should it be background/scheduled or on-demand?
- What are the resource implications (CPU, memory, I/O)?

### 2. Block Selection
- Which blocks should be compacted together?
- How to handle overlapping time ranges?
- Maximum/minimum block sizes for compaction?
- Should CompactionLevel field track number of compactions?

### 3. Aggregate Recomputation
- Which aggregates need recomputation? (metrics, quantiles, histograms)
- How to handle KLL sketches during merge?
- How to handle T-Digest/DDSketch during merge?
- Performance impact of recomputing vs. incremental merge?

### 4. Late-Arriving Data
- How to identify late-arriving spans (>5 minutes late)?
- Which aggregates are affected?
- Recompute entire block or incremental update?
- Trade-offs: accuracy vs. performance

### 5. Retention & Cleanup
- How to identify blocks past retention period?
- Safe deletion strategy (reference counting, tombstones)?
- Interaction with query layer (avoid deleting actively-read blocks)

### 6. State Management
- How to track compaction state (in-progress, completed)?
- Atomic operations for replacing old blocks with compacted blocks?
- Rollback strategy if compaction fails?

### 7. Integration Points
- Changes needed in Writer? Reader? Executor?
- BlockMeta fields required (CompactionLevel, OriginalBlocks)?
- Impact on query planning and block selection?

## Deliverables

1. **Design Document**: Compaction strategy with:
   - Trigger conditions
   - Block selection algorithm
   - Aggregate recomputation approach
   - State management and atomicity guarantees

2. **Architecture Proposal**:
   - New types/interfaces needed
   - Integration with existing Reader/Writer
   - Background goroutine management

3. **Implementation Plan**:
   - Phase 1: Core compaction (merge blocks)
   - Phase 2: Aggregate recomputation
   - Phase 3: Late-arriving data handling
   - Phase 4: Retention/cleanup

4. **Testing Strategy**:
   - Unit tests for compaction logic
   - Integration tests for end-to-end compaction
   - Benchmarks for performance impact
   - Correctness tests for aggregate accuracy

## Related Files

- `doc/BLOCKPACK_FORMAT.md` (lines 1710-1714)
- `doc/PRECOMPUTED_METRIC_STREAMS_REPORT.md` (lines 343-350)
- `.planning/phases/01-core-tempo-api-integration/01-RESEARCH.md` (line 792)
- `internal/blockio/writer/` (writer logic)
- `internal/executor/` (query execution, block selection)

## Success Criteria

- [ ] Compaction triggers clearly defined
- [ ] Block selection algorithm documented
- [ ] Aggregate merge strategy validated (correctness)
- [ ] Late-arriving data handling specified
- [ ] Retention/cleanup strategy outlined
- [ ] State management and atomicity guaranteed
- [ ] Integration points identified
- [ ] Implementation phases defined with effort estimates
- [ ] Testing strategy covers correctness and performance

## Estimated Effort

**Research**: 6-8 hours
**Design Document**: 4-6 hours
**Total**: 10-14 hours

## Next Steps

1. Read compaction strategies from similar systems (Tempo, Cortex, Thanos)
2. Analyze current blockio Writer/Reader interfaces
3. Prototype block merge logic
4. Design aggregate recomputation for KLL sketches
5. Document findings and create implementation task

---

**Note**: This is research/design work. Implementation will be a separate task once strategy is validated.
