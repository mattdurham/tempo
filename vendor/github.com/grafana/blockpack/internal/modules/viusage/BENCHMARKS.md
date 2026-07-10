# viusage — Benchmark Specifications

This document defines the benchmark suite for `internal/modules/viusage`. Baseline numbers
serve as regression thresholds, per root `SPEC.md` SPEC-ROOT-009.

## ID convention

Entries use the module-local, sequential prefix `BENCH-VIUSAGE-N`, independent of the shared
`BENCH-VI-N` space maintained by `valueindex`/`valueindexcompactor`/`valueindexconsumer`'s own
BENCHMARKS.md files (see `SPECS.md`'s ID-convention section).

**This file is a stub — no real production backfill cost measurement exists yet.** #496's own
cost-model tunables (R4: the repeated-use threshold, the 1h trigger window, the 48h backfill
window) are explicitly disclosed as unmeasured starting points (`NOTES.md` NOTE-VIUSAGE-3) —
there is no existing production usage-registry telemetry to calibrate against, because THIS
module is what will eventually produce that telemetry. Until real production backfill runs
exist, this file documents the COST MODEL this design is built around, not a measured
baseline.

Next free ID: **BENCH-VIUSAGE-1**.

---

## Cost model (documented, not yet measured)

**`BackfillEngine.Run`'s dominant cost is full historical block I/O, not extraction or PUT.**
Per R6/SPEC-VIUSAGE-5, the backfill engine reads RAW historical blocks via the injected
`BlockFetcher` — this repo's core I/O invariant (ARCH-002/003: always read an entire block in
a single I/O operation, never per-column) means every block in the `[now-WindowSeconds, now]`
window is fetched in full, regardless of how much of that block's data actually belongs to the
one column being backfilled. This is the exact reason R4 chose a 48h window (narrower than
cube's 7-day window): cube's backfill reads cheap, already-extracted VI files; VI's own
backfill cannot, because backfilling a NEVER-INDEXED column means no pre-extracted VI data
exists for it yet — the whole point of the backfill is to produce that data for the first
time.

**Expected scaling:** backfill cost for one triggered column is approximately linear in
`WindowSeconds × (block flush rate for the tenant)` — the number of historical blocks
overlapping the window — NOT in the column's own cardinality or value distribution (extraction
of a single allowlisted column from an already-fetched block is comparatively cheap; the block
fetch itself dominates). A tenant with a high block-flush rate (frequent, small block flushes)
will see proportionally more blocks-per-window, and therefore proportionally more backfill
cost, than a tenant with the same wall-clock window but a lower flush rate.

**Candidate future benchmarks (not yet written, listed for whoever picks this up once real
telemetry exists):**
- `BenchmarkBackfillEngine_Run` — wall-clock and I/O-op count for one column's backfill run
  against a synthetic block set of known size/count, as a regression baseline for the
  block-fetch-dominates-cost claim above (analogous to `valueindex/BENCHMARKS.md`'s
  `BENCH-VI-1` methodology: measure the property the design claims, with an explicit
  before/after or scaling-ratio comparison, not a single absolute number).
- `Registry.updateEntryWithRetry` round-trip latency under realistic conflict-retry rates —
  relevant once real concurrent-trigger volume is observed in production (this bounds how much
  registry contention costs at scale, distinct from the backfill I/O cost above).
- A real production measurement of "how many distinct historical blocks actually exist within
  a 48h window, per tenant, at realistic block-flush cadences" — this is the missing input
  that would let R4's window default be recalibrated from evidence rather than the current
  documented guess.
