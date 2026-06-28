## NOTE-VI-017 — Value index compactor service (issue #399)

Date: 2026-06-25

The compactor is the third and final stage of the value-index pipeline (publisher NOTE-VI-015
#397, consumer NOTE-VI-016 #398, compactor #399). It periodically merges the many small L0
value index files the consumer writes into fewer, larger L1/L2 files per (tenant, column), and
drops entries whose originating blockpack file has been deleted by retention.

### Single pass = one level per column

`compactColumn` groups a column directory's files by parsed compaction level and compacts only
the **lowest** level that meets `CompactThresholdFiles`. Only one level is merged per pass; a
higher level that has since crossed the threshold (because a prior pass produced more L1 files)
is picked up on a later pass. This keeps each `CompactFiles` call same-level (required by
VI-012 — output level = input level + 1) without trying to cascade L0→L1→L2 in a single run,
which would complicate crash safety.

### Retention via source-existence, not delete messages

There are deliberately no delete events on the queue (#397 publishes create-only). Instead each
entry carries its `SourceRef` (the originating blockpack path), and during compaction every
unique source is probed via `SourceExister.Exists` (an S3 HEAD in production). Entries from a
deleted source are dropped rather than propagated, so dead entries are reclaimed organically as
retention removes the source blocks. `cachingRefChecker` (store.go) caches existence per source
path for the duration of one compaction job so each unique source is probed at most once,
regardless of how many entries reference it — the issue's "batch HEAD by unique source path"
cost optimisation. `exister` may be nil to skip the check entirely (all entries propagated).

### Write-then-delete crash safety; stateless / multi-instance safe

`mergeLevel` writes the merged output file(s) with **fresh** output IDs (`valueindex.NewID()`)
*before* deleting any input. A crash after Put but before Delete leaves both the new output and
the old inputs in place; the next pass recompacts and the duplicate entries are removed by the
merge's dedup. Because outputs use fresh IDs and inputs are only deleted after a successful
write, multiple compactor instances are safe — at worst they produce redundant outputs that the
next pass dedups. No persistent state is kept between passes.

### Per-(tenant, column) isolation, best-effort pass

`RunOnce` iterates tenants, `compactTenant` iterates that tenant's column directories. Both
record only the first error and continue so one bad column/tenant does not stall the rest; the
first error is returned for the caller to log. `Run` ignores per-pass errors (logs are the
caller's job) and keeps ticking. Tenants are either an explicit list or `"*"` (discovered by
listing the index prefix and taking the first path segment after it).

### IndexStore abstraction

Object storage is abstracted behind `IndexStore` (List/Get/Put/Delete, all `context`-aware) so
the orchestration is unit-testable without a real object store. Mirroring the consumer's
`ObjectPutter` decision (NOTE-VI-016), the production S3-backed `IndexStore` and `SourceExister`
are supplied by **tempo**, not blockpack — blockpack stays object-store-agnostic. `List` returns
full keys (not leaf names) so the caller can Get/Delete directly; the compactor groups by
`path.Dir`.

### Output key

`<index_prefix>/<tenant>/<col_hash>/L<level>-<xid>.blockpack` (via
`valueindex.FormatFilename(level, valueindex.NewID())`). Same column → same `<col_hash>`
directory, which both the consumer writes into and the compactor lists by.

### Public re-export (embedder pattern)

`/valueindexcompactor/valueindexcompactor.go` re-exports the minimal API (type aliases + ctor
wrapper), mirroring NOTE-VI-015 / NOTE-VI-016 / NOTE-370, because tempo cannot import
`internal/*`.

Back-refs:
`internal/modules/valueindexcompactor/service.go`,
`internal/modules/valueindexcompactor/store.go`,
`internal/modules/valueindexcompactor/config.go`,
`valueindexcompactor/valueindexcompactor.go`

## NOTE-VI-023 — Prometheus metrics for the compactor (issue #407)

Date: 2026-06-28

The compactor exposes Prometheus metrics via a `Registerer prometheus.Registerer` field on
`Config` (`yaml:"-"`, injected by tempo as `prometheus.DefaultRegisterer`), nil = total no-op.
See the consumer's NOTE-VI-023 for the shared design (nil-receiver no-ops, `registerOrReuse`
AlreadyRegisteredError tolerance, pre-resolved native-histogram observers). The collectors live
in `metrics.go`.

### CompactFiles now returns CompactStats

`valueindex.CompactFiles` previously returned only `error`; it now returns
`(CompactStats, error)` where `CompactStats{Retained, Dropped int}` counts entries kept vs
dropped by the retention `Checker`. These are **pre-dedup** counts (each input occurrence is
counted once), so `entries_retained_total` reflects the live entries flowing into the merge, not
the deduped output cardinality. This is the only exported-signature change; all callers (the
compactor `mergeLevel`, the valueindex compaction tests) were updated in the same commit. The
compactor feeds `stats.Retained`/`stats.Dropped` into the
`blockpack_value_index_compactor_entries_{retained,dropped}_total` counters.

### Per-merge file counts

`mergeLevel` records `files_read` (= input count), `files_written` (incremented in the
`CompactFiles` output callback, so it counts split outputs correctly), and `files_deleted`
(incremented per successful input delete, so a partial delete failure undercounts deletions
rather than overcounting). `merge_duration_seconds` wraps the whole get→merge→put→delete cycle.

### Per-pass run counters

`RunOnce` wraps the whole pass with `run_duration_seconds` and increments `runs_total{success}`
or `runs_total{error}` based on the first-error result. List/get/put/delete failures also bump
`errors_total{op}` at their call sites.

Back-refs:
`internal/modules/valueindexcompactor/metrics.go`,
`internal/modules/valueindexcompactor/service.go`,
`internal/modules/valueindexcompactor/config.go`,
`internal/modules/valueindex/compaction.go`
