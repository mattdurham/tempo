# Code Review Guidance

Patterns derived from GitHub Copilot review comments on the last 20 merged PRs.
These are recurring failure modes — check every PR for each item before requesting review.

---

## 1. Comment & Documentation Accuracy

**When a function signature changes, update every comment that describes return values.**
- If a function grows from 3 return values to 4, every call-site comment citing the old tuple `(nil, 0, nil)` is now wrong.
- Copilot flags these reliably; reviewers read them less reliably.

**When an algorithm changes, update all prose that describes it.**
- If TopK switches from exact counting to Space-Saving (approximate), every comment, explain string, spec entry, and NOTES entry that says "exact count" must change.
- The same comment often appears in: the function body, the caller, `explain.go`, `NOTES.md`, `SPECS.md`. Update all of them.

**"Before" and "after" comments in perf PRs must accurately describe the before state.**
- Example: "Before: O(n) struct allocations" — if the old code actually did one large allocation, the comment is wrong and misleads future bisecters.
- Benchmark comments that say "produces ~9 blocks" must match the actual math (10 blocks if MaxBlockSpans=1024 and spans=10_000).

**NOTE-PERF-* and similar spec tags in comments must reflect the code they annotate.**
- Don't copy a NOTE tag from an old function into a new one if the rationale no longer applies.

---

## 2. Spec / Code Alignment (SPECS.md, NOTES.md, TESTS.md, BENCHMARKS.md)

**Spec back-refs must point to real function names.**
- `TESTS.md` entries with `Back-ref: rowset_test.go:TestRowSetWithCap_NegativeHint` fail when that test function doesn't exist.
- Before writing a back-ref, confirm the referenced function name is real.

**When behavior changes, update the spec entry on the same PR.**
- If `collectIntrinsicPlain` changes from zero-block-reads to block-reads, `SPECS.md` entries about that path must change in the same commit.
- When a stage is removed (e.g., CMS pruning stage), remove it from every spec table that lists stages.

**BENCH-* entries must match the actual benchmark setup.**
- If a benchmark uses 200 spans with MaxBlockSpans=10 (~20 blocks), but the spec says "spans >64 distinct blocks," the spec is wrong.
- Pre-condition state (e.g., "before the fix, blockOrder was preallocated to cap=8") must match the actual pre-fix code, not a stale assumption.

**Spec IDs referenced in code comments must correspond to their actual spec content.**
- `(SPEC-SK-18)` annotating `TopK.Add` is misleading if SPEC-SK-18 describes key truncation that no longer exists.
- Either update the spec entry or update the code comment to point to the correct ID.

---

## 3. Bounds Checking & Integer Overflow

**Never multiply untrusted file values in `int` before a bounds check.**
- `pos + rowCount*8` overflows `int` on 32-bit when `rowCount` is large or corrupt.
- Use division-based checks instead: `rowCount > (len(raw)-pos)/8`.
- Or promote to `uint64`/`int64` for the arithmetic, then compare.

**`uint32` → `int` conversions from file bytes can wrap on 32-bit.**
- `numBlocks` and `numColumns` read as `uint32` then cast to `int` bypass the MaxBlocks/MaxColumns guards on 32-bit platforms.
- Validate the `uint32` value against `math.MaxInt` before casting.

**Multiplied skip-lengths must be checked before use.**
- `skipColumnCMS` computing `depth*width*2*presentCount` in `int` can overflow and make the subsequent length guard unreliable.
- Use `uint64` math with explicit overflow/limit checks; only convert back to `int` after validating.

**Zero-length inputs from files deserve explicit rejection.**
- `bloomSize == 0` must be rejected: a zero-length bloom causes the per-block loop to not advance `pos`, desynchronizing all subsequent parsing.
- Reject at parse time with a clear error rather than producing silent corruption.

---

## 4. Memory Allocation Discipline

**Don't use dictionary length as a capacity hint for block-count-keyed maps.**
- A map keyed by `BlockIdx` has at most `blockCount` entries, not `len(DictEntries)`.
- `make(map[uint32]foo, len(col.DictEntries))` can massively over-allocate when there are many distinct values but few blocks.
- Cap hints: `min(len(col.DictEntries), 64)` or derive from a block-count bound.

**Preallocated slices with `SpanCount()` as the hint regress B/op for selective queries.**
- `make([]int, 0, hint)` with `hint = block.SpanCount()` eagerly allocates O(SpanCount) ints even when the query matches 0 spans.
- Cap at a reasonable upper bound (64, 256, 1024) or skip the hint for paths expected to be selective.

**Pool safety: don't return an object to a pool while refs to its fields are still live.**
- `w.bbPool.Put(bb)` while `builtBlock.traceRows` still points into `bb`'s maps causes a race: a concurrent `Get+reset` clears those maps before the merge pass reads them.
- Only pool after all consumers of the pooled object's data have finished.

**Zero-copy slice semantics must match the comments.**
- If the comment says "bloom data is copied at parse time" but the code stores `data[pos:pos+bloomSize]` (a slice into the retained buffer), the comment is wrong — and readers will make incorrect assumptions about buffer lifetimes.

---

## 5. Error Path Completeness

**Clear buffered writer state before returning errors from flush paths.**
- If `w.out.Write()` fails mid-flush, `w.pending`, `protoRoots`, and per-block index/meta updates may be partially applied.
- Subsequent `Flush()`/auto-flush calls will behave incorrectly unless the state is cleaned up.
- Mirror the `g.Wait()` error handling: clear pending + proto anchors before returning.

**Populate `QueryStats` / `TotalDuration` even on error paths.**
- Callers surface `QueryStats` to users; an empty stats struct on error loses all I/O context.
- Set `TotalDuration = time.Since(start)` and append a partial step with available counters before returning any error.

**`OnStats` must be called exactly once per `Collect()` invocation.**
- Early-return paths (e.g., bloom rejection) that skip `opts.OnStats` violate the contract.
- Populate a minimal `CollectStats` (ExecutionPath, SelectedBlocks=0) and call `OnStats` before any early return.

---

## 6. Test Quality

**Test names must match what the test actually asserts.**
- `TestZeroBlockRead` that now asserts `Block != nil` is misleading.
- `TestSharedLRUCache_KeyUsesFullIntLength_LargeValues` that uses 65KB inputs (not near `math.MaxInt32`) doesn't test what its name implies.
- Rename, or rewrite to actually reach the code path the name implies.

**Regression tests must reach the regression code path.**
- A test for BUG-5 (prealloc overflow) that passes an empty `buckets` map returns before the overflow calculation — it never exercises the guard.
- Factor the overflow logic into a testable helper, or arrange inputs so the path is actually reached.

**Don't create tests that OOM or are extremely slow in CI.**
- `numBuckets=5_000_000` with per-bucket map allocations will OOM under CI memory limits.
- Use small synthetic inputs to test behavior; use benchmarks (not `_test.go` unit tests) for scale validation.

**`-bench` flag takes a regex, not a comma-separated list.**
- `go test -bench=BenchmarkA,BenchmarkB` matches zero benchmarks.
- Use `go test -bench='BenchmarkA|BenchmarkB'` or a broad pattern like `Benchmark.*AllocCount`.

---

## 7. API Contract Consistency

**Public API surface changes require explicit permission (see `api.go` header).**
- Adding an exported function expands the public surface; confirm this is approved before the PR.
- If unsure, keep new entrypoints unexported until confirmed.

**Document all `ExecutionPath` values in one canonical place.**
- New paths like `"bloom-rejected"` must appear in the `CollectStats` doc comment.
- The set of valid values must be complete and accurate; callers use this for observability.

**Zero values of option types must have documented semantics.**
- `TopK == 0` should document whether it means "no limit" or "use default (10)."
- Inconsistency between validation (only reject `< 0`) and apply-defaults (`<= 0` → default) confuses callers.

**`SelectColumns` nil vs empty-non-nil must have a single, documented meaning.**
- If the contract says "non-nil applies filtering," then `len == 0` with non-nil must also filter (to zero fields), not behave like nil.
- Pick a semantic, document it, and implement it consistently.

---

## 8. Concurrency Safety

**Lazy memoization without `sync.Once` is a data race.**
- A nil check + assignment on a shared field (e.g., `r.intrinsicNames`) without a mutex races under concurrent calls.
- Use `sync.Once` for all lazy initialization. Model after `fileBloomOnce`/`fileSummaryOnce`.
- If a method is intentionally not concurrency-safe, document it explicitly.

**Object cache comments must not imply `GOMEMLIMIT` is a hard bound.**
- `GOMEMLIMIT` is a GC pacing target, not a hard cap; strongly-referenced entries cannot be reclaimed.
- Wording like "memory is bounded by GOMEMLIMIT" is incorrect and misleads operators into skipping eviction logic.

---

## 9. Input Validation at Parse Time

**Validate file version at open time, not deep in execution.**
- `NewLeanReaderFromProviderWithOptions` should reject non-V13 files when the codebase only supports V13.
- Letting a legacy file proceed to block reads produces misleading errors far from the real cause.

**Format version bytes and magic numbers must be checked before trusting lengths.**
- `buildFileBloomInfo` that skips version-byte validation on a "cached" buffer will misbehave on corrupt data.
- Mirror the pattern in `parseFileBloomSection`: check version, validate `colCount` bounds, ensure lengths don't exceed `len(raw)`.

---

## 10. `discovery.md` and Planning Docs Hygiene

**`.discovery.md` and `.bob/` task docs must be updated when public APIs change.**
- Stale signatures (e.g., `QueryTraceQL` returning `([]SpanMatch, error)` after it grew a `QueryStats` return) mislead agents building on that API.
- Absolute local filesystem paths (`/home/mdurham/source/...`) don't work for other developers; use repo-relative paths.
- When a struct field is removed (e.g., `Plan.PrunedByCMS`), remove it from every planning doc that references it.

---

## 11. Raw Block Byte Reads Must Route Through Cache (SPEC-ROOT-015)

**`ReadGroup` and `ReadBlocks` must never bypass `r.cache` for raw block bytes.**
- Any call to `ReadCoalescedBlocks(r.provider, ...)` or `r.provider.ReadAt(...)` from
  `ReadGroup` or `ReadBlocks` that does not first check `r.cache` violates SPEC-ROOT-015.
- The canonical pattern: probe cache for each blockID → full-group S3 fetch on any miss →
  store every fetched block back via `r.cache.Put(fileID+"/block/"+blockID, data)`.
- The `r.fileID == ""` guard is the only valid bypass — document it explicitly when present.

**New I/O helpers that read raw block bytes must use `ReadGroup` as their entry point.**
- Don't add new functions that call `ReadCoalescedBlocks` directly from within a `*Reader`.
- If a new caller needs batched block reads, it should call `ReadBlocks` or iterate `ReadGroup`
  over `CoalescedGroups` — both already route through the cache.

**Missing cache key in the SPEC-011 / SPECS.md cache key table is a spec violation.**
- Every new cache key written in the reader package must appear in the "Cache Key Mapping"
  table in `internal/modules/blockio/reader/SPECS.md`. Missing rows hide cache interactions
  from operators debugging memory pressure or unexpected S3 traffic.
