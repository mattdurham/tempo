# Code Quality Review: Intrinsic Paging + Top-K Refactor

Branch: `blockpack-integration`
Date: 2026-03-13
Scope: `vendor/github.com/grafana/blockpack/` and `cmd/bpanalyze/`

---

## 1. Dead Code

### 1a. `intrinsicDictMatchRefs` is dead code
**File:** `executor/predicates.go:1332`
**Issue:** `intrinsicDictMatchRefs` is defined but has zero callers. The `scanIntrinsicLeafRefs` function replaced it -- dict columns now go through `ScanDictColumnRefsWithBloom` (raw blob scanning), never through the old full-decode path.
**Fix:** Delete `intrinsicDictMatchRefs` entirely.

### 1b. `intrinsicFlatMatchRefs` is nearly dead -- only used as fallback for equality on flat columns
**File:** `executor/predicates.go:1378`, called at line 1305
**Issue:** `intrinsicFlatMatchRefs` requires a full `GetIntrinsicColumn` decode (building the entire `IntrinsicColumn` struct). It is only reached when `scanIntrinsicLeafRefs` handles equality predicates on flat columns (`len(leaf.Values) > 0`). The comment at line 1299 says "use full-decode fallback for simplicity since equality on flat columns is rare." This is the only remaining call site. Consider replacing it with a raw-scan variant for consistency, or at minimum add a TODO.
**Fix:** Add `// TODO: replace with raw-scan variant (equality on flat columns is rare but this forces full struct decode)` or implement `ScanFlatColumnEqualityRefs`.

### 1c. `CollectTopK` is now a trivial shim
**File:** `executor/stream_topk.go:142`
**Issue:** `CollectTopK` just delegates to `e.Collect(r, program, opts)`. The body went from ~70 lines of real logic to a one-liner. The method is exported and referenced in SPECS.md and api.go comments, so it cannot be deleted immediately, but it should be marked deprecated.
**Fix:** Add a `// Deprecated: use Collect directly.` comment for Go documentation tooling.

---

## 2. Duplicated Sorting Logic

### 2a. `encodeFlatColumn` duplicates `sortFlatAccum`
**File:** `writer/intrinsic_accum.go:244-277` vs `writer/intrinsic_accum.go:499-529`
**Issue:** `encodeFlatColumn` (v1 monolithic path) contains inline sorting code that is identical to `sortFlatAccum` (extracted for the v2 paged path). The two sort blocks build the same `type row struct`, call `sort.Slice` the same way, and write back the same way.
**Fix:** Replace the inline sort in `encodeFlatColumn` with a call to `sortFlatAccum(c)` before the encoding loop, matching how `encodePagedFlatColumn` does it at line 598.

---

## 3. Duplicated Page TOC Parsing Preamble

### 3a. Three `scanFlatPaged*` functions repeat identical TOC-parsing boilerplate
**Files:** `shared/intrinsic_codec.go` lines 887-905 (`scanFlatPagedBlob`), 977-995 (`scanFlatPagedTopK`), 1070-1088 (`scanFlatPagedFiltered`)
**Issue:** Each function independently does: check `len(blob) < 5`, skip sentinel, read `tocLen`, bounds-check, `DecodePageTOC`, validate format/colType, extract `blockW`/`rowW`/`refSize`. This is 18 lines copy-pasted three times with zero variation.
**Fix:** Extract a helper:
```go
type pagedFlatContext struct {
    toc      PagedIntrinsicTOC
    blobBase int // byte offset of first page blob in blob
    blockW   int
    rowW     int
    refSize  int
}

func parseFlatPagedHeader(blob []byte) (*pagedFlatContext, error) { ... }
```
Each `scanFlatPaged*` function calls `parseFlatPagedHeader` and gets a struct back instead of repeating 18 lines.

### 3b. `scanDictPagedBlob` has the same preamble pattern
**File:** `shared/intrinsic_codec.go`, `scanDictPagedBlob`
**Issue:** Same sentinel-skip, tocLen-read, DecodePageTOC pattern. Could share a generic `parsePagedHeader` that returns format-agnostic TOC context.
**Fix:** Generalize the helper from 3a to work for both flat and dict paged blobs (just don't validate format in the helper -- let callers check `toc.Format`).

---

## 4. Unbounded Process-Level Caches (Memory Leak Risk)

### 4a. `parsedIntrinsicCache` and `parsedSketchCache` are never evicted
**File:** `reader/parser.go:19,24`
**Issue:** Both are `sync.Map` with comments stating "entries are never evicted." In a long-running Tempo process that reads many blockpack files over time (compaction creates new files, old files get deleted), these maps grow unboundedly. Each `IntrinsicColumn` can hold millions of `BlockRef` entries and `[]uint64` values -- potentially hundreds of MB per column. The `sketchIndex` is smaller but still accumulates.
**Fix:** Replace `sync.Map` with an LRU cache bounded by entry count or byte size. The existing `SharedLRUProvider` in the codebase could serve as a model. At minimum, add eviction when the source blockpack file is deleted/compacted away, or use a `sync.Map` with periodic sweeps.

---

## 5. Stringly-Typed Column Names

### 5a. Raw string literals for intrinsic column names
**Files:** `reader/intrinsic_reader.go:107` (`"span:end"`), `reader/intrinsic_reader.go:143` (`"span:start"`, `"span:duration"`), `executor/predicates.go` (column name map)
**Issue:** Column names like `"span:end"`, `"span:start"`, `"span:duration"` are repeated as raw strings across files. The `traceIntrinsicColumns` map in `predicates.go` already enumerates them, but there are no shared constants. A typo like `"span:End"` would silently fail.
**Fix:** Define constants in `shared/constants.go`:
```go
const (
    ColSpanStart    = "span:start"
    ColSpanEnd      = "span:end"
    ColSpanDuration = "span:duration"
    // ... etc
)
```
Use them consistently. Not urgent but prevents silent bugs.

---

## 6. `pageRefsStart` Ignores Its Second Parameter

**File:** `shared/intrinsic_codec.go:1408`
```go
func pageRefsStart(pageRaw []byte, _ int) int {
```
**Issue:** The `rowCount` parameter is declared but explicitly ignored with `_`. The function is called from 4 sites that all pass `rowCount`. Either the parameter should be used for validation, or the signature should drop it to avoid confusion.
**Fix:** Remove the unused parameter from the signature and all 4 call sites, or add a bounds check using it.

---

## 7. Bloom Filter: FNV Double-Hash Quality

**File:** `shared/intrinsic_bloom.go:33-38`
```go
func intrinsicBloomHashes(key []byte) (h1, h2 uint64) {
    h := fnv.New64a()
    _, _ = h.Write(key)
    h1 = h.Sum64()
    _, _ = h.Write(key) // feed key again to get a distinct second hash
    h2 = h.Sum64() | 1
    return h1, h2
}
```
**Issue:** This feeds the same `key` bytes twice into the same hasher instance to produce h2. The second `h.Write(key)` extends the internal state (it does not reset), so `h2 = FNV(key || key)`. For short keys (e.g., 3-byte service names), h1 and h2 are correlated because FNV is a simple polynomial hash -- `FNV(key || key)` is a deterministic function of `FNV(key)`. Kirsch-Mitzenmacher requires h1 and h2 to be independent.
**Fix:** Use `fnv.New64()` (non-"a" variant) for h2, or use a 128-bit hash and split. The current approach likely works acceptably for the cardinalities involved (low false-positive rates at 10 bits/item with k=7) but is technically unsound.

---

## 8. Minor Issues

### 8a. `synthesizeSpanEnd` does not populate `parsedIntrinsicCache`
**File:** `reader/intrinsic_reader.go:136-175`
**Issue:** `GetIntrinsicColumn` stores decoded columns in `parsedIntrinsicCache` for cross-Reader reuse. But `synthesizeSpanEnd` only stores in the per-Reader `intrinsicDecoded` map, not the process-level cache. Every new Reader for the same file will re-synthesize span:end from scratch.
**Fix:** Add `parsedIntrinsicCache.Store(r.fileID+"/intrinsic/span:end", col)` after synthesis.

### 8b. `layout.go` page TOC parsing duplicates `intrinsic_codec.go`
**File:** `reader/layout.go` (diff lines 482-497)
**Issue:** The `FileLayout` method manually parses the paged header (sentinel byte check, read tocLen, decode TOC) instead of calling through the codec's shared path.
**Fix:** Use a shared "is-paged + parse TOC" helper rather than duplicating the wire-format parsing inline.

---

## Summary

| Priority | Count | Category |
|----------|-------|----------|
| High     | 1     | Dead code (`intrinsicDictMatchRefs`) |
| High     | 1     | Unbounded process-level caches |
| Medium   | 2     | Duplicated sort logic, duplicated TOC preamble (4 copies) |
| Medium   | 1     | Bloom hash quality concern |
| Low      | 4     | Unused param, stringly-typed names, missing cache store, layout duplication |
