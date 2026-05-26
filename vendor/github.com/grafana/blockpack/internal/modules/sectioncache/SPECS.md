# SPECS: sectioncache

## SPEC-SC-001: SectionCache Interface Contract

`SectionCache` is the typed cache interface used internally by `Reader`. Each method
corresponds to a distinct section type, enabling independent routing, eviction budgets,
and storage tiers per section.

**GetOrFetch* methods:**
- On cache hit: return cached bytes, nil error.
- On cache miss: call `fetch()` and store the result if non-nil and no error.
- If `fetch()` returns `(nil, nil)`, treat as a miss — do NOT store.
- If `fetch()` returns a non-nil error, propagate the error; do NOT store.
- `err` is non-nil only if the underlying store fails independently of `fetch()`.

**Get methods (non-fetch probes):**
- Return `(data, true, nil)` on hit.
- Return `(nil, false, nil)` on miss.
- Return `(nil, false, err)` on store error.
- Never call a fetch function.

**Cache/Store methods:**
- Errors are non-fatal — callers may discard them.
- A cache write failure does not affect correctness; the next read will re-fetch.

**Nil safety:** A nil `SectionCache` is NOT safe; use `NopSectionCache` as a no-op stand-in.

Back-ref: `internal/modules/sectioncache/sectioncache.go:SectionCache`

---

## SPEC-SC-002: FilecacheAdapter Key Format (Authoritative)

`FilecacheAdapter` bridges any `filecache.Cache` to `SectionCache` by reconstructing
the legacy string key format. This table is the authoritative reference for all key formats:

| SectionCache method | Adapter key |
|---|---|
| `GetOrFetchFooter(fileID, "", ...)` | `fileID + "/footer"` |
| `GetOrFetchFooter(fileID, variant, ...)` | `fileID + "/footer" + variant` |
| `GetOrFetchHeader(fileID, ...)` | `fileID + "/header"` |
| `GetOrFetchV8TOC(fileID, ...)` | `fileID + "/v8/toc/dec"` |
| `GetOrFetchV8Section(fileID, t, s, name, ...)` | `fmt.Sprintf("%s\x00v8\x00%d\x00%d\x00%s", fileID, t, s, name)` |
| `GetOrFetchV14Section(fileID, secType, ...)` | `fmt.Sprintf("%s/v14/sec/%02x/dec", fileID, secType)` |
| `GetV14Section(fileID, secType)` | `fmt.Sprintf("%s/v14/sec/%02x/dec", fileID, secType)` |
| `GetOrFetchBloom(fileID, false, ...)` | `fileID + "/compact-header"` |
| `GetOrFetchBloom(fileID, true, ...)` | `fileID + "/v14/compact-header"` |
| `GetOrFetchMetadata(fileID, ...)` | `fileID + "/metadata/dec"` |
| `GetOrFetchTraceIndex(fileID, true, ...)` | `fileID + "/compact"` |
| `GetOrFetchTraceIndex(fileID, false, ...)` | `fileID + "/compact-trace-index"` |
| `GetBlockColumns(fileID, idx)` | `sectioncache.BlockColumnsKey(fileID, idx)` |
| `CacheBlockColumns(fileID, idx, data)` | `sectioncache.BlockColumnsKey(fileID, idx)` |
| `GetOrFetchIntrinsic(fileID, name, ...)` | `sectioncache.IntrinsicKey(fileID, name)` |

When adding new cache operations: add a typed method to `SectionCache`, implement it in
`FilecacheAdapter` and `TypedTieredCache`, and add a routing test in `typed_test.go`.

Back-ref: `internal/modules/sectioncache/sectioncache.go:FilecacheAdapter`

---

## SPEC-SC-003: NopSectionCache Null-Object Contract

`NopSectionCache` is a `SectionCache` that:
- On all `GetOrFetch*` calls: always calls `fetch()` and returns its result (no storage).
- On all `Get*` calls: always returns `(nil, false, nil)`.
- On all `Cache*`/`Close` calls: always returns `nil`.

Use `NopSectionCache` as a drop-in when no caching is desired. It satisfies all interface
invariants and never causes nil-pointer panics.

Back-ref: `internal/modules/sectioncache/sectioncache.go:NopSectionCache`
