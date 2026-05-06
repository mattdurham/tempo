# NOTES: sectioncache

## NOTE-SC-001: Why Typed Interface Over String-Key Classifier
*Added: 2026-05-05*

**Decision:** `SectionCache` exposes one method per section type rather than a single
`Get(key string)` method with a caller-supplied key string.

**Rationale:**
- Direct typed dispatch eliminates key parsing overhead on every cache call.
- Each method signature makes the section type explicit at the call site — no ambiguity
  about which key format to use.
- Typed routing enables independent eviction policies per section type without string parsing.
- `FilecacheAdapter` bridges existing `filecache.Cache` implementations by reconstructing
  the canonical legacy key string internally — backward compat without leaking key formats.

**Alternative rejected:** A `Classify(key string) SectionType` classifier function was
considered but rejected: it imposes O(len(key)) string scanning on every cache call and
introduces a second place where key format knowledge must be maintained in sync.

**Consequence:** Adding a new section type requires adding a typed method to `SectionCache`,
implementing it in `FilecacheAdapter` and `TypedTieredCache`, and updating `SPECS.md` with
the canonical key format.

Back-ref: `internal/modules/sectioncache/sectioncache.go:SectionCache`,
`internal/modules/sectioncache/sectioncache.go:FilecacheAdapter`

## NOTE-SC-002: BlockColumnsKey and IntrinsicKey Shared Helpers
*Added: 2026-05-05*

`keys.go` provides `BlockColumnsKey` and `BlockColumnsKeyFast` for block column keys, and
`IntrinsicKey` for intrinsic column keys. Both `FilecacheAdapter` and `TypedTieredCache`
use these helpers to guarantee identical key strings for the same inputs.

`BlockColumnsKeyFast` uses `strconv.Itoa` (faster than `fmt.Sprintf` on the hot path for
small integers). `BlockColumnsKey` uses `fmt.Sprintf` (used by `FilecacheAdapter` for
consistency with other key formats in that file). Both produce identical output.

Back-ref: `internal/modules/sectioncache/keys.go:BlockColumnsKey`,
`internal/modules/sectioncache/keys.go:BlockColumnsKeyFast`
