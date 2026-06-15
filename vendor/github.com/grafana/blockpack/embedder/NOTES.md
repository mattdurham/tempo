# embedder (public subpackage) — Design Notes

## NOTE-370: Dedicated importable subpackage for the write-path embedder (2026-06-15)
*Added: 2026-06-15*

**Decision:** The HTTP embedder write-path constructor (`NewHTTPEmbedder`,
`HTTPConfig`, the `Embedder` type) lives in the dedicated importable subpackage
`github.com/grafana/blockpack/embedder` rather than in the top-level `blockpack`
package (where they were `NewHTTPEmbedder` / `EmbedderHTTPConfig` / `Embedder`).

**Rationale (issue #346):** The embedder was entangled in `storage.go`'s general
storage/query API surface, which is an implicit constraint on restructuring the
embedder internals. The only external consumer is tempo's write path
(`tempodb/encoding/vblockpack/embedder.go`).

The issue originally proposed having tempo import
`github.com/grafana/blockpack/internal/modules/embedder` directly. That is
**not possible**: Go's internal-package rule forbids a package outside the
`github.com/grafana/blockpack/...` import tree (tempo is
`github.com/grafana/tempo`) from importing any `.../internal/...` package, and
the `replace` directive changes only the source location, not the import path —
so the rule still applies. Verified empirically with a minimal two-module repro.

A thin, non-internal subpackage threads this needle: it removes the embedder
from the top-level `blockpack` surface (the goal) while remaining importable by
tempo. It re-exports only the minimal write-path constructor, delegating to
`internal/modules/embedder`.

**Consequence:**
- Top-level `blockpack` no longer exports `Embedder`, `EmbedderHTTPConfig`, or
  `NewHTTPEmbedder`. `storage.go` no longer imports `internal/modules/embedder`
  or `internal/vm`.
- `blockpack.QueryOptions.Embedder` (the query-path `vm.TextEmbedder` interface
  field for VECTOR_AI) is **unchanged** — it accepts any `vm.TextEmbedder`, and
  `*embedder.Embedder` satisfies it.
- The deadcode anchor for the public HTTP constructor moved from
  `blockpack.NewHTTPEmbedder` to `pubembedder.NewHTTPEmbedder` in
  `cmd/deadcode/main.go`. The internal-package anchors (`embedder.New`,
  `NewWithBackend`, `AssembleText`, `DefaultModelPath`, `NewHTTP`) are retained
  — they remain genuine public surface of the kept internal package.

**Back-refs:** `embedder/embedder.go`, `storage.go` (NOTE-370 comment),
`cmd/deadcode/main.go`, tempo `tempodb/encoding/vblockpack/embedder.go`.
