# embedder — Design Notes

## NOTE-001: Sequential-Per-Text Inference (Not True Batch) (2026-04-02)
*Added: 2026-04-02*

**Decision:** `EmbedBatch` processes each text via a separate `llama.BatchGetOne` call
in a sequential loop, not a single batched inference operation.

**Rationale:** The yzma binding's `llama.BatchGetOne` API accepts a single token sequence
per call. Multi-sequence batching (processing multiple texts simultaneously on GPU/CPU)
is not exposed by the current yzma API. Implementing true batch inference would require
either changes to the yzma bindings or a different embedding backend.

**Consequence:** Throughput is proportional to the number of texts, not batch-optimized.
Callers expecting GPU batch utilization should note that this function provides no speedup
over calling `Embed` in a loop. The function is named `EmbedBatch` for API consistency;
the name does not imply parallel inference.

**TODO:** If yzma adds multi-sequence batched inference support, update `EmbedBatch` to
use it and document the throughput improvement here.

**Back-ref:** `internal/modules/embedder/embedder.go:EmbedBatch`

---

## NOTE-002: L2 Normalization on Every Output Vector (2026-04-02)
*Added: 2026-04-02*

**Decision:** All vectors returned by `embedOne` are L2-normalized before returning.

**Rationale:** Cosine similarity (used in `vectormath.CosineDistance`) is equivalent to
dot product for unit vectors. Pre-normalizing at embed time means the writer stores unit
vectors, and the reader can compute cosine similarity via dot product directly — avoiding
per-query normalization of stored vectors during the PQ reranking phase.

**Consequence:** Stored embeddings are always unit vectors. Any downstream code that
treats embeddings as unnormalized (e.g., magnitude-weighted scoring) will produce
incorrect results.

**Back-ref:** `internal/modules/embedder/embedder.go:embedOne`,
`internal/modules/embedder/embedder.go:l2Normalize`

---

## NOTE-003: AssembleText Field Ordering (2026-04-02)
*Added: 2026-04-02*

**Decision:** `AssembleText` emits fields in primary → context → secondary order.
Primary fields contribute plain text; context and secondary fields contribute `key=value` pairs.

**Rationale:** Embedding models (e.g., nomic-embed-text-v1.5) attend most strongly to
the beginning of the input sequence. Placing the most semantically discriminating fields
(span operation name, service name) first maximizes their influence on the embedding.
Context and secondary fields add disambiguation without dominating the representation.

**Consequence:** Changing `Fields` order in `Config` changes the embedding output.
Writers and readers must use the same `Config.Fields` to produce comparable embeddings.
Stored embeddings are not forward-compatible if field ordering changes.

**Back-ref:** `internal/modules/embedder/embedder.go:AssembleText`

---

## NOTE-004: Auto-Download Uses Temp-File-Then-Rename for Atomicity (2026-04-02)
*Added: 2026-04-02*

**Decision:** `EnsureModel` writes to a `.tmp-embedder-model-*` file in the same directory
as the target model, then performs `os.Rename` to atomically install it. The temp file is
removed on any error path via `defer`.

**Rationale:** GGUF models are large (hundreds of MB to several GB). A crash mid-download
would leave a partial file that looks like a valid path to `New`, causing a confusing
"invalid model" error on the next run. Writing to a temp file in the same directory ensures:
(a) the rename is on the same filesystem and thus atomic at the OS level, (b) a partial
download never appears at the canonical path, and (c) cleanup on error leaves no stale file.

**Consequence:** The temp file and the final path must reside on the same filesystem (same
directory). This is guaranteed by using `filepath.Dir(cfg.ModelPath)` as the temp directory.
Cross-filesystem renames would fail — callers must not set `ModelPath` to a path on a
different mount than the temp directory.

**Back-ref:** `internal/modules/embedder/download.go:EnsureModel`

---

## NOTE-005: No Context Propagation in EnsureModel (2026-04-02)
*Added: 2026-04-02*

**Decision:** `EnsureModel` uses the default `http.Get` without a context parameter.

**Rationale:** Model download is a one-shot startup operation expected to run to completion.
Adding context would require changing the `EnsureModel` signature, complicating callers
that invoke it at init time (e.g., from `New`). The `noctx` linter suppression comment is
present to document this deliberate choice.

**Consequence:** Callers that need download cancellation must wrap `EnsureModel` in a
goroutine and implement their own timeout/cancellation logic externally. This is an acceptable
trade-off for the simplicity of the current API.

**Back-ref:** `internal/modules/embedder/download.go:EnsureModel`

---

## NOTE-006: Backend Interface Decouples CGo from Call Sites (2026-04-02)
*Added: 2026-04-02*

**Decision:** The `embedder` package defines a `Backend` interface (`Embed`, `EmbedBatch`, `Dim`, `Close`) that all embedding implementations satisfy. The `Embedder` struct holds a `Backend` field instead of directly referencing `llama.Model`/`llama.Context`.

**Rationale:** The yzma backend requires CGo and a native shared library, making it unsuitable for environments where CGo is disabled (e.g., cross-compilation, containers without libllama.so). Abstracting behind `Backend` allows `httpBackend` (pure Go, no CGo) and `MockBackend` (pure Go, test-only) to be used in those environments. The public API (`New`, `NewHTTP`, `NewWithBackend`, `Embed`, `EmbedBatch`, `Dim`, `Close`, `AssembleText`) is unchanged.

**Consequence:** Any new embedding backend must implement `Backend`. Adding CGo-free backends (e.g., ONNX via a CGo-free Go package) requires no changes to `Embedder` itself — only a new `Backend` implementation.

**Back-ref:** `internal/modules/embedder/backend.go:Backend`, `internal/modules/embedder/embedder.go:Embedder`

---

## NOTE-007: HTTP Backend Probes Server on Construction (2026-04-02)
*Added: 2026-04-02*

**Decision:** `NewHTTP` sends an empty-batch POST /embed probe at construction time to determine `Dim()` from the server's response.

**Rationale:** The HTTP backend has no local model to query for the embedding dimension. Rather than requiring callers to supply `dim` in `HTTPConfig`, probing the server at init time is simpler and fails fast if the server is unreachable. An empty batch returns `dim` with no inference cost.

**Consequence:** `NewHTTP` returns an error if the server is unreachable at construction time — fail-fast is intentional. Callers that need lazy connection must wrap `NewHTTP` in a retry loop.

**Back-ref:** `internal/modules/embedder/backend_http.go:NewHTTP`

---

## NOTE-008: MockBackend Uses FNV-1a Hash Seed for Determinism (2026-04-02)
*Added: 2026-04-02*

**Decision:** `MockBackend.Embed` seeds a PCG RNG from the FNV-1a hash of the input text, producing deterministic (text → vector) mapping within a process.

**Rationale:** Tests benefit from deterministic output: the same text always produces the same vector, making assertions on vector equality and "different texts produce different vectors" reliable. Using a hash seed avoids global state (no shared seed counter) and makes `MockBackend` naturally thread-safe.

**Consequence:** Vectors change if the hash function or RNG changes. The current implementation is not intended for persistence — stored mock vectors will not match future versions of `MockBackend`.

**Back-ref:** `internal/modules/embedder/mock_backend.go:hashText`, `internal/modules/embedder/mock_backend.go:MockBackend.Embed`
