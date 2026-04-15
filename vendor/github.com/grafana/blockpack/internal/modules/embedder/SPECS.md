# embedder — Specifications

## SPEC-001: Package Scope and Invariants
*Added: 2026-04-02*

`embedder` wraps yzma (llama.cpp Go bindings) for local embedding model inference.
It converts span/log text fields into L2-normalized float32 embedding vectors using a GGUF model.

**Invariants:**
- `Embedder` is NOT thread-safe — callers must serialize all method calls.
- `New` loads the llama.cpp shared library and GGUF model; it must be called once per
  `Embedder` instance. Multiple `Embedder` instances share the underlying llama.cpp global
  state — creating multiple instances in the same process is not tested and may not be safe.
- `Close` must be called to free model and context resources.
- All returned vectors are L2-normalized (unit vectors in float32).

---

## SPEC-002: Config
*Added: 2026-04-02*

```go
type Config struct {
    LibPath       string           // absolute path to llama.cpp shared library
    ModelPath     string           // absolute path to GGUF model file
    Fields        []EmbeddingField // fields to include in embedding text; ordered
    BatchSize     int              // tokens per inference batch (default: 512)
    MaxTextLength int              // max chars of assembled text (default: 2048)
}
```

**Invariants:**
- `LibPath` and `ModelPath` must be non-empty absolute paths.
- `BatchSize` and `MaxTextLength` default to 512 and 2048 respectively if <= 0.

---

## SPEC-003: EmbedBatch Behavior
*Added: 2026-04-02*

```go
func (e *Embedder) EmbedBatch(texts []string) ([][]float32, error)
```

**Behavior:** Processes each text sequentially — one llama.cpp inference call per text.
This is NOT a batched GPU/CPU inference operation; texts are processed one at a time.

**Invariants:**
- Returns exactly `len(texts)` vectors on success, one per input text, in input order.
- On error for any text, returns `nil, err` (no partial results).
- Empty token sequences (empty text) return a zero vector of dimension `nEmbd` (not an error).
- All output vectors are L2-normalized.

---

## SPEC-004: AssembleText Semantics
*Added: 2026-04-02*

```go
func (e *Embedder) AssembleText(fields map[string]string) string
```

Assembles a single string from span fields for embedding input, using the `Fields` ordering
from `Config`:

1. **primary** fields: value only (plain text), joined by space.
2. **context** fields: `"key=value"` pairs, appended after primary.
3. **secondary** fields: `"key=value"` pairs, appended last.

**Invariant:** The assembled text is truncated to `MaxTextLength` characters (rune-safe
truncation). If a field name is not present in the input map, it is skipped silently.

---

## SPEC-005: Dim
*Added: 2026-04-02*

```go
func (e *Embedder) Dim() int
```

Returns the embedding dimension of the loaded model (the `nEmbd` value from llama.cpp).
This value must match `Config.VectorDimension` in the blockpack writer to produce compatible
embedding vectors.

---

## SPEC-006: Auto-Download Config Fields
*Added: 2026-04-02*

```go
type Config struct {
    // ...existing fields...
    AutoDownload bool   // enable automatic model download on first use (default: true when ModelPath is empty)
    ModelURL     string // URL to download from (default: DefaultModelURL)
}
```

**Invariants:**
- `ModelPath` may be empty; when empty, `New` substitutes `DefaultModelPath()` and sets `AutoDownload = true`.
- `ModelURL` may be empty; when empty, `EnsureModel` substitutes `DefaultModelURL`.
- `AutoDownload` is ignored if `ModelPath` is non-empty and the file already exists.

---

## SPEC-007: DefaultModelURL and DefaultModelPath
*Added: 2026-04-02*

```go
const DefaultModelURL = "https://huggingface.co/nomic-ai/nomic-embed-text-v1.5-GGUF/resolve/main/nomic-embed-text-v1.5.Q8_0.gguf"

func DefaultModelPath() string
```

**Invariants:**
- `DefaultModelURL` is the canonical HuggingFace URL for the nomic-embed-text-v1.5 Q8_0 GGUF model.
- `DefaultModelPath()` returns an absolute path ending in `nomic-embed-text-v1.5.Q8_0.gguf`
  under `~/.blockpack/models/`. The home directory is expanded at call time via `os.UserHomeDir`.
- If `os.UserHomeDir` fails, the path is returned relative to the current working directory
  (fallback only; not expected in practice).

Back-ref: `internal/modules/embedder/download.go:DefaultModelPath`, `internal/modules/embedder/download.go:DefaultModelURL`

---

## SPEC-008: EnsureModel
*Added: 2026-04-02*

```go
func EnsureModel(cfg Config) error
```

Ensures the GGUF model file exists at `cfg.ModelPath`, downloading it if necessary.

**Behavior:**
1. If `cfg.ModelPath` exists on disk (via `os.Stat`), returns `nil` immediately — no network call.
2. If the file does not exist, downloads from `cfg.ModelURL` (or `DefaultModelURL` if empty).
3. Creates parent directories with `os.MkdirAll` before writing.
4. Writes to a temp file (same directory, `.tmp-embedder-model-*` prefix) then atomically renames
   to `cfg.ModelPath` — no partial file is ever visible at the final path.
5. Logs download start, model size (if known from `Content-Length`), and bytes transferred.

**Error conditions:**
- Returns error if the HTTP response status is not 200.
- Returns error if any filesystem operation (MkdirAll, CreateTemp, Rename) fails.
- Returns error if the network request itself fails.
- On any error, the temp file is removed; `cfg.ModelPath` is never created.

**Invariants:**
- Idempotent: calling `EnsureModel` when the file already exists is a no-op (returns nil).
- Atomic: `cfg.ModelPath` is either absent or complete — never a partial write.
- Uses `net/http` default client; no custom timeout is applied (model files are large; callers
  that need timeout control should wrap the call in a goroutine with a context).

Back-ref: `internal/modules/embedder/download.go:EnsureModel`

---

## SPEC-009: Backend Interface
*Added: 2026-04-02*

```go
type Backend interface {
    Embed(text string) ([]float32, error)
    EmbedBatch(texts []string) ([][]float32, error)
    Dim() int
    Close()
}
```

**Invariants:**
- `Embed` and `EmbedBatch` return L2-normalized float32 vectors.
- `Dim` returns the embedding dimension (constant for the lifetime of the backend).
- `Close` releases resources; calling it more than once must not panic.
- Implementations need not be thread-safe unless documented otherwise.
  `MockBackend` is thread-safe; `httpBackend` is safe for concurrent use (stateless
  after construction; `EmbedBatch` uses per-call goroutines with no shared mutable state).

Back-ref: `internal/modules/embedder/backend.go:Backend`

---

## SPEC-010: HTTPConfig and NewHTTP
*Added: 2026-04-02*
*Updated: 2026-04-14*

```go
type HTTPConfig struct {
    ServerURL            string
    Fields               []EmbeddingField
    MaxTextLength        int
    Timeout              time.Duration // defaults to 120s
    MaxBatchSize         int           // defaults to 32 (TEI per-request limit)
    MaxConcurrentBatches int           // defaults to 4 (parallel pod distribution)
}

func NewHTTP(cfg HTTPConfig) (*Embedder, error)
```

**Behavior:**
- `NewHTTP` sends a probe request (`POST /embed` with `["probe"]`) via `doPost` (single
  attempt, no retry loop) to determine `Dim()`.
- Returns error if the server is unreachable, returns a non-200 status, or returns an
  empty vector response.

**HTTP protocol (TEI — HuggingFace Text Embeddings Inference):**
- Request: `POST <ServerURL>/embed` with JSON body `{"inputs": ["t1", "t2", ...], "normalize": true}`
- Response: `[[float32, ...], ...]` — flat JSON array of float32 vectors (one per input text)

**Invariants:**
- `Dim()` returns `len(response[0])` from the probe response.
- `EmbedBatch` chunks large batches into sub-batches of size `MaxBatchSize` and sends
  up to `MaxConcurrentBatches` chunks in parallel.
- `EmbedBatch` returns error if any chunk request fails or if `len(response) != len(chunk)`.
- `Close` is a no-op (no persistent connection is held; keep-alives are disabled).
- `HTTPConfig.Timeout` defaults to 120s if zero.
- `HTTPConfig.MaxBatchSize` defaults to 32 if zero.
- `HTTPConfig.MaxConcurrentBatches` defaults to 4 if zero.

Back-ref: `internal/modules/embedder/backend_http.go:NewHTTP`

---

## SPEC-011: MockBackend
*Added: 2026-04-02*

```go
func NewMockBackend(dim int) *MockBackend
```

**Behavior:** Returns random L2-normalized float32 vectors seeded by FNV-1a hash of the input text.
Same text always produces the same vector within a process (deterministic).

**Invariants:**
- Thread-safe: `Embed` and `EmbedBatch` may be called concurrently.
- `dim <= 0` defaults to 768.
- `Close` is a no-op.

Back-ref: `internal/modules/embedder/mock_backend.go:MockBackend`

---

## SPEC-012: NewWithBackend
*Added: 2026-04-02*

```go
func NewWithBackend(b Backend, fields []EmbeddingField, maxTextLength int) *Embedder
```

Creates an `Embedder` with a caller-supplied `Backend`. `maxTextLength` defaults to 2048 if <= 0.
Useful for injecting `MockBackend` or custom backends in tests.

Back-ref: `internal/modules/embedder/embedder.go:NewWithBackend`
