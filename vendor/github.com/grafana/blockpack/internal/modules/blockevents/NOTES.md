# blockevents package notes

## NOTE-VI-015 — Queue-agnostic block-event publisher (issue #397)

Date: 2026-06-25

The value index pipeline (issues #397/#398/#399) needs to know when a blockpack
file is created so a downstream consumer can index it. This package is the
foundation: a minimal `Publisher` interface plus three implementations.

### Design decisions

- **Minimal message.** `Message{Action, Path}` only. The consumer reads tenant,
  columns and entries from the blockpack file itself. Keeping the message tiny
  means a queue backend swap (SQS/NATS/Kafka) touches only the publisher.

- **Create-only.** No delete events. Retention-deleted blocks are handled
  organically by the value index compactor's source-existence check (issue #399),
  so emitting deletes would be redundant coordination.

- **Non-blocking hot path.** `Publish` enqueues onto a bounded buffered channel
  and returns immediately; a background goroutine performs the Redis `XADD`. If
  the buffer is full the message is **dropped** (counted by `Dropped()`), never
  blocking block creation/compaction. A lost create event is recoverable — the
  value index can rebuild from scratch. This is the explicit trade-off the issue
  calls for: availability of the write path over guaranteed event delivery.

- **XADD errors are swallowed** in the drain goroutine for the same reason: a
  failed publish must not stall or fail the block write.

- **`MaxLen` with `Approx`** (`MAXLEN ~ N`) caps the Redis stream so it cannot
  grow unbounded; approximate trimming is cheap. `MaxLen < 0` disables trimming.

- **Disabled by default.** `Config.Enabled=false` → `NewPublisher` returns a
  `NoopPublisher` (zero allocation, no goroutine, no I/O). Block creation pays
  nothing unless the operator opts in.

### Testability

`RedisStreamsPublisher` depends on a tiny unexported `streamWriter` interface
(`XAdd`, `Close`) rather than `*redis.Client` directly. Tests inject a fake (and
a deliberately-blocking fake to exercise the drop-when-full path), so the package
has full unit coverage with **no real Redis and no miniredis dependency**.

### Public surface

Go's internal-package rule blocks tempo from importing
`internal/modules/blockevents`. The importable surface is the thin re-export
package `github.com/grafana/blockpack/blockevents` (type aliases + constructor
wrappers), mirroring the `embedder` package pattern (NOTE-370). Anchored in
`cmd/deadcode/main.go`.

### Wiring (tempo)

- `vblockpack.ConfigureBlockEvents` installs a process-level publisher once at
  startup (mirrors the `configuredEmbedURL`/`processEmbedder` singleton pattern).
  Default is a `NoopPublisher`.
- `CreateBlock` publishes a create event after `WriteBlockMeta` succeeds.
- `Compactor.Compact` publishes a create event per output block after
  `CompactBlocksStreaming` succeeds.
- Event `Path` is the backend object key `<tenant>/<block-id>/data.blockpack` —
  the stable identifier the consumer uses to open the block.

Back-ref: `internal/modules/blockevents/publisher.go`,
`internal/modules/blockevents/redis.go`,
`internal/modules/blockevents/chan.go`,
`internal/modules/blockevents/noop.go`,
`blockevents/blockevents.go`,
`cmd/deadcode/main.go`
