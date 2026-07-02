# Task: Fix value-index write path + wire VCNT and cubes into tempo

## Working directory

/home/mdurham/source/blockpack_collection/tempo

## Related repos

- blockpack source: /home/mdurham/source/blockpack_collection/blockpack
- blockpack vendor in tempo: /home/mdurham/source/blockpack_collection/tempo/vendor/github.com/grafana/blockpack

## Problems to solve

### 1. Value-index L0 write path stopped silently (~4 hours ago)

- Blocks are flushing fine (block-builder logs show `Flushed block` every ~90s)
- Index files stopped being written at 07:15 UTC (last: `L0-1782904397-...`)
- No error logs — `WriteValueIndexL0` is being silently skipped
- Root cause: in `create.go`, `NewReaderFromProvider(&fileReaderProvider{f: tmp})` likely
  returns an error for the v2 lean-format blocks (new format since `76b7477b`), so
  `rerr != nil` → write is skipped silently
- The blockpack format changed: `76b7477b` added BucketGroup consumer write path.
  Need to verify `NewReaderFromProvider` still works on freshly-written temp files.
- Fix: add error logging when `NewReaderFromProvider` fails so the issue is visible;
  investigate if the v2 format requires a different code path for extraction.

### 2. VCNT (value count index) — not wired into tempo at all

- `internal/modules/valuecounts/` package exists in blockpack with a full VCNT section writer
- VCNT writes signed per-column per-minute span counts for tag autocomplete
- The block-builder should accumulate VCNT counts per minute and flush alongside blocks
- No tempo-side wiring exists yet
- See blockpack `internal/modules/valuecounts/` for the section format
- See `ToCSubTypeValueCounts` in blockpack constants

### 3. Cubes — not wired into tempo at all

- `internal/modules/cube/` package exists in blockpack with full cube accumulator,
  registry, trigger, backfill, rollup, compactor
- Cubes are pre-aggregated per-minute span counters for metrics acceleration
- The block-builder should use `cube.Accumulator` per active cube and flush per minute
- The `cube.CreationTrigger` + `cube.Registry` handle first-query creation
- No tempo-side wiring exists yet

## Key files in tempo to examine

- `tempodb/encoding/vblockpack/create.go` — value-index write path (broken)
- `tempodb/encoding/vblockpack/valueindex.go` — ConfigureValueIndex singleton
- `tempodb/encoding/vblockpack/compactor.go` — compaction value-index write
- `tempodb/tempodb.go` — startup wiring
- `tempodb/encoding/common/config.go` — BlockpackConfig struct

## Key files in blockpack to examine

- `valueindex_l0write.go` — WriteValueIndexL0 public API
- `internal/modules/valuecounts/` — VCNT writer
- `internal/modules/cube/accumulator.go` — Cube ingest accumulator
- `internal/modules/cube/registry.go` — Cube definition registry
- `internal/modules/cube/trigger.go` — First-query creation trigger

## Constraints

- Do NOT run benchmarks or connect to tempo-dev-test-03 to run queries
- Deploy after implementing (bash /home/mdurham/source/blockpack_collection/deploy.sh)
- Verify deployment: kubectl logs block-builder-0 -n tempo-dev-test-03 | grep "Flushed block"
- All changes must pass: go build ./... and go test ./tempodb/encoding/vblockpack/...
- make precommit must be green in blockpack if blockpack changes are needed
- Push commits immediately; update tempo vendor if blockpack changes
