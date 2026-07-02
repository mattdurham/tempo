# parquetconv — Design Notes

This document captures the non-obvious design decisions, rationale, and invariants for the
`internal/parquetconv` package.

---

## NOTE-001: Import-Cycle Constraint — Must NOT Be Imported by api.go
*Added: 2026-03-05*

**Decision:** `internal/parquetconv` imports `github.com/grafana/tempo/tempodb/encoding`.
This package **must not** be imported (directly or transitively) by `api.go` or any package
in the blockpack public API surface.

**Rationale:** Tempo (`github.com/grafana/tempo`) imports `github.com/grafana/blockpack`
as a dependency. If `blockpack`'s public API imported `tempo/tempodb/encoding`, this would
create a dependency cycle:

```
blockpack → tempo/tempodb/encoding → blockpack
```

This cycle would make both modules uncompilable as a combined dependency graph.

**Consequence:**
- `ConvertFromParquetBlock` and `WriteParquet`/`WriteTempoBlock` are only reachable via
  CLI tools (`cmd/`) and benchmarks (`benchmark/`), never via the public API.
- Any new functionality that depends on Tempo imports must remain in `internal/parquetconv`
  or a sibling `internal/` package, not be promoted to `api.go`.
- CI should be monitored for import-cycle violations (run `go build ./...` to detect them).

Back-ref: `internal/parquetconv/from.go:1` (package comment)

---

## NOTE-002: gogo→google Protobuf Wire-Format Bridge in tempoTraceToOTLP
*Added: 2026-03-05*

**Decision:** `tempoTraceToOTLP` converts `*tempopb.Trace` (Tempo's gogo-protobuf type) to
`*tracev1.TracesData` (OTLP google-protobuf v2 type) by marshaling with `github.com/gogo/protobuf/proto`
and then unmarshaling with `google.golang.org/protobuf/proto`.

**Rationale:** Tempo's internal trace representation (`tempopb.Trace`) is generated with
`github.com/gogo/protobuf`, which is a deprecated fork of the official protobuf library
(staticcheck `SA1019`). OTLP's Go bindings use `google.golang.org/protobuf` (the official
v2 API). These two libraries generate incompatible Go types — you cannot cast or assign
between them, even though they share the same protobuf wire format.

The wire-format bridge (marshal → unmarshal) is the correct approach here because:
1. The gogo and google proto libraries share the same binary wire format (proto3).
2. No data loss occurs as long as the field numbers match (they do for OTLP trace protos).
3. No reflection or unsafe casts are needed.

**The `//nolint:staticcheck` suppression** on the `util.HexStringToTraceID` call is
separate — that function uses a deprecated gogo API but is the only way to correctly
handle Tempo's trace IDs, which may have leading zeros stripped. Replacing it with a
standard hex decoder would silently corrupt short trace IDs.

**Alternative considered:** Direct field-by-field copy between `tempopb` and OTLP structs.
Rejected because it would need updating for every new OTLP field and is fragile across
Tempo version bumps.

Back-ref: `internal/parquetconv/from.go:tempoTraceToOTLP`
