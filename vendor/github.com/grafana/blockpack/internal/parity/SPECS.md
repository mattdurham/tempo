# parity — Specification

This document defines the invariants and contracts for the `internal/parity` package.

---

## 1. Package Role

`internal/parity` is a CI-gated smoke test package. It exports no production code.
All logic lives in `smoke_test.go`.

See NOTE-001 for the rationale behind this package's existence.

---

## 2. What the Parity Tests Verify

### PSPEC-001: Intrinsic Column Round-Trip (PST-01)

Every intrinsic column captured by the blockpack writer must be readable back through
the blockpack reader with the correct value and type.

**Covered columns:**
- Trace: `trace:id`, `trace:state`
- Span: `span:id`, `span:parent_id`, `span:name`, `span:kind`, `span:start`, `span:end`,
  `span:duration`, `span:status`, `span:status_message`
- Schema URLs: `resource:schema_url`, `scope:schema_url`

**Invariant:** No intrinsic column listed here may be silently dropped or return an
incorrect value after a write-flush-read cycle.

Back-ref: `smoke_test.go:TestParitySmokeTest`

---

### PSPEC-002: Attribute Value Type Round-Trip (PST-01)

All five OTLP attribute value types must survive a write-flush-read cycle without loss
or coercion, across all three attribute namespaces (span, resource, scope).

| Type    | Column accessor   |
|---------|-------------------|
| string  | `StringValue(i)`  |
| int64   | `Int64Value(i)`   |
| float64 | `Float64Value(i)` |
| bool    | `BoolValue(i)`    |
| bytes   | `BytesValue(i)`   |

**Invariant:** Each column accessor must return `ok=true` and the original written value
for every covered attribute.

Back-ref: `smoke_test.go:TestParitySmokeTest`

---

### PSPEC-003: Single Flush Produces Single Block (PST-01)

Writing one span and flushing once must produce exactly one block.

**Invariant:** `reader.BlockCount() == 1` and `block.SpanCount() == 1` after a single
flush of a single span.

Back-ref: `smoke_test.go:TestParitySmokeTest`

---

### PSPEC-004: Public DataType Constants Non-Zero (PST-02)

All exported `blockpack.DataType` constants except `DataTypeUnknown` must have distinct
non-zero values. `DataTypeUnknown` is the zero value and is excluded from verification.

The verified constants are:

- `DataTypeFooter`
- `DataTypeHeader`
- `DataTypeMetadata`
- `DataTypeTraceBloomFilter`
- `DataTypeTimestampIndex`
- `DataTypeBlock`

(`DataTypeIndex` and `DataTypeCompact` were removed; they no longer exist in the public API.)

**Invariant:** Every constant in the verified list above is non-zero and distinct.

Back-ref: `smoke_test.go:TestDataTypeConstantsAreNonZero`

---

### PSPEC-005: ReaderProvider Interface Wiring (PST-03)

A struct satisfying `blockpack.ReaderProvider` must be passable to
`blockpack.NewReaderFromProvider` without panic. An empty provider must return a parse
error (not nil, not a panic).

**Invariant:** `NewReaderFromProvider(emptyProvider)` returns `(nil, non-nil error)`.

Back-ref: `smoke_test.go:TestReaderProviderInterfaceSatisfaction`
