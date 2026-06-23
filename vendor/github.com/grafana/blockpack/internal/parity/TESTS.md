# parity — Test Plan

This document enumerates all tests in the `internal/parity` package and their coverage goals.
All tests are CI-gated (run in `make test` and `make ci`).

---

## PST-01: TestParitySmokeTest

*Status: implemented — `smoke_test.go:TestParitySmokeTest`*

**Purpose:** End-to-end field-coverage check. Writes a single OTLP span covering all
intrinsic fields captured as block columns and all five attribute value types (string,
int64, float64, bool, bytes) across span, resource, and scope namespaces. Reads back via
the blockpack reader and asserts every column value matches the written input.

**Intrinsic columns verified:**
- `trace:id`, `span:id`, `span:parent_id`
- `span:name`, `span:kind`
- `span:start`, `span:end`, `span:duration`
- `span:status`, `span:status_message`
- `trace:state`
- `resource:schema_url`, `scope:schema_url`

**Attribute columns verified (all five value types per namespace):**
- Span: `span.parity.str`, `span.parity.int`, `span.parity.float`, `span.parity.bool`, `span.parity.bytes`
- Resource: `resource.service.name`, `resource.res.int`, `resource.res.float`, `resource.res.bool`, `resource.res.bytes`
- Scope: `scope.version`, `scope.weight`, `scope.ratio`, `scope.enabled`, `scope.sig`

**Invariants checked:** One flush produces exactly one block; block contains exactly one span.

---

## PST-02: TestDataTypeConstantsAreNonEmpty

*Status: implemented — `smoke_test.go:TestDataTypeConstantsAreNonEmpty`*

**Purpose:** Verifies that all public `DataType` constants exported from the `blockpack`
package resolve to non-empty string values. A zero-value constant would indicate a broken
alias after the modules/rw extraction migration.

**Constants verified:** `DataTypeFooter`, `DataTypeHeader`, `DataTypeMetadata`,
`DataTypeBlock`, `DataTypeIndex`, `DataTypeCompact`.

---

## PST-03: TestReaderProviderInterfaceSatisfaction

*Status: implemented — `smoke_test.go:TestReaderProviderInterfaceSatisfaction`*

**Purpose:** Verifies that a struct implementing `blockpack.ReaderProvider` compiles and
can be passed to `blockpack.NewReaderFromProvider`. The empty provider triggers a format
parse error (not a panic or type assertion failure), confirming the public interface
wiring is correct after the migration.

**Pass condition:** `NewReaderFromProvider` returns a non-nil error; no panic occurs.

---

## Coverage Goals

| Goal | Target |
|------|--------|
| Overall package coverage | N/A (test-only package) |
| All intrinsic columns covered by PST-01 | 100% |
| All five attribute value types per namespace | 100% |
| Public API aliases validated | PST-02 + PST-03 |
