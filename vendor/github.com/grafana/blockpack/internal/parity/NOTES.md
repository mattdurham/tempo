# parity — Design Notes

This document captures the non-obvious design decisions, rationale, and invariants for
the `internal/parity` package.

---

## NOTE-001: Package Role — CI Smoke Test for Field Coverage
*Added: 2026-03-05*

**Decision:** `internal/parity` exists solely as a CI-gated smoke test package. It does
not export any production code — `doc.go` contains only the package declaration and
documentation comment.

**Rationale:** The writer and reader have many independent unit tests, but no single test
previously verified the full round-trip for every intrinsic column and every attribute
value type in one place. As new columns were added (e.g. `scope:schema_url`,
`trace:state`), they were occasionally missed in existing tests. The parity package
provides a single authoritative check that no column is silently dropped.

**CI integration:** `TestParitySmokeTest` runs in both `make test` (via `$(PACKAGES)`)
and `make ci` as a dedicated step.

---

## NOTE-002: PST- Test ID Prefix
*Added: 2026-03-05*

**Decision:** Tests in this package use the `PST-` (Parity Smoke Test) prefix for
structured test IDs, following the same tagging convention as other modules
(SPEC-, NOTE-, TEST-, BENCH-).

**Rationale:** The `PST-` prefix distinguishes parity smoke tests from integration test
IDs (`EX-`, `BENCH-W-`, etc.) in other packages, making cross-references unambiguous.

---

## NOTE-003: Public API Surface Checks Live Here
*Added: 2026-03-05*

**Decision:** Compile-time interface satisfaction checks for the public `blockpack`
package (`smokeReaderProvider` implements `blockpack.ReaderProvider`) and runtime checks
that `DataType` constants are non-empty (PST-02, PST-03) live in `parity/smoke_test.go`,
not in `api_test.go` or an internal package test.

**Rationale:** The parity package imports both the public `blockpack` package and
`internal/modules/blockio` internals. This makes it the natural location for end-to-end
checks that span the public/internal boundary. Placing these checks here avoids import
cycles and keeps the public `blockpack` package's test file focused on behavioral tests.

---

## NOTE-004: Assertions Extended as Reader Support Grows
*Added: 2026-03-05*

**Decision:** PST-01 includes a comment noting that "assertions are extended as reader
support for additional columns (event:name, link:trace_id) is implemented."

**Rationale:** Some OTLP fields (span events, span links) are not yet captured as block
columns. When support is added to the writer and reader, PST-01 must be extended to
cover those columns. The comment serves as a TODO anchor so the extension is not
forgotten.
