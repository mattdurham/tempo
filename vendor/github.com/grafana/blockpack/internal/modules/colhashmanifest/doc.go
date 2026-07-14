// Package colhashmanifest implements a best-effort, tenant-scoped audit manifest
// mapping a value-index/value-counts column hash (valueindex.ColHash / valuecounts.ColHash
// — the same SHA-256[:16] hash, computed independently by each package, NOTE-VI-106) back to
// its human-readable column name.
//
// Both the value index (VI) and value counts (VCNT) pipelines key their on-disk file layout
// by colHash alone, with no metadata file anywhere recording which column name produced a
// given hash — an operational blind spot: browsing storage directly shows only opaque
// hash-named directories. This package closes that gap with a single, additive, per-tenant
// manifest file (mirroring internal/modules/cube's Registry / internal/modules/viusage's
// Registry pattern) that VI and VCNT write paths update, at most once per (tenant, colHash,
// source), the first time they observe a colHash.
//
// This package is intentionally NOT imported by internal/modules/valueindex or
// internal/modules/valuecounts themselves — those two packages deliberately keep their
// ColHash implementations independent/uncoupled (see valuecountscompactor's
// ownsShard doc comment), and this manifest is orthogonal infrastructure, not indexing
// logic. Callers (internal/modules/valueindexconsumer, internal/modules/valuecountscompactor)
// already compute (tenant, colHash, colName) at their own write boundaries and pass them in
// directly, so this package never needs to know about either ColHash function.
//
// Never consulted by any read/write/query path — see NOTES.md NOTE-COLMANIFEST-1.
//
// See SPECS.md for the wire format and contract, NOTES.md for design rationale.
package colhashmanifest
