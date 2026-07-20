// Package pgcatalog implements blockpack_file_catalog, the shared
// Postgres-backed catalog of physical objects (files) for the VI, VCNT, and
// cube subsystems (issue #522). Each subsystem's compactor writes one row per
// physical object it produces or supersedes, discriminated by Subsystem;
// job-planner (tempo repo) queries this catalog to select compaction
// candidates, and a reaper (also tempo repo) physically deletes objects whose
// catalog row has been Subsystem-agnostically marked compacted for longer than
// a grace window.
//
// This package owns only the catalog rows themselves. It never touches
// object storage directly -- writing/deleting the underlying objects is
// always the caller's responsibility, in a fixed order (write object, Insert
// its row, MarkCompacted its inputs, only then report success -- see
// plan.md Section E) that keeps a mid-sequence crash recoverable rather than
// silently lossy.
//
// See NOTES.md for design rationale.
package pgcatalog
