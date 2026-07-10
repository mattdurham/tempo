// Package viusage implements the usage-registry and repeated-use trigger for VI's
// query-usage-driven backfill (blockpack/#496): tracking distinct-query use of
// non-dedicated columns per (tenant, column-hash, column-type), evaluating a rolling-
// window repeated-use threshold, and coordinating exclusive backfill execution via a
// TTL-leased lock in the same registry entry.
//
// NOTE: Own registry/types (R1) — does NOT reuse internal/modules/cube's Registry or
// RegistryEntry; the conditional-PUT-with-retry ObjectStore pattern is copied as a
// design pattern, independently implemented, per the team-lead ruling in
// .bob/state/plan.md Section 0 (R1).
//
// See SPECS.md for wire formats, NOTES.md for design decisions, TESTS.md for test plan.
package viusage
