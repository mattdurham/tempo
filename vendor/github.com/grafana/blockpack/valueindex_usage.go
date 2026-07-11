package blockpack

// valueindex_usage.go — public glue for #496's usage-registry/backfill-trigger module
// (internal/modules/viusage). Mirrors valueindex_query.go's re-export style (Lister,
// LookupStore, IndexFileCache, ColumnWatermark): viusage is an internal package, so
// external consumers — specifically tempo's B1 usage-recording hook, B2 backfill-job
// launcher, and B3 config wiring (plan.md Part B) — cannot import it directly (Go's
// internal-package rule is enforced by import path, not by module vendoring; tempo's
// own package paths never share the github.com/grafana/blockpack/ prefix). This file
// exports exactly the surface Part B needs to call from tempo: registering a use and
// evaluating the repeated-use trigger (RecordUseAndMaybeTrigger/
// MaybeRecordUseAndMaybeTrigger) and constructing a Registry against tempo's own
// ObjectStore implementation. It deliberately does NOT export viusage's fully internal
// helpers (e.g. the private conditional-PUT retry loop) that no external caller has any
// business calling directly.
//
// BackfillEngine/BackfillConfig/BlockFetcher/BackfillProgress are NOT re-exported here —
// they are root-NATIVE types (valueindex_backfill.go), not viusage types, because
// BackfillEngine itself needs *Reader/ObjectPutter/ExtractValueIndexEntriesForColumns
// directly. Keeping BackfillEngine in viusage while ALSO re-exporting viusage's other
// types back out from root would create a real import cycle (root -> viusage -> root,
// since viusage would import root for BackfillEngine's own needs) — viusage must remain
// a leaf package with respect to this one (R1) for ANY of this file to be possible at
// all, mirroring exactly why ColumnPolicy (valueindex_policy.go) already lives here
// instead of in viusage.

import (
	"context"
	"time"

	"github.com/grafana/blockpack/internal/modules/valueindex"
	"github.com/grafana/blockpack/internal/modules/viusage"
)

// ColTypeName returns the short, human-readable bucket name for a column type (e.g.
// "string", "int64") — the exact colType string form #496's usage registry keys
// entries by (Tenant, ColumnHash, ColumnType, plan.md Section 4.1). Re-exported here so
// external callers (tempo's B1 usage-recording hook, converting a compiled query's
// resolved LeafColumnInfo.ColType into the string RecordUseAndMaybeTrigger expects) can
// derive it without importing blockpack's internal valueindex package.
func ColTypeName(colType ColumnType) string {
	return valueindex.ColTypeName(colType)
}

// Entry is the full, stable description of one tracked (tenant, column)
// usage/backfill record (#496 plan.md Section 4.1).
type Entry = viusage.Entry

// BackfillState is one column's backfill lifecycle state, including the R7
// query-time coverage-check primitive CoversRange (#496 plan.md Section 4.1/4.7).
type BackfillState = viusage.BackfillState

// Config carries #496's top-level dedicated-columns feature toggle (R12 safety valve).
type Config = viusage.Config

// DefaultConfig returns R12's documented default: the feature enabled.
func DefaultConfig() Config {
	return viusage.DefaultConfig()
}

// TriggerConfig parameterises #496's repeated-use trigger (R4), now unconditional on
// first use (team-lead ruling 2026-07-11) — only the R8 backfill lease TTL remains.
type TriggerConfig = viusage.TriggerConfig

// TriggerResult is RecordUseAndMaybeTrigger/MaybeRecordUseAndMaybeTrigger's outcome.
type TriggerResult = viusage.TriggerResult

// ObjectStore is the minimal S3-compatible interface the usage Registry needs — the
// same Get/ConditionalPut-with-412-detection shape as CubeObjectStore, mirrored for
// #496's own registry rather than shared with it (R1).
type ObjectStore = viusage.ObjectStore

// ErrConflict is returned by ObjectStore.ConditionalPut on a 412 conflict.
var ErrConflict = viusage.ErrConflict

// ErrNotFound is returned by ObjectStore.Get when the requested object does not exist —
// the ONLY signal Registry.Load treats as "empty index" (go-presubmit.md CRITICAL
// finding). External ObjectStore implementations (e.g. tempo's minio-backed store) MUST
// return this (wrapped or bare) on a genuine 404, never a nil error with empty data —
// see viusage.ObjectStore's own doc comment for why shape-based inference is unsafe.
var ErrNotFound = viusage.ErrNotFound

// Registry loads and persists the per-tenant usage/backfill index from object storage
// (#496 plan.md Section 4.1/4.4).
type Registry = viusage.Registry

// NewRegistry creates a Registry for tenant backed by store.
func NewRegistry(store ObjectStore, tenant string) *Registry {
	return viusage.NewRegistry(store, tenant)
}

// EntryStore is the row-oriented storage interface an alternative Registry backend
// implements (2026-07-11 Postgres support) — in place of ObjectStore's whole-blob
// conditional-PUT shape, this is one row per (tenant, colHash, colType). tempo's
// pgx-backed implementation lives entirely in tempo (this package never imports a SQL
// driver) — mirrors ObjectStore's own "interface owned here, concrete backend owned by
// the caller" split exactly.
type EntryStore = viusage.EntryStore

// NewRegistryFromEntryStore constructs a Registry over an externally-supplied
// EntryStore (e.g. tempo's Postgres-backed implementation) instead of an ObjectStore.
// Registry's own public methods (Load/RenewLease/UpdateWatermark/UpdateCatalogCursor)
// are byte-identical regardless of which constructor built it.
func NewRegistryFromEntryStore(store EntryStore, tenant string) *Registry {
	return viusage.NewRegistryFromEntryStore(store, tenant)
}

// RecordUseAndMaybeTrigger appends one usage timestamp for (tenant, colName, colType)
// and, in the same conditional-PUT retry pass, evaluates R4's repeated-use threshold
// and R8's lease lifecycle, returning whether the caller must (re)launch a backfill
// job (#496 plan.md Section 4.2/4.4). Call MaybeRecordUseAndMaybeTrigger instead when
// the caller has a Config to honor R12's safety valve.
func RecordUseAndMaybeTrigger(
	ctx context.Context,
	registry *Registry,
	tenant, colName, colType string,
	now time.Time,
	cfg TriggerConfig,
) (TriggerResult, error) {
	return viusage.RecordUseAndMaybeTrigger(ctx, registry, tenant, colName, colType, now, cfg)
}

// MaybeRecordUseAndMaybeTrigger is RecordUseAndMaybeTrigger gated by cfg's R12 safety
// valve: when cfg.DedicatedColumnsEnabled is false, it returns a zero TriggerResult
// immediately without any registry I/O — tempo's B1 usage-recording hook should call
// this, not RecordUseAndMaybeTrigger directly, so a single
// Config.DedicatedColumnsEnabled=false disables the entire usage-tracking/backfill
// machinery, not just the forward write-path ColumnPolicy.
func MaybeRecordUseAndMaybeTrigger(
	ctx context.Context,
	cfg Config,
	registry *Registry,
	tenant, colName, colType string,
	now time.Time,
	triggerCfg TriggerConfig,
) (TriggerResult, error) {
	return viusage.MaybeRecordUseAndMaybeTrigger(ctx, cfg, registry, tenant, colName, colType, now, triggerCfg)
}

// DefaultDedicatedColumns is #496's provisional bootstrap dedicated-column list (R2),
// sourced from Tempo's Parquet 14 defaults, translated into blockpack's scope-prefixed
// column-name convention. Explicitly provisional — see internal/modules/viusage's own
// doc comment and NOTES.md for the full rationale.
var DefaultDedicatedColumns = viusage.DefaultDedicatedColumns
