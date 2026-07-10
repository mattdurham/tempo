package blockpack

// valueindex_policy.go — #496 column-indexing policy (plan.md Section 4.6).
//
// This type lives in the root package (not internal/modules/viusage) so that
// WriteValueIndexL0/ExtractValueIndexEntries can reference it directly and
// internal/modules/viusage's BackfillEngine (A4) can import this root package
// for *Reader/ObjectPutter/ExtractValueIndexEntriesForColumns without creating
// an import cycle (viusage -> blockpack, never blockpack -> viusage). Callers
// that need viusage's usage-registry types (Entry, DefaultDedicatedColumns,
// Registry, RecordUseAndMaybeTrigger) import viusage separately — there is no
// dependency from this file onto viusage.

// ColumnPolicy controls which columns the standard per-column value-index
// write path (WriteValueIndexL0) persists as a standalone L0 file. It is an
// allowlist-with-hard-exclusion rather than a bare denylist: the caller only
// needs to know the small, curated set of columns it wants indexed, not a
// block's full column universe.
//
// The zero value (Enabled: false) indexes every column -- byte-for-byte
// identical to the pre-#496 nil-denylist behavior, INCLUDING
// HardExcludedColumns not being enforced (R12 safety valve: disabling the
// feature disables its entire policy layer, not just part of it).
//
// SPEC-VI-11. Originally assigned as SPEC-VIU-1 in internal/modules/viusage
// (per spec-oracle's initial answer, before the viusage<->blockpack
// import-cycle finding moved this type to the root package); re-tagged
// SPEC-VI-11 to match its actual home in internal/modules/valueindex's spec
// domain, which already documents root-package
// valueindex_extract.go/valueindex_l0write.go changes (see
// NOTE-VI-018/042/051/070 precedent). spec-oracle has been notified of the
// move for A7.
type ColumnPolicy struct {
	Allow         map[string]struct{}
	AlwaysExclude map[string]struct{}
	Enabled       bool
}

// HardExcludedColumns is PERMANENTLY excluded from the standard per-column
// value index, regardless of dedicated-list membership or usage-triggered
// backfill status (#496 R2, NOTE-VI-027 history). trace:id is already
// excluded independently via WriteValueIndexL0/valueindexconsumer's
// TraceGroup-routing special case; the other three (span:id, span:parent_id,
// span:start) are not excluded anywhere else today and rely on this set.
var HardExcludedColumns = map[string]struct{}{
	"span:id":        {},
	"span:parent_id": {},
	"trace:id":       {},
	"span:start":     {},
}

// Allowed reports whether name should be indexed as a standard per-column
// value-index entry under p. Disabled policies index everything (R12).
// Enabled policies index name iff it is in Allow and NOT in AlwaysExclude --
// AlwaysExclude wins over Allow, so a column mistakenly placed in both a
// caller's dedicated list and AlwaysExclude is still excluded.
func (p ColumnPolicy) Allowed(name string) bool {
	if !p.Enabled {
		return true
	}
	if _, excluded := p.AlwaysExclude[name]; excluded {
		return false
	}
	_, allowed := p.Allow[name]
	return allowed
}

// BuildColumnPolicy computes the ColumnPolicy WriteValueIndexL0's forward
// write path should use, given whether #496's dedicated-column feature is
// enabled (R12) and the caller's dedicated + usage-triggered column lists
// (typically viusage.DefaultDedicatedColumns / a tenant override, and the
// usage registry's triggered-column names, respectively). A column is
// indexed iff it is in dedicatedList OR triggeredColumns, AND is NOT in
// HardExcludedColumns.
func BuildColumnPolicy(enabled bool, dedicatedList, triggeredColumns []string) ColumnPolicy {
	allow := make(map[string]struct{}, len(dedicatedList)+len(triggeredColumns))
	for _, c := range dedicatedList {
		allow[c] = struct{}{}
	}
	for _, c := range triggeredColumns {
		allow[c] = struct{}{}
	}
	return ColumnPolicy{
		Enabled:       enabled,
		Allow:         allow,
		AlwaysExclude: HardExcludedColumns,
	}
}
