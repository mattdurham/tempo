package viusage

// NOTE: SPEC-VIUSAGE-004 — Config.DedicatedColumnsEnabled is #496's R12 safety valve: a
// single boolean that, when false, disables the ENTIRE feature -- not just A1's
// write-path ColumnPolicy, but also this module's usage-tracking/backfill machinery.
// MaybeRecordUseAndMaybeTrigger is the gate a caller (tempo's B1 hook) uses so the SAME
// boolean governs both sides, per plan.md's A6.

import (
	"context"
	"time"
)

// Config carries #496's top-level feature toggle (R12).
type Config struct {
	// DedicatedColumnsEnabled, when false, fully restores pre-#496 behavior: every
	// column is indexed (see ColumnPolicy's disabled Allowed behavior, A1) and no
	// usage-tracking/backfill machinery is engaged at all -- MaybeRecordUseAndMaybeTrigger
	// below never touches the registry when this is false. Default true (R12): the
	// feature is the new default behavior once complete; disabling it is a real, tested
	// rollback path, not just a theoretical escape hatch.
	DedicatedColumnsEnabled bool
}

// DefaultConfig returns R12's documented default: the feature enabled.
func DefaultConfig() Config {
	return Config{DedicatedColumnsEnabled: true}
}

// MaybeRecordUseAndMaybeTrigger is RecordUseAndMaybeTrigger gated by cfg's R12 safety
// valve: when cfg.DedicatedColumnsEnabled is false, it returns a zero TriggerResult
// immediately without any registry I/O -- the caller (tempo's B1 usage-recording hook)
// should call this instead of RecordUseAndMaybeTrigger directly so a single
// Config.DedicatedColumnsEnabled=false disables both the forward write-path policy
// (ColumnPolicy.Enabled, A1) and this usage-tracking/trigger path, matching R12's "no
// usage-tracking/backfill machinery engaged at all."
func MaybeRecordUseAndMaybeTrigger(
	ctx context.Context,
	cfg Config,
	registry *Registry,
	tenant, colName, colType string,
	now time.Time,
	triggerCfg TriggerConfig,
) (TriggerResult, error) {
	if !cfg.DedicatedColumnsEnabled {
		return TriggerResult{}, nil
	}
	return RecordUseAndMaybeTrigger(ctx, registry, tenant, colName, colType, now, triggerCfg)
}
