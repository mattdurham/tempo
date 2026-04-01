package executor

// NOTE: Any changes to this file must be reflected in the corresponding SPECS.md or NOTES.md.

import "time"

// QueryStats is returned by Collect and CollectLogs with per-phase execution metrics.
// It replaces CollectStats (internal) and LogQueryStats (public api.go).
//
// ExecutionPath identifies which code path ran. One of:
//
//	"intrinsic-plain", "intrinsic-topk-kll", "intrinsic-topk-scan",
//	"mixed-plain", "mixed-topk", "block-plain", "block-topk",
//	"intrinsic-need-block-scan", "bloom-rejected", "block-pruned".
//
// Steps contains one entry per phase that actually ran. Phases that were
// skipped (e.g. intrinsic paths always skip the block-scan phase) are absent.
type QueryStats struct {
	ExecutionPath string
	Steps         []StepStats
	TotalDuration time.Duration
}

// Explain returns the block-pruning explanation string from the "plan" step,
// or "" if the plan step is absent or has no explain value.
func (qs QueryStats) Explain() string {
	for i := range qs.Steps {
		if qs.Steps[i].Name == "plan" {
			if v, ok := qs.Steps[i].Metadata["explain"].(string); ok {
				return v
			}
			return ""
		}
	}
	return ""
}

// SelectedBlocks returns the number of selected blocks from the "plan" step,
// or 0 if the plan step is absent.
func (qs QueryStats) SelectedBlocks() int {
	for i := range qs.Steps {
		if qs.Steps[i].Name == "plan" {
			if v, ok := qs.Steps[i].Metadata["selected_blocks"].(int); ok {
				return v
			}
			return 0
		}
	}
	return 0
}

// StepStats holds per-phase metrics for a single execution phase.
// BytesRead is an approximation: sum of BlockMeta.Length for blocks fetched
// in this phase (logical block bytes, not wire bytes).
// IOOps counts ReadGroup calls issued in this phase.
type StepStats struct {
	Metadata  map[string]any // flexible per-step key/value metadata
	Name      string         // "plan", "bloom", "intrinsic", "mixed-prefilter", "block-scan"
	Duration  time.Duration
	BytesRead int64
	IOOps     int
}
