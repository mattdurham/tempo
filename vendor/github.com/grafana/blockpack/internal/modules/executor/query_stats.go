package executor

// NOTE: Any changes to this file must be reflected in the corresponding SPECS.md or NOTES.md.

import "time"

// ExecutionPath constants identify which code path ran in a query.
const (
	ExecPathBlockPlain         = "block-plain"
	ExecPathBlockTopK          = "block-topk"
	ExecPathBlockPruned        = "block-pruned"
	ExecPathIntrinsicPlain     = "intrinsic-plain"
	ExecPathIntrinsicTopKKLL   = "intrinsic-topk-kll"
	ExecPathIntrinsicTopKScan  = "intrinsic-topk-scan"
	ExecPathMixedPlain         = "mixed-plain"
	ExecPathMixedTopK          = "mixed-topk"
	ExecPathBloomRejected      = "bloom-rejected"
	ExecPathIntrinsicNeedBlock = "intrinsic-need-block-scan"
)

// StepStats.Name constants identify the phase within the query execution pipeline.
const (
	stepNamePlan           = "plan"
	stepNameBlockScan      = "block-scan"
	stepNameIntrinsic      = "intrinsic"
	stepNameMixedPrefilter = "mixed-prefilter"
)

// QueryStats is returned by Collect and CollectLogs with per-phase execution metrics.
// It replaces CollectStats (internal) and LogQueryStats (public api.go).
//
// ExecutionPath identifies which code path ran:
//
//	Collect (trace/span queries):
//	  "intrinsic-plain", "intrinsic-topk-kll", "intrinsic-topk-scan",
//	  "mixed-plain", "mixed-topk", "block-plain", "block-topk",
//	  "intrinsic-need-block-scan", "bloom-rejected", "block-pruned".
//
//	CollectLogs (log queries):
//	  "block-plain" (full block scan, no top-K), "block-topk" (heap-based top-K by timestamp).
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
		if qs.Steps[i].Name == stepNamePlan {
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
		if qs.Steps[i].Name == stepNamePlan {
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
