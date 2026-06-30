package queryplanner

// NOTE: Any changes to this file must be reflected in the corresponding specs.md or NOTES.md.

import (
	"fmt"
	"strings"
)

// explainPlan builds a multi-section ASCII trace of the block-selection pipeline.
//
// Section 1 — Predicate tree: the columns each predicate references.
// Section 2 — Pruning pipeline: block counts at each stage with running totals.
//
// timeBlocks is the sorted list of blocks that survived time pruning (nil if no time pruning).
func explainPlan(predicates []Predicate, plan *Plan, timeBlocks []int) {
	var sb strings.Builder

	if len(predicates) == 0 && plan.PrunedByTime == 0 {
		fmt.Fprintf(&sb, "no predicates → all %d blocks", plan.TotalBlocks)
		plan.Explain = sb.String()
		return
	}

	// --- Section 1: Predicate tree ---
	parts := make([]string, 0, len(predicates)+1)
	for _, pred := range predicates {
		parts = append(parts, explainPred(pred))
	}
	if timeBlocks != nil {
		parts = append(parts, fmt.Sprintf("ts:%s", formatBlockList(timeBlocks)))
	}
	switch len(parts) {
	case 0:
		// No predicates and no time blocks — only reachable if PrunedByTime > 0 with nil timeBlocks.
		fmt.Fprintf(&sb, "time-pruned → %s", formatBlockList(plan.SelectedBlocks))
	case 1:
		fmt.Fprintf(&sb, "%s → %s", parts[0], formatBlockList(plan.SelectedBlocks))
	default:
		fmt.Fprintf(&sb, "(%s) → %s", strings.Join(parts, " && "), formatBlockList(plan.SelectedBlocks))
	}

	// --- Section 2: Pruning pipeline summary ---
	if plan.PrunedByTime > 0 {
		sb.WriteString("\n\nPruning pipeline:\n")
		remaining := plan.TotalBlocks
		fmt.Fprintf(&sb, "  start: %d blocks\n", remaining)
		remaining -= plan.PrunedByTime
		fmt.Fprintf(&sb, "  time-range:   -%d → %d blocks\n", plan.PrunedByTime, remaining)
	}

	plan.Explain = sb.String()
}

// explainPred recursively builds the ASCII representation for a single predicate node.
func explainPred(pred Predicate) string {
	if len(pred.Children) == 0 {
		return explainLeaf(pred)
	}

	op := "&&"
	if pred.Op == LogicalOR {
		op = "||"
	}

	parts := make([]string, 0, len(pred.Children))
	for _, child := range pred.Children {
		parts = append(parts, explainPred(child))
	}

	return "(" + strings.Join(parts, " "+op+" ") + ")"
}

// explainLeaf builds the ASCII representation for a leaf predicate, showing
// the column name(s) it references.
func explainLeaf(pred Predicate) string {
	col := "?"
	if len(pred.Columns) > 0 {
		col = pred.Columns[0]
		if len(pred.Columns) > 1 {
			col = strings.Join(pred.Columns, "|")
		}
	}
	return col
}

// formatBlockList formats a sorted slice of block indices as a compact string
// using run-length compression. Contiguous ranges use "..." notation:
//
//	[]              — empty
//	[5]             — single block
//	[0,1,2]         — short list
//	[0...8]         — contiguous range 0 through 8
//	[0...8,15,20...25,30] — mix of ranges and singles
func formatBlockList(blocks []int) string {
	if len(blocks) == 0 {
		return "[]"
	}

	var sb strings.Builder
	sb.WriteByte('[')

	i := 0
	for i < len(blocks) {
		if i > 0 {
			sb.WriteByte(',')
		}

		// Find the end of this contiguous run.
		runStart := i
		for i+1 < len(blocks) && blocks[i+1] == blocks[i]+1 {
			i++
		}

		if i-runStart >= 2 {
			// Run of 3+ → use "start...end"
			fmt.Fprintf(&sb, "%d...%d", blocks[runStart], blocks[i])
		} else {
			// 1 or 2 elements — list individually.
			fmt.Fprintf(&sb, "%d", blocks[runStart])
			for j := runStart + 1; j <= i; j++ {
				fmt.Fprintf(&sb, ",%d", blocks[j])
			}
		}
		i++
	}

	sb.WriteByte(']')
	return sb.String()
}
