package queryplanner

// NOTE: Any changes to this file must be reflected in the corresponding specs.md or NOTES.md.

import (
	"fmt"
	"slices"
	"strings"
)

// explainPlan builds a multi-section ASCII trace of the full pruning pipeline.
//
// Section 1 — Predicate tree: how predicates resolved to range-index block sets.
// Section 2 — Pruning pipeline: block counts at each stage with running totals.
// Section 3 — Block priority: per-block scores with English reasoning (cardinality,
// frequency, why blocks are ranked the way they are). Only present when BlockScores
// is non-empty.
//
// timeBlocks is the sorted list of blocks that survived time pruning (nil if no time pruning).
func explainPlan(r BlockIndexer, predicates []Predicate, plan *Plan, timeBlocks []int) {
	var sb strings.Builder

	if len(predicates) == 0 && plan.PrunedByTime == 0 {
		fmt.Fprintf(&sb, "no predicates → all %d blocks", plan.TotalBlocks)
		plan.Explain = sb.String()
		return
	}

	// --- Section 1: Predicate tree ---
	parts := make([]string, 0, len(predicates)+1)
	for _, pred := range predicates {
		parts = append(parts, explainPred(r, pred, plan.TotalBlocks))
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
	if plan.PrunedByTime > 0 || plan.PrunedByIndex > 0 {
		sb.WriteString("\n\nPruning pipeline:\n")
		remaining := plan.TotalBlocks
		fmt.Fprintf(&sb, "  start: %d blocks\n", remaining)
		if plan.PrunedByTime > 0 {
			remaining -= plan.PrunedByTime
			fmt.Fprintf(&sb, "  time-range:   -%d → %d blocks\n", plan.PrunedByTime, remaining)
		}
		if plan.PrunedByIndex > 0 {
			remaining -= plan.PrunedByIndex
			fmt.Fprintf(&sb, "  range-index:  -%d → %d blocks\n", plan.PrunedByIndex, remaining)
		}
	}

	plan.Explain = sb.String()
}

// explainPred recursively builds the ASCII representation for a single predicate node.
func explainPred(r BlockIndexer, pred Predicate, blockCount int) string {
	if len(pred.Children) == 0 {
		return explainLeaf(r, pred, blockCount)
	}

	op := "&&"
	if pred.Op == LogicalOR {
		op = "||"
	}

	parts := make([]string, 0, len(pred.Children))
	for _, child := range pred.Children {
		parts = append(parts, explainPred(r, child, blockCount))
	}

	return "(" + strings.Join(parts, " "+op+" ") + ")"
}

// explainLeaf builds the ASCII representation for a leaf predicate, showing
// the column name and the block set returned by the range index.
func explainLeaf(r BlockIndexer, pred Predicate, blockCount int) string {
	col := "?"
	if len(pred.Columns) > 0 {
		col = pred.Columns[0]
		if len(pred.Columns) > 1 {
			col = strings.Join(pred.Columns, "|")
		}
	}

	set, constrained, err := leafBlockSet(r, pred, blockCount)
	if err != nil {
		return fmt.Sprintf("%s=err(%v)", col, err)
	}
	if !constrained {
		return fmt.Sprintf("%s=[]", col)
	}

	var blocks []int
	set.iter(func(b int) { blocks = append(blocks, b) })
	slices.Sort(blocks)
	return fmt.Sprintf("%s=%s", col, formatBlockList(blocks))
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
