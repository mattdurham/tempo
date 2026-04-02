package queryplanner

// NOTE: Any changes to this file must be reflected in the corresponding specs.md or NOTES.md.

import (
	"cmp"
	"fmt"
	"slices"
	"strings"

	"github.com/grafana/blockpack/internal/modules/sketch"
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
		parts = append(parts, explainPred(r, pred))
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
	if plan.PrunedByTime > 0 || plan.PrunedByIndex > 0 || plan.PrunedByFuse > 0 {
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
		if plan.PrunedByFuse > 0 {
			remaining -= plan.PrunedByFuse
			fmt.Fprintf(&sb, "  fuse-filter:  -%d → %d blocks (membership exclusion, 0.39%% FPR)\n",
				plan.PrunedByFuse, remaining)
		}
	}

	// --- Section 3: Block priority with English reasoning ---
	if len(plan.BlockScores) > 0 && len(predicates) > 0 {
		explainBlockPriority(&sb, r, plan, predicates)
	}

	plan.Explain = sb.String()
}

// explainPred recursively builds the ASCII representation for a single predicate node.
func explainPred(r BlockIndexer, pred Predicate) string {
	if len(pred.Children) == 0 {
		return explainLeaf(r, pred)
	}

	op := "&&"
	if pred.Op == LogicalOR {
		op = "||"
	}

	parts := make([]string, 0, len(pred.Children))
	for _, child := range pred.Children {
		parts = append(parts, explainPred(r, child))
	}

	return "(" + strings.Join(parts, " "+op+" ") + ")"
}

// explainLeaf builds the ASCII representation for a leaf predicate, showing
// the column name and the block set returned by the range index.
func explainLeaf(r BlockIndexer, pred Predicate) string {
	col := "?"
	if len(pred.Columns) > 0 {
		col = pred.Columns[0]
		if len(pred.Columns) > 1 {
			col = strings.Join(pred.Columns, "|")
		}
	}

	set, err := leafBlockSet(r, pred)
	if err != nil {
		return fmt.Sprintf("%s=err(%v)", col, err)
	}
	if set == nil {
		return fmt.Sprintf("%s=[]", col)
	}

	blocks := make([]int, 0, len(set))
	for b := range set {
		blocks = append(blocks, b)
	}
	slices.Sort(blocks)
	return fmt.Sprintf("%s=%s", col, formatBlockList(blocks))
}

// explainBlockPriority appends per-block scoring details to the explain output.
// Blocks are sorted by score descending (best candidates first) and annotated
// with cardinality, frequency source (TopK exact or CMS estimate), and an English
// summary of why the block ranks where it does.
func explainBlockPriority(sb *strings.Builder, r BlockIndexer, plan *Plan, predicates []Predicate) {
	// Collect scored blocks sorted by score descending, then by block index ascending.
	type scoredBlock struct {
		blockIdx int
		score    float64
	}
	blocks := make([]scoredBlock, 0, len(plan.BlockScores))
	for b, s := range plan.BlockScores {
		blocks = append(blocks, scoredBlock{b, s})
	}
	slices.SortFunc(blocks, func(a, b scoredBlock) int {
		if a.score != b.score {
			return cmp.Compare(b.score, a.score) // descending
		}
		return cmp.Compare(a.blockIdx, b.blockIdx) // deterministic tiebreak
	})

	sb.WriteString("\nBlock priority (best first):\n")

	// For each block, build per-predicate detail.
	for rank, blk := range blocks {
		fmt.Fprintf(sb, "  #%d block %d (score=%.2f)", rank+1, blk.blockIdx, blk.score)
		details := explainBlockDetails(r, blk.blockIdx, predicates)
		if details != "" {
			fmt.Fprintf(sb, " — %s", details)
		}
		sb.WriteByte('\n')
	}

	// Also note unscored blocks (survived pruning but no sketch data to score).
	var unscored []int
	for _, b := range plan.SelectedBlocks {
		if _, ok := plan.BlockScores[b]; !ok {
			unscored = append(unscored, b)
		}
	}
	if len(unscored) > 0 {
		fmt.Fprintf(sb, "  unscored: %s (no sketch data)\n", formatBlockList(unscored))
	}
}

// explainBlockDetails builds an English description of why a block scored the way it did.
// Examines each leaf predicate's column sketch to report cardinality and frequency.
func explainBlockDetails(r BlockIndexer, blockIdx int, predicates []Predicate) string {
	var parts []string
	for _, pred := range predicates {
		explainBlockPred(r, blockIdx, pred, &parts)
	}
	if len(parts) == 0 {
		return ""
	}
	return strings.Join(parts, "; ")
}

// explainBlockPred recursively collects detail strings for a single predicate.
func explainBlockPred(r BlockIndexer, blockIdx int, pred Predicate, parts *[]string) {
	if len(pred.Children) > 0 {
		for _, child := range pred.Children {
			explainBlockPred(r, blockIdx, child, parts)
		}
		return
	}

	if len(pred.Values) == 0 || len(pred.Columns) != 1 {
		return
	}
	col := pred.Columns[0]
	cs := r.ColumnSketch(col)
	if cs == nil {
		return
	}

	distinct := cs.Distinct()
	if blockIdx >= len(distinct) {
		return
	}
	card := distinct[blockIdx]

	// Describe cardinality bucket.
	var cardDesc string
	switch {
	case card <= 5:
		cardDesc = "very low"
	case card <= 50:
		cardDesc = "low"
	case card <= 500:
		cardDesc = "medium"
	default:
		cardDesc = "high"
	}

	// Aggregate frequency across all queried values using TopK.
	var totalFreq uint32
	for _, val := range pred.Values {
		valFP := sketch.HashForFuse(val)
		topk := cs.TopKMatch(valFP)
		if blockIdx < len(topk) && topk[blockIdx] > 0 {
			totalFreq += uint32(topk[blockIdx]) //nolint:gosec // safe: uint16 fits uint32
		}
	}

	// Build the short column name for readability.
	shortCol := col
	if idx := strings.LastIndex(col, "."); idx >= 0 && idx < len(col)-1 {
		shortCol = col[idx+1:]
	}

	*parts = append(*parts, fmt.Sprintf(
		"%s: %s cardinality (%d distinct), freq=%d (topk)",
		shortCol, cardDesc, card, totalFreq,
	))
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
