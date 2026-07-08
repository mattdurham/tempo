package cube

// NOTE: SPEC-CUBE-016 — Rollup merges multiple cube files (same cube, same level) into one
// file at a coarser resolution. L0 (1-min) + L1 (60-min) + L2 (1440-min) rollup is exact:
// counts sum directly. Dictionaries are merged (union of all values from input files assigned
// new sequential IDs in the output dictionary).
//
// E-5 (#491) generalizes the merge to Sum/Min/Max/Buckets per aggAttr: SampleCount/Sum are
// additive, Min/Max are pairwise, Buckets are element-wise additive across the fixed 64-slot axis
// — the bucket axis is level-independent, so no rebucketing is ever needed regardless of
// targetLevel. Count/Sum/Min/Max/Buckets are all associative and order-independent across levels
// (ruling 4's cross-cutting fact): L0->L1->L2 produces the same result as L0->L2 directly.

import (
	"fmt"
	"math"
	"sort"
)

// RollupLevel is the resolution level of a cube file.
type RollupLevel uint32

// Rollup levels matching the ticket spec.
const (
	RollupL0 RollupLevel = 1    // 1-minute granularity
	RollupL1 RollupLevel = 60   // 1-hour granularity (60 minutes per bucket)
	RollupL2 RollupLevel = 1440 // 1-day granularity (1440 minutes per bucket)
)

// MergedCell is one aggregated cell in the rollup output.
// Minute is rounded down to the bucket boundary for the target level.
type MergedCell struct {
	Dim1Val string
	Dim2Val string
	// AggAttrs holds the merged per-aggAttr values (same order as the cube's registered
	// AggAttrs). Reuses AggAttrValues directly (cell.go) rather than a separately-maintained
	// "AggAttrMergedValues" type — the shape is identical and Writer.AddAggCell already consumes
	// exactly this type, so a second type would be a second, redundant maintenance point
	// (ruling 5's single-source-of-truth principle). Empty for a legacy zero-aggAttr cube.
	AggAttrs []AggAttrValues
	Minute   uint32 // bucket start minute at the target resolution
	Count    uint32
}

// mergedAggState is one cell's in-progress merge across rollup inputs — mirrors accumulator.go's
// cellAggState shape (AggAttrValues reused directly as the merge accumulator, sentinel-initialized
// Min/Max) for the same single-source-of-truth reason.
type mergedAggState struct {
	aggs  []AggAttrValues
	count uint32
}

// newMergedAggState allocates a mergedAggState with every aggAttr's Min/Max sentinel-initialized
// (math.MaxFloat64 / -math.MaxFloat64 — same convention as accumulator.go's newCellAggState).
func newMergedAggState(numAggAttrs int) *mergedAggState {
	aggs := make([]AggAttrValues, numAggAttrs)
	for i := range aggs {
		aggs[i].Min = math.MaxFloat64
		aggs[i].Max = -math.MaxFloat64
	}
	return &mergedAggState{aggs: aggs}
}

// mergeAggAttrInto folds src into dst. A src with SampleCount==0 (no valid samples for this
// attribute at this cell in this particular input file) contributes NOTHING — this is the guard
// that keeps a never-sampled attribute's sentinel Min/Max from ever entering the running merge.
func mergeAggAttrInto(dst *AggAttrValues, src AggAttrValues) {
	if src.SampleCount == 0 {
		return
	}
	dst.SampleCount += src.SampleCount
	dst.Sum += src.Sum
	if src.Min < dst.Min {
		dst.Min = src.Min
	}
	if src.Max > dst.Max {
		dst.Max = src.Max
	}
	for i, c := range src.Buckets {
		dst.Buckets[i] += c
	}
}

// finalizeAggAttrs resets Min/Max to 0 for any aggAttr whose SampleCount is still 0 after merging
// every input — i.e. no input ever had a valid sample for this attribute at this cell. Without
// this, the sentinel-init values (math.MaxFloat64/-math.MaxFloat64) would otherwise leak into the
// merged output (TestRollup_MinMaxSentinelNeverLeaksIntoOutput).
func finalizeAggAttrs(aggs []AggAttrValues) {
	for i := range aggs {
		if aggs[i].SampleCount == 0 {
			aggs[i].Min = 0
			aggs[i].Max = 0
		}
	}
}

// RollupInput is one opened cube file contributing cells to the rollup.
type RollupInput struct {
	// Reader is the open cube reader.
	Reader *Reader
	// Dim1Values / Dim2Values expose the per-file dictionary for value resolution.
	Dim1Values []string
	Dim2Values []string
}

// Rollup merges all cells from inputs into a single sorted []MergedCell at targetLevel
// resolution. Counts for the same (bucketMinute, dim1Value, dim2Value) triple are summed.
//
// targetLevel is the number of minutes per output bucket (1, 60, or 1440).
// minMinute / maxMinute are the overall time window to include (inclusive); cells outside
// this window are skipped.
//
// The output is sorted by (Minute ASC, Dim1Val ASC, Dim2Val ASC) — ready to feed a Writer.
func Rollup(inputs []RollupInput, targetLevel uint32, minMinute, maxMinute uint32) ([]MergedCell, error) {
	if len(inputs) == 0 {
		return nil, nil
	}
	if targetLevel == 0 {
		return nil, fmt.Errorf("cube rollup: targetLevel must be > 0")
	}

	type cellKey struct {
		dim1Val string
		dim2Val string
		minute  uint32
	}
	merged := make(map[cellKey]*mergedAggState, 1024)

	for i, inp := range inputs {
		if inp.Reader == nil {
			continue
		}
		// Stream all AggCells from this reader (base fields + every per-aggAttr record).
		cells, err := inp.Reader.GetCellsRange(minMinute, maxMinute)
		if err != nil {
			return nil, fmt.Errorf("cube rollup: input %d GetCellsRange: %w", i, err)
		}
		for _, ac := range cells {
			// Resolve dimension IDs to strings via the per-file dictionary.
			var dim1Val, dim2Val string
			if int(ac.Dim1ID) < len(inp.Dim1Values) {
				dim1Val = inp.Dim1Values[ac.Dim1ID]
			}
			if int(ac.Dim2ID) < len(inp.Dim2Values) {
				dim2Val = inp.Dim2Values[ac.Dim2ID]
			}
			// Round minute down to the target resolution bucket.
			bucket := (ac.Minute / targetLevel) * targetLevel
			key := cellKey{minute: bucket, dim1Val: dim1Val, dim2Val: dim2Val}
			ms, ok := merged[key]
			if !ok {
				ms = newMergedAggState(len(ac.Aggs))
				merged[key] = ms
			}
			ms.count += ac.Count
			for j := range ac.Aggs {
				if j >= len(ms.aggs) {
					continue // defensive: a cube's aggAttr set is fixed for its whole life
				}
				mergeAggAttrInto(&ms.aggs[j], ac.Aggs[j])
			}
		}
	}

	if len(merged) == 0 {
		return nil, nil
	}

	out := make([]MergedCell, 0, len(merged))
	for k, ms := range merged {
		finalizeAggAttrs(ms.aggs)
		out = append(out, MergedCell{
			Minute:   k.minute,
			Dim1Val:  k.dim1Val,
			Dim2Val:  k.dim2Val,
			Count:    ms.count,
			AggAttrs: ms.aggs,
		})
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Minute != out[j].Minute {
			return out[i].Minute < out[j].Minute
		}
		if out[i].Dim1Val != out[j].Dim1Val {
			return out[i].Dim1Val < out[j].Dim1Val
		}
		return out[i].Dim2Val < out[j].Dim2Val
	})
	return out, nil
}

// RollupToWriter merges inputs and feeds the result into a fresh Writer.
// cubeID and resolution are written into the output file header.
// Returns the Writer with all cells added (not yet flushed).
func RollupToWriter(
	cubeID [16]byte,
	resolution uint32,
	inputs []RollupInput,
	minMinute, maxMinute uint32,
) (*Writer, error) {
	cells, err := Rollup(inputs, resolution, minMinute, maxMinute)
	if err != nil {
		return nil, err
	}
	w := NewWriter(cubeID, resolution)
	for _, c := range cells {
		if addErr := w.AddAggCell(c.Minute, c.Dim1Val, c.Dim2Val, c.Count, c.AggAttrs); addErr != nil {
			return nil, fmt.Errorf("cube rollup: add cell: %w", addErr)
		}
	}
	return w, nil
}

// NewRollupInput opens a cube file for use as a rollup input.
func NewRollupInput(r *Reader) RollupInput {
	return RollupInput{
		Reader:     r,
		Dim1Values: r.dict.Dim1Values,
		Dim2Values: r.dict.Dim2Values,
	}
}

// GetCellsRange is a convenience wrapper for Rollup that reads ALL AggCells (base fields + every
// per-aggAttr record) from a reader without filtering by dim string. The Reader.GetCellsInRange
// interface requires dim strings, so we use the dict's reverse maps to iterate all known values.
func (r *Reader) GetCellsRange(minMinute, maxMinute uint32) ([]AggCell, error) {
	// Collect all cells by iterating every (dim1,dim2) pair in the chunk directory.
	// This is O(chunks) rather than O(unique dim pairs) — suitable for small cubes.
	var all []AggCell
	for i := range r.dir {
		chunk, err := r.decompressChunk(i)
		if err != nil {
			return nil, err
		}
		for _, ac := range chunk {
			if ac.Minute >= minMinute && ac.Minute <= maxMinute {
				all = append(all, ac)
			}
		}
	}
	return all, nil
}
