package cube

// NOTE: SPEC-CUBE-016 — Rollup merges multiple cube files (same cube, same level) into one
// file at a coarser resolution. L0 (1-min) + L1 (60-min) + L2 (1440-min) rollup is exact:
// counts sum directly. Dictionaries are merged (union of all values from input files assigned
// new sequential IDs in the output dictionary).

import (
	"fmt"
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
	Minute  uint32 // bucket start minute at the target resolution
	Count   uint32
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
	sums := make(map[cellKey]uint32, 1024)

	for i, inp := range inputs {
		if inp.Reader == nil {
			continue
		}
		// Stream all cells from this reader.
		cells, err := inp.Reader.GetCellsRange(minMinute, maxMinute)
		if err != nil {
			return nil, fmt.Errorf("cube rollup: input %d GetCellsRange: %w", i, err)
		}
		for _, c := range cells {
			// Resolve dimension IDs to strings via the per-file dictionary.
			var dim1Val, dim2Val string
			if int(c.Dim1ID) < len(inp.Dim1Values) {
				dim1Val = inp.Dim1Values[c.Dim1ID]
			}
			if int(c.Dim2ID) < len(inp.Dim2Values) {
				dim2Val = inp.Dim2Values[c.Dim2ID]
			}
			// Round minute down to the target resolution bucket.
			bucket := (c.Minute / targetLevel) * targetLevel
			key := cellKey{minute: bucket, dim1Val: dim1Val, dim2Val: dim2Val}
			sums[key] += c.Count
		}
	}

	if len(sums) == 0 {
		return nil, nil
	}

	out := make([]MergedCell, 0, len(sums))
	for k, count := range sums {
		out = append(out, MergedCell{
			Minute:  k.minute,
			Dim1Val: k.dim1Val,
			Dim2Val: k.dim2Val,
			Count:   count,
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
		if addErr := w.AddCell(c.Minute, c.Dim1Val, c.Dim2Val, c.Count); addErr != nil {
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

// GetCellsRange is a convenience wrapper for Rollup that reads ALL cells from a reader
// without filtering by dim string. The Reader.GetCellsInRange interface requires dim strings,
// so we use the dict's reverse maps to iterate all known values.
func (r *Reader) GetCellsRange(minMinute, maxMinute uint32) ([]Cell, error) {
	// Collect all cells by iterating every (dim1,dim2) pair in the chunk directory.
	// This is O(chunks) rather than O(unique dim pairs) — suitable for small cubes.
	var all []Cell
	for i := range r.dir {
		chunk, err := r.decompressChunk(i)
		if err != nil {
			return nil, err
		}
		for _, c := range chunk {
			if c.Minute >= minMinute && c.Minute <= maxMinute {
				all = append(all, c)
			}
		}
	}
	return all, nil
}
