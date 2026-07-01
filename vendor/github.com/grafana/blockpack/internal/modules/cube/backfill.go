package cube

// NOTE: SPEC-CUBE-015 — Backfiller populates historical cube files by reading the value
// index (no data-block reads) and aggregating per-(dim1,dim2) counts per minute. It processes
// newest→oldest so recent data becomes available first. Progress is tracked by a watermark
// stored alongside the RegistryEntry; the querier uses cube files for minutes ≥ watermark
// and falls back to the value index for earlier minutes.

import (
	"context"
	"fmt"
	"time"

	"github.com/grafana/blockpack/internal/modules/valueindex"
)

// ValueIndexSource provides per-column value-index data for backfill reads.
// A real implementation fetches and opens VI files from object storage.
// The interface is narrow so tests can inject synthetic data.
type ValueIndexSource interface {
	// LookupColumn returns all QueryResult entries for the given column in [minSec, maxSec].
	LookupColumn(ctx context.Context, tenant, column string, minSec, maxSec uint64) ([]valueindex.QueryResult, error)
}

// BackfillWatermark tracks the oldest minute that has been backfilled for a cube.
// The querier uses cube files for minutes ≥ WatermarkMinute and the value index for earlier.
type BackfillWatermark struct {
	CubeID          string `json:"cube_id"`
	WatermarkMinute uint32 `json:"watermark_minute"` // inclusive: cube data available from this minute onward
	Done            bool   `json:"done"`             // true when the full backfill window is complete
}

// BackfillConfig parameterises the backfill worker.
type BackfillConfig struct {
	// Store is used to write finished cube files.
	Store ObjectPutter
	// Workers is the number of parallel minute-workers (default 10).
	Workers int
	// WindowMinutes is the number of minutes to backfill (default 30 days = 43200).
	WindowMinutes uint32
}

func (c *BackfillConfig) setDefaults() {
	if c.WindowMinutes == 0 {
		c.WindowMinutes = 43200 // 30 days
	}
	if c.Workers == 0 {
		c.Workers = 10
	}
}

// BackfillProgress reports backfill state for one cube.
type BackfillProgress struct {
	// LastError is the most recent worker error, if any.
	LastError error
	Watermark BackfillWatermark
}

// Backfiller processes one cube's historical backfill from the value index.
type Backfiller struct {
	cfg    BackfillConfig
	src    ValueIndexSource
	tenant string
	entry  RegistryEntry
}

// NewBackfiller creates a Backfiller for entry.
func NewBackfiller(entry RegistryEntry, src ValueIndexSource, cfg BackfillConfig) *Backfiller {
	cfg.setDefaults()
	return &Backfiller{entry: entry, src: src, cfg: cfg, tenant: entry.Tenant}
}

// Run starts the backfill from currentMinute-1 down to (currentMinute - WindowMinutes),
// writing one cube file per minute. It calls progressFn after each completed minute
// (watermark advanced) so callers can persist the watermark to S3 and handle ctx
// cancellation.
//
// progressFn may return an error to abort the backfill. Run returns when the full
// window is complete or ctx is canceled or progressFn returns an error.
func (b *Backfiller) Run(
	ctx context.Context,
	currentMinute uint32,
	progressFn func(BackfillProgress) error,
) error {
	if currentMinute == 0 {
		currentMinute = uint32(time.Now().Unix() / 60) //nolint:gosec // unix timestamp fits uint32 until 2106
	}

	startMinute := currentMinute - 1
	endMinute := uint32(0)
	if currentMinute > b.cfg.WindowMinutes {
		endMinute = currentMinute - b.cfg.WindowMinutes
	}

	// Process minutes newest→oldest so recent data becomes available first.
	for m := startMinute; m >= endMinute; m-- {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if err := b.processMinute(ctx, m); err != nil {
			prog := BackfillProgress{
				Watermark: BackfillWatermark{CubeID: b.entry.CubeID, WatermarkMinute: m + 1},
				LastError: err,
			}
			if fnErr := progressFn(prog); fnErr != nil {
				return fnErr
			}
			continue
		}
		prog := BackfillProgress{
			Watermark: BackfillWatermark{CubeID: b.entry.CubeID, WatermarkMinute: m, Done: m == endMinute},
		}
		if fnErr := progressFn(prog); fnErr != nil {
			return fnErr
		}
		if m == 0 { // guard against uint32 underflow on edge case
			break
		}
	}
	return nil
}

// processMinute builds one cube file for the given minute from value index data.
func (b *Backfiller) processMinute(ctx context.Context, minute uint32) error {
	minSec := uint64(minute) * 60
	maxSec := minSec + 59

	// Build an accumulator for this minute (uses the existing ingest layer).
	dim2Col := "_"
	if len(b.entry.Dimensions) > 1 {
		dim2Col = b.entry.Dimensions[1]
	}
	acc := NewAccumulator(Definition{
		Dim1Column: b.entry.Dimensions[0],
		Dim2Column: dim2Col,
		ID:         func() [16]byte { id, _ := IDFromBytes(b.entry.CubeID); return id }(),
		Resolution: b.entry.Resolution,
	}, minute)

	// Read dim1 entries from the value index.
	dim1Entries, err := b.src.LookupColumn(ctx, b.tenant, b.entry.Dimensions[0], minSec, maxSec)
	if err != nil {
		return fmt.Errorf("cube backfill: lookup dim1 %q minute %d: %w", b.entry.Dimensions[0], minute, err)
	}

	if len(dim1Entries) == 0 {
		return nil // no data for this minute — write nothing (sparse cube)
	}

	// If there is only one dimension, use a fixed sentinel for dim2.
	dim2 := "_"
	if len(b.entry.Dimensions) > 1 {
		// With 2 dimensions, we need both. Build a (traceID+spanID) → dim1value map from
		// dim1 entries, then scan dim2 entries to find matching spans and record (dim1,dim2) pairs.
		spanDim1 := make(map[[24]byte]string, len(dim1Entries))
		for _, e := range dim1Entries {
			var key [24]byte
			copy(key[:16], e.TraceID[:])
			copy(key[16:], e.SpanID[:])
			spanDim1[key] = e.SourceRef // SourceRef stores the column value in VINX
		}
		dim2Entries, dim2Err := b.src.LookupColumn(ctx, b.tenant, b.entry.Dimensions[1], minSec, maxSec)
		if dim2Err != nil {
			return fmt.Errorf("cube backfill: lookup dim2 %q minute %d: %w", b.entry.Dimensions[1], minute, dim2Err)
		}
		for _, e2 := range dim2Entries {
			var key [24]byte
			copy(key[:16], e2.TraceID[:])
			copy(key[16:], e2.SpanID[:])
			d1val, ok := spanDim1[key]
			if !ok {
				continue
			}
			d2val := e2.SourceRef
			// Apply cube filters (best-effort: only numeric int64 filters on span values
			// embedded in SourceRef are supported at backfill time; other filters are skipped).
			sv := valueIndexSpanValues{
				vals: map[string]string{b.entry.Dimensions[0]: d1val, b.entry.Dimensions[1]: d2val},
			}
			if _, addErr := acc.Add(sv); addErr != nil {
				return fmt.Errorf("cube backfill: add cell: %w", addErr)
			}
		}
	} else {
		// Single dimension: each dim1 entry increments the (dim1, "_") cell.
		for _, e := range dim1Entries {
			d1val := e.SourceRef
			sv := valueIndexSpanValues{vals: map[string]string{b.entry.Dimensions[0]: d1val, dim2Col: dim2}}
			if _, addErr := acc.Add(sv); addErr != nil {
				return fmt.Errorf("cube backfill: add cell: %w", addErr)
			}
		}
	}

	if acc.CellCount() == 0 {
		return nil // no data accumulated for this minute
	}

	// Flush to object store.
	key, err := acc.FlushTo(b.cfg.Store, b.tenant)
	if err != nil {
		return fmt.Errorf("cube backfill: flush minute %d: %w", minute, err)
	}
	_ = key
	return nil
}

// valueIndexSpanValues wraps pre-resolved (dim1col→val, dim2col→val) pairs as SpanValues
// for the Accumulator. The accumulator calls String(colName) using the actual column names.
type valueIndexSpanValues struct {
	vals map[string]string // column name → resolved value
}

func (v valueIndexSpanValues) String(col string) (string, bool) {
	s, ok := v.vals[col]
	return s, ok
}

func (v valueIndexSpanValues) Int64(_ string) (int64, bool) { return 0, false }
