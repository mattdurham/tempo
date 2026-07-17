package cube

// NOTE: SPEC-CUBE-026 — Backfiller populates historical cube files by reading the value
// index (no data-block reads) and aggregating per-(dim1,dim2) counts per minute. It processes
// newest→oldest so recent data becomes available first. Progress is tracked by a watermark
// stored alongside the RegistryEntry; the querier uses cube files for minutes ≥ watermark
// and falls back to the value index for earlier minutes.

import (
	"context"
	"fmt"
	"strconv"
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

// viEntryKey identifies one span for cross-column VI joins: dim2 and aggAttr entries are looked
// up as separate VI queries from dim1 and joined back to it by (traceID, spanID).
func viEntryKey(traceID [16]byte, spanID [8]byte) [24]byte {
	var key [24]byte
	copy(key[:16], traceID[:])
	copy(key[16:], spanID[:])
	return key
}

// AggAttrDefsFor converts a registry's AggAttrs column names to AggAttrDefs. DurationColumn is
// always Int64-typed (nanoseconds); every other column defaults to Float64-typed, since a bare
// column name carries no type information — a conservative choice that still gets Sum/Min/Max/Avg
// correctly, just without Buckets[]/histogram support for that attribute (ruling 1 already scopes
// bucketing to Int64/Duration-typed attrs only, so this is not a capability regression versus what
// a Float64-typed attr would get anyway).
//
// Exported (#491 Phase E fix pass, review.md Issue 1) so cube_ingest.go's
// CubeRegistryEntryToDefinition can reuse the SAME type-defaulting convention backfill uses,
// instead of maintaining a second, independently-drifting copy of this rule.
func AggAttrDefsFor(columns []string) []AggAttrDef {
	defs := make([]AggAttrDef, len(columns))
	for i, col := range columns {
		typ := AggAttrTypeFloat64
		if col == DurationColumn {
			typ = AggAttrTypeInt64
		}
		defs[i] = AggAttrDef{Column: col, Type: typ}
	}
	return defs
}

// lookupAggAttrValues fetches VI entries for every aggAttr column not already covered by the
// cube's own dimensions, returning column → (traceID,spanID) → value-string, ready to join
// against whichever dimension entries drive the per-span loop in processMinute. extraExcluded
// additionally skips columns whose value the caller already has via a different lookup (e.g. the
// zero-dim anchor lookup on DurationColumn, whose SourceRef already IS that column's value) — a
// nil extraExcluded is safe (zero-value map lookups are always false) and behaves exactly as
// before this parameter existed.
func (b *Backfiller) lookupAggAttrValues(
	ctx context.Context,
	minute uint32,
	minSec, maxSec uint64,
	extraExcluded map[string]bool,
) (map[string]map[[24]byte]string, error) {
	dimSet := make(map[string]bool, len(b.entry.Dimensions))
	for _, d := range b.entry.Dimensions {
		dimSet[d] = true
	}
	out := make(map[string]map[[24]byte]string, len(b.entry.AggAttrs))
	for _, col := range b.entry.AggAttrs {
		if dimSet[col] || extraExcluded[col] {
			continue // already covered by a dimension lookup or an extra caller-supplied lookup
		}
		entries, err := b.src.LookupColumn(ctx, b.tenant, col, minSec, maxSec)
		if err != nil {
			return nil, fmt.Errorf("cube backfill: lookup aggAttr %q minute %d: %w", col, minute, err)
		}
		m := make(map[[24]byte]string, len(entries))
		for _, e := range entries {
			m[viEntryKey(e.TraceID, e.SpanID)] = e.SourceRef
		}
		out[col] = m
	}
	return out, nil
}

// buildSpanVals assembles one span's full column→value map: the dimension values already
// resolved by the caller, plus every aggAttr column's value (if the span has one) joined by key.
func buildSpanVals(
	dim1Col, dim2Col, d1val, d2val string,
	key [24]byte,
	aggAttrValues map[string]map[[24]byte]string,
) map[string]string {
	vals := map[string]string{dim1Col: d1val, dim2Col: d2val}
	for col, byKey := range aggAttrValues {
		if v, ok := byKey[key]; ok {
			vals[col] = v
		}
	}
	return vals
}

// processMinute builds one cube file for the given minute from value index data.
func (b *Backfiller) processMinute(ctx context.Context, minute uint32) error {
	// zeroDim (ungrouped, len(Dimensions)==0) cubes have no dimension column to anchor a VI
	// lookup on, so processMinuteZeroDim below reuses the mandatory DurationColumn AggAttr lookup
	// as the span-enumeration source instead -- see its own doc comment for the full strategy.
	zeroDim := len(b.entry.Dimensions) == 0

	minSec := uint64(minute) * 60
	maxSec := minSec + 59

	// Build an accumulator for this minute (uses the existing ingest layer). AggAttrs come
	// directly from the registry entry — a real RegistryEntry is only ever created via
	// CreationTrigger.TryCreate, which already enforces (via validateDefinition, E-4) that
	// duration is present, so this is not re-derived defensively here.
	// NOTE-CUBE-033 (Bug 2): must match CubeRegistryEntryToDefinition's forward-ingest sentinel
	// exactly (AllDimSentinel, "__all__") -- a mismatched literal here previously ("_") meant a
	// single-dimension cube's backfilled and forward-ingested files carried two DIFFERENT
	// dim2 dictionary values, so CubeRollup treated them as two distinct series per dim1 value
	// instead of merging them.
	dim2Col := AllDimSentinel
	if len(b.entry.Dimensions) > 1 {
		dim2Col = b.entry.Dimensions[1]
	}
	// Filters must be converted from the registry entry's baked-in predicate (#491 Phase E fix
	// pass, review.md Issue 3) — ColumnFilterToFilter is the SAME single-source-of-truth
	// conversion tempo's cubemanager.go passes as LoadCubeDefinitions' filterFn for forward
	// ingest, so backfill and forward ingest can never independently drift on filter semantics.
	// Before this fix, b.entry.Filters was read nowhere in this file, so historical backfill of a
	// filtered cube silently counted every span matching the cube's dimensions, ignoring the
	// filter entirely.
	filters := make([]Filter, 0, len(b.entry.Filters))
	for _, cf := range b.entry.Filters {
		if f := ColumnFilterToFilter(cf); f != nil {
			filters = append(filters, f)
		}
	}
	// dim2Col is already AllDimSentinel here when zeroDim (len(Dimensions)==0 implies the
	// len(Dimensions)>1 branch above never ran).
	dim1Col := AllDimSentinel
	if !zeroDim {
		dim1Col = b.entry.Dimensions[0]
	}
	acc, err := NewAccumulator(Definition{
		Dim1Column: dim1Col,
		Dim2Column: dim2Col,
		Filters:    filters,
		AggAttrs:   AggAttrDefsFor(b.entry.AggAttrs),
		ID:         func() [16]byte { id, _ := IDFromBytes(b.entry.CubeID); return id }(),
		Resolution: b.entry.Resolution,
	}, minute)
	if err != nil {
		return fmt.Errorf("cube backfill: new accumulator: %w", err)
	}

	if zeroDim {
		return b.processMinuteZeroDim(ctx, acc, minute, minSec, maxSec)
	}

	// Read dim1 entries from the value index.
	dim1Entries, err := b.src.LookupColumn(ctx, b.tenant, b.entry.Dimensions[0], minSec, maxSec)
	if err != nil {
		return fmt.Errorf("cube backfill: lookup dim1 %q minute %d: %w", b.entry.Dimensions[0], minute, err)
	}

	if len(dim1Entries) == 0 {
		return nil // no data for this minute — write nothing (sparse cube)
	}

	aggAttrValues, err := b.lookupAggAttrValues(ctx, minute, minSec, maxSec, nil)
	if err != nil {
		return err
	}

	// If there is only one dimension, use a fixed sentinel for dim2 (must match dim2Col above,
	// and CubeRegistryEntryToDefinition's forward-ingest sentinel -- see its own comment).
	dim2 := AllDimSentinel
	if len(b.entry.Dimensions) > 1 {
		// With 2 dimensions, we need both. Build a (traceID+spanID) → dim1value map from
		// dim1 entries, then scan dim2 entries to find matching spans and record (dim1,dim2) pairs.
		spanDim1 := make(map[[24]byte]string, len(dim1Entries))
		for _, e := range dim1Entries {
			spanDim1[viEntryKey(e.TraceID, e.SpanID)] = e.SourceRef // SourceRef stores the column value in VINX
		}
		dim2Entries, dim2Err := b.src.LookupColumn(ctx, b.tenant, b.entry.Dimensions[1], minSec, maxSec)
		if dim2Err != nil {
			return fmt.Errorf("cube backfill: lookup dim2 %q minute %d: %w", b.entry.Dimensions[1], minute, dim2Err)
		}
		for _, e2 := range dim2Entries {
			key := viEntryKey(e2.TraceID, e2.SpanID)
			d1val, ok := spanDim1[key]
			if !ok {
				continue
			}
			d2val := e2.SourceRef
			// Cube filters (registered via b.entry.Filters, converted to acc.def.Filters above
			// via ColumnFilterToFilter) are applied here through the accumulator's normal Add
			// path, exactly like forward ingest — Add rejects any span failing a filter before
			// counting it. A filter whose value ColumnFilterToFilter could not parse is skipped
			// (not enforced), per that function's documented fallback.
			sv := valueIndexSpanValues{
				vals: buildSpanVals(b.entry.Dimensions[0], b.entry.Dimensions[1], d1val, d2val, key, aggAttrValues),
			}
			if _, addErr := acc.Add(sv); addErr != nil {
				return fmt.Errorf("cube backfill: add cell: %w", addErr)
			}
		}
	} else {
		// Single dimension: each dim1 entry increments the (dim1, AllDimSentinel) cell.
		for _, e := range dim1Entries {
			key := viEntryKey(e.TraceID, e.SpanID)
			d1val := e.SourceRef
			sv := valueIndexSpanValues{
				vals: buildSpanVals(b.entry.Dimensions[0], dim2Col, d1val, dim2, key, aggAttrValues),
			}
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

// processMinuteZeroDim handles the ungrouped (len(Dimensions)==0) case: every span in the
// minute window collapses into ONE (AllDimSentinel, AllDimSentinel) cell. There is no dimension
// column to anchor a VI lookup on, so this reuses the mandatory DurationColumn AggAttr lookup
// (every v2 cube materializes duration, validateDefinition/E-4) as the span-enumeration source
// instead -- LookupColumn(DurationColumn) returns one VI entry per span with a duration value in
// the window (effectively every span), and each entry's SourceRef IS that span's duration value,
// so no separate aggAttr join is needed for DurationColumn itself (only for any OTHER AggAttrs a
// zero-dim cube may also declare -- see lookupAggAttrValues's extraExcluded param).
func (b *Backfiller) processMinuteZeroDim(
	ctx context.Context, acc *Accumulator, minute uint32, minSec, maxSec uint64,
) error {
	anchorEntries, err := b.src.LookupColumn(ctx, b.tenant, DurationColumn, minSec, maxSec)
	if err != nil {
		return fmt.Errorf("cube backfill: lookup zero-dim anchor %q minute %d: %w", DurationColumn, minute, err)
	}
	if len(anchorEntries) == 0 {
		return nil // no data for this minute -- write nothing (sparse cube)
	}

	aggAttrValues, err := b.lookupAggAttrValues(ctx, minute, minSec, maxSec, map[string]bool{DurationColumn: true})
	if err != nil {
		return err
	}

	for _, e := range anchorEntries {
		key := viEntryKey(e.TraceID, e.SpanID)
		vals := buildSpanVals(AllDimSentinel, AllDimSentinel, AllDimSentinel, AllDimSentinel, key, aggAttrValues)
		vals[DurationColumn] = e.SourceRef // anchor entry IS the duration lookup; reuse its value directly
		sv := valueIndexSpanValues{vals: vals}
		if _, addErr := acc.Add(sv); addErr != nil {
			return fmt.Errorf("cube backfill: add cell: %w", addErr)
		}
	}

	if acc.CellCount() == 0 {
		return nil
	}
	if _, err := acc.FlushTo(b.cfg.Store, b.tenant); err != nil {
		return fmt.Errorf("cube backfill: flush minute %d: %w", minute, err)
	}
	return nil
}

// valueIndexSpanValues wraps pre-resolved (column→val) pairs as SpanValues for the Accumulator.
// The accumulator calls String(colName)/Float64(colName) using the actual column names.
type valueIndexSpanValues struct {
	vals map[string]string // column name → resolved VI SourceRef string
}

func (v valueIndexSpanValues) String(col string) (string, bool) {
	s, ok := v.vals[col]
	return s, ok
}

// Int64 parses the VI's string-typed SourceRef as an integer (#491 Phase E fix pass, review.md
// Issue 3 compounding note) — mirrors Float64's own parse-and-tolerate-absence convention below.
// Before this fix, Int64 unconditionally returned (0, false), so ANY NumericFilter-based
// predicate (which reads exclusively via Int64, accumulator.go) rejected every span once #3a
// wired Filters into backfill's Definition — turning a filtered backfill into an
// empty-but-not-obviously-wrong result instead of a correct one.
func (v valueIndexSpanValues) Int64(col string) (int64, bool) {
	s, ok := v.vals[col]
	if !ok {
		return 0, false
	}
	i, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return 0, false
	}
	return i, true
}

// Float64 parses the VI's string-typed SourceRef as a float (E-8, #491) — this is how
// numeric aggAttr values (duration, and any other numeric span attribute) reach the
// accumulator during backfill. A parse failure (missing column, or a non-numeric SourceRef)
// returns (0, false), matching SpanValues' documented "absent" convention rather than an error —
// backfill has no way to distinguish "not present" from "present but malformed" once the value
// has already been stored as an opaque VI string, and the accumulator already treats
// Float64-not-ok as "no sample" for that attribute (E-4), which is the correct behavior here too.
func (v valueIndexSpanValues) Float64(col string) (float64, bool) {
	s, ok := v.vals[col]
	if !ok {
		return 0, false
	}
	f, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return 0, false
	}
	return f, true
}
