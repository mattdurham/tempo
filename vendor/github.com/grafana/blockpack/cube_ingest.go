package blockpack

// cube_ingest.go — public surface for the metrics-cube ingest pipeline (#441–#453).
// Re-exports the types tempo needs to accumulate per-minute span counts into cubes
// and flush L0 cube files to S3.
//
// Cube files are written to:
//   <tenant>/cubes/<hex_cube_id>/L0-<xid>.cube

import (
	"context"

	"github.com/grafana/blockpack/internal/modules/cube"
)

// CubeSpanValues abstracts per-span field access for cube accumulation.
// Tempo implements this over an OTLP span + resource.
type CubeSpanValues = cube.SpanValues

// CubeDefinition is the minimal cube description the accumulator needs at ingest
// time: two dimension columns, optional filters, cube ID, and resolution (minutes).
type CubeDefinition = cube.Definition

// CubeRegistryEntry is the stable S3-persisted cube descriptor stored in
// <tenant>/cubes/index.json.
type CubeRegistryEntry = cube.RegistryEntry

// CubeObjectPutter writes an encoded cube file to S3.
// blockpack.ObjectPutter satisfies this interface.
type CubeObjectPutter = cube.ObjectPutter

// CubeFilter is a per-span predicate baked into a cube definition.
type CubeFilter = cube.Filter

// CubeFilterOp is a comparison operator for numeric filters.
type CubeFilterOp = cube.FilterOp

// CubeColumnFilter is a serialized filter baked into a RegistryEntry.
type CubeColumnFilter = cube.ColumnFilter

// CubeDefFilterOp is the serialized comparison operator stored in a
// CubeColumnFilter (the stable wire/JSON form persisted in index.json).
// External callers (tempo) construct CubeColumnFilter values to differentiate
// cubes by their originating query filter; they need this type and its
// constants to set CubeColumnFilter.Op. See issue #480.
type CubeDefFilterOp = cube.DefFilterOp

const (
	// CubeDefFilterOpGT is the ">" operator in a cube definition filter.
	CubeDefFilterOpGT = cube.DefFilterOpGT
	// CubeDefFilterOpGTE is the ">=" operator in a cube definition filter.
	CubeDefFilterOpGTE = cube.DefFilterOpGTE
	// CubeDefFilterOpLT is the "<" operator in a cube definition filter.
	CubeDefFilterOpLT = cube.DefFilterOpLT
	// CubeDefFilterOpLTE is the "<=" operator in a cube definition filter.
	CubeDefFilterOpLTE = cube.DefFilterOpLTE
	// CubeDefFilterOpEQ is the "=" operator in a cube definition filter.
	CubeDefFilterOpEQ = cube.DefFilterOpEQ
)

// CubeObjectStore is the minimal S3-compatible interface the CubeRegistry needs.
type CubeObjectStore = cube.ObjectStore

// CubeErrConflict is returned by CubeObjectStore.ConditionalPut on a 412 conflict.
var CubeErrConflict = cube.ErrConflict

// CubeHeaderSize is the number of bytes in a cube file header.
// A ranged GET of the first CubeHeaderSize bytes is sufficient to read
// MinMinute, MaxMinute, and Resolution without downloading the whole file.
const CubeHeaderSize = cube.HeaderSize

// CubeReadHeader parses the first 36 bytes of a cube file into its header fields.
// Use this after a ranged GET of the first CubeHeaderSize bytes to discover
// a file's time range and compaction level without reading the full file.
func CubeReadHeader(buf []byte) (minMinute, maxMinute, resolution uint32, err error) {
	h, err := cube.DecodeHeader(buf)
	if err != nil {
		return 0, 0, 0, err
	}
	return h.MinMinute, h.MaxMinute, h.Resolution, nil
}

// CubeComputeID returns the deterministic hex cube ID for a
// (tenant, dimensions, filters) combination. It is the routing key: two queries
// sharing the same tenant+dims but differing in filters produce different IDs, so a
// filtered cube is never reused for a query with a different filter (issue #480).
func CubeComputeID(tenant string, dimensions []string, filters []CubeColumnFilter) string {
	return cube.ComputeCubeID(tenant, dimensions, filters)
}

// CubeIDFromHex parses a hex cube ID string into its [16]byte representation.
// Used when the caller has a CubeRegistryEntry.CubeID string and needs the
// binary form for Compactor.Execute.
func CubeIDFromHex(cubeID string) ([16]byte, error) {
	return cube.IDFromBytes(cubeID)
}

// CubeRegistry loads and persists the per-tenant cube index from
// <tenant>/cubes/index.json.
type CubeRegistry = cube.Registry

// NewCubeRegistry creates a Registry backed by store for the given tenant.
func NewCubeRegistry(store CubeObjectStore, tenant string) *CubeRegistry {
	return cube.NewRegistry(store, tenant)
}

// CubeReader reads cells from an in-memory cube file.
type CubeReader = cube.Reader

// OpenCubeReaderFromBytes opens a CubeReader from in-memory bytes (e.g. from S3).
func OpenCubeReaderFromBytes(data []byte) (*CubeReader, error) {
	return cube.OpenReaderFromBytes(data)
}

// CubeRollupInput is one file's contribution to a rollup merge.
type CubeRollupInput = cube.RollupInput

// CubeNewRollupInput builds a RollupInput from a CubeReader.
func CubeNewRollupInput(r *CubeReader) CubeRollupInput {
	return cube.NewRollupInput(r)
}

// CubeMergedCell is one cell in the rolled-up result.
type CubeMergedCell = cube.MergedCell

// CubeRollup merges cells from multiple readers into a single sorted slice.
func CubeRollup(inputs []CubeRollupInput, targetLevel, minMinute, maxMinute uint32) ([]CubeMergedCell, error) {
	return cube.Rollup(inputs, targetLevel, minMinute, maxMinute)
}

// CubeQueryRouter resolves a (tenant, dims, filters, resolution) to the best matching cube.
type CubeQueryRouter = cube.QueryRouter

// NewCubeQueryRouter creates a router from a pre-loaded registry snapshot.
func NewCubeQueryRouter(entries []CubeRegistryEntry) *CubeQueryRouter {
	return cube.NewQueryRouter(entries)
}

// CubeRoutingResult is the outcome of a CubeQueryRouter.Route call.
type CubeRoutingResult = cube.RoutingResult

// CubeCreationTrigger checks the cardinality gate and registers a new cube.
type CubeCreationTrigger = cube.CreationTrigger

// NewCubeCreationTrigger creates a CreationTrigger backed by the given registry.
func NewCubeCreationTrigger(reg *CubeRegistry, cfg cube.TriggerConfig) *CubeCreationTrigger {
	return cube.NewCreationTrigger(reg, cfg)
}

// CubeTriggerConfig parameterises the creation trigger (cardinality limits + max cubes).
type CubeTriggerConfig = cube.TriggerConfig

// CubeTriggerResult is returned by CubeCreationTrigger.TryCreate.
type CubeTriggerResult = cube.TriggerResult

// CubeErrLimitReached is returned when the per-tenant cube limit is exhausted.
type CubeErrLimitReached = cube.ErrLimitReached

// CubeCompactor merges small L0 cube files and rolls up to L1/L2.
type CubeCompactor = cube.Compactor

// CubeCompactorConfig parameterises the compactor.
type CubeCompactorConfig = cube.CompactorConfig

// CubeFileInfo describes one cube file in object storage.
type CubeFileInfo = cube.FileInfo

// CubeFileStore is the minimal S3-compatible interface the compactor needs.
type CubeFileStore = cube.FileStore

// CubeCompactionPlan describes one planned merge.
type CubeCompactionPlan = cube.CompactionPlan

// NewCubeCompactor creates a cube compactor.
func NewCubeCompactor(store CubeFileStore, registry *CubeRegistry, cfg CubeCompactorConfig) *CubeCompactor {
	return cube.NewCompactor(store, registry, cfg)
}

// PlanCubeL0Merge returns compaction plans for L0 files covering a complete hour.
func PlanCubeL0Merge(files []CubeFileInfo, threshold int, tenant, cubeID string) []CubeCompactionPlan {
	return cube.PlanL0Merge(files, threshold, tenant, cubeID)
}

// PlanCubeL1Rollup returns input keys to roll up one hour of L0 files into an L1 file.
func PlanCubeL1Rollup(files []CubeFileInfo, hourStart uint32, tenant, cubeID string) ([]string, bool) {
	return cube.PlanL1Rollup(files, hourStart, tenant, cubeID)
}

// CubeBackfiller processes historical backfill for one cube from the value index.
type CubeBackfiller = cube.Backfiller

// CubeBackfillConfig configures the backfill worker.
type CubeBackfillConfig = cube.BackfillConfig

// CubeBackfillProgress reports backfill state for one cube.
type CubeBackfillProgress = cube.BackfillProgress

// CubeBackfillWatermark tracks the oldest minute that has been backfilled.
type CubeBackfillWatermark = cube.BackfillWatermark

// CubeValueIndexSource provides per-column value-index data for backfill reads.
type CubeValueIndexSource = cube.ValueIndexSource

// NewCubeBackfiller creates a Backfiller for the given registry entry.
func NewCubeBackfiller(entry CubeRegistryEntry, src CubeValueIndexSource, cfg CubeBackfillConfig) *CubeBackfiller {
	return cube.NewBackfiller(entry, src, cfg)
}

// LoadCubeDefinitions loads active cube definitions from the registry and
// converts them to CubeDefinition values ready for NewCubeAccumulator.
// filterFn is called once per RegistryEntry filter to build runtime predicates;
// pass nil to skip all filters.
func LoadCubeDefinitions(
	ctx context.Context,
	reg *CubeRegistry,
	filterFn func(CubeColumnFilter) CubeFilter,
) ([]CubeDefinition, error) {
	entries, _, err := reg.Load(ctx)
	if err != nil {
		return nil, err
	}
	defs := make([]CubeDefinition, 0, len(entries))
	for _, e := range entries {
		def, derr := CubeRegistryEntryToDefinition(e, filterFn)
		if derr != nil {
			continue // skip malformed entries
		}
		defs = append(defs, def)
	}
	return defs, nil
}

const (
	// CubeFilterOpLess accepts spans where the int64 column value is < threshold.
	CubeFilterOpLess = cube.FilterOpLess
	// CubeFilterOpLessEqual accepts spans where the int64 column value is <= threshold.
	CubeFilterOpLessEqual = cube.FilterOpLessEqual
	// CubeFilterOpGreater accepts spans where the int64 column value is > threshold.
	CubeFilterOpGreater = cube.FilterOpGreater
	// CubeFilterOpGreaterEqual accepts spans where the int64 column value is >= threshold.
	CubeFilterOpGreaterEqual = cube.FilterOpGreaterEqual
	// CubeFilterOpEqual accepts spans where the int64 column value equals threshold.
	CubeFilterOpEqual = cube.FilterOpEqual
)

// CubeAccumulator is an in-memory per-minute span counter for one cube.
type CubeAccumulator = cube.Accumulator

// NewCubeAccumulator creates an in-memory accumulator for one cube and one minute
// bucket. Call Add(span) per span; call FlushTo(store, tenant) at minute rotation.
// Not safe for concurrent use.
func NewCubeAccumulator(def CubeDefinition, minute uint32) *CubeAccumulator {
	return cube.NewAccumulator(def, minute)
}

// NewCubeNumericFilter builds a CubeFilter that keeps a span only when its int64
// column satisfies (value op threshold).
func NewCubeNumericFilter(column string, op CubeFilterOp, threshold int64) CubeFilter {
	return cube.NumericFilter(column, op, threshold)
}

// CubeRegistryEntryToDefinition converts a RegistryEntry to a Definition suitable
// for NewCubeAccumulator. It maps Dimensions[0] → Dim1Column, Dimensions[1] →
// Dim2Column, and Filters to runtime Filter predicates from filterFn.
// filterFn is called once per RegistryEntry filter; return nil to skip that filter.
func CubeRegistryEntryToDefinition(
	entry CubeRegistryEntry,
	filterFn func(cube.ColumnFilter) CubeFilter,
) (CubeDefinition, error) {
	id, err := cube.IDFromBytes(entry.CubeID)
	if err != nil {
		return CubeDefinition{}, err
	}
	def := CubeDefinition{
		ID:         id,
		Resolution: entry.Resolution,
	}
	if len(entry.Dimensions) >= 1 {
		def.Dim1Column = entry.Dimensions[0]
	}
	if len(entry.Dimensions) >= 2 {
		def.Dim2Column = entry.Dimensions[1]
	} else {
		// Single-dimension cube: use a fixed sentinel for dim2 so the accumulator
		// does not skip spans (Accumulator.Add requires both dims present).
		def.Dim2Column = "__all__"
	}
	if filterFn != nil {
		for _, cf := range entry.Filters {
			if f := filterFn(cf); f != nil {
				def.Filters = append(def.Filters, f)
			}
		}
	}
	return def, nil
}
