package pgqueue

// details.go — JSONB detail shapes for each JobType, mirroring tempo
// jobstore.go's identical CatalogReconcileDetail/CatalogReapDetail/
// ViCompactionDetail shapes (issue #522 Section G.2 is a near-verbatim port).

// CatalogReconcileDetail identifies which (subsystem, tenant) pair
// compaction-worker's catalog_reconcile handler reconciles.
type CatalogReconcileDetail struct {
	Subsystem string `json:"subsystem"` // "vi" | "vcnt" | "cube"
	Tenant    string `json:"tenant"`
}

// CatalogReapDetail identifies one blockpack_file_catalog row
// compaction-worker should physically delete (its object, then the row
// itself) once its grace window has elapsed.
type CatalogReapDetail struct {
	Subsystem string `json:"subsystem"`
	ObjectKey string `json:"object_key"`
	RowID     int64  `json:"row_id"`
}

// ViCompactionDetail identifies the exactly-2 same-level VI input objects
// compaction-worker's vi_compaction handler merges (issue #522 Phase 1.2/1.3,
// retargeted from tempo's now-deleted modules/jobplanner/plan_vi_compaction.go/
// modules/backendworker/vi_compaction.go, algorithm unchanged).
type ViCompactionDetail struct {
	ResourceID      string   `json:"resource_id"`
	InputObjectKeys []string `json:"input_object_keys"`
	Level           int      `json:"level"`
}

// VcntCompactionDetail identifies the exactly-2 same-level VCNT input objects
// compaction-worker's vcnt_compaction handler merges (issue #522 Phase 2.2,
// blockpack-native from the start -- mirrors ViCompactionDetail's shape).
type VcntCompactionDetail struct {
	ResourceID      string   `json:"resource_id"`
	InputObjectKeys []string `json:"input_object_keys"`
	Level           int      `json:"level"`
}

// CubeCompactionDetail identifies the exactly-2 same-level cube input objects
// compaction-worker's cube_compaction handler merges (issue #522 Phase 3.2,
// blockpack-native from the start -- mirrors ViCompactionDetail/VcntCompactionDetail's shape).
// ResourceID is the cube's CubeID.
type CubeCompactionDetail struct {
	ResourceID      string   `json:"resource_id"`
	InputObjectKeys []string `json:"input_object_keys"`
	Level           int      `json:"level"`
}

// TraceCompactionDetail identifies the exactly-2 same-tenant, same-window trace/span
// block data-file objects compaction-worker's trace_compaction handler merges (trace/span
// compaction moved fully into blockpack, retargeted from tempo's now-deleted
// modules/jobplanner/plan_trace_compaction.go/modules/backendworker/trace_compaction.go).
// Unlike VI/VCNT/cube, there is no ResourceID dimension -- trace/span blocks are grouped
// purely by (tenant, time-window), not by any per-column/per-cube resource.
// InputObjectKeys are each input block's data-file key ("<tenant>/<blockID>/data.blockpack"),
// not a bare block ID -- consistent with every other job type's object-key-based candidacy.
type TraceCompactionDetail struct {
	InputObjectKeys []string `json:"input_object_keys"`
	Level           int      `json:"level"`
}

// ViBackfillDetail identifies which (tenant, column) a vi_backfill job indexes, and the exact
// span it covers -- either shape below may appear in a live compaction_jobs row; compaction-
// worker's dispatch (compactionworker.processViBackfillJob) branches on which one a row carries.
//
// Two coexisting job shapes (issue #532 transition):
//
//   - OLD, window-shaped (issue #529): BlockObjectKey is empty. [WindowStartSec, WindowEndSec) is
//     a fixed, synthetic 1-minute wall-clock slice; the worker lists every trace block
//     overlapping it and processes all of them. Still fully supported for DRAINING already
//     in-flight rows (a live cluster can carry hundreds of thousands of these at migration
//     time) -- no new rows of this shape are created once #532 ships.
//   - NEW, block-shaped (issue #532): BlockObjectKey names the exact trace block object key to
//     process -- exactly one block per job, no listing needed at all. WindowStartSec/
//     WindowEndSec are still populated (from the block's own real MinSec/MaxSec, not a
//     synthetic slice) so query-time coverage checks (ViBackfillGapRanges) keep working
//     unchanged across both shapes without knowing which one produced a given row.
//
// Rationale for the redesign: blocks don't align to 1-minute wall-clock boundaries, so the old
// model routinely had MULTIPLE 1-minute-window jobs (even within a single column's own
// backfill) redundantly re-list and re-fetch the SAME underlying block. Per-block jobs make
// "has this block been processed for this column" the unit of dedup/coverage instead.
type ViBackfillDetail struct {
	ColumnHash string `json:"column_hash"`
	ColumnName string `json:"column_name"`
	ColumnType string `json:"column_type"`
	// BlockObjectKey is empty for an OLD window-shaped job, non-empty (the exact trace block's
	// object key) for a NEW block-shaped job (issue #532). This is the field
	// compactionworker's dispatch branches on.
	BlockObjectKey string `json:"block_object_key,omitempty"`
	// BlockSizeBytes is the block's known object size (pgcatalog.Row.SizeBytes, captured at
	// enumeration time), populated ONLY alongside BlockObjectKey. Lets
	// compactionworker.catalogDiskBlockProvider.Size() answer without staging the block to
	// disk at all when the size is already known -- required for issue #530's cache to
	// actually skip ALL I/O (not just the in-memory-buffering it was originally, and
	// incorrectly, designed around) when every section a job needs is already cached. Zero
	// (e.g. an older enqueued job predating this field) degrades gracefully to staging the
	// block to answer Size(), exactly like an unset hint already does for the OLD
	// window-shaped path's own per-listed-block knownSizes map.
	BlockSizeBytes int64 `json:"block_size_bytes,omitempty"`
	WindowStartSec int64 `json:"window_start_sec"`
	WindowEndSec   int64 `json:"window_end_sec"`
}

// CubeBackfillDetail identifies which (tenant, cube) a cube_backfill job
// processes, and how far back -- mirrors tempo jobstore.go's now-retired
// identical struct.
type CubeBackfillDetail struct {
	CubeID        string `json:"cube_id"`
	WindowMinutes uint32 `json:"window_minutes"`
}
