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

// ViBackfillDetail identifies which trace block a vi_backfill job processes (issue #533: one job
// per block; column identity lives entirely in vi_backfill_job_columns as membership rows, not
// here or on the parent job row at all). BlockObjectKey names the exact trace block object key to
// process -- exactly one block per job, no listing needed. WindowStartSec/WindowEndSec are the
// block's own real [MinSec, MaxSec) (not a synthetic wall-clock slice), so query-time coverage
// checks (ViBackfillGapRanges) can still range-filter on them directly.
type ViBackfillDetail struct {
	// BlockObjectKey is the exact trace block object key this job processes. Every row has one,
	// by construction -- there is no other job shape anymore.
	BlockObjectKey string `json:"block_object_key"`
	// BlockSizeBytes is the block's known object size (pgcatalog.Row.SizeBytes, captured at
	// enumeration time). Lets compactionworker.catalogDiskBlockProvider.Size() answer without
	// staging the block to disk at all when the size is already known -- required for issue
	// #530's cache to actually skip ALL I/O (not just the in-memory-buffering it was originally,
	// and incorrectly, designed around) when every section a job needs is already cached. Zero
	// (e.g. an older enqueued job predating this field) degrades gracefully to staging the block
	// to answer Size() instead.
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
