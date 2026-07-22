package blockpack

// job_queue.go — public glue for internal/modules/pgqueue's compaction_jobs
// queue (issue #522), specifically the two job types with a reactive,
// query-path trigger: vi_backfill and cube_backfill. Every other job type in
// this queue (vi_compaction/vcnt_compaction/cube_compaction/trace_compaction/
// catalog_reconcile/catalog_reap) is planned and executed entirely within
// blockpack (compactionplanner/compactionworker) and needs no root export at
// all. vi_backfill/cube_backfill are different: the FIRST job for a given
// column/cube is inserted synchronously from tempo's query path (the instant
// a never-before-queried column/cube is seen) via Postgres.InsertViBackfillJob/
// InsertCubeBackfillJob (postgres.go).
//
// The chained-continuation half of vi_backfill/cube_backfill (planning the
// NEXT bounded-window job for a column/cube already triggered at least once)
// lives entirely inside compactionplanner and has no root export — only the
// reactive first-trigger insert crosses the repo boundary.

import (
	"time"

	"github.com/grafana/blockpack/internal/modules/pgqueue"
)

// ViBackfillDetail identifies which (tenant, column) a vi_backfill job indexes and the exact
// 1-minute window it covers.
type ViBackfillDetail = pgqueue.ViBackfillDetail

// ViBackfillColumn identifies the (tenant, column) a batch of vi_backfill windows covers.
type ViBackfillColumn = pgqueue.ViBackfillColumn

// CubeBackfillDetail identifies which (tenant, cube) a cube_backfill job
// processes, and how far back.
type CubeBackfillDetail = pgqueue.CubeBackfillDetail

// WindowRange is one contiguous [StartSec, EndSec) span -- ViBackfillGapRanges's return shape.
type WindowRange = pgqueue.WindowRange

// ViBackfillWindowsForRetention generates one job-window per 1-minute slice covering
// [now-retention, now), newest-first -- the exact set InsertViBackfillHistory bulk-inserts.
// Exported so callers computing their own retention-driven window sets (or just inspecting the
// count/shape in tests) don't need to reach into internal/modules/pgqueue directly.
func ViBackfillWindowsForRetention(retention time.Duration, now time.Time) []pgqueue.WindowSpec {
	return pgqueue.WindowsForRetention(retention, now)
}
