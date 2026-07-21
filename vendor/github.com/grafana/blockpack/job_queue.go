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
	"github.com/grafana/blockpack/internal/modules/pgqueue"
)

// ViBackfillDetail identifies which (tenant, column) a vi_backfill job
// historically indexes, and how far back.
type ViBackfillDetail = pgqueue.ViBackfillDetail

// CubeBackfillDetail identifies which (tenant, cube) a cube_backfill job
// processes, and how far back.
type CubeBackfillDetail = pgqueue.CubeBackfillDetail
