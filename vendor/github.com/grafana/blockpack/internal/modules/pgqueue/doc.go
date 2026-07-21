// Package pgqueue is the Postgres-backed durable job queue for blockpack's
// compaction-planner/compaction-worker (compaction_jobs table, issue #522
// Section G.2). A near-verbatim port of tempo's own proven
// tempodb/encoding/vblockpack/jobstore package -- blockpack cannot import
// tempo's package across the repo/module boundary, so this is a fresh
// implementation of the identical SELECT ... FOR UPDATE SKIP LOCKED design,
// not a novel one.
//
// Deliberately generic across job_type from day one: Claim has no job_type
// filter, so a caller claims the oldest claimable row of ANY type and
// dispatches on the returned JobType -- mirrors the "any pod, any job"
// philosophy already settled for tempo's own backend-worker.
package pgqueue
