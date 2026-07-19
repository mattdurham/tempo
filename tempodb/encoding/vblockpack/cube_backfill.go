package vblockpack

// cube_backfill.go — value-index adapter construction for cube backfill, and the
// job-queue-dispatched RunCubeBackfill entry point.
//
// #508: viBackfillSource, decodeCanonicalVI, fetchVCNTSection, buildVCNTSection,
// runCubeBackfillCore, cubeBackfillMaxConsecutiveFailures, and LoadCubeEntry all moved into
// blockpack (cube_backfill_runner.go: newCubeVIBackfillSource, RunCubeBackfill, LoadCubeEntry).
// The background-goroutine launch that used to live here as launchBackfill (fired from the
// first-query creation trigger) is now inlined directly into cubequerypath.go's
// ConfigureCubeQueryPath OnCreateAttempt callback -- verified via grep that launchBackfill had
// exactly one remaining call site after Phase 11's rewrite, so a separate function would only
// add an indirection with no other caller.
//
// What remains here: the shared value-index store adapter (newBackfillVIStore, reused by both
// ConfigureCubeQueryPath's OnCreateAttempt closure and RunCubeBackfill below) and the
// job-queue-dispatched RunCubeBackfill entry point itself (used by
// modules/backendworker/backendworker.go).

import (
	"context"
	"errors"
	"math"
	"time"

	"github.com/go-kit/log/level"
	blockpack "github.com/grafana/blockpack"
	util_log "github.com/grafana/tempo/pkg/util/log"
	s3backend "github.com/grafana/tempo/tempodb/backend/s3"
	"github.com/jackc/pgx/v5/pgxpool"
	minio "github.com/minio/minio-go/v7"
)

// backfillListTTL bounds how long a viBackfillSource List(prefix) result is
// reused (blockpack issue #478). It must be short enough that a backfill/query
// still observes VI/VCNT files written after it starts within a bounded delay,
// but long enough to collapse the repeated re-lists of the same
// (tenant, column, type) prefix that a single backfill pass — or a burst of
// cardinality-gate TryCreate attempts before a cube is actually created — issues.
// 30s comfortably covers a single pass's re-list burst while keeping any
// staleness window small relative to the (now full-history) backfill window.
const backfillListTTL = 30 * time.Second

// backfillContentCacheBytes bounds the immutable VI/VCNT content cache shared by
// the cube-backfill/cardinality-gate reads (blockpack issue #478). VI/VCNT files
// are immutable once written, so a keyed cache needs no TTL; this budget caps how
// many are retained so re-scanning overlapping windows reuses the same file rather
// than re-downloading it. Sized well below the query-path default (defaultContentCacheBytes,
// 2 GiB) because this is a write/backfill-adjacent path, not the user-facing query hot path.
const backfillContentCacheBytes = 512 << 20 // 512 MiB

// newBackfillVIStore builds the shared value-index store the cube backfill /
// cardinality-gate path reads through (blockpack issue #478): a cachingStore
// wrapping the same minioVIStore the query path uses, with the immutable content
// cache (Get/Size/ReadAt) plus a short-TTL listing cache. Unifying on minioVIStore
// removes the third parallel object-store implementation this path used to carry.
func newBackfillVIStore(client *minio.Client, bucket string) valueIndexStore {
	return newCachingStoreWithListTTL(
		&minioVIStore{client: client, bucket: bucket},
		backfillContentCacheBytes,
		backfillListTTL,
	)
}

// RunCubeBackfill runs the cube backfill synchronously in the calling goroutine, blocking until
// the backfill is complete or ctx is done. Used by the backend-worker job executor
// (modules/backendworker/backendworker.go), unlike the OnCreateAttempt-triggered background
// backfill (cubequerypath.go), which fires-and-forgets. pgPool backs the cube registry (issue
// #504: Postgres is now the only supported cube registry backend); s3cfg remains required for
// the value-index read side and the cube file write side, which are unrelated to the registry
// backend. Returns a real error on any genuine failure (S3 client construction, or a
// blockpack.RunCubeBackfill error, including context cancellation/deadline) so the caller
// (processCubeBackfillJobPostgres) can report the job as failed rather than unconditionally as
// succeeded.
//
// retentionMinutes bounds the backfill window (2026-07-17 follow-up to the 2026-07-11
// "full history, not an artificial cap" ruling): blocks physically cannot exist past the
// tenant's own block retention, so a genuinely unbounded math.MaxUint32 window was never
// actually "full history" — it was "iterate one real minute at a time, serially, all the way
// back to whenever data first runs out", discovered live to be impractically slow once
// #512 exposed that Run had zero parallelism. retentionMinutes == 0 (retention disabled/
// unbounded for this tenant) preserves the original unbounded-window behavior; the caller is
// responsible for resolving the tenant's effective retention (per-tenant override, else the
// compactor's configured default).
func RunCubeBackfill(
	ctx context.Context, entry blockpack.CubeRegistryEntry, s3cfg *s3backend.Config, pgPool *pgxpool.Pool,
	jobWindowMinutes, retentionMinutes uint32,
) error {
	if s3cfg == nil {
		return nil
	}
	client, err := newMinioClientFromS3Config(s3cfg)
	if err != nil {
		level.Warn(util_log.Logger).Log("msg", "vblockpack: RunCubeBackfill: S3 client init failed", "err", err)
		return err
	}
	viStore := newBackfillVIStore(client, s3cfg.Bucket)
	store := &s3ObjectPutter{client: client, bucket: s3cfg.Bucket}
	cfg := blockpack.CubeBackfillConfig{
		Workers:       4,
		WindowMinutes: min(jobWindowMinutes, cubeBackfillWindowMinutes(retentionMinutes)),
	}
	metricCubeBackfillStarted.Inc()
	err = blockpack.RunCubeBackfill(ctx, entry, viStore, store, pgPool, cfg, currentMinuteAnchor(entry), defaultValueIndexPref)
	if err != nil {
		// A ctx cancellation/deadline is the caller's own decision, not a genuine backfill
		// failure -- mirrors this function's pre-#508 posture of not counting that case as
		// metricCubeBackfillFailed.
		if !errors.Is(err, ctx.Err()) {
			metricCubeBackfillFailed.Inc()
			level.Warn(util_log.Logger).Log("msg", "vblockpack: cube backfill error",
				"tenant", entry.Tenant, "cube_id", entry.CubeID, "err", err)
		}
		return err
	}
	// blockpack.RunCubeBackfill returns nil if and only if the full backfill window was
	// genuinely exhausted (SPEC-CUBE-032) -- the metric increment moves here, out of the
	// per-minute progress callback, since blockpack owns no metrics dependency.
	metricCubeBackfillCompleted.Inc()
	return nil
}

// cubeBackfillWindowMinutes converts a resolved tenant retention (in minutes, 0 meaning
// "retention disabled/unbounded for this tenant") into the WindowMinutes value RunCubeBackfill
// passes to blockpack.CubeBackfillConfig. Pure and extracted from RunCubeBackfill specifically
// so this one piece of logic is unit-testable without S3/ctx machinery (RunCubeBackfill itself
// has no fake-injection seam — see this file's own test file doc comment).
func cubeBackfillWindowMinutes(retentionMinutes uint32) uint32 {
	if retentionMinutes == 0 {
		return math.MaxUint32
	}
	return retentionMinutes
}

// currentMinuteAnchor resolves the resume point for a chained cube backfill (issue
// #518, Correction 1): the oldest minute already confirmed backfilled
// (Watermarks[CubeRollupL0].MinMinute), or 0 (meaning "anchor to wall-clock now",
// blockpack's own zero-value convention, cube_backfill_runner.go/backfill.go) for a
// cube with no L0 watermark yet -- i.e. its first backfill pass.
//
// Passing wm.MinMinute directly (not wm.MinMinute-1) is deliberate: Backfiller.Run's
// window is [currentMinute-1, currentMinute-WindowMinutes], so the NEXT window ends
// exactly at wm.MinMinute-1, one minute older than the last confirmed minute, with no
// gap and no redundant reprocessing of an already-covered minute.
func currentMinuteAnchor(entry blockpack.CubeRegistryEntry) uint32 {
	wm, ok := entry.Watermarks[blockpack.CubeRollupL0]
	if !ok {
		return 0
	}
	return wm.MinMinute
}
