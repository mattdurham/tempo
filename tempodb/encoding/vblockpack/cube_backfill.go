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
func RunCubeBackfill(ctx context.Context, entry blockpack.CubeRegistryEntry, s3cfg *s3backend.Config, pgPool *pgxpool.Pool) error {
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
		Workers: 4,
		// WindowMinutes: full history, not an artificial cap (2026-07-11 ruling) --
		// math.MaxUint32 minutes trivially exceeds any real currentMinute, so
		// Backfiller.Run's endMinute always resolves to 0. Bounded only by how far
		// back the value index itself actually has data, not by this config.
		WindowMinutes: math.MaxUint32,
	}
	metricCubeBackfillStarted.Inc()
	err = blockpack.RunCubeBackfill(ctx, entry, viStore, store, pgPool, cfg, 0, defaultValueIndexPref)
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
