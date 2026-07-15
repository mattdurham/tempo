package vblockpack

// cube_scheduler.go — background rollup-ladder job (#491, E-12b).
//
// ConfigureCubeScheduler ticks periodically and, for every active cube of every configured
// tenant: merges small L0 files on the same threshold-based cadence the old CubeCompactorService
// used (no bug there — merging same-resolution L0 files never loses data regardless of hour
// completeness), then rolls up FULLY ELAPSED hour boundaries into L1 and FULLY ELAPSED day
// boundaries into L2, then evicts L0 files that Compactor.EvictAgedL0
// (internal/modules/cube/compactor.go, E-12a) confirms are both past retention and already
// reflected in the cube's L1 watermark. A boundary that has not fully elapsed is never rolled up.
//
// This is now the SOLE cube compaction driver, replacing cube_compactor.go's former
// CubeCompactorService (deleted; see cube_compactor.go's doc comment for why): that service's
// L1-rollup loop had no boundary-completeness gate, so it rolled up any hour with >=2 L0 files
// regardless of whether the hour had finished, permanently undercounting that hour's L1 data.
// This had been harmless only because tryQueryFromCube hardcoded resolution=1 (L0) — L1 was
// write-only until E-10/E-6b started actually routing queries to it. Wired from
// cmd/tempo/app/value_index.go in place of the old service.

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/go-kit/log/level"
	blockpack "github.com/grafana/blockpack"
	util_log "github.com/grafana/tempo/pkg/util/log"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/minio/minio-go/v7"
	"golang.org/x/sync/errgroup"
)

const (
	// cubeLevelL0/L1/L2 alias blockpack's canonical rollup-level constants (#491 Phase E fix
	// pass, go-presubmit.md #3) rather than independently duplicating the same three literal
	// values — previously this package and cube_compactor.go's cubeTierToLevel each maintained
	// their own copy of 1/60/1440 with no compile-time link back to blockpack's definition.
	cubeLevelL0 = blockpack.CubeRollupL0
	cubeLevelL1 = blockpack.CubeRollupL1
	cubeLevelL2 = blockpack.CubeRollupL2

	defaultCubeSchedulerTick        = 10 * time.Minute
	defaultCubeSchedulerConcurrency = 4
	// defaultL0MergeThreshold matches the old CubeCompactorService's hardcoded threshold exactly,
	// preserving its (unproblematic — same-resolution consolidation never loses data) cadence.
	defaultL0MergeThreshold = 10
)

// CubeSchedulerConfig parameterises the background rollup-ladder job.
type CubeSchedulerConfig struct {
	// TickInterval is how often the scheduler scans for fully-elapsed boundaries. Default 10min.
	TickInterval time.Duration
	// Compactor is passed straight through to blockpack.NewCubeCompactor (retention, thresholds).
	Compactor blockpack.CubeCompactorConfig
	// Concurrency bounds simultaneous tenant processing. Default 4.
	Concurrency int
}

func (c *CubeSchedulerConfig) setDefaults() {
	if c.TickInterval <= 0 {
		c.TickInterval = defaultCubeSchedulerTick
	}
	if c.Concurrency <= 0 {
		c.Concurrency = defaultCubeSchedulerConcurrency
	}
	if c.Compactor.L0MergeThreshold <= 0 {
		c.Compactor.L0MergeThreshold = defaultL0MergeThreshold
	}
}

// CubeScheduler drives the periodic L0->L1 hourly / L1->L2 daily rollup ladder plus L0 eviction.
type CubeScheduler struct {
	client  *minio.Client
	bucket  string
	tenants []string
	cfg     CubeSchedulerConfig
	// pgPool backs the cube registry (issue #504: Postgres is now the only supported cube
	// registry backend, no blob/index.json fallback). Required whenever the scheduler actually
	// runs -- tempodb/config.go's validateConfig hard-fails at startup if CubeTenants is
	// non-empty with cfg.Postgres == nil, so this is never nil in a valid production config.
	pgPool *pgxpool.Pool
	// nowFunc returns the current wall-clock minute; overridable in tests for deterministic
	// boundary-completeness assertions.
	nowFunc func() uint32
	// processTenantFn processes one tenant's cube compaction+rollup ladder. Defaults to
	// s.processTenant; overridable in tests to inject a panicking fake without a real S3 backend
	// (mirrors nowFunc's existing testability pattern, #491 Phase E fix pass, go-presubmit.md #4).
	processTenantFn func(ctx context.Context, tenant string, nowMinute uint32)
}

// ConfigureCubeScheduler creates a CubeScheduler for the given tenants. pgPool is the required
// Postgres connection pool backing the cube registry (issue #504: Postgres is now the only
// supported cube registry backend). pgPool must be non-nil whenever the scheduler is actually
// wired up (see the CubeScheduler.pgPool field doc comment) -- blockpack.NewPgCubeRegistry's
// underlying entry store dereferences the pool directly with no nil-guard, so a nil pgPool here
// would panic on the scheduler's first tick rather than degrade gracefully.
func ConfigureCubeScheduler(client *minio.Client, bucket string, tenants []string, cfg CubeSchedulerConfig, pgPool *pgxpool.Pool) *CubeScheduler {
	cfg.setDefaults()
	level.Info(util_log.Logger).Log("msg", "vblockpack: cube scheduler configured",
		"tenants", strings.Join(tenants, ","), "tick_interval", cfg.TickInterval)
	cs := &CubeScheduler{
		client:  client,
		bucket:  bucket,
		tenants: tenants,
		cfg:     cfg,
		pgPool:  pgPool,
		nowFunc: wallMinute,
	}
	cs.processTenantFn = cs.processTenant
	return cs
}

// Run drives the scheduling loop until ctx is done.
func (s *CubeScheduler) Run(ctx context.Context) {
	ticker := time.NewTicker(s.cfg.TickInterval)
	defer ticker.Stop()
	s.runOnce(ctx)
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.runOnce(ctx)
		}
	}
}

func (s *CubeScheduler) runOnce(ctx context.Context) {
	nowMinute := s.nowFunc()
	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(s.cfg.Concurrency)
	for _, tenant := range s.tenants {
		g.Go(func() (err error) {
			// Panic isolation: errgroup.WithContext cancels gctx the instant any goroutine
			// returns a non-nil error, which would abort sibling tenants' in-flight work.
			// A panic in one tenant's processing must never propagate as a cancelling error.
			defer func() {
				if r := recover(); r != nil {
					level.Error(util_log.Logger).Log("msg", "vblockpack: cube scheduler panic recovered",
						"tenant", tenant, "panic", r)
					err = nil
				}
			}()
			s.processTenantFn(gctx, tenant, nowMinute)
			return nil
		})
	}
	_ = g.Wait()
}

func (s *CubeScheduler) processTenant(ctx context.Context, tenant string, nowMinute uint32) {
	reg := blockpack.NewPgCubeRegistry(s.pgPool, tenant)
	entries, _, err := reg.Load(ctx)
	if err != nil {
		level.Warn(util_log.Logger).Log("msg", "vblockpack: cube scheduler load registry failed",
			"tenant", tenant, "err", err)
		return
	}
	if len(entries) == 0 {
		return
	}

	store := &cubeFileStore{client: s.client, bucket: s.bucket}
	compactor := blockpack.NewCubeCompactor(store, reg, s.cfg.Compactor)
	for _, entry := range entries {
		processCube(ctx, tenant, entry, store, compactor, s.cfg.Compactor.L0MergeThreshold, nowMinute)
	}
}

// processCube runs the full compaction+rollup ladder for one cube: small-L0-file merging on a
// threshold cadence, L0->L1 for every fully-elapsed hour, L1->L2 for every fully-elapsed day, then
// L0 eviction. store/compactor are passed as parameters (rather than read off *CubeScheduler) so
// tests can exercise this pure orchestration logic against a fake blockpack.CubeFileStore without
// touching minio.
func processCube(
	ctx context.Context,
	tenant string,
	entry blockpack.CubeRegistryEntry,
	store blockpack.CubeFileStore,
	compactor *blockpack.CubeCompactor,
	l0MergeThreshold int,
	nowMinute uint32,
) {
	id, err := blockpack.CubeIDFromHex(entry.CubeID)
	if err != nil {
		level.Warn(util_log.Logger).Log("msg", "vblockpack: cube scheduler parse id failed",
			"cube_id", entry.CubeID, "err", err)
		return
	}
	paddedCubeID := padCubeID(entry.CubeID)

	files, err := store.List(ctx, tenant, entry.CubeID)
	if err != nil {
		level.Warn(util_log.Logger).Log("msg", "vblockpack: cube scheduler list failed",
			"tenant", tenant, "cube_id", entry.CubeID, "err", err)
		return
	}

	mergeSmallL0Files(ctx, tenant, paddedCubeID, id, files, compactor, l0MergeThreshold)

	// Re-list: the merge pass may have consolidated small L0 files into fewer, larger ones.
	files, err = store.List(ctx, tenant, entry.CubeID)
	if err != nil {
		level.Warn(util_log.Logger).Log("msg", "vblockpack: cube scheduler re-list after L0 merge failed",
			"tenant", tenant, "cube_id", entry.CubeID, "err", err)
		return
	}

	rollUpCompletedHours(ctx, tenant, paddedCubeID, id, files, compactor, nowMinute)

	// Re-list: the L0->L1 pass may have written new L1 files (their L0 inputs are, per E-12a,
	// retention-decoupled and deleted later by EvictAgedL0 below, not immediately).
	files, err = store.List(ctx, tenant, entry.CubeID)
	if err != nil {
		level.Warn(util_log.Logger).Log("msg", "vblockpack: cube scheduler re-list after L1 rollup failed",
			"tenant", tenant, "cube_id", entry.CubeID, "err", err)
		return
	}

	rollUpCompletedDays(ctx, tenant, paddedCubeID, id, files, compactor, nowMinute)

	// Re-list once more before eviction: L1->L2 does not affect which L0 files exist, but keeps
	// this pass's view of the world consistent with whatever the ladder above just wrote.
	files, err = store.List(ctx, tenant, entry.CubeID)
	if err != nil {
		level.Warn(util_log.Logger).Log("msg", "vblockpack: cube scheduler re-list before eviction failed",
			"tenant", tenant, "cube_id", entry.CubeID, "err", err)
		return
	}

	evictAgedL0Files(ctx, entry.CubeID, files, compactor, nowMinute)
}

// mergeSmallL0Files consolidates small same-minute L0 files into fewer, larger L0 files whenever
// an hour has accumulated >= threshold of them (blockpack.PlanCubeL0Merge's own grouping). Unlike
// the hourly/daily rollup steps below, this has no boundary-completeness gate — it never changes
// resolution (input and output are both L0), so merging mid-hour loses nothing: the merged file
// still serves identical minute-resolution reads.
func mergeSmallL0Files(
	ctx context.Context,
	tenant, paddedCubeID string,
	id [16]byte,
	files []blockpack.CubeFileInfo,
	compactor *blockpack.CubeCompactor,
	threshold int,
) {
	for _, plan := range blockpack.PlanCubeL0Merge(files, threshold, tenant, paddedCubeID) {
		if err := compactor.Execute(ctx, id, plan); err != nil {
			level.Warn(util_log.Logger).Log("msg", "vblockpack: cube scheduler L0 merge failed",
				"cube_id", paddedCubeID, "err", err)
		}
	}
}

// rollUpCompletedHours rolls up every hour boundary that both has L0 data and has fully elapsed
// (nowMinute >= hourStart+60) into a new L1 file via compactor.Execute.
func rollUpCompletedHours(
	ctx context.Context,
	tenant, paddedCubeID string,
	id [16]byte,
	files []blockpack.CubeFileInfo,
	compactor *blockpack.CubeCompactor,
	nowMinute uint32,
) {
	for _, hourStart := range completedHourStarts(files, nowMinute) {
		keys, ok := blockpack.PlanCubeL1Rollup(files, hourStart, tenant, paddedCubeID)
		if !ok {
			continue
		}
		plan := blockpack.CubeCompactionPlan{
			InputKeys: keys,
			OutputKey: fmt.Sprintf("%s/cubes/%s/L1-%d-%d-%s.cube",
				tenant, paddedCubeID, hourStart, hourStart+cubeLevelL1-1, blockpack.VCNTNewID()),
			Level:     cubeLevelL1,
			MinMinute: hourStart,
			MaxMinute: hourStart + cubeLevelL1 - 1,
		}
		if err := compactor.Execute(ctx, id, plan); err != nil {
			level.Warn(util_log.Logger).Log("msg", "vblockpack: cube scheduler hourly rollup failed",
				"cube_id", paddedCubeID, "hour_start", hourStart, "err", err)
		}
	}
}

// rollUpCompletedDays rolls up every day boundary that both has L1 data and has fully elapsed
// (nowMinute >= dayStart+1440) into a new L2 file via compactor.Execute.
func rollUpCompletedDays(
	ctx context.Context,
	tenant, paddedCubeID string,
	id [16]byte,
	files []blockpack.CubeFileInfo,
	compactor *blockpack.CubeCompactor,
	nowMinute uint32,
) {
	for _, dayStart := range completedDayStarts(files, nowMinute) {
		keys, ok := planDayRollup(files, dayStart)
		if !ok {
			continue
		}
		plan := blockpack.CubeCompactionPlan{
			InputKeys: keys,
			OutputKey: fmt.Sprintf("%s/cubes/%s/L2-%d-%d-%s.cube",
				tenant, paddedCubeID, dayStart, dayStart+cubeLevelL2-1, blockpack.VCNTNewID()),
			Level:     cubeLevelL2,
			MinMinute: dayStart,
			MaxMinute: dayStart + cubeLevelL2 - 1,
		}
		if err := compactor.Execute(ctx, id, plan); err != nil {
			level.Warn(util_log.Logger).Log("msg", "vblockpack: cube scheduler daily rollup failed",
				"cube_id", paddedCubeID, "day_start", dayStart, "err", err)
		}
	}
}

// evictAgedL0Files calls compactor.EvictAgedL0 for every remaining L0 file; EvictAgedL0 itself
// enforces the retention-and-already-rolled-up-into-L1 gate (E-12a) — this loop only supplies the
// candidate L0 files.
func evictAgedL0Files(
	ctx context.Context,
	cubeID string,
	files []blockpack.CubeFileInfo,
	compactor *blockpack.CubeCompactor,
	nowMinute uint32,
) {
	for _, f := range files {
		if f.Level != cubeLevelL0 {
			continue
		}
		if err := compactor.EvictAgedL0(ctx, cubeID, f, nowMinute); err != nil {
			level.Warn(util_log.Logger).Log("msg", "vblockpack: cube scheduler L0 eviction failed",
				"cube_id", cubeID, "key", f.Key, "err", err)
		}
	}
}

// hourBoundaryComplete reports whether the hour starting at hourStart minutes has FULLY elapsed
// as of nowMinute. A boundary that has not fully elapsed is never rolled up.
func hourBoundaryComplete(hourStart, nowMinute uint32) bool {
	return nowMinute >= hourStart+cubeLevelL1
}

// dayBoundaryComplete reports whether the day starting at dayStart minutes has FULLY elapsed as
// of nowMinute. A boundary that has not fully elapsed is never rolled up.
func dayBoundaryComplete(dayStart, nowMinute uint32) bool {
	return nowMinute >= dayStart+cubeLevelL2
}

// completedHourStarts returns the sorted set of hour-start minutes (multiples of 60) that have
// at least one L0 file AND have fully elapsed as of nowMinute.
func completedHourStarts(files []blockpack.CubeFileInfo, nowMinute uint32) []uint32 {
	seen := make(map[uint32]bool)
	for _, f := range files {
		if f.Level != cubeLevelL0 {
			continue
		}
		hourStart := (f.MinMinute / cubeLevelL1) * cubeLevelL1
		if !hourBoundaryComplete(hourStart, nowMinute) {
			continue
		}
		seen[hourStart] = true
	}
	return sortedUint32Keys(seen)
}

// completedDayStarts returns the sorted set of day-start minutes (multiples of 1440) that have
// at least one L1 file AND have fully elapsed as of nowMinute.
func completedDayStarts(files []blockpack.CubeFileInfo, nowMinute uint32) []uint32 {
	seen := make(map[uint32]bool)
	for _, f := range files {
		if f.Level != cubeLevelL1 {
			continue
		}
		dayStart := (f.MinMinute / cubeLevelL2) * cubeLevelL2
		if !dayBoundaryComplete(dayStart, nowMinute) {
			continue
		}
		seen[dayStart] = true
	}
	return sortedUint32Keys(seen)
}

func sortedUint32Keys(m map[uint32]bool) []uint32 {
	out := make([]uint32, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

// planDayRollup returns the L1 input keys fully contained within one day (dayStart through
// dayStart+1439), or (nil, false) if none are found. Mirrors blockpack.PlanCubeL1Rollup at day
// granularity — no day-rollup planner is exported (only the hour-rollup PlanCubeL1Rollup).
func planDayRollup(files []blockpack.CubeFileInfo, dayStart uint32) ([]string, bool) {
	var keys []string
	for _, f := range files {
		if f.Level != cubeLevelL1 {
			continue
		}
		if f.MinMinute >= dayStart && f.MaxMinute < dayStart+cubeLevelL2 {
			keys = append(keys, f.Key)
		}
	}
	if len(keys) == 0 {
		return nil, false
	}
	return keys, true
}

// padCubeID expands a registry's 16-hex-char cube ID to the 32-hex-char zero-padded form used in
// S3 object paths (same convention cubeFileStore.List uses for its own prefix).
func padCubeID(cubeID string) string {
	if len(cubeID) == 16 {
		return cubeID + strings.Repeat("0", 16)
	}
	return cubeID
}
