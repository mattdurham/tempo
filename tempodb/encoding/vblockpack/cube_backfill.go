package vblockpack

// cube_backfill.go — value-index backed CubeValueIndexSource for cube backfill,
// and backfill launch wired into the first-query cube creation trigger.
//
// When a cube is created (CreationTrigger.TryCreate returns Created=true), we
// immediately launch a background goroutine that backfills historical data from
// the value index, newest-first, writing L0 cube files per minute.

import (
	"context"
	"errors"
	"math"
	"path"
	"strings"
	"time"

	"github.com/go-kit/log/level"
	blockpack "github.com/grafana/blockpack"
	util_log "github.com/grafana/tempo/pkg/util/log"
	s3backend "github.com/grafana/tempo/tempodb/backend/s3"
	minio "github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

// viBackfillSource implements blockpack.CubeValueIndexSource over S3 VI files.
// LookupColumn lists + downloads VI files for (tenant, column) in [minSec, maxSec],
// returning one VIQueryResult per span with SourceRef set to the decoded string
// column value (the convention expected by the cube Backfiller).
//
// blockpack issue #478: reads go through a shared valueIndexStore (a cachingStore
// wrapping the same minioVIStore the query path uses), NOT a bespoke minio-client
// list/get. This unifies what had been a THIRD parallel object-store implementation
// with the value-index/trace-by-id store and reuses its caching: List hits a TTL'd
// listing cache (repeated re-lists of the same tenant/column/type prefix during a
// backfill pass or repeated cardinality-gate TryCreate collapse to one LIST) and
// Get hits the immutable content cache with singleflight dedup (re-scanning
// overlapping windows no longer re-downloads the same VI/VCNT file).
type viBackfillSource struct {
	store       valueIndexStore
	indexPrefix string
}

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

// fetchVCNTSection lists and downloads the .vcnt files covering each proposed cube
// dimension, then merges them into one consolidated VCNT section (data + dir) via
// blockpack.VCNTBuildSectionFromObjects — the shape the cardinality gate consumes
// (#483). Reads go through the same shared cachingStore-wrapped minioVIStore the
// backfill/query paths use (issue #478), so repeated gate attempts for the same dims
// collapse to cached LISTs/GETs rather than re-hitting S3.
//
// VCNT filenames written since issue #494 embed a wall-clock time range (v2 format,
// L<level>-<wallMinSec>-<wallMaxSec>-<id>.vcnt); VCNTFileOverlapsRange (issue #495) uses that
// range to skip GET-ing files that provably cannot overlap [minTS, maxTS], before any S3 read.
// A v1-shaped or unparseable filename is always fetched (unknown range, never dropped). The
// query window is still additionally applied at the record level by the gate's ValuesInRange
// decode after fetch — this file-level check only avoids unnecessary GETs, it does not replace
// that record-level filtering.
//
// On any error (or absent coverage) it returns a nil/empty section, which the gate
// treats as "no coverage" and passes by default — a VCNT read failure must never block
// cube creation, only inform it when data is present.
func (cqp *cubeQueryPath) fetchVCNTSection(
	ctx context.Context,
	tenant string,
	dims []string,
	minTS, maxTS uint64,
) ([]byte, []blockpack.VCNTChunkDirEntry) {
	// F-10 (issue #481 part 3): a nil client (the test-only cqp.store injection seam,
	// cubequerypath.go's objectStore(), covers the REGISTRY store only — this is a SEPARATE
	// value-index store construction with no equivalent seam) is exactly the "absent
	// capability" case this function's own doc comment already treats as safe/expected —
	// same posture as "absent coverage": return an empty section rather than dereferencing a
	// nil *minio.Client inside newBackfillVIStore's ListObjects call.
	if cqp.client == nil {
		return nil, nil
	}
	return buildVCNTSection(ctx, newBackfillVIStore(cqp.client, cqp.bucket), tenant, dims, minTS, maxTS)
}

// buildVCNTSection is the store-agnostic core of fetchVCNTSection: given any
// valueIndexStore, list+download the .vcnt files for each dim and merge them into a
// single consolidated section. Split out from fetchVCNTSection so the list/filter/build
// glue is unit-testable against an in-memory store without a live object store (#483).
func buildVCNTSection(
	ctx context.Context,
	store valueIndexStore,
	tenant string,
	dims []string,
	minSec, maxSec uint64,
) ([]byte, []blockpack.VCNTChunkDirEntry) {
	var objects [][]byte
	for _, dim := range dims {
		colHash := blockpack.VCNTColHash(dim)
		prefix := path.Join(tenant, "value_counts", colHash) + "/"
		keys, err := store.List(ctx, prefix)
		if err != nil || len(keys) == 0 {
			continue
		}
		for _, k := range keys {
			if path.Ext(k) != ".vcnt" {
				continue
			}
			if !VCNTFileOverlapsRange(path.Base(k), minSec, maxSec) {
				continue
			}
			data, getErr := store.Get(ctx, k)
			if getErr != nil || len(data) == 0 {
				continue
			}
			objects = append(objects, data)
		}
	}
	if len(objects) == 0 {
		return nil, nil
	}
	data, dir, skipped := blockpack.VCNTBuildSectionFromObjects(objects)
	if skipped > 0 {
		level.Warn(util_log.Logger).Log(
			"msg", "vblockpack: buildVCNTSection: skipped unreadable .vcnt objects",
			"tenant", tenant,
			"dims", strings.Join(dims, ","),
			"skipped", skipped,
			"total", len(objects),
		)
	}
	return data, dir
}

func (s *viBackfillSource) LookupColumn(
	ctx context.Context,
	tenant, column string,
	minSec, maxSec uint64,
) ([]blockpack.VIQueryResult, error) {
	colHash := blockpack.VCNTColHash(column)

	var results []blockpack.VIQueryResult
	for _, typeName := range []string{"string", "int64", "uint64", "bool", "float64"} {
		prefix := path.Join(tenant, s.indexPrefix, colHash, typeName) + "/"
		keys, err := s.store.List(ctx, prefix)
		if err != nil || len(keys) == 0 {
			continue
		}

		for _, k := range keys {
			meta, perr := blockpack.VIParseFilenameV2(path.Base(k))
			if perr != nil {
				continue
			}
			if !meta.IsInTimeRange(minSec, maxSec) {
				continue
			}
			data, getErr := s.store.Get(ctx, k)
			if getErr != nil {
				continue
			}
			r, openErr := blockpack.VIOpenReader(data)
			if openErr != nil {
				continue
			}
			tr := [2]uint64{minSec, maxSec}
			hits, qErr := r.Lookup(nil, &tr)
			if qErr != nil {
				continue
			}
			for _, h := range hits {
				// The backfill reads the column value from SourceRef, not Value.
				// Decode the canonical value bytes to a string.
				valStr := decodeCanonicalVI(h.Value)
				results = append(results, blockpack.VIQueryResult{
					SourceRef: valStr,
					Value:     h.Value,
					TimeSec:   h.TimeSec,
					TraceID:   h.TraceID,
					SpanID:    h.SpanID,
					RowIdx:    h.RowIdx,
				})
			}
		}
	}
	return results, nil
}

// decodeCanonicalVI converts canonical VI value bytes to a string.
// String columns store raw UTF-8; numeric columns store 8-byte little-endian.
func decodeCanonicalVI(b []byte) string {
	if len(b) == 0 {
		return ""
	}
	// If all bytes are valid UTF-8 printable, treat as string.
	for _, c := range b {
		if c < 0x20 {
			goto numeric
		}
	}
	return string(b)
numeric:
	if len(b) == 8 {
		v := int64(b[0]) | int64(b[1])<<8 | int64(b[2])<<16 | int64(b[3])<<24 |
			int64(b[4])<<32 | int64(b[5])<<40 | int64(b[6])<<48 | int64(b[7])<<56 //nolint:gosec
		return path.Join("", string(rune('0'+v%10))) // simple numeric → string
	}
	return string(b)
}

// runCubeBackfillCore is launchBackfill/RunCubeBackfill's dependency-injected
// core: constructs the CubeRegistry from objStore and runs the CubeBackfiller,
// persisting progress via CubeRegistry.UpdateWatermarks on EVERY successful
// progressFn callback, not just on Done (R9 -- go-presubmit.md/review.md's
// #496-planning-time finding: launchBackfill/RunCubeBackfill's progressFn
// previously only logged, so a backfill pass never itself advanced the
// registry; the watermark only ever moved later, incidentally, via a
// subsequent compaction pass calling this SAME UpdateWatermarks method).
// Mirrors vi_backfill.go's runViBackfillCore split so this is unit-testable
// against fakes without a live S3/minio server (cube_backfill.go previously
// had no test coverage at all — the exact R9 gap this closes).
//
// Unlike VI's BackfillProgress (whose LastError field is effectively unused —
// BackfillEngine.Run/processBlocks return real errors directly, never via
// progressFn), cube's own Backfiller.Run calls progressFn on a per-minute
// processMinute failure too (LastError set, Watermark.WatermarkMinute left at
// the prior, already-covered boundary) and continues to the next older
// minute rather than aborting. Persisting on that call would be a genuine
// no-op at best (the reported minute was already covered by an earlier
// successful call) and a false completeness claim at worst (if it was the
// very first call, before anything was ever covered) — so persistence here is
// gated on prog.LastError == nil, persisting exactly the single minute
// [wm.WatermarkMinute, wm.WatermarkMinute] that call's real work newly
// covered; CubeRegistry.UpdateWatermarks' own min-of-mins/max-of-maxes
// expansion (registry.go) accumulates these into the run's full covered
// range as newest-to-oldest processing proceeds.
func runCubeBackfillCore(
	ctx context.Context,
	entry blockpack.CubeRegistryEntry,
	src blockpack.CubeValueIndexSource,
	objStore blockpack.CubeObjectStore,
	cfg blockpack.CubeBackfillConfig,
) error {
	registry := blockpack.NewCubeRegistry(objStore, entry.Tenant)
	bf := blockpack.NewCubeBackfiller(entry, src, cfg)
	return bf.Run(ctx, 0, func(prog blockpack.CubeBackfillProgress) error {
		if prog.LastError == nil {
			wm := prog.Watermark
			if uwErr := registry.UpdateWatermarks(
				ctx, entry.CubeID, blockpack.CubeRollupL0, wm.WatermarkMinute, wm.WatermarkMinute,
			); uwErr != nil {
				// A persist failure aborts the run rather than continuing to spend
				// backfill I/O the registry cannot yet account for (R7's VI precedent,
				// applied here too): mirrors runViBackfillCore's identical posture.
				level.Warn(util_log.Logger).Log(
					"msg", "vblockpack: cube backfill: watermark persist failed",
					"tenant", entry.Tenant, "cube_id", entry.CubeID, "err", uwErr,
				)
				return uwErr
			}
		}
		if prog.Watermark.Done {
			level.Info(util_log.Logger).Log(
				"msg", "vblockpack: cube backfill complete",
				"tenant", entry.Tenant,
				"cube_id", entry.CubeID,
			)
		}
		return nil
	})
}

// launchBackfill starts a background goroutine that backfills a newly-created cube
// from the value index, reading VI files newest→oldest for the configured window.
func launchBackfill(entry blockpack.CubeRegistryEntry) {
	cqp := getCubeQueryPath()
	if cqp == nil {
		return
	}
	src := &viBackfillSource{
		store:       newBackfillVIStore(cqp.client, cqp.bucket),
		indexPrefix: defaultValueIndexPref,
	}
	store := &s3ObjectPutter{client: cqp.client, bucket: cqp.bucket}
	objStore := &minioObjectStore{client: cqp.client, bucket: cqp.bucket}
	cfg := blockpack.CubeBackfillConfig{
		Store:   store,
		Workers: 4,
		// WindowMinutes: full history, not an artificial cap (2026-07-11 ruling) --
		// math.MaxUint32 minutes trivially exceeds any real currentMinute, so
		// Backfiller.Run's endMinute always resolves to 0. Bounded only by how far
		// back the value index itself actually has data, not by this config.
		WindowMinutes: math.MaxUint32,
	}

	go func() {
		level.Info(util_log.Logger).Log(
			"msg", "vblockpack: cube backfill started",
			"tenant", entry.Tenant,
			"cube_id", entry.CubeID,
		)
		if err := runCubeBackfillCore(context.Background(), entry, src, objStore, cfg); err != nil {
			level.Warn(util_log.Logger).Log(
				"msg", "vblockpack: cube backfill error",
				"tenant", entry.Tenant,
				"cube_id", entry.CubeID,
				"err", err,
			)
		}
	}()
}

// RunCubeBackfill runs the cube backfill synchronously in the calling goroutine.
// Unlike launchBackfill, this blocks until the backfill is complete or ctx is done.
// Used by the backend-worker job executor.
func RunCubeBackfill(ctx context.Context, entry blockpack.CubeRegistryEntry, s3cfg *s3backend.Config) {
	if s3cfg == nil {
		return
	}
	endpoint := s3cfg.Endpoint
	if endpoint == "" {
		endpoint = "s3." + s3cfg.Region + ".amazonaws.com"
	}
	client, err := minio.New(endpoint, &minio.Options{
		Creds:  credentials.NewEnvAWS(),
		Secure: !s3cfg.Insecure,
		Region: s3cfg.Region,
	})
	if err != nil {
		level.Warn(util_log.Logger).Log("msg", "vblockpack: RunCubeBackfill: S3 client init failed", "err", err)
		return
	}
	src := &viBackfillSource{
		store:       newBackfillVIStore(client, s3cfg.Bucket),
		indexPrefix: defaultValueIndexPref,
	}
	store := &s3ObjectPutter{client: client, bucket: s3cfg.Bucket}
	objStore := &minioObjectStore{client: client, bucket: s3cfg.Bucket}
	cfg := blockpack.CubeBackfillConfig{
		Store:   store,
		Workers: 4,
		// WindowMinutes: full history, matching launchBackfill's own ruling above --
		// this job-queue-dispatched path must backfill the same range as the inline path.
		WindowMinutes: math.MaxUint32,
	}
	err = runCubeBackfillCore(ctx, entry, src, objStore, cfg)
	if err != nil && !errors.Is(err, ctx.Err()) {
		level.Warn(util_log.Logger).Log("msg", "vblockpack: cube backfill error",
			"tenant", entry.Tenant, "cube_id", entry.CubeID, "err", err)
	}
}

// LoadCubeEntry loads the actual CubeRegistryEntry for cubeID from S3.
// Returns the default entry if loading fails or the entry is not found.
func LoadCubeEntry(ctx context.Context, s3cfg *s3backend.Config, tenant, cubeID string, def blockpack.CubeRegistryEntry) blockpack.CubeRegistryEntry {
	if s3cfg == nil {
		return def
	}
	endpoint := s3cfg.Endpoint
	if endpoint == "" {
		endpoint = "s3." + s3cfg.Region + ".amazonaws.com"
	}
	client, err := minio.New(endpoint, &minio.Options{
		Creds:  credentials.NewEnvAWS(),
		Secure: !s3cfg.Insecure,
		Region: s3cfg.Region,
	})
	if err != nil {
		return def
	}
	os := &minioObjectStore{client: client, bucket: s3cfg.Bucket}
	reg := blockpack.NewCubeRegistry(os, tenant)
	entries, _, loadErr := reg.Load(ctx)
	if loadErr != nil {
		return def
	}
	for _, e := range entries {
		if e.CubeID == cubeID {
			return e
		}
	}
	return def
}
