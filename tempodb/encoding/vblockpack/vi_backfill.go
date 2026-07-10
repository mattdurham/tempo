package vblockpack

// vi_backfill.go — #496 B2: VI usage-triggered backfill launcher, structurally
// mirroring cube_backfill.go's launchBackfill/RunCubeBackfill.
//
// R6 divergence from cube's backfill (by design, not an oversight): cube's
// backfill reads cheap, pre-extracted VI files (viBackfillSource above). VI's
// OWN backfill has no such pre-extracted data to read for a never-indexed
// column — it must read RAW HISTORICAL BLOCKS (viBlockFetcher, this file),
// full block I/O per this repo's core I/O invariant.
//
// R9 divergence from cube's backfill (the critical one): plan.md Section 1's
// independent verification found that cube's own launchBackfill/RunCubeBackfill
// progressFn callbacks ONLY log — neither ever calls a watermark-persistence
// method on the registry. VI's wiring below (runViBackfillCore) MUST NOT repeat
// that gap: it calls viusage.Registry.UpdateWatermark on EVERY progressFn
// callback, not just on Done. TestRunViBackfill_Synchronous_CallsUpdateWatermarkOnEachProgress
// is the explicit regression test for this property.

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"sort"
	"time"

	"github.com/go-kit/log/level"
	blockpack "github.com/grafana/blockpack"
	util_log "github.com/grafana/tempo/pkg/util/log"
	"github.com/grafana/tempo/tempodb/backend"
	s3backend "github.com/grafana/tempo/tempodb/backend/s3"
	minio "github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

// viUsageObjectStore satisfies blockpack.ObjectStore over a minio client,
// mirroring minioObjectStore's shape (cubemanager.go) but returning
// blockpack.ErrConflict (not blockpack.CubeErrConflict) on a 412 — the two
// registries are independent types (R1) with their own conflict sentinels;
// returning the wrong one would make viusage.Registry's conditional-PUT retry
// loop fail to recognize a real conflict as retryable.
type viUsageObjectStore struct {
	client *minio.Client
	bucket string
}

func (s *viUsageObjectStore) Get(ctx context.Context, path string) ([]byte, string, error) {
	obj, err := s.client.GetObject(ctx, s.bucket, path, minio.GetObjectOptions{})
	if err != nil {
		resp := minio.ToErrorResponse(err)
		if resp.Code == minioNoSuchKeyCode || resp.StatusCode == 404 {
			// #496 go-presubmit.md CRITICAL fix: must return blockpack.ErrNotFound (not a
			// nil error) so Registry.Load can distinguish a genuine miss from a real
			// transient failure -- both previously had the identical (nil, "", ?) shape,
			// which caused every real error to be silently treated as an empty index.
			return nil, "", blockpack.ErrNotFound
		}
		return nil, "", err
	}
	defer func() { _ = obj.Close() }()
	data, err := io.ReadAll(obj)
	if err != nil {
		return nil, "", err
	}
	info, statErr := s.client.StatObject(ctx, s.bucket, path, minio.StatObjectOptions{})
	if statErr != nil {
		return data, "", nil
	}
	return data, info.ETag, nil
}

func (s *viUsageObjectStore) ConditionalPut(ctx context.Context, path string, data []byte, etag string) error {
	opts := minio.PutObjectOptions{ContentType: "application/json"}
	if etag != "" {
		opts.SetMatchETag(etag)
	}
	_, err := s.client.PutObject(ctx, s.bucket, path, bytes.NewReader(data), int64(len(data)), opts)
	if err != nil {
		resp := minio.ToErrorResponse(err)
		if resp.StatusCode == 412 {
			return blockpack.ErrConflict
		}
		return err
	}
	return nil
}

// viBlockFetcher implements blockpack.BlockFetcher over tempo's real
// backend.Reader, listing and opening RAW TRACE BLOCKS (not pre-extracted VI
// files, per R6). ListBlocksInRange re-sorts every candidate block by its own
// NOMINAL BlockMeta.StartTime, newest-first — it does NOT trust backend.Reader's
// own Blocks() ordering (undocumented, ingester-dependent). This ordering is a
// processing-preference (recent data usable soonest) and observability aid ONLY —
// go-presubmit.md's HIGH finding established that NOMINAL block time is not a safe
// proxy for each block's REAL per-span content range (late-arriving data, clock skew,
// multi-writer flush jitter), so blockpack.BackfillEngine.processBlocks no longer
// relies on this (or any) ordering for its R7 correctness invariant: it defers ANY
// watermark advance until every listed block has genuinely been processed, regardless
// of what order they arrive in. See valueindex_backfill.go's processBlocks doc comment
// for the full design.
type viBlockFetcher struct {
	reader backend.Reader
}

func (f *viBlockFetcher) ListBlocksInRange(
	ctx context.Context,
	tenant string,
	minSec, maxSec uint64,
) ([]string, error) {
	blockIDs, _, err := f.reader.Blocks(ctx, tenant)
	if err != nil {
		return nil, err
	}

	type candidate struct {
		ref      string
		startSec uint64
	}
	var candidates []candidate
	for _, id := range blockIDs {
		meta, merr := f.reader.BlockMeta(ctx, id, tenant)
		if merr != nil {
			// go-presubmit.md MEDIUM fix: a block whose meta.json can't be read must
			// FAIL the whole listing, not be silently skipped. BackfillEngine.Run has
			// no way to know a block was skipped and unconditionally sets
			// Done=true/WatermarkSec=minSec once it has processed every ref it WAS
			// given -- a silently-skipped block's data (if it contained the target
			// column) would never be indexed, yet the persisted watermark would claim
			// full coverage, exactly the "mostly-complete" false claim R7 forbids.
			// Failing here instead means RecordUseAndMaybeTrigger's lease simply
			// expires and self-heals via R8's existing re-acquire logic on a later
			// retry, rather than a listing gap silently reaching a false Done=true.
			return nil, fmt.Errorf("viBlockFetcher: read block meta for %s: %w", id, merr)
		}
		startSec := unixSecOrZero(meta.StartTime)
		endSec := unixSecOrZero(meta.EndTime)
		if endSec < minSec || startSec > maxSec {
			continue // no overlap with the requested window
		}
		candidates = append(candidates, candidate{
			ref:      blockObjectKey(tenant, id.String()),
			startSec: startSec,
		})
	}
	sort.Slice(candidates, func(i, j int) bool { return candidates[i].startSec > candidates[j].startSec })

	refs := make([]string, len(candidates))
	for i, c := range candidates {
		refs[i] = c.ref
	}
	return refs, nil
}

func (f *viBlockFetcher) FetchBlock(_ context.Context, sourceRef string) (*blockpack.Reader, error) {
	tenantID, blockID, err := parseBlockObjectKey(sourceRef)
	if err != nil {
		return nil, err
	}
	provider := &tempoReaderProvider{reader: f.reader, tenantID: tenantID, blockID: blockID}
	return blockpack.NewReaderFromProvider(provider)
}

// runViBackfillCore is RunViBackfill's dependency-injected core: constructs
// the Registry from objStore and the BackfillEngine from fetcher/putter, then
// runs the backfill, persisting progress via Registry.UpdateWatermark on
// EVERY progressFn callback (R9 -- see this file's package doc comment).
// Split out from RunViBackfill so this logic is unit-testable against fakes
// without a live S3/minio server (cube_backfill.go's own launchBackfill/
// RunCubeBackfill have no such split and, per the R9 finding, no test
// coverage at all today).
func runViBackfillCore(
	ctx context.Context,
	entry blockpack.Entry,
	fetcher blockpack.BlockFetcher,
	objStore blockpack.ObjectStore,
	putter blockpack.ObjectPutter,
	indexPrefix string,
) error {
	registry := blockpack.NewRegistry(objStore, entry.Tenant)
	eng := blockpack.NewBackfillEngine(entry, blockpack.BackfillConfig{
		Store:       putter,
		Fetcher:     fetcher,
		IndexPrefix: indexPrefix,
	})
	return eng.Run(ctx, func(prog blockpack.BackfillProgress) error {
		if uwErr := registry.UpdateWatermark(
			ctx, entry.Tenant, entry.ColumnHash, entry.ColumnType,
			prog.WatermarkSec, prog.WindowStartSec, prog.WindowEndSec, prog.Done,
		); uwErr != nil {
			// A persist failure aborts the run rather than continuing to spend
			// backfill I/O the registry cannot yet account for (R7): the lease
			// (R8) will simply expire and a future RecordUseAndMaybeTrigger call
			// re-acquires it, self-healing without manual intervention.
			level.Warn(util_log.Logger).Log(
				"msg", "vblockpack: VI backfill: watermark persist failed",
				"tenant", entry.Tenant, "column", entry.ColumnName, "err", uwErr,
			)
			return uwErr
		}
		if prog.Done {
			level.Info(util_log.Logger).Log(
				"msg", "vblockpack: VI backfill complete",
				"tenant", entry.Tenant, "column", entry.ColumnName,
			)
		}
		return nil
	})
}

// RunViBackfill runs entry's column backfill synchronously in the calling
// goroutine, constructing real S3-backed dependencies from s3cfg. Used by the
// backend-worker job executor (B2's JOB_TYPE_VI_BACKFILL dispatch case).
func RunViBackfill(ctx context.Context, entry blockpack.Entry, s3cfg *s3backend.Config) error {
	if s3cfg == nil {
		return nil
	}
	client, err := newViBackfillMinioClient(s3cfg)
	if err != nil {
		level.Warn(util_log.Logger).Log("msg", "vblockpack: RunViBackfill: S3 client init failed", "err", err)
		return err
	}
	rawR, _, _, err := s3backend.New(s3cfg)
	if err != nil {
		level.Warn(util_log.Logger).Log("msg", "vblockpack: RunViBackfill: backend reader init failed", "err", err)
		return err
	}
	fetcher := &viBlockFetcher{reader: backend.NewReader(rawR)}
	objStore := &viUsageObjectStore{client: client, bucket: s3cfg.Bucket}
	putter := &s3ObjectPutter{client: client, bucket: s3cfg.Bucket}

	err = runViBackfillCore(ctx, entry, fetcher, objStore, putter, defaultValueIndexPref)
	if err != nil && !isContextErr(ctx, err) {
		level.Warn(util_log.Logger).Log(
			"msg", "vblockpack: VI backfill error",
			"tenant", entry.Tenant, "column", entry.ColumnName, "err", err,
		)
	}
	return err
}

// launchViBackfill starts a background goroutine that runs entry's column
// backfill (the async, querier-triggered path -- B1's hook calls this when
// RecordUseAndMaybeTrigger returns ShouldBackfill=true).
func launchViBackfill(entry blockpack.Entry, s3cfg *s3backend.Config) {
	go func() {
		level.Info(util_log.Logger).Log(
			"msg", "vblockpack: VI backfill started",
			"tenant", entry.Tenant, "column", entry.ColumnName,
		)
		_ = RunViBackfill(context.Background(), entry, s3cfg)
	}()
}

func newViBackfillMinioClient(s3cfg *s3backend.Config) (*minio.Client, error) {
	endpoint := s3cfg.Endpoint
	if endpoint == "" {
		endpoint = "s3." + s3cfg.Region + ".amazonaws.com"
	}
	return minio.New(endpoint, &minio.Options{
		Creds:  credentials.NewEnvAWS(),
		Secure: !s3cfg.Insecure,
		Region: s3cfg.Region,
	})
}

func isContextErr(ctx context.Context, err error) bool {
	cerr := ctx.Err()
	return cerr != nil && errors.Is(err, cerr)
}

// unixSecOrZero converts t to unix seconds, or 0 for the zero time.Time
// (a block whose meta.json genuinely has no start/end recorded -- treated as
// "unbounded old/new" by ListBlocksInRange's overlap check rather than
// panicking or erroring).
func unixSecOrZero(t time.Time) uint64 {
	if t.IsZero() {
		return 0
	}
	sec := t.Unix()
	if sec < 0 {
		return 0
	}
	return uint64(sec)
}
