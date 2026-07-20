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
	"math"
	"sort"
	"time"

	"github.com/go-kit/log/level"
	blockpack "github.com/grafana/blockpack"
	"github.com/jackc/pgx/v5/pgxpool"

	util_log "github.com/grafana/tempo/pkg/util/log"
	"github.com/grafana/tempo/tempodb/backend"
	s3backend "github.com/grafana/tempo/tempodb/backend/s3"
	minio "github.com/minio/minio-go/v7"
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
		if isMinioNoSuchKey(err) {
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
		// minio-go's GetObject is lazy: for a nonexistent key it returns a reader with a
		// nil error, and the real 404 only surfaces here, on the first Read (empirically
		// confirmed live: this branch was returning the raw, untranslated minio error
		// "The specified key does not exist." on every tenant's first-ever registry
		// access, which Registry.Load treated as a hard failure -- so the registry file
		// was NEVER created, and no column ever crossed the trigger threshold, for the
		// entire lifetime of any tenant that had not already had one created some other
		// way). Apply the exact same NoSuchKey/404 classification as the GetObject error
		// above.
		if isMinioNoSuchKey(err) {
			return nil, "", blockpack.ErrNotFound
		}
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

func (f *viBlockFetcher) FetchBlock(ctx context.Context, sourceRef string) (*blockpack.Reader, error) {
	return fetchBlockViaReader(ctx, f.reader, sourceRef)
}

// fetchBlockViaReader opens sourceRef (a blockObjectKey-formatted string) as a
// *blockpack.Reader via reader -- the shared fetch-by-ref logic used by both
// viBlockFetcher (live S3/Local/GCS/Azure listing) and catalogBlockFetcher
// (Postgres file_catalog-backed listing, vi_backfill_catalog.go). The ONE
// piece of genuine code-sharing between the two fetchers: parsing sourceRef
// and opening it via tempoReaderProvider is identical either way -- only
// ListBlocksInRange's data source differs between them.
func fetchBlockViaReader(_ context.Context, reader backend.Reader, sourceRef string) (*blockpack.Reader, error) {
	tenantID, blockID, err := parseBlockObjectKey(sourceRef)
	if err != nil {
		return nil, err
	}
	provider := &tempoReaderProvider{reader: reader, tenantID: tenantID, blockID: blockID}
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
	registry *blockpack.Registry,
	putter blockpack.ObjectPutter,
	indexPrefix string,
	windowSeconds uint64,
) error {
	eng := blockpack.NewBackfillEngine(entry, blockpack.BackfillConfig{
		Store:       putter,
		Fetcher:     fetcher,
		IndexPrefix: indexPrefix,
		// AnchorSec resumes a chained backfill (issue #518) from the column's
		// already-persisted watermark instead of wall-clock now -- zero for a
		// never-backfilled column (WatermarkSec is genuinely 0), which
		// BackfillConfig's own zero-value fallback already treats as "anchor to
		// now," exactly the correct behavior for a first pass.
		AnchorSec:     entry.Backfill.WatermarkSec,
		WindowSeconds: windowSeconds,
	})
	runErr := eng.Run(ctx, func(prog blockpack.BackfillProgress) error {
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
			metricViBackfillCompleted.Inc()
			level.Info(util_log.Logger).Log(
				"msg", "vblockpack: VI backfill complete",
				"tenant", entry.Tenant, "column", entry.ColumnName,
			)
		}
		return nil
	})
	if runErr != nil {
		// A partial/failed run never reaches the cursor-persist step below --
		// the SAME candidate set is safely re-listed and re-processed on the
		// next attempt (relying on FlushAndPutValueIndexColumn's existing
		// idempotent-overwrite behavior for any block that was in fact already
		// written), mirroring R7's "don't claim coverage until confirmed"
		// philosophy applied to catalog discovery too.
		return runErr
	}
	// Catalog-cursor persist (Part 3.4, 2026-07-11): only fires when a
	// catalog-backed fetcher was used -- zero effect on the existing
	// S3/Local/GCS/Azure viBlockFetcher path (the type assertion simply
	// fails and this whole block is skipped).
	if cf, ok := fetcher.(*catalogBlockFetcher); ok {
		if cursorErr := registry.UpdateCatalogCursor(
			ctx, entry.Tenant, entry.ColumnHash, entry.ColumnType, cf.MaxRowIDSeen(),
		); cursorErr != nil {
			level.Warn(util_log.Logger).Log(
				"msg", "vblockpack: VI backfill: catalog cursor persist failed",
				"tenant", entry.Tenant, "column", entry.ColumnName, "err", cursorErr,
			)
			return cursorErr
		}
	}
	return nil
}

// RunViBackfillDeps bundles the backend-specific dependencies
// RunViBackfill/launchViBackfill need to run a column's backfill: fetching
// raw historical blocks (Fetcher), the viusage registry's backing store
// (ObjStore), and where finished VI files get written (Putter). Constructed
// once per invocation via NewViBackfillDepsS3 (existing S3 path, byte-for-byte
// unchanged) or NewViBackfillDepsRaw (new, generic Local/GCS/Azure path,
// Track A/C) -- RunViBackfill/launchViBackfill themselves are pure
// plumbing+metrics+error-handling with zero backend-specific construction
// left inside them.
//
// Registry (2026-07-11, live-bug fix): the ALREADY-CONSTRUCTED, tenant- and
// backend-correct *blockpack.Registry to use for this specific entry's
// backfill run. Set by the caller (ConfigureViUsage's onShouldBackfill
// closure, via the SAME realUsageRecorder.registryFor(tenant) call the
// triggering RecordUse used) -- NOT rebuilt from ObjStore inside
// runViBackfillCore. A live tenant-11638 test confirmed the bug this fixes:
// when Postgres is configured, RecordUseAndMaybeTrigger creates/updates the
// entry via the Postgres-backed registry, but runViBackfillCore used to
// unconditionally build a FRESH blob-backed registry from ObjStore, which had
// never heard of that entry -- every watermark-persist call failed with
// "entry ... not found," aborting the backfill on its very first progress
// callback. Nil means "not set" -- RunViBackfill falls back to constructing
// one from ObjStore (the pre-2026-07-11 behavior), which remains CORRECT only
// for callers that never configure Postgres (e.g. backend-worker's own
// RunViBackfill call site as of this fix -- flagged, not yet threaded with
// pgPool; same bug class would resurface there for a Postgres-configured
// tenant whose backfill lease happens to be picked up by backend-worker
// instead of a querier/frontend process).
type RunViBackfillDeps struct {
	Fetcher  blockpack.BlockFetcher
	ObjStore blockpack.ObjectStore
	Putter   blockpack.ObjectPutter
	Registry *blockpack.Registry
}

// NewViBackfillDepsS3 builds RunViBackfillDeps from S3 config -- a pure code
// move of RunViBackfill's own former S3-construction lines (unchanged logic,
// just relocated), including its former "s3cfg == nil" no-op contract: a nil
// s3cfg now returns a zero-value RunViBackfillDeps (RunViBackfill's own
// zero-value check below is the new home for that no-op), not an error.
func NewViBackfillDepsS3(s3cfg *s3backend.Config) (RunViBackfillDeps, error) {
	if s3cfg == nil {
		return RunViBackfillDeps{}, nil
	}
	client, err := newMinioClientFromS3Config(s3cfg)
	if err != nil {
		return RunViBackfillDeps{}, err
	}
	rawR, _, _, err := s3backend.New(s3cfg)
	if err != nil {
		return RunViBackfillDeps{}, err
	}
	return RunViBackfillDeps{
		Fetcher:  &viBlockFetcher{reader: backend.NewReader(rawR)},
		ObjStore: &viUsageObjectStore{client: client, bucket: s3cfg.Bucket},
		Putter:   &s3ObjectPutter{client: client, bucket: s3cfg.Bucket},
	}, nil
}

// NewViBackfillDepsRaw builds RunViBackfillDeps over an already-live
// backend.RawReader/RawWriter (Local/GCS/Azure) -- rawR/rawW are the SAME
// backend already serving trace blocks, so no separate client/backend
// construction is needed the way S3's own minio client is: Track A's
// newObjectStoreForBackend and Track C's newRawObjectPutter each probe for
// their backend's native or emulated capability internally.
func NewViBackfillDepsRaw(rawR backend.RawReader, rawW backend.RawWriter) RunViBackfillDeps {
	return RunViBackfillDeps{
		Fetcher:  &viBlockFetcher{reader: backend.NewReader(rawR)},
		ObjStore: newObjectStoreForBackend(rawR, rawW),
		Putter:   newRawObjectPutter(rawW),
	}
}

// NewViBackfillDepsCatalogOverride takes an ALREADY-BUILT deps (from
// NewViBackfillDepsS3/Raw, unchanged) and returns a copy with .Fetcher
// swapped to a *catalogBlockFetcher -- an orthogonal override, independent of
// which object-store backend is otherwise active (ObjStore/Putter are left
// untouched; only block LISTING moves from live S3/Local/GCS/Azure listing to
// the Postgres file_catalog table). cursorRowID starts from
// entry.Backfill.LastCatalogRowID (zero means "never run against the
// catalog," which lists ALL rows for the tenant -- equivalent to a full first
// listing). Called from ConfigureViUsage's onShouldBackfill closure only when
// pgPool != nil.
func NewViBackfillDepsCatalogOverride(
	deps RunViBackfillDeps, pool *pgxpool.Pool, entry blockpack.Entry, reader backend.Reader,
) RunViBackfillDeps {
	deps.Fetcher = &catalogBlockFetcher{
		pool:        pool,
		reader:      reader,
		cursorRowID: entry.Backfill.LastCatalogRowID,
	}
	return deps
}

// NewViBackfillDepsWithPgRegistry (2026-07-17 follow-up to the 2026-07-11 live-bug fix
// documented on RunViBackfillDeps.Registry's own doc comment) takes an ALREADY-BUILT deps and
// returns a copy with .Registry set to the SAME Postgres-backed registry construction
// realUsageRecorder.registryFor uses (blockpack.NewRegistryFromEntryStore(blockpack.
// NewPgViUsageEntryStore(pool), tenant)) -- closing the exact gap that comment flagged as "not
// yet threaded": backend-worker's own RunViBackfill call site (processViBackfillJobPostgres)
// used to always fall back to RunViBackfill's zero-value-Registry branch, which builds a FRESH
// blob-backed registry that has never heard of an entry the Postgres-backed registry already
// created -- every watermark-persist call then fails with "entry ... not found," aborting on the
// very first progress callback. A nil pool is a safe no-op (returns deps unchanged), matching
// every other *Postgres-optional construction in this file.
func NewViBackfillDepsWithPgRegistry(deps RunViBackfillDeps, pool *pgxpool.Pool, tenant string) RunViBackfillDeps {
	if pool == nil {
		return deps
	}
	deps.Registry = blockpack.NewRegistryFromEntryStore(blockpack.NewPgViUsageEntryStore(pool), tenant)
	return deps
}

// RunViBackfill runs entry's column backfill synchronously in the calling
// goroutine, using the already-constructed deps (NewViBackfillDepsS3/Raw). A
// zero-value deps (e.g. NewViBackfillDepsS3(nil)'s return) is a safe no-op --
// the new home for RunViBackfill's former "s3cfg == nil" early return. Used
// by the backend-worker job executor (B2's JOB_TYPE_VI_BACKFILL dispatch
// case) and launchViBackfill's async wrapper below.
// windowSeconds bounds how far back from entry's watermark this run
// processes; zero means unbounded (full remaining history), matching
// vi_usage_hook.go's reactive first-trigger contract.
func RunViBackfill(ctx context.Context, entry blockpack.Entry, deps RunViBackfillDeps, windowSeconds uint64) error {
	if deps.Fetcher == nil || deps.ObjStore == nil || deps.Putter == nil {
		return nil
	}
	registry := deps.Registry
	if registry == nil {
		// Pre-2026-07-11 fallback for callers that haven't set Registry yet
		// (backend-worker's own RunViBackfill call site) -- only correct when
		// Postgres is never configured for the tenant in question, since this
		// always builds the blob-backed registry regardless.
		registry = blockpack.NewRegistry(deps.ObjStore, entry.Tenant)
	}
	if windowSeconds == 0 {
		windowSeconds = math.MaxUint64
	}
	metricViBackfillStarted.Inc()
	err := runViBackfillCore(ctx, entry, deps.Fetcher, registry, deps.Putter, defaultValueIndexPref, windowSeconds)
	if err != nil && !isContextErr(ctx, err) {
		metricViBackfillFailed.Inc()
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
func launchViBackfill(entry blockpack.Entry, deps RunViBackfillDeps, windowSeconds uint64) {
	go func() {
		level.Info(util_log.Logger).Log(
			"msg", "vblockpack: VI backfill started",
			"tenant", entry.Tenant, "column", entry.ColumnName,
		)
		_ = RunViBackfill(context.Background(), entry, deps, windowSeconds)
	}()
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
