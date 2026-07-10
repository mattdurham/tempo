package valueindexcompactor

import (
	"context"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path"
	"runtime/debug"
	"sort"
	"strings"
	"sync/atomic"
	"time"

	"github.com/grafana/blockpack/internal/modules/valueindex"
	"golang.org/x/sync/errgroup"
)

// NOTE-VI-017: see internal/modules/valueindexcompactor/NOTES.md.
// Any changes to this file must be reflected there.

// Service is the value-index compactor orchestrator. It periodically scans each
// configured tenant's column directories and compacts same-level value index
// files into fewer, larger files, dropping entries whose source blockpack has
// been deleted by retention.
type Service struct {
	store   IndexStore
	exister SourceExister
	metrics *compactorMetrics // nil when Config.Registerer is nil (no-op)
	now     func() time.Time
	cfg     Config
}

// NewService builds a compactor service. cfg.Tenants must be non-empty and the
// store must be non-nil. exister may be nil, in which case the source-existence
// retention check is skipped (all entries are propagated). The store and
// exister are injected so the orchestration is testable without a real object
// store.
//
// NewService also runs a one-time, best-effort sweep of any vi-merge-*.tmp files left behind
// under os.TempDir() by a prior process that crashed mid-merge (plan.md Decision 4). NewService
// is the single construction point both production and tests go through, so a call here
// naturally runs exactly once per process lifetime without needing a sync.Once guard. A sweep
// failure is recorded via metrics but never fails service construction — a leftover orphaned
// file is a disk-hygiene concern, not a correctness blocker.
func NewService(cfg Config, store IndexStore, exister SourceExister) (*Service, error) {
	cfg = cfg.withDefaults()
	if len(cfg.Tenants) == 0 {
		return nil, errors.New("valueindexcompactor: at least one tenant must be configured")
	}
	if store == nil {
		return nil, errors.New("valueindexcompactor: store is required")
	}
	s := &Service{
		store:   store,
		exister: exister,
		cfg:     cfg,
		metrics: newCompactorMetrics(cfg.Registerer),
		now:     time.Now,
	}
	if _, err := valueindex.SweepOrphanedMergeTempFiles(); err != nil {
		s.metrics.incError(compactorOpSweep)
	}
	s.metrics.setConfiguredConcurrency(cfg.CompactConcurrency)
	return s, nil
}

// columnWork is a fully-resolved (tenant, colDir) work item.
type columnWork struct {
	tenant string
	colDir string // e.g. "11638/indexes/abc123.../string"
}

// Run drives the compaction loop until ctx is canceled.
//
// Rather than a periodic "scan all columns, then wait" pass, Run builds a flat
// list of every (tenant, column) it owns and cycles through them continuously.
// Each iteration lists one column's files, merges a batch if the threshold is
// met, then immediately moves to the next column. When the end of the list is
// reached it is rebuilt (picking up newly-created columns) and the cycle
// restarts. There is no wait between columns — the S3 list call is the natural
// rate-limiter. A short sleep is added only when a full cycle produced no work,
// to avoid busy-spinning when all columns are caught up.
//
// SPEC-VI-4: up to CompactConcurrency columns are dispatched
// concurrently per lap via errgroup.SetLimit, but g.Wait() always drains the
// in-flight lap before the work list is rebuilt. This is the sole mechanism that
// prevents two goroutines from ever processing the same colDir concurrently across
// a lap boundary — buildWorkList already guarantees no duplicate colDir within one
// lap by construction. Do not reorder the g.Wait() calls relative to the work-list
// rebuild.
func (s *Service) Run(ctx context.Context) error {
	if !s.cfg.Enabled {
		<-ctx.Done()
		return ctx.Err()
	}

	concurrency := s.cfg.CompactConcurrency
	if concurrency <= 0 {
		concurrency = 1
	}

	var (
		work []columnWork
		idx  int
		did  atomic.Int32
		g    *errgroup.Group
		gctx context.Context
	)

	for {
		if ctx.Err() != nil {
			if g != nil {
				_ = g.Wait()
			}
			return ctx.Err()
		}

		// Rebuild the work list when exhausted.
		if idx >= len(work) {
			// Drain the just-finished lap's in-flight merges BEFORE rebuilding the
			// work list or evaluating the idle-sleep check. This is the sole
			// mechanism that prevents two goroutines from ever processing the same
			// colDir concurrently across a lap boundary -- buildWorkList already
			// guarantees no duplicate colDir *within* one lap by construction.
			if g != nil {
				_ = g.Wait()
			}
			// If we completed a full cycle with nothing to do, pause briefly.
			if len(work) > 0 && did.Load() == 0 {
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-time.After(s.cfg.CompactInterval):
				}
			}
			work = s.buildWorkList(ctx)
			idx = 0
			did.Store(0)
			g, gctx = errgroup.WithContext(ctx)
			g.SetLimit(concurrency)
			if len(work) == 0 {
				// No columns found (misconfigured or transient error); back off.
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-time.After(s.cfg.CompactInterval):
				}
				continue
			}
		}

		w := work[idx]
		idx++

		g.Go(func() error {
			defer func() {
				if rec := recover(); rec != nil {
					// SPEC-ROOT-001: goroutine panics must not crash the process.
					//
					// Deliberately return nil here, not a non-nil error: errgroup.WithContext
					// cancels gctx the first time any g.Go closure returns non-nil, which would
					// abort every OTHER concurrently in-flight merge in this lap the moment one
					// column panics -- exactly the cross-column blast radius this panic-isolation
					// closure exists to prevent. A panicking column must affect only itself.
					slog.Error("valueindexcompactor: Run worker panic",
						"panic", rec, "colDir", w.colDir, "tenant", w.tenant,
						"stack", string(debug.Stack()))
					s.metrics.incError(compactorOpPanic)
				}
			}()
			s.metrics.incInFlight()
			defer s.metrics.decInFlight()

			keys, lerr := s.store.List(gctx, w.colDir+"/")
			if lerr != nil {
				slog.Error("valueindexcompactor: List failed", "tenant", w.tenant, "colDir", w.colDir, "err", lerr)
				s.metrics.incError(compactorOpList)
				return nil
			}
			merged, cerr := s.compactColumn(gctx, w.tenant, w.colDir, keys)
			if cerr != nil {
				// A listed file getting deleted before Get/Delete runs is an expected,
				// self-healing race under concurrent compaction (another merge on this
				// same colDir in a later lap, or retention, consumed it first) -- the
				// column is simply retried next lap. Store doesn't expose a typed
				// not-found error (kept backend-agnostic so external callers can supply
				// their own S3 client), so this is a best-effort text match on the error
				// the underlying object store returns for a missing key.
				if isObjectNotFoundErr(cerr) {
					slog.Warn("valueindexcompactor: compactColumn skipped, input already removed",
						"tenant", w.tenant, "colDir", w.colDir, "err", cerr)
				} else {
					slog.Error("valueindexcompactor: compactColumn failed", "tenant", w.tenant, "colDir", w.colDir, "err", cerr)
				}
				s.metrics.incRun(compactorStatusError)
				return nil
			}
			if merged {
				did.Add(1)
				s.metrics.incRun(compactorStatusSuccess)
				s.metrics.setLastRunNow()
			}
			return nil
		})
	}
}

// buildWorkList discovers all (tenant, colDir) pairs this shard owns.
func (s *Service) buildWorkList(ctx context.Context) []columnWork {
	tenants, err := s.resolveTenants(ctx)
	if err != nil {
		s.metrics.incError(compactorOpList)
		return nil
	}
	var work []columnWork
	for _, tenant := range tenants {
		tenantPrefix := path.Join(tenant, s.cfg.IndexPrefix) + "/"
		colDirs, err := s.store.ListDirs(ctx, tenantPrefix)
		if err != nil {
			s.metrics.incError(compactorOpList)
			continue
		}
		for _, colDir := range colDirs {
			colName := path.Base(strings.TrimSuffix(colDir, "/"))
			// NOTE-VI-096: "unique_values" is VCNT's top-level directory
			// (<tenant>/indexes/unique_values/<colHash>/...), not a VI column-hash
			// directory. VI must never descend into it -- doing so previously let VI's
			// compactor pick up .vcnt files as false-positive candidates and delete them
			// on magic-byte mismatch. See NOTES.md dated entry.
			if colName == "unique_values" {
				continue
			}
			if s.cfg.ShardCount > 1 && !s.ownsShard(colName) {
				continue
			}
			typeDirs, err := s.store.ListDirs(ctx, colDir)
			if err != nil {
				s.metrics.incError(compactorOpList)
				continue
			}
			for _, typeDir := range typeDirs {
				work = append(work, columnWork{
					tenant: tenant,
					colDir: strings.TrimSuffix(typeDir, "/"),
				})
			}
		}
	}
	return work
}

// ownsShard reports whether a column (identified by its 32-char hex hash) belongs
// to this compactor instance. The first byte of the hash (two hex digits) is
// decoded to a uint8 and mapped: byte % ShardCount == ShardIndex. If the column
// name is not a valid hex string, the column is always owned (fail-safe).
func (s *Service) ownsShard(colHash string) bool {
	if len(colHash) < 2 {
		return true
	}
	b, err := hex.DecodeString(colHash[:2])
	if err != nil || len(b) == 0 {
		return true
	}
	return int(b[0])%s.cfg.ShardCount == s.cfg.ShardIndex
}

// resolveTenants returns the tenant IDs to compact. For an explicit list it
// returns the list verbatim; for "*" it discovers tenants by listing the index
// prefix and extracting the first path segment after the prefix.
func (s *Service) resolveTenants(ctx context.Context) ([]string, error) {
	if !s.cfg.allTenants() {
		return s.cfg.Tenants, nil
	}

	// Key layout is <tenant>/<indexPrefix>/..., so tenant dirs sit at the root.
	// List the top-level dirs (non-recursive) and return those whose names do not
	// equal the index prefix itself (which would be a misconfigured flat layout).
	topDirs, err := s.store.ListDirs(ctx, "")
	if err != nil {
		s.metrics.incError(compactorOpList)
		return nil, err
	}
	seen := make(map[string]struct{})
	var tenants []string
	for _, dir := range topDirs {
		seg := strings.TrimSuffix(dir, "/")
		if seg == "" || seg == s.cfg.IndexPrefix {
			continue
		}
		if _, dup := seen[seg]; dup {
			continue
		}
		seen[seg] = struct{}{}
		tenants = append(tenants, seg)
	}
	sort.Strings(tenants)
	return tenants, nil
}

// levelFile pairs a key with its parsed compaction level and object size.
type levelFile struct {
	key   string
	level int
	size  int64
}

// effectiveBatchBytes scales CompactBatchBytes down by CompactConcurrency so
// aggregate worst-case output-buffer memory across all concurrently in-flight
// merges stays bounded near CompactBatchBytes regardless of concurrency. An
// explicit "no cap" (0) always survives scaling unchanged, and integer
// division is floored to 1 rather than allowed to truncate a real cap to 0.
func (s *Service) effectiveBatchBytes() int64 {
	if s.cfg.CompactBatchBytes <= 0 {
		return s.cfg.CompactBatchBytes
	}
	concurrency := int64(s.cfg.CompactConcurrency)
	if concurrency < 1 {
		concurrency = 1
	}
	effective := s.cfg.CompactBatchBytes / concurrency
	if effective < 1 {
		effective = 1
	}
	return effective
}

// compactColumn compacts the lowest compaction level present in one column
// directory, if that level has at least CompactThresholdFiles files. Returns
// true when a merge was performed, false when there was nothing to do.
func (s *Service) compactColumn(ctx context.Context, tenant, colDir string, objs []IndexObject) (bool, error) {
	// Parse levels, group by level. Magic-byte validation (Peek) is deliberately NOT
	// done here for every listed object -- for a column with a large backlog (thousands
	// of L0 files), that would cost one S3 round-trip per file on every single attempt
	// regardless of CompactMaxInputFiles, dominating attempt latency long before the
	// actual (bounded) merge even starts. It is instead deferred to just this attempt's
	// capped candidate batch, below, so legacy-file purging happens gradually across
	// attempts for a large backlog rather than eagerly on every listed file.
	byLevel := make(map[int][]levelFile)
	var skipped int
	for _, obj := range objs {
		level, _, err := valueindex.ParseFilename(path.Base(obj.Key))
		if err != nil {
			// Not a value index file (or malformed) — skip it.
			skipped++
			continue
		}
		byLevel[level] = append(byLevel[level], levelFile{key: obj.Key, level: level, size: obj.Size})
	}
	s.metrics.setBacklogL0(tenant, len(byLevel[0]))

	// Find the lowest level that meets the threshold.
	levels := make([]int, 0, len(byLevel))
	for lvl := range byLevel {
		levels = append(levels, lvl)
	}
	sort.Ints(levels)

	const vbg2Magic uint32 = 0x56424732
	for _, lvl := range levels {
		files := byLevel[lvl]
		if len(files) < s.cfg.CompactThresholdFiles {
			continue
		}
		// Cap the batch by total input bytes (oldest files first) so peak memory
		// stays bounded regardless of individual file size.
		sort.Slice(files, func(i, j int) bool { return files[i].key < files[j].key })
		// Cap the batch by file count first, independent of bytes, to bound
		// fd/local-disk usage regardless of individual file size. Floored to 2 so
		// a pathological cap (e.g. 1) never stalls the merge on a 1-file no-op.
		if s.cfg.CompactMaxInputFiles > 0 && len(files) > s.cfg.CompactMaxInputFiles {
			maxFiles := s.cfg.CompactMaxInputFiles
			if maxFiles < 2 {
				maxFiles = 2
			}
			files = files[:maxFiles]
		}
		// maxForcedBatchMultiple bounds how far the "always include >= 2 files" floor
		// below is allowed to push the batch past effectiveBatchBytes. That floor
		// exists to avoid stalling when the cap is smaller than 2 small files'
		// combined size -- a normal, expected scenario. But once individual files
		// have themselves grown far larger than the cap (typically after many rounds
		// of merging have compounded sizes at a high level, e.g. multi-GB files at
		// L5+), forcing the minimum would combine files whose total size vastly
		// exceeds what the cap was ever meant to bound, risking OOM regardless of
		// concurrency. 4x is generous slack for the normal case while still catching
		// genuinely oversized files.
		const maxForcedBatchMultiple = 4
		skipOversizedLevel := false
		if effectiveBatchBytes := s.effectiveBatchBytes(); effectiveBatchBytes > 0 {
			var batchBytes int64
			for cut := range files {
				batchBytes += files[cut].size
				if batchBytes > effectiveBatchBytes {
					// Always include at least 2 files (threshold) to make progress --
					// but only within maxForcedBatchMultiple's safety margin (see above).
					if cut < 2 {
						floor := min(2, len(files))
						var floorBytes int64
						for _, f := range files[:floor] {
							floorBytes += f.size
						}
						if floorBytes > effectiveBatchBytes*maxForcedBatchMultiple {
							skipOversizedLevel = true
							break
						}
						cut = 2
					}
					files = files[:cut]
					break
				}
			}
		}
		if skipOversizedLevel {
			s.metrics.incOversizedLevelsSkipped()
			continue
		}

		// Stage 3, traceindex.go wiring plan (Finding 2): trace-index colDirs
		// carry TraceGroup files, which have no vbg2Magic framing at all -- the
		// dispatch decision MUST be made here, before the Peek/purge loop below
		// runs, or that loop would silently delete every one of them as legacy
		// junk on its very first pass.
		if isTraceIndexColDir(colDir) {
			if err := s.mergeTraceLevel(ctx, colDir, files); err != nil {
				s.metrics.incSkipped(skipped)
				return false, err
			}
			s.metrics.incColumnsCompacted(lvl)
			s.metrics.incSkipped(skipped)
			return true, nil
		}

		// Peek-validate only this attempt's bounded candidate batch, purging any
		// legacy VIMT/VINX files found within it before any full download.
		valid := make([]levelFile, 0, len(files))
		var purged int
		for _, f := range files {
			if err := ctx.Err(); err != nil {
				return false, err
			}
			head, perr := s.store.Peek(ctx, f.key, 4)
			if perr == nil && len(head) == 4 && binary.LittleEndian.Uint32(head) != vbg2Magic {
				purged++
				_ = s.store.Delete(ctx, f.key)
				continue
			}
			valid = append(valid, f)
		}
		skipped += purged
		if len(valid) < 2 {
			// The whole candidate batch was legacy junk that just got purged -- try
			// again next lap with whatever files remain.
			continue
		}
		files = valid

		if err := s.mergeLevel(ctx, colDir, files); err != nil {
			s.metrics.incSkipped(skipped)
			return false, err
		}
		s.metrics.incColumnsCompacted(lvl)
		s.metrics.incSkipped(skipped)
		return true, nil
	}
	s.metrics.incSkipped(skipped)
	return false, nil
}

// mergeLevel reads all files at one level, merges + dedups them via
// valueindex.StreamCompactBucketFiles (dropping retention-deleted sources), writes the
// output file(s) at level+1, then deletes the inputs. Outputs are written before
// inputs are deleted so a crash mid-merge leaves the inputs in place for retry.
//
// SPEC-VI-1 (valueindexcompactor, corrected): inputs are staged to local disk one at a time
// as they are fetched; each input's decoded representation is bounded to one block at a time
// via a disk-backed lazy iterator (valueindex.NewDiskBucketFileIterator) — peak decoded memory
// is bounded by the number of concurrently-open iterators times one block, not by the number
// or total size of input files.
func (s *Service) mergeLevel(ctx context.Context, colDir string, files []levelFile) error {
	mergeStart := s.now()

	// Sort inputs by key for deterministic ordering.
	sort.Slice(files, func(i, j int) bool { return files[i].key < files[j].key })

	outputLevel := files[0].level + 1

	// NOTE-VI-077 (#482): MaxOutputBytes is threaded into StreamCompactBucketFiles below so
	// the v2 BucketGroup path splits its output into multiple size-bounded files at block
	// boundaries once the cap is exceeded, instead of emitting one unbounded file per input
	// set. Each emitted file gets its own V2 filename (time range embedded) so discovery
	// pruning (DiscoverIndexFiles) treats them independently.
	var checker valueindex.RefChecker
	if s.exister != nil {
		checker = newCachingRefChecker(s.exister)
	}

	// NOTE-VI-045 (#429)/NOTE-VI-046 (corrected): s.store.Get stays exactly once per input
	// file, but instead of decoding the fetched bytes into a fully in-memory *BucketFile,
	// they are staged to a local temp file and wrapped in a disk-backed lazy iterator that
	// decodes one block at a time. Magic was already checked in compactColumn via Peek;
	// download only known-VBG2 files.
	iterators := make([]valueindex.GroupIterator, 0, len(files))
	defer func() {
		for _, it := range iterators {
			_ = it.Close()
		}
	}()
	for _, f := range files {
		if err := ctx.Err(); err != nil {
			return err
		}
		data, err := s.store.Get(ctx, f.key)
		if err != nil {
			s.metrics.incError(compactorOpGet)
			return fmt.Errorf("valueindexcompactor: get %q: %w", f.key, err)
		}
		tmpPath, err := writeLocalTempInput(os.TempDir(), data)
		if err != nil {
			return fmt.Errorf("valueindexcompactor: stage %q locally: %w", f.key, err)
		}
		it, err := valueindex.NewDiskBucketFileIterator(ctx, tmpPath, checker)
		if err != nil {
			_ = os.Remove(tmpPath)
			return fmt.Errorf("valueindexcompactor: decode %q: %w", f.key, err)
		}
		if it == nil {
			// Legacy pre-v2 file: skip, not abort. No iterator was constructed, so
			// nothing else owns tmpPath (plan.md Decision 2's ownership table).
			_ = os.Remove(tmpPath)
			continue
		}
		iterators = append(iterators, it)
	}

	var written int
	err := valueindex.StreamCompactBucketFiles(ctx, iterators, 0, s.cfg.MaxOutputBytes, func(outPath string) error {
		//nolint:gosec // G304: outPath is StreamCompactBucketFiles' own local temp output file, not user input
		data, err := os.ReadFile(outPath)
		if err != nil {
			return fmt.Errorf("valueindexcompactor: read local output %q: %w", outPath, err)
		}
		// NOTE-VI-037 (#431): embed the merged file's wall time range in the output
		// filename so DiscoverIndexFiles/IndexFileCache can prune compacted files by
		// time exactly as it prunes L0 files. Writing a v1 filename (no range) here
		// would make the output unparseable by ParseFilenameV2 (the v1 fallback was
		// removed), so it would be silently skipped by discovery entirely rather than
		// found. The BucketGroup footer carries file-level min/max time_sec directly.
		var wallMinSec, wallMaxSec uint64
		if ft, ferr := valueindex.DecodeBucketFooter(data); ferr == nil {
			wallMinSec, wallMaxSec = ft.MinTimeSec, ft.MaxTimeSec
		}
		key := path.Join(colDir, valueindex.FormatFilenameV2(outputLevel, wallMinSec, wallMaxSec, valueindex.NewID()))
		if err := s.store.Put(ctx, key, data); err != nil {
			s.metrics.incError(compactorOpPut)
			return fmt.Errorf("valueindexcompactor: put %q: %w", key, err)
		}
		written++
		return nil
	})
	if err != nil {
		return err
	}

	// Every iterator is fully drained by a successful StreamCompactBucketFiles call, so each
	// disk-backed iterator's cumulative per-file retention stats (accumulated block-by-block
	// as StreamCompactBucketFiles' merge loop calls Advance) now reflect a complete total.
	var stats valueindex.CompactStats
	for _, it := range iterators {
		if sp, ok := it.(valueindex.StatsProvider); ok {
			fstats := sp.Stats()
			stats.Retained += fstats.Retained
			stats.Dropped += fstats.Dropped
		}
	}

	// Delete inputs only after all outputs are durably written.
	var firstErr error
	var deleted int
	for _, f := range files {
		if err := s.store.Delete(ctx, f.key); err != nil {
			s.metrics.incError(compactorOpDelete)
			if firstErr == nil {
				firstErr = fmt.Errorf("valueindexcompactor: delete %q: %w", f.key, err)
			}
			continue
		}
		deleted++
	}

	s.metrics.observeMerge(s.now().Sub(mergeStart))
	s.metrics.addMergeCounts(len(files), written, deleted, stats.Retained, stats.Dropped)
	return firstErr
}

// RunOnce performs one cycle through all columns this service owns, compacting
// each that meets the threshold. It is provided for testing; production code
// uses Run which loops continuously without a pass boundary.
func (s *Service) RunOnce(ctx context.Context) error {
	start := s.now()
	work := s.buildWorkList(ctx)
	var firstErr error
	for _, w := range work {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		keys, err := s.store.List(ctx, w.colDir+"/")
		if err != nil {
			s.metrics.incError(compactorOpList)
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
		if _, err := s.compactColumn(ctx, w.tenant, w.colDir, keys); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	s.metrics.observeRun(s.now().Sub(start))
	if firstErr != nil {
		s.metrics.incRun(compactorStatusError)
	} else {
		s.metrics.incRun(compactorStatusSuccess)
	}
	return firstErr
}

// isObjectNotFoundErr reports whether err looks like an object-store "key not found"
// error. Store.Get/Delete's contract only promises a plain error (kept backend-agnostic
// so external callers can supply their own S3 client), so this is a best-effort text
// match rather than a type assertion against a specific client library's error type.
func isObjectNotFoundErr(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "specified key does not exist") ||
		strings.Contains(msg, "nosuchkey") ||
		strings.Contains(msg, "no such key")
}
