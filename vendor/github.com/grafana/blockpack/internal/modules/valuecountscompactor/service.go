package valuecountscompactor

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"path"
	"sort"
	"strings"
	"time"

	"github.com/grafana/blockpack/internal/modules/valuecounts"
)

// NOTE: see internal/modules/valuecountscompactor/NOTES.md.
// Any changes to this file must be reflected there.

// Service is the value-counts compactor orchestrator. It periodically scans each configured
// tenant's column directories and compacts same-level VCNT files into fewer, larger files,
// summing counts and dropping any group whose net count is <= 0 (valuecounts.Compact's own
// retention rule — there is no source-existence probe here, unlike valueindexcompactor).
type Service struct {
	store   Store
	metrics *compactorMetrics // nil when Config.Registerer is nil (no-op)
	now     func() time.Time
	cfg     Config
}

// NewService builds a compactor service. cfg.Tenants must be non-empty and store must be
// non-nil.
func NewService(cfg Config, store Store) (*Service, error) {
	cfg = cfg.withDefaults()
	if len(cfg.Tenants) == 0 {
		return nil, errors.New("valuecountscompactor: at least one tenant must be configured")
	}
	if store == nil {
		return nil, errors.New("valuecountscompactor: store is required")
	}
	return &Service{
		store:   store,
		cfg:     cfg,
		metrics: newCompactorMetrics(cfg.Registerer),
		now:     time.Now,
	}, nil
}

// columnWork is a fully-resolved (tenant, colDir) work item.
type columnWork struct {
	tenant string
	colDir string // "<tenant>/value_counts/<colHash>"
}

// Run drives the compaction loop until ctx is canceled. See
// valueindexcompactor.Service.Run's doc comment for the work-stealing/rebuild-on-exhaustion
// design this is ported from verbatim (only the receiver types differ).
func (s *Service) Run(ctx context.Context) error {
	if !s.cfg.Enabled {
		<-ctx.Done()
		return ctx.Err()
	}

	var (
		work []columnWork
		idx  int
		did  int // merges done in current cycle
	)

	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		if idx >= len(work) {
			if len(work) > 0 && did == 0 {
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-time.After(s.cfg.CompactInterval):
				}
			}
			work = s.buildWorkList(ctx)
			idx = 0
			did = 0
			if len(work) == 0 {
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

		objs, err := s.store.List(ctx, w.colDir+"/")
		if err != nil {
			s.metrics.incError(compactorOpList)
			continue
		}
		merged, err := s.compactColumn(ctx, w.colDir, objs)
		if err != nil {
			// A listed L0 file getting deleted before this Get runs is an expected,
			// self-healing race under concurrent compaction (another merge or retention
			// consumed it first) -- the column is simply retried next lap. Store doesn't
			// expose a typed not-found error (kept backend-agnostic so external callers
			// can supply their own S3 client), so this is a best-effort text match on the
			// error the underlying object store returns for a missing key.
			if isObjectNotFoundErr(err) {
				slog.Warn("valuecountscompactor: compactColumn skipped, input already removed",
					"tenant", w.tenant, "colDir", w.colDir, "err", err)
			} else {
				slog.Error("valuecountscompactor: compactColumn failed", "tenant", w.tenant, "colDir", w.colDir, "err", err)
			}
			s.metrics.incRun(compactorStatusError)
		}
		if merged {
			did++
			s.metrics.incRun(compactorStatusSuccess)
			s.metrics.lastRun.SetToCurrentTime()
		}
	}
}

// buildWorkList discovers all (tenant, colDir) pairs this shard owns via a one-level
// directory walk (VCNT's key layout has no <type> segment, unlike valueindexcompactor's
// three-level walk).
func (s *Service) buildWorkList(ctx context.Context) []columnWork {
	tenants, err := s.resolveTenants(ctx)
	if err != nil {
		s.metrics.incError(compactorOpList)
		return nil
	}
	var work []columnWork
	for _, tenant := range tenants {
		// NOTE-VC-019: value_counts is a direct child of tenant, a sibling of the VI
		// indexPrefix tree and cube's own top-level prefix -- not nested under either.
		prefix := path.Join(tenant, "value_counts") + "/"
		colDirs, err := s.store.ListDirs(ctx, prefix)
		if err != nil {
			s.metrics.incError(compactorOpList)
			continue
		}
		for _, colDir := range colDirs {
			colHash := path.Base(strings.TrimSuffix(colDir, "/"))
			if s.cfg.ShardCount > 1 && !s.ownsShard(colHash) {
				continue
			}
			work = append(work, columnWork{tenant: tenant, colDir: strings.TrimSuffix(colDir, "/")})
		}
	}
	return work
}

// ownsShard reports whether a column (identified by its hex hash) belongs to this compactor
// instance. The first byte of the hash (two hex digits) is decoded to a uint8 and mapped:
// byte % ShardCount == ShardIndex. If the column hash is not a valid hex string, the column
// is always owned (fail-safe). Byte-identical construction to
// valueindexcompactor.Service.ownsShard — pinned by TestOwnsShard_MatchesValueIndexConvention
// since valuecounts.ColHash and valueindex.ColHash are not compile-time coupled.
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

// resolveTenants returns the tenant IDs to compact. For an explicit list it returns the list
// verbatim; for "*" it discovers tenants by listing the bucket root and treating each top-level
// directory as a tenant ID (VCNT's key layout is tenant-first: <tenant>/value_counts/<colHash>).
func (s *Service) resolveTenants(ctx context.Context) ([]string, error) {
	if !s.cfg.allTenants() {
		return s.cfg.Tenants, nil
	}

	topDirs, err := s.store.ListDirs(ctx, "")
	if err != nil {
		s.metrics.incError(compactorOpList)
		return nil, err
	}
	seen := make(map[string]struct{})
	var tenants []string
	for _, dir := range topDirs {
		seg := strings.TrimSuffix(dir, "/")
		if seg == "" {
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

// levelFile pairs a key with its parsed compaction level, object size, and wall-clock time
// range (minSec/maxSec, from the v2 filename — issue #494). minSec/maxSec are populated by
// compactColumn's ParseFilenameV2 parse and consumed by clusterByTimeRange/pickCluster.
type levelFile struct {
	key    string
	level  int
	size   int64
	minSec uint64
	maxSec uint64
}

// compactColumn compacts one time-cluster of same-level files in one column directory, if that
// cluster has at least CompactThresholdFiles files. Candidate files are parsed via
// valuecounts.ParseFilenameV2 (an unparseable filename -- including any straggler v1-format
// file, issue #494 R4 -- increments the skip counter and is left in place, exactly as with a
// magic-header mismatch elsewhere in this codebase). Same-level candidates are then greedily
// clustered by wall-clock time range (clusterByTimeRange) and one cluster is chosen
// (pickCluster: most files wins, ties broken by earliest start -- see cluster.go's doc
// comments for the full rule). The chosen cluster must itself meet CompactThresholdFiles --
// meeting the threshold at the level's raw total is not sufficient, since the level's files
// may be split across disjoint clusters none of which individually qualify. Returns true when
// a merge was performed, false when there was nothing to do.
func (s *Service) compactColumn(ctx context.Context, colDir string, objs []Object) (bool, error) {
	byLevel := make(map[int][]levelFile, len(objs))
	var skipped int
	for _, obj := range objs {
		meta, err := valuecounts.ParseFilenameV2(path.Base(obj.Key))
		if err != nil {
			skipped++
			continue
		}
		byLevel[meta.Level] = append(byLevel[meta.Level], levelFile{
			key: obj.Key, level: meta.Level, size: obj.Size,
			minSec: meta.WallMinSec, maxSec: meta.WallMaxSec,
		})
	}
	s.metrics.incSkipped(skipped)

	levels := make([]int, 0, len(byLevel))
	for lvl := range byLevel {
		levels = append(levels, lvl)
	}
	sort.Ints(levels)

	for _, lvl := range levels {
		clusters := clusterByTimeRange(byLevel[lvl], s.cfg.MaxTimeSpanPerMerge)
		chosen := pickCluster(clusters)
		if len(chosen) < s.cfg.CompactThresholdFiles {
			continue
		}
		chosen = s.capBatchBytes(chosen)
		if err := s.mergeLevel(ctx, colDir, chosen); err != nil {
			return false, err
		}
		s.metrics.incColumnsCompacted(lvl)
		return true, nil
	}
	return false, nil
}

// capBatchBytes trims files (oldest-by-key first) down to CompactBatchBytes total input bytes,
// always keeping at least CompactThresholdFiles files so a pass makes progress even if the
// very first file alone exceeds the cap. Applied WITHIN a single already-chosen time cluster
// (issue #494, R2) -- the decoded-record-count admission gate (MaxRecordsPerMerge) is a
// separate, independent cap enforced inside mergeLevel, since record counts aren't knowable
// from Object.Size alone.
func (s *Service) capBatchBytes(files []levelFile) []levelFile {
	if s.cfg.CompactBatchBytes <= 0 {
		return files
	}
	sort.Slice(files, func(i, j int) bool { return files[i].key < files[j].key })
	var batchBytes int64
	for cut := range files {
		batchBytes += files[cut].size
		if batchBytes > s.cfg.CompactBatchBytes {
			if cut < s.cfg.CompactThresholdFiles {
				cut = s.cfg.CompactThresholdFiles
			}
			return files[:cut]
		}
	}
	return files
}

// mergeLevel reads files at one level (stopping early if MaxRecordsPerMerge is reached),
// decodes each via valuecounts.DecodeVCNTObject (the self-describing EncodeVCNTFile format —
// the only format written since #490 A-3/A-Tempo-1), accumulates their records, merges/sums/nets them via
// valuecounts.Compact, writes the result at level+1 using the self-describing EncodeVCNTFile
// format, then deletes only the inputs actually processed. Write-then-delete: inputs are only
// removed after the merged output's Put succeeds. Files left unprocessed because the record
// ceiling was hit are deferred to the next pass, not deleted or otherwise touched.
//
// Output filenames are v2-format (valuecounts.FormatFilenameV2, issue #494), embedding the
// genuine [minSec,maxSec] range returned by valuecounts.TimeRange(merged) — a full scan of the
// merged records, never a copy of one input's range or the last-sorted record's TimeEnd. See
// SPEC-VC-3/SPEC-VC-7.
//
// NOTE-VC-009 (known, documented residual risk — not eliminated by the retry mitigation
// below): unlike valueindexcompactor, valuecounts.Compact SUMS Count per merge key rather than
// deduping by identity (Record carries no identity field). A *partial* Delete failure within one
// batch (some inputs deleted, one or more not, while the merged output Put already succeeded) can
// leave a surviving input that gets summed a second time by a later merge, permanently
// double-counting that value. deleteWithRetry (below) retries each failed Delete a few times to
// close the window for transient failures, and any exhausted-retry failure is surfaced via both
// the returned error and a dedicated metric — but a sufficiently persistent storage outage can
// still exceed the retry budget. This is a documented limitation, not a design guarantee.
//
// SPEC-VC-1 (valuecountscompactor): inputs are decoded one at a time and the raw compressed
// bytes go out of scope immediately after decode — this function never holds all inputs' raw
// bytes in memory simultaneously.
// SPEC-VC-2 (valuecountscompactor): the MaxRecordsPerMerge decoded-record-count admission gate
// defers files beyond the ceiling to the next pass; it never drops or corrupts them.
func (s *Service) mergeLevel(ctx context.Context, colDir string, files []levelFile) error {
	mergeStart := s.now()
	sort.Slice(files, func(i, j int) bool { return files[i].key < files[j].key })
	outputLevel := files[0].level + 1
	maxRecords := s.cfg.MaxRecordsPerMerge

	var all []valuecounts.Record
	processed := make([]levelFile, 0, len(files))
	var deferred int
	for _, f := range files {
		if err := ctx.Err(); err != nil {
			return err
		}
		data, err := s.store.Get(ctx, f.key)
		if err != nil {
			s.metrics.incError(compactorOpGet)
			return fmt.Errorf("valuecountscompactor: get %q: %w", f.key, err)
		}
		recs, err := valuecounts.DecodeVCNTObject(data)
		if err != nil {
			// DecodeVCNTObject only understands the self-describing format now (#490
			// A-3) -- any error here means this file's data is unrecoverable with this
			// codebase, not a transient read glitch.
			// Retrying changes nothing for a deterministic decode failure, so quarantine
			// (delete) it immediately rather than leaving it to block every future
			// compaction attempt for this column forever. This is a deliberate,
			// logged data-loss decision, not a silent drop.
			s.metrics.incError(compactorOpDecode)
			s.metrics.incQuarantined(1)
			slog.Error("valuecountscompactor: quarantining undecodable input, data is unrecoverable",
				"colDir", colDir, "key", f.key, "err", err)
			_ = s.store.Delete(ctx, f.key)
			continue
		}
		all = append(all, recs...)
		processed = append(processed, f)
		// Decoded-record-count admission gate: CompactBatchBytes already bounded compressed
		// input bytes in compactColumn; this bounds decoded RSS directly. Always process at
		// least CompactThresholdFiles files so a pass makes progress even if the very first
		// files already exceed the ceiling (mirrors compactColumn's CompactBatchBytes "always
		// include >= threshold" floor).
		if maxRecords > 0 && len(all) >= maxRecords && len(processed) >= s.cfg.CompactThresholdFiles {
			deferred = len(files) - len(processed)
			break
		}
	}

	merged := valuecounts.Compact(all)
	var written int
	if len(merged) > 0 {
		minSec, maxSec := valuecounts.TimeRange(merged)
		data := valuecounts.EncodeVCNTFile(merged, 0)
		key := path.Join(colDir, valuecounts.FormatFilenameV2(outputLevel, minSec, maxSec, valuecounts.NewID()))
		if err := s.store.Put(ctx, key, data); err != nil {
			s.metrics.incError(compactorOpPut)
			return fmt.Errorf("valuecountscompactor: put %q: %w", key, err)
		}
		written = 1
	}

	var firstErr error
	var deleted int
	for _, f := range processed {
		// NOTE-VC-009: retry a failed Delete a few times before giving up on this key — see
		// deleteWithRetry's doc comment for why this matters more here than it would for
		// valueindexcompactor's identity-deduped equivalent.
		if err := s.deleteWithRetry(ctx, f.key); err != nil {
			s.metrics.incError(compactorOpDelete)
			s.metrics.incDeleteFailedAfterRetry()
			if firstErr == nil {
				firstErr = fmt.Errorf(
					"valuecountscompactor: delete %q failed after %d attempts: %w",
					f.key, deleteMaxAttempts, err,
				)
			}
			continue
		}
		deleted++
	}

	s.metrics.observeMerge(s.now().Sub(mergeStart))
	s.metrics.addMergeCounts(len(processed), written, deleted, len(all), len(merged))
	s.metrics.incDeferred(deferred)
	return firstErr
}

// deleteMaxAttempts and deleteRetryBackoff bound mergeLevel's mitigation for the CRITICAL
// finding recorded in NOTE-VC-009: because valuecounts.Compact sums Count rather than deduping
// by identity, a survivor left behind by a partial Delete failure can be double-counted by a
// future merge. Retrying a handful of times with a short, fixed backoff closes the window for
// the vast majority of real transient object-storage failures (throttling, transient 5xx,
// timeout) without requiring a design change. A fixed (not exponential) backoff is used
// deliberately: this is meant to ride out brief transient errors, not a prolonged outage — a
// prolonged outage is caught by the final returned error and the
// merge_delete_failed_after_retry_total metric instead of an ever-growing wait.
const (
	deleteMaxAttempts  = 3
	deleteRetryBackoff = 20 * time.Millisecond
)

// deleteWithRetry calls store.Delete for key, retrying up to deleteMaxAttempts times with a
// deleteRetryBackoff pause between attempts if it fails. Returns nil on the first successful
// attempt, or the last observed error once attempts are exhausted. Aborts early (returning
// ctx.Err()) if ctx is canceled while waiting between attempts.
func (s *Service) deleteWithRetry(ctx context.Context, key string) error {
	var err error
	for attempt := 0; attempt < deleteMaxAttempts; attempt++ {
		if attempt > 0 {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(deleteRetryBackoff):
			}
		}
		if err = s.store.Delete(ctx, key); err == nil {
			return nil
		}
	}
	return err
}

// RunOnce performs one cycle through all columns this service owns, compacting each that
// meets the threshold. It is provided for testing; production code uses Run which loops
// continuously without a pass boundary.
func (s *Service) RunOnce(ctx context.Context) error {
	start := s.now()
	work := s.buildWorkList(ctx)
	var firstErr error
	for _, w := range work {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		objs, err := s.store.List(ctx, w.colDir+"/")
		if err != nil {
			s.metrics.incError(compactorOpList)
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
		if _, err := s.compactColumn(ctx, w.colDir, objs); err != nil && firstErr == nil {
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
// error. Store.Get's contract only promises a plain error (kept backend-agnostic so
// external callers can supply their own S3 client), so this is a best-effort text match
// rather than a type assertion against a specific client library's error type.
func isObjectNotFoundErr(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "specified key does not exist") ||
		strings.Contains(msg, "nosuchkey") ||
		strings.Contains(msg, "no such key")
}
