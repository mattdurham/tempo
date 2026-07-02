package valueindexcompactor

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"path"
	"sort"
	"strings"
	"time"

	"github.com/grafana/blockpack/internal/modules/valueindex"
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
func NewService(cfg Config, store IndexStore, exister SourceExister) (*Service, error) {
	cfg = cfg.withDefaults()
	if len(cfg.Tenants) == 0 {
		return nil, errors.New("valueindexcompactor: at least one tenant must be configured")
	}
	if store == nil {
		return nil, errors.New("valueindexcompactor: store is required")
	}
	return &Service{
		store:   store,
		exister: exister,
		cfg:     cfg,
		metrics: newCompactorMetrics(cfg.Registerer),
		now:     time.Now,
	}, nil
}

// Run drives the compaction loop until ctx is canceled. It runs one pass
// immediately, then once per CompactInterval. Run returns ctx.Err() on clean
// shutdown. A pass error is non-fatal: it is returned so the caller can log it,
// but the loop continues on the next tick.
func (s *Service) Run(ctx context.Context) error {
	if !s.cfg.Enabled {
		<-ctx.Done()
		return ctx.Err()
	}

	ticker := time.NewTicker(s.cfg.CompactInterval)
	defer ticker.Stop()

	// Run one pass immediately so startup does not wait a full interval.
	_ = s.RunOnce(ctx)

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			_ = s.RunOnce(ctx)
		}
	}
}

// RunOnce performs a single compaction pass over all configured tenants and
// their column directories. It returns the first error encountered; remaining
// tenants/columns are still attempted so one bad column does not stall the rest.
func (s *Service) RunOnce(ctx context.Context) error {
	runStart := s.now()

	// Snapshot backlog before we start compacting so the gauge reflects the
	// queue depth at pass start, not after partial compaction.
	s.snapshotBacklog(ctx)

	tenants, err := s.resolveTenants(ctx)
	if err != nil {
		s.metrics.observeRun(s.now().Sub(runStart))
		s.metrics.incRun(compactorStatusError)
		return fmt.Errorf("valueindexcompactor: resolve tenants: %w", err)
	}

	var firstErr error
	for _, tenant := range tenants {
		if err := ctx.Err(); err != nil {
			s.metrics.observeRun(s.now().Sub(runStart))
			s.metrics.incRun(compactorStatusError)
			return err
		}
		if err := s.compactTenant(ctx, tenant); err != nil && firstErr == nil {
			firstErr = err
		}
	}

	s.metrics.observeRun(s.now().Sub(runStart))
	if firstErr != nil {
		s.metrics.incRun(compactorStatusError)
	} else {
		s.metrics.incRun(compactorStatusSuccess)
	}
	return firstErr
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

// snapshotBacklog counts L0 files per tenant and records them as the backlog
// gauge. It uses the same ListDirs walk as compactTenant but only counts L0
// filenames rather than reading content, so it is cheap (no Get calls).
// Errors are silently swallowed — a failed snapshot is non-fatal.
func (s *Service) snapshotBacklog(ctx context.Context) {
	tenants, err := s.resolveTenants(ctx)
	if err != nil {
		return
	}
	for _, tenant := range tenants {
		tenantPrefix := path.Join(tenant, s.cfg.IndexPrefix) + "/"
		colDirs, err := s.store.ListDirs(ctx, tenantPrefix)
		if err != nil {
			continue
		}
		var l0Count int
		for _, colDir := range colDirs {
			if s.cfg.ShardCount > 1 {
				colName := path.Base(strings.TrimSuffix(colDir, "/"))
				if !s.ownsShard(colName) {
					continue
				}
			}
			typeDirs, err := s.store.ListDirs(ctx, colDir)
			if err != nil {
				continue
			}
			for _, typeDir := range typeDirs {
				keys, err := s.store.List(ctx, typeDir)
				if err != nil {
					continue
				}
				for _, key := range keys {
					level, _, err := valueindex.ParseFilename(path.Base(key))
					if err == nil && level == 0 {
						l0Count++
					}
				}
			}
		}
		s.metrics.setBacklogL0(tenant, l0Count)
	}
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

// compactTenant compacts every column directory for one tenant. Rather than
// issuing one giant recursive list of all tenant files (which can be millions
// of objects and takes many minutes), it walks the two-level
// <col-hash>/<type>/ hierarchy with three cheap non-recursive directory
// listings: one for col-hash dirs, one per col-hash for type dirs, and one per
// (col-hash, type) for the actual index files. This keeps each individual S3
// list call small and lets compaction start within seconds.
//
// The first error is returned but all columns are still attempted.
func (s *Service) compactTenant(ctx context.Context, tenant string) error {
	// Key layout written by WriteValueIndexL0: <tenant>/<indexPrefix>/<colHash>/<type>/...
	// Match that order here so we list the right prefix.
	tenantPrefix := path.Join(tenant, s.cfg.IndexPrefix) + "/"

	// List immediate column-hash subdirs (non-recursive).
	colDirs, err := s.store.ListDirs(ctx, tenantPrefix)
	if err != nil {
		s.metrics.incError(compactorOpList)
		return fmt.Errorf("valueindexcompactor: list tenant %q: %w", tenant, err)
	}

	var firstErr error
	for _, colDir := range colDirs {
		if err := ctx.Err(); err != nil {
			return err
		}

		// Sharding: skip columns that belong to a different shard.
		// The column dir name is the 32-char hex col hash; read the first byte
		// (two hex chars) and assign by: byte % ShardCount == ShardIndex.
		if s.cfg.ShardCount > 1 {
			colName := path.Base(strings.TrimSuffix(colDir, "/"))
			if !s.ownsShard(colName) {
				continue
			}
		}

		// List immediate type subdirs (int64, string, bool, …) under each col-hash.
		typeDirs, err := s.store.ListDirs(ctx, colDir)
		if err != nil {
			s.metrics.incError(compactorOpList)
			if firstErr == nil {
				firstErr = fmt.Errorf("valueindexcompactor: list col %q: %w", colDir, err)
			}
			continue
		}

		for _, typeDir := range typeDirs {
			if err := ctx.Err(); err != nil {
				return err
			}

			// List the actual index files under <col-hash>/<type>/.
			keys, err := s.store.List(ctx, typeDir)
			if err != nil {
				s.metrics.incError(compactorOpList)
				if firstErr == nil {
					firstErr = fmt.Errorf("valueindexcompactor: list type dir %q: %w", typeDir, err)
				}
				continue
			}

			// typeDir is already the exact column directory for compactColumn.
			trimmed := strings.TrimSuffix(typeDir, "/")
			if err := s.compactColumn(ctx, trimmed, keys); err != nil && firstErr == nil {
				firstErr = err
			}
		}
	}
	return firstErr
}

// levelFile pairs a key with its parsed compaction level.
type levelFile struct {
	key   string
	level int
}

// compactColumn compacts the lowest compaction level present in one column
// directory, if that level has at least CompactThresholdFiles files. Only one
// level is compacted per pass; the next level (if it crosses the threshold) is
// picked up on a subsequent pass.
func (s *Service) compactColumn(ctx context.Context, colDir string, keys []string) error {
	// Parse levels, group by level.
	byLevel := make(map[int][]levelFile)
	var skipped int
	for _, key := range keys {
		level, _, err := valueindex.ParseFilename(path.Base(key))
		if err != nil {
			// Not a value index file (or malformed) — skip it.
			skipped++
			continue
		}
		byLevel[level] = append(byLevel[level], levelFile{key: key, level: level})
	}
	s.metrics.incSkipped(skipped)

	// Find the lowest level that meets the threshold.
	levels := make([]int, 0, len(byLevel))
	for lvl := range byLevel {
		levels = append(levels, lvl)
	}
	sort.Ints(levels)

	for _, lvl := range levels {
		files := byLevel[lvl]
		if len(files) < s.cfg.CompactThresholdFiles {
			continue
		}
		if err := s.mergeLevel(ctx, colDir, files); err != nil {
			return err
		}
		s.metrics.incColumnsCompacted(lvl)
		return nil
	}
	return nil
}

// mergeLevel reads all files at one level, merges + dedups them via
// valueindex.CompactFiles (dropping retention-deleted sources), writes the
// output file(s) at level+1, then deletes the inputs. Outputs are written before
// inputs are deleted so a crash mid-merge leaves the inputs in place for retry.
func (s *Service) mergeLevel(ctx context.Context, colDir string, files []levelFile) error {
	mergeStart := s.now()

	// Sort inputs by key for deterministic ordering.
	sort.Slice(files, func(i, j int) bool { return files[i].key < files[j].key })

	// NOTE-VI-045 (#429): inputs are v2 BucketGroup files. Read the raw bytes and merge
	// them via CompactBucketFiles (decode → retention-filter → merge → split → encode).
	fileBytes := make([][]byte, 0, len(files))
	for _, f := range files {
		data, err := s.store.Get(ctx, f.key)
		if err != nil {
			s.metrics.incError(compactorOpGet)
			return fmt.Errorf("valueindexcompactor: get %q: %w", f.key, err)
		}
		fileBytes = append(fileBytes, data)
	}

	outputLevel := files[0].level + 1

	cfg := valueindex.CompactConfig{MaxOutputBytes: s.cfg.MaxOutputBytes}
	if s.exister != nil {
		cfg.Checker = newCachingRefChecker(s.exister)
	}

	var written int
	stats, err := valueindex.CompactBucketFiles(ctx, fileBytes, cfg, 0, func(data []byte) error {
		// NOTE-VI-037 (#431): embed the merged file's wall time range in the output
		// filename so DiscoverIndexFiles/IndexFileCache can prune compacted files by
		// time exactly as it prunes L0 files. Writing v1 filenames (no range) here
		// would force every compacted file to "always match" the time filter,
		// silently defeating discovery pruning for all data above level 0. The
		// BucketGroup footer carries file-level min/max time_sec directly.
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
