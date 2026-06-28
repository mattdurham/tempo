package valueindexcompactor

import (
	"context"
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

// resolveTenants returns the tenant IDs to compact. For an explicit list it
// returns the list verbatim; for "*" it discovers tenants by listing the index
// prefix and extracting the first path segment after the prefix.
func (s *Service) resolveTenants(ctx context.Context) ([]string, error) {
	if !s.cfg.allTenants() {
		return s.cfg.Tenants, nil
	}

	prefix := s.cfg.IndexPrefix + "/"
	keys, err := s.store.List(ctx, prefix)
	if err != nil {
		s.metrics.incError(compactorOpList)
		return nil, err
	}
	seen := make(map[string]struct{})
	var tenants []string
	for _, key := range keys {
		rest := strings.TrimPrefix(key, prefix)
		seg, _, ok := strings.Cut(rest, "/")
		if !ok || seg == "" {
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

// compactTenant compacts every column directory for one tenant. The first error
// is returned but all columns are still attempted.
func (s *Service) compactTenant(ctx context.Context, tenant string) error {
	tenantPrefix := path.Join(s.cfg.IndexPrefix, tenant) + "/"
	keys, err := s.store.List(ctx, tenantPrefix)
	if err != nil {
		s.metrics.incError(compactorOpList)
		return fmt.Errorf("valueindexcompactor: list tenant %q: %w", tenant, err)
	}

	// Group keys by column directory.
	byColumn := make(map[string][]string)
	for _, key := range keys {
		dir := path.Dir(key)
		byColumn[dir] = append(byColumn[dir], key)
	}

	var firstErr error
	for colDir, colKeys := range byColumn {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := s.compactColumn(ctx, colDir, colKeys); err != nil && firstErr == nil {
			firstErr = err
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
	for _, key := range keys {
		level, _, err := valueindex.ParseFilename(path.Base(key))
		if err != nil {
			// Not a value index file (or malformed) — skip it.
			continue
		}
		byLevel[level] = append(byLevel[level], levelFile{key: key, level: level})
	}

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
		return s.mergeLevel(ctx, colDir, files)
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

	readers := make([]*valueindex.Reader, 0, len(files))
	for _, f := range files {
		data, err := s.store.Get(ctx, f.key)
		if err != nil {
			s.metrics.incError(compactorOpGet)
			return fmt.Errorf("valueindexcompactor: get %q: %w", f.key, err)
		}
		r, err := valueindex.OpenReader(data)
		if err != nil {
			return fmt.Errorf("valueindexcompactor: open %q: %w", f.key, err)
		}
		readers = append(readers, r)
	}

	outputLevel := files[0].level + 1

	cfg := valueindex.CompactConfig{MaxOutputBytes: s.cfg.MaxOutputBytes}
	if s.exister != nil {
		cfg.Checker = newCachingRefChecker(s.exister)
	}

	var written int
	stats, err := valueindex.CompactFiles(ctx, readers, cfg, func(data []byte) error {
		key := path.Join(colDir, valueindex.FormatFilename(outputLevel, valueindex.NewID()))
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
