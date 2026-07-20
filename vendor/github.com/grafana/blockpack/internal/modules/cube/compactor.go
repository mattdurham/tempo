package cube

// NOTE: SPEC-CUBE-018 — Compactor merges many small L0 flush files for one cube into
// larger L0 files and produces L1/L2 rollup files. It also evicts cubes that have not
// been queried within the eviction window. No per-span S3 reads — only cube files.

import (
	"context"
	"fmt"
	"time"
)

// CompactorConfig parameterises the compactor.
type CompactorConfig struct {
	// EvictionWindowDays cubes not queried for this many days are evicted (default 30).
	EvictionWindowDays int
}

func (c *CompactorConfig) setDefaults() {
	if c.EvictionWindowDays == 0 {
		c.EvictionWindowDays = 30
	}
}

// FileInfo describes one cube file discovered in object storage.
type FileInfo struct {
	Key       string // S3 object key
	MinMinute uint32
	MaxMinute uint32
	Level     uint32 // 1=L0, 60=L1, 1440=L2 (legacy); >= 10000 = new pairwise merge-depth (issue #522 Phase 3)
	// Size is the file's byte size in object storage (issue #522 Phase 3.1) -- needed by
	// compaction-planner's candidate-selection query (the global 1GiB size-cutoff filter,
	// mirroring VI/VCNT's own size_bytes column) once a lister populates blockpack_file_catalog
	// for cube. Zero-valued by any caller that doesn't have it readily available (e.g. a
	// filename-only parse with no object-storage round trip) -- callers that need an accurate
	// size must populate it themselves from their own List/Stat result.
	Size int64
}

// FileStore is the minimal object-storage interface the compactor needs.
type FileStore interface {
	// List returns all cube files for (tenant, cubeID).
	List(ctx context.Context, tenant, cubeID string) ([]FileInfo, error)
	// Get fetches a cube file by key and opens a Reader.
	Get(ctx context.Context, key string) (*Reader, error)
	// Put writes encoded bytes to key.
	Put(key string, data []byte) error
	// Delete removes a cube file by key.
	Delete(ctx context.Context, key string) error
}

// Compactor plans and executes cube file compaction for one cube.
type Compactor struct {
	store    FileStore
	registry *Registry
	cfg      CompactorConfig
}

// NewCompactor creates a Compactor.
func NewCompactor(store FileStore, registry *Registry, cfg CompactorConfig) *Compactor {
	cfg.setDefaults()
	return &Compactor{store: store, registry: registry, cfg: cfg}
}

// CellCount returns the number of distinct cells accumulated in the Writer.
// Exposed for compaction decision-making.
func (w *Writer) CellCount() int { return len(w.cells) }

// Evict removes a cube from the registry and deletes all its S3 files.
// Called when a cube has not been queried within the eviction window.
func (c *Compactor) Evict(ctx context.Context, tenant, cubeID string) error {
	files, err := c.store.List(ctx, tenant, cubeID)
	if err != nil {
		return fmt.Errorf("cube evict: list %q: %w", cubeID, err)
	}
	for _, f := range files {
		if delErr := c.store.Delete(ctx, f.Key); delErr != nil {
			return fmt.Errorf("cube evict: delete %q: %w", f.Key, delErr)
		}
	}
	return c.registry.Remove(ctx, cubeID)
}

// ShouldEvict reports whether a cube should be evicted given its last-queried timestamp.
func (c *Compactor) ShouldEvict(lastQueriedSec uint64) bool {
	threshold := uint64(time.Now().Add(-time.Duration(c.cfg.EvictionWindowDays) * 24 * time.Hour).Unix()) //nolint:gosec
	return lastQueriedSec < threshold
}
