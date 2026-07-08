package cube

// NOTE: SPEC-CUBE-018 — Compactor merges many small L0 flush files for one cube into
// larger L0 files and produces L1/L2 rollup files. It also evicts cubes that have not
// been queried within the eviction window. No per-span S3 reads — only cube files.

import (
	"context"
	"encoding/hex"
	"fmt"
	"time"
)

// CompactorConfig parameterises the compactor.
type CompactorConfig struct {
	// L0MergeThreshold triggers an L0 merge when ≥ this many L0 files cover the same hour.
	L0MergeThreshold int
	// L1MergeThreshold triggers a day rollup when ≥ this many L1 files cover the same day.
	L1MergeThreshold int
	// EvictionWindowDays cubes not queried for this many days are evicted (default 30).
	EvictionWindowDays int
	// L0RetentionMinutes is how long an L0 file must survive after being rolled into L1 before
	// EvictAgedL0 may delete it (default 180) — long enough to comfortably serve "the last few
	// hours" at minute resolution, short enough not to let L0 accumulate indefinitely (ruling
	// 4(a), E-12a). Retention is L0-specific; it never extends to L1 files.
	L0RetentionMinutes uint32
}

func (c *CompactorConfig) setDefaults() {
	if c.L0MergeThreshold == 0 {
		c.L0MergeThreshold = 60
	}
	if c.L1MergeThreshold == 0 {
		c.L1MergeThreshold = 24
	}
	if c.EvictionWindowDays == 0 {
		c.EvictionWindowDays = 30
	}
	if c.L0RetentionMinutes == 0 {
		c.L0RetentionMinutes = 180
	}
}

// FileInfo describes one cube file discovered in object storage.
type FileInfo struct {
	Key       string // S3 object key
	MinMinute uint32
	MaxMinute uint32
	Level     uint32 // 1=L0, 60=L1, 1440=L2
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

// CompactionPlan describes one planned merge: which input keys to read and what
// output key to write.
type CompactionPlan struct {
	OutputKey string
	InputKeys []string
	Level     uint32 // output resolution (1, 60, or 1440)
	MinMinute uint32
	MaxMinute uint32
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

// PlanL0Merge returns compaction plans for L0 files that cover overlapping time windows.
// It groups L0 files by their hour bucket and emits a plan for each hour that has ≥
// L0MergeThreshold files. A file whose OWN span already straddles an hour boundary (only
// possible for a previously-merged L0 output — a fresh per-minute flush always has
// MinMinute == MaxMinute) is excluded from grouping entirely (#491 Phase E fix pass, review.md
// Issue 6): including it would produce (or perpetuate) a merged output that itself straddles the
// boundary, which PlanL1Rollup's strict per-hour containment check can then never select for
// EITHER hour it touches, stranding that hour at L0 forever with no operator-visible signal. A
// straddling file is left un-merged (still individually queryable at minute resolution — no data
// loss) rather than merged incorrectly. Because every file placed into a group is now fully
// contained within one hour, the group's computed MinMinute/MaxMinute (and therefore any merged
// output this function produces) is ALSO always fully contained within that hour — this is
// self-correcting: once applied, no future merge output can ever straddle an hour boundary again.
func PlanL0Merge(files []FileInfo, threshold int, tenant, cubeID string) []CompactionPlan {
	// Group by hour bucket (minute / 60) of MinMinute; a file whose MinMinute and MaxMinute fall
	// in different hour buckets is excluded (see doc comment above).
	byHour := make(map[uint32][]FileInfo)
	for _, f := range files {
		if f.Level != uint32(RollupL0) {
			continue
		}
		hourMin := f.MinMinute / 60
		hourMax := f.MaxMinute / 60
		if hourMin != hourMax {
			continue
		}
		byHour[hourMin] = append(byHour[hourMin], f)
	}

	var plans []CompactionPlan
	for _, group := range byHour {
		if len(group) < threshold {
			continue
		}
		var minM, maxM uint32 = ^uint32(0), 0
		var keys []string
		for _, f := range group {
			keys = append(keys, f.Key)
			if f.MinMinute < minM {
				minM = f.MinMinute
			}
			if f.MaxMinute > maxM {
				maxM = f.MaxMinute
			}
		}
		outKey := fmt.Sprintf("%s/cubes/%s/L0-%d-%d-%s.cube", tenant, cubeID, minM, maxM, newXID())
		plans = append(plans, CompactionPlan{
			InputKeys: keys,
			OutputKey: outKey,
			Level:     uint32(RollupL0),
			MinMinute: minM,
			MaxMinute: maxM,
		})
	}
	return plans
}

// PlanL1Rollup returns a plan to roll up all L0 files for one hour into a single L1 file.
// It returns (nil, false) when fewer than threshold L0 files cover the hour.
func PlanL1Rollup(files []FileInfo, hourStart uint32, tenant, cubeID string) ([]string, bool) {
	var keys []string
	for _, f := range files {
		if f.Level != uint32(RollupL0) {
			continue
		}
		if f.MinMinute >= hourStart && f.MaxMinute < hourStart+60 {
			keys = append(keys, f.Key)
		}
	}
	if len(keys) == 0 {
		return nil, false
	}
	return keys, true
}

// Execute runs one compaction plan: reads input files, merges, writes output, updates the
// cube's watermark for plan.Level, then deletes inputs — EXCEPT for an L0->L1 rollup
// (plan.Level == RollupL1), whose L0 inputs are retention-decoupled (ruling 4(a), E-12a): they
// survive until EvictAgedL0 confirms they are both past L0RetentionMinutes and already reflected
// in the L1 watermark just written above, so recent data stays queryable at minute resolution
// even after being rolled into L1. A pure L0 merge/consolidation (plan.Level == RollupL0, no
// resolution change — the merged output already serves identical minute-resolution needs) and an
// L1->L2 rollup (plan.Level == RollupL2, retention is L0-specific and never extends to L1) both
// delete their inputs immediately, unchanged from pre-#491 behavior.
func (c *Compactor) Execute(ctx context.Context, cubeID [16]byte, plan CompactionPlan) error {
	// Open all input readers.
	inputs := make([]RollupInput, 0, len(plan.InputKeys))
	for _, key := range plan.InputKeys {
		r, err := c.store.Get(ctx, key)
		if err != nil {
			return fmt.Errorf("cube compactor: get %q: %w", key, err)
		}
		inputs = append(inputs, NewRollupInput(r))
	}

	// Build merged Writer.
	w, err := RollupToWriter(cubeID, plan.Level, inputs, plan.MinMinute, plan.MaxMinute)
	if err != nil {
		return fmt.Errorf("cube compactor: rollup: %w", err)
	}
	if w.CellCount() == 0 {
		// No cells after merge — delete inputs and skip writing.
		return c.deleteInputs(ctx, plan.InputKeys)
	}

	data, err := w.Encode()
	if err != nil {
		return fmt.Errorf("cube compactor: encode: %w", err)
	}
	if err := c.store.Put(plan.OutputKey, data); err != nil {
		return fmt.Errorf("cube compactor: put %q: %w", plan.OutputKey, err)
	}

	hexCubeID := hex.EncodeToString(cubeID[:8])
	if err := c.registry.UpdateWatermarks(ctx, hexCubeID, plan.Level, plan.MinMinute, plan.MaxMinute); err != nil {
		return fmt.Errorf("cube compactor: update watermarks: %w", err)
	}

	if plan.Level == uint32(RollupL1) {
		return nil // retention-decoupled: EvictAgedL0 deletes these L0 inputs later
	}
	return c.deleteInputs(ctx, plan.InputKeys)
}

// CellCount returns the number of distinct cells accumulated in the Writer.
// Exposed for compaction decision-making.
func (w *Writer) CellCount() int { return len(w.cells) }

func (c *Compactor) deleteInputs(ctx context.Context, keys []string) error {
	for _, key := range keys {
		if err := c.store.Delete(ctx, key); err != nil {
			return fmt.Errorf("cube compactor: delete %q: %w", key, err)
		}
	}
	return nil
}

// EvictAgedL0 deletes an L0 file only when it is BOTH older than L0RetentionMinutes AND already
// covered by the cube's L1 watermark (i.e. its data has been safely rolled up into L1) — the
// retention-decoupled deletion ruling 4(a) requires. nowMinute is caller-supplied (rather than
// computed internally) for deterministic testing. A non-L0 file is a caller error, not a no-op —
// this function's contract is specifically about L0 retention, and a bug feeding it non-L0 files
// should be visible, not silently ignored.
func (c *Compactor) EvictAgedL0(ctx context.Context, cubeID string, file FileInfo, nowMinute uint32) error {
	if file.Level != uint32(RollupL0) {
		return fmt.Errorf("cube compactor: EvictAgedL0 called with non-L0 file (level=%d)", file.Level)
	}
	if nowMinute < file.MaxMinute || nowMinute-file.MaxMinute < c.cfg.L0RetentionMinutes {
		return nil // within the retention window — never evict
	}

	entries, _, err := c.registry.Load(ctx)
	if err != nil {
		return fmt.Errorf("cube compactor: EvictAgedL0 load registry: %w", err)
	}
	var entry *RegistryEntry
	for i := range entries {
		if entries[i].CubeID == cubeID {
			entry = &entries[i]
			break
		}
	}
	if entry == nil {
		return fmt.Errorf("cube compactor: EvictAgedL0: cube %q not found in registry", cubeID)
	}

	wm, ok := entry.Watermarks[uint32(RollupL1)]
	if !ok || wm.MinMinute > file.MinMinute || wm.MaxMinute < file.MaxMinute {
		return nil // not yet (fully) rolled up into L1 — do not evict
	}

	return c.store.Delete(ctx, file.Key)
}

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

// newXID returns a short unique ID string for object keys.
func newXID() string {
	// Filename produces "t/cubes/<32-hex>/L0-<20-char-xid>.cube".
	// Extract the 20-char xid from the end, before ".cube".
	f := Filename("t", [16]byte{})
	// f ends with "/L0-<xid>.cube", xid is 20 chars before ".cube"
	return f[len(f)-len(".cube")-20 : len(f)-len(".cube")]
}
