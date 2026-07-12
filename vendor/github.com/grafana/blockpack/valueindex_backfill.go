package blockpack

// valueindex_backfill.go -- #496's BackfillEngine (originally task #107/A4,
// internal/modules/viusage/backfill.go). Moved here from viusage (2026-07-10) because
// it imports this root package directly (*Reader/ObjectPutter/
// ExtractValueIndexEntriesForColumns/FlushAndPutValueIndexColumn) -- exactly the same
// reason ColumnPolicy (valueindex_policy.go) already lives here rather than in
// viusage. viusage must remain a LEAF package with respect to this one (R1: "viusage ->
// blockpack, never blockpack -> viusage") so that valueindex_usage.go can re-export
// viusage's other types (Entry/Registry/Config/TriggerConfig/etc.) back out to external
// consumers (tempo's Part B) without creating an import cycle: as long as ANY file in
// viusage imported this package, this package could never import viusage at all, for
// ANYTHING -- Go import cycles are per-package, not per-file.
//
// NOTE: SPEC-VIUSAGE-002 -- BackfillEngine reads raw historical blocks (R6: no
// pre-extracted VI data exists for a never-indexed column) newest-to-oldest across one
// column's configured backfill window, extracting only that column via
// ExtractValueIndexEntriesForColumns and writing standard L0 value index files through
// FlushAndPutValueIndexColumn -- the same file-key convention WriteValueIndexL0 uses, so
// valueindexcompactor's normal column-scoped lap discovers them with zero
// special-casing. Persisting progress (BackfillState.WatermarkSec/Done) to the registry
// is the CALLER's job (R7/R9) -- this engine only reports it via progressFn.

import (
	"context"
	"fmt"
	"time"

	"github.com/grafana/blockpack/internal/modules/valueindex"
)

// defaultBackfillWindowSeconds is R4's documented, unmeasured starting default (48h)
// for a triggered column's historical backfill window -- narrower than cube's 7-day
// backfill window because VI backfill reads raw historical blocks (full block I/O per
// this repo's core I/O invariant), not cheap pre-extracted VI files like cube's
// backfill does.
const defaultBackfillWindowSeconds = 48 * 3600

// defaultBackfillWorkers mirrors cube's tempo-side override default.
const defaultBackfillWorkers = 4

// BlockFetcher abstracts raw historical block reads for the backfill engine (R6: this
// engine reads BLOCKS, not pre-extracted VI files -- no pre-extracted data exists for a
// never-indexed column). A real implementation lists tenant blocks overlapping a time
// window and opens each via the existing single-I/O-per-block read path; a test injects
// an in-memory fake.
type BlockFetcher interface {
	// ListBlocksInRange returns the source-ref keys of every block for tenant that may
	// overlap [minSec, maxSec], newest-first (so BackfillEngine.Run can process
	// newest-to-oldest, mirroring cube's fill direction for the same "recent data
	// usable soonest" reason). The fetcher, not the engine, is responsible for the
	// ordering guarantee.
	ListBlocksInRange(ctx context.Context, tenant string, minSec, maxSec uint64) ([]string, error)
	// FetchBlock opens sourceRef as a *Reader via the existing single-I/O block-fetch
	// path (this repo's core I/O invariant: one full read per block, never
	// per-column).
	FetchBlock(ctx context.Context, sourceRef string) (*Reader, error)
}

// BackfillConfig parameterises one column's backfill run.
type BackfillConfig struct {
	Store   ObjectPutter // reuses the EXISTING WriteValueIndexL0 sink interface
	Fetcher BlockFetcher
	// Now returns the current time; defaults to time.Now when nil. Injected so tests
	// get a deterministic, fixed backfill window.
	Now           func() time.Time
	IndexPrefix   string
	WindowSeconds uint64 // default 48h (R4)
	Workers       int    // default 4, mirrors cube's tempo-side override
}

// BackfillEngine runs one column's historical backfill.
type BackfillEngine struct {
	cfg   BackfillConfig
	entry Entry
}

// NewBackfillEngine constructs a BackfillEngine for entry, applying R4's documented
// defaults for any zero-valued BackfillConfig field.
func NewBackfillEngine(entry Entry, cfg BackfillConfig) *BackfillEngine {
	if cfg.WindowSeconds == 0 {
		cfg.WindowSeconds = defaultBackfillWindowSeconds
	}
	if cfg.Workers <= 0 {
		cfg.Workers = defaultBackfillWorkers
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	return &BackfillEngine{entry: entry, cfg: cfg}
}

// BackfillProgress reports one unit of completed backfill work (one fetched block, or
// the trivial empty-range case).
//
// WindowStartSec/WindowEndSec are a deliberate, additive deviation from plan.md Section
// 4.3's literal {WatermarkSec, Done, LastError} shape: Entry.BackfillState.
// WindowStartSec/WindowEndSec (plan.md Section 4.1) must be set by someone at
// trigger/launch time, and neither A2's registry nor A3's trigger function sets them.
// Reporting the resolved window here lets the caller (tempo's B2 launcher, which
// constructs this BackfillEngine and owns persistence per plan.md's Part B) persist the
// actual window alongside the watermark on its very first progressFn call, instead of
// duplicating this engine's window-resolution logic in a second repo.
type BackfillProgress struct {
	LastError      error
	WatermarkSec   uint64
	WindowStartSec uint64
	WindowEndSec   uint64
	Done           bool
}

// Run processes historical blocks for e's (tenant, column) newest-to-oldest across
// [now-WindowSeconds, now], calling progressFn once per fetched block so the caller can
// persist BackfillState.WatermarkSec via the registry's conditional-PUT retry loop (R7
// -- persistence is the CALLER's job here, exactly like cube's Backfiller.Run, but see
// plan.md Section 1: unlike cube's tempo-side caller, VI's tempo-side caller MUST
// actually call Registry.UpdateWatermark from progressFn).
//
// For each fetched block: extract via ExtractValueIndexEntriesForColumns scoped to
// exactly e.entry.ColumnName, filtered further to entries matching e.entry.ColumnType (a
// column observed as a DIFFERENT type elsewhere in history belongs to a distinct
// Entry/key -- (Tenant, ColumnHash, ColumnType) -- and must not be folded into this
// run), then PUT through FlushAndPutValueIndexColumn's existing L0 file layout so
// valueindexcompactor's normal column-scoped lap picks up the new files with zero
// special-casing (R6).
//
// ctx cancellation is checked before fetching each block; Run returns ctx.Err() without
// processing further blocks.
//
// R2 defense-in-depth (go-presubmit.md/review.md HIGH finding): refuses outright to
// backfill any of HardExcludedColumns, regardless of what the caller's Entry claims.
// Today, nothing in the call graph actually constructs a Triggered registry Entry for one
// of those four names -- but that is an emergent property of the current callers, not an
// invariant this engine itself enforces, and R2 requires PERMANENT exclusion "regardless
// of... usage-triggered backfill status." Reports Done immediately (writing nothing) so a
// caller doesn't keep re-attempting a backfill that will never do anything.
func (e *BackfillEngine) Run(ctx context.Context, progressFn func(BackfillProgress) error) error {
	maxSec := uint64(e.cfg.Now().Unix()) //nolint:gosec // Unix time is non-negative for any real clock
	var minSec uint64
	if maxSec > e.cfg.WindowSeconds {
		minSec = maxSec - e.cfg.WindowSeconds
	}

	if _, excluded := HardExcludedColumns[e.entry.ColumnName]; excluded {
		return progressFn(BackfillProgress{
			WatermarkSec:   minSec,
			WindowStartSec: minSec,
			WindowEndSec:   maxSec,
			Done:           true,
		})
	}

	// Small operator-facing addition (2026-07-11): a <colHash>/metadata.json naming
	// this column and when its Entry was first registered, so "are we seeing these
	// columns come through?" is answerable by reading one small object per column,
	// instead of log archeology or the viusage registry's own less-discoverable path.
	if err := writeColumnMetadata(
		e.cfg.Store, e.entry.Tenant, e.cfg.IndexPrefix, e.entry.ColumnName, e.entry.ColumnType, e.entry.CreatedAt,
	); err != nil {
		return fmt.Errorf("blockpack: BackfillEngine.Run: write column metadata: %w", err)
	}

	refs, err := e.cfg.Fetcher.ListBlocksInRange(ctx, e.entry.Tenant, minSec, maxSec)
	if err != nil {
		return fmt.Errorf("blockpack: BackfillEngine.Run: list blocks: %w", err)
	}

	if len(refs) == 0 {
		return progressFn(BackfillProgress{
			WatermarkSec:   minSec,
			WindowStartSec: minSec,
			WindowEndSec:   maxSec,
			Done:           true,
		})
	}

	return e.processBlocks(ctx, refs, minSec, maxSec, progressFn)
}

// processBlocks fetches and writes each block in refs (already ordered newest-first by
// the caller's BlockFetcher), reporting progress after each.
//
// R7 safety (go-presubmit.md HIGH finding): refs is sorted newest-to-oldest by the
// fetcher's NOMINAL block time (e.g. BlockMeta.StartTime) — not by each block's REAL
// per-span content range. Late-arriving data, clock skew, or multi-writer flush jitter
// can make a not-yet-processed (nominally "older") block contain real span data newer
// than a range an EARLIER progressFn call already claimed as covered. Concretely: the
// previous implementation computed each fetched block's real per-span content range and
// reported a shrinking "running min of blocks processed so far" after EVERY block; a
// query issued between two progressFn calls could observe an intermediate WatermarkSec
// claiming coverage of a range some not-yet-processed block still has real, unwritten
// data in — the exact "false complete answer assembled from partial data" bug R7 exists
// to prevent, just manifesting mid-run instead of only in the final persisted state.
//
// Fix: NO progressFn call before the run's last block may advance WatermarkSec below
// maxSec (the window's own newest edge) — this is the safe, correctly-declining "nothing
// beyond the window's boundary is confirmed yet" sentinel (CoversRange requires
// minSec >= WatermarkSec, which a WatermarkSec of maxSec never satisfies for any real
// historical query). Only the FINAL call, once every listed block has genuinely been
// fetched and written, advances WatermarkSec to minSec (Done=true, which independently
// makes CoversRange always return true regardless of WatermarkSec's exact value — see
// BackfillState.CoversRange). This still satisfies R9 ("the caller MUST actually call
// Registry.UpdateWatermark from progressFn," closing cube's own gap) since the call
// happens on every block — it just does not let intermediate calls make a coverage
// claim this loop cannot yet prove safe. See
// TestBackfillEngine_OverlappingOutOfOrderBlocksNeverOverstatesCoverage for the
// adversarial regression pin, and NOTE-VIUSAGE-11 for the full design rationale
// (including why extending BlockFetcher with per-block nominal bounds still would not
// close the gap without also trusting that metadata, the exact thing under question).
func (e *BackfillEngine) processBlocks(
	ctx context.Context,
	refs []string,
	minSec, maxSec uint64,
	progressFn func(BackfillProgress) error,
) error {
	allowlist := map[string]struct{}{e.entry.ColumnName: {}}

	for i, ref := range refs {
		if err := ctx.Err(); err != nil {
			return err
		}

		r, ferr := e.cfg.Fetcher.FetchBlock(ctx, ref)
		if ferr != nil {
			return fmt.Errorf("blockpack: BackfillEngine.Run: fetch block %q: %w", ref, ferr)
		}

		if werr := e.extractAndWriteBlock(r, ref, allowlist); werr != nil {
			return werr
		}

		done := i == len(refs)-1
		watermark := maxSec // unconfirmed sentinel -- see this function's own doc comment
		if done {
			watermark = minSec
		}

		if perr := progressFn(BackfillProgress{
			WatermarkSec:   watermark,
			WindowStartSec: minSec,
			WindowEndSec:   maxSec,
			Done:           done,
		}); perr != nil {
			return perr
		}

		if err := ctx.Err(); err != nil {
			return err
		}
	}
	return nil
}

// extractAndWriteBlock extracts e.entry's target column from r (allowlist already
// scoped to e.entry.ColumnName), keeps only entries whose extracted type matches
// e.entry.ColumnType, groups by ColType (defensive -- normally exactly one type
// survives the filter), and flushes/PUTs each group via FlushAndPutValueIndexColumn.
//
// R2 defense-in-depth: repeats Run's own HardExcludedColumns guard here too, at the
// point that actually performs I/O -- Run's guard alone would be bypassed by any future
// refactor that constructs a BackfillEngine and calls a lower-level method directly, and
// this is genuinely the LAST point before a hard-excluded column would ever be written.
func (e *BackfillEngine) extractAndWriteBlock(
	r *Reader,
	sourceRef string,
	allowlist map[string]struct{},
) error {
	if _, excluded := HardExcludedColumns[e.entry.ColumnName]; excluded {
		return nil
	}
	byType := map[ColumnType][]ValueIndexEntry{}
	err := ExtractValueIndexEntriesForColumns(r, allowlist, func(ent ValueIndexEntry) error {
		if valueindex.ColTypeName(ent.ColType) != e.entry.ColumnType {
			return nil
		}
		byType[ent.ColType] = append(byType[ent.ColType], ent)
		return nil
	})
	if err != nil {
		return fmt.Errorf("blockpack: BackfillEngine.Run: extract %q: %w", sourceRef, err)
	}

	for colType, entries := range byType {
		putErr := FlushAndPutValueIndexColumn(
			entries, e.cfg.Store, sourceRef, e.entry.Tenant, e.cfg.IndexPrefix, e.entry.ColumnName, colType,
		)
		if putErr != nil {
			return fmt.Errorf("blockpack: BackfillEngine.Run: flush %q: %w", sourceRef, putErr)
		}
	}
	return nil
}
