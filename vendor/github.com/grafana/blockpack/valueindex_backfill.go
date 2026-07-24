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
	// AnchorSec, if non-zero, is the window's newest edge (replacing Now()) --
	// set by a caller resuming a chained backfill from a previously-persisted
	// watermark. Zero means "anchor to the current wall clock", preserving all
	// pre-existing callers' behavior unchanged.
	AnchorSec uint64
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
//
// #519: Done distinguishes two internally-separate signals that must not be conflated.
// windowExhausted (this run's own listing finished, i.e. this is the final progressFn
// call) is what's safe to use to advance WatermarkSec — see processBlocks' doc comment.
// trueDone (what's actually persisted here as Done) additionally requires the resolved
// window floor (minSec) to be genuinely 0 — the true beginning of time, or an unbounded
// run. Done drives ONLY job-planner's "stop chaining more backfill runs for this column"
// decision; it is never consulted by CoversRange (BackfillState.CoversRange /
// ColumnWatermark.CoversRange), which ranges-checks WatermarkSec unconditionally.
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
	maxSec := e.cfg.AnchorSec
	if maxSec == 0 {
		maxSec = uint64(e.cfg.Now().Unix()) //nolint:gosec // Unix time is non-negative for any real clock
	}
	var minSec uint64
	if maxSec > e.cfg.WindowSeconds {
		minSec = maxSec - e.cfg.WindowSeconds
	}

	if _, excluded := HardExcludedColumns[e.entry.ColumnName]; excluded {
		// #519: WatermarkSec:0 (not minSec) — "nothing will ever be backfilled for
		// this column" (R2 permanent exclusion) is vacuously "covers everything
		// from the beginning of time" under CoversRange's corrected contract, the
		// actually-intended meaning here, not an artifact of this call's window.
		// WindowStartSec/WindowEndSec are left as the resolved window purely for
		// operator-facing observability (informational only — CoversRange never
		// reads them).
		return progressFn(BackfillProgress{
			WatermarkSec:   0,
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
		// #519: this call's own listing is trivially exhausted (there was nothing
		// to process), but Done must still require minSec==0 — an empty listing
		// within a BOUNDED window (e.g. no blocks exist in [minSec,maxSec] simply
		// because nothing was ingested that hour) says nothing about whether
		// history older than minSec is covered.
		return progressFn(BackfillProgress{
			WatermarkSec:   minSec,
			WindowStartSec: minSec,
			WindowEndSec:   maxSec,
			Done:           minSec == 0,
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
// fetched and written, advances WatermarkSec to minSec. Done is reported alongside per
// #519's windowExhausted-AND-minSec==0 contract (see BackfillState.Done) — Done no longer
// affects CoversRange's answer at all, only job-planner's chaining decision. This still
// satisfies R9 ("the caller MUST actually call Registry.UpdateWatermark from
// progressFn," closing cube's own gap) since the call happens on every block — it just
// does not let intermediate calls make a coverage claim this loop cannot yet prove safe.
// See
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

		windowExhausted := i == len(refs)-1
		watermark := maxSec // unconfirmed sentinel -- see this function's own doc comment
		if windowExhausted {
			watermark = minSec
		}
		// #519: trueDone additionally requires minSec==0 -- windowExhausted alone
		// (this run's own listing finished) says nothing about whether history
		// OLDER than this run's window floor is covered. See BackfillState.Done's
		// doc comment for the full contract.
		trueDone := windowExhausted && minSec == 0

		if perr := progressFn(BackfillProgress{
			WatermarkSec:   watermark,
			WindowStartSec: minSec,
			WindowEndSec:   maxSec,
			Done:           trueDone,
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
// e.entry.ColumnType, and streams them directly into one l0Group's writer as they're
// produced, flushing/PUTing the result via flushAndPutL0 once extraction finishes.
//
// SPEC-ROOT-026: entries are streamed straight into the writer instead of being
// accumulated into an intermediate []ValueIndexEntry slice first. valueindex.Writer's
// AddEntry* already spills its own buffer to disk once it reaches
// shared.ValueIndexWriterSpillEntries -- that mechanism is unconditional and already
// existed before this fix, but accumulating a full-file slice one layer above it (the
// old FlushAndPutValueIndexColumn(entries, ...) call this replaced) meant every entry
// still had to live in memory simultaneously as a ValueIndexEntry regardless, defeating
// the writer's own bounded-memory design. There is normally exactly one ColType among
// matching entries (the filter below already narrows to e.entry.ColumnType), so a single
// group -- lazily created on the first matching entry -- replaces the old defensive
// map[ColumnType][]ValueIndexEntry grouping.
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

	var g *l0Group
	err := ExtractValueIndexEntriesForColumns(r, allowlist, func(ent ValueIndexEntry) error {
		if valueindex.ColTypeName(ent.ColType) != e.entry.ColumnType {
			return nil
		}
		if g == nil {
			g = &l0Group{
				writer:  valueindex.NewWriter(e.entry.ColumnName, ent.ColType),
				colName: e.entry.ColumnName,
				colType: ent.ColType,
			}
		}
		return addValueIndexEntryToGroup(g, ent, sourceRef)
	})
	if err != nil {
		return fmt.Errorf("blockpack: BackfillEngine.Run: extract %q: %w", sourceRef, err)
	}
	if g == nil {
		return nil // no entries matched e.entry.ColumnType -- nothing to flush
	}
	defer g.writer.Close()

	indexPrefix := e.cfg.IndexPrefix
	if indexPrefix == "" {
		indexPrefix = defaultL0IndexPrefix
	}
	if putErr := flushAndPutL0(e.cfg.Store, g, e.entry.Tenant, indexPrefix); putErr != nil {
		return fmt.Errorf("blockpack: BackfillEngine.Run: flush %q: %w", sourceRef, putErr)
	}
	return nil
}

// BackfillColumnTarget names one (ColumnName, ColumnType) pair for
// ExtractAndWriteBlockColumns' multi-column single-pass extraction. Deliberately smaller
// than Entry (no Tenant/Backfill/ColumnHash fields): ColumnHash is derived internally from
// ColumnName by valueindex.ColHash exactly like flushAndPutL0's existing single-column
// callers already do, and Tenant is supplied once for the whole call via
// ExtractAndWriteBlockColumns' own tenant parameter, since every target in one call is, by
// construction, backfilling against the SAME already-fetched block for the SAME tenant.
type BackfillColumnTarget struct {
	ColumnName string
	ColumnType string
	// CreatedAtSec feeds writeColumnMetadata's per-column metadata.json, mirroring
	// BackfillEngine.Run's own entry.CreatedAt usage exactly.
	CreatedAtSec uint64
}

// BackfillColumnResult is one target's outcome from ExtractAndWriteBlockColumns, returned
// once per target, in Targets' input order, after the whole batch's single extraction pass
// and every target's own metadata-write/flush have been attempted. Err is nil for a target
// that was hard-excluded (mirrors BackfillEngine.Run's own "immediate success, write
// nothing" contract for HardExcludedColumns) or that was written successfully; non-nil for
// a genuine per-target metadata-write or flush failure. A caller loops results in order and
// stops at the first non-nil Err exactly like the old per-column BackfillEngine.Run loop
// stopped at the first column whose Run call failed -- see
// processViBackfillPendingColumns.
type BackfillColumnResult struct {
	Err    error
	Target BackfillColumnTarget
}

// ExtractAndWriteBlockColumns performs a SINGLE extraction pass over r for every target in
// targets, writing one L0 value-index file per target through store -- the multi-column
// analog of BackfillEngine's own per-column extractAndWriteBlock, for the case where N
// columns are all pending against the SAME already-fetched/staged block
// (compactionworker's issue #533 block-shaped vi_backfill jobs, one job per real block with
// columns as vi_backfill_job_columns membership rows).
//
// This is a pprof-evidenced memory fix, not a CPU-efficiency nicety (the #533 brainstorm
// identified "multi-column single-pass extraction" but deferred it as the latter). A live
// heap profile on a compaction-worker pod showed buildSpanStartSecByRef (valueindex_extract.go)
// alone at 69% of in-use heap: it builds a map[uint32]uint64 with one entry per span in the
// WHOLE file, and BackfillEngine.Run's per-column contract meant a block with N pending
// columns rebuilt that multi-GB map from scratch N times, back-to-back, within one job --
// where extractAndWriteBlock calls ExtractValueIndexEntriesForColumns once per column, each
// call independently re-parsing every inner block. Because every target here is
// extracted via exactly ONE ExtractValueIndexEntriesForColumns call -- scoped to the
// COMBINED allowlist of every target's ColumnName -- that per-inner-block parse work
// (including buildSpanStartSecByRef) happens exactly once regardless of len(targets).
//
// Each yielded entry is dispatched to the matching target's own l0Group by (ColName,
// ColType), not ColName alone: two targets can legitimately share a name but differ in
// type (a column observed as two distinct types is two independent Entries, mirroring
// l0Group's own keying convention in valueindex_l0write.go, NOTE-VI-024).
//
// HardExcludedColumns are skipped identically to extractAndWriteBlock's own R2 guard
// (permanent exclusion regardless of what any target claims): no metadata is written and no
// group is created for an excluded target, and its BackfillColumnResult.Err is left nil
// (success, nothing written) -- mirroring Run's own "immediate Done, writing nothing"
// contract for a single-column excluded run.
//
// A metadata-write or add-entry failure for one target does not abort the batch for any
// OTHER target: this function's own per-target isolation is strictly more granular than the
// old "one BackfillEngine.Run call per column" loop ever needed to be, since every target
// here shares one already-in-flight extraction pass rather than each getting its own
// independent Run call. Only a genuine failure of the shared extraction pass itself (r's
// underlying read failing in a way that surfaces as a yield error -- see
// ExtractValueIndexEntriesForColumns' own contract) aborts the whole call, returning a
// non-nil error with no results, since that failure mode poisons every target's
// partially-built group identically.
//
// SPEC-ROOT-027.
func ExtractAndWriteBlockColumns(
	r *Reader,
	store ObjectPutter,
	sourceRef, tenant, indexPrefix string,
	targets []BackfillColumnTarget,
) ([]BackfillColumnResult, error) {
	if r == nil || store == nil || len(targets) == 0 {
		return nil, nil
	}
	if indexPrefix == "" {
		indexPrefix = defaultL0IndexPrefix
	}

	results := make([]BackfillColumnResult, len(targets))
	groups := make(map[string]*l0Group, len(targets))
	allowlist := make(map[string]struct{}, len(targets))
	targetIdx := make(map[string]int, len(targets))

	for i, t := range targets {
		results[i] = BackfillColumnResult{Target: t}
		if _, excluded := HardExcludedColumns[t.ColumnName]; excluded {
			continue
		}
		if err := writeColumnMetadata(store, tenant, indexPrefix, t.ColumnName, t.ColumnType, t.CreatedAtSec); err != nil {
			results[i].Err = fmt.Errorf(
				"blockpack: ExtractAndWriteBlockColumns: write column metadata %q: %w", t.ColumnName, err,
			)
			continue
		}
		allowlist[t.ColumnName] = struct{}{}
		targetIdx[t.ColumnName+"\x00"+t.ColumnType] = i
	}

	if len(allowlist) == 0 {
		return results, nil
	}

	err := ExtractValueIndexEntriesForColumns(r, allowlist, func(e ValueIndexEntry) error {
		key := e.ColName + "\x00" + valueindex.ColTypeName(e.ColType)
		idx, ok := targetIdx[key]
		if !ok || results[idx].Err != nil {
			return nil //nolint:nilerr // deliberate per-target isolation: a target that already failed
			// (results[idx].Err set) must not abort the SHARED extraction pass for every other
			// target, so this intentionally swallows the already-recorded failure and keeps
			// dispatching entries to every OTHER target's own group instead of stopping the whole
			// batch. Not the "swallowed real error" bug nilerr normally guards against.
		}
		g := groups[key]
		if g == nil {
			g = &l0Group{writer: valueindex.NewWriter(e.ColName, e.ColType), colName: e.ColName, colType: e.ColType}
			groups[key] = g
		}
		if addErr := addValueIndexEntryToGroup(g, e, sourceRef); addErr != nil {
			results[idx].Err = fmt.Errorf(
				"blockpack: ExtractAndWriteBlockColumns: add entry %q: %w", e.ColName, addErr,
			)
		}
		return nil
	})
	if err != nil {
		for _, g := range groups {
			g.writer.Close()
		}
		return nil, fmt.Errorf("blockpack: ExtractAndWriteBlockColumns: extract %q: %w", sourceRef, err)
	}

	for key, g := range groups {
		idx := targetIdx[key]
		if results[idx].Err == nil {
			if putErr := flushAndPutL0(store, g, tenant, indexPrefix); putErr != nil {
				results[idx].Err = fmt.Errorf(
					"blockpack: ExtractAndWriteBlockColumns: flush %q: %w", sourceRef, putErr,
				)
			}
		}
		g.writer.Close()
	}

	return results, nil
}
