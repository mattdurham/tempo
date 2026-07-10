package blockpack

// valueindex_l0write.go — synchronous in-process L0 value-index write path
// (NOTE-VI-042, issue #464).
//
// The value-index pipeline was originally event-driven: a publisher emitted a
// "create" event per block (NOTE-VI-015 #397), a Redis-Streams consumer ingested
// those events and flushed time-windowed L0 files (NOTE-VI-016 #398). That path
// requires a running Redis; when no Redis is configured the publisher is a Noop
// and NO index files are ever written.
//
// WriteValueIndexL0 collapses extract → accumulate → flush → put into a single
// synchronous call so the block-builder and compactor can index a block in-process
// the moment it is written, with no broker. It reuses the exact extraction
// (ExtractValueIndexEntries, NOTE-VI-018) and per-column writer (valueindex.Writer)
// the consumer uses, so the on-disk file format and object-key layout are
// byte-identical to the Redis path's output. The querier read path is unchanged.

import (
	"context"
	"fmt"
	"path"

	modules_shared "github.com/grafana/blockpack/internal/modules/blockio/shared"
	"github.com/grafana/blockpack/internal/modules/valueindex"
)

// defaultL0IndexPrefix is the object-storage key prefix under which value-index
// files are written when the caller passes an empty prefix. It must match the
// consumer/compactor default ("indexes") so the synchronous and event-driven
// write paths land in the same key space and the querier discovers both.
const defaultL0IndexPrefix = "indexes"

// ObjectPutter is the minimal object-storage write surface WriteValueIndexL0
// needs: write a fully-formed value-index file to a key. blockpack.WritableStorage
// satisfies it; tempo supplies an S3-backed implementation. It mirrors the
// valueindexconsumer.ObjectPutter contract so the same store can serve both the
// synchronous and event-driven write paths.
type ObjectPutter interface {
	Put(key string, data []byte) error
}

// l0Group accumulates entries for one (column-name, type) pair into a single
// value-index writer. Keying by (name, type) — not name alone — keeps columns
// that share a name but differ in type under distinct files: their value
// encodings are not interchangeable (NOTE-VI-024, issue #409).
type l0Group struct {
	writer  valueindex.Writer
	colName string
	colType ColumnType
}

// l0TraceAccum accumulates one trace's SpanEntry rows during a single
// WriteValueIndexL0 pass. Unlike valueindexconsumer's traceGroupBuffer (which
// spills to disk across many async messages within a flush window),
// WriteValueIndexL0 processes exactly one reader synchronously, so an
// in-memory map is sufficient -- no cross-call buffering is needed.
type l0TraceAccum struct {
	spans   []valueindex.SpanEntry
	timeSec uint64
	seen    bool
}

// WriteValueIndexL0 extracts every indexable (column, span) observation from r,
// groups the observations by (column name, column type), and writes one L0 value
// index file per group to store under:
//
//	<tenant>/<indexPrefix>/<colHash>/<typeName>/L0-<wallMinSec>-<wallMaxSec>-<id>.blockpack
//
// sourceRef is the backend object key of the source block (e.g.
// "<tenant>/<block-id>/data.blockpack"); it is stamped on every entry so the
// querier can open the source block from an index hit. The object-key layout and
// file format are identical to the Redis-consumer write path (NOTE-VI-016), so
// the querier read path needs no change.
//
// A nil reader or a block with no indexable columns writes nothing and returns
// nil. Extraction or write errors are returned; the caller (block creation /
// compaction) treats a value-index write failure as best-effort and must not fail
// the block write on it — the index can always be rebuilt from the source block.
//
// policy (#496, plan.md Section 4.6) gates which columns become their own
// standalone per-column L0 file: a disabled (zero-value) policy indexes every
// column, reproducing pre-#496 behavior bit-for-bit. Extraction itself always
// runs unfiltered (see the ExtractValueIndexEntries call below) — policy is
// applied in this function's own yield callback, AFTER the span:id
// trace-group accumulation, so the trace-by-id index (which depends on
// receiving span:id's own loop-yielded entries) is never affected by the
// caller's column policy, mirroring valueindexconsumer's identical
// "unconditional trace-by-id buffering, regardless of the configured column
// allowlist" pattern (service.go).
func WriteValueIndexL0(r *Reader, store ObjectPutter, sourceRef, tenant, indexPrefix string, policy ColumnPolicy) error {
	if r == nil || store == nil {
		return nil
	}
	if indexPrefix == "" {
		indexPrefix = defaultL0IndexPrefix
	}

	// One writer per (column name, type). The map is keyed by name+type so a
	// column observed as two distinct types lands in two separate files.
	groups := make(map[string]*l0Group)
	defer func() {
		for _, g := range groups {
			g.writer.Close()
		}
	}()

	// Trace-by-ID index accumulation (issue #468 data-production gap): this is the
	// only write path that runs against live production data today
	// (valueindexconsumer, which builds the same TraceGroup index asynchronously
	// via traceflush.go, is not currently deployed) -- without this, GetTraceByID's
	// index-hit path (wired per #468) would have no data to ever find, silently
	// falling back to a full scan on every lookup regardless of the wiring being
	// correct. Mirrors valueindexconsumer's bufferTraceRow/flushTraceGroups exactly,
	// collapsed into a single in-memory pass since WriteValueIndexL0 processes
	// exactly one block synchronously -- no cross-call buffering is needed.
	traceGroups := make(map[[16]byte]*l0TraceAccum)

	// Extraction always runs with a disabled (zero-value) policy (NOTE-VI-027,
	// issue #414): span:id/parent_id/trace:id/start must reach this callback
	// unfiltered for the structural stamping and trace-group accumulation below
	// to work. The caller's real column policy (#496) is applied further down,
	// after those structural special cases, gating only whether a column becomes
	// its own standalone L0 file.
	err := ExtractValueIndexEntries(r, ColumnPolicy{}, func(e ValueIndexEntry) error {
		// Trace-by-ID index: triggered on the span:id sentinel column, independent of
		// the configured column allowlist -- trace-by-id coverage must not depend on
		// which attribute columns an operator chose to index. Mirrors
		// valueindexconsumer's ingest() exactly: accumulate for the trace index AND
		// continue to standard per-column indexing below (span:id is not excluded
		// like trace:id is).
		if e.ColName == modules_shared.SpanIDColumnName {
			ta := traceGroups[e.TraceID]
			if ta == nil {
				ta = &l0TraceAccum{}
				traceGroups[e.TraceID] = ta
			}
			if !ta.seen || e.TimeSec < ta.timeSec {
				ta.timeSec = e.TimeSec
			}
			ta.seen = true
			rowIdx := uint16(e.RowIdx) //nolint:gosec // RowIdx bounded by MaxBlockSpans <= 65534
			ta.spans = append(ta.spans, valueindex.SpanEntry{
				SourceRef:    sourceRef,
				SpanID:       e.SpanID,
				ParentSpanID: e.ParentSpanID,
				BlockRef:     e.BlockRef,
				RowIdx:       rowIdx,
			})
		}
		// trace:id is excluded from the standard per-column value-index path
		// (SPEC-VI-4/NOTE-VI-068, mirrors the same exclusion in
		// valueindexconsumer's ingest()): both this path and the dedicated
		// TraceGroup index key their L0 files under the identical
		// colHash("trace:id")/uuid/ directory, which the compactor's
		// format-dispatch (Finding 2) treats as 100% TraceGroup format. A
		// genuine BucketGroup-format trace:id file landing there would be
		// silently misrouted into mergeTraceLevel, fail to decode, and be
		// stuck at L0 forever. No code anywhere queries trace:id via the
		// standard value-index scan, so this exclusion has no user-visible cost.
		if e.ColName == modules_shared.TraceIDColumnName {
			return nil
		}
		// SPEC-VI-11, #496 R2/4.6: the caller's column policy gates every OTHER
		// column's eligibility for a standalone L0 file (dedicated-list /
		// usage-triggered allowlist, plus HardExcludedColumns permanently
		// excluding span:id/span:parent_id/span:start regardless of policy.Allow
		// membership). A disabled (zero-value) policy indexes everything, matching
		// this function's pre-#496 behavior bit-for-bit (R12 safety valve).
		if !policy.Allowed(e.ColName) {
			return nil
		}
		typeName := valueindex.ColTypeName(e.ColType)
		if typeName == "" {
			// Unindexable type (NOTE-VI-024): skip rather than bucket under an
			// empty path segment.
			return nil
		}
		key := e.ColName + "\x00" + typeName
		g := groups[key]
		if g == nil {
			g = &l0Group{
				writer:  valueindex.NewWriter(e.ColName, e.ColType),
				colName: e.ColName,
				colType: e.ColType,
			}
			groups[key] = g
		}
		return addValueIndexEntryToGroup(g, e, sourceRef)
	})
	if err != nil {
		return fmt.Errorf("blockpack: WriteValueIndexL0: extract: %w", err)
	}

	for _, g := range groups {
		if err := flushAndPutL0(store, g, tenant, indexPrefix); err != nil {
			return err
		}
	}

	return flushAndPutTraceGroups(store, traceGroups, tenant, indexPrefix)
}

// addValueIndexEntryToGroup adds one extracted entry to g's writer, choosing
// AddEntry/AddEntryV2/AddEntryV4 by which identity fields e carries. TraceID
// is threaded through in every case (Stage 5, traceindex.go wiring plan):
// bucket stream-compaction merge/dedup keys SpanRefs by TraceID
// (bucketmerge.go/stream_compaction.go), so a hardcoded zero would make every
// distinct trace observed at the same (SourceRef, BlockRef) collide under one
// shared key and silently lose spans on the first compaction. Shared by
// WriteValueIndexL0's streaming yield callback and FlushAndPutValueIndexColumn
// (#496's backfill engine helper) so the identity-dispatch logic has exactly
// one implementation.
func addValueIndexEntryToGroup(g *l0Group, e ValueIndexEntry, sourceRef string) error {
	switch {
	case e.BlockRef.PageNum > 0 || e.BlockRef.LenPages > 0:
		if e.SpanID != ([8]byte{}) {
			rowIdx := uint16(e.RowIdx) //nolint:gosec // RowIdx bounded by MaxBlockSpans ≤ 65534
			return g.writer.AddEntryV4(e.Value, e.TraceID, sourceRef, e.BlockRef, e.TimeSec, e.SpanID, rowIdx)
		}
		return g.writer.AddEntryV2(e.Value, e.TraceID, sourceRef, e.BlockRef, e.TimeSec)
	default:
		return g.writer.AddEntry(e.Value, e.TraceID, sourceRef, e.BlockID, e.TimeSec)
	}
}

// FlushAndPutValueIndexColumn builds one value-index L0 file from entries --
// all observed in a single source block, and therefore sharing one sourceRef
// -- for one (colName, colType) pair, and PUTs it through the exact same
// key convention flushAndPutL0 uses:
//
//	<tenant>/<indexPrefix>/<colHash>/<typeName>/L0-<wallMinSec>-<wallMaxSec>-<id>.blockpack
//
// Exported for #496's backfill engine (root's own BackfillEngine,
// valueindex_backfill.go -- relocated from internal/modules/viusage per
// NOTE-VIUSAGE-7's addendum since tempo cannot import an internal/ package;
// plan.md Section 4.3), which extracts entries for exactly one triggered
// column via ExtractValueIndexEntriesForColumns and needs this package's
// unexported flushAndPutL0/l0Group file-key-format logic without duplicating
// it -- a drift here would make valueindexcompactor's column-scoped discovery
// silently miss the backfilled files. A caller processing multiple blocks
// (BackfillEngine.Run's newest-to-oldest walk) calls this once per block, each
// with that block's own sourceRef.
func FlushAndPutValueIndexColumn(
	entries []ValueIndexEntry,
	store ObjectPutter,
	sourceRef, tenant, indexPrefix, colName string,
	colType ColumnType,
) error {
	if len(entries) == 0 || store == nil {
		return nil
	}
	if indexPrefix == "" {
		indexPrefix = defaultL0IndexPrefix
	}

	g := &l0Group{writer: valueindex.NewWriter(colName, colType), colName: colName, colType: colType}
	defer g.writer.Close()

	for _, e := range entries {
		if err := addValueIndexEntryToGroup(g, e, sourceRef); err != nil {
			return fmt.Errorf("blockpack: FlushAndPutValueIndexColumn: add entry: %w", err)
		}
	}
	return flushAndPutL0(store, g, tenant, indexPrefix)
}

// flushAndPutTraceGroups encodes the trace groups accumulated during one
// WriteValueIndexL0 pass and PUTs them as a single TraceGroup-format L0 file
// under the trace:id column directory, mirroring valueindexconsumer's
// flushTraceGroups. A no-op when accum is empty (no span:id-sentinel rows
// observed -- e.g. a reader with no spans).
func flushAndPutTraceGroups(
	store ObjectPutter,
	accum map[[16]byte]*l0TraceAccum,
	tenant, indexPrefix string,
) error {
	if len(accum) == 0 {
		return nil
	}

	groups := make([]valueindex.TraceGroup, 0, len(accum))
	var wallMinSec, wallMaxSec uint64
	first := true
	for tid, ta := range accum {
		groups = append(groups, valueindex.TraceGroup{
			TraceID: tid,
			TimeSec: ta.timeSec,
			Spans:   ta.spans,
		})
		if first || ta.timeSec < wallMinSec {
			wallMinSec = ta.timeSec
		}
		if first || ta.timeSec > wallMaxSec {
			wallMaxSec = ta.timeSec
		}
		first = false
	}

	data, err := valueindex.EncodeTraceGroups(groups)
	if err != nil {
		// Mirrors flushTraceGroups: string-table overflow within one block is an
		// accepted, documented low-probability edge case for v1, surfaced as a hard
		// error rather than silently dropping trace-by-ID coverage for this block.
		return fmt.Errorf("blockpack: WriteValueIndexL0: encode trace groups: %w", err)
	}

	key := path.Join(
		tenant,
		indexPrefix,
		valueindex.ColHash(modules_shared.TraceIDColumnName),
		valueindex.ColTypeName(modules_shared.ColumnTypeUUID),
		valueindex.FormatFilenameV2(0, wallMinSec, wallMaxSec, valueindex.NewID()),
	)
	if err := store.Put(key, data); err != nil {
		return fmt.Errorf("blockpack: WriteValueIndexL0: put %q: %w", key, err)
	}
	return nil
}

// flushAndPutL0 seals one group's value-index file and PUTs it under the
// time-range-embedded key (NOTE-VI-030, #431) so the querier can discover it by
// wall-clock window without opening the file.
func flushAndPutL0(store ObjectPutter, g *l0Group, tenant, indexPrefix string) error {
	// NOTE-VI-045 (#429): write the v2 BucketGroup format directly.
	data, err := g.writer.FlushBucket(context.Background(), 0)
	if err != nil {
		return fmt.Errorf("blockpack: WriteValueIndexL0: flush %q: %w", g.colName, err)
	}
	if len(data) == 0 {
		return nil
	}

	// Embed the wall-clock time range in the filename for O(1) discovery
	// (NOTE-VI-030). The BucketGroup footer carries file-level min/max time_sec.
	var wallMinSec, wallMaxSec uint64
	if ft, ferr := valueindex.DecodeBucketFooter(data); ferr == nil {
		wallMinSec, wallMaxSec = ft.MinTimeSec, ft.MaxTimeSec
	}

	key := path.Join(
		tenant,
		indexPrefix,
		valueindex.ColHash(g.colName),
		valueindex.ColTypeName(g.colType),
		valueindex.FormatFilenameV2(0, wallMinSec, wallMaxSec, valueindex.NewID()),
	)
	if err := store.Put(key, data); err != nil {
		return fmt.Errorf("blockpack: WriteValueIndexL0: put %q: %w", key, err)
	}
	return nil
}
