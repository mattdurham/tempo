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
func WriteValueIndexL0(r *Reader, store ObjectPutter, sourceRef, tenant, indexPrefix string) error {
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

	// nil denylist indexes every column (NOTE-VI-027, issue #414): the value index
	// is policy-free; the querier decides which columns are useful at read time.
	err := ExtractValueIndexEntries(r, nil, func(e ValueIndexEntry) error {
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
		// TraceID is stamped per row by ExtractValueIndexEntries (Stage 5,
		// traceindex.go wiring plan) and must be threaded through here: bucket
		// stream-compaction merge/dedup keys SpanRefs by TraceID
		// (bucketmerge.go/stream_compaction.go), so a hardcoded zero would make
		// every distinct trace observed at the same (SourceRef, BlockRef) collide
		// under one shared key and silently lose spans on the first compaction.
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
	})
	if err != nil {
		return fmt.Errorf("blockpack: WriteValueIndexL0: extract: %w", err)
	}

	for _, g := range groups {
		if err := flushAndPutL0(store, g, tenant, indexPrefix); err != nil {
			return err
		}
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
