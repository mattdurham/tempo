package valueindex

// NOTE: Any changes to this file must be reflected in the corresponding SPECS.md or NOTES.md.
//
// SPEC-VI-18 (issue #503): disk-spill for mergeGroupsAtKey's transient per-key merge
// buffer. Mirrors runspill.go's external sort-merge shape (NOTE-VI-026) but is a
// separate, purpose-built format: keySpillRecord is a flat, ephemeral, process-internal
// wire record (one contributed SpanRef occurrence for a given (already-interned
// SourceID, BlockRef) ref, before per-TraceID SpanIndexes union has happened). Spill
// files created by this file are created and fully consumed within a single
// mergeGroupsAtKey call, so this format has no versioning/compat concerns unlike
// runspill.go's rawEntry format.

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"sort"
)

// keySpillRecord is one flat spill record: a single contributed SpanRef occurrence for
// a given (sourceID, ref) pair, within the accumulation of one (TimeSec, CanonicalValue)
// key. sourceID is the already-interned outTable string-table index (see Design decision
// section in .bob/state/plan.md — interning happens eagerly, in first-arrival order, so
// spilled and non-spilled accumulation produce the identical total order).
type keySpillRecord struct {
	spanIndexes []uint16
	ref         BlockRef
	traceID     [16]byte
	sourceID    uint16
}

// keySpillRecordFixedSize is the wire byte size of every field except spanIndexes:
// sourceID[2] + BlockRefSize[5] + traceID[16] + idx_count[2].
const keySpillRecordFixedSize = 2 + BlockRefSize + 16 + 2

// writeKeySpillRecord serializes one keySpillRecord in the spill wire format:
// sourceID[2] + BlockRef[5] + traceID[16] + idx_count[2] + idx[2]*idx_count.
func writeKeySpillRecord(w io.Writer, r *keySpillRecord) error {
	buf := make([]byte, 0, keySpillRecordFixedSize+2*len(r.spanIndexes))
	buf = binary.LittleEndian.AppendUint16(buf, r.sourceID)
	buf = AppendBlockRef(buf, r.ref)
	buf = append(buf, r.traceID[:]...)
	buf = binary.LittleEndian.AppendUint16(buf, uint16(len(r.spanIndexes))) //nolint:gosec // bounded
	for _, idx := range r.spanIndexes {
		buf = binary.LittleEndian.AppendUint16(buf, idx)
	}
	_, err := w.Write(buf)
	return err
}

// readKeySpillRecord reads one keySpillRecord from a spill file. Returns io.EOF at a
// clean end of stream. spanIndexes is nil (not an empty non-nil slice) when idx_count is
// 0, so a round-tripped record with a nil input is byte-for-byte comparable via
// reflect.DeepEqual.
func readKeySpillRecord(r *bufio.Reader) (keySpillRecord, error) {
	var fixed [keySpillRecordFixedSize]byte
	if _, err := io.ReadFull(r, fixed[:]); err != nil {
		return keySpillRecord{}, err // io.EOF propagates on a clean boundary
	}

	var rec keySpillRecord
	rec.sourceID = binary.LittleEndian.Uint16(fixed[0:2])
	rec.ref = DecodeBlockRef(fixed[:], 2)
	copy(rec.traceID[:], fixed[2+BlockRefSize:2+BlockRefSize+16])
	idxCount := binary.LittleEndian.Uint16(fixed[2+BlockRefSize+16:])

	if idxCount > 0 {
		idxBytes := make([]byte, int(idxCount)*2)
		if _, err := io.ReadFull(r, idxBytes); err != nil {
			return keySpillRecord{}, fmt.Errorf("valueindex: readKeySpillRecord: spanIndexes: %w", err)
		}
		rec.spanIndexes = make([]uint16, idxCount)
		for i := range rec.spanIndexes {
			rec.spanIndexes[i] = binary.LittleEndian.Uint16(idxBytes[i*2:])
		}
	}
	return rec, nil
}

// estimateKeySpillRecordBytes returns the wire size of r — the same formula
// writeKeySpillRecord's encoding produces (keySpillRecordFixedSize + 2 bytes per
// spanIndex) — used to track the running accumulation total against
// shared.ValueIndexMergeBufferSpillBytes without a real encode per record.
func estimateKeySpillRecordBytes(r *keySpillRecord) int {
	return keySpillRecordFixedSize + 2*len(r.spanIndexes)
}

// compareKeySpillRecord orders two records by (sourceID, ref.PageNum, traceID) — the
// same total order mergeGroupsAtKey's own final sort produces today, required so the
// eventual k-way merge across spilled chunks (Phase 4) reproduces identical output order.
func compareKeySpillRecord(a, b *keySpillRecord) int {
	if a.sourceID != b.sourceID {
		if a.sourceID < b.sourceID {
			return -1
		}
		return 1
	}
	if a.ref.PageNum != b.ref.PageNum {
		if a.ref.PageNum < b.ref.PageNum {
			return -1
		}
		return 1
	}
	return bytes.Compare(a.traceID[:], b.traceID[:])
}

// streamingSplitPacker is an incremental, byte-for-byte behavioral port of
// splitBucketGroupBySpanCap's greedy first-fit packing logic (bucketfile.go:256-304).
// Feeding refs one at a time via addRef, then calling finish(), produces the exact same
// result as calling splitBucketGroupBySpanCap once on the same refs in the same order
// (TestStreamingSplitPacker_MatchesSplitBucketGroupBySpanCap pins this equivalence) — the
// piece that makes #503's memory-bound claim provable, since sibling packing can now run
// ref-by-ref during the streaming k-way reduce (Phase 4) instead of materializing the
// full merged slice first. This type holds at most ONE in-progress group at a time; do
// not "improve" the packing strategy here — any strategy change is a separate decision.
type streamingSplitPacker struct {
	canonicalValue []byte
	cur            BucketGroup
	timeSec        uint64
	maxSpans       int
	curCount       int
	anyRefSeen     bool
}

// newStreamingSplitPacker constructs a packer for one (TimeSec, CanonicalValue) key.
func newStreamingSplitPacker(timeSec uint64, value []byte, maxSpans int) *streamingSplitPacker {
	return &streamingSplitPacker{
		timeSec:        timeSec,
		canonicalValue: value,
		maxSpans:       maxSpans,
		cur:            BucketGroup{TimeSec: timeSec, CanonicalValue: value},
	}
}

// flush resets the in-progress group, returning the flushed group if it held any refs
// (nil otherwise) — mirrors splitBucketGroupBySpanCap's own flush closure.
func (p *streamingSplitPacker) flush() *BucketGroup {
	if len(p.cur.Refs) == 0 {
		p.cur = BucketGroup{TimeSec: p.timeSec, CanonicalValue: p.canonicalValue}
		return nil
	}
	flushed := p.cur
	p.cur = BucketGroup{TimeSec: p.timeSec, CanonicalValue: p.canonicalValue}
	p.curCount = 0
	return &flushed
}

// testHookPackerCurSpans is a zero-cost-in-production instrumentation hook (nil by
// default), wired to a counter ONLY by
// TestMergeGroupsAtKey_NoInMemoryStructureExceedsStructuralBound (Phase 8, #503) to observe
// streamingSplitPacker's in-progress group's running span count every time it changes.
var testHookPackerCurSpans func(n int)

// addRef appends ref to the packer, returning zero or more COMPLETED sibling groups that
// the caller must flush immediately (this packer never buffers more than one
// in-progress group across addRef calls). maxSpans <= 0 behaves exactly like
// splitBucketGroupBySpanCap's own no-op convention: never split, everything accumulates
// into one group, returned only by finish().
func (p *streamingSplitPacker) addRef(ref BucketBlockRef) []BucketGroup {
	p.anyRefSeen = true
	if p.maxSpans <= 0 {
		p.cur.Refs = append(p.cur.Refs, ref)
		return nil
	}

	var out []BucketGroup
	refCount := len(ref.Spans)
	switch {
	case refCount > p.maxSpans:
		// This ref alone exceeds the cap: flush whatever's pending, then split its own
		// Spans slice across as many fresh siblings as needed.
		if flushed := p.flush(); flushed != nil {
			out = append(out, *flushed)
		}
		remaining := ref.Spans
		for len(remaining) > 0 {
			take := min(p.maxSpans, len(remaining))
			out = append(out, BucketGroup{
				TimeSec:        p.timeSec,
				CanonicalValue: p.canonicalValue,
				Refs: []BucketBlockRef{{
					SourceID: ref.SourceID,
					Ref:      ref.Ref,
					Spans:    remaining[:take],
				}},
			})
			remaining = remaining[take:]
		}
	case p.curCount+refCount > p.maxSpans:
		if flushed := p.flush(); flushed != nil {
			out = append(out, *flushed)
		}
		p.cur.Refs = append(p.cur.Refs, ref)
		p.curCount = refCount
	default:
		p.cur.Refs = append(p.cur.Refs, ref)
		p.curCount += refCount
	}
	if testHookPackerCurSpans != nil {
		testHookPackerCurSpans(p.curCount)
	}
	return out
}

// finish flushes whatever's pending, mirroring splitBucketGroupBySpanCap's final flush().
// If addRef was never called at all (zero contributing refs for this key), returns a
// single empty group matching splitBucketGroupBySpanCap's own no-op path (which always
// returns exactly one group, even for a zero-ref input) — see Edge Case 1 in plan.md.
func (p *streamingSplitPacker) finish() []BucketGroup {
	if p.maxSpans <= 0 {
		return []BucketGroup{p.cur}
	}
	if flushed := p.flush(); flushed != nil {
		return []BucketGroup{*flushed}
	}
	if !p.anyRefSeen {
		return []BucketGroup{{TimeSec: p.timeSec, CanonicalValue: p.canonicalValue}}
	}
	return nil
}

// keySpillChunk is a single sorted run of keySpillRecords spilled to disk when one key's
// accumulating union crosses ValueIndexMergeBufferSpillBytes — mirrors runspill.go's
// runFile.
type keySpillChunk struct {
	f    *os.File
	path string
}

// writeKeySpillChunk sorts records by compareKeySpillRecord and writes them to a new
// temp file inside tmpDir (never os.TempDir() — the caller must thread the PVC-backed
// scratch directory explicitly). The returned keySpillChunk is positioned at offset 0
// for subsequent reading, mirroring runspill.go's writeRun.
func writeKeySpillChunk(tmpDir string, records []keySpillRecord) (*keySpillChunk, error) {
	sort.Slice(records, func(i, j int) bool {
		return compareKeySpillRecord(&records[i], &records[j]) < 0
	})

	f, err := os.CreateTemp(tmpDir, "vi-keymerge-*.tmp")
	if err != nil {
		return nil, fmt.Errorf("valueindex: writeKeySpillChunk: create temp: %w", err)
	}
	name := f.Name()
	bw := bufio.NewWriterSize(f, 256<<10)
	for i := range records {
		if err := writeKeySpillRecord(bw, &records[i]); err != nil {
			_ = f.Close()
			_ = os.Remove(name) //nolint:gosec // G703: name comes from os.CreateTemp, not user input
			return nil, fmt.Errorf("valueindex: writeKeySpillChunk: %w", err)
		}
	}
	if err := bw.Flush(); err != nil {
		_ = f.Close()
		_ = os.Remove(name) //nolint:gosec // G703: name comes from os.CreateTemp, not user input
		return nil, fmt.Errorf("valueindex: writeKeySpillChunk: flush: %w", err)
	}
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		_ = f.Close()
		_ = os.Remove(name) //nolint:gosec // G703: name comes from os.CreateTemp, not user input
		return nil, fmt.Errorf("valueindex: writeKeySpillChunk: seek: %w", err)
	}
	return &keySpillChunk{f: f, path: name}, nil
}

// remove closes and deletes the chunk's temp file, mirroring runspill.go's
// runFile.remove().
func (c *keySpillChunk) remove() {
	if c.f != nil {
		_ = c.f.Close()
	}
	if c.path != "" {
		_ = os.Remove(c.path) //nolint:gosec // G703: path comes from os.CreateTemp, not user input
	}
}

// keySpillChunkReader is one input stream of the final per-key k-way merge (Phase 4): a
// buffered reader over a spilled chunk file, OR the sorted in-memory tail, unified
// behind one advance()/cur/valid shape — mirrors runspill.go's runReader.
type keySpillChunkReader struct {
	br    *bufio.Reader // nil for the in-memory tail source
	tail  []keySpillRecord
	cur   keySpillRecord
	tailI int
	valid bool
}

// advance loads the next record into cur, setting valid=false at a clean end of stream.
func (cr *keySpillChunkReader) advance() error {
	if cr.br != nil {
		r, err := readKeySpillRecord(cr.br)
		if err != nil {
			if err == io.EOF { //nolint:errorlint // exact sentinel per readKeySpillRecord's own contract
				cr.valid = false
				return nil
			}
			return err
		}
		cr.cur = r
		cr.valid = true
		return nil
	}
	if cr.tailI < len(cr.tail) {
		cr.cur = cr.tail[cr.tailI]
		cr.tailI++
		cr.valid = true
		return nil
	}
	cr.valid = false
	return nil
}

// nextKeySpillRecord finds the reader holding the smallest current record among readers
// (by compareKeySpillRecord), advances that reader, and returns the record it held —
// mirrors runspill.go's mergeRuns "find min among readers, advance" step. Reports
// ok=false once every reader is exhausted.
func nextKeySpillRecord(readers []*keySpillChunkReader) (keySpillRecord, bool, error) {
	minIdx := -1
	for i, r := range readers {
		if !r.valid {
			continue
		}
		if minIdx == -1 || compareKeySpillRecord(&r.cur, &readers[minIdx].cur) < 0 {
			minIdx = i
		}
	}
	if minIdx == -1 {
		return keySpillRecord{}, false, nil
	}
	rec := readers[minIdx].cur
	if err := readers[minIdx].advance(); err != nil {
		return keySpillRecord{}, false, err
	}
	return rec, true, nil
}

// testHookReduceUnionSize is a zero-cost-in-production instrumentation hook (nil by
// default), wired to a counter ONLY by
// TestMergeGroupsAtKey_NoInMemoryStructureExceedsStructuralBound (Phase 8, #503) to observe
// reduceKeySpillRecords' per-TraceID union map size every time it gains an entry.
var testHookReduceUnionSize func(n int)

// reduceKeySpillRecords performs the streaming k-way merge of one key's spilled chunks
// plus its sorted in-memory tail, grouping consecutive same-(sourceID, ref) records into a
// completed BucketBlockRef and, within that, grouping consecutive same-traceID records
// into a unioned SpanRef — a map[uint16]struct{} scoped to ONLY the current TraceID being
// built, freed the instant the TraceID changes, then sorted via sortUint16 before
// appending (matching mergeGroupsAtKey's own output order exactly). This UNIONS
// SpanIndexes across duplicate (sourceID, ref, traceID) triples rather than collapsing to
// one entry — a correctness-critical divergence from runspill.go's sameEntry/mergeRuns
// dedup rule ("same-entry collapse"), called out explicitly in plan.md's Key Decisions.
// Each completed ref is fed into a streamingSplitPacker (Phase 3) as soon as it's
// finished, so sibling packing runs incrementally instead of after a full
// materialization. ctx.Err() is checked once per ref boundary (the same cadence
// accumulateAndSpillKeyRecords already uses for the accumulation phase, Edge Case 4,
// plan.md) so a long-running hot-key reduce/merge/pack pass can still be canceled
// promptly, closing the half of Edge Case 4 that accumulation alone did not cover.
func reduceKeySpillRecords(
	ctx context.Context,
	chunks []*keySpillChunk,
	tail []keySpillRecord,
	timeSec uint64,
	value []byte,
	maxSpansPerGroup int,
) ([]BucketGroup, error) {
	sort.Slice(tail, func(i, j int) bool {
		return compareKeySpillRecord(&tail[i], &tail[j]) < 0
	})

	readers := make([]*keySpillChunkReader, 0, len(chunks)+1)
	for _, c := range chunks {
		r := &keySpillChunkReader{br: bufio.NewReaderSize(c.f, 256<<10)}
		if err := r.advance(); err != nil {
			return nil, fmt.Errorf("valueindex: reduceKeySpillRecords: %w", err)
		}
		readers = append(readers, r)
	}
	if len(tail) > 0 {
		r := &keySpillChunkReader{tail: tail}
		if err := r.advance(); err != nil {
			return nil, fmt.Errorf("valueindex: reduceKeySpillRecords: %w", err)
		}
		readers = append(readers, r)
	}

	packer := newStreamingSplitPacker(timeSec, value, maxSpansPerGroup)
	var out []BucketGroup
	var curRef *BucketBlockRef
	var curIdx map[uint16]struct{}
	var curTraceID [16]byte
	haveCurTraceID := false

	flushSpan := func() {
		if curRef == nil || !haveCurTraceID {
			return
		}
		idxs := make([]uint16, 0, len(curIdx))
		for idx := range curIdx {
			idxs = append(idxs, idx)
		}
		sortUint16(idxs)
		curRef.Spans = append(curRef.Spans, SpanRef{TraceID: curTraceID, SpanIndexes: idxs})
		curIdx = nil
		haveCurTraceID = false
	}
	flushRef := func() {
		flushSpan()
		if curRef == nil {
			return
		}
		out = append(out, packer.addRef(*curRef)...)
		curRef = nil
	}

	for {
		rec, ok, err := nextKeySpillRecord(readers)
		if err != nil {
			return nil, fmt.Errorf("valueindex: reduceKeySpillRecords: %w", err)
		}
		if !ok {
			break
		}
		if curRef == nil || curRef.SourceID != rec.sourceID || curRef.Ref != rec.ref {
			flushRef()
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			curRef = &BucketBlockRef{SourceID: rec.sourceID, Ref: rec.ref}
		}
		if !haveCurTraceID || curTraceID != rec.traceID {
			flushSpan()
			curTraceID = rec.traceID
			haveCurTraceID = true
			curIdx = make(map[uint16]struct{}, len(rec.spanIndexes))
		}
		for _, idx := range rec.spanIndexes {
			curIdx[idx] = struct{}{}
		}
		if testHookReduceUnionSize != nil {
			testHookReduceUnionSize(len(curIdx))
		}
	}
	flushRef()
	out = append(out, packer.finish()...)
	return out, nil
}
