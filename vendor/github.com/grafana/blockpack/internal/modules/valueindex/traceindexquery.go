package valueindex

// NOTE: Any changes to this file must be reflected in the corresponding NOTES.md.
//
// traceindexquery.go — partial-read query helpers for the v2 batched TraceGroup
// index format (issue #476, NOTE-VI-075). These let a trace-by-id lookup answer "which spans
// does trace T have" WITHOUT downloading and decoding the whole index file — the
// structural defect the flat-blob v1 format had (19MB-205MB files, full download
// + full decode per lookup).
//
// The flow mirrors the search/metrics BucketGroup read path (bucketquery.go +
// disk_iterator.go):
//
//  1. Read only the fixed traceFooterSize tail — the block directory offset/len
//     and file min/max time — with one ranged read.
//  2. Read the block directory (TOC): one entry per block with its
//     [minTraceID, maxTraceID] range and [minTimeSec, maxTimeSec] range.
//  3. Prune blocks: skip any whose TraceID range does not contain the target and
//     any whose time range does not overlap the query window.
//  4. For each surviving block, ranged-read + snappy-decode ONLY that block, test
//     its trace-ID bloom, and scan its groups for the target TraceID.
//
// A miss anywhere (target outside every block's range, or absent from the bloom)
// resolves with zero body fetches. A hit fetches only the one block that could
// contain it. Because groups are sorted by TraceID, at most one block per file
// can contain a given TraceID's range boundary, but the target may span adjacent
// blocks only if its ID equals a block boundary; the pruning below scans every
// block whose inclusive [min,max] range covers the target, which is correct in
// all cases.

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"log/slog"
	"time"

	"github.com/golang/snappy"
	"go.opentelemetry.io/otel/attribute"
)

// traceLookupStepThreshold is the per-step duration above which LookupTraceGroupPartial's
// step helper (traceLookupStep) additionally emits a slog.Warn line, on top of the OTel span
// it always records (task #202). An OTel span alone requires a trace exporter/viewer on hand
// to inspect; a slow round trip buried inside the object-store client's OWN internal
// retry/backoff loop (invisible to this package -- e.g. minio-go's executeMethod defaults to
// MaxRetry=10 attempts with exponential backoff up to a 1s cap per attempt, silently consuming
// several seconds with no single call ever "looking" abnormally slow to a naive timeout check)
// is exactly the kind of thing an operator investigating a live incident needs to see in plain
// application logs first, without needing a sampled trace already captured for that specific
// request. 500ms is comfortably above blockpack's own CLAUDE.md guidance that a single object-
// store round trip is normally 50-100ms, so a step this slow is already anomalous.
const traceLookupStepThreshold = 500 * time.Millisecond

// traceLookupStep runs fn as one named step of a LookupTraceGroupPartial call (task #202):
// "size", "footer_read", "tail_read", or "block_read". It wraps fn in a child OTel span named
// "valueindex.trace_lookup.<step>" carrying the object key, byte count, and duration (plus any
// caller-supplied extra attributes, e.g. a block index) so a future 10-20s trace-by-id timeout
// can be attributed to the EXACT step responsible -- Size(key) [StatObject], the footer ReadAt,
// the tail (block-directory + string-table) ReadAt, or one SPECIFIC block-body ReadAt -- instead
// of surfacing as one aggregate "context deadline exceeded" with no breakdown. When fn takes at
// least traceLookupStepThreshold, a structured slog.Warn line is also emitted so the same
// attribution is visible in plain application logs even without a trace viewer.
func traceLookupStep(
	ctx context.Context, key, step string, byteLen int, fn func() error, extra ...attribute.KeyValue,
) error {
	_, span := tracer.Start(ctx, "valueindex.trace_lookup."+step)
	defer span.End()

	start := time.Now()
	err := fn()
	dur := time.Since(start)

	if span.IsRecording() {
		attrs := append([]attribute.KeyValue{
			attribute.String("valueindex.trace_lookup.key", key),
			attribute.Int("valueindex.trace_lookup.bytes", byteLen),
			attribute.Int64("valueindex.trace_lookup.duration_ms", dur.Milliseconds()),
		}, extra...)
		if err != nil {
			attrs = append(attrs, attribute.String("valueindex.trace_lookup.error", err.Error()))
		}
		span.SetAttributes(attrs...)
	}
	if dur >= traceLookupStepThreshold {
		slog.Warn("valueindex: LookupTraceGroupPartial: slow step",
			"key", key, "step", step, "bytes", byteLen, "duration_ms", dur.Milliseconds(), "err", err)
	}
	return err
}

// TraceFooter is the decoded fixed-size v2 TraceGroup file footer.
type TraceFooter struct {
	BlockIndexOff  uint64
	BlockIndexLen  uint64
	StringTableOff uint64
	StringTableLen uint64
	MinTimeSec     uint64
	MaxTimeSec     uint64
	Version        uint8
}

// DecodeTraceFooter parses the fixed-size footer at the tail of data. The caller
// may pass only the last traceFooterSize bytes of the file (e.g. from a ranged
// tail read) — file pruning by time range needs nothing else.
func DecodeTraceFooter(data []byte) (TraceFooter, error) {
	if len(data) < traceFooterSize {
		return TraceFooter{}, fmt.Errorf("valueindex: trace footer too short (%d bytes)", len(data))
	}
	f := data[len(data)-traceFooterSize:]
	if binary.LittleEndian.Uint32(f[:4]) != TraceFileMagic {
		return TraceFooter{}, fmt.Errorf("valueindex: bad trace footer magic")
	}
	return TraceFooter{
		BlockIndexOff:  binary.LittleEndian.Uint64(f[4:12]),
		BlockIndexLen:  binary.LittleEndian.Uint64(f[12:20]),
		StringTableOff: binary.LittleEndian.Uint64(f[20:28]),
		StringTableLen: binary.LittleEndian.Uint64(f[28:36]),
		MinTimeSec:     binary.LittleEndian.Uint64(f[36:44]),
		MaxTimeSec:     binary.LittleEndian.Uint64(f[44:52]),
		Version:        f[52],
	}, nil
}

// OverlapsTimeRange reports whether the file's [MinTimeSec, MaxTimeSec] intersects
// the closed query window [minTS, maxTS]. Used to prune whole files before
// fetching the body.
func (ft TraceFooter) OverlapsTimeRange(minTS, maxTS uint64) bool {
	return ft.MaxTimeSec >= minTS && ft.MinTimeSec <= maxTS
}

// TraceRandomReader is the ranged-read surface LookupTraceGroupPartial needs: the
// object's total size and an io.ReaderAt-style ranged fetch. It is a subset of
// the FileStore/ValueIndexFileStore contract the querier already holds — the same
// minimal Size + ReadAt shape (issue #476). key identifies the object; every call
// targets the same file.
type TraceRandomReader interface {
	Size(key string) (int64, error)
	ReadAt(key string, p []byte, off int64) (int, error)
}

// LookupTraceGroupPartial resolves a single TraceID against one v2 batched
// TraceGroup index file using targeted partial reads, never a whole-object fetch
// (issue #476). It returns:
//
//   - (group, true, nil): the file holds groups for traceID (merged across any
//     blocks whose range covers it; spans deduplicated by SpanID, first wins;
//     TimeSec is the minimum across matches — the same live-merge semantics as
//     findTraceGroupInCandidates and MergeTraceGroups).
//   - (zero, false, nil): the file is a valid v2 file that does not hold traceID
//     within the query window.
//   - (zero, false, err): a read or decode failure — the file could not be
//     trusted (the authoritative-index contract surfaces this, never masks it).
//
// queryMinSec/queryMaxSec is the inclusive time window; blocks whose time range
// does not overlap it are pruned. Pass (0, math.MaxUint64) for no time bound.
func LookupTraceGroupPartial(
	ctx context.Context,
	store TraceRandomReader,
	key string,
	traceID [16]byte,
	queryMinSec, queryMaxSec uint64,
) (TraceGroup, bool, error) {
	ctx, span := tracer.Start(ctx, "valueindex.trace_lookup")
	defer span.End()
	if span.IsRecording() {
		span.SetAttributes(attribute.String("valueindex.trace_lookup.key", key))
	}

	var size int64
	if serr := traceLookupStep(ctx, key, "size", 0, func() error {
		var err error
		size, err = store.Size(key)
		return err
	}); serr != nil {
		return TraceGroup{}, false, fmt.Errorf("valueindex: trace lookup size %q: %w", key, serr)
	}
	if size < int64(traceFooterSize) {
		return TraceGroup{}, false, fmt.Errorf(
			"valueindex: trace file %q too short (%d bytes) for v2 footer", key, size,
		)
	}

	// Read the footer (fixed tail).
	footerBuf := make([]byte, traceFooterSize)
	if rerr := traceLookupStep(ctx, key, "footer_read", traceFooterSize, func() error {
		_, err := store.ReadAt(key, footerBuf, size-int64(traceFooterSize))
		return err
	}); rerr != nil {
		return TraceGroup{}, false, fmt.Errorf("valueindex: trace lookup footer %q: %w", key, rerr)
	}
	ft, ferr := DecodeTraceFooter(footerBuf)
	if ferr != nil {
		return TraceGroup{}, false, fmt.Errorf("valueindex: trace lookup %q: %w", key, ferr)
	}
	if !ft.OverlapsTimeRange(queryMinSec, queryMaxSec) {
		return TraceGroup{}, false, nil
	}

	// Read the block directory (TOC) + string table together in one ranged read:
	// they are contiguous — the string table immediately precedes the block index
	// (see EncodeTraceGroups layout) — so a single read from strTableOff to the
	// footer covers both, avoiding a second round trip.
	tailStart := ft.StringTableOff
	if ft.BlockIndexOff < tailStart {
		tailStart = ft.BlockIndexOff
	}
	if tailStart > uint64(size) {
		return TraceGroup{}, false, fmt.Errorf("valueindex: trace file %q tail offset out of bounds", key)
	}
	tailLen := uint64(size) - tailStart
	tail := make([]byte, tailLen)
	//nolint:gosec // tailLen is bounded by size (int64), validated above.
	if rerr := traceLookupStep(ctx, key, "tail_read", int(tailLen), func() error {
		_, err := store.ReadAt(key, tail, int64(tailStart)) //nolint:gosec // tailStart <= size
		return err
	}); rerr != nil {
		return TraceGroup{}, false, fmt.Errorf("valueindex: trace lookup tail %q: %w", key, rerr)
	}

	// Resolve directory + string table slices relative to tailStart.
	strRel := ft.StringTableOff - tailStart
	if strRel+ft.StringTableLen > tailLen {
		return TraceGroup{}, false, fmt.Errorf("valueindex: trace file %q string table out of bounds", key)
	}
	table, _, terr := DecodeStringTable(tail[strRel : strRel+ft.StringTableLen])
	if terr != nil {
		return TraceGroup{}, false, fmt.Errorf("valueindex: trace lookup %q string table: %w", key, terr)
	}
	idxRel := ft.BlockIndexOff - tailStart
	if idxRel+ft.BlockIndexLen > tailLen {
		return TraceGroup{}, false, fmt.Errorf("valueindex: trace file %q block index out of bounds", key)
	}
	dir, derr := decodeTraceBlockIndex(tail[idxRel : idxRel+ft.BlockIndexLen])
	if derr != nil {
		return TraceGroup{}, false, fmt.Errorf("valueindex: trace lookup %q: %w", key, derr)
	}

	acc := traceMergeAccum{seenSpan: make(map[[8]byte]struct{})}
	for i := range dir {
		if err := ctx.Err(); err != nil {
			return TraceGroup{}, false, err
		}
		d := &dir[i]
		if !traceBlockMayContain(d, traceID, queryMinSec, queryMaxSec) {
			continue
		}
		blkGroups, berr := readTraceBlockForID(ctx, store, key, size, d, table, traceID, i)
		if berr != nil {
			return TraceGroup{}, false, fmt.Errorf(
				"valueindex: trace lookup %q block %d: %w", key, i, berr,
			)
		}
		acc.merge(blkGroups, traceID, queryMinSec, queryMaxSec)
	}
	if !acc.found {
		return TraceGroup{}, false, nil
	}
	return acc.merged, true, nil
}

// traceBlockMayContain reports whether a block's directory entry survives both
// the TraceID-range prune (the target must fall within [minTraceID, maxTraceID])
// and the time-range prune (the block must overlap the query window).
func traceBlockMayContain(
	d *traceBlockDirEntry,
	traceID [16]byte,
	queryMinSec, queryMaxSec uint64,
) bool {
	if bytes.Compare(traceID[:], d.minTraceID[:]) < 0 ||
		bytes.Compare(traceID[:], d.maxTraceID[:]) > 0 {
		return false
	}
	return d.maxTimeSec >= queryMinSec && d.minTimeSec <= queryMaxSec
}

// readTraceBlockForID ranged-reads only the given block's compressed body and
// returns every group in it matching traceID (nil if the bloom excludes it). blockIdx
// identifies this block's position in the file's directory (task #202) purely for the
// "valueindex.trace_lookup.block_index" span attribute below -- it distinguishes WHICH of
// potentially several surviving blocks' ReadAt call was slow, since a file can hold many
// blocks and LookupTraceGroupPartial's own doc comment already notes more than one can
// legitimately survive pruning for a given TraceID.
func readTraceBlockForID(
	ctx context.Context,
	store TraceRandomReader,
	key string,
	size int64,
	d *traceBlockDirEntry,
	table *StringTable,
	traceID [16]byte,
	blockIdx int,
) ([]TraceGroup, error) {
	end := d.compOff + d.compLen
	usize := uint64(size) //nolint:gosec // size is validated >= traceFooterSize by the caller, so non-negative
	if d.compOff > usize || d.compLen > usize || end > usize {
		return nil, fmt.Errorf("body out of bounds")
	}
	blockBuf := make([]byte, d.compLen)
	//nolint:gosec // d.compLen is bounded by size (int64), validated above.
	rerr := traceLookupStep(ctx, key, "block_read", int(d.compLen), func() error {
		_, err := store.ReadAt(key, blockBuf, int64(d.compOff)) //nolint:gosec // compOff <= size
		return err
	}, attribute.Int("valueindex.trace_lookup.block_index", blockIdx))
	if rerr != nil {
		return nil, rerr
	}
	blkGroups, _, berr := scanTraceBlockForID(blockBuf, table, traceID)
	return blkGroups, berr
}

// traceMergeAccum folds matching groups across blocks into one TraceGroup with
// live-merge semantics: dedup spans by SpanID (first wins), keep the minimum
// TimeSec.
type traceMergeAccum struct {
	seenSpan map[[8]byte]struct{}
	merged   TraceGroup
	found    bool
}

// merge folds every group in blkGroups whose TimeSec is within the window into
// the accumulator.
func (a *traceMergeAccum) merge(
	blkGroups []TraceGroup,
	traceID [16]byte,
	queryMinSec, queryMaxSec uint64,
) {
	for gi := range blkGroups {
		g := &blkGroups[gi]
		if g.TimeSec < queryMinSec || g.TimeSec > queryMaxSec {
			continue
		}
		if !a.found {
			a.merged.TraceID = traceID
			a.merged.TimeSec = g.TimeSec
			a.found = true
		} else if g.TimeSec < a.merged.TimeSec {
			a.merged.TimeSec = g.TimeSec
		}
		for si := range g.Spans {
			s := g.Spans[si]
			if _, dup := a.seenSpan[s.SpanID]; dup {
				continue
			}
			a.seenSpan[s.SpanID] = struct{}{}
			a.merged.Spans = append(a.merged.Spans, s)
		}
	}
}

// scanTraceBlockForID snappy-decodes a block body, tests its trace-ID bloom for
// traceID, and returns every group in the block matching traceID. bloomHit is
// false only when the bloom definitively excludes traceID (in which case the
// returned group slice is nil and the block was not scanned). A true bloomHit
// means the block was scanned; the returned slice may still be empty (a bloom
// false positive, or the ID sorts within the block's range but is absent).
func scanTraceBlockForID(compressed []byte, table *StringTable, traceID [16]byte) ([]TraceGroup, bool, error) {
	raw, err := snappy.Decode(nil, compressed)
	if err != nil {
		return nil, false, fmt.Errorf("snappy decode: %w", err)
	}
	// Read the block header enough to test the bloom before decoding all groups.
	pos := 16 + 16 + 8 + 8 // min/max trace id + min/max time
	if pos+4 > len(raw) {
		return nil, false, fmt.Errorf("block bloom_len truncated")
	}
	bloomLen := int(binary.LittleEndian.Uint32(raw[pos:]))
	pos += 4
	if bloomLen < 0 || pos+bloomLen > len(raw) {
		return nil, false, fmt.Errorf("block bloom truncated")
	}
	bloom := raw[pos : pos+bloomLen]
	if !TestValueBloom(bloom, traceID[:]) {
		return nil, false, nil
	}

	groups, gerr := decodeTraceBlockBody(raw, table)
	if gerr != nil {
		return nil, true, gerr
	}
	var matched []TraceGroup
	for i := range groups {
		if groups[i].TraceID == traceID {
			matched = append(matched, groups[i])
		}
	}
	return matched, true, nil
}
