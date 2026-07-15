package valueindex

// NOTE: Any changes to this file must be reflected in the corresponding NOTES.md.
//
// disk_trace_iterator.go — a lazy, block-at-a-time TraceGroupIterator over a v2 "VTG2"
// TraceGroup file staged on local disk (issue #500, NOTE-VI-109). Mirrors disk_iterator.go's
// BucketGroup design exactly: diskTraceGroupFileIterator decodes only bounded metadata
// (header magic, footer, string table, block directory) eagerly at construction, and decodes
// one block's groups at a time — lazily, on Advance crossing a block boundary — bounding peak
// decoded memory per input file to one block regardless of file size.

import (
	"context"
	"encoding/binary"
	"fmt"
	"os"

	"github.com/golang/snappy"
)

// TraceGroupIterator is the interface StreamCompactTraceGroups' k-way merge requires: a
// forward-only, (TraceID ASC, TimeSec ASC) sorted stream of TraceGroups. This is a distinct
// interface from GroupIterator (stream_compaction.go), not a shared one, for two reasons found
// while implementing this file:
//
//  1. Different merge key: BucketGroup's merge key is (TimeSec, CanonicalValue); TraceGroup's
//     is TraceID alone (TimeSec is not part of the key — MergeTraceGroups collapses every group
//     sharing a TraceID, regardless of TimeSec, into one output group). Forcing both through one
//     Peek()-typed interface would need a lossy adapter either way.
//  2. No StringTable() method: unlike BucketBlockRef.SourceID (a uint16 meaningful only
//     relative to its own file's StringTable, resolved lazily by the merge), SpanEntry.SourceRef
//     is already resolved to a plain string at decode time (decodeTraceGroupAt calls
//     table.Lookup(idx) once, eagerly) — a decoded TraceGroup carries no further
//     table-dependent state, so there is nothing for a StringTable() accessor to expose.
type TraceGroupIterator interface {
	// Peek returns the current group without advancing, or (nil, false) if exhausted OR if
	// Err() is non-nil.
	Peek() (*TraceGroup, bool)
	// Advance moves to the next group. May perform local disk I/O and may set Err().
	Advance(ctx context.Context)
	// Err returns the first error encountered while advancing, or nil.
	Err() error
	// Close releases resources (e.g. an open local temp file). Idempotent.
	Close() error
}

// diskTraceGroupFileIterator is the disk-backed TraceGroupIterator implementation. See
// NewDiskTraceGroupFileIterator for the construction/decode contract.
type diskTraceGroupFileIterator struct {
	checker    RefChecker
	err        error
	f          *os.File
	table      *StringTable
	live       map[string]bool // SourceRef -> isLive cache, shared across every block of this file
	curBlock   []TraceGroup    // nil: no current block (not yet found one, or exhausted)
	path       string
	dir        []traceBlockDirEntry
	stats      CompactStats
	nextDirIdx int
	groupIdx   int // position within curBlock
}

var _ TraceGroupIterator = (*diskTraceGroupFileIterator)(nil)

// var _ StatsProvider = (*diskTraceGroupFileIterator)(nil) reuses stream_compaction.go's
// StatsProvider interface unmodified — diskTraceGroupFileIterator accumulates CompactStats the
// same way diskBucketFileIterator does, so no TraceGroup-specific stats interface is needed.
var _ StatsProvider = (*diskTraceGroupFileIterator)(nil)

// NewDiskTraceGroupFileIterator opens path and eagerly decodes its header magic, footer,
// string table, and block directory — all bounded/cheap regardless of file size — then eagerly
// decodes forward from block 0 until it finds a block with at least one (optionally
// retention-filtered) group, so Peek never needs to perform I/O or return an error.
//
// Returns (nil, nil) — an explicit untyped nil TraceGroupIterator, never a typed-nil pointer
// wrapped in the interface — if the file fails only the header-magic check, mirroring
// NewDiskBucketFileIterator's "legacy/bad-magic file, skip don't abort" contract for
// consistency. Any other decode failure is a real error, returned here.
//
// Ownership/cleanup contract (mirrors NewDiskBucketFileIterator): on the (nil, nil) skip path
// or any other non-success return, this constructor closes the *os.File it opened (if it got
// that far) but never removes path — the caller retains ownership of path removal in both
// cases. Only on success does ownership of both the fd and path removal transfer to the
// returned iterator's Close().
func NewDiskTraceGroupFileIterator(ctx context.Context, path string, checker RefChecker) (TraceGroupIterator, error) {
	f, err := os.Open(path) //nolint:gosec // G304: path is mergeTraceLevel's own local temp file, not user input
	if err != nil {
		return nil, fmt.Errorf("valueindex: NewDiskTraceGroupFileIterator: open %q: %w", path, err)
	}

	magic := make([]byte, 4)
	if _, rerr := f.ReadAt(magic, 0); rerr != nil {
		_ = f.Close()
		return nil, fmt.Errorf("valueindex: NewDiskTraceGroupFileIterator: read header magic: %w", rerr)
	}
	if binary.LittleEndian.Uint32(magic) != TraceFileMagic {
		// Not a v2 "VTG2" file: skip, not abort. The caller (mergeTraceLevel) still owns path,
		// and per its own documented contract treats this the same as any other undecodable
		// input — left in place, not deleted (the v1 flat-blob rollover window has closed per
		// NOTE-VI-079, so there is no longer a "safe to discard, known-legacy" case here, unlike
		// disk_iterator.go's BucketGroup sibling).
		_ = f.Close()
		return nil, nil
	}

	dir, table, err := readTraceFileMetadata(f)
	if err != nil {
		_ = f.Close()
		return nil, fmt.Errorf("valueindex: NewDiskTraceGroupFileIterator: %w", err)
	}

	it := &diskTraceGroupFileIterator{
		f:       f,
		path:    path,
		table:   table,
		dir:     dir,
		checker: checker,
		live:    make(map[string]bool),
	}
	it.advanceBlock(ctx)
	if it.err != nil {
		_ = f.Close()
		return nil, fmt.Errorf("valueindex: NewDiskTraceGroupFileIterator: %w", it.err)
	}
	return it, nil
}

// readTraceFileMetadata reads and decodes f's footer, string table, and block directory — the
// eager, bounded metadata NewDiskTraceGroupFileIterator needs before any block can be
// addressed. f's header magic must already have been validated by the caller. Self-contained
// (does not go through the RangedSource/ReadBucketFileMetadata abstraction disk_iterator.go
// uses — that abstraction is BucketGroup-specific; TraceGroup has no equivalent shared ranged
// metadata reader today, and traceindexquery.go's own ranged read path has its own inline
// footer/tail decode, unrelated to this local-disk construction path).
func readTraceFileMetadata(f *os.File) ([]traceBlockDirEntry, *StringTable, error) {
	fi, err := f.Stat()
	if err != nil {
		return nil, nil, fmt.Errorf("stat: %w", err)
	}
	size := fi.Size()
	if size < int64(traceFooterSize) {
		return nil, nil, fmt.Errorf("file too short (%d bytes) for v2 trace footer", size)
	}

	footerBuf := make([]byte, traceFooterSize)
	if _, rerr := f.ReadAt(footerBuf, size-int64(traceFooterSize)); rerr != nil {
		return nil, nil, fmt.Errorf("read footer: %w", rerr)
	}
	ft, ferr := DecodeTraceFooter(footerBuf)
	if ferr != nil {
		return nil, nil, fmt.Errorf("decode footer: %w", ferr)
	}

	// Overflow-safe bounds validation, mirroring readBucketFileTail's NOTE-VI-046 guard: check
	// each term against sz individually before summing, so a corrupt footer with huge
	// offsets/lengths cannot wrap uint64 and slip past the check.
	sz := uint64(size) //nolint:gosec // size is a validated non-negative length
	if ft.StringTableOff > sz || ft.StringTableLen > sz || ft.StringTableOff+ft.StringTableLen > sz ||
		ft.BlockIndexOff > sz || ft.BlockIndexLen > sz || ft.BlockIndexOff+ft.BlockIndexLen > sz {
		return nil, nil, fmt.Errorf("footer offsets out of bounds")
	}

	strBuf := make([]byte, ft.StringTableLen)
	if _, rerr := f.ReadAt(strBuf, int64(ft.StringTableOff)); rerr != nil { //nolint:gosec // bounds-checked above
		return nil, nil, fmt.Errorf("read string table: %w", rerr)
	}
	table, _, terr := DecodeStringTable(strBuf)
	if terr != nil {
		return nil, nil, fmt.Errorf("decode string table: %w", terr)
	}

	dirBuf := make([]byte, ft.BlockIndexLen)
	if _, rerr := f.ReadAt(dirBuf, int64(ft.BlockIndexOff)); rerr != nil { //nolint:gosec // bounds-checked above
		return nil, nil, fmt.Errorf("read block index: %w", rerr)
	}
	dir, derr := decodeTraceBlockIndex(dirBuf)
	if derr != nil {
		return nil, nil, fmt.Errorf("decode block index: %w", derr)
	}

	// Per-entry bounds validation: block bodies live in [headerLen, StringTableOff) by
	// construction (encodeTraceGroups writes body, then string table, then block index, then
	// footer), mirroring readBucketFileTail's identical guard for the BucketGroup format.
	strOff := ft.StringTableOff
	for i := range dir {
		d := &dir[i]
		if d.compOff > strOff || d.compLen > strOff || d.compOff+d.compLen > strOff {
			return nil, nil, fmt.Errorf(
				"block %d directory entry out of bounds (off=%d len=%d bodyRegion=%d)", i, d.compOff, d.compLen, strOff,
			)
		}
	}
	return dir, table, nil
}

// decodeBlockAt reads, decompresses, decodes, and (if a checker is configured) retention-
// filters block i. Returns (nil, nil) if the block decodes cleanly but every group's spans are
// dropped by filtering — an empty-but-not-erroneous result, distinct from a decode error.
func (it *diskTraceGroupFileIterator) decodeBlockAt(ctx context.Context, i int) ([]TraceGroup, error) {
	d := it.dir[i]
	raw := make([]byte, d.compLen)
	if _, err := it.f.ReadAt(raw, int64(d.compOff)); err != nil { //nolint:gosec // bounds-checked in readTraceFileMetadata
		return nil, fmt.Errorf("block %d: read compressed bytes: %w", i, err)
	}
	decompressed, err := snappy.Decode(nil, raw)
	if err != nil {
		return nil, fmt.Errorf("block %d: snappy decode: %w", i, err)
	}
	groups, err := decodeTraceBlockBody(decompressed, it.table)
	if err != nil {
		return nil, fmt.Errorf("block %d: decode payload: %w", i, err)
	}
	if it.checker == nil {
		it.stats.Retained += countTraceSpans(groups)
		return groups, nil
	}
	filtered, bstats, ferr := filterDeadSpansBlock(ctx, groups, it.checker, it.live)
	if ferr != nil {
		return nil, fmt.Errorf("block %d: filter dead spans: %w", i, ferr)
	}
	it.stats.Retained += bstats.Retained
	it.stats.Dropped += bstats.Dropped
	return filtered, nil
}

// countTraceSpans counts every SpanEntry across every group in groups — the per-block
// equivalent of countBlockRefs (disk_iterator.go), used when no RefChecker is configured so
// every span is counted as retained.
func countTraceSpans(groups []TraceGroup) int {
	n := 0
	for i := range groups {
		n += len(groups[i].Spans)
	}
	return n
}

// filterDeadSpansBlock returns a copy of groups with every SpanEntry whose SourceRef is
// confirmed deleted removed, or nil if every group becomes empty. live is a SourceRef->isLive
// cache the caller shares across every block of one file (SourceRef is already a resolved
// string at this point — see TraceGroupIterator's doc comment — so, unlike
// filterDeadRefsBlock's uint16 SourceID cache, no StringTable lookup is needed here at all).
func filterDeadSpansBlock(
	ctx context.Context,
	groups []TraceGroup,
	checker RefChecker,
	live map[string]bool,
) ([]TraceGroup, CompactStats, error) {
	var stats CompactStats
	out := make([]TraceGroup, 0, len(groups))
	for gi := range groups {
		g := &groups[gi]
		var spans []SpanEntry
		for si := range g.Spans {
			s := &g.Spans[si]
			isLive, ok := live[s.SourceRef]
			if !ok {
				l, err := checker.IsLive(ctx, s.SourceRef)
				if err != nil {
					return nil, CompactStats{}, fmt.Errorf(
						"valueindex: RefChecker.IsLive(%q): %w", s.SourceRef, err,
					)
				}
				isLive = l
				live[s.SourceRef] = l
			}
			if !isLive {
				stats.Dropped++
				continue
			}
			stats.Retained++
			spans = append(spans, *s)
		}
		if len(spans) > 0 {
			out = append(out, TraceGroup{TimeSec: g.TimeSec, TraceID: g.TraceID, Spans: spans})
		}
	}
	if len(out) == 0 {
		return nil, stats, nil
	}
	return out, stats, nil
}

// advanceBlock decodes forward from nextDirIdx until it finds a block with at least one group
// (after any retention filtering), or runs out of blocks (curBlock becomes nil: exhausted), or
// hits a decode error (it.err is set, curBlock becomes nil). It is a no-op if it.err is already
// set.
func (it *diskTraceGroupFileIterator) advanceBlock(ctx context.Context) {
	if it.err != nil {
		return
	}
	for it.nextDirIdx < len(it.dir) {
		idx := it.nextDirIdx
		it.nextDirIdx++
		groups, err := it.decodeBlockAt(ctx, idx)
		if err != nil {
			it.err = err
			it.curBlock = nil
			return
		}
		if len(groups) > 0 {
			it.curBlock = groups
			it.groupIdx = 0
			return
		}
	}
	it.curBlock = nil
}

// Peek returns the current group without advancing, or (nil, false) if the iterator is
// exhausted OR errored.
func (it *diskTraceGroupFileIterator) Peek() (*TraceGroup, bool) {
	if it.err != nil || it.curBlock == nil {
		return nil, false
	}
	return &it.curBlock[it.groupIdx], true
}

// Advance moves to the next group, decoding the next block (and applying retention filtering)
// if the current block is exhausted. No-op on an already-errored or already-exhausted iterator.
func (it *diskTraceGroupFileIterator) Advance(ctx context.Context) {
	if it.err != nil || it.curBlock == nil {
		return
	}
	it.groupIdx++
	if it.groupIdx < len(it.curBlock) {
		return
	}
	it.advanceBlock(ctx)
}

// Err returns the first error encountered while decoding, or nil.
func (it *diskTraceGroupFileIterator) Err() error { return it.err }

// Stats returns the CompactStats accumulated across every block decoded so far. For a
// fully-consumed iterator this is the final per-file total.
func (it *diskTraceGroupFileIterator) Stats() CompactStats { return it.stats }

// Close releases the open file handle and removes the local temp file at path. Idempotent.
func (it *diskTraceGroupFileIterator) Close() error {
	if it.f == nil {
		return nil
	}
	f := it.f
	it.f = nil
	closeErr := f.Close()
	removeErr := os.Remove(it.path)
	if closeErr != nil {
		return closeErr
	}
	return removeErr
}
