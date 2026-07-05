package valueindex

// NOTE: Any changes to this file must be reflected in the corresponding NOTES.md.
//
// disk_iterator.go — a lazy, block-at-a-time GroupIterator over a BucketFile staged on local
// disk (plan.md Decision 2). Unlike BucketFileIterator (stream_compaction.go), which walks an
// already-fully-decoded in-memory *BucketFile, diskBucketFileIterator decodes only bounded
// metadata (header magic, footer, string table, block directory) eagerly at construction, and
// decodes one block's groups at a time — lazily, on Advance crossing a block boundary —
// bounding peak decoded memory per input file to one block regardless of file size.

import (
	"context"
	"encoding/binary"
	"fmt"
	"os"

	"github.com/golang/snappy"
)

// diskBucketFileIterator is the disk-backed GroupIterator implementation. See
// NewDiskBucketFileIterator for the construction/decode contract and the ownership/cleanup
// table in plan.md Decision 2.
type diskBucketFileIterator struct {
	checker    RefChecker
	err        error
	f          *os.File
	table      *StringTable
	live       map[uint16]bool // SourceID -> isLive cache, shared across every block of this file
	curBlock   *BucketBlock    // nil: no current block (not yet found one, or exhausted)
	path       string
	dir        []BlockDirEntry
	stats      CompactStats // retention-filter stats, accumulated across every block decoded so far
	nextDirIdx int          // index into dir of the next block to decode when curBlock is exhausted
	groupIdx   int          // position within curBlock.Groups
}

// var _ GroupIterator = (*diskBucketFileIterator)(nil) is a compile-time assertion that
// diskBucketFileIterator satisfies GroupIterator.
var _ GroupIterator = (*diskBucketFileIterator)(nil)

// NewDiskBucketFileIterator opens path and eagerly decodes its header magic, footer, string
// table, and block directory — all bounded/cheap regardless of file size (footer is fixed
// size, the string table is proportional to distinct interned source paths, the block
// directory is proportional to block count) — then eagerly decodes forward from block 0 until
// it finds a block with at least one (optionally retention-filtered) group, so Peek never
// needs to perform I/O or return an error.
//
// Returns (nil, nil) — an explicit untyped nil GroupIterator, never a typed-nil pointer
// wrapped in the interface — if the file fails only the header-magic check: this is the one
// signal treated as "legacy pre-v2 file, skip this file, do not abort the merge" (mirrors
// DecodeBucketFile's own first check). Any other decode failure (footer, string table, block
// index, or an individual block's decode/filter failure encountered while eagerly seeking the
// first live block) is a real error, returned here.
//
// Ownership/cleanup contract (plan.md Decision 2): on the (nil, nil) skip path or any other
// non-success return, this constructor closes the *os.File it opened (if it got that far) but
// never removes path — the caller retains ownership of path removal in both cases. Only on
// success (a live, non-nil GroupIterator returned) does ownership of both the fd and path
// transfer to the returned iterator's Close().
func NewDiskBucketFileIterator(ctx context.Context, path string, checker RefChecker) (GroupIterator, error) {
	f, err := os.Open(path) //nolint:gosec // G304: path is mergeLevel's own local temp file, not user input
	if err != nil {
		return nil, fmt.Errorf("valueindex: NewDiskBucketFileIterator: open %q: %w", path, err)
	}

	magic := make([]byte, 4)
	if _, rerr := f.ReadAt(magic, 0); rerr != nil {
		_ = f.Close()
		return nil, fmt.Errorf("valueindex: NewDiskBucketFileIterator: read header magic: %w", rerr)
	}
	if binary.LittleEndian.Uint32(magic) != BucketFileMagic {
		// Legacy pre-v2 file: skip, not abort. The caller (mergeLevel) still owns path.
		_ = f.Close()
		return nil, nil
	}

	dir, table, err := readBucketFileMetadata(f)
	if err != nil {
		_ = f.Close()
		return nil, fmt.Errorf("valueindex: NewDiskBucketFileIterator: %w", err)
	}

	it := &diskBucketFileIterator{
		f:       f,
		path:    path,
		table:   table,
		dir:     dir,
		checker: checker,
		live:    make(map[uint16]bool),
	}
	it.advanceBlock(ctx)
	if it.err != nil {
		_ = f.Close()
		return nil, fmt.Errorf("valueindex: NewDiskBucketFileIterator: %w", it.err)
	}
	return it, nil
}

// readBucketFileMetadata reads and decodes f's footer, string table, and block directory —
// the eager, bounded metadata NewDiskBucketFileIterator needs before any block can be
// addressed. f's header magic must already have been validated by the caller.
func readBucketFileMetadata(f *os.File) ([]BlockDirEntry, *StringTable, error) {
	fi, err := f.Stat()
	if err != nil {
		return nil, nil, fmt.Errorf("stat: %w", err)
	}
	size := fi.Size()
	if size < int64(bucketFooterSize) {
		return nil, nil, fmt.Errorf("file too short (%d bytes)", size)
	}

	footerBuf := make([]byte, bucketFooterSize)
	if _, rerr := f.ReadAt(footerBuf, size-int64(bucketFooterSize)); rerr != nil {
		return nil, nil, fmt.Errorf("read footer: %w", rerr)
	}
	footer, err := DecodeBucketFooter(footerBuf)
	if err != nil {
		return nil, nil, fmt.Errorf("decode footer: %w", err)
	}
	if footer.StringTableOff+footer.StringTableLen > uint64(size) ||
		footer.BlockIndexOff+footer.BlockIndexLen > uint64(size) {
		return nil, nil, fmt.Errorf("footer offsets out of bounds")
	}

	strBuf := make([]byte, footer.StringTableLen)
	if _, rerr := f.ReadAt(strBuf, int64(footer.StringTableOff)); rerr != nil { //nolint:gosec // bounds-checked above
		return nil, nil, fmt.Errorf("read string table: %w", rerr)
	}
	table, _, err := DecodeStringTable(strBuf)
	if err != nil {
		return nil, nil, fmt.Errorf("decode string table: %w", err)
	}

	dirBuf := make([]byte, footer.BlockIndexLen)
	if _, rerr := f.ReadAt(dirBuf, int64(footer.BlockIndexOff)); rerr != nil { //nolint:gosec // bounds-checked above
		return nil, nil, fmt.Errorf("read block index: %w", rerr)
	}
	dir, err := decodeBlockIndex(dirBuf)
	if err != nil {
		return nil, nil, fmt.Errorf("decode block index: %w", err)
	}
	return dir, table, nil
}

// decodeBlockAt reads, decompresses, decodes, and (if a checker is configured) retention-
// filters block i. Returns (nil, nil) if the block decodes cleanly but every group's refs are
// dropped by filtering — an empty-but-not-erroneous result, distinct from a decode error.
func (it *diskBucketFileIterator) decodeBlockAt(ctx context.Context, i int) (*BucketBlock, error) {
	d := it.dir[i]
	raw := make([]byte, d.CompLen)
	if _, err := it.f.ReadAt(raw, int64(d.CompOff)); err != nil { //nolint:gosec // CompOff/CompLen are file-derived
		return nil, fmt.Errorf("block %d: read compressed bytes: %w", i, err)
	}
	decompressed, err := snappy.Decode(nil, raw)
	if err != nil {
		return nil, fmt.Errorf("block %d: snappy decode: %w", i, err)
	}
	blk, err := decodeBucketBlock(decompressed)
	if err != nil {
		return nil, fmt.Errorf("block %d: decode payload: %w", i, err)
	}
	if it.checker == nil {
		// Mirrors DecodeFilteredBucketFile's nil-checker case (CompactStats{Retained:
		// countBucketRefs(f)}): with no retention checker configured, every ref in every
		// block is retained, none dropped.
		it.stats.Retained += countBlockRefs(blk)
		return blk, nil
	}
	filtered, bstats, ferr := filterDeadRefsBlock(ctx, blk, it.table, it.checker, it.live)
	if ferr != nil {
		return nil, fmt.Errorf("block %d: filter dead refs: %w", i, ferr)
	}
	it.stats.Retained += bstats.Retained
	it.stats.Dropped += bstats.Dropped
	return filtered, nil
}

// countBlockRefs counts every BlockRef across every group in b — the per-block equivalent of
// countBucketRefs (bucketmerge.go), used when no RefChecker is configured so all refs are
// counted as retained.
func countBlockRefs(b *BucketBlock) int {
	n := 0
	for gi := range b.Groups {
		n += len(b.Groups[gi].Refs)
	}
	return n
}

// advanceBlock decodes forward from nextDirIdx until it finds a block with at least one group
// (after any retention filtering), or runs out of blocks (curBlock becomes nil: exhausted), or
// hits a decode error (it.err is set, curBlock becomes nil). It is a no-op if it.err is already
// set (Edge Case 5 — an already-errored iterator does not attempt further I/O).
func (it *diskBucketFileIterator) advanceBlock(ctx context.Context) {
	if it.err != nil {
		return
	}
	for it.nextDirIdx < len(it.dir) {
		idx := it.nextDirIdx
		it.nextDirIdx++
		blk, err := it.decodeBlockAt(ctx, idx)
		if err != nil {
			it.err = err
			it.curBlock = nil
			return
		}
		if blk != nil && len(blk.Groups) > 0 {
			it.curBlock = blk
			it.groupIdx = 0
			return
		}
	}
	it.curBlock = nil
}

// Peek returns the current group without advancing, or (nil, false) if the iterator is
// exhausted OR errored (an errored iterator looks exhausted to Peek alone — callers must
// check Err() to distinguish the two, per GroupIterator's contract).
func (it *diskBucketFileIterator) Peek() (*BucketGroup, bool) {
	if it.err != nil || it.curBlock == nil {
		return nil, false
	}
	return &it.curBlock.Groups[it.groupIdx], true
}

// Advance moves to the next group, decoding the next block (and applying retention filtering)
// if the current block is exhausted. It is a no-op on an already-errored or already-exhausted
// iterator (SPEC-ROOT-001 / Edge Case 5 — no panic, no compounding errors, no further I/O).
func (it *diskBucketFileIterator) Advance(ctx context.Context) {
	if it.err != nil || it.curBlock == nil {
		return
	}
	it.groupIdx++
	if it.groupIdx < len(it.curBlock.Groups) {
		return
	}
	it.advanceBlock(ctx)
}

// StringTable returns the table needed to resolve this file's SourceID references. Never nil
// for a live (non-exhausted, non-errored) iterator — it is decoded eagerly at construction.
func (it *diskBucketFileIterator) StringTable() *StringTable { return it.table }

// Err returns the first error encountered while decoding, or nil.
func (it *diskBucketFileIterator) Err() error { return it.err }

// StatsProvider is implemented by GroupIterator implementations that track retention-filter
// statistics as they lazily decode (currently only diskBucketFileIterator — BucketFileIterator
// does its own filtering upfront via DecodeFilteredBucketFile, before construction, so it has
// no need to implement this). It is intentionally not part of GroupIterator itself: Stats() is
// never called by the merge algorithm (StreamCompactBucketFiles/mergeGroupsAtKey), only by a
// caller's own metrics bookkeeping (valueindexcompactor.mergeLevel, after fully consuming the
// iterator) — keeping it a separate, optional interface avoids widening GroupIterator's
// contract for a concern the merge algorithm itself has no stake in.
type StatsProvider interface {
	// Stats returns the CompactStats accumulated so far. For a fully-consumed iterator
	// (Advance called until Peek reports exhaustion) this is the final per-file total.
	Stats() CompactStats
}

// var _ StatsProvider = (*diskBucketFileIterator)(nil) is a compile-time assertion that
// diskBucketFileIterator satisfies StatsProvider.
var _ StatsProvider = (*diskBucketFileIterator)(nil)

// Stats returns the CompactStats accumulated across every block decoded so far.
func (it *diskBucketFileIterator) Stats() CompactStats { return it.stats }

// Close releases the open file handle and removes the local temp file at path. Idempotent —
// safe to call more than once. Per the ownership contract (plan.md Decision 2), Close is only
// ever called on a successfully-constructed iterator: it owns both the fd and path removal.
func (it *diskBucketFileIterator) Close() error {
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
