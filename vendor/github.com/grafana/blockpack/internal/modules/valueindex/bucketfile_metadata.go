package valueindex

// NOTE: Any changes to this file must be reflected in the corresponding NOTES.md.
//
// bucketfile_metadata.go — store-agnostic eager metadata decode for a v2 BucketGroup file
// (issue #488, B-2). ReadBucketFileMetadata reads and decodes a file's footer, string table,
// and block directory from any RangedSource — a local *os.File (disk_iterator.go's compaction
// path) or a per-key object-storage adapter (vibuilder's ranged query path) — so both consume
// one binary-format parser instead of two independently-maintained copies of the same offset
// arithmetic.

import "fmt"

// RangedSource is the minimal object-storage read surface a v2 BucketGroup file needs for
// partial (ranged) decoding: byte length and arbitrary-offset reads (SPEC-VI-7). Both a local
// *os.File (compaction's disk_iterator.go) and vibuilder's FileStore (adapted per-key) satisfy
// this.
type RangedSource interface {
	// Size returns the total byte length of the underlying file/object.
	Size() (int64, error)
	// ReadAt fills p from the source starting at off, following io.ReaderAt semantics (a
	// short read returns a non-nil error; EOF is io.EOF).
	ReadAt(p []byte, off int64) (int, error)
}

// ReadBucketFileMetadata reads and decodes src's footer, string table, and block directory —
// the eager, bounded metadata a ranged reader needs before any block can be addressed or the
// file pruned by time range (SPEC-VI-8). Shared by disk_iterator.go (compaction) and
// vibuilder's ranged query path (#488) so both consume one binary-format parser instead of two.
// src's header magic must already have been validated by the caller (this function only reads
// the tail of the file — footer, string table, block index — never the header).
func ReadBucketFileMetadata(src RangedSource) (BucketFooter, []BlockDirEntry, *StringTable, error) {
	size, footer, err := readBucketFileFooter(src)
	if err != nil {
		return BucketFooter{}, nil, nil, err
	}
	dir, table, err := readBucketFileTail(src, footer, size)
	if err != nil {
		return BucketFooter{}, nil, nil, err
	}
	return footer, dir, table, nil
}

// SPEC-VI-8: readBucketFileFooter reads and decodes src's fixed-size footer, returning it
// alongside src's total size (needed by readBucketFileTail's bounds check) so a caller that
// must inspect the footer before deciding whether to fetch the rest of the metadata at all
// (QueryBucketFileRanged's file-level time prune, B-4/#488) does not have to re-decode it
// afterward via readBucketFileTail — only ReadBucketFileMetadata's own combined call re-derives
// it, and does so exactly once.
func readBucketFileFooter(src RangedSource) (int64, BucketFooter, error) {
	size, err := src.Size()
	if err != nil {
		return 0, BucketFooter{}, fmt.Errorf("size: %w", err)
	}
	if size < int64(bucketFooterSize) {
		// Too short to hold a footer at all — not a bucket file. Wrapped with
		// ErrNotBucketFile for the same reason DecodeBucketFooter wraps its own too-short
		// case: a caller like QueryBucketFileRanged that only has a RangedSource (no prior
		// header-magic check) needs a single sentinel to recognize "skip, don't abort."
		return 0, BucketFooter{}, fmt.Errorf("file too short (%d bytes): %w", size, ErrNotBucketFile)
	}

	footerBuf := make([]byte, bucketFooterSize)
	if _, rerr := src.ReadAt(footerBuf, size-int64(bucketFooterSize)); rerr != nil {
		return 0, BucketFooter{}, fmt.Errorf("read footer: %w", rerr)
	}
	footer, err := DecodeBucketFooter(footerBuf)
	if err != nil {
		return 0, BucketFooter{}, fmt.Errorf("decode footer: %w", err)
	}
	return size, footer, nil
}

// SPEC-VI-8: readBucketFileTail reads and decodes the string table and block directory given
// an already-decoded footer and the file's total size. Split out from ReadBucketFileMetadata so
// a caller that decoded the footer itself first (to avoid an unconditional metadata fetch when
// a cheaper file-level prune already rules the file out) can reuse this without a redundant
// footer read.
func readBucketFileTail(src RangedSource, footer BucketFooter, size int64) ([]BlockDirEntry, *StringTable, error) {
	// size is a real file/object length (from RangedSource.Size, already validated >=
	// bucketFooterSize by readBucketFileFooter), never negative.
	//
	// Overflow-safe bounds validation: a corrupt footer can carry huge offsets/lengths whose
	// sum wraps uint64 and slips past a naive `off+len > size` check, then hands a negative
	// offset to ReadAt below. Checking each term against sz first (an offset/length
	// individually cannot legitimately exceed the file size) makes the subsequent sum
	// overflow-free — the same pattern DecodeBucketFile already uses for its own footer-offset
	// validation (bucketfile.go:DecodeBucketFile, NOTE-VI-046). This path is reachable from
	// QueryBucketFileRanged on object-storage bucket files fetched over the network, not only
	// from disk_iterator.go's trusted local compaction temp files, so it must not be weaker
	// than DecodeBucketFile's whole-file check.
	sz := uint64(size) //nolint:gosec // size is a validated non-negative length
	if footer.StringTableOff > sz || footer.StringTableLen > sz || footer.StringTableOff+footer.StringTableLen > sz ||
		footer.BlockIndexOff > sz || footer.BlockIndexLen > sz || footer.BlockIndexOff+footer.BlockIndexLen > sz {
		return nil, nil, fmt.Errorf("footer offsets out of bounds")
	}

	strBuf := make([]byte, footer.StringTableLen)
	if _, rerr := src.ReadAt(strBuf, int64(footer.StringTableOff)); rerr != nil { //nolint:gosec // bounds-checked above
		return nil, nil, fmt.Errorf("read string table: %w", rerr)
	}
	table, _, err := DecodeStringTable(strBuf)
	if err != nil {
		return nil, nil, fmt.Errorf("decode string table: %w", err)
	}

	dirBuf := make([]byte, footer.BlockIndexLen)
	if _, rerr := src.ReadAt(dirBuf, int64(footer.BlockIndexOff)); rerr != nil { //nolint:gosec // bounds-checked above
		return nil, nil, fmt.Errorf("read block index: %w", rerr)
	}
	dir, err := decodeBlockIndex(dirBuf)
	if err != nil {
		return nil, nil, fmt.Errorf("decode block index: %w", err)
	}

	// Per-entry bounds validation: a corrupted or truncated directory entry can carry an
	// arbitrary CompOff/CompLen (up to 2^64-1), which would otherwise drive an unbounded
	// make([]byte, CompLen) allocation and an out-of-range ReadAt at the block-body read sites
	// (bucketquery_ranged.go's readAndDecodeBlockRanged, disk_iterator.go's decodeBlockAt)
	// before any I/O is attempted there — a possible large-allocation DoS vector, in particular
	// on the ranged (S3) path's network-exposed entry point. Block bodies always live in
	// [headerLen, StringTableOff) by construction (bucketfile.go's assembleBucketFileBytes/
	// writeBucketFileTail write body, then string table, then block index, then footer), so
	// StringTableOff is the same "region that can legally contain block bodies" bound
	// DecodeBucketFile already checks against (bucketfile.go:DecodeBucketFile: `end > strOff`,
	// NOTE-VI-046). Validated once, here, for every directory entry, so both consumers only
	// ever see already-validated entries and need no bounds check of their own.
	strOff := footer.StringTableOff
	for i := range dir {
		d := &dir[i]
		if d.CompOff > strOff || d.CompLen > strOff || d.CompOff+d.CompLen > strOff {
			return nil, nil, fmt.Errorf(
				"block %d directory entry out of bounds (off=%d len=%d bodyRegion=%d)", i, d.CompOff, d.CompLen, strOff,
			)
		}
	}
	return dir, table, nil
}
