package valueindex

// NOTE: Any changes to this file must be reflected in the corresponding NOTES.md.
//
// bucketquery_ranged.go — the ranged (partial-read) query entry point for a v2 BucketGroup
// file (NOTE-VI-045 lineage, issue #488, B-4). QueryBucketFiles (bucketquery.go) requires the
// caller to have already downloaded the entire file into memory; QueryBucketFileRanged instead
// takes a RangedSource and issues only the ReadAt calls needed to prune and decode surviving
// blocks — footer first, then the block directory, then only the bodies of blocks that survive
// time- and value-range pruning. This is the read path vibuilder's lookupColumn/lookupColumnAll
// rewire onto (B-5) to avoid downloading whole value-index files just to discard most of their
// blocks.

import (
	"context"
	"errors"
	"fmt"

	"github.com/golang/snappy"
)

// QueryBucketFileRanged performs a ranged, partial read of one v2 BucketGroup file against
// pred/timeRange (SPEC-VI-10): footer ReadAt for file-level time pruning, block-directory
// ReadAt (via ReadBucketFileMetadata, B-2) for block-level time AND value-range pruning
// (blockExcludedByValue, B-3 — stronger than QueryBucketFiles' in-memory path, since it can
// skip a block's body read entirely rather than only skipping it after decoding), then
// block-body ReadAt only for surviving blocks. Group/predicate matching within a decoded block
// is delegated to matchGroupsInBlock — the same shared helper QueryBucketFiles calls — so the
// two read paths reproduce identical file/block/group semantics and cannot silently diverge.
//
// A footer-magic mismatch (data too short, or not a v2 file at all — surfaced as
// errors.Is(err, ErrNotBucketFile) from DecodeBucketFooter) is treated as "not a bucket file"
// and returns (nil, nil) — the per-file analog of QueryBucketFiles' ErrNotBucketFile-skip, not
// an error. Any other decode failure is a genuine error on a real v2 file and is returned, so
// the caller falls back to a full scan rather than silently under-count (NOTE-VI-115).
func QueryBucketFileRanged(
	ctx context.Context, src RangedSource, pred Predicate, timeRange *[2]uint64,
) ([]LookupResult, error) {
	// Checked before any read: a ctx already canceled when this function is entered (e.g. a
	// sibling goroutine's error canceled the shared errgroup ctx before this goroutine's turn)
	// must cost zero ReadAt calls, not pay for the footer read below first.
	if err := ctx.Err(); err != nil {
		return nil, fmt.Errorf("valueindex: QueryBucketFileRanged: %w", err)
	}

	minTS, maxTS := uint64(0), ^uint64(0)
	if timeRange != nil {
		minTS, maxTS = timeRange[0], timeRange[1]
	}

	size, footer, err := readBucketFileFooter(src)
	if err != nil {
		if errors.Is(err, ErrNotBucketFile) {
			return nil, nil //nolint:nilnil // explicit skip signal, documented above
		}
		return nil, fmt.Errorf("valueindex: QueryBucketFileRanged: footer: %w", err)
	}

	// File-level time prune: no further reads needed if the file's own time range doesn't
	// overlap the query window. This is why the footer is decoded here rather than via a single
	// call to ReadBucketFileMetadata (which always also fetches the string table and block
	// directory) — a file excluded by this check must cost exactly one ReadAt.
	//
	// SPEC-VI-4: minTS/maxTS are compared here exactly as received, with no internal
	// minute-flooring. TimeSec is floored at write time (root valueindex_extract.go); the
	// caller (ultimately tempo-mrd's nanoWindowToSec) is responsible for flooring an
	// externally-derived lower bound to the same alignment before it reaches this function —
	// QueryBucketFiles places the same responsibility on its caller (no flooring call exists
	// anywhere in this package), so this raw comparison is required for exact parity, not an
	// oversight.
	if !footer.OverlapsTimeRange(minTS, maxTS) {
		return nil, nil
	}

	dir, table, err := readBucketFileTail(src, footer, size)
	if err != nil {
		return nil, fmt.Errorf("valueindex: QueryBucketFileRanged: metadata: %w", err)
	}

	// Checked again after the metadata read (string table + block directory) and before the
	// per-block loop starts: a ctx canceled while those reads were in flight must stop here
	// rather than proceed into (and pay for) any block-body reads.
	if err := ctx.Err(); err != nil {
		return nil, fmt.Errorf("valueindex: QueryBucketFileRanged: %w", err)
	}

	var out []LookupResult
	for i := range dir {
		if ctx.Err() != nil {
			return nil, fmt.Errorf("valueindex: QueryBucketFileRanged: %w", ctx.Err())
		}
		d := &dir[i]
		// SPEC-VI-4: same raw, unfloored comparison as the file-level check above — the
		// algebraic negation of BucketBlock.OverlapsTimeRange, the identical check
		// QueryBucketFiles applies per block via matchGroupsInBlock's caller.
		if d.MaxTimeSec < minTS || d.MinTimeSec > maxTS {
			continue
		}
		if pred != nil && blockExcludedByValue(pred, d.MinValue, d.MaxValue) {
			continue
		}
		blk, berr := readAndDecodeBlockRanged(src, d)
		if berr != nil {
			return nil, fmt.Errorf("valueindex: QueryBucketFileRanged: block %d: %w", i, berr)
		}
		out = append(out, matchGroupsInBlock(blk, table, pred, minTS, maxTS)...)
	}
	return out, nil
}

// QueryBucketFileRangedNewestFirst mirrors QueryBucketFileRanged but iterates dir in REVERSE
// (newest block first -- SplitIntoBlocks/sortBucketBlock guarantee ascending on-disk order,
// SPEC-VI-1 amended) and stops once len(out) >= limit (limit <= 0 means unbounded, identical to
// the non-limited sibling). matchGroupsInBlockReverse consumes each surviving block's own
// group-level order in reverse too, so the combined result is globally newest-first, not merely
// block-level. The limit check happens once per BLOCK (after that block's own matches are fully
// appended), not per entry, so the returned slice is always an exact PREFIX of the full
// newest-first ordering with length >= limit -- it may overshoot within the block that first
// satisfies the limit, but never returns out-of-order or wrong-identity results. Added as a
// SIBLING function -- QueryBucketFileRanged itself is never modified, since every existing
// non-early-stopping caller must remain byte-identical.
func QueryBucketFileRangedNewestFirst(
	ctx context.Context, src RangedSource, pred Predicate, timeRange *[2]uint64, limit int,
) ([]LookupResult, error) {
	if err := ctx.Err(); err != nil {
		return nil, fmt.Errorf("valueindex: QueryBucketFileRangedNewestFirst: %w", err)
	}

	minTS, maxTS := uint64(0), ^uint64(0)
	if timeRange != nil {
		minTS, maxTS = timeRange[0], timeRange[1]
	}

	size, footer, err := readBucketFileFooter(src)
	if err != nil {
		if errors.Is(err, ErrNotBucketFile) {
			return nil, nil //nolint:nilnil // explicit skip signal, documented above
		}
		return nil, fmt.Errorf("valueindex: QueryBucketFileRangedNewestFirst: footer: %w", err)
	}

	if !footer.OverlapsTimeRange(minTS, maxTS) {
		return nil, nil
	}

	dir, table, err := readBucketFileTail(src, footer, size)
	if err != nil {
		return nil, fmt.Errorf("valueindex: QueryBucketFileRangedNewestFirst: metadata: %w", err)
	}

	if err := ctx.Err(); err != nil {
		return nil, fmt.Errorf("valueindex: QueryBucketFileRangedNewestFirst: %w", err)
	}

	var out []LookupResult
	for i := len(dir) - 1; i >= 0; i-- {
		if ctx.Err() != nil {
			return nil, fmt.Errorf("valueindex: QueryBucketFileRangedNewestFirst: %w", ctx.Err())
		}
		d := &dir[i]
		if d.MaxTimeSec < minTS || d.MinTimeSec > maxTS {
			continue
		}
		if pred != nil && blockExcludedByValue(pred, d.MinValue, d.MaxValue) {
			continue
		}
		blk, berr := readAndDecodeBlockRanged(src, d)
		if berr != nil {
			return nil, fmt.Errorf("valueindex: QueryBucketFileRangedNewestFirst: block %d: %w", i, berr)
		}
		out = append(out, matchGroupsInBlockReverse(blk, table, pred, minTS, maxTS)...)
		if limit > 0 && len(out) >= limit {
			return out, nil
		}
	}
	return out, nil
}

// SPEC-VI-10: readAndDecodeBlockRanged reads d's compressed body from src and decodes it into
// a *BucketBlock. Unlike QueryBucketFiles (which decodes every block from an already-fully-
// in-memory file), this issues exactly one ReadAt for d's byte range — the whole point of the
// ranged read path.
func readAndDecodeBlockRanged(src RangedSource, d *BlockDirEntry) (*BucketBlock, error) {
	// d.CompOff/CompLen were already bounds-checked against the string-table offset by
	// readBucketFileTail (bucketfile_metadata.go) when dir was decoded — this function never
	// sees an unvalidated entry (NOTE-VI-115).
	raw := make([]byte, d.CompLen)
	if _, err := src.ReadAt(raw, int64(d.CompOff)); err != nil { //nolint:gosec // bounds-checked by readBucketFileTail
		return nil, fmt.Errorf("read compressed bytes: %w", err)
	}
	decompressed, err := snappy.Decode(nil, raw)
	if err != nil {
		return nil, fmt.Errorf("snappy decode: %w", err)
	}
	blk, err := decodeBucketBlock(decompressed)
	if err != nil {
		return nil, fmt.Errorf("decode payload: %w", err)
	}
	return blk, nil
}
