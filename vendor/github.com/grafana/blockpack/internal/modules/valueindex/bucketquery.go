package valueindex

// NOTE: Any changes to this file must be reflected in the corresponding NOTES.md.
//
// bucketquery.go — query-side helpers for the v2 BucketGroup file format (NOTE-VI-043, #427).
//
// These are the pruning primitives the querier uses to skip work cheaply:
//   - DecodeBucketFooter reads only the fixed-size footer (file min/max time + offsets) so a
//     file can be pruned by time range without fetching the body.
//   - BucketBlock.MayContainValue tests the per-block value bloom (definite no / maybe yes).
//   - BucketFile.LookupValue returns the matching groups for an exact value within a time range.

import (
	"encoding/binary"
	"errors"
	"fmt"
)

// BucketFooter is the decoded fixed-size file footer.
type BucketFooter struct {
	BlockIndexOff  uint64
	BlockIndexLen  uint64
	StringTableOff uint64
	StringTableLen uint64
	MinTimeSec     uint64
	MaxTimeSec     uint64
	Version        uint8
}

// DecodeBucketFooter parses the fixed-size footer at the tail of data. The caller may pass
// only the last bucketFooterSize bytes of the file (e.g. from a ranged S3 GET) — file
// pruning by time range needs nothing else. A magic mismatch is reported as ErrNotBucketFile
// (data is too short or not a v2 file at all) — the same sentinel DecodeBucketFile uses for its
// own header/footer magic checks — so a ranged reader that only ever sees the footer (e.g.
// QueryBucketFileRanged, B-4/#488) can use this as its sole "not a bucket file, skip" signal
// (NOTE-VI-083: both branches below were widened to wrap ErrNotBucketFile as part of B-4; no
// existing caller checked the previous plain-error shape).
func DecodeBucketFooter(data []byte) (BucketFooter, error) {
	if len(data) < bucketFooterSize {
		return BucketFooter{}, fmt.Errorf("valueindex: footer too short (%d bytes): %w", len(data), ErrNotBucketFile)
	}
	f := data[len(data)-bucketFooterSize:]
	if binary.LittleEndian.Uint32(f[:4]) != BucketFileMagic {
		return BucketFooter{}, fmt.Errorf("valueindex: bad footer magic: %w", ErrNotBucketFile)
	}
	return BucketFooter{
		BlockIndexOff:  binary.LittleEndian.Uint64(f[4:12]),
		BlockIndexLen:  binary.LittleEndian.Uint64(f[12:20]),
		StringTableOff: binary.LittleEndian.Uint64(f[20:28]),
		StringTableLen: binary.LittleEndian.Uint64(f[28:36]),
		MinTimeSec:     binary.LittleEndian.Uint64(f[36:44]),
		MaxTimeSec:     binary.LittleEndian.Uint64(f[44:52]),
		Version:        f[52],
	}, nil
}

// OverlapsTimeRange reports whether the file's [MinTimeSec, MaxTimeSec] intersects the
// closed query window [minTS, maxTS]. Used to prune whole files before fetching the body.
func (ft BucketFooter) OverlapsTimeRange(minTS, maxTS uint64) bool {
	return ft.MaxTimeSec >= minTS && ft.MinTimeSec <= maxTS
}

// BucketFooterSize is the exported fixed footer byte size, for callers performing a ranged
// tail GET to fetch only the footer.
const BucketFooterSize = bucketFooterSize

// MayContainValue returns false only if value is definitely absent from the block (per the
// block's value bloom). A true result may be a false positive — the caller must still scan
// the block to confirm.
func (b *BucketBlock) MayContainValue(value []byte) bool {
	return TestValueBloom(b.Bloom, value)
}

// OverlapsTimeRange reports whether the block's time span intersects [minTS, maxTS].
func (b *BucketBlock) OverlapsTimeRange(minTS, maxTS uint64) bool {
	return b.MaxTimeSec >= minTS && b.MinTimeSec <= maxTS
}

// LookupValue returns every BucketGroup in the file whose CanonicalValue equals value and
// whose TimeSec falls in the closed range [minTS, maxTS]. Blocks are skipped via the value
// bloom and time-range metadata before scanning. The returned groups alias the file's data
// (no copy); callers must not mutate them.
func (f *BucketFile) LookupValue(value []byte, minTS, maxTS uint64) []*BucketGroup {
	var out []*BucketGroup
	for bi := range f.Blocks {
		b := &f.Blocks[bi]
		if !b.OverlapsTimeRange(minTS, maxTS) {
			continue
		}
		if !b.MayContainValue(value) {
			continue
		}
		for gi := range b.Groups {
			g := &b.Groups[gi]
			if g.TimeSec < minTS || g.TimeSec > maxTS {
				continue
			}
			if compareCanonicalBytes(g.CanonicalValue, value) == 0 {
				out = append(out, g)
			}
		}
	}
	return out
}

// QueryBucketFiles evaluates pred against each v2 BucketGroup file and returns every matching
// span as a LookupResult (NOTE-VI-045, issue #429). A nil predicate matches every value
// (match-all). timeRange, if non-nil, is an inclusive [minSec, maxSec] filter applied at the
// block, group, and file level. Each SpanRef span index yields one LookupResult carrying the
// resolved SourceRef path, page-addressed BlockRef, trace id, and row index.
//
// This is the BucketGroup analog of QueryFiles: it lets the querier read the v2 write-path
// output without the flat VINX reader.
func QueryBucketFiles(pred Predicate, timeRange *[2]uint64, files ...[]byte) ([]LookupResult, error) {
	minTS, maxTS := uint64(0), ^uint64(0)
	if timeRange != nil {
		minTS, maxTS = timeRange[0], timeRange[1]
	}
	var out []LookupResult
	for _, data := range files {
		f, err := DecodeBucketFile(data)
		if err != nil {
			if errors.Is(err, ErrNotBucketFile) {
				// Not a v2 BucketGroup file at all (bad magic) — e.g. a stray
				// old-format or non-value-index object sharing the prefix. It holds
				// no v2 postings, so skipping it cannot under-count results. Safe to
				// skip and continue.
				continue
			}
			// A genuine decode failure on a real v2 file (corruption past the magic:
			// bad offsets, truncated block index, snappy failure, block-body
			// overrun). Silently skipping it would drop this file's postings from a
			// result the querier treats as authoritative coverage (NOTE-VI-033),
			// silently under-counting — the exact silent-partial-result bug the
			// trace-by-id review caught (NOTE-VI-046). Surface the error so the
			// caller falls back to a full scan rather than return a wrong result.
			return nil, fmt.Errorf("valueindex: query bucket file: %w", err)
		}
		for bi := range f.Blocks {
			b := &f.Blocks[bi]
			if !b.OverlapsTimeRange(minTS, maxTS) {
				continue
			}
			out = append(out, matchGroupsInBlock(b, f.StringTable, pred, minTS, maxTS)...)
		}
	}
	return out, nil
}

// NOTE-VI-081: matchGroupsInBlock scans b's groups for those matching pred within the closed
// time range [minTS, maxTS], flattening every SpanRef span index into a LookupResult. Extracted
// from QueryBucketFiles' inner loop and shared with QueryBucketFileRanged (bucketquery_ranged.go,
// B-4/#488) so the two read paths cannot silently diverge on group-level matching semantics.
func matchGroupsInBlock(b *BucketBlock, table *StringTable, pred Predicate, minTS, maxTS uint64) []LookupResult {
	var out []LookupResult
	for gi := range b.Groups {
		g := &b.Groups[gi]
		if g.TimeSec < minTS || g.TimeSec > maxTS {
			continue
		}
		if pred != nil && !pred.Match(g.CanonicalValue) {
			continue
		}
		for ri := range g.Refs {
			r := &g.Refs[ri]
			src := table.Lookup(r.SourceID)
			for si := range r.Spans {
				s := &r.Spans[si]
				for _, idx := range s.SpanIndexes {
					out = append(out, LookupResult{
						SourceRef: src,
						TimeSec:   g.TimeSec,
						BlockRef:  r.Ref,
						TraceID:   s.TraceID,
						RowIdx:    idx,
					})
				}
			}
		}
	}
	return out
}
