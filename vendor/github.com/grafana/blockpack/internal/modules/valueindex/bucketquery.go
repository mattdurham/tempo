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
// pruning by time range needs nothing else.
func DecodeBucketFooter(data []byte) (BucketFooter, error) {
	if len(data) < bucketFooterSize {
		return BucketFooter{}, fmt.Errorf("valueindex: footer too short (%d bytes)", len(data))
	}
	f := data[len(data)-bucketFooterSize:]
	if binary.LittleEndian.Uint32(f[:4]) != BucketFileMagic {
		return BucketFooter{}, fmt.Errorf("valueindex: bad footer magic")
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
