package valueindex

// NOTE: Any changes to this file must be reflected in the corresponding NOTES.md.
//
// bucketmerge.go — compaction merge for the v2 BucketGroup file format (NOTE-VI-043, #427).
//
// Merging two value-index files merges their BucketGroups by (time_sec, value), unions the
// per-block Refs, deduplicates SpanRefs (by trace id, unioning span indexes), and rebuilds
// the bloom filter and min/max metadata. Each input file has its own string table, so a
// SourceID is meaningful only relative to its source file; the merge re-interns every
// SourceRef path into the output file's table and rewrites the ids.

import (
	"context"
	"encoding/binary"
	"fmt"
	"sort"
)

// MergeBucketFiles merges any number of BucketFiles into a single output file. All groups
// across all input blocks are merged into one logical group set keyed by (time_sec, value);
// the caller may subsequently repartition into multiple output blocks via SplitIntoBlocks.
//
// The output is a single-block file. Refs are unioned across inputs; SpanRefs with the same
// TraceID are merged (span indexes unioned and deduplicated); BlockRefs with the same
// (SourceRef, page) are merged. The output string table is freshly built so SourceIDs are
// dense and within the uint16 space; ErrStringTableOverflow is returned if more than
// MaxStringTableEntries distinct SourceRefs are referenced.
func MergeBucketFiles(inputs ...*BucketFile) (*BucketFile, error) {
	outTable := NewStringTable()

	// groupKey → merge accumulator. We key groups by (time_sec, value) and within a group
	// key refs by (sourceRef path, pageNum) so the same data block from two files unions.
	type spanAcc struct {
		idx     map[uint16]struct{}
		traceID [16]byte
	}
	type refAcc struct {
		spans      map[[16]byte]*spanAcc
		sourcePath string
		ref        BlockRef
	}
	type groupAcc struct {
		refs    map[string]*refAcc // key: sourcePath + "\x00" + page (via formatRefKey)
		value   []byte
		timeSec uint64
	}

	groups := make(map[string]*groupAcc)

	for _, f := range inputs {
		if f == nil {
			continue
		}
		for bi := range f.Blocks {
			b := &f.Blocks[bi]
			for gi := range b.Groups {
				g := &b.Groups[gi]
				gk := formatGroupKey(g.TimeSec, g.CanonicalValue)
				ga := groups[gk]
				if ga == nil {
					ga = &groupAcc{
						timeSec: g.TimeSec,
						value:   append([]byte(nil), g.CanonicalValue...),
						refs:    make(map[string]*refAcc),
					}
					groups[gk] = ga
				}
				for ri := range g.Refs {
					r := &g.Refs[ri]
					path := f.StringTable.Lookup(r.SourceID)
					rk := formatRefKey(path, r.Ref)
					ra := ga.refs[rk]
					if ra == nil {
						ra = &refAcc{
							sourcePath: path,
							ref:        r.Ref,
							spans:      make(map[[16]byte]*spanAcc),
						}
						ga.refs[rk] = ra
					}
					for si := range r.Spans {
						s := &r.Spans[si]
						sa := ra.spans[s.TraceID]
						if sa == nil {
							sa = &spanAcc{traceID: s.TraceID, idx: make(map[uint16]struct{})}
							ra.spans[s.TraceID] = sa
						}
						for _, idx := range s.SpanIndexes {
							sa.idx[idx] = struct{}{}
						}
					}
				}
			}
		}
	}

	// Materialize merged groups into a single block, interning source paths.
	out := &BucketBlock{Groups: make([]BucketGroup, 0, len(groups))}
	for _, ga := range groups {
		mg := BucketGroup{
			TimeSec:        ga.timeSec,
			CanonicalValue: ga.value,
			Refs:           make([]BucketBlockRef, 0, len(ga.refs)),
		}
		for _, ra := range ga.refs {
			id, ok := outTable.Intern(ra.sourcePath)
			if !ok {
				return nil, fmt.Errorf("valueindex: merge: %w", ErrStringTableOverflow)
			}
			br := BucketBlockRef{
				SourceID: id,
				Ref:      ra.ref,
				Spans:    make([]SpanRef, 0, len(ra.spans)),
			}
			for _, sa := range ra.spans {
				idxs := make([]uint16, 0, len(sa.idx))
				for idx := range sa.idx {
					idxs = append(idxs, idx)
				}
				sortUint16(idxs)
				br.Spans = append(br.Spans, SpanRef{TraceID: sa.traceID, SpanIndexes: idxs})
			}
			mg.Refs = append(mg.Refs, br)
		}
		out.Groups = append(out.Groups, mg)
	}

	sortBucketBlock(out)
	out.ComputeBlockMeta()

	f := &BucketFile{
		StringTable: outTable,
		Blocks:      []BucketBlock{*out},
	}
	if len(out.Groups) > 0 {
		f.MinTimeSec = out.MinTimeSec
		f.MaxTimeSec = out.MaxTimeSec
	}
	return f, nil
}

// SplitIntoBlocks repartitions a single-block file's groups into multiple blocks of at most
// groupsPerBlock groups each, preserving the (time_sec, value) sort order. Each output block
// gets its own recomputed metadata and bloom. A file with multiple blocks is collapsed to one
// first. groupsPerBlock <= 0 is a no-op.
func SplitIntoBlocks(f *BucketFile, groupsPerBlock int) {
	if f == nil || groupsPerBlock <= 0 {
		return
	}
	// Collapse to a single ordered group list.
	var all []BucketGroup
	for bi := range f.Blocks {
		all = append(all, f.Blocks[bi].Groups...)
	}
	sort.Slice(all, func(i, j int) bool {
		if all[i].TimeSec != all[j].TimeSec {
			return all[i].TimeSec < all[j].TimeSec
		}
		return compareCanonicalBytes(all[i].CanonicalValue, all[j].CanonicalValue) < 0
	})

	blocks := make([]BucketBlock, 0, (len(all)+groupsPerBlock-1)/groupsPerBlock)
	for start := 0; start < len(all); start += groupsPerBlock {
		end := start + groupsPerBlock
		if end > len(all) {
			end = len(all)
		}
		blk := BucketBlock{Groups: all[start:end]}
		blk.ComputeBlockMeta()
		blocks = append(blocks, blk)
	}
	f.Blocks = blocks
}

// filterDeadRefs returns a copy of f with every BucketBlockRef whose SourceRef is confirmed
// deleted removed. Groups and blocks that become empty are dropped. It counts retained vs
// dropped refs. The string table is rebuilt implicitly by MergeBucketFiles downstream, so
// here we only prune; SourceIDs remain valid against f.StringTable for the returned file.
//
// Implemented in terms of filterDeadRefsBlock, called once per block with one live-cache
// shared across the whole file — a behavior-preserving refactor (disk_iterator.go reuses
// filterDeadRefsBlock at per-block decode granularity for the disk-backed iterator; this
// function is unchanged in observable behavior, only in how it is composed).
func filterDeadRefs(ctx context.Context, f *BucketFile, checker RefChecker) (*BucketFile, CompactStats, error) {
	var stats CompactStats
	live := make(map[uint16]bool)
	out := &BucketFile{StringTable: f.StringTable}
	for bi := range f.Blocks {
		nb, bstats, err := filterDeadRefsBlock(ctx, &f.Blocks[bi], f.StringTable, checker, live)
		if err != nil {
			return nil, CompactStats{}, err
		}
		stats.Retained += bstats.Retained
		stats.Dropped += bstats.Dropped
		if nb != nil {
			out.Blocks = append(out.Blocks, *nb)
		}
	}
	if len(out.Blocks) > 0 {
		out.MinTimeSec = out.Blocks[0].MinTimeSec
		out.MaxTimeSec = out.Blocks[0].MaxTimeSec
		for bi := range out.Blocks {
			if out.Blocks[bi].MinTimeSec < out.MinTimeSec {
				out.MinTimeSec = out.Blocks[bi].MinTimeSec
			}
			if out.Blocks[bi].MaxTimeSec > out.MaxTimeSec {
				out.MaxTimeSec = out.Blocks[bi].MaxTimeSec
			}
		}
	}
	return out, stats, nil
}

// filterDeadRefsBlock returns a copy of b with every BucketBlockRef whose SourceRef is
// confirmed deleted removed, or nil if every group in b becomes empty. live is a
// SourceID->isLive cache the caller shares across every block of one file, so a SourceID
// repeated across blocks is probed via checker.IsLive at most once per file — the same
// cache-reuse behavior filterDeadRefs (whole-file) already had, now evaluated incrementally
// per block instead of over the whole file's blocks in one pass (Decision 2, plan.md).
func filterDeadRefsBlock(
	ctx context.Context,
	b *BucketBlock,
	table *StringTable,
	checker RefChecker,
	live map[uint16]bool,
) (*BucketBlock, CompactStats, error) {
	var stats CompactStats
	nb := BucketBlock{}
	for gi := range b.Groups {
		g := &b.Groups[gi]
		ng := BucketGroup{TimeSec: g.TimeSec, CanonicalValue: g.CanonicalValue}
		for ri := range g.Refs {
			r := &g.Refs[ri]
			isLive, ok := live[r.SourceID]
			if !ok {
				l, err := checker.IsLive(ctx, table.Lookup(r.SourceID))
				if err != nil {
					return nil, CompactStats{}, fmt.Errorf(
						"valueindex: RefChecker.IsLive(%q): %w", table.Lookup(r.SourceID), err,
					)
				}
				isLive = l
				live[r.SourceID] = l
			}
			if !isLive {
				stats.Dropped++
				continue
			}
			stats.Retained++
			ng.Refs = append(ng.Refs, *r)
		}
		if len(ng.Refs) > 0 {
			nb.Groups = append(nb.Groups, ng)
		}
	}
	if len(nb.Groups) == 0 {
		return nil, stats, nil
	}
	nb.ComputeBlockMeta()
	return &nb, stats, nil
}

// countBucketRefs counts total BucketBlockRefs across all groups (retained-count proxy when
// no RefChecker is configured).
func countBucketRefs(f *BucketFile) int {
	n := 0
	for bi := range f.Blocks {
		for gi := range f.Blocks[bi].Groups {
			n += len(f.Blocks[bi].Groups[gi].Refs)
		}
	}
	return n
}

func formatGroupKey(timeSec uint64, value []byte) string {
	// 8-byte big-endian time prefix keeps map keys unambiguous; value appended raw.
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], timeSec)
	return string(buf[:]) + string(value)
}

func formatRefKey(sourcePath string, ref BlockRef) string {
	// page_num[3 LE] + len_pages[2 LE] uniquely identifies a block within a source file.
	buf := AppendBlockRef(nil, ref)
	return sourcePath + "\x00" + string(buf)
}
