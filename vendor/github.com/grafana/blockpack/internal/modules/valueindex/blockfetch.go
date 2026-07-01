package valueindex

// NOTE: Any changes to this file must be reflected in the corresponding NOTES.md.
//
// blockfetch.go — v2 querier direct block fetch (NOTE-VI-044, issue #424).
//
// A value-index hit is a LookupResult carrying (SourceRef, BlockRef, TraceID).
// BlockRef is a page-addressed reference (NOTE-VI-027) that names the exact byte
// range of one inner block in a v2 blockpack file. Because a v2 block is
// self-contained (all columns, including identity, live inside the block), a hit
// can be resolved with a single ranged GET — no TOC fetch is required.
//
// This file provides the pure I/O-planning half of the querier lookup path:
//
//  1. GroupHitsBySource — group LookupResults by SourceRef (one S3 object per file).
//  2. CoalesceBlockRefs — merge adjacent/overlapping BlockRef byte ranges within a
//     single file into as few ranged GETs as possible (the same waste/gap policy as
//     the reader's CoalesceBlocks, but operating on page-addressed refs).
//  3. FetchBlocks — drive a caller-supplied BlockFetcher over the coalesced ranges
//     and slice each response back into individual block byte slices keyed by BlockRef.
//
// Block decode + in-block traceID lookup is the caller's job (executor / reader):
// a v2 block decodes standalone from its raw bytes, and the caller binary-searches
// the trace:id column (sorted by (service.name, span.name, traceID)) to assemble spans.

import (
	"cmp"
	"fmt"
	"slices"

	"github.com/grafana/blockpack/internal/modules/blockio/shared"
)

// BlockRefRead is a single merged ranged read over one blockpack file. It covers a
// contiguous byte window [Offset, Offset+Length) that spans one or more BlockRefs.
// Refs lists the individual blocks folded into this read, in ascending offset order,
// so the caller can slice the response back into per-block byte slices.
type BlockRefRead struct {
	// Refs are the individual block references covered by this read, ascending by offset.
	Refs []BlockRef
	// Offset is the absolute byte offset of the merged read within the file.
	Offset int64
	// Length is the merged read length in bytes.
	Length int64
}

// SourceHits is the set of LookupResults belonging to a single SourceRef (one
// blockpack file), produced by GroupHitsBySource. Order of Results within a source
// is preserved from the input slice.
type SourceHits struct {
	SourceRef string
	Results   []LookupResult
}

// GroupHitsBySource partitions results by SourceRef. Groups are returned sorted by
// SourceRef for deterministic iteration; results within a group keep input order.
// LookupResults with an empty SourceRef are dropped (they cannot be fetched).
func GroupHitsBySource(results []LookupResult) []SourceHits {
	if len(results) == 0 {
		return nil
	}
	idx := make(map[string]int, len(results))
	var groups []SourceHits
	for _, r := range results {
		if r.SourceRef == "" {
			continue
		}
		gi, ok := idx[r.SourceRef]
		if !ok {
			gi = len(groups)
			idx[r.SourceRef] = gi
			groups = append(groups, SourceHits{SourceRef: r.SourceRef})
		}
		groups[gi].Results = append(groups[gi].Results, r)
	}
	slices.SortFunc(groups, func(a, b SourceHits) int { return cmp.Compare(a.SourceRef, b.SourceRef) })
	return groups
}

// CoalesceBlockRefs merges a set of page-addressed BlockRefs (from a single file)
// into as few ranged reads as possible under cfg. refs need not be sorted or unique;
// duplicate refs (same PageNum+LenPages) are collapsed into one.
//
// The merge policy mirrors reader.CoalesceBlocks: two runs merge when the inter-run
// gap is within cfg.MaxGapBytes, the wasted (unrequested) fraction of the merged
// window is within cfg.MaxWasteRatio, and the merged length is within cfg.MaxReadBytes
// (zero = no cap). A block larger than MaxReadBytes is still read whole on its own.
func CoalesceBlockRefs(refs []BlockRef, cfg shared.CoalesceConfig) []BlockRefRead {
	if len(refs) == 0 {
		return nil
	}

	// Sort by offset; drop exact duplicates so the same block is fetched once.
	sorted := make([]BlockRef, 0, len(refs))
	sorted = append(sorted, refs...)
	slices.SortFunc(sorted, func(a, b BlockRef) int {
		if c := cmp.Compare(a.PageNum, b.PageNum); c != 0 {
			return c
		}
		return cmp.Compare(a.LenPages, b.LenPages)
	})
	sorted = slices.CompactFunc(sorted, func(a, b BlockRef) bool { return a == b })

	var out []BlockRefRead
	cur := BlockRefRead{
		Refs:   []BlockRef{sorted[0]},
		Offset: sorted[0].ByteOffset(),
		Length: sorted[0].ByteLen(),
	}
	for _, r := range sorted[1:] {
		rOff := r.ByteOffset()
		rEnd := rOff + r.ByteLen()
		curEnd := cur.Offset + cur.Length

		mergedEnd := max(rEnd, curEnd)
		mergedLen := mergedEnd - cur.Offset
		usefulBytes := min(cur.Length+r.ByteLen(), mergedLen) // approx: may double-count overlap
		wasteBytes := mergedLen - usefulBytes
		wasteRatio := float64(0)
		if mergedLen > 0 {
			wasteRatio = float64(wasteBytes) / float64(mergedLen)
		}
		gap := rOff - curEnd
		if gap < 0 {
			gap = 0 // overlapping runs have no gap
		}

		canMerge := gap <= cfg.MaxGapBytes && wasteRatio <= cfg.MaxWasteRatio &&
			(cfg.MaxReadBytes <= 0 || mergedLen <= cfg.MaxReadBytes)

		if canMerge {
			cur.Length = mergedLen
			cur.Refs = append(cur.Refs, r)
		} else {
			out = append(out, cur)
			cur = BlockRefRead{
				Refs:   []BlockRef{r},
				Offset: rOff,
				Length: r.ByteLen(),
			}
		}
	}
	out = append(out, cur)
	return out
}

// BlockFetcher reads a byte range from one blockpack file. sourceRef is the object
// key (S3 path); the implementation issues a ranged GET
// `Range: bytes=offset-(offset+length-1)` and returns exactly length bytes. It is the
// read half of the querier's storage backend (tempo's S3 reader in production, an
// in-memory map in tests).
type BlockFetcher interface {
	FetchRange(sourceRef string, offset, length int64) ([]byte, error)
}

// FetchedBlock is one decoded-ready block: its raw (still block-encoded) bytes plus
// the BlockRef that named it, so the caller can associate it with its hits.
type FetchedBlock struct {
	Data []byte
	Ref  BlockRef
}

// FetchBlocks resolves the block bytes for one source's hits. It coalesces the hits'
// BlockRefs into ranged reads, drives fetcher over each merged range, and slices each
// response back into per-block byte slices.
//
// The returned blocks are in ascending-offset order and deduplicated by BlockRef (a
// block referenced by several spans is fetched and returned once). The caller decodes
// each block standalone and looks up its own hits by TraceID within it.
//
// A short read (fewer bytes than requested) is a hard error: it means the file is
// truncated or the BlockRef is out of range, either of which would silently drop spans.
func FetchBlocks(
	fetcher BlockFetcher,
	sourceRef string,
	hits []LookupResult,
	cfg shared.CoalesceConfig,
) ([]FetchedBlock, error) {
	if fetcher == nil {
		return nil, fmt.Errorf("valueindex: FetchBlocks: nil fetcher")
	}
	refs := make([]BlockRef, 0, len(hits))
	for _, h := range hits {
		// Zero BlockRef means a v1 hit (uses BlockID instead); not fetchable here.
		if h.BlockRef == (BlockRef{}) {
			continue
		}
		refs = append(refs, h.BlockRef)
	}
	if len(refs) == 0 {
		return nil, nil
	}

	reads := CoalesceBlockRefs(refs, cfg)
	out := make([]FetchedBlock, 0, len(refs))
	for _, rd := range reads {
		buf, err := fetcher.FetchRange(sourceRef, rd.Offset, rd.Length)
		if err != nil {
			return nil, fmt.Errorf("valueindex: FetchBlocks: fetch %s [%d:%d]: %w",
				sourceRef, rd.Offset, rd.Offset+rd.Length, err)
		}
		if int64(len(buf)) != rd.Length {
			return nil, fmt.Errorf("valueindex: FetchBlocks: short read %s [%d:%d]: got %d want %d",
				sourceRef, rd.Offset, rd.Offset+rd.Length, len(buf), rd.Length)
		}
		for _, ref := range rd.Refs {
			bOff := ref.ByteOffset() - rd.Offset
			bLen := ref.ByteLen()
			end := bOff + bLen
			if bOff < 0 || end > int64(len(buf)) {
				return nil, fmt.Errorf("valueindex: FetchBlocks: block slice [%d:%d] out of range for read len %d",
					bOff, end, len(buf))
			}
			// Copy into an independent allocation so the merged read buffer can be
			// released (no shared sub-slice aliases held by the caller).
			data := make([]byte, bLen)
			copy(data, buf[bOff:end])
			out = append(out, FetchedBlock{Ref: ref, Data: data})
		}
	}
	return out, nil
}
