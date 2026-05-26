// Package writer provides block building and serialization for blockpack files.
// NOTE: Sketch index build and serialization for per-block HLL + SketchBloom + TopK.
// SPEC-SK-16: HashForFuse(key) is called here at write time; must match query time.
//
// Column-major wire format (magic 0x534B5445 "SKTE" — bloom only, no CMS):
//
//	magic[4 LE] = 0x534B5445
//	num_blocks[4 LE]
//	num_columns[4 LE]
//
//	per column (sorted by name):
//	  col_name_len[2 LE] + col_name[N]
//	  presence_bytes[ceil(num_blocks/8)]        // 1 bit per block
//	  distinct_count[num_blocks × 4 LE uint32]  // HLL.Cardinality() per block, 0 absent
//	  topk_k[1] = 20
//	  per present block: topk_entry_count[1] + entries (fp[8 LE] + count[2 LE])
//	  bloom_size[2 LE] = 2048
//	  per present block: bloom_data[bloom_size]
//
// Old files written with magic 0x534B5444 ("SKTD") included CMS before bloom.
// Old files written with magic 0x534B5443 ("SKTC") used BinaryFuse8 in the last
// section. Readers that encounter the old magic degrade gracefully (no pruning).
package writer

import (
	"encoding/binary"
	"math"
	"sync"

	"github.com/grafana/blockpack/internal/modules/sketch"
)

const (
	// sketchSectionMagic is the magic for the bloom-only sketch section ("SKTE" — no CMS).
	// Old "SKTD" (0x534B5444) included CMS; old "SKTC" (0x534B5443) used fuse filters.
	// Readers encountering the old magic degrade gracefully.
	sketchSectionMagic     = uint32(0x534B5445) // "SKTE"
	sketchTimestampColName = "__timestamp__"
)

// colSketchPool and blockSketchSetPool reuse sketch accumulators across blocks.
// Pooling avoids per-block GC churn for HLL, TopK, and SketchBloom objects.
var (
	colSketchPool      sync.Pool
	blockSketchSetPool sync.Pool
)

// getColSketch returns a reset colSketch from the pool, or allocates a fresh one.
func getColSketch() *colSketch {
	if v := colSketchPool.Get(); v != nil {
		return v.(*colSketch)
	}
	return &colSketch{
		hll:   sketch.NewHyperLogLog(),
		topk:  sketch.NewTopK(),
		bloom: sketch.NewSketchBloom(),
	}
}

// putColSketch resets cs and returns it to the pool.
// cs must not be used after this call.
func putColSketch(cs *colSketch) {
	cs.hll.Reset()
	cs.topk.Reset()
	cs.bloom.Reset()
	colSketchPool.Put(cs)
}

// getBlockSketchSet returns a cleared blockSketchSet from the pool, or allocates a fresh one.
func getBlockSketchSet() blockSketchSet {
	if v := blockSketchSetPool.Get(); v != nil {
		return v.(blockSketchSet)
	}
	return make(blockSketchSet, 64)
}

// releaseBlockSketchSet returns all colSketches and the map itself to their pools.
// bs must not be used after this call.
func releaseBlockSketchSet(bs blockSketchSet) {
	for k, cs := range bs {
		putColSketch(cs)
		delete(bs, k)
	}
	blockSketchSetPool.Put(bs)
}

// colSketch holds sketch accumulators for one column within one block.
// bloom is updated incrementally at add() time, eliminating the need to
// accumulate a keys slice for BinaryFuse8 construction.
type colSketch struct {
	hll   *sketch.HyperLogLog
	topk  *sketch.TopK
	bloom *sketch.SketchBloom
}

// blockSketchSet maps column name → colSketch for one block.
type blockSketchSet map[string]*colSketch

// newBlockSketchSet creates an empty blockSketchSet.
func newBlockSketchSet() blockSketchSet {
	return make(blockSketchSet, 64)
}

// add records one observed value (key string) for the named column.
// key is the wire-encoded string passed to updateMinMax (8-byte LE for numerics, raw string for strings).
// SPEC-SK-16: HashForFuse(key) is the canonical hash used at query time as well.
// fp is computed once and shared across HLL, TopK, and the bloom filter.
func (bs blockSketchSet) add(col, key string) {
	cs, ok := bs[col]
	if !ok {
		cs = getColSketch()
		bs[col] = cs
	}
	fp := sketch.HashForFuse(key)
	cs.hll.AddHash(fp)
	cs.topk.AddFP(fp)
	cs.bloom.Add(fp)
}

// writeOneColumnSketchBlob serializes sketch data for one column across all blocks.
// Returns nil, nil if the column has no sketch data in any block.
//
// Wire format (per-column, no name field):
//
//	num_blocks[4 LE]
//	presence[ceil(num_blocks/8) bytes]   // 1 bit per block; bit i set if block i has this column
//	distinct_count[num_blocks × 4 LE uint32]
//	topk_k[1] = 20
//	per present block: entry_count[1] + entries (fp[8 LE] + count[2 LE])
//	bloom_size[2 LE] = 2048
//	per present block: bloom_data[bloom_size]
func writeOneColumnSketchBlob(colName string, sketchIdx []blockSketchSet) ([]byte, error) {
	numBlocks := len(sketchIdx)
	if numBlocks == 0 {
		return nil, nil
	}
	// Check if any block has this column.
	hasData := false
	for _, bs := range sketchIdx {
		if _, ok := bs[colName]; ok {
			hasData = true
			break
		}
	}
	if !hasData {
		return nil, nil
	}

	presenceBytes := (numBlocks + 7) / 8
	buf := make(
		[]byte,
		0,
		4+presenceBytes+numBlocks*4+1+numBlocks*(1+sketch.TopKSize*10)+2+numBlocks*sketch.SketchBloomBytes,
	)

	// num_blocks[4 LE]
	var tmp4 [4]byte
	binary.LittleEndian.PutUint32(tmp4[:], uint32(numBlocks)) //nolint:gosec // safe: block count bounded
	buf = append(buf, tmp4[:]...)

	// Build presence bitset.
	presence := make([]byte, presenceBytes)
	for blockIdx, bs := range sketchIdx {
		if _, ok := bs[colName]; ok {
			presence[blockIdx/8] |= 1 << uint(blockIdx%8)
		}
	}
	buf = append(buf, presence...)

	// distinct_count[num_blocks × 4 LE uint32]
	for _, bs := range sketchIdx {
		cs := bs[colName]
		var card uint32
		if cs != nil {
			card = uint32(cs.hll.Cardinality()) //nolint:gosec
		}
		binary.LittleEndian.PutUint32(tmp4[:], card)
		buf = append(buf, tmp4[:]...)
	}

	// topk_k[1] = 20
	buf = append(buf, byte(sketch.TopKSize))

	// Per present block: topk_entry_count[1] + entries (fp[8 LE uint64] + count[2 LE uint16]).
	var tmp8 [8]byte
	var tmp2 [2]byte
	for _, bs := range sketchIdx {
		cs := bs[colName]
		if cs == nil {
			continue
		}
		entries := cs.topk.Entries()
		buf = append(buf, byte(len(entries))) //nolint:gosec // bounded by TopKSize (20)
		for _, e := range entries {
			binary.LittleEndian.PutUint64(tmp8[:], e.FP)
			buf = append(buf, tmp8[:]...)
			uint16Count := uint16(min(e.Count, math.MaxUint16)) //nolint:gosec
			binary.LittleEndian.PutUint16(tmp2[:], uint16Count)
			buf = append(buf, tmp2[:]...)
		}
	}

	// bloom_size[2 LE]
	binary.LittleEndian.PutUint16(tmp2[:], uint16(sketch.SketchBloomBytes)) //nolint:gosec
	buf = append(buf, tmp2[:]...)

	// Per present block: bloom_data[bloom_size].
	for _, bs := range sketchIdx {
		cs := bs[colName]
		if cs == nil {
			continue
		}
		buf = append(buf, cs.bloom.Marshal()...)
	}

	return buf, nil
}

// encodeSecondBucket converts a nanosecond timestamp to a 1-second bucket key.
// The bucket is floor(nanos / 1_000_000_000), stored as 8-byte LE uint64 string.
// This matches the encoding used by other uint64 columns in updateMinMax.
func encodeSecondBucket(nanos uint64) string {
	bucket := nanos / 1_000_000_000
	var b [8]byte
	binary.LittleEndian.PutUint64(b[:], bucket)
	return string(b[:])
}
