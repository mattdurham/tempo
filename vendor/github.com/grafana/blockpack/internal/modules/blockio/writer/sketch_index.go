// Package writer provides block building and serialization for blockpack files.
// NOTE: Sketch index build and serialization for per-block HLL + BinaryFuse8 + TopK.
// SPEC-SK-16: HashForFuse(key) is called here at write time; must match query time.
//
// Column-major wire format:
//
//	magic[4 LE] = 0x534B5445 ("SKTE")
//	num_blocks[4 LE]
//	num_columns[4 LE]
//
//	per column (sorted by name):
//	  col_name_len[2 LE] + col_name[N]
//	  presence_bytes[ceil(num_blocks/8)]        // 1 bit per block
//	  distinct_count[num_blocks × 4 LE uint32]  // HLL.Cardinality() per block, 0 absent
//	  topk_k[1] = 20
//	  per present block: topk_entry_count[1] + entries (fp[8 LE] + count[2 LE])
//	  per present block: fuse_len[4 LE] + fuse_data[fuse_len]
package writer

import (
	"encoding/binary"
	"fmt"
	"slices"

	"github.com/grafana/blockpack/internal/modules/sketch"
)

const (
	sketchSectionMagic     = uint32(0x534B5445) // "SKTE"
	sketchTimestampColName = "__timestamp__"
)

// colSketch holds sketch accumulators for one column within one block.
// keys holds all hashed values for BinaryFuse8 construction (flushed once at writeSketchIndexSection).
type colSketch struct {
	hll  *sketch.HyperLogLog
	topk *sketch.TopK
	keys []uint64 // hashed values for BinaryFuse8 construction
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
func (bs blockSketchSet) add(col, key string) {
	cs, ok := bs[col]
	if !ok {
		cs = &colSketch{
			hll:  sketch.NewHyperLogLog(),
			topk: sketch.NewTopK(),
			keys: make([]uint64, 0, 64),
		}
		bs[col] = cs
	}
	cs.hll.Add(key)
	cs.topk.Add(key)
	cs.keys = append(cs.keys, sketch.HashForFuse(key))
}

// writeSketchIndexSection serializes the sketch index for all blocks in column-major format.
// Returns (nil, nil) when sketchIdx is empty (no blocks).
func writeSketchIndexSection(sketchIdx []blockSketchSet) ([]byte, error) {
	numBlocks := len(sketchIdx)
	if numBlocks == 0 {
		return nil, nil
	}

	// Collect ALL unique column names across ALL blocks, sorted for determinism.
	colSet := make(map[string]struct{}, 64)
	for _, bs := range sketchIdx {
		for name := range bs {
			colSet[name] = struct{}{}
		}
	}
	colNames := make([]string, 0, len(colSet))
	for name := range colSet {
		colNames = append(colNames, name)
	}
	slices.Sort(colNames)
	numColumns := len(colNames)

	presenceBytes := (numBlocks + 7) / 8

	// Pre-allocate buffer: header + estimated per-column data.
	buf := make(
		[]byte,
		0,
		12+numColumns*(64+presenceBytes+numBlocks*4+1+numBlocks*(1+20*10)+numBlocks*16),
	)

	// magic[4 LE]
	var tmp4 [4]byte
	binary.LittleEndian.PutUint32(tmp4[:], sketchSectionMagic)
	buf = append(buf, tmp4[:]...)

	// num_blocks[4 LE]
	binary.LittleEndian.PutUint32(tmp4[:], uint32(numBlocks)) //nolint:gosec // safe: block count bounded
	buf = append(buf, tmp4[:]...)

	// num_columns[4 LE]
	binary.LittleEndian.PutUint32(tmp4[:], uint32(numColumns)) //nolint:gosec // safe: column count bounded
	buf = append(buf, tmp4[:]...)

	var tmp2 [2]byte

	for _, name := range colNames {
		// col_name_len[2 LE] + col_name[N]
		binary.LittleEndian.PutUint16(tmp2[:], uint16(len(name))) //nolint:gosec // safe: name length bounded
		buf = append(buf, tmp2[:]...)
		buf = append(buf, name...)

		// Build presence bitset: bit i = 1 if block i has this column.
		presence := make([]byte, presenceBytes)
		for blockIdx, bs := range sketchIdx {
			if _, ok := bs[name]; ok {
				wordIdx := blockIdx / 8
				bitIdx := uint(blockIdx % 8)
				presence[wordIdx] |= 1 << bitIdx
			}
		}
		buf = append(buf, presence...)

		// distinct_count[num_blocks × 4 LE uint32]: HLL.Cardinality() per block, 0 for absent.
		for _, bs := range sketchIdx {
			cs := bs[name]
			var card uint32
			if cs != nil {
				card = uint32(cs.hll.Cardinality()) //nolint:gosec // safe: cardinality fits uint32
			}
			binary.LittleEndian.PutUint32(tmp4[:], card)
			buf = append(buf, tmp4[:]...)
		}

		// topk_k[1] = 20
		buf = append(buf, byte(sketch.TopKSize))

		// Per present block: topk_entry_count[1] + entries (fp[8 LE uint64] + count[2 LE uint16]).
		var tmp8 [8]byte
		for _, bs := range sketchIdx {
			cs := bs[name]
			if cs == nil {
				continue
			}
			entries := cs.topk.Entries()
			buf = append(buf, byte(len(entries))) //nolint:gosec // safe: entries len bounded by TopKSize (20)
			for _, e := range entries {
				fp := sketch.HashForFuse(e.Key)
				binary.LittleEndian.PutUint64(tmp8[:], fp)
				buf = append(buf, tmp8[:]...)
				uint16Count := uint16(e.Count) //nolint:gosec
				binary.LittleEndian.PutUint16(tmp2[:], uint16Count)
				buf = append(buf, tmp2[:]...)
			}
		}

		// Per present block: fuse_len[4 LE] + fuse_data[fuse_len].
		for _, bs := range sketchIdx {
			cs := bs[name]
			if cs == nil {
				continue
			}
			fuse, err := sketch.NewBinaryFuse8(cs.keys)
			if err != nil {
				return nil, fmt.Errorf("sketch_index col %q: fuse8: %w", name, err)
			}
			fuseBytes, err := fuse.MarshalBinary()
			if err != nil {
				return nil, fmt.Errorf("sketch_index col %q: fuse8 marshal: %w", name, err)
			}
			binary.LittleEndian.PutUint32(tmp4[:], uint32(len(fuseBytes))) //nolint:gosec // safe: fuse size fits uint32
			buf = append(buf, tmp4[:]...)
			buf = append(buf, fuseBytes...)
		}
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
