package writer

// NOTE: Any changes to this file must be reflected in the corresponding specs.md or NOTES.md.

import (
	"encoding/binary"
	"fmt"
	"math"
	"sync"

	"github.com/klauspost/compress/zstd"

	"github.com/grafana/blockpack/internal/modules/blockio/shared"
)

// vectorZstdEnc is a package-level zstd encoder used only by vectorF32ColumnBuilder.
var (
	vectorZstdEncOnce sync.Once
	vectorZstdEnc     *zstd.Encoder //nolint:gochecknoglobals
)

func getVectorZstdEncoder() *zstd.Encoder {
	vectorZstdEncOnce.Do(func() {
		var err error
		vectorZstdEnc, err = zstd.NewWriter(nil,
			zstd.WithEncoderLevel(zstd.SpeedDefault),
			zstd.WithEncoderConcurrency(1),
		)
		if err != nil {
			panic("writer: vector zstd.NewWriter: " + err.Error())
		}
	})
	return vectorZstdEnc
}

// vectorF32ColumnBuilder accumulates float32 vectors for one column.
// Wire format (buildData output):
//
//	enc_version[1]=2 + KindVectorF32[1] + dim[2 LE] + row_count[4 LE] +
//	presence_rle_len[4 LE] + presence_rle[N] +
//	float_data_len[4 LE] + zstd(flat_float32_LE[present_count * dim * 4])
//
// The header (enc_version through presence_rle) is uncompressed; only the float
// payload is zstd-compressed. This matches the pattern of all other column builders,
// where readColumnEncoding reads enc_version/kind uncompressed then dispatches.
// Absent rows contribute nothing to the flat_float32 payload (sparse presence).
//
// Indexed-write design: values[i] holds the float32 slice for row i, or nil if absent.
// This enables prepare(nRows) to pre-allocate the slice and setVectorAt(rowIdx, vec)
// to write directly by index, which is required because the embedding column is not
// written for every span (unlike string/int columns which are written sequentially).
type vectorF32ColumnBuilder struct {
	values [][]float32 // len == nRows; nil entry means absent
	dim    int         // set on first setVectorAt call
}

// setVectorAt marks row rowIdx as present with the given vector.
// If rowIdx >= len(values), the slice is extended with nil entries.
func (b *vectorF32ColumnBuilder) setVectorAt(rowIdx int, vec []float32) {
	if b.dim == 0 && len(vec) > 0 {
		b.dim = len(vec)
	}
	// Extend if needed (should not happen when prepare() is called first).
	for len(b.values) <= rowIdx {
		b.values = append(b.values, nil)
	}
	cp := make([]float32, len(vec))
	copy(cp, vec)
	b.values[rowIdx] = cp
}

// addVectorF32 appends a row (present or absent) sequentially.
// Used only in tests; production code uses setVectorAt via addVectorPresent.
func (b *vectorF32ColumnBuilder) addVectorF32(val []float32, present bool) {
	if present && len(val) > 0 && b.dim == 0 {
		b.dim = len(val)
	}
	if present {
		if b.dim > 0 && len(val) != b.dim {
			// Dimension mismatch: skip this row (absent) to avoid data corruption.
			b.values = append(b.values, nil)
			return
		}
		cp := make([]float32, len(val))
		copy(cp, val)
		b.values = append(b.values, cp)
	} else {
		b.values = append(b.values, nil)
	}
}

func (b *vectorF32ColumnBuilder) addString(_ string, _ bool)   {}
func (b *vectorF32ColumnBuilder) addInt64(_ int64, _ bool)     {}
func (b *vectorF32ColumnBuilder) addUint64(_ uint64, _ bool)   {}
func (b *vectorF32ColumnBuilder) addFloat64(_ float64, _ bool) {}
func (b *vectorF32ColumnBuilder) addBool(_ bool, _ bool)       {}
func (b *vectorF32ColumnBuilder) addBytes(_ []byte, _ bool)    {}

func (b *vectorF32ColumnBuilder) rowCount() int { return len(b.values) }

func (b *vectorF32ColumnBuilder) nullCount() int {
	n := 0
	for _, v := range b.values {
		if v == nil {
			n++
		}
	}
	return n
}

func (b *vectorF32ColumnBuilder) colType() shared.ColumnType { return shared.ColumnTypeVectorF32 }

func (b *vectorF32ColumnBuilder) buildData() ([]byte, error) {
	nRows := len(b.values)
	dim := b.dim

	// Build presence bitset.
	bitsetLen := (nRows + 7) / 8
	bitset := make([]byte, bitsetLen)
	for i, v := range b.values {
		if v != nil {
			bitset[i/8] |= 1 << uint(i%8)
		}
	}

	rleData, err := shared.EncodePresenceRLE(bitset, nRows)
	if err != nil {
		return nil, err
	}

	// Count present rows and build float32 payload.
	presentCount := 0
	for _, v := range b.values {
		if v != nil {
			presentCount++
		}
	}

	floatBuf := make([]byte, presentCount*dim*4)
	off := 0
	for _, vec := range b.values {
		if vec == nil {
			continue
		}
		for _, f := range vec {
			bits := math.Float32bits(f)
			binary.LittleEndian.PutUint32(floatBuf[off:], bits)
			off += 4
		}
	}

	compressed := getVectorZstdEncoder().EncodeAll(floatBuf, nil)
	if len(compressed) == 0 && len(floatBuf) > 0 {
		return nil, fmt.Errorf("vectorF32: zstd compress produced empty output")
	}

	// Assemble the column blob:
	// enc_version[1] + kind[1] + dim[2] + row_count[4] + rle_len[4] + rle_data
	// + float_data_len[4] + zstd(float_data)
	headerSize := 1 + 1 + 2 + 4 + 4
	buf := make([]byte, 0, headerSize+len(rleData)+4+len(compressed))
	buf = append(buf, shared.ColumnEncodingVersion, KindVectorF32)
	buf = append(buf, byte(dim), byte(dim>>8))      //nolint:gosec // safe: dim bounded by model spec (e.g. 768)
	buf = appendUint32LE(buf, uint32(nRows))        //nolint:gosec // safe: nRows bounded by block size
	buf = appendUint32LE(buf, uint32(len(rleData))) //nolint:gosec // safe: rleData bounded by block size
	buf = append(buf, rleData...)
	buf = appendUint32LE(buf, uint32(len(compressed))) //nolint:gosec // safe: compressed data bounded by block size
	buf = append(buf, compressed...)
	return buf, nil
}

func (b *vectorF32ColumnBuilder) resetForReuse(_ string) {
	b.values = b.values[:0]
	b.dim = 0
}

func (b *vectorF32ColumnBuilder) prepare(nRows int) {
	if cap(b.values) >= nRows {
		b.values = b.values[:nRows]
		clear(b.values)
	} else {
		b.values = make([][]float32, nRows)
	}
	b.dim = 0
}
