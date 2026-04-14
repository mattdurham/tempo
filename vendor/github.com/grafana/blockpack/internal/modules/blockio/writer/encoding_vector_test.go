package writer

import (
	"encoding/binary"
	"math"
	"testing"

	"github.com/klauspost/compress/zstd"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/grafana/blockpack/internal/modules/blockio/shared"
)

// makeTestVec creates a float32 slice with values [base, base+1, ..., base+dim-1].
func makeTestVec(dim int, base float32) []float32 {
	v := make([]float32, dim)
	for i := range dim {
		v[i] = base + float32(i)
	}
	return v
}

func decompressZstdBytes(t *testing.T, data []byte) []byte {
	t.Helper()
	dec, err := zstd.NewReader(nil)
	require.NoError(t, err)
	defer dec.Close()
	out, err := dec.DecodeAll(data, nil)
	require.NoError(t, err)
	return out
}

func TestVectorF32Builder_colType(t *testing.T) {
	b := &vectorF32ColumnBuilder{}
	assert.Equal(t, shared.ColumnTypeVectorF32, b.colType())
}

func TestVectorF32Builder_roundtrip(t *testing.T) {
	b := &vectorF32ColumnBuilder{}
	// 10 rows: rows 0,2,4,6,8 present; 1,3,5,7,9 absent
	for i := range 10 {
		present := i%2 == 0
		b.addVectorF32(makeTestVec(4, float32(i)), present)
	}
	assert.Equal(t, 10, b.rowCount())
	assert.Equal(t, 5, b.nullCount())

	data, err := b.buildData()
	require.NoError(t, err)
	require.NotEmpty(t, data)
}

func TestVectorF32Builder_allAbsent(t *testing.T) {
	b := &vectorF32ColumnBuilder{}
	for range 5 {
		b.addVectorF32(makeTestVec(8, 1.0), false)
	}
	assert.Equal(t, 5, b.rowCount())
	assert.Equal(t, 5, b.nullCount())

	data, err := b.buildData()
	require.NoError(t, err)
	require.NotEmpty(t, data)
}

func TestVectorF32Builder_singleRow(t *testing.T) {
	b := &vectorF32ColumnBuilder{}
	b.addVectorF32([]float32{1.0, 2.0, 3.0, 4.0}, true)
	assert.Equal(t, 1, b.rowCount())
	assert.Equal(t, 0, b.nullCount())

	data, err := b.buildData()
	require.NoError(t, err)
	require.NotEmpty(t, data)
}

func TestVectorF32Builder_resetForReuse(t *testing.T) {
	b := &vectorF32ColumnBuilder{}
	b.addVectorF32(makeTestVec(4, 1.0), true)
	b.addVectorF32(makeTestVec(4, 2.0), false)
	assert.Equal(t, 2, b.rowCount())

	b.resetForReuse("__embedding__")
	assert.Equal(t, 0, b.rowCount())

	// Can add new rows after reset.
	b.addVectorF32(makeTestVec(4, 5.0), true)
	assert.Equal(t, 1, b.rowCount())
}

func TestVectorF32Builder_prepare(t *testing.T) {
	b := &vectorF32ColumnBuilder{}
	b.prepare(7)
	assert.Equal(t, 7, b.rowCount())
	// All rows should be absent after prepare.
	assert.Equal(t, 7, b.nullCount())
}

func TestVectorF32Builder_encodingStructure(t *testing.T) {
	// Verify the wire format:
	// enc_version[1] + kind[1] + dim[2] + row_count[4] + rle_len[4] + rle_data
	// + float_data_len[4] + zstd(float32 values)
	const dim = 2

	b := &vectorF32ColumnBuilder{}
	b.addVectorF32([]float32{1.0, 2.0}, true)
	b.addVectorF32([]float32{3.0, 4.0}, false)
	b.addVectorF32([]float32{5.0, 6.0}, true)

	data, err := b.buildData()
	require.NoError(t, err)

	// Check uncompressed header (first 10 bytes before rle).
	require.GreaterOrEqual(t, len(data), 10, "data too short for header")
	assert.Equal(t, shared.ColumnEncodingVersion, data[0], "enc_version")
	assert.Equal(t, KindVectorF32, data[1], "kind")
	assert.Equal(t, uint16(dim), binary.LittleEndian.Uint16(data[2:4]), "dim")
	assert.Equal(t, uint32(3), binary.LittleEndian.Uint32(data[4:8]), "row_count")

	// Skip past rle_len + rle_data to get to float_data_len + zstd(floats).
	rleLen := int(binary.LittleEndian.Uint32(data[8:12]))
	floatDataOff := 12 + rleLen
	require.GreaterOrEqual(t, len(data), floatDataOff+4, "data too short for float_data_len")

	compressedLen := int(binary.LittleEndian.Uint32(data[floatDataOff : floatDataOff+4]))
	require.GreaterOrEqual(t, len(data), floatDataOff+4+compressedLen, "data too short for compressed floats")

	// Decompress the float payload and verify values.
	floats := decompressZstdBytes(t, data[floatDataOff+4:floatDataOff+4+compressedLen])
	require.GreaterOrEqual(t, len(floats), 2*dim*4, "decompressed float data too short")

	// Check first present row: [1.0, 2.0]
	v0 := math.Float32frombits(binary.LittleEndian.Uint32(floats[0:]))
	v1 := math.Float32frombits(binary.LittleEndian.Uint32(floats[4:]))
	assert.InDelta(t, 1.0, v0, 1e-6)
	assert.InDelta(t, 2.0, v1, 1e-6)
	// Check second present row: [5.0, 6.0]
	v2 := math.Float32frombits(binary.LittleEndian.Uint32(floats[8:]))
	v3 := math.Float32frombits(binary.LittleEndian.Uint32(floats[12:]))
	assert.InDelta(t, 5.0, v2, 1e-6)
	assert.InDelta(t, 6.0, v3, 1e-6)
}
