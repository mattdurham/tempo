package shared_test

import (
	"encoding/binary"
	"math/rand"
	"testing"

	"github.com/golang/snappy"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/grafana/blockpack/internal/modules/blockio/shared"
)

// ---- Presence RLE tests (RLE-01 through RLE-05) ----

// TestPresenceRLEAllPresent: all bits set → roundtrip → all bits set (RLE-01).
func TestPresenceRLEAllPresent(t *testing.T) {
	const nBits = 10
	bitset := allOnes(nBits)

	rle, err := shared.EncodePresenceRLE(bitset, nBits)
	require.NoError(t, err)

	decoded, err := shared.DecodePresenceRLE(rle, nBits)
	require.NoError(t, err)

	for i := range nBits {
		assert.True(t, shared.IsPresent(decoded, i), "bit %d should be set", i)
	}
}

// TestPresenceRLEAllAbsent: no bits set → roundtrip → no bits set (RLE-02).
func TestPresenceRLEAllAbsent(t *testing.T) {
	const nBits = 10
	bitset := make([]byte, (nBits+7)/8)

	rle, err := shared.EncodePresenceRLE(bitset, nBits)
	require.NoError(t, err)

	decoded, err := shared.DecodePresenceRLE(rle, nBits)
	require.NoError(t, err)

	for i := range nBits {
		assert.False(t, shared.IsPresent(decoded, i), "bit %d should be unset", i)
	}
}

// TestPresenceRLEAlternating: alternating bits → roundtrip correct (RLE-03).
func TestPresenceRLEAlternating(t *testing.T) {
	const nBits = 20
	bitset := make([]byte, (nBits+7)/8)
	for i := range nBits {
		if i%2 == 0 {
			bitset[i/8] |= 1 << uint(i%8) //nolint:gosec // safe: i%8 is always 0..7, fits in uint
		}
	}

	rle, err := shared.EncodePresenceRLE(bitset, nBits)
	require.NoError(t, err)

	decoded, err := shared.DecodePresenceRLE(rle, nBits)
	require.NoError(t, err)

	for i := range nBits {
		want := i%2 == 0
		got := shared.IsPresent(decoded, i)
		assert.Equal(t, want, got, "bit %d mismatch", i)
	}
}

// TestPresenceRLERandom: 1000 random bits → roundtrip correct.
func TestPresenceRLERandom(t *testing.T) {
	const nBits = 1000
	rng := rand.New(rand.NewSource(42)) //nolint:gosec // not used for cryptography
	bitset := make([]byte, (nBits+7)/8)
	want := make([]bool, nBits)
	for i := range nBits {
		if rng.Intn(2) == 1 {
			bitset[i/8] |= 1 << uint(i%8) //nolint:gosec // safe: i%8 is always 0..7, fits in uint
			want[i] = true
		}
	}

	rle, err := shared.EncodePresenceRLE(bitset, nBits)
	require.NoError(t, err)

	decoded, err := shared.DecodePresenceRLE(rle, nBits)
	require.NoError(t, err)

	for i := range nBits {
		assert.Equal(t, want[i], shared.IsPresent(decoded, i), "bit %d mismatch", i)
	}
}

// TestPresenceRLECountPresent: CountPresent on known bitset.
func TestPresenceRLECountPresent(t *testing.T) {
	const nBits = 8
	// Set bits 0, 2, 4, 6 → count = 4.
	bitset := []byte{0b01010101}
	assert.Equal(t, 4, shared.CountPresent(bitset, nBits))
	// All set.
	assert.Equal(t, 8, shared.CountPresent([]byte{0xFF}, 8))
	// None set.
	assert.Equal(t, 0, shared.CountPresent([]byte{0x00}, 8))
	// Partial last byte: nBits=5, byte=0xFF → bits 0..4 = 5 present.
	assert.Equal(t, 5, shared.CountPresent([]byte{0xFF}, 5))
}

// TestPresenceRLEIsPresent: IsPresent on individual bits.
func TestPresenceRLEIsPresent(t *testing.T) {
	// byte 0 = 0b00000101 → bits 0 and 2 set.
	bitset := []byte{0b00000101, 0b00000000}
	assert.True(t, shared.IsPresent(bitset, 0))
	assert.False(t, shared.IsPresent(bitset, 1))
	assert.True(t, shared.IsPresent(bitset, 2))
	assert.False(t, shared.IsPresent(bitset, 3))
	assert.False(t, shared.IsPresent(bitset, 8))
	// Out of bounds.
	assert.False(t, shared.IsPresent(bitset, 100))
}

// TestPresenceRLEEmpty: nBits=0 → no error, empty slice.
func TestPresenceRLEEmpty(t *testing.T) {
	rle, err := shared.EncodePresenceRLE(nil, 0)
	require.NoError(t, err)

	decoded, err := shared.DecodePresenceRLE(rle, 0)
	require.NoError(t, err)
	assert.Empty(t, decoded)
}

// TestPresenceRLEVersionCheck: corrupt version byte → error.
func TestPresenceRLEVersionCheck(t *testing.T) {
	const nBits = 5
	bitset := allOnes(nBits)
	rle, err := shared.EncodePresenceRLE(bitset, nBits)
	require.NoError(t, err)

	// Corrupt the version byte.
	rle[0] = 0xFF

	_, err = shared.DecodePresenceRLE(rle, nBits)
	assert.Error(t, err, "corrupted version byte must return error")
}

// ---- Index RLE tests ----

// TestIndexRLEAllSame: all same index → single run → roundtrip.
func TestIndexRLEAllSame(t *testing.T) {
	const n = 20
	indexes := make([]uint32, n)
	for i := range indexes {
		indexes[i] = 7
	}

	data, err := shared.EncodeIndexRLE(indexes)
	require.NoError(t, err)

	decoded, err := shared.DecodeIndexRLE(data, n)
	require.NoError(t, err)
	assert.Equal(t, indexes, decoded)
}

// TestIndexRLERandom: random uint32 slice → roundtrip.
func TestIndexRLERandom(t *testing.T) {
	rng := rand.New(rand.NewSource(99)) //nolint:gosec // not used for cryptography
	const n = 200
	indexes := make([]uint32, n)
	for i := range indexes {
		indexes[i] = uint32(rng.Intn(50)) //nolint:gosec // not used for cryptography
	}

	data, err := shared.EncodeIndexRLE(indexes)
	require.NoError(t, err)

	decoded, err := shared.DecodeIndexRLE(data, n)
	require.NoError(t, err)
	assert.Equal(t, indexes, decoded)
}

// TestIndexRLEEmpty: empty → roundtrip → empty.
func TestIndexRLEEmpty(t *testing.T) {
	data, err := shared.EncodeIndexRLE(nil)
	require.NoError(t, err)

	decoded, err := shared.DecodeIndexRLE(data, 0)
	require.NoError(t, err)
	assert.Empty(t, decoded)
}

// TestIndexRLEWrongCount: decoded count mismatch → error.
func TestIndexRLEWrongCount(t *testing.T) {
	indexes := []uint32{1, 1, 2, 2, 3}
	data, err := shared.EncodeIndexRLE(indexes)
	require.NoError(t, err)

	// Request more elements than encoded.
	_, err = shared.DecodeIndexRLE(data, 100)
	assert.Error(t, err, "count mismatch must return error")
}

// ---- CoalesceConfig tests ----

// TestAggressiveCoalesceConfig: MaxGapBytes=4MB, MaxWasteRatio=1.0.
func TestAggressiveCoalesceConfig(t *testing.T) {
	cfg := shared.AggressiveCoalesceConfig
	assert.Equal(t, int64(4*1024*1024), cfg.MaxGapBytes)
	assert.Equal(t, 1.0, cfg.MaxWasteRatio)
}

// ---- Trace ID bloom filter tests ----

// TestTraceIDBloomSize: size grows with trace count and stays within bounds.
func TestTraceIDBloomSize(t *testing.T) {
	assert.Equal(t, shared.TraceIDBloomMinBytes, shared.TraceIDBloomSize(0), "0 traces → min bytes")
	assert.Equal(t, shared.TraceIDBloomMinBytes, shared.TraceIDBloomSize(1), "1 trace → min bytes")

	size1K := shared.TraceIDBloomSize(1_000)
	size10K := shared.TraceIDBloomSize(10_000)
	assert.Greater(t, size10K, size1K, "10K traces needs more bytes than 1K")

	assert.LessOrEqual(t, shared.TraceIDBloomSize(100_000_000), shared.TraceIDBloomMaxBytes, "capped at max")
}

// TestTraceIDBloomNoFalseNegatives: every added trace ID must test true.
func TestTraceIDBloomNoFalseNegatives(t *testing.T) {
	const n = 200
	bloom := make([]byte, shared.TraceIDBloomSize(n))
	rng := rand.New(rand.NewSource(42)) //nolint:gosec // not used for cryptography
	ids := make([][16]byte, n)
	for i := range ids {
		_, _ = rng.Read(ids[i][:])
		shared.AddTraceIDToBloom(bloom, ids[i])
	}
	for i, id := range ids {
		assert.True(t, shared.TestTraceIDBloom(bloom, id), "trace %d must test true after insertion", i)
	}
}

// TestTraceIDBloomEmptyBloom: nil or zero-length bloom → vacuous true (no false negatives).
func TestTraceIDBloomEmptyBloom(t *testing.T) {
	var id [16]byte
	assert.True(t, shared.TestTraceIDBloom(nil, id), "nil bloom must return true")
	assert.True(t, shared.TestTraceIDBloom([]byte{}, id), "empty slice bloom must return true")
}

// TestTraceIDBloomZeroBloom: an all-zero, non-nil bloom must never report membership.
// This ensures no false positives when no trace IDs have been added.
func TestTraceIDBloomZeroBloom(t *testing.T) {
	const n = 100
	zeroBloom := make([]byte, shared.TraceIDBloomSize(n))
	rng := rand.New(rand.NewSource(7)) //nolint:gosec // not used for cryptography

	falsePositives := 0
	const trials = 10_000
	for range trials {
		var id [16]byte
		_, _ = rng.Read(id[:])
		if shared.TestTraceIDBloom(zeroBloom, id) {
			falsePositives++
		}
	}
	assert.Zero(t, falsePositives, "all-zero bloom must never return true")
}

// TestTraceIDBloomFalsePositiveRate: populate with N traces, test M absent IDs, FP rate reasonable.
func TestTraceIDBloomFalsePositiveRate(t *testing.T) {
	const n = 1000
	bloom := make([]byte, shared.TraceIDBloomSize(n))
	rng := rand.New(rand.NewSource(13)) //nolint:gosec // not used for cryptography

	// Insert n random trace IDs.
	inserted := make(map[[16]byte]struct{}, n)
	for range n {
		var id [16]byte
		_, _ = rng.Read(id[:])
		inserted[id] = struct{}{}
		shared.AddTraceIDToBloom(bloom, id)
	}

	// Test 10K random IDs not in the set; count only IDs actually tested (not in inserted).
	const trials = 10_000
	fps, tested := 0, 0
	for range trials {
		var id [16]byte
		_, _ = rng.Read(id[:])
		if _, in := inserted[id]; in {
			continue
		}
		tested++
		if shared.TestTraceIDBloom(bloom, id) {
			fps++
		}
	}
	fpRate := float64(fps) / float64(tested)
	// With 10 bits/trace and k=7, expected FP ≈ 0.8%. Allow up to 3% in tests.
	assert.Less(t, fpRate, 0.03, "false positive rate should be < 3%%, got %.2f%%", fpRate*100)
}

// ---- Intrinsic bloom size clamp tests (GAP-2) ----

// TestIntrinsicBloomSize_ClampBoundary verifies that IntrinsicBloomSize clamps output
// to 4096 bytes for large item counts and returns correct sizes below the clamp.
// Formula: b = (itemCount * IntrinsicPageBloomBitsPerItem + 7) / 8, clamped to [min, 4096].
func TestIntrinsicBloomSize_ClampBoundary(t *testing.T) {
	const bitsPerItem = shared.IntrinsicPageBloomBitsPerItem // 10

	// Compute the boundary itemCounts:
	// b = (n * 10 + 7) / 8
	// b = 4095 → n: (4095*8 - 7) / 10 = 32753/10 = 3275 → check: (3275*10+7)/8 = 32757/8 = 4094.6 → 4094
	// Try n = 3276: (3276*10+7)/8 = 32767/8 = 4095 bytes — just below clamp
	// Try n = 3277: (3277*10+7)/8 = 32777/8 = 4097 bytes — over limit → 4096

	cases := []struct {
		desc      string
		itemCount int
		wantSize  int
	}{
		{"just below clamp boundary", 3276, 4095},
		{"exactly at clamp boundary (first over)", 3277, 4096},
		{"well above clamp", 10_000, 4096},
		{"far above clamp", 1_000_000, 4096},
		{"zero items → min bytes", 0, shared.IntrinsicPageBloomMinBytes},
		{"1 item → min bytes (formula result < min)", 1, shared.IntrinsicPageBloomMinBytes},
	}

	// Validate formula for the boundary cases manually.
	require.Equal(t, 4095, (3276*bitsPerItem+7)/8, "formula check for 3276 items")
	require.Equal(t, 4097, (3277*bitsPerItem+7)/8, "formula check for 3277 items (pre-clamp)")

	for _, tc := range cases {
		t.Run(tc.desc, func(t *testing.T) {
			got := shared.IntrinsicBloomSize(tc.itemCount)
			assert.Equal(t, tc.wantSize, got,
				"IntrinsicBloomSize(%d): want %d, got %d", tc.itemCount, tc.wantSize, got)
		})
	}
}

// TestIntrinsicBloomSize_ConstantCheck verifies IntrinsicPageSize constant has not changed.
// GAP-4 supplement: ensure the page size boundary constant is exactly 10_000.
func TestIntrinsicBloomSize_ConstantCheck(t *testing.T) {
	assert.Equal(t, 10_000, shared.IntrinsicPageSize,
		"IntrinsicPageSize constant must be 10_000 (format boundary, must not change silently)")
}

// ---- DecodeIntrinsicColumnBlob corrupt-bytes tests (GAP-12) ----

// snappyEncode is a helper that snappy-encodes raw bytes for use as a column blob.
func snappyEncode(raw []byte) []byte {
	return snappy.Encode(nil, raw)
}

// buildFlatUint64Blob builds a minimal valid v2-paged flat uint64 column blob with
// the given row count. Each value is 0; refs have BlockIdx=0, RowIdx=i.
// Uses EncodePageTOC and assembles the paged blob manually.
func buildFlatUint64Blob(rowCount int) []byte {
	// Build the flat page blob: values_len[4] + varints[...] + refs[rowCount×4]
	// All values are 0 (delta encoded as 0 varints), refs are (0, i).
	varintBuf := make([]byte, 0, rowCount) // delta=0 encodes as 1 byte per row
	var varintTmp [10]byte
	var prev uint64
	for range rowCount {
		n := binary.PutUvarint(varintTmp[:], 0-prev) // delta=0
		varintBuf = append(varintBuf, varintTmp[:n]...)
		prev = 0
	}
	// pageBuf: values_len[4] + varints + refs[rowCount×4 bytes]
	pageBuf := make([]byte, 0, 4+len(varintBuf)+rowCount*4)
	var tmp4 [4]byte
	binary.LittleEndian.PutUint32(tmp4[:], uint32(len(varintBuf))) //nolint:gosec
	pageBuf = append(pageBuf, tmp4[:]...)
	pageBuf = append(pageBuf, varintBuf...)
	var tmp2 [2]byte
	for i := range rowCount {
		binary.LittleEndian.PutUint16(tmp2[:], 0) // blockIdx=0
		pageBuf = append(pageBuf, tmp2[:]...)
		binary.LittleEndian.PutUint16(tmp2[:], uint16(i)) //nolint:gosec
		pageBuf = append(pageBuf, tmp2[:]...)
	}
	pageBlob := snappy.Encode(nil, pageBuf)

	// Build TOC.
	toc := shared.PagedIntrinsicTOC{
		Pages: []shared.PageMeta{{
			Offset:   0,
			Length:   uint32(len(pageBlob)), //nolint:gosec
			RowCount: uint32(rowCount),      //nolint:gosec
		}},
		BlockIdxWidth: 2,
		RowIdxWidth:   2,
		Format:        shared.IntrinsicFormatFlat,
		ColType:       shared.ColumnTypeUint64,
	}
	tocBlob, _ := shared.EncodePageTOC(toc)

	// Assemble: sentinel[1] + toc_len[4] + toc_blob + page_blob.
	out := make([]byte, 0, 1+4+len(tocBlob)+len(pageBlob))
	out = append(out, shared.IntrinsicPagedVersion)
	binary.LittleEndian.PutUint32(tmp4[:], uint32(len(tocBlob))) //nolint:gosec
	out = append(out, tmp4[:]...)
	out = append(out, tocBlob...)
	out = append(out, pageBlob...)
	return out
}

// TestDecodeIntrinsicColumnBlob_Corrupt verifies that DecodeIntrinsicColumnBlob returns
// errors (not panics) for a range of corrupt or truncated input blobs. (GAP-12)
func TestDecodeIntrinsicColumnBlob_Corrupt(t *testing.T) {
	t.Run("invalid snappy encoding", func(t *testing.T) {
		// Random bytes are almost never valid snappy.
		_, err := shared.DecodeIntrinsicColumnBlob([]byte{0x00, 0x01, 0x02, 0x03, 0xFF, 0xFE})
		assert.Error(t, err, "invalid snappy bytes must return error")
	})

	t.Run("too short after snappy decode", func(t *testing.T) {
		// Snappy-encode just 2 bytes — decoder expects at least 3.
		blob := snappyEncode([]byte{0x01, 0x01})
		_, err := shared.DecodeIntrinsicColumnBlob(blob)
		assert.Error(t, err, "2-byte raw blob must return 'too short' error")
	})

	t.Run("flat column truncated before row_count", func(t *testing.T) {
		// 3 header bytes but no row_count field.
		raw := []byte{
			shared.IntrinsicFormatVersion,
			shared.IntrinsicFormatFlat,
			byte(shared.ColumnTypeUint64),
		}
		blob := snappyEncode(raw)
		_, err := shared.DecodeIntrinsicColumnBlob(blob)
		assert.Error(t, err, "truncated before row_count must return error")
	})

	t.Run("flat column truncated before ref widths", func(t *testing.T) {
		// Header + row_count=1 but no ref-width bytes.
		raw := make([]byte, 7)
		raw[0] = shared.IntrinsicFormatVersion
		raw[1] = shared.IntrinsicFormatFlat
		raw[2] = byte(shared.ColumnTypeUint64)
		binary.LittleEndian.PutUint32(raw[3:], 1) // row_count = 1
		blob := snappyEncode(raw)
		_, err := shared.DecodeIntrinsicColumnBlob(blob)
		assert.Error(t, err, "truncated before ref widths must return error")
	})

	t.Run("flat column truncated mid-values", func(t *testing.T) {
		// Header + row_count=2 + ref widths, but only 4 value bytes (need 16).
		raw := make([]byte, 3+4+2+4) // 4 bytes of values instead of 16
		raw[0] = shared.IntrinsicFormatVersion
		raw[1] = shared.IntrinsicFormatFlat
		raw[2] = byte(shared.ColumnTypeUint64)
		binary.LittleEndian.PutUint32(raw[3:], 2) // row_count = 2
		raw[7] = 1                                // blockW
		raw[8] = 1                                // rowW
		blob := snappyEncode(raw)
		_, err := shared.DecodeIntrinsicColumnBlob(blob)
		assert.Error(t, err, "truncated mid-values must return error")
	})

	t.Run("flat column truncated mid-refs", func(t *testing.T) {
		// Valid header + 1 value (8 bytes) but no ref bytes.
		raw := make([]byte, 3+4+2+8)
		raw[0] = shared.IntrinsicFormatVersion
		raw[1] = shared.IntrinsicFormatFlat
		raw[2] = byte(shared.ColumnTypeUint64)
		binary.LittleEndian.PutUint32(raw[3:], 1) // row_count = 1
		raw[7] = 1                                // blockW
		raw[8] = 1                                // rowW
		// values at [9:17] = 8 zero bytes, no ref bytes follow
		blob := snappyEncode(raw)
		_, err := shared.DecodeIntrinsicColumnBlob(blob)
		assert.Error(t, err, "truncated mid-refs must return error")
	})

	t.Run("dict column truncated before dict ref widths", func(t *testing.T) {
		// Header + value_count=1 but no ref-width bytes.
		raw := make([]byte, 7)
		raw[0] = shared.IntrinsicFormatVersion
		raw[1] = shared.IntrinsicFormatDict
		raw[2] = byte(shared.ColumnTypeString)
		binary.LittleEndian.PutUint32(raw[3:], 1) // value_count = 1
		blob := snappyEncode(raw)
		_, err := shared.DecodeIntrinsicColumnBlob(blob)
		assert.Error(t, err, "dict column truncated before ref widths must return error")
	})

	t.Run("dict column truncated mid value_len", func(t *testing.T) {
		// value_count=1, ref widths present, but no value_len bytes.
		raw := make([]byte, 9)
		raw[0] = shared.IntrinsicFormatVersion
		raw[1] = shared.IntrinsicFormatDict
		raw[2] = byte(shared.ColumnTypeString)
		binary.LittleEndian.PutUint32(raw[3:], 1) // value_count = 1
		raw[7] = 1                                // blockW
		raw[8] = 1                                // rowW
		// no value_len[2] follows
		blob := snappyEncode(raw)
		_, err := shared.DecodeIntrinsicColumnBlob(blob)
		assert.Error(t, err, "dict column truncated at value_len must return error")
	})

	t.Run("dict column truncated mid string value", func(t *testing.T) {
		// value_count=1, value_len=10 but only 3 bytes of value data present.
		raw := make([]byte, 9+2+3)
		raw[0] = shared.IntrinsicFormatVersion
		raw[1] = shared.IntrinsicFormatDict
		raw[2] = byte(shared.ColumnTypeString)
		binary.LittleEndian.PutUint32(raw[3:], 1)  // value_count = 1
		raw[7] = 1                                 // blockW
		raw[8] = 1                                 // rowW
		binary.LittleEndian.PutUint16(raw[9:], 10) // value_len = 10
		// only 3 bytes of value data at [11:14]
		blob := snappyEncode(raw)
		_, err := shared.DecodeIntrinsicColumnBlob(blob)
		assert.Error(t, err, "dict column truncated mid string value must return error")
	})

	t.Run("valid flat blob roundtrips cleanly", func(t *testing.T) {
		// Sanity check: a properly-formed blob decodes without error.
		blob := buildFlatUint64Blob(3)
		col, err := shared.DecodeIntrinsicColumnBlob(blob)
		require.NoError(t, err)
		require.NotNil(t, col)
		assert.Equal(t, 3, len(col.Uint64Values))
		assert.Equal(t, 3, len(col.BlockRefs))
	})

	t.Run("paged blob: bad toc_len exceeds remaining bytes", func(t *testing.T) {
		// v2 paged format: sentinel[1] + toc_len[4 LE] + toc_blob[toc_len] + pages
		// Set toc_len larger than remaining bytes.
		raw := make([]byte, 1+4)
		raw[0] = shared.IntrinsicPagedVersion
		binary.LittleEndian.PutUint32(raw[1:], 9999) // toc_len claims 9999 bytes but blob is only 5
		_, err := shared.DecodeIntrinsicColumnBlob(raw)
		assert.Error(t, err, "paged blob with toc_len exceeding size must return error")
	})
}

// ---- helpers ----

// allOnes returns a bitset with all nBits bits set.
func allOnes(nBits int) []byte {
	b := make([]byte, (nBits+7)/8)
	for i := range nBits {
		b[i/8] |= 1 << uint(i%8) //nolint:gosec // safe: i%8 is always 0..7, fits in uint
	}
	return b
}

// buildFlatV1BlobWithValues builds a snappy-encoded v1 flat uint64 column blob
// with the given absolute values and block/row indices. values must be sorted ascending.
func buildFlatV1BlobWithValues(values []uint64, blockIdxs, rowIdxs []uint8) []byte {
	n := len(values)
	// Layout: format_version[1], format[1], col_type[1], row_count[4], blockW[1], rowW[1],
	//         values[n×8], refs[n×2]
	raw := make([]byte, 3+4+2+n*8+n*2)
	pos := 0
	raw[pos] = shared.IntrinsicFormatVersion
	pos++
	raw[pos] = shared.IntrinsicFormatFlat
	pos++
	raw[pos] = byte(shared.ColumnTypeUint64)
	pos++
	binary.LittleEndian.PutUint32(raw[pos:], uint32(n)) //nolint:gosec
	pos += 4
	raw[pos] = 1 // blockW
	pos++
	raw[pos] = 1 // rowW
	pos++
	// Delta-encode values as 8-byte LE uint64.
	var prev uint64
	for i, v := range values {
		delta := v - prev
		prev = v
		binary.LittleEndian.PutUint64(raw[pos+i*8:], delta)
	}
	pos += n * 8
	for i := range n {
		raw[pos] = blockIdxs[i]
		raw[pos+1] = rowIdxs[i]
		pos += 2
	}
	return snappy.Encode(nil, raw)
}

// buildDictV1Blob builds a snappy-encoded v1 dict column blob.
// entries: map from string value → list of (blockIdx, rowIdx) pairs.
// Order is preserved via the keys/refs slices.
func buildDictV1Blob(keys []string, refs [][]shared.BlockRef) []byte {
	var buf []byte
	buf = append(buf, shared.IntrinsicFormatVersion)
	buf = append(buf, shared.IntrinsicFormatDict)
	buf = append(buf, byte(shared.ColumnTypeString))
	var tmp4 [4]byte
	var tmp2 [2]byte
	binary.LittleEndian.PutUint32(tmp4[:], uint32(len(keys))) //nolint:gosec
	buf = append(buf, tmp4[:]...)
	buf = append(buf, 1, 1) // blockW=1, rowW=1
	for i, k := range keys {
		binary.LittleEndian.PutUint16(tmp2[:], uint16(len(k))) //nolint:gosec
		buf = append(buf, tmp2[:]...)
		buf = append(buf, k...)
		binary.LittleEndian.PutUint32(tmp4[:], uint32(len(refs[i]))) //nolint:gosec
		buf = append(buf, tmp4[:]...)
		for _, r := range refs[i] {
			buf = append(buf, uint8(r.BlockIdx), uint8(r.RowIdx)) //nolint:gosec
		}
	}
	return snappy.Encode(nil, buf)
}

// buildFlatPageRaw builds a raw (pre-snappy) v2 flat page blob.
// values must be sorted ascending; refs are parallel.
func buildFlatPageRaw(values []uint64, blockIdxs, rowIdxs []uint8) []byte {
	// Encode varint deltas.
	varBuf := make([]byte, 0, len(values)*binary.MaxVarintLen64)
	var prev uint64
	for _, v := range values {
		var tmp [binary.MaxVarintLen64]byte
		n := binary.PutUvarint(tmp[:], v-prev)
		varBuf = append(varBuf, tmp[:n]...)
		prev = v
	}
	raw := make([]byte, 4+len(varBuf)+len(values)*2)
	binary.LittleEndian.PutUint32(raw[:4], uint32(len(varBuf))) //nolint:gosec
	copy(raw[4:], varBuf)
	pos := 4 + len(varBuf)
	for i := range values {
		raw[pos] = blockIdxs[i]
		raw[pos+1] = rowIdxs[i]
		pos += 2
	}
	return raw
}

// encodeUint64LE encodes a uint64 as an 8-byte little-endian string (used for page min/max).
func encodeUint64LE(v uint64) string {
	var b [8]byte
	binary.LittleEndian.PutUint64(b[:], v)
	return string(b[:])
}

// buildV2FlatBlob builds a complete v2 paged flat column blob from one or more sets of sorted values.
func buildV2FlatBlob(pageValues [][]uint64, pageBlockIdxs, pageRowIdxs [][]uint8) []byte {
	pageBlobs := make([][]byte, 0, len(pageValues))
	pageMetas := make([]shared.PageMeta, 0, len(pageValues))
	var offset uint32
	for i, vals := range pageValues {
		raw := buildFlatPageRaw(vals, pageBlockIdxs[i], pageRowIdxs[i])
		compressed := snappy.Encode(nil, raw)
		length := uint32(len(compressed)) //nolint:gosec // len always fits in uint32 for test data
		pageBlobs = append(pageBlobs, compressed)
		pageMetas = append(pageMetas, shared.PageMeta{
			Offset:   offset,
			Length:   length,
			RowCount: uint32(len(vals)), //nolint:gosec
			Min:      encodeUint64LE(vals[0]),
			Max:      encodeUint64LE(vals[len(vals)-1]),
		})
		offset += length
	}
	toc := shared.PagedIntrinsicTOC{
		BlockIdxWidth: 1,
		RowIdxWidth:   1,
		Format:        shared.IntrinsicFormatFlat,
		ColType:       shared.ColumnTypeUint64,
		Pages:         pageMetas,
	}
	tocBlob, err := shared.EncodePageTOC(toc)
	if err != nil {
		panic("buildV2FlatBlob: EncodePageTOC: " + err.Error())
	}
	result := make([]byte, 0, 1+4+len(tocBlob))
	result = append(result, shared.IntrinsicPagedVersion)
	var tocLen [4]byte
	binary.LittleEndian.PutUint32(tocLen[:], uint32(len(tocBlob))) //nolint:gosec
	result = append(result, tocLen[:]...)
	result = append(result, tocBlob...)
	for _, pb := range pageBlobs {
		result = append(result, pb...)
	}
	return result
}

// buildDictPageRaw builds a raw (pre-snappy) v2 dict page blob.
func buildDictPageRaw(keys []string, refs [][]shared.BlockRef) []byte {
	buf := make([]byte, 0, 64)
	var tmp4 [4]byte
	var tmp2 [2]byte
	binary.LittleEndian.PutUint32(tmp4[:], uint32(len(keys))) //nolint:gosec
	buf = append(buf, tmp4[:]...)
	for i, k := range keys {
		binary.LittleEndian.PutUint16(tmp2[:], uint16(len(k))) //nolint:gosec
		buf = append(buf, tmp2[:]...)
		buf = append(buf, k...)
		binary.LittleEndian.PutUint32(tmp4[:], uint32(len(refs[i]))) //nolint:gosec
		buf = append(buf, tmp4[:]...)
		for _, r := range refs[i] {
			buf = append(buf, uint8(r.BlockIdx), uint8(r.RowIdx)) //nolint:gosec
		}
	}
	return buf
}

// buildV2DictBlob builds a v2 paged dict column blob with optional per-page bloom filters.
func buildV2DictBlob(pageKeys [][]string, pageRefs [][][]shared.BlockRef, blooms [][]byte) []byte {
	pageBlobs := make([][]byte, 0, len(pageKeys))
	pageMetas := make([]shared.PageMeta, 0, len(pageKeys))
	var offset uint32
	for i, keys := range pageKeys {
		raw := buildDictPageRaw(keys, pageRefs[i])
		compressed := snappy.Encode(nil, raw)
		length := uint32(len(compressed)) //nolint:gosec // len always fits in uint32 for test data
		pageBlobs = append(pageBlobs, compressed)
		var rowCount uint32
		for _, r := range pageRefs[i] {
			rowCount += uint32(len(r)) //nolint:gosec
		}
		pm := shared.PageMeta{
			Offset:   offset,
			Length:   length,
			RowCount: rowCount,
		}
		if i < len(blooms) {
			pm.Bloom = blooms[i]
		}
		pageMetas = append(pageMetas, pm)
		offset += length
	}
	toc := shared.PagedIntrinsicTOC{
		BlockIdxWidth: 1,
		RowIdxWidth:   1,
		Format:        shared.IntrinsicFormatDict,
		ColType:       shared.ColumnTypeString,
		Pages:         pageMetas,
	}
	tocBlob, err := shared.EncodePageTOC(toc)
	if err != nil {
		panic("buildV2DictBlob: EncodePageTOC: " + err.Error())
	}
	result := make([]byte, 0, 1+4+len(tocBlob))
	result = append(result, shared.IntrinsicPagedVersion)
	var tocLen [4]byte
	binary.LittleEndian.PutUint32(tocLen[:], uint32(len(tocBlob))) //nolint:gosec
	result = append(result, tocLen[:]...)
	result = append(result, tocBlob...)
	for _, pb := range pageBlobs {
		result = append(result, pb...)
	}
	return result
}

// ---- DecodeTOC tests ----

func TestDecodeTOC_Roundtrip(t *testing.T) {
	// Build a TOC blob manually: toc_version[1] + col_count[4] + entries.
	raw := make([]byte, 0, 128)
	raw = append(raw, 0x01) // toc_version
	var tmp4 [4]byte
	var tmp2 [2]byte
	binary.LittleEndian.PutUint32(tmp4[:], 1) // col_count=1
	raw = append(raw, tmp4[:]...)
	// Entry: name="span:duration", col_type=ColumnTypeUint64, format=IntrinsicFormatFlat
	name := "span:duration"
	binary.LittleEndian.PutUint16(tmp2[:], uint16(len(name))) //nolint:gosec
	raw = append(raw, tmp2[:]...)
	raw = append(raw, name...)
	raw = append(raw, byte(shared.ColumnTypeUint64))
	raw = append(raw, shared.IntrinsicFormatFlat)
	// offset[8] + length[4] + entry_count[4]
	var tmp8 [8]byte
	binary.LittleEndian.PutUint64(tmp8[:], 1000)
	raw = append(raw, tmp8[:]...) // offset[8]
	binary.LittleEndian.PutUint32(tmp4[:], 256)
	raw = append(raw, tmp4[:]...) // length
	binary.LittleEndian.PutUint32(tmp4[:], 50)
	raw = append(raw, tmp4[:]...) // entry_count
	// min: 8 bytes
	minVal := encodeUint64LE(100)
	binary.LittleEndian.PutUint16(tmp2[:], uint16(len(minVal))) //nolint:gosec
	raw = append(raw, tmp2[:]...)
	raw = append(raw, minVal...)
	// max: 8 bytes
	maxVal := encodeUint64LE(1000)
	binary.LittleEndian.PutUint16(tmp2[:], uint16(len(maxVal))) //nolint:gosec
	raw = append(raw, tmp2[:]...)
	raw = append(raw, maxVal...)
	// bloom: empty
	binary.LittleEndian.PutUint16(tmp2[:], 0)
	raw = append(raw, tmp2[:]...)

	blob := snappy.Encode(nil, raw)
	metas, err := shared.DecodeTOC(blob)
	require.NoError(t, err)
	require.Len(t, metas, 1)
	assert.Equal(t, "span:duration", metas[0].Name)
	assert.Equal(t, shared.ColumnTypeUint64, metas[0].Type)
}

func TestDecodeTOC_Corrupt(t *testing.T) {
	_, err := shared.DecodeTOC([]byte{0xFF, 0x00})
	assert.Error(t, err)
}

// ---- Intrinsic bloom function tests ----

func TestIntrinsicBloom_AddAndTest(t *testing.T) {
	bloom := make([]byte, shared.IntrinsicBloomSize(10))
	key := []byte("resource.service.name=mysvc")
	shared.AddIntrinsicToBloom(bloom, key)
	assert.True(t, shared.TestIntrinsicBloom(bloom, key), "added key must be present")
	assert.False(t, shared.TestIntrinsicBloom(bloom, []byte("other.key=absent")), "un-added key should be absent")
}

func TestIntrinsicBloom_EmptyBloom(t *testing.T) {
	// nil bloom: TestIntrinsicBloom returns true (vacuous — no false negatives).
	assert.True(t, shared.TestIntrinsicBloom(nil, []byte("anything")))
	// empty slice: same semantics.
	assert.True(t, shared.TestIntrinsicBloom([]byte{}, []byte("anything")))
}

func TestIntrinsicBloom_AddToNilIsNoop(t *testing.T) {
	// Should not panic.
	assert.NotPanics(t, func() { shared.AddIntrinsicToBloom(nil, []byte("key")) })
	assert.NotPanics(t, func() { shared.AddIntrinsicToBloom([]byte{}, []byte("key")) })
}

func TestIntrinsicBloom_NoFalseNegatives(t *testing.T) {
	bloom := make([]byte, shared.IntrinsicBloomSize(100))
	keys := []string{"alpha", "beta", "gamma", "delta", "epsilon"}
	for _, k := range keys {
		shared.AddIntrinsicToBloom(bloom, []byte(k))
	}
	for _, k := range keys {
		assert.True(t, shared.TestIntrinsicBloom(bloom, []byte(k)), "added key %q must be present", k)
	}
}

// ---- EncodePageTOC / DecodePageTOC roundtrip tests ----

func TestEncodeDecodePageTOC_Roundtrip(t *testing.T) {
	orig := shared.PagedIntrinsicTOC{
		BlockIdxWidth: 1,
		RowIdxWidth:   2,
		Format:        shared.IntrinsicFormatFlat,
		ColType:       shared.ColumnTypeUint64,
		Pages: []shared.PageMeta{
			{Offset: 0, Length: 100, RowCount: 50, Min: "a", Max: "z", Bloom: []byte{0xAB, 0xCD}},
			{Offset: 100, Length: 200, RowCount: 75, Min: "b", Max: "y"},
		},
	}
	blob, err := shared.EncodePageTOC(orig)
	require.NoError(t, err)
	got, err := shared.DecodePageTOC(blob)
	require.NoError(t, err)
	assert.Equal(t, orig.BlockIdxWidth, got.BlockIdxWidth)
	assert.Equal(t, orig.RowIdxWidth, got.RowIdxWidth)
	assert.Equal(t, orig.Format, got.Format)
	assert.Equal(t, orig.ColType, got.ColType)
	require.Len(t, got.Pages, 2)
	assert.Equal(t, orig.Pages[0].RowCount, got.Pages[0].RowCount)
	assert.Equal(t, orig.Pages[0].Min, got.Pages[0].Min)
	assert.Equal(t, orig.Pages[0].Max, got.Pages[0].Max)
	assert.Equal(t, orig.Pages[0].Bloom, got.Pages[0].Bloom)
	assert.Equal(t, orig.Pages[1].Offset, got.Pages[1].Offset)
}

func TestDecodePageTOC_Corrupt(t *testing.T) {
	_, err := shared.DecodePageTOC([]byte{0xFF, 0xFE, 0x00})
	assert.Error(t, err, "invalid snappy must return error")
}

// ---- DecodeFlatPage tests ----

func TestDecodeFlatPage_Uint64_Roundtrip(t *testing.T) {
	// Build raw flat page: values_len[4] + varint-deltas + refs.
	// Values: [10, 20, 30] → deltas [10, 10, 10] as varints.
	valsBuf := make([]byte, 0, 3*binary.MaxVarintLen64)
	vals := []uint64{10, 20, 30}
	raw := make([]byte, 0, 4+len(vals)*binary.MaxVarintLen64+len(vals)*2)
	var prev uint64
	for _, v := range vals {
		delta := v - prev
		prev = v
		var buf [binary.MaxVarintLen64]byte
		n := binary.PutUvarint(buf[:], delta)
		valsBuf = append(valsBuf, buf[:n]...)
	}
	var vlen [4]byte
	binary.LittleEndian.PutUint32(vlen[:], uint32(len(valsBuf))) //nolint:gosec
	raw = append(raw, vlen[:]...)
	raw = append(raw, valsBuf...)
	// refs: 3 × 2 bytes (blockW=1, rowW=1)
	raw = append(raw, 0, 1, 0, 2, 0, 3)

	col, err := shared.DecodeFlatPage(raw, 1, 1, 3, shared.ColumnTypeUint64)
	require.NoError(t, err)
	require.NotNil(t, col)
	assert.Equal(t, []uint64{10, 20, 30}, col.Uint64Values)
	assert.Equal(t, 3, len(col.BlockRefs))
	assert.Equal(t, shared.BlockRef{BlockIdx: 0, RowIdx: 1}, col.BlockRefs[0])
	assert.Equal(t, shared.BlockRef{BlockIdx: 0, RowIdx: 2}, col.BlockRefs[1])
	assert.Equal(t, shared.BlockRef{BlockIdx: 0, RowIdx: 3}, col.BlockRefs[2])
}

func TestDecodeFlatPage_Corrupt(t *testing.T) {
	_, err := shared.DecodeFlatPage(nil, 1, 1, 1, shared.ColumnTypeUint64)
	assert.Error(t, err, "nil raw must return error")
}

// ---- DecodeDictPage tests ----

func TestDecodeDictPage_String_Roundtrip(t *testing.T) {
	// Build raw dict page: value_count[4] + per-value: vlen[2]+val+refcount[4]+refs.
	raw := make([]byte, 0, 128)
	var tmp4 [4]byte
	var tmp2 [2]byte
	binary.LittleEndian.PutUint32(tmp4[:], 2) // value_count=2
	raw = append(raw, tmp4[:]...)
	// value "foo" → 1 ref
	binary.LittleEndian.PutUint16(tmp2[:], 3)
	raw = append(raw, tmp2[:]...)
	raw = append(raw, "foo"...)
	binary.LittleEndian.PutUint32(tmp4[:], 1)
	raw = append(raw, tmp4[:]...)
	raw = append(raw, 1, 0) // blockIdx=1, rowIdx=0
	// value "bar" → 2 refs
	binary.LittleEndian.PutUint16(tmp2[:], 3)
	raw = append(raw, tmp2[:]...)
	raw = append(raw, "bar"...)
	binary.LittleEndian.PutUint32(tmp4[:], 2)
	raw = append(raw, tmp4[:]...)
	raw = append(raw, 2, 0, 3, 0) // two refs

	col, err := shared.DecodeDictPage(raw, 1, 1, shared.ColumnTypeString)
	require.NoError(t, err)
	require.NotNil(t, col)
	require.Len(t, col.DictEntries, 2)
	assert.Equal(t, "foo", col.DictEntries[0].Value)
	assert.Len(t, col.DictEntries[0].BlockRefs, 1)
	assert.Equal(t, "bar", col.DictEntries[1].Value)
	assert.Len(t, col.DictEntries[1].BlockRefs, 2)
}

func TestDecodeDictPage_Corrupt(t *testing.T) {
	_, err := shared.DecodeDictPage(nil, 1, 1, shared.ColumnTypeString)
	assert.Error(t, err, "nil raw must return error")
}

// ---- ScanFlatColumnRefs tests ----

func TestScanFlatColumnRefs_RangeMatch(t *testing.T) {
	// Values: [10, 20, 30, 40, 50], refs: blockIdx=0, rowIdx=i
	values := []uint64{10, 20, 30, 40, 50}
	blocks := []uint8{0, 0, 0, 0, 0}
	rows := []uint8{0, 1, 2, 3, 4}
	blob := buildFlatV1BlobWithValues(values, blocks, rows)

	// Range [20,40] should return rows 1,2,3 (values 20,30,40).
	refs := shared.ScanFlatColumnRefs(blob, 20, 40, true, true, 0)
	require.Len(t, refs, 3)
	assert.Equal(t, uint16(1), refs[0].RowIdx)
	assert.Equal(t, uint16(2), refs[1].RowIdx)
	assert.Equal(t, uint16(3), refs[2].RowIdx)
}

func TestScanFlatColumnRefs_NoMatch(t *testing.T) {
	values := []uint64{10, 20, 30}
	blob := buildFlatV1BlobWithValues(values, []uint8{0, 0, 0}, []uint8{0, 1, 2})
	// Range [50,100] → no matches.
	refs := shared.ScanFlatColumnRefs(blob, 50, 100, true, true, 0)
	assert.Empty(t, refs)
}

func TestScanFlatColumnRefs_NonFlatReturnsNil(t *testing.T) {
	// Dict blob passed to ScanFlatColumnRefs must return nil.
	blob := buildDictV1Blob([]string{"foo"}, [][]shared.BlockRef{{{BlockIdx: 0, RowIdx: 0}}})
	refs := shared.ScanFlatColumnRefs(blob, 0, 100, true, true, 0)
	assert.Nil(t, refs)
}

func TestScanFlatColumnTopKRefs_Forward(t *testing.T) {
	values := []uint64{10, 20, 30, 40, 50}
	blob := buildFlatV1BlobWithValues(values, []uint8{0, 0, 0, 0, 0}, []uint8{0, 1, 2, 3, 4})
	// Forward limit=3: smallest 3 timestamps → rows 0,1,2
	refs := shared.ScanFlatColumnTopKRefs(blob, 3, false)
	require.Len(t, refs, 3)
	assert.Equal(t, uint16(0), refs[0].RowIdx)
	assert.Equal(t, uint16(1), refs[1].RowIdx)
	assert.Equal(t, uint16(2), refs[2].RowIdx)
}

func TestScanFlatColumnTopKRefs_Backward(t *testing.T) {
	values := []uint64{10, 20, 30, 40, 50}
	blob := buildFlatV1BlobWithValues(values, []uint8{0, 0, 0, 0, 0}, []uint8{0, 1, 2, 3, 4})
	// backward=true reads last `limit` refs in ascending order (rows 2,3,4).
	refs := shared.ScanFlatColumnTopKRefs(blob, 3, true)
	require.Len(t, refs, 3)
	assert.Equal(t, uint16(2), refs[0].RowIdx)
	assert.Equal(t, uint16(3), refs[1].RowIdx)
	assert.Equal(t, uint16(4), refs[2].RowIdx)
}

func TestScanFlatColumnRefsFiltered_EvenRows(t *testing.T) {
	values := []uint64{10, 20, 30, 40, 50}
	blob := buildFlatV1BlobWithValues(values, []uint8{0, 0, 0, 0, 0}, []uint8{0, 1, 2, 3, 4})
	// Filter: only even rowIdx. limit must be > 0.
	refs := shared.ScanFlatColumnRefsFiltered(blob, false, 10, func(r shared.BlockRef) bool {
		return r.RowIdx%2 == 0
	})
	require.Len(t, refs, 3) // rows 0, 2, 4
	for _, r := range refs {
		assert.Equal(t, uint16(0), r.RowIdx%2, "only even rows expected")
	}
}

// ---- ScanDictColumnRefs tests ----

func TestScanDictColumnRefs_StringMatch(t *testing.T) {
	keys := []string{"foo", "bar", "baz"}
	refs := [][]shared.BlockRef{
		{{BlockIdx: 1, RowIdx: 0}},
		{{BlockIdx: 2, RowIdx: 0}, {BlockIdx: 2, RowIdx: 1}},
		{{BlockIdx: 3, RowIdx: 0}},
	}
	blob := buildDictV1Blob(keys, refs)

	// Match only "bar" → 2 refs.
	got := shared.ScanDictColumnRefs(blob, func(v string, _ int64, _ bool) bool { return v == "bar" }, 0)
	require.Len(t, got, 2)
	assert.Equal(t, uint16(2), got[0].BlockIdx)
}

func TestScanDictColumnRefs_NoMatch(t *testing.T) {
	blob := buildDictV1Blob([]string{"foo"}, [][]shared.BlockRef{{{BlockIdx: 0, RowIdx: 0}}})
	got := shared.ScanDictColumnRefs(blob, func(v string, _ int64, _ bool) bool { return v == "missing" }, 0)
	assert.Empty(t, got)
}

func TestScanDictColumnRefs_NonDictReturnsNil(t *testing.T) {
	// Flat blob passed to ScanDictColumnRefs must return nil.
	blob := buildFlatV1BlobWithValues([]uint64{1}, []uint8{0}, []uint8{0})
	got := shared.ScanDictColumnRefs(blob, func(v string, _ int64, _ bool) bool { return true }, 0)
	assert.Nil(t, got)
}

// ---- V2 paged flat blob scan tests ----

func TestScanFlatColumnRefs_V2_Paged(t *testing.T) {
	// Two pages: page1=[10,20,30], page2=[40,50,60]
	blob := buildV2FlatBlob(
		[][]uint64{{10, 20, 30}, {40, 50, 60}},
		[][]uint8{{0, 0, 0}, {1, 1, 1}},
		[][]uint8{{0, 1, 2}, {0, 1, 2}},
	)
	// Range [20,50] spans both pages.
	refs := shared.ScanFlatColumnRefs(blob, 20, 50, true, true, 0)
	require.GreaterOrEqual(t, len(refs), 4, "values 20,30,40,50 all in range")
}

func TestScanFlatColumnTopKRefs_V2_Paged(t *testing.T) {
	blob := buildV2FlatBlob(
		[][]uint64{{10, 20, 30}, {40, 50}},
		[][]uint8{{0, 0, 0}, {1, 1}},
		[][]uint8{{0, 1, 2}, {0, 1}},
	)
	refs := shared.ScanFlatColumnTopKRefs(blob, 3, true)
	require.Len(t, refs, 3)
}

func TestScanFlatColumnRefsFiltered_V2_Paged(t *testing.T) {
	blob := buildV2FlatBlob(
		[][]uint64{{10, 20, 30}},
		[][]uint8{{0, 1, 2}},
		[][]uint8{{0, 0, 0}},
	)
	refs := shared.ScanFlatColumnRefsFiltered(blob, false, 10, func(r shared.BlockRef) bool {
		return r.BlockIdx%2 == 0
	})
	require.Greater(t, len(refs), 0)
}

// ---- V2 paged dict blob scan tests ----

func TestScanDictColumnRefs_V2_Paged(t *testing.T) {
	// Two pages, each with one key.
	blob := buildV2DictBlob(
		[][]string{{"foo"}, {"bar"}},
		[][][]shared.BlockRef{
			{{{BlockIdx: 1, RowIdx: 0}}},
			{{{BlockIdx: 2, RowIdx: 0}}},
		},
		nil,
	)
	got := shared.ScanDictColumnRefs(blob, func(v string, _ int64, _ bool) bool { return v == "bar" }, 0)
	require.Len(t, got, 1)
	assert.Equal(t, uint16(2), got[0].BlockIdx)
}

func TestScanDictColumnRefsWithBloom_V2_BloomSkip(t *testing.T) {
	// Build a bloom for the first page that does NOT contain "target".
	// ScanDictColumnRefsWithBloom should skip that page.
	bloomPage1 := make([]byte, shared.IntrinsicBloomSize(10))
	shared.AddIntrinsicToBloom(bloomPage1, []byte("other"))
	// Don't add "target" to bloom for page1 — page1 should be skipped.

	blob := buildV2DictBlob(
		[][]string{{"other"}, {"target"}},
		[][][]shared.BlockRef{
			{{{BlockIdx: 1, RowIdx: 0}}},
			{{{BlockIdx: 2, RowIdx: 0}}},
		},
		[][]byte{bloomPage1, nil},
	)
	bloomKeys := [][]byte{[]byte("target")}
	got := shared.ScanDictColumnRefsWithBloom(blob,
		func(v string, _ int64, _ bool) bool { return v == "target" },
		bloomKeys, 0)
	// Should find "target" from page2.
	require.Len(t, got, 1)
	assert.Equal(t, uint16(2), got[0].BlockIdx)
}

// TestDecodeIntrinsicColumnBlob_V2_DictPaged_MergesEntries verifies that mergeIntrinsicColumns
// is called when two pages have a common key — the merged entry should have refs from both pages.
func TestDecodeIntrinsicColumnBlob_V2_DictPaged_MergesEntries(t *testing.T) {
	// Both pages have "shared-key"; page1 also has "only-p1".
	blob := buildV2DictBlob(
		[][]string{{"shared-key", "only-p1"}, {"shared-key"}},
		[][][]shared.BlockRef{
			{{{BlockIdx: 1, RowIdx: 0}}, {{BlockIdx: 1, RowIdx: 1}}},
			{{{BlockIdx: 2, RowIdx: 0}}},
		},
		nil,
	)
	col, err := shared.DecodeIntrinsicColumnBlob(blob)
	require.NoError(t, err)
	require.NotNil(t, col)
	// Find "shared-key" entry — must have refs from both pages (2 refs total).
	var sharedEntry *shared.IntrinsicDictEntry
	for i := range col.DictEntries {
		if col.DictEntries[i].Value == "shared-key" {
			sharedEntry = &col.DictEntries[i]
			break
		}
	}
	require.NotNil(t, sharedEntry, "shared-key must be present in merged column")
	assert.Equal(t, 2, len(sharedEntry.BlockRefs), "merged entry must have refs from both pages")
}

func TestScanDictColumnRefsWithBloom_V1DelegatesToScan(t *testing.T) {
	// For v1 blobs, ScanDictColumnRefsWithBloom simply delegates to ScanDictColumnRefs.
	blob := buildDictV1Blob([]string{"foo", "bar"}, [][]shared.BlockRef{
		{{BlockIdx: 1, RowIdx: 0}},
		{{BlockIdx: 2, RowIdx: 0}},
	})
	bloomKeys := [][]byte{[]byte("foo")}
	got := shared.ScanDictColumnRefsWithBloom(blob,
		func(v string, _ int64, _ bool) bool { return v == "foo" },
		bloomKeys, 0)
	require.Len(t, got, 1)
	assert.Equal(t, uint16(1), got[0].BlockIdx)
}
