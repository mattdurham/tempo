package reader

// NOTE: Any changes to this file must be reflected in the corresponding specs.md or NOTES.md.

import (
	"encoding/binary"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/grafana/blockpack/internal/modules/blockio/shared"
)

// buildV1TraceIndexPayload constructs a minimal v1 trace index byte slice by hand.
// v1 format: fmt_version[1]=0x01 + trace_count[4] + per-trace: trace_id[16] +
// block_ref_count[2] + per-block: block_id[2]+span_count[2]+span_indices[N×2].
func buildV1TraceIndexPayload(traceID [16]byte) []byte {
	var tmp4 [4]byte
	var tmp2 [2]byte
	// Pre-size: 1 (ver) + 4 (count) + 16 (tid) + 2 (brc) + 2+2+3×2 (blk0) + 2+2+1×2 (blk1)
	buf := make([]byte, 0, 1+4+16+2+(2+2+6)+(2+2+2))

	buf = append(buf, shared.TraceIndexFmtVersion) // 0x01

	binary.LittleEndian.PutUint32(tmp4[:], 1) // trace_count = 1
	buf = append(buf, tmp4[:]...)

	buf = append(buf, traceID[:]...)

	binary.LittleEndian.PutUint16(tmp2[:], 2) // block_ref_count = 2
	buf = append(buf, tmp2[:]...)

	// block[0]: block_id=0, span_count=3, indices={0,1,2}
	binary.LittleEndian.PutUint16(tmp2[:], 0)
	buf = append(buf, tmp2[:]...)
	binary.LittleEndian.PutUint16(tmp2[:], 3)
	buf = append(buf, tmp2[:]...)
	for _, idx := range []uint16{0, 1, 2} {
		binary.LittleEndian.PutUint16(tmp2[:], idx)
		buf = append(buf, tmp2[:]...)
	}

	// block[1]: block_id=1, span_count=1, indices={5}
	binary.LittleEndian.PutUint16(tmp2[:], 1)
	buf = append(buf, tmp2[:]...)
	binary.LittleEndian.PutUint16(tmp2[:], 1)
	buf = append(buf, tmp2[:]...)
	binary.LittleEndian.PutUint16(tmp2[:], 5)
	buf = append(buf, tmp2[:]...)

	return buf
}

// TestParseTraceBlockIndex_V1BackwardCompat verifies that a hand-crafted v1 trace index
// (with per-block span indices) is parsed correctly: block IDs are extracted and span
// indices are discarded, producing the same map shape as v2.
// TBI-05: v1 backward compat — span indices discarded, block IDs preserved.
func TestParseTraceBlockIndex_V1BackwardCompat(t *testing.T) {
	traceID := [16]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}

	buf := buildV1TraceIndexPayload(traceID)

	result, consumed, err := parseTraceBlockIndex(buf)
	require.NoError(t, err)
	assert.Equal(t, len(buf), consumed, "all bytes consumed")
	require.Len(t, result, 1, "one trace entry")

	blockIDs, ok := result[traceID]
	require.True(t, ok, "trace ID found in result")
	assert.Equal(t, []uint16{0, 1}, blockIDs, "block IDs extracted, span indices discarded")
}

// TestParseTraceBlockIndex_V2 verifies parsing of a v2 trace index (block IDs only).
func TestParseTraceBlockIndex_V2(t *testing.T) {
	var traceID [16]byte
	traceID[0] = 0xAB

	var tmp4 [4]byte
	var tmp2 [2]byte
	buf := make([]byte, 0, 1+4+16+2+2+2) // 1(ver)+4(cnt)+16(tid)+2(brc)+2+2(ids)

	buf = append(buf, shared.TraceIndexFmtVersion2) // 0x02

	binary.LittleEndian.PutUint32(tmp4[:], 1)
	buf = append(buf, tmp4[:]...)

	buf = append(buf, traceID[:]...)

	binary.LittleEndian.PutUint16(tmp2[:], 2) // block_ref_count = 2
	buf = append(buf, tmp2[:]...)
	binary.LittleEndian.PutUint16(tmp2[:], 3) // block_id = 3
	buf = append(buf, tmp2[:]...)
	binary.LittleEndian.PutUint16(tmp2[:], 7) // block_id = 7
	buf = append(buf, tmp2[:]...)

	result, consumed, err := parseTraceBlockIndex(buf)
	require.NoError(t, err)
	assert.Equal(t, len(buf), consumed)
	require.Len(t, result, 1)
	assert.Equal(t, []uint16{3, 7}, result[traceID])
}

// TestParseTraceBlockIndex_UnsupportedVersion verifies that an unknown fmt_version
// returns an error rather than silently producing incorrect data.
func TestParseTraceBlockIndex_UnsupportedVersion(t *testing.T) {
	buf := make([]byte, 10)
	buf[0] = 0xFF // unsupported version

	_, _, err := parseTraceBlockIndex(buf)
	assert.ErrorContains(t, err, "unsupported fmt_version")
}
