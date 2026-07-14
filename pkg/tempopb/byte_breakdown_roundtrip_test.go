package tempopb

// byte_breakdown_roundtrip_test.go -- issue #218 Phase 1: verifies the new
// per-source byte-breakdown fields on SearchMetrics, MetadataMetrics, and
// TraceByIDMetrics actually survive a real Marshal/Unmarshal round-trip.
// These fields were hand-patched into tempo.pb.go (protoc/protoc-gen-gogo
// tooling is not available in this environment; make gen-proto's delete
// step is also broken here), so this test guards against the exact class of
// wire-format gap documented in vibackfill_roundtrip_test.go's
// TestJobDetail_CubeBackfill_KnownWireFormatGap, where a struct-tag-only
// field is silently dropped by Marshal/Unmarshal.

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSearchMetrics_NewByteFields_MarshalUnmarshalRoundTrip(t *testing.T) {
	original := &SearchMetrics{
		InspectedTraces:   11,
		InspectedBytes:    22,
		TotalBlocks:       33,
		CompletedJobs:     44,
		TotalJobs:         55,
		TotalBlockBytes:   66,
		InspectedSpans:    77,
		IndexBytesRead:    111,
		DataFileBytesRead: 222,
		VcntBytesRead:     333,
		CubeBytesRead:     444,
	}

	data, err := original.Marshal()
	require.NoError(t, err)
	require.NotEmpty(t, data)

	var decoded SearchMetrics
	require.NoError(t, decoded.Unmarshal(data))

	assert.Equal(t, original.InspectedTraces, decoded.InspectedTraces)
	assert.Equal(t, original.InspectedBytes, decoded.InspectedBytes)
	assert.Equal(t, original.TotalBlocks, decoded.TotalBlocks)
	assert.Equal(t, original.CompletedJobs, decoded.CompletedJobs)
	assert.Equal(t, original.TotalJobs, decoded.TotalJobs)
	assert.Equal(t, original.TotalBlockBytes, decoded.TotalBlockBytes)
	assert.Equal(t, original.InspectedSpans, decoded.InspectedSpans)
	assert.Equal(t, uint64(111), decoded.IndexBytesRead)
	assert.Equal(t, uint64(222), decoded.DataFileBytesRead)
	assert.Equal(t, uint64(333), decoded.VcntBytesRead)
	assert.Equal(t, uint64(444), decoded.CubeBytesRead)
}

func TestSearchMetrics_NewByteFields_SizeMatchesMarshaledLength(t *testing.T) {
	sm := &SearchMetrics{
		IndexBytesRead:    111,
		DataFileBytesRead: 222,
		VcntBytesRead:     333,
		CubeBytesRead:     444,
	}
	data, err := sm.Marshal()
	require.NoError(t, err)
	assert.Equal(t, sm.Size(), len(data), "Size() must match the actual Marshal() output length")
}

func TestMetadataMetrics_NewByteFields_MarshalUnmarshalRoundTrip(t *testing.T) {
	original := &MetadataMetrics{
		InspectedBytes:    1,
		TotalJobs:         2,
		CompletedJobs:     3,
		TotalBlocks:       4,
		TotalBlockBytes:   5,
		IndexBytesRead:    555,
		DataFileBytesRead: 666,
		VcntBytesRead:     777,
	}

	data, err := original.Marshal()
	require.NoError(t, err)
	require.NotEmpty(t, data)

	var decoded MetadataMetrics
	require.NoError(t, decoded.Unmarshal(data))

	assert.Equal(t, original.InspectedBytes, decoded.InspectedBytes)
	assert.Equal(t, original.TotalJobs, decoded.TotalJobs)
	assert.Equal(t, original.CompletedJobs, decoded.CompletedJobs)
	assert.Equal(t, original.TotalBlocks, decoded.TotalBlocks)
	assert.Equal(t, original.TotalBlockBytes, decoded.TotalBlockBytes)
	assert.Equal(t, uint64(555), decoded.IndexBytesRead)
	assert.Equal(t, uint64(666), decoded.DataFileBytesRead)
	assert.Equal(t, uint64(777), decoded.VcntBytesRead)
}

func TestMetadataMetrics_NewByteFields_SizeMatchesMarshaledLength(t *testing.T) {
	mm := &MetadataMetrics{
		IndexBytesRead:    555,
		DataFileBytesRead: 666,
		VcntBytesRead:     777,
	}
	data, err := mm.Marshal()
	require.NoError(t, err)
	assert.Equal(t, mm.Size(), len(data), "Size() must match the actual Marshal() output length")
}

func TestTraceByIDMetrics_NewByteFields_MarshalUnmarshalRoundTrip(t *testing.T) {
	original := &TraceByIDMetrics{
		InspectedBytes:    888,
		IndexBytesRead:    999,
		DataFileBytesRead: 1010,
	}

	data, err := original.Marshal()
	require.NoError(t, err)
	require.NotEmpty(t, data)

	var decoded TraceByIDMetrics
	require.NoError(t, decoded.Unmarshal(data))

	assert.Equal(t, original.InspectedBytes, decoded.InspectedBytes)
	assert.Equal(t, uint64(999), decoded.IndexBytesRead)
	assert.Equal(t, uint64(1010), decoded.DataFileBytesRead)
}

func TestTraceByIDMetrics_NewByteFields_SizeMatchesMarshaledLength(t *testing.T) {
	tm := &TraceByIDMetrics{
		IndexBytesRead:    999,
		DataFileBytesRead: 1010,
	}
	data, err := tm.Marshal()
	require.NoError(t, err)
	assert.Equal(t, tm.Size(), len(data), "Size() must match the actual Marshal() output length")
}
