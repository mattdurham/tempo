package tempopb

// vibackfill_roundtrip_test.go — #496 B2: verifies JobDetail.ViBackfill
// actually survives a real Marshal/Unmarshal round-trip. This test exists
// specifically because implementing this feature revealed that JobDetail.
// CubeBackfill (JOB_TYPE_CUBE_BACKFILL's own equivalent field, added the same
// hand-patched way since protoc/protoc-gen-gogo are not available in this
// environment) is declared with a protobuf tag but was never wired into
// JobDetail's MarshalToSizedBuffer/Unmarshal/Size methods -- it is silently
// dropped on every real gRPC round-trip despite being actively read by
// modules/backendworker/backendworker.go's processCubeBackfillJob. Reported
// to team lead as a standalone finding, not fixed here (cube's own code, out
// of #496's scope). ViBackfill must not repeat that mistake.

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestJobDetail_ViBackfill_MarshalUnmarshalRoundTrips(t *testing.T) {
	original := &JobDetail{
		Tenant:  "tenant-a",
		BatchId: "batch-1",
		ViBackfill: &ViBackfillDetail{
			ColumnHash: "abc123",
			ColumnName: "span.custom.attr",
			ColumnType: "string",
		},
	}

	data, err := original.Marshal()
	require.NoError(t, err)
	require.NotEmpty(t, data)

	var decoded JobDetail
	require.NoError(t, decoded.Unmarshal(data))

	require.NotNil(t, decoded.ViBackfill, "ViBackfill must survive the round-trip, unlike CubeBackfill's known gap")
	assert.Equal(t, original.Tenant, decoded.Tenant)
	assert.Equal(t, original.BatchId, decoded.BatchId)
	assert.Equal(t, original.ViBackfill.ColumnHash, decoded.ViBackfill.ColumnHash)
	assert.Equal(t, original.ViBackfill.ColumnName, decoded.ViBackfill.ColumnName)
	assert.Equal(t, original.ViBackfill.ColumnType, decoded.ViBackfill.ColumnType)
}

func TestJobDetail_ViBackfill_SizeMatchesMarshaledLength(t *testing.T) {
	jd := &JobDetail{
		Tenant: "tenant-a",
		ViBackfill: &ViBackfillDetail{
			ColumnHash: "abc123",
			ColumnName: "span.custom.attr",
			ColumnType: "string",
		},
	}
	data, err := jd.Marshal()
	require.NoError(t, err)
	assert.Equal(t, jd.Size(), len(data), "Size() must match the actual Marshal() output length")
}

func TestJobDetail_NilViBackfill_MarshalUnmarshalRoundTrips(t *testing.T) {
	original := &JobDetail{Tenant: "tenant-a", BatchId: "batch-1"}

	data, err := original.Marshal()
	require.NoError(t, err)

	var decoded JobDetail
	require.NoError(t, decoded.Unmarshal(data))
	assert.Nil(t, decoded.ViBackfill, "a JobDetail with no ViBackfill set must decode with a nil ViBackfill, not a zero-value struct")
}

func TestJobType_ViBackfillEnumValue(t *testing.T) {
	assert.Equal(t, JobType(5), JobType_JOB_TYPE_VI_BACKFILL)
	assert.Equal(t, "JOB_TYPE_VI_BACKFILL", JobType_JOB_TYPE_VI_BACKFILL.String())
}

// TestJobDetail_CubeBackfill_KnownWireFormatGap documents (does not fix) the
// standalone finding above: CubeBackfill is silently dropped by Marshal/
// Unmarshal. This test is intentionally written to demonstrate the gap, not
// to enforce correct behavior -- if this test ever starts failing (i.e.
// CubeBackfill starts round-tripping), that's a sign someone fixed the gap
// and this test (and its comment) should be deleted, not "fixed" to keep
// failing.
func TestJobDetail_CubeBackfill_KnownWireFormatGap(t *testing.T) {
	original := &JobDetail{
		Tenant:       "tenant-a",
		CubeBackfill: &CubeBackfillDetail{CubeID: "cube-1", WindowMinutes: 60},
	}
	data, err := original.Marshal()
	require.NoError(t, err)

	var decoded JobDetail
	require.NoError(t, decoded.Unmarshal(data))
	assert.Nil(t, decoded.CubeBackfill, "documents the known gap: CubeBackfill is NOT wired into Marshal/Unmarshal/Size")
}
