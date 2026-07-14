package tempopb

// vibackfill_roundtrip_test.go — #496 B2: verifies JobDetail.ViBackfill
// actually survives a real Marshal/Unmarshal round-trip. This test exists
// specifically because implementing this feature revealed that JobDetail.
// CubeBackfill (JOB_TYPE_CUBE_BACKFILL's own equivalent field, added the same
// hand-patched way since protoc/protoc-gen-gogo are not available in this
// environment) was declared with a protobuf tag but never wired into
// JobDetail's MarshalToSizedBuffer/Unmarshal/Size methods -- it was silently
// dropped on every real gRPC round-trip. #181 Phase 5 deleted CubeBackfillDetail/
// JobDetail.CubeBackfill entirely once cube_backfill moved off this gRPC path
// onto Postgres, so that gap no longer exists to document. ViBackfill must not
// repeat that mistake.

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
