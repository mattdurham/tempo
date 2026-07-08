package vblockpack

// blockevents_test.go — pins blockObjectKey/parseBlockObjectKey's round-trip contract and
// readerForSourceRef's ability to open a DIFFERENT block than the one Fetch is currently
// scoped to (plan-d.md D3/DT1: Option A's multi-file trace materialization needs this to
// resolve a structural match's spans across sibling compaction-boundary blocks).

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/grafana/tempo/tempodb/backend"
	"github.com/grafana/tempo/tempodb/backend/local"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseBlockObjectKey_RoundTripsWithBlockObjectKey(t *testing.T) {
	tenant := "test-tenant"
	blockID := uuid.New()
	key := blockObjectKey(tenant, blockID.String())

	gotTenant, gotBlockID, err := parseBlockObjectKey(key)
	require.NoError(t, err)
	assert.Equal(t, tenant, gotTenant)
	assert.Equal(t, blockID, gotBlockID)
}

func TestParseBlockObjectKey_TenantContainingSlashes(t *testing.T) {
	// Tenant IDs are not expected to contain '/', but parseBlockObjectKey must still recover
	// the RIGHTMOST tenant/block-id separator correctly (LastIndex, not the first slash) so a
	// pathological tenant name doesn't silently misparse the block ID.
	tenant := "org/sub-tenant"
	blockID := uuid.New()
	key := blockObjectKey(tenant, blockID.String())

	gotTenant, gotBlockID, err := parseBlockObjectKey(key)
	require.NoError(t, err)
	assert.Equal(t, tenant, gotTenant)
	assert.Equal(t, blockID, gotBlockID)
}

func TestParseBlockObjectKey_MissingDataFileSuffix(t *testing.T) {
	_, _, err := parseBlockObjectKey("test-tenant/" + uuid.NewString() + "/not-the-data-file")
	require.Error(t, err)
}

func TestParseBlockObjectKey_MissingTenantSeparator(t *testing.T) {
	_, _, err := parseBlockObjectKey(DataFileName)
	require.Error(t, err)
}

func TestParseBlockObjectKey_InvalidBlockID(t *testing.T) {
	_, _, err := parseBlockObjectKey("test-tenant/not-a-uuid/" + DataFileName)
	require.Error(t, err)
}

// TestReaderForSourceRef_OpensSiblingBlock pins readerForSourceRef's generalization over
// newReader: given block A's own blockpackBlock, it can still open a reader for a DIFFERENT
// block B in the same tenant purely from B's SourceRef string — the capability Option A's
// multi-file trace materialization depends on.
func TestReaderForSourceRef_OpensSiblingBlock(t *testing.T) {
	dir := t.TempDir()
	tenant := "test-tenant"
	viStore := &fakeVISink{}
	withVISink(t, viStore, "indexes")

	metaA, _ := writeSvcBlock(t, dir, viStore, tenant, uuid.New(), "svc-a", 3)
	metaB, _ := writeSvcBlock(t, dir, viStore, tenant, uuid.New(), "svc-b", 3)

	rawR, _, _, err := local.New(&local.Config{Path: dir})
	require.NoError(t, err)
	blockA := newBackendBlock(metaA, backend.NewReader(rawR))

	sourceRefB := blockObjectKey(tenant, uuid.UUID(metaB.BlockID).String())
	r, err := blockA.readerForSourceRef(context.Background(), sourceRefB)
	require.NoError(t, err, "readerForSourceRef must open block B even though it was called on block A")
	require.NotNil(t, r)
	assert.Equal(t, 1, r.BlockCount())
}
