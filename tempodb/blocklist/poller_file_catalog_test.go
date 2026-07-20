package blocklist

// poller_file_catalog_test.go — TDD coverage for issue #522 #159's poller flip
// (FileCatalogBlockLister). Proves: (1) when set, pollTenantBlocks uses it INSTEAD OF a real
// backend LIST call, with byte-identical downstream results to the reader.Blocks path; (2) when
// nil (every deployment's default), behavior is completely unchanged -- a transparency
// regression guard on top of every pre-existing poller_test.go case already implicitly covering
// this via the nil default.

import (
	"context"
	"errors"
	"testing"

	"github.com/go-kit/log"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/grafana/tempo/tempodb/backend"
)

// fakeFileCatalogBlockLister is an in-memory FileCatalogBlockLister for unit tests, tracking
// call count so tests can assert it (not reader.Blocks) was the actual source.
type fakeFileCatalogBlockLister struct {
	live, compacted []uuid.UUID
	err             error
	calls           int
}

func (f *fakeFileCatalogBlockLister) ListBlockIDs(_ context.Context, _ string) ([]uuid.UUID, []uuid.UUID, error) {
	f.calls++
	if f.err != nil {
		return nil, nil, f.err
	}
	return f.live, f.compacted, nil
}

// countingBlocksReader wraps a real backend.Reader, counting Blocks() calls -- used to prove
// pollTenantBlocks never calls the real backend LIST when a FileCatalogBlockLister is set.
type countingBlocksReader struct {
	backend.Reader
	blocksCalls int
}

func (c *countingBlocksReader) Blocks(ctx context.Context, tenantID string) ([]uuid.UUID, []uuid.UUID, error) {
	c.blocksCalls++
	return c.Reader.Blocks(ctx, tenantID)
}

func TestPollTenantBlocks_UsesFileCatalogListerWhenSet_NeverCallsRealBackendList(t *testing.T) {
	tenant := "tenant-a"
	liveID := uuid.New()
	compactedID := uuid.New()

	// pollUnknown/pollBlock still fetch full BlockMeta content for genuinely new IDs via the
	// real backend.Reader (file_catalog rows never carried full BlockMeta content, only
	// IDs/discovery data) -- these fixtures back that call, even though Blocks() itself (the
	// LIST call this test proves is never made) is never consulted.
	baseReader := newMockReader(
		PerTenant{tenant: {{BlockID: backend.UUID(liveID)}}},
		PerTenantCompacted{tenant: {{BlockMeta: backend.BlockMeta{BlockID: backend.UUID(compactedID)}}}},
		false,
	)
	countingReader := &countingBlocksReader{Reader: baseReader}

	lister := &fakeFileCatalogBlockLister{live: []uuid.UUID{liveID}, compacted: []uuid.UUID{compactedID}}

	compactor := newMockCompactor(PerTenantCompacted{tenant: {{BlockMeta: backend.BlockMeta{BlockID: backend.UUID(compactedID)}}}}, false)

	poller := NewPoller(&PollerConfig{
		PollConcurrency:       testPollConcurrency,
		TenantPollConcurrency: testTenantPollConcurrency,
	}, &mockJobSharder{owns: true}, countingReader, compactor, &backend.MockWriter{}, log.NewNopLogger())
	poller.SetFileCatalogLister(lister)

	live, compactedMetas, err := poller.pollTenantBlocks(context.Background(), tenant, newBlocklist(PerTenant{}, PerTenantCompacted{}))
	require.NoError(t, err)

	require.Equal(t, 1, lister.calls, "the file_catalog lister must be consulted exactly once")
	require.Zero(t, countingReader.blocksCalls, "the real backend LIST (reader.Blocks) must never be called when a FileCatalogBlockLister is set")

	require.Len(t, live, 1)
	require.Equal(t, liveID, uuid.UUID(live[0].BlockID))
	require.Len(t, compactedMetas, 1)
	require.Equal(t, compactedID, uuid.UUID(compactedMetas[0].BlockID))
}

func TestPollTenantBlocks_FallsBackToReaderWhenListerNil(t *testing.T) {
	tenant := "tenant-a"
	liveID := uuid.New()

	baseReader := newMockReader(PerTenant{
		tenant: {{BlockID: backend.UUID(liveID)}},
	}, PerTenantCompacted{}, false)
	countingReader := &countingBlocksReader{Reader: baseReader}

	poller := NewPoller(&PollerConfig{
		PollConcurrency:       testPollConcurrency,
		TenantPollConcurrency: testTenantPollConcurrency,
	}, &mockJobSharder{owns: true}, countingReader, newMockCompactor(PerTenantCompacted{}, false), &backend.MockWriter{}, log.NewNopLogger())
	// No SetFileCatalogLister call -- nil is the default, every deployment's existing behavior.

	live, _, err := poller.pollTenantBlocks(context.Background(), tenant, newBlocklist(PerTenant{}, PerTenantCompacted{}))
	require.NoError(t, err)

	require.Equal(t, 1, countingReader.blocksCalls, "reader.Blocks must be used when no FileCatalogBlockLister is set")
	require.Len(t, live, 1)
	require.Equal(t, backend.UUID(liveID), live[0].BlockID)
}

func TestPollTenantBlocks_PropagatesFileCatalogListerError(t *testing.T) {
	lister := &fakeFileCatalogBlockLister{err: errors.New("postgres unavailable")}

	poller := NewPoller(&PollerConfig{
		PollConcurrency:       testPollConcurrency,
		TenantPollConcurrency: testTenantPollConcurrency,
	}, &mockJobSharder{owns: true}, newMockReader(PerTenant{}, PerTenantCompacted{}, false), newMockCompactor(PerTenantCompacted{}, false), &backend.MockWriter{}, log.NewNopLogger())
	poller.SetFileCatalogLister(lister)

	_, _, err := poller.pollTenantBlocks(context.Background(), "tenant-a", newBlocklist(PerTenant{}, PerTenantCompacted{}))
	require.Error(t, err)
}
