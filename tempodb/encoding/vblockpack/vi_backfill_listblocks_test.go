package vblockpack

// vi_backfill_listblocks_test.go — go-presubmit.md MEDIUM finding: viBlockFetcher.
// ListBlocksInRange must FAIL the whole listing when any block's meta.json cannot be
// read, not silently skip it (see vi_backfill.go's ListBlocksInRange doc comment for
// the full rationale — a silently-skipped block could leave BackfillEngine believing
// it processed everything, persisting a false Done=true/WatermarkSec=minSec claim for
// data that was never actually indexed).

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/google/uuid"
	"github.com/grafana/tempo/tempodb/backend"
	"github.com/grafana/tempo/tempodb/backend/local"
	"github.com/stretchr/testify/require"
)

// TestViBlockFetcher_ListBlocksInRange_FailsOnUnreadableBlockMeta constructs two real
// blocks via the standard write path, corrupts one block's meta.json, and asserts
// ListBlocksInRange returns an error (not a partial, silently-truncated list).
func TestViBlockFetcher_ListBlocksInRange_FailsOnUnreadableBlockMeta(t *testing.T) {
	dir := t.TempDir()
	const tenant = "test-tenant"

	_, _ = writeEmptyBlock(t, dir, tenant, uuid.New())
	_, badDataPath := writeEmptyBlock(t, dir, tenant, uuid.New())

	// Corrupt the second block's meta.json (sibling of its data file) so BlockMeta
	// fails to unmarshal it.
	badMetaPath := filepath.Join(filepath.Dir(badDataPath), backend.MetaName)
	require.NoError(t, os.WriteFile(badMetaPath, []byte("{not valid json"), 0o644))

	rawR, _, _, err := local.New(&local.Config{Path: dir})
	require.NoError(t, err)
	fetcher := &viBlockFetcher{reader: backend.NewReader(rawR)}

	_, err = fetcher.ListBlocksInRange(context.Background(), tenant, 0, ^uint64(0))
	require.Error(t, err, "a block whose meta.json cannot be read must fail the whole listing, not be silently skipped")
}

// TestViBlockFetcher_ListBlocksInRange_SucceedsWhenAllMetaReadable is the positive-path
// companion: with every block's meta.json genuinely readable, listing succeeds and
// returns both blocks.
func TestViBlockFetcher_ListBlocksInRange_SucceedsWhenAllMetaReadable(t *testing.T) {
	dir := t.TempDir()
	const tenant = "test-tenant"

	writeEmptyBlock(t, dir, tenant, uuid.New())
	writeEmptyBlock(t, dir, tenant, uuid.New())

	rawR, _, _, err := local.New(&local.Config{Path: dir})
	require.NoError(t, err)
	fetcher := &viBlockFetcher{reader: backend.NewReader(rawR)}

	refs, err := fetcher.ListBlocksInRange(context.Background(), tenant, 0, ^uint64(0))
	require.NoError(t, err)
	require.Len(t, refs, 2)
}
