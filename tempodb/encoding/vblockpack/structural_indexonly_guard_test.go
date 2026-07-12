package vblockpack

// structural_indexonly_guard_test.go — Phase 6 (plan-scan-fallback.md) hard-constraint pin:
// tryStructuralIndexFetch's !indexOnly early-return is a PERMANENT, correctness-required
// boundary (see value_index_structural_query.go's own package doc comment for the full
// duplication-risk rationale), never a stale interim gate to be relaxed once Phase 5's
// newest-first ordering fix or DT1's DispatchTimeSliced shipment landed. This test pins that
// the guard fires unconditionally for every indexOnly=false call, REGARDLESS of vr state or
// genuine index coverage -- proven with a spy showing the store is never even consulted.

import (
	"context"
	"testing"

	"github.com/google/uuid"
	blockpack "github.com/grafana/blockpack"
	"github.com/grafana/tempo/tempodb/backend"
	"github.com/grafana/tempo/tempodb/backend/local"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestTryStructuralIndexFetch_NeverAnswersUnderPerBlockDispatch is Phase 6's dedicated
// regression guard for the asymmetry hard constraint: tryStructuralIndexFetch must still return
// its routine (nil, false, stats, nil) decline for every indexOnly=false call, even with a
// REAL, fully-covered value index configured (writeParentChildBlock + withVISink) -- if the
// guard were ever relaxed, this exact fixture WOULD answer from the index (proven by
// TestFetch_StructuralQuery_TriesIndexPathBeforeScanFallback's own indexOnly=true variant using
// the identical fixture), so a false pass here would mean the guard is gone, not merely that
// coverage happened to be absent.
func TestTryStructuralIndexFetch_NeverAnswersUnderPerBlockDispatch(t *testing.T) {
	dir := t.TempDir()
	viStore := &fakeVISink{}
	withVISink(t, viStore, "indexes")
	spy := &spyValueIndexStore{valueIndexStore: viStore}
	withVIQueryReader(t, spy, "indexes")

	meta, _ := writeParentChildBlock(t, dir, uuid.New())

	rawR, _, _, err := local.New(&local.Config{Path: dir})
	require.NoError(t, err)
	block := newBackendBlock(meta, backend.NewReader(rawR))

	matches, ok, _, idxErr := block.tryStructuralIndexFetch(
		context.Background(), structQuery, blockpack.QueryOptions{}, false, /* indexOnly */
	)
	require.NoError(t, idxErr, "the routine decline itself must not be an error")
	assert.False(t, ok, "tryStructuralIndexFetch must decline under per-block (indexOnly=false) dispatch, even with full index coverage available")
	assert.Nil(t, matches)
	assert.False(t, spy.called.Load(),
		"the guard must fire BEFORE ever consulting the value-index store -- proves this is a "+
			"dispatch-shape gate, not an incidental coverage-driven decline")
}
