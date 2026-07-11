package vblockpack

// vi_watermark_cache_wiring_test.go — go-presubmit.md's top CRITICAL finding
// fix: the fully-tested viWatermarkCache was never called from any real
// query call site (every one of value_index_query.go/
// value_index_structural_query.go/backend_block.go's 6 BuildValueIndexSource/
// ForMetrics calls passed a literal nil). Every existing R7 test exercised
// BuildValueIndexSource directly with an explicit watermarks argument, never
// through the real call site -- exactly the gap that let the wiring miss
// ship silently. This file closes that gap: a REAL block, a REAL VI file
// written for it, a REAL watermark cache seeded with a Triggered=true,
// Done=false entry whose WatermarkSec does NOT cover the query's window,
// driving the actual public entry point (block.Fetch -> tryIndexFetch) and
// asserting the query is correctly declined (ErrSearchNoCoverage, the R7/F-7
// hard-error-not-silent-scan contract) rather than silently answering from
// the on-disk file as if coverage were complete.

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	blockpack "github.com/grafana/blockpack"
	"github.com/grafana/tempo/pkg/traceql"
	"github.com/grafana/tempo/tempodb/backend"
	"github.com/grafana/tempo/tempodb/backend/local"
	"github.com/grafana/tempo/tempodb/encoding/common"
)

// withViWatermarkCache installs cache as the process-level watermark cache
// for the duration of the test, restoring the prior value on cleanup —
// mirrors withVIQueryReader/withUsageRecorder's identical singleton-swap
// pattern.
func withViWatermarkCache(t *testing.T, cache *viWatermarkCache) {
	t.Helper()
	viWatermarkCacheMu.Lock()
	prev := viWatermarkCachePtr
	viWatermarkCachePtr = cache
	viWatermarkCacheMu.Unlock()
	t.Cleanup(func() {
		viWatermarkCacheMu.Lock()
		viWatermarkCachePtr = prev
		viWatermarkCacheMu.Unlock()
	})
}

// seedTriggeredWatermarkEntry records one use (Threshold=1 triggers
// immediately) for (tenant, colName, colType) against store, then persists
// watermarkSec via UpdateWatermark with Done=false — an in-progress backfill
// whose confirmed-complete coverage starts at watermarkSec, exactly
// blockpack's own R7 adversarial-test setup, just against tempo's real
// viUsageObjectStore-shaped store instead of blockpack's internal fake.
func seedTriggeredWatermarkEntry(t *testing.T, store blockpack.ObjectStore, tenant, colName, colType string, watermarkSec uint64) {
	t.Helper()
	registry := blockpack.NewRegistry(store, tenant)
	result, err := blockpack.RecordUseAndMaybeTrigger(
		context.Background(), registry, tenant, colName, colType, time.Now(),
		blockpack.TriggerConfig{LeaseTTLSeconds: 1800},
	)
	require.NoError(t, err)
	require.True(t, result.ShouldBackfill)
	require.NoError(t, registry.UpdateWatermark(
		context.Background(), tenant, result.Entry.ColumnHash, result.Entry.ColumnType,
		watermarkSec, 0, watermarkSec, false,
	))
}

// TestTryIndexFetch_RealWatermarkCache_DeclinesOnPartialCoverage is the
// mandatory true end-to-end test: a real VI file exists on disk for
// resource.service.name (writeSvcBlock's real WriteValueIndexL0 write path),
// but the watermark cache reports that column as Triggered/not-Done with a
// WatermarkSec set to a point AFTER the block's actual span time -- i.e. the
// on-disk file's coverage is NOT yet confirmed complete for the query's
// window. Driving the real block.Fetch entry point (not BuildValueIndexSource
// directly) must decline this query rather than silently answering from the
// discoverable-but-unconfirmed file.
func TestTryIndexFetch_RealWatermarkCache_DeclinesOnPartialCoverage(t *testing.T) {
	const tenant = "test-tenant"
	const col = "resource.service.name"

	viStore := &fakeVISink{}
	withVISink(t, viStore, "indexes")
	withVIQueryReader(t, viStore, "indexes")

	dir := t.TempDir()
	metaA, _ := writeSvcBlock(t, dir, viStore, tenant, uuid.New(), "svc-alpha", 3)
	rawR, _, _, err := local.New(&local.Config{Path: dir})
	require.NoError(t, err)
	block := newBackendBlock(metaA, backend.NewReader(rawR))

	// The block's spans are written at "now" (writeSvcBlock). Set the
	// watermark to confirm coverage only from 1 hour in the FUTURE --
	// meaning even "now" (the span's real time) is NOT yet confirmed
	// covered, simulating a still-in-progress backfill.
	watermarkSec := uint64(time.Now().Add(time.Hour).Unix()) //nolint:gosec

	objStore := newFakeViObjectStore()
	seedTriggeredWatermarkEntry(t, objStore, tenant, col, "string", watermarkSec)
	withViWatermarkCache(t, newViWatermarkCache(objStore, time.Minute))

	query := `{ resource.service.name = "svc-alpha" }`
	ctx := common.WithOriginalTraceQLQuery(context.Background(), query, false)
	req := traceql.FetchSpansRequest{
		Conditions: []traceql.Condition{{
			Attribute: traceql.NewScopedAttribute(traceql.AttributeScopeResource, false, "service.name"),
			Op:        traceql.OpEqual,
			Operands:  traceql.Operands{traceql.NewStaticString("svc-alpha")},
		}},
		AllConditions: true,
	}
	// No limit set (MaxTraces: 0) -> boundedAuthorized is false -> a routine
	// decline hard-errors with ErrSearchNoCoverage instead of silently
	// falling back to a scan (F-7/R7's "never an implicit scan" contract) --
	// this is the directly observable signal that the index path declined
	// rather than answering from the on-disk-but-unconfirmed file.
	_, err = block.Fetch(ctx, req, common.SearchOptions{})
	require.Error(t, err, "the watermark gate must decline this query through the REAL call site, not silently answer from the discoverable file")
	assert.True(t, errors.Is(err, ErrSearchNoCoverage), "expected ErrSearchNoCoverage, got %v", err)
}

// TestTryIndexFetch_RealWatermarkCache_ControlCoveredRangeSucceeds is the
// control sibling: the SAME on-disk file, the SAME watermark entry, but a
// watermark that already confirms coverage (WatermarkSec in the past) must
// let the query succeed from the index -- proving the decline above is
// caused by partial coverage specifically, not a blanket "any watermarked
// column always declines" bug.
func TestTryIndexFetch_RealWatermarkCache_ControlCoveredRangeSucceeds(t *testing.T) {
	const tenant = "test-tenant"
	const col = "resource.service.name"

	viStore := &fakeVISink{}
	withVISink(t, viStore, "indexes")
	withVIQueryReader(t, viStore, "indexes")

	dir := t.TempDir()
	metaA, _ := writeSvcBlock(t, dir, viStore, tenant, uuid.New(), "svc-alpha", 3)
	rawR, _, _, err := local.New(&local.Config{Path: dir})
	require.NoError(t, err)
	block := newBackendBlock(metaA, backend.NewReader(rawR))

	// Watermark confirms coverage from an hour AGO -- the span's real "now"
	// time is already covered.
	now := time.Now()
	watermarkSec := uint64(now.Add(-time.Hour).Unix()) //nolint:gosec

	objStore := newFakeViObjectStore()
	seedTriggeredWatermarkEntry(t, objStore, tenant, col, "string", watermarkSec)
	withViWatermarkCache(t, newViWatermarkCache(objStore, time.Minute))

	query := `{ resource.service.name = "svc-alpha" }`
	ctx := common.WithOriginalTraceQLQuery(context.Background(), query, false)
	req := traceql.FetchSpansRequest{
		Conditions: []traceql.Condition{{
			Attribute: traceql.NewScopedAttribute(traceql.AttributeScopeResource, false, "service.name"),
			Op:        traceql.OpEqual,
			Operands:  traceql.Operands{traceql.NewStaticString("svc-alpha")},
		}},
		AllConditions: true,
		// Explicit, BOUNDED window starting comfortably AFTER the watermark
		// (not an unbounded query, which would floor minSec to 0 and always
		// decline against any positive watermark regardless of coverage) and
		// ending comfortably after "now" so the block's real span falls
		// inside it.
		StartTimeUnixNanos: uint64(now.Add(-50 * time.Minute).UnixNano()), //nolint:gosec
		EndTimeUnixNanos:   uint64(now.Add(time.Hour).UnixNano()),         //nolint:gosec
	}
	resp, err := block.Fetch(ctx, req, common.SearchOptions{})
	require.NoError(t, err, "a fully-covered watermark must let the query succeed from the index")
	require.NotNil(t, resp.Results)
}
