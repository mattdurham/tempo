package vblockpack

// vi_column_policy_wiring_test.go — go-presubmit.md/review.md's Issue 2 CRITICAL
// finding fix: all 3 real VI write paths (create.go, compactor.go,
// cmd/tempo/app/value_index.go) hardcoded a disabled blockpack.ColumnPolicy{},
// making R2/R12's write-path policy a no-op in production regardless of config.
// This file's mandatory test drives the REAL public entry point (CreateBlock,
// not WriteValueIndexL0 directly) with DedicatedColumnsEnabled=true and a real
// dedicated list, asserting a non-dedicated, non-triggered column's block
// write does NOT produce a standalone L0 file for it, while a dedicated
// column's write DOES -- no such test existed before this fix; every prior
// tempo-side write-path test implicitly passed/expected ColumnPolicy{}.

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	blockpack "github.com/grafana/blockpack"
	tempopb "github.com/grafana/tempo/pkg/tempopb"
	tempocommon "github.com/grafana/tempo/pkg/tempopb/common/v1"
	temporesource "github.com/grafana/tempo/pkg/tempopb/resource/v1"
	tempotrace "github.com/grafana/tempo/pkg/tempopb/trace/v1"
	"github.com/grafana/tempo/tempodb/backend"
	"github.com/grafana/tempo/tempodb/backend/local"
	"github.com/grafana/tempo/tempodb/encoding/common"
)

// TestCreateBlock_RealColumnPolicy_NonDedicatedColumnNotIndexed drives the
// real CreateBlock entry point with DedicatedColumnsEnabled=true and a
// dedicated list containing ONLY resource.service.name. A trace also carries
// a non-dedicated, never-triggered span attribute. The resulting VI write
// must produce a standalone L0 file for resource.service.name but NOT for
// the non-dedicated attribute.
func TestCreateBlock_RealColumnPolicy_NonDedicatedColumnNotIndexed(t *testing.T) {
	viStore := &fakeVISink{}
	withVISink(t, viStore, "indexes")
	// No watermark cache installed -> triggeredColumnsOrNil returns nil for
	// every tenant, which is the correct, safe default: nothing has been
	// usage-triggered, so BuildColumnPolicy's Allow set is exactly the
	// dedicated list.
	withViWatermarkCache(t, nil)

	dir := t.TempDir()
	rawR, rawW, _, err := local.New(&local.Config{Path: dir})
	require.NoError(t, err)

	traceID := []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 0, 0, 0, 0, 0, 1}
	trace := &tempopb.Trace{
		ResourceSpans: []*tempotrace.ResourceSpans{{
			Resource: &temporesource.Resource{
				Attributes: []*tempocommon.KeyValue{{
					Key: "service.name",
					Value: &tempocommon.AnyValue{
						Value: &tempocommon.AnyValue_StringValue{StringValue: "svc-alpha"},
					},
				}},
			},
			ScopeSpans: []*tempotrace.ScopeSpans{{Spans: []*tempotrace.Span{{
				TraceId: traceID,
				SpanId:  []byte{1, 2, 3, 4, 5, 6, 7, 8},
				Name:    "op",
				Attributes: []*tempocommon.KeyValue{{
					Key: "never.triggered",
					Value: &tempocommon.AnyValue{
						Value: &tempocommon.AnyValue_StringValue{StringValue: "x"},
					},
				}},
			}}}},
		}},
	}
	iter := &mockIterator{traces: []*tempopb.Trace{trace}, ids: [][]byte{traceID}}

	tenant := "test-tenant"
	blockID := uuid.New()
	meta := backend.NewBlockMeta(tenant, blockID, VersionString)

	cfg := &common.BlockConfig{RowGroupSizeBytes: 200}
	cfg.Blockpack.ViUsage = common.ViUsageConfig{
		DedicatedColumnsEnabled:  true,
		DedicatedColumnsOverride: []string{"resource.service.name"},
	}

	_, err = CreateBlock(context.Background(), cfg, meta, iter, backend.NewReader(rawR), backend.NewWriter(rawW))
	require.NoError(t, err)

	dedicatedHash := "/" + valueIndexColHash(t, "resource.service.name") + "/"
	nonDedicatedHash := "/" + valueIndexColHash(t, "span.never.triggered") + "/"

	viStore.mu.Lock()
	defer viStore.mu.Unlock()
	var sawDedicated, sawNonDedicated bool
	for k := range viStore.objs {
		if contains(k, dedicatedHash) {
			sawDedicated = true
		}
		if contains(k, nonDedicatedHash) {
			sawNonDedicated = true
		}
	}
	assert.True(t, sawDedicated, "the dedicated column must produce a standalone L0 file, keys=%v", keysOf(viStore.objs))
	assert.False(t, sawNonDedicated, "the non-dedicated, non-triggered column must NOT produce a standalone L0 file, keys=%v", keysOf(viStore.objs))
}

// TestCreateBlock_RealColumnPolicy_DisabledIndexesEverything is the R12
// safety-valve control: DedicatedColumnsEnabled=false must reproduce
// pre-#496 behavior exactly -- every column indexed, including one that is
// neither dedicated nor triggered.
func TestCreateBlock_RealColumnPolicy_DisabledIndexesEverything(t *testing.T) {
	viStore := &fakeVISink{}
	withVISink(t, viStore, "indexes")
	withViWatermarkCache(t, nil)

	dir := t.TempDir()
	rawR, rawW, _, err := local.New(&local.Config{Path: dir})
	require.NoError(t, err)

	traceID := []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 0, 0, 0, 0, 0, 2}
	trace := &tempopb.Trace{
		ResourceSpans: []*tempotrace.ResourceSpans{{
			ScopeSpans: []*tempotrace.ScopeSpans{{Spans: []*tempotrace.Span{{
				TraceId: traceID,
				SpanId:  []byte{2, 2, 3, 4, 5, 6, 7, 8},
				Name:    "op",
				Attributes: []*tempocommon.KeyValue{{
					Key: "never.triggered",
					Value: &tempocommon.AnyValue{
						Value: &tempocommon.AnyValue_StringValue{StringValue: "x"},
					},
				}},
			}}}},
		}},
	}
	iter := &mockIterator{traces: []*tempopb.Trace{trace}, ids: [][]byte{traceID}}

	tenant := "test-tenant"
	blockID := uuid.New()
	meta := backend.NewBlockMeta(tenant, blockID, VersionString)

	cfg := &common.BlockConfig{RowGroupSizeBytes: 200}
	cfg.Blockpack.ViUsage = common.ViUsageConfig{DedicatedColumnsEnabled: false}

	_, err = CreateBlock(context.Background(), cfg, meta, iter, backend.NewReader(rawR), backend.NewWriter(rawW))
	require.NoError(t, err)

	nonDedicatedHash := "/" + valueIndexColHash(t, "span.never.triggered") + "/"
	viStore.mu.Lock()
	defer viStore.mu.Unlock()
	var sawNonDedicated bool
	for k := range viStore.objs {
		if contains(k, nonDedicatedHash) {
			sawNonDedicated = true
		}
	}
	assert.True(t, sawNonDedicated, "DedicatedColumnsEnabled=false must index every column, keys=%v", keysOf(viStore.objs))
}

// valueIndexColHash mirrors cube_backfill.go's reuse of blockpack.VCNTColHash
// to compute a value-index column-hash path segment: both valuecounts.ColHash
// and valueindex.ColHash are lower_hex(SHA-256(colName)[:16]), the identical
// algorithm, so the exported VCNTColHash gives the same result without this
// tempo-side test needing to import blockpack's internal valueindex package.
func valueIndexColHash(_ *testing.T, colName string) string {
	return blockpack.VCNTColHash(colName)
}

func contains(s, substr string) bool {
	return len(s) >= len(substr) && (func() bool {
		for i := 0; i+len(substr) <= len(s); i++ {
			if s[i:i+len(substr)] == substr {
				return true
			}
		}
		return false
	})()
}

func keysOf(m map[string][]byte) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}
