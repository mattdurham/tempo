package vblockpack

// valueindex_test.go — tests for the writer-side synchronous value-index write
// path (NOTE-VI-042, issue #464): the sink singleton and its integration into
// CreateBlock. The S3 client construction is exercised by deployment, not unit
// tested here; these tests inject a fake ObjectPutter directly (same-package).

import (
	"context"
	"io"
	"strings"
	"sync"
	"testing"
	"time"

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

// fakeVISink captures Put calls so a test can assert index files were written. It also
// implements the read side (List/Get/Size/ReadAt, blockpack.LookupStore +
// blockpack.ValueIndexFileStore) so the same instance can drive withVIQueryReader
// (value_index_query_test.go) and let a test exercise a real write-then-read round trip
// through the authoritative value index without a live object store.
type fakeVISink struct {
	objs map[string][]byte
	mu   sync.Mutex
}

var (
	_ blockpack.ObjectPutter        = (*fakeVISink)(nil)
	_ blockpack.LookupStore         = (*fakeVISink)(nil)
	_ blockpack.ValueIndexFileStore = (*fakeVISink)(nil)
)

func (f *fakeVISink) Put(key string, data []byte) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.objs == nil {
		f.objs = map[string][]byte{}
	}
	cp := make([]byte, len(data))
	copy(cp, data)
	f.objs[key] = cp
	return nil
}

func (f *fakeVISink) List(_ context.Context, prefix string) ([]string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	var keys []string
	for k := range f.objs {
		if strings.HasPrefix(k, prefix) {
			keys = append(keys, k)
		}
	}
	return keys, nil
}

func (f *fakeVISink) Get(_ context.Context, key string) ([]byte, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	data, ok := f.objs[key]
	if !ok {
		return nil, blockpack.ErrValueIndexFileNotFound
	}
	return data, nil
}

func (f *fakeVISink) Size(key string) (int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	data, ok := f.objs[key]
	if !ok {
		return 0, blockpack.ErrValueIndexFileNotFound
	}
	return int64(len(data)), nil
}

func (f *fakeVISink) ReadAt(key string, p []byte, off int64) (int, error) {
	f.mu.Lock()
	data, ok := f.objs[key]
	f.mu.Unlock()
	if !ok {
		return 0, blockpack.ErrValueIndexFileNotFound
	}
	if off >= int64(len(data)) {
		return 0, io.EOF
	}
	n := copy(p, data[off:])
	if n < len(p) {
		return n, io.ErrUnexpectedEOF
	}
	return n, nil
}

// withVISink installs sink as the process-level value-index sink for the duration
// of the test and restores the prior state on cleanup. The configure-once guard is
// bypassed by setting the package vars directly (same-package test).
func withVISink(t *testing.T, sink blockpack.ObjectPutter, prefix string) {
	t.Helper()
	restoreExplicit := markViExplicitlySetForTest()
	valueIndexSinkMu.Lock()
	prevSink, prevPrefix := valueIndexSink, valueIndexPrefix
	valueIndexSink, valueIndexPrefix = sink, prefix
	valueIndexSinkMu.Unlock()
	t.Cleanup(func() {
		valueIndexSinkMu.Lock()
		valueIndexSink, valueIndexPrefix = prevSink, prevPrefix
		valueIndexSinkMu.Unlock()
		restoreExplicit()
	})
}

func TestGetValueIndexSink_DisabledByDefault(t *testing.T) {
	// With nothing configured (and no prior test leaking state), the sink getter
	// returns nil so create.go / compactor.go skip the index write entirely.
	withVISink(t, nil, "")
	store, prefix := getValueIndexSink()
	assert.Nil(t, store)
	assert.Empty(t, prefix)
}

func TestConfigureValueIndex_DisabledIsNoop(t *testing.T) {
	withVISink(t, nil, "")
	// enabled=false must not configure a sink even with a non-nil-looking config.
	ConfigureValueIndex(false, nil, nil, "indexes")
	store, _ := getValueIndexSink()
	assert.Nil(t, store, "disabled config must leave the sink unset")
}

func TestConfigureValueIndex_GenericBackendUsesRawObjectPutter(t *testing.T) {
	withVISink(t, nil, "")
	valueIndexConfigOnce = sync.Once{}
	_, rawW := newLocalRawBackend(t)

	ConfigureValueIndex(true, nil, rawW, "indexes")

	store, prefix := getValueIndexSink()
	require.NotNil(t, store, "generic rawW path must configure a sink")
	assert.Equal(t, "indexes", prefix)
	_, ok := store.(*rawObjectPutter)
	assert.True(t, ok, "expected the generic path to install a *rawObjectPutter, got %T", store)
	assert.Same(t, store, getVCNTSink(), "valueIndexSink and vcntSink must share the same putter on the generic path")
}

func TestCreateBlock_WritesValueIndexL0(t *testing.T) {
	sink := &fakeVISink{}
	withVISink(t, sink, "indexes")

	ctx := context.Background()
	cfg := &common.BlockConfig{RowGroupSizeBytes: 100 * 1024 * 1024}

	// Two traces with distinct service names so the index has a queryable column.
	traceA := createTestTraceWithService([]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}, "svc-a")
	traceB := createTestTraceWithService([]byte{2, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}, "svc-b")
	iter := &mockIterator{
		traces: []*tempopb.Trace{traceA, traceB},
		ids:    [][]byte{{1}, {2}},
	}

	tempDir := t.TempDir()
	rawR, rawW, _, err := local.New(&local.Config{Path: tempDir})
	require.NoError(t, err)
	r := backend.NewReader(rawR)
	w := backend.NewWriter(rawW)
	meta := backend.NewBlockMeta("test-tenant", uuid.New(), VersionString)

	_, err = CreateBlock(ctx, cfg, meta, iter, r, w)
	require.NoError(t, err)

	sink.mu.Lock()
	defer sink.mu.Unlock()
	require.NotEmpty(t, sink.objs, "CreateBlock must write at least one L0 value-index file")
	for k := range sink.objs {
		assert.True(t, strings.HasPrefix(k, "test-tenant/indexes/"),
			"index key %q must be under <tenant>/<prefix>/", k)
		assert.True(t, strings.HasSuffix(k, ".blockpack"), "index key %q must end .blockpack", k)
	}
}

// TestFindTraceByID_QueryWindowIsMinuteFloored is the regression test for
// backend_block.go's queryMinSec flooring (floorToMinuteSec, value_index_query.go).
// The block's own StartTime is real wall-clock time and essentially never falls
// exactly on a minute boundary, while the write-side TimeSec is always floored to
// the minute (blockpack valueindex_extract.go:buildSpanStartSecByRef). Without the
// floor on the query side, DiscoverIndexFiles reports zero covering files for a
// block whose only span landed just after a minute boundary, which NOTE-VI-072
// treats as a hard coverage-gap error, not a scan-fallback-masked non-issue.
func TestFindTraceByID_QueryWindowIsMinuteFloored(t *testing.T) {
	store := &fakeVISink{}
	withVISink(t, store, "indexes")
	withVIQueryReader(t, store, "indexes")

	ctx := context.Background()
	cfg := &common.BlockConfig{RowGroupSizeBytes: 100 * 1024 * 1024}

	// CreateBlock does not itself populate meta.StartTime/EndTime (create.go: that's
	// left to the caller -- WAL bookkeeping in production, setBlockTimeRange for
	// compaction output). A realistic caller sets it from the same real wall-clock
	// span timestamps written into the block, which essentially never land exactly on
	// a minute boundary -- that misalignment is exactly what this test exercises.
	now := time.Now()
	traceID := []byte{7, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}
	trace := &tempopb.Trace{
		ResourceSpans: []*tempotrace.ResourceSpans{{
			Resource: &temporesource.Resource{Attributes: []*tempocommon.KeyValue{{
				Key:   "service.name",
				Value: &tempocommon.AnyValue{Value: &tempocommon.AnyValue_StringValue{StringValue: "svc-floor"}},
			}}},
			ScopeSpans: []*tempotrace.ScopeSpans{{
				Spans: []*tempotrace.Span{{
					TraceId:           traceID,
					SpanId:            []byte{1, 0, 0, 0, 0, 0, 0, 1},
					Name:              "test-span",
					StartTimeUnixNano: uint64(now.UnixNano()),             //nolint:gosec // test data
					EndTimeUnixNano:   uint64(now.UnixNano()) + 1_000_000, //nolint:gosec // test data
				}},
			}},
		}},
	}
	iter := &mockIterator{traces: []*tempopb.Trace{trace}, ids: [][]byte{traceID}}

	tempDir := t.TempDir()
	rawR, rawW, _, err := local.New(&local.Config{Path: tempDir})
	require.NoError(t, err)
	r := backend.NewReader(rawR)
	w := backend.NewWriter(rawW)
	meta := backend.NewBlockMeta("test-tenant", uuid.New(), VersionString)
	meta.StartTime = now
	meta.EndTime = now.Add(time.Second)

	blockMeta, err := CreateBlock(ctx, cfg, meta, iter, r, w)
	require.NoError(t, err)

	block := newBackendBlock(blockMeta, r)
	resp, err := block.FindTraceByID(ctx, traceID, common.SearchOptions{})
	require.NoError(t, err, "a block whose StartTime isn't minute-aligned must still find its own trace")
	require.NotNil(t, resp)
	require.NotNil(t, resp.Trace)
}

func TestCreateBlock_NoSinkWritesNoIndex(t *testing.T) {
	withVISink(t, nil, "")

	ctx := context.Background()
	cfg := &common.BlockConfig{RowGroupSizeBytes: 100 * 1024 * 1024}
	traceID := []byte{9, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}
	iter := &mockIterator{
		traces: []*tempopb.Trace{createTestTrace(traceID, 2)},
		ids:    [][]byte{traceID},
	}
	tempDir := t.TempDir()
	rawR, rawW, _, err := local.New(&local.Config{Path: tempDir})
	require.NoError(t, err)
	// Must complete cleanly with no panic and no index sink — byte-identical to
	// the pre-#464 behaviour.
	_, err = CreateBlock(ctx, cfg, backend.NewBlockMeta("t", uuid.New(), VersionString),
		iter, backend.NewReader(rawR), backend.NewWriter(rawW))
	require.NoError(t, err)
}

// createTestTraceWithService builds a single-span trace with a resource
// service.name so the value index has a string column with distinct values.
func createTestTraceWithService(traceID []byte, svc string) *tempopb.Trace {
	now := uint64(time.Now().UnixNano())
	svcAttr := &tempocommon.KeyValue{
		Key:   "service.name",
		Value: &tempocommon.AnyValue{Value: &tempocommon.AnyValue_StringValue{StringValue: svc}},
	}
	return &tempopb.Trace{
		ResourceSpans: []*tempotrace.ResourceSpans{{
			Resource: &temporesource.Resource{Attributes: []*tempocommon.KeyValue{svcAttr}},
			ScopeSpans: []*tempotrace.ScopeSpans{{
				Spans: []*tempotrace.Span{{
					TraceId:           traceID,
					SpanId:            []byte{1, 0, 0, 0, 0, 0, 0, 1},
					Name:              "test-span",
					StartTimeUnixNano: now,
					EndTimeUnixNano:   now + 1_000_000,
				}},
			}},
		}},
	}
}
