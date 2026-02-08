package vblockpack

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/grafana/tempo/pkg/tempopb"
	tempotrace "github.com/grafana/tempo/pkg/tempopb/trace/v1"
	"github.com/grafana/tempo/pkg/traceql"
	"github.com/grafana/tempo/tempodb/backend"
	"github.com/grafana/tempo/tempodb/backend/local"
	"github.com/grafana/tempo/tempodb/encoding/common"
)

// TestWALBlockBasicOperations tests basic WAL block operations
func TestWALBlockBasicOperations(t *testing.T) {
	// Create temp directory for test
	tmpDir := t.TempDir()
	walDir := filepath.Join(tmpDir, "wal")

	// Create block metadata
	meta := backend.NewBlockMeta("test-tenant", uuid.New(), VersionString)
	meta.StartTime = time.Now().Add(-time.Hour)
	meta.EndTime = time.Now()

	// Create WAL block
	block, err := createWALBlock(meta, walDir, time.Minute)
	if err != nil {
		t.Fatalf("failed to create WAL block: %v", err)
	}

	// Create test trace
	trace := &tempopb.Trace{
		ResourceSpans: []*tempotrace.ResourceSpans{
			{
				ScopeSpans: []*tempotrace.ScopeSpans{
					{
						Spans: []*tempotrace.Span{
							{
								TraceId:           []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16},
								SpanId:            []byte{1, 2, 3, 4, 5, 6, 7, 8},
								Name:              "test-span",
								StartTimeUnixNano: uint64(time.Now().UnixNano()),
								EndTimeUnixNano:   uint64(time.Now().Add(time.Second).UnixNano()),
							},
						},
					},
				},
			},
		},
	}

	// Append trace
	traceID := common.ID(trace.ResourceSpans[0].ScopeSpans[0].Spans[0].TraceId)
	start := uint32(meta.StartTime.Unix())
	end := uint32(meta.EndTime.Unix())

	err = block.AppendTrace(traceID, trace, start, end, false)
	if err != nil {
		t.Fatalf("failed to append trace: %v", err)
	}

	// Verify data length increased
	if block.DataLength() == 0 {
		t.Fatalf("expected data length > 0, got 0")
	}

	// Flush block
	err = block.Flush()
	if err != nil {
		t.Fatalf("failed to flush block: %v", err)
	}

	// Verify file was created
	dataFile := filepath.Join(walDir, DataFileName)
	if _, err := os.Stat(dataFile); os.IsNotExist(err) {
		t.Fatalf("expected data file to exist at %s", dataFile)
	}

	// Clean up
	err = block.Clear()
	if err != nil {
		t.Fatalf("failed to clear block: %v", err)
	}
}

// TestCreateBlockBasicOperations tests CreateBlock functionality
func TestCreateBlockBasicOperations(t *testing.T) {
	tmpDir := t.TempDir()

	// Set up backend storage
	rawReader, rawWriter, _, err := local.New(&local.Config{Path: tmpDir})
	if err != nil {
		t.Fatalf("failed to create local backend: %v", err)
	}

	reader := backend.NewReader(rawReader)
	writer := backend.NewWriter(rawWriter)

	// Create test trace
	trace := &tempopb.Trace{
		ResourceSpans: []*tempotrace.ResourceSpans{
			{
				ScopeSpans: []*tempotrace.ScopeSpans{
					{
						Spans: []*tempotrace.Span{
							{
								TraceId:           []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16},
								SpanId:            []byte{1, 2, 3, 4, 5, 6, 7, 8},
								Name:              "test-span",
								StartTimeUnixNano: uint64(time.Now().UnixNano()),
								EndTimeUnixNano:   uint64(time.Now().Add(time.Second).UnixNano()),
							},
						},
					},
				},
			},
		},
	}

	// Create iterator with test trace
	iter := &testIterator{
		traces: []struct {
			id    common.ID
			trace *tempopb.Trace
		}{
			{
				id:    common.ID(trace.ResourceSpans[0].ScopeSpans[0].Spans[0].TraceId),
				trace: trace,
			},
		},
	}

	// Create block metadata
	meta := backend.NewBlockMeta("test-tenant", uuid.New(), VersionString)

	// Configure block
	cfg := &common.BlockConfig{
		RowGroupSizeBytes: 10000,
	}

	// Create block
	ctx := context.Background()
	resultMeta, err := CreateBlock(ctx, cfg, meta, iter, reader, writer)
	if err != nil {
		t.Fatalf("failed to create block: %v", err)
	}

	// Verify metadata was updated
	if resultMeta.TotalObjects != 1 {
		t.Errorf("expected TotalObjects=1, got %d", resultMeta.TotalObjects)
	}
	if resultMeta.Size_ == 0 {
		t.Errorf("expected Size>0, got 0")
	}
}

// testIterator is a simple iterator implementation for testing
type testIterator struct {
	traces []struct {
		id    common.ID
		trace *tempopb.Trace
	}
	index int
}

func (i *testIterator) Next(ctx context.Context) (common.ID, *tempopb.Trace, error) {
	if i.index >= len(i.traces) {
		return nil, nil, io.EOF
	}
	item := i.traces[i.index]
	i.index++
	return item.id, item.trace, nil
}

func (i *testIterator) Close() {}

// TestBackendBlockFindTraceByID tests FindTraceByID with end-to-end trace reconstruction
func TestBackendBlockFindTraceByID(t *testing.T) {
	tmpDir := t.TempDir()

	// Set up backend storage
	rawReader, rawWriter, _, err := local.New(&local.Config{Path: tmpDir})
	if err != nil {
		t.Fatalf("failed to create local backend: %v", err)
	}

	reader := backend.NewReader(rawReader)
	writer := backend.NewWriter(rawWriter)

	// Create test trace with known ID
	traceIDBytes := []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}
	trace := &tempopb.Trace{
		ResourceSpans: []*tempotrace.ResourceSpans{
			{
				ScopeSpans: []*tempotrace.ScopeSpans{
					{
						Spans: []*tempotrace.Span{
							{
								TraceId:           traceIDBytes,
								SpanId:            []byte{1, 2, 3, 4, 5, 6, 7, 8},
								Name:              "test-span",
								StartTimeUnixNano: uint64(time.Now().UnixNano()),
								EndTimeUnixNano:   uint64(time.Now().Add(time.Second).UnixNano()),
							},
						},
					},
				},
			},
		},
	}

	// Create iterator
	iter := &testIterator{
		traces: []struct {
			id    common.ID
			trace *tempopb.Trace
		}{
			{
				id:    common.ID(traceIDBytes),
				trace: trace,
			},
		},
	}

	// Create block metadata
	meta := backend.NewBlockMeta("test-tenant", uuid.New(), VersionString)

	// Configure block
	cfg := &common.BlockConfig{
		RowGroupSizeBytes: 10000,
	}

	// Create blockpack block
	ctx := context.Background()
	resultMeta, err := CreateBlock(ctx, cfg, meta, iter, reader, writer)
	if err != nil {
		t.Fatalf("failed to create block: %v", err)
	}

	// Now open as backend block
	backendBlock := newBackendBlock(resultMeta, reader)

	// Try to find the trace by ID
	response, err := backendBlock.FindTraceByID(ctx, common.ID(traceIDBytes), common.SearchOptions{})
	if err != nil {
		t.Fatalf("FindTraceByID failed: %v", err)
	}

	// Verify we got a trace back
	if response == nil || response.Trace == nil {
		t.Fatalf("expected trace to be found, got nil")
	}

	// Verify trace has correct structure
	if len(response.Trace.ResourceSpans) == 0 {
		t.Fatalf("expected at least one ResourceSpan")
	}
	if len(response.Trace.ResourceSpans[0].ScopeSpans) == 0 {
		t.Fatalf("expected at least one ScopeSpan")
	}
	if len(response.Trace.ResourceSpans[0].ScopeSpans[0].Spans) == 0 {
		t.Fatalf("expected at least one Span")
	}

	// Verify span details
	foundSpan := response.Trace.ResourceSpans[0].ScopeSpans[0].Spans[0]
	if foundSpan.Name != "test-span" {
		t.Errorf("expected span name 'test-span', got '%s'", foundSpan.Name)
	}
	if string(foundSpan.TraceId) != string(traceIDBytes) {
		t.Errorf("expected trace ID to match")
	}

	// Try to find a non-existent trace
	nonExistentID := common.ID([]byte{99, 99, 99, 99, 99, 99, 99, 99, 99, 99, 99, 99, 99, 99, 99, 99})
	response2, err := backendBlock.FindTraceByID(ctx, nonExistentID, common.SearchOptions{})
	if err != nil {
		t.Fatalf("FindTraceByID failed for non-existent trace: %v", err)
	}
	if response2 != nil {
		t.Errorf("expected nil response for non-existent trace, got %v", response2)
	}
}

// TestSearchTags tests SearchTags functionality
func TestSearchTags(t *testing.T) {
	tmpDir := t.TempDir()

	// Set up backend storage
	rawReader, rawWriter, _, err := local.New(&local.Config{Path: tmpDir})
	if err != nil {
		t.Fatalf("failed to create local backend: %v", err)
	}

	reader := backend.NewReader(rawReader)
	writer := backend.NewWriter(rawWriter)

	// Create test trace with span
	traceIDBytes := []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}
	trace := &tempopb.Trace{
		ResourceSpans: []*tempotrace.ResourceSpans{
			{
				ScopeSpans: []*tempotrace.ScopeSpans{
					{
						Spans: []*tempotrace.Span{
							{
								TraceId:           traceIDBytes,
								SpanId:            []byte{1, 2, 3, 4, 5, 6, 7, 8},
								Name:              "test-span",
								StartTimeUnixNano: uint64(time.Now().UnixNano()),
								EndTimeUnixNano:   uint64(time.Now().Add(time.Second).UnixNano()),
							},
						},
					},
				},
			},
		},
	}

	// Create block
	iter := &testIterator{
		traces: []struct {
			id    common.ID
			trace *tempopb.Trace
		}{
			{id: common.ID(traceIDBytes), trace: trace},
		},
	}

	meta := backend.NewBlockMeta("test-tenant", uuid.New(), VersionString)
	cfg := &common.BlockConfig{RowGroupSizeBytes: 10000}
	ctx := context.Background()

	resultMeta, err := CreateBlock(ctx, cfg, meta, iter, reader, writer)
	if err != nil {
		t.Fatalf("failed to create block: %v", err)
	}

	// Open as backend block
	backendBlock := newBackendBlock(resultMeta, reader)

	// Search for tags
	foundTags := make(map[string]bool)
	callback := func(tag string, scope traceql.AttributeScope) {
		foundTags[tag] = true
	}

	metricsCallback := func(bytesRead uint64) {}

	err = backendBlock.SearchTags(ctx, traceql.AttributeScopeNone, callback, metricsCallback, common.SearchOptions{})
	if err != nil {
		t.Fatalf("SearchTags failed: %v", err)
	}

	// We should find at least some column names
	if len(foundTags) == 0 {
		t.Error("expected to find at least one tag, got none")
	}
}

// TestSearchTagValues tests SearchTagValues functionality
func TestSearchTagValues(t *testing.T) {
	tmpDir := t.TempDir()

	// Set up backend storage
	rawReader, rawWriter, _, err := local.New(&local.Config{Path: tmpDir})
	if err != nil {
		t.Fatalf("failed to create local backend: %v", err)
	}

	reader := backend.NewReader(rawReader)
	writer := backend.NewWriter(rawWriter)

	// Create test trace with span name
	traceIDBytes := []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}
	trace := &tempopb.Trace{
		ResourceSpans: []*tempotrace.ResourceSpans{
			{
				ScopeSpans: []*tempotrace.ScopeSpans{
					{
						Spans: []*tempotrace.Span{
							{
								TraceId:           traceIDBytes,
								SpanId:            []byte{1, 2, 3, 4, 5, 6, 7, 8},
								Name:              "test-span",
								StartTimeUnixNano: uint64(time.Now().UnixNano()),
								EndTimeUnixNano:   uint64(time.Now().Add(time.Second).UnixNano()),
							},
						},
					},
				},
			},
		},
	}

	// Create block
	iter := &testIterator{
		traces: []struct {
			id    common.ID
			trace *tempopb.Trace
		}{
			{id: common.ID(traceIDBytes), trace: trace},
		},
	}

	meta := backend.NewBlockMeta("test-tenant", uuid.New(), VersionString)
	cfg := &common.BlockConfig{RowGroupSizeBytes: 10000}
	ctx := context.Background()

	resultMeta, err := CreateBlock(ctx, cfg, meta, iter, reader, writer)
	if err != nil {
		t.Fatalf("failed to create block: %v", err)
	}

	// Open as backend block
	backendBlock := newBackendBlock(resultMeta, reader)

	// Search for tag values of span name
	foundValues := make(map[string]bool)
	callback := func(value string) bool {
		foundValues[value] = true
		return false // Continue
	}

	metricsCallback := func(bytesRead uint64) {}

	err = backendBlock.SearchTagValues(ctx, "name", callback, metricsCallback, common.SearchOptions{})
	if err != nil {
		t.Fatalf("SearchTagValues failed: %v", err)
	}

	// Should find "test-span"
	if !foundValues["test-span"] {
		t.Errorf("expected to find 'test-span' in tag values, found: %v", foundValues)
	}
}

// TestEndToEndTraceFlow tests the complete flow from trace creation to backend block read
// This tests: trace creation -> iterator -> block creation -> block read
func TestEndToEndTraceFlow(t *testing.T) {
	tmpDir := t.TempDir()

	// Set up backend storage
	rawReader, rawWriter, _, err := local.New(&local.Config{Path: tmpDir})
	if err != nil {
		t.Fatalf("failed to create local backend: %v", err)
	}

	reader := backend.NewReader(rawReader)
	writer := backend.NewWriter(rawWriter)

	// Create multiple test traces
	traceData := []struct {
		id   []byte
		name string
	}{
		{[]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}, "trace-one"},
		{[]byte{2, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}, "trace-two"},
		{[]byte{3, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}, "trace-three"},
	}

	traces := make([]struct {
		id    common.ID
		trace *tempopb.Trace
	}, len(traceData))

	for i, tc := range traceData {
		trace := &tempopb.Trace{
			ResourceSpans: []*tempotrace.ResourceSpans{
				{
					ScopeSpans: []*tempotrace.ScopeSpans{
						{
							Spans: []*tempotrace.Span{
								{
									TraceId:           tc.id,
									SpanId:            []byte{byte(i + 1), 2, 3, 4, 5, 6, 7, 8},
									Name:              tc.name,
									StartTimeUnixNano: uint64(time.Now().UnixNano()),
									EndTimeUnixNano:   uint64(time.Now().Add(time.Second).UnixNano()),
								},
							},
						},
					},
				},
			},
		}
		traces[i] = struct {
			id    common.ID
			trace *tempopb.Trace
		}{common.ID(tc.id), trace}
	}

	// Step 1: Create iterator with traces
	iter := &testIterator{traces: traces}

	// Step 2: Create backend block from iterator
	blockMeta := backend.NewBlockMeta("test-tenant", uuid.New(), VersionString)
	cfg := &common.BlockConfig{RowGroupSizeBytes: 10000}
	ctx := context.Background()

	resultMeta, err := CreateBlock(ctx, cfg, blockMeta, iter, reader, writer)
	if err != nil {
		t.Fatalf("failed to create backend block: %v", err)
	}

	// Step 3: Read traces from backend block
	backendBlock := newBackendBlock(resultMeta, reader)

	for _, tc := range traceData {
		response, err := backendBlock.FindTraceByID(ctx, common.ID(tc.id), common.SearchOptions{})
		if err != nil {
			t.Fatalf("failed to find trace %s: %v", tc.name, err)
		}

		if response == nil || response.Trace == nil {
			t.Fatalf("expected to find trace %s, got nil", tc.name)
		}

		// Verify span name
		if len(response.Trace.ResourceSpans) == 0 ||
			len(response.Trace.ResourceSpans[0].ScopeSpans) == 0 ||
			len(response.Trace.ResourceSpans[0].ScopeSpans[0].Spans) == 0 {
			t.Fatalf("trace %s has invalid structure", tc.name)
		}

		foundSpan := response.Trace.ResourceSpans[0].ScopeSpans[0].Spans[0]
		if foundSpan.Name != tc.name {
			t.Errorf("trace %s: expected span name '%s', got '%s'", tc.name, tc.name, foundSpan.Name)
		}
	}

	// Verify metadata
	if resultMeta.TotalObjects != int64(len(traces)) {
		t.Errorf("expected TotalObjects=%d, got %d", len(traces), resultMeta.TotalObjects)
	}

	// Step 4: Test WAL block operations independently
	walDir := filepath.Join(tmpDir, "wal")
	walMeta := backend.NewBlockMeta("test-tenant", uuid.New(), VersionString)
	walMeta.StartTime = time.Now().Add(-time.Hour)
	walMeta.EndTime = time.Now()

	walBlock, err := createWALBlock(walMeta, walDir, time.Minute)
	if err != nil {
		t.Fatalf("failed to create WAL block: %v", err)
	}

	// Append a trace to WAL
	testTrace := traces[0].trace
	start := uint32(walMeta.StartTime.Unix())
	end := uint32(walMeta.EndTime.Unix())
	err = walBlock.AppendTrace(traces[0].id, testTrace, start, end, false)
	if err != nil {
		t.Fatalf("failed to append trace to WAL: %v", err)
	}

	// Verify WAL has data
	if walBlock.DataLength() == 0 {
		t.Error("expected WAL to have data after append")
	}

	// Flush WAL
	err = walBlock.Flush()
	if err != nil {
		t.Fatalf("failed to flush WAL: %v", err)
	}

	// Verify file was created
	dataFile := filepath.Join(walDir, DataFileName)
	if _, err := os.Stat(dataFile); os.IsNotExist(err) {
		t.Errorf("expected WAL data file to exist at %s", dataFile)
	}
}

// TestMultipleBlocksSearch tests search operations across multiple blocks
func TestMultipleBlocksSearch(t *testing.T) {
	tmpDir := t.TempDir()

	// Set up backend storage
	rawReader, rawWriter, _, err := local.New(&local.Config{Path: tmpDir})
	if err != nil {
		t.Fatalf("failed to create local backend: %v", err)
	}

	reader := backend.NewReader(rawReader)
	writer := backend.NewWriter(rawWriter)

	ctx := context.Background()
	cfg := &common.BlockConfig{RowGroupSizeBytes: 10000}

	// Create multiple blocks with different traces
	blocks := make([]*blockpackBlock, 0, 3)

	for blockIdx := 0; blockIdx < 3; blockIdx++ {
		traces := make([]struct {
			id    common.ID
			trace *tempopb.Trace
		}, 0, 2)

		for traceIdx := 0; traceIdx < 2; traceIdx++ {
			traceID := []byte{
				byte(blockIdx*10 + traceIdx),
				2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16,
			}
			trace := &tempopb.Trace{
				ResourceSpans: []*tempotrace.ResourceSpans{
					{
						ScopeSpans: []*tempotrace.ScopeSpans{
							{
								Spans: []*tempotrace.Span{
									{
										TraceId:           traceID,
										SpanId:            []byte{1, 2, 3, 4, 5, 6, 7, 8},
										Name:              "span-" + string(rune('a'+blockIdx*2+traceIdx)),
										StartTimeUnixNano: uint64(time.Now().UnixNano()),
										EndTimeUnixNano:   uint64(time.Now().Add(time.Second).UnixNano()),
									},
								},
							},
						},
					},
				},
			}
			traces = append(traces, struct {
				id    common.ID
				trace *tempopb.Trace
			}{common.ID(traceID), trace})
		}

		iter := &testIterator{traces: traces}
		meta := backend.NewBlockMeta("test-tenant", uuid.New(), VersionString)

		resultMeta, err := CreateBlock(ctx, cfg, meta, iter, reader, writer)
		if err != nil {
			t.Fatalf("failed to create block %d: %v", blockIdx, err)
		}

		blocks = append(blocks, newBackendBlock(resultMeta, reader))
	}

	// Verify each block has correct number of objects
	for i, block := range blocks {
		if block.BlockMeta().TotalObjects != 2 {
			t.Errorf("block %d: expected 2 objects, got %d", i, block.BlockMeta().TotalObjects)
		}
	}

	// Test searching for tags across all blocks
	allTags := make(map[string]int)
	for i, block := range blocks {
		callback := func(tag string, scope traceql.AttributeScope) {
			allTags[tag]++
		}
		metricsCallback := func(bytesRead uint64) {}

		err := block.SearchTags(ctx, traceql.AttributeScopeNone, callback, metricsCallback, common.SearchOptions{})
		if err != nil {
			t.Errorf("block %d: SearchTags failed: %v", i, err)
		}
	}

	// All blocks should have found similar tags
	if len(allTags) == 0 {
		t.Error("expected to find tags across blocks, got none")
	}
}

// TestLargeTraceReconstruction tests reconstruction of traces with many spans
func TestLargeTraceReconstruction(t *testing.T) {
	tmpDir := t.TempDir()

	rawReader, rawWriter, _, err := local.New(&local.Config{Path: tmpDir})
	if err != nil {
		t.Fatalf("failed to create local backend: %v", err)
	}

	reader := backend.NewReader(rawReader)
	writer := backend.NewWriter(rawWriter)

	// Create a trace with many spans
	traceID := []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}
	spanCount := 50

	spans := make([]*tempotrace.Span, spanCount)
	for i := 0; i < spanCount; i++ {
		spans[i] = &tempotrace.Span{
			TraceId:           traceID,
			SpanId:            []byte{byte(i), 2, 3, 4, 5, 6, 7, 8},
			Name:              "span-" + string(rune('a'+i%26)),
			StartTimeUnixNano: uint64(time.Now().Add(time.Duration(i) * time.Millisecond).UnixNano()),
			EndTimeUnixNano:   uint64(time.Now().Add(time.Duration(i+1) * time.Millisecond).UnixNano()),
		}
	}

	trace := &tempopb.Trace{
		ResourceSpans: []*tempotrace.ResourceSpans{
			{
				ScopeSpans: []*tempotrace.ScopeSpans{
					{
						Spans: spans,
					},
				},
			},
		},
	}

	iter := &testIterator{
		traces: []struct {
			id    common.ID
			trace *tempopb.Trace
		}{
			{common.ID(traceID), trace},
		},
	}

	meta := backend.NewBlockMeta("test-tenant", uuid.New(), VersionString)
	cfg := &common.BlockConfig{RowGroupSizeBytes: 10000}
	ctx := context.Background()

	resultMeta, err := CreateBlock(ctx, cfg, meta, iter, reader, writer)
	if err != nil {
		t.Fatalf("failed to create block: %v", err)
	}

	// Read back and verify all spans are present
	backendBlock := newBackendBlock(resultMeta, reader)
	response, err := backendBlock.FindTraceByID(ctx, common.ID(traceID), common.SearchOptions{})
	if err != nil {
		t.Fatalf("FindTraceByID failed: %v", err)
	}

	if response == nil || response.Trace == nil {
		t.Fatal("expected trace to be found, got nil")
	}

	// Count total spans
	totalSpans := 0
	for _, rs := range response.Trace.ResourceSpans {
		for _, ss := range rs.ScopeSpans {
			totalSpans += len(ss.Spans)
		}
	}

	if totalSpans != spanCount {
		t.Errorf("expected %d spans, got %d", spanCount, totalSpans)
	}
}

// TestConcurrentBlockAccess tests concurrent reads from the same block
func TestConcurrentBlockAccess(t *testing.T) {
	tmpDir := t.TempDir()

	rawReader, rawWriter, _, err := local.New(&local.Config{Path: tmpDir})
	if err != nil {
		t.Fatalf("failed to create local backend: %v", err)
	}

	reader := backend.NewReader(rawReader)
	writer := backend.NewWriter(rawWriter)

	// Create test block
	traceID := []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}
	trace := &tempopb.Trace{
		ResourceSpans: []*tempotrace.ResourceSpans{
			{
				ScopeSpans: []*tempotrace.ScopeSpans{
					{
						Spans: []*tempotrace.Span{
							{
								TraceId:           traceID,
								SpanId:            []byte{1, 2, 3, 4, 5, 6, 7, 8},
								Name:              "test-span",
								StartTimeUnixNano: uint64(time.Now().UnixNano()),
								EndTimeUnixNano:   uint64(time.Now().Add(time.Second).UnixNano()),
							},
						},
					},
				},
			},
		},
	}

	iter := &testIterator{
		traces: []struct {
			id    common.ID
			trace *tempopb.Trace
		}{
			{common.ID(traceID), trace},
		},
	}

	meta := backend.NewBlockMeta("test-tenant", uuid.New(), VersionString)
	cfg := &common.BlockConfig{RowGroupSizeBytes: 10000}
	ctx := context.Background()

	resultMeta, err := CreateBlock(ctx, cfg, meta, iter, reader, writer)
	if err != nil {
		t.Fatalf("failed to create block: %v", err)
	}

	backendBlock := newBackendBlock(resultMeta, reader)

	// Spawn multiple goroutines to read concurrently
	concurrency := 10
	done := make(chan error, concurrency)

	for i := 0; i < concurrency; i++ {
		go func() {
			response, err := backendBlock.FindTraceByID(ctx, common.ID(traceID), common.SearchOptions{})
			if err != nil {
				done <- err
				return
			}
			if response == nil || response.Trace == nil {
				done <- io.ErrUnexpectedEOF
				return
			}
			done <- nil
		}()
	}

	// Wait for all goroutines
	for i := 0; i < concurrency; i++ {
		if err := <-done; err != nil {
			t.Errorf("concurrent read %d failed: %v", i, err)
		}
	}
}

// TestBlockValidation tests the Validate method
func TestBlockValidation(t *testing.T) {
	tmpDir := t.TempDir()

	rawReader, rawWriter, _, err := local.New(&local.Config{Path: tmpDir})
	if err != nil {
		t.Fatalf("failed to create local backend: %v", err)
	}

	reader := backend.NewReader(rawReader)
	writer := backend.NewWriter(rawWriter)

	// Create valid block
	traceID := []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}
	trace := &tempopb.Trace{
		ResourceSpans: []*tempotrace.ResourceSpans{
			{
				ScopeSpans: []*tempotrace.ScopeSpans{
					{
						Spans: []*tempotrace.Span{
							{
								TraceId:           traceID,
								SpanId:            []byte{1, 2, 3, 4, 5, 6, 7, 8},
								Name:              "test-span",
								StartTimeUnixNano: uint64(time.Now().UnixNano()),
								EndTimeUnixNano:   uint64(time.Now().Add(time.Second).UnixNano()),
							},
						},
					},
				},
			},
		},
	}

	iter := &testIterator{
		traces: []struct {
			id    common.ID
			trace *tempopb.Trace
		}{
			{common.ID(traceID), trace},
		},
	}

	meta := backend.NewBlockMeta("test-tenant", uuid.New(), VersionString)
	cfg := &common.BlockConfig{RowGroupSizeBytes: 10000}
	ctx := context.Background()

	resultMeta, err := CreateBlock(ctx, cfg, meta, iter, reader, writer)
	if err != nil {
		t.Fatalf("failed to create block: %v", err)
	}

	backendBlock := newBackendBlock(resultMeta, reader)

	// Validate should succeed
	err = backendBlock.Validate(ctx)
	if err != nil {
		t.Errorf("Validate failed on valid block: %v", err)
	}
}

// TestEmptyBlock tests handling of blocks with no traces
func TestEmptyBlock(t *testing.T) {
	tmpDir := t.TempDir()

	rawReader, rawWriter, _, err := local.New(&local.Config{Path: tmpDir})
	if err != nil {
		t.Fatalf("failed to create local backend: %v", err)
	}

	reader := backend.NewReader(rawReader)
	writer := backend.NewWriter(rawWriter)

	// Create empty iterator
	iter := &testIterator{traces: nil}

	meta := backend.NewBlockMeta("test-tenant", uuid.New(), VersionString)
	cfg := &common.BlockConfig{RowGroupSizeBytes: 10000}
	ctx := context.Background()

	resultMeta, err := CreateBlock(ctx, cfg, meta, iter, reader, writer)
	if err != nil {
		t.Fatalf("failed to create empty block: %v", err)
	}

	// Verify metadata reflects empty block
	if resultMeta.TotalObjects != 0 {
		t.Errorf("expected TotalObjects=0, got %d", resultMeta.TotalObjects)
	}

	backendBlock := newBackendBlock(resultMeta, reader)

	// Try to find a non-existent trace
	traceID := []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}
	response, err := backendBlock.FindTraceByID(ctx, common.ID(traceID), common.SearchOptions{})
	if err != nil {
		t.Errorf("FindTraceByID should not error on empty block: %v", err)
	}
	if response != nil {
		t.Errorf("expected nil response for empty block, got %v", response)
	}
}
