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
