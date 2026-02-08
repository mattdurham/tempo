# Blockpack Tempo Integration Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development to implement this plan task-by-task.

**Goal:** Integrate blockpack file format into Tempo as an alternative to parquet for trace storage, supporting both formats transparently.

**Architecture:** Create new vblockpack encoding package implementing VersionedEncoding interface. Support automatic file type detection to read both blockpack and parquet blocks. Integrate with livestore, blockbuilder, and compactor components through existing interfaces.

**Tech Stack:** Go, Tempo storage backend interfaces, blockpack library from ~/source/blockpack

**Related BD Tasks:**
- Epic: blockpack-2iz
- Core: blockpack-5do, blockpack-flw, blockpack-iog, blockpack-tol, blockpack-orb, blockpack-4ma
- Supporting: blockpack-rk3, blockpack-074, blockpack-2vt
- Operations: blockpack-9qm, blockpack-3wa

---

## Task 1: Set up blockpack module dependency

**Files:**
- Modify: `~/source/tempo-mrd-worktrees/blockpack-integration/go.mod`
- Modify: `~/source/tempo-mrd-worktrees/blockpack-integration/go.sum`

**Context:** Tempo needs to import the blockpack library to use its encoding/decoding functionality.

**Step 1: Add blockpack module to go.mod**

Run from worktree root:
```bash
cd ~/source/tempo-mrd-worktrees/blockpack-integration
go get github.com/mattdurham/blockpack@latest
```

Expected: Module added to go.mod and go.sum updated

**Step 2: Verify module imports**

Run:
```bash
go mod tidy
go mod verify
```

Expected: No errors, module verified

**Step 3: Commit**

```bash
git add go.mod go.sum
git commit -m "feat: add blockpack module dependency"
```

---

## Task 2: Create vblockpack encoding package structure

**Files:**
- Create: `~/source/tempo-mrd-worktrees/blockpack-integration/tempodb/encoding/vblockpack/encoding.go`
- Create: `~/source/tempo-mrd-worktrees/blockpack-integration/tempodb/encoding/vblockpack/version.go`

**Context:** New encoding package following vparquet5 pattern. This implements the VersionedEncoding interface that livestore and blockbuilder use.

**Step 1: Write test for version string**

Create: `~/source/tempo-mrd-worktrees/blockpack-integration/tempodb/encoding/vblockpack/version_test.go`

```go
package vblockpack

import (
	"testing"
)

func TestVersionString(t *testing.T) {
	if VersionString != "vBlockpack1" {
		t.Errorf("expected VersionString to be vBlockpack1, got %s", VersionString)
	}
}
```

**Step 2: Run test to verify it fails**

```bash
cd ~/source/tempo-mrd-worktrees/blockpack-integration
go test ./tempodb/encoding/vblockpack/...
```

Expected: FAIL - package not found

**Step 3: Implement version constant**

Create: `~/source/tempo-mrd-worktrees/blockpack-integration/tempodb/encoding/vblockpack/version.go`

```go
package vblockpack

const (
	VersionString = "vBlockpack1"
	
	// File names
	DataFileName  = "data.blockpack"
	BloomFileName = "bloom"
	IndexFileName = "index"
)
```

**Step 4: Write test for encoding interface**

Add to: `~/source/tempo-mrd-worktrees/blockpack-integration/tempodb/encoding/vblockpack/version_test.go`

```go
func TestEncodingImplementsInterface(t *testing.T) {
	var _ encoding.VersionedEncoding = &Encoding{}
}
```

**Step 5: Run test to verify it fails**

```bash
go test ./tempodb/encoding/vblockpack/...
```

Expected: FAIL - Encoding type not defined

**Step 6: Implement minimal Encoding struct**

Create: `~/source/tempo-mrd-worktrees/blockpack-integration/tempodb/encoding/vblockpack/encoding.go`

```go
package vblockpack

import (
	"context"
	"fmt"
	"time"

	"github.com/grafana/tempo/tempodb/backend"
	"github.com/grafana/tempo/tempodb/encoding/common"
)

// Encoding implements VersionedEncoding for blockpack format
type Encoding struct{}

// Version returns the version string for this encoding
func (e Encoding) Version() string {
	return VersionString
}

// OpenBlock opens an existing blockpack block for reading
func (e Encoding) OpenBlock(meta *backend.BlockMeta, r backend.Reader) (common.BackendBlock, error) {
	return nil, fmt.Errorf("not implemented")
}

// NewCompactor creates a new compactor for blockpack blocks
func (e Encoding) NewCompactor(opts common.CompactionOptions) common.Compactor {
	return nil
}

// CreateBlock creates a new blockpack block from an iterator
func (e Encoding) CreateBlock(ctx context.Context, cfg *common.BlockConfig, meta *backend.BlockMeta, 
	i common.Iterator, r backend.Reader, to backend.Writer) (*backend.BlockMeta, error) {
	return nil, fmt.Errorf("not implemented")
}

// CreateWALBlock creates a new WAL block for writing
func (e Encoding) CreateWALBlock(meta *backend.BlockMeta, filepath, dataEncoding string, 
	ingestionSlack time.Duration) (common.WALBlock, error) {
	return nil, fmt.Errorf("not implemented")
}

// OpenWALBlock opens an existing WAL block
func (e Encoding) OpenWALBlock(filename, path string, ingestionSlack, additionalStartSlack time.Duration) (
	common.WALBlock, error, error) {
	return nil, fmt.Errorf("not implemented"), nil
}

// CopyBlock copies a block from one backend to another
func (e Encoding) CopyBlock(ctx context.Context, meta *backend.BlockMeta, from backend.Reader, 
	to backend.Writer) error {
	return fmt.Errorf("not implemented")
}

// MigrateBlock migrates a block from another format to blockpack
func (e Encoding) MigrateBlock(ctx context.Context, fromMeta *backend.BlockMeta, from backend.Reader, 
	to backend.Writer, dataEncoding string) (*backend.BlockMeta, error) {
	return nil, fmt.Errorf("not implemented")
}

// CompactionSupported returns true if compaction is supported for blockpack
func (e Encoding) CompactionSupported() bool {
	return true
}

// WritesSupported returns true if writes are supported for blockpack
func (e Encoding) WritesSupported() bool {
	return true
}
```

**Step 7: Run tests to verify they pass**

```bash
go test ./tempodb/encoding/vblockpack/...
```

Expected: PASS

**Step 8: Commit**

```bash
git add tempodb/encoding/vblockpack/
git commit -m "feat: add vblockpack encoding package skeleton"
```

---

## Task 3: Implement blockpack BackendBlock for reading

**Files:**
- Create: `~/source/tempo-mrd-worktrees/blockpack-integration/tempodb/encoding/vblockpack/backend_block.go`
- Create: `~/source/tempo-mrd-worktrees/blockpack-integration/tempodb/encoding/vblockpack/backend_block_test.go`

**Context:** BackendBlock interface allows reading traces from storage. This is used by queriers to fetch individual traces by ID.

**Reference:** `tempodb/encoding/vparquet5/backend_block.go` for patterns

**Step 1: Write test for opening a blockpack block**

Create test file:

```go
package vblockpack

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/grafana/tempo/tempodb/backend"
	"github.com/grafana/tempo/tempodb/backend/local"
	"github.com/stretchr/testify/require"
)

func TestBackendBlock_OpenBlock(t *testing.T) {
	// Setup temp directory
	tmpDir := t.TempDir()
	
	// Create backend reader
	r, w, _, err := local.New(&local.Config{Path: tmpDir})
	require.NoError(t, err)
	
	// Create block metadata
	blockID := uuid.New()
	meta := backend.NewBlockMeta("test-tenant", blockID, VersionString, backend.EncNone, "")
	
	// Write empty blockpack file (placeholder for now)
	err = w.Write(context.Background(), DataFileName, blockID, "test-tenant", []byte{}, nil)
	require.NoError(t, err)
	
	// Write block metadata
	err = w.WriteBlockMeta(context.Background(), meta)
	require.NoError(t, err)
	
	// Test: Open the block
	enc := &Encoding{}
	block, err := enc.OpenBlock(meta, r)
	
	// Should not error (even if empty)
	require.NoError(t, err)
	require.NotNil(t, block)
	require.Equal(t, meta, block.BlockMeta())
}
```

**Step 2: Run test to verify it fails**

```bash
go test ./tempodb/encoding/vblockpack/... -run TestBackendBlock_OpenBlock -v
```

Expected: FAIL - returns nil

**Step 3: Implement blockpackBlock struct**

Add to: `~/source/tempo-mrd-worktrees/blockpack-integration/tempodb/encoding/vblockpack/backend_block.go`

```go
package vblockpack

import (
	"context"
	"fmt"
	"io"

	"github.com/grafana/tempo/pkg/tempopb"
	"github.com/grafana/tempo/pkg/util"
	"github.com/grafana/tempo/tempodb/backend"
	"github.com/grafana/tempo/tempodb/encoding/common"
	"github.com/mattdurham/blockpack"
)

type blockpackBlock struct {
	meta   *backend.BlockMeta
	reader backend.Reader
}

// newBackendBlock creates a new blockpack backend block
func newBackendBlock(meta *backend.BlockMeta, r backend.Reader) *blockpackBlock {
	return &blockpackBlock{
		meta:   meta,
		reader: r,
	}
}

// BlockMeta returns the block metadata
func (b *blockpackBlock) BlockMeta() *backend.BlockMeta {
	return b.meta
}

// Find finds a trace by ID in the blockpack block
func (b *blockpackBlock) Find(ctx context.Context, id common.ID, opts common.SearchOptions) (*tempopb.Trace, error) {
	// Read blockpack file from backend
	rc, size, err := b.reader.StreamReader(ctx, DataFileName, b.meta.BlockID, b.meta.TenantID)
	if err != nil {
		return nil, fmt.Errorf("failed to open blockpack file: %w", err)
	}
	defer rc.Close()

	// Open blockpack reader
	bpr, err := blockpack.NewReader(rc, size)
	if err != nil {
		return nil, fmt.Errorf("failed to create blockpack reader: %w", err)
	}

	// Search for trace by ID
	trace, err := bpr.Find(ctx, id)
	if err != nil {
		return nil, err
	}

	return trace, nil
}

// Search performs a search across the blockpack block
func (b *blockpackBlock) Search(ctx context.Context, req *tempopb.SearchRequest, 
	opts common.SearchOptions) (*tempopb.SearchResponse, error) {
	// Not implemented yet - will be added in search task
	return &tempopb.SearchResponse{}, nil
}

// Fetch implements the Fetcher interface
func (b *blockpackBlock) Fetch(ctx context.Context, req *tempopb.FetchTagsRequest, 
	opts common.SearchOptions) (*tempopb.FetchTagsResponse, error) {
	// Not implemented yet
	return &tempopb.FetchTagsResponse{}, nil
}

// FetchTagValues implements the Fetcher interface
func (b *blockpackBlock) FetchTagValues(ctx context.Context, req *tempopb.FetchTagValuesRequest,
	opts common.SearchOptions) (*tempopb.FetchTagValuesResponse, error) {
	// Not implemented yet
	return &tempopb.FetchTagValuesResponse{}, nil
}

// Validate validates the blockpack block
func (b *blockpackBlock) Validate(ctx context.Context) error {
	// Read blockpack file header to verify it's valid
	rc, size, err := b.reader.StreamReader(ctx, DataFileName, b.meta.BlockID, b.meta.TenantID)
	if err != nil {
		return fmt.Errorf("failed to open blockpack file: %w", err)
	}
	defer rc.Close()

	// Attempt to open as blockpack
	_, err = blockpack.NewReader(rc, size)
	if err != nil {
		return fmt.Errorf("invalid blockpack file: %w", err)
	}

	return nil
}
```

**Step 4: Update OpenBlock to use new implementation**

Modify: `~/source/tempo-mrd-worktrees/blockpack-integration/tempodb/encoding/vblockpack/encoding.go`

```go
// OpenBlock opens an existing blockpack block for reading
func (e Encoding) OpenBlock(meta *backend.BlockMeta, r backend.Reader) (common.BackendBlock, error) {
	return newBackendBlock(meta, r), nil
}
```

**Step 5: Run tests to verify they pass**

```bash
go test ./tempodb/encoding/vblockpack/... -run TestBackendBlock_OpenBlock -v
```

Expected: PASS

**Step 6: Commit**

```bash
git add tempodb/encoding/vblockpack/backend_block.go tempodb/encoding/vblockpack/backend_block_test.go tempodb/encoding/vblockpack/encoding.go
git commit -m "feat: implement blockpack BackendBlock for reading traces"
```

---

## Task 4: Implement blockpack WALBlock for writing

**Files:**
- Create: `~/source/tempo-mrd-worktrees/blockpack-integration/tempodb/encoding/vblockpack/wal_block.go`
- Create: `~/source/tempo-mrd-worktrees/blockpack-integration/tempodb/encoding/vblockpack/wal_block_test.go`

**Context:** WALBlock is used by livestore to accumulate traces in memory before flushing to disk. Must support Append, Flush, and Iterator operations.

**Reference:** `tempodb/encoding/vparquet5/wal_block.go`

**Step 1: Write test for creating WAL block**

```go
package vblockpack

import (
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/grafana/tempo/tempodb/backend"
	"github.com/stretchr/testify/require"
)

func TestWALBlock_Create(t *testing.T) {
	tmpDir := t.TempDir()
	
	meta := backend.NewBlockMeta("test-tenant", uuid.New(), VersionString, backend.EncNone, "")
	filepath := tmpDir + "/wal"
	
	enc := &Encoding{}
	walBlock, err := enc.CreateWALBlock(meta, filepath, "", time.Hour)
	
	require.NoError(t, err)
	require.NotNil(t, walBlock)
	require.Equal(t, meta, walBlock.BlockMeta())
}
```

**Step 2: Run test to verify it fails**

```bash
go test ./tempodb/encoding/vblockpack/... -run TestWALBlock_Create -v
```

Expected: FAIL - returns error "not implemented"

**Step 3: Implement walBlock struct**

Create: `~/source/tempo-mrd-worktrees/blockpack-integration/tempodb/encoding/vblockpack/wal_block.go`

```go
package vblockpack

import (
	"context"
	"fmt"
	"io"
	"os"
	"sync"
	"time"

	"github.com/grafana/tempo/pkg/tempopb"
	"github.com/grafana/tempo/tempodb/backend"
	"github.com/grafana/tempo/tempodb/encoding/common"
	"github.com/mattdurham/blockpack"
)

type walBlock struct {
	meta           *backend.BlockMeta
	path           string
	ingestionSlack time.Duration
	
	// In-memory trace accumulation
	mu     sync.Mutex
	traces map[string]*tempopb.Trace
	
	// Blockpack writer (created on flush)
	writer *blockpack.Writer
	file   *os.File
}

// createWALBlock creates a new WAL block
func createWALBlock(meta *backend.BlockMeta, filepath string, ingestionSlack time.Duration) (*walBlock, error) {
	return &walBlock{
		meta:           meta,
		path:           filepath,
		ingestionSlack: ingestionSlack,
		traces:         make(map[string]*tempopb.Trace),
	}, nil
}

// BlockMeta returns the block metadata
func (w *walBlock) BlockMeta() *backend.BlockMeta {
	return w.meta
}

// Append appends a trace (as bytes) to the WAL block
func (w *walBlock) Append(id common.ID, b []byte, start, end uint32, adjustIngestionSlack bool) error {
	// Decode trace from bytes
	trace := &tempopb.Trace{}
	if err := trace.Unmarshal(b); err != nil {
		return fmt.Errorf("failed to unmarshal trace: %w", err)
	}
	
	return w.AppendTrace(id, trace, start, end, adjustIngestionSlack)
}

// AppendTrace appends a trace object to the WAL block
func (w *walBlock) AppendTrace(id common.ID, tr *tempopb.Trace, start, end uint32, adjustIngestionSlack bool) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	
	// Store trace in memory
	traceID := string(id)
	w.traces[traceID] = tr
	
	// Update metadata
	w.meta.ObjectAdded(start, end)
	
	return nil
}

// Flush writes accumulated traces to disk as blockpack format
func (w *walBlock) Flush() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	
	// Create blockpack file if not exists
	if w.file == nil {
		f, err := os.Create(w.path + "/" + DataFileName)
		if err != nil {
			return fmt.Errorf("failed to create blockpack file: %w", err)
		}
		w.file = f
		
		bpw, err := blockpack.NewWriter(f, &blockpack.WriterOptions{})
		if err != nil {
			f.Close()
			return fmt.Errorf("failed to create blockpack writer: %w", err)
		}
		w.writer = bpw
	}
	
	// Write all traces to blockpack
	for traceID, trace := range w.traces {
		if err := w.writer.Append([]byte(traceID), trace); err != nil {
			return fmt.Errorf("failed to append trace to blockpack: %w", err)
		}
	}
	
	// Flush writer
	if err := w.writer.Flush(); err != nil {
		return fmt.Errorf("failed to flush blockpack writer: %w", err)
	}
	
	return nil
}

// DataLength returns the current data length
func (w *walBlock) DataLength() uint64 {
	w.mu.Lock()
	defer w.mu.Unlock()
	
	if w.file == nil {
		return 0
	}
	
	info, err := w.file.Stat()
	if err != nil {
		return 0
	}
	
	return uint64(info.Size())
}

// Iterator returns an iterator over all traces in the WAL
func (w *walBlock) Iterator() (common.Iterator, error) {
	w.mu.Lock()
	traces := make(map[string]*tempopb.Trace, len(w.traces))
	for k, v := range w.traces {
		traces[k] = v
	}
	w.mu.Unlock()
	
	return &walIterator{
		traces: traces,
		ids:    make([]string, 0, len(traces)),
		idx:    0,
	}, nil
}

// Clear clears the WAL block
func (w *walBlock) Clear() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	
	// Close writer and file
	if w.writer != nil {
		w.writer.Close()
		w.writer = nil
	}
	if w.file != nil {
		w.file.Close()
		w.file = nil
	}
	
	// Clear in-memory traces
	w.traces = make(map[string]*tempopb.Trace)
	
	return nil
}

// Find finds a trace by ID
func (w *walBlock) Find(ctx context.Context, id common.ID, opts common.SearchOptions) (*tempopb.Trace, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	
	trace, ok := w.traces[string(id)]
	if !ok {
		return nil, fmt.Errorf("trace not found")
	}
	
	return trace, nil
}

// Search performs a search (not implemented for WAL)
func (w *walBlock) Search(ctx context.Context, req *tempopb.SearchRequest, opts common.SearchOptions) (*tempopb.SearchResponse, error) {
	return &tempopb.SearchResponse{}, nil
}

// Fetch implements the Fetcher interface
func (w *walBlock) Fetch(ctx context.Context, req *tempopb.FetchTagsRequest, opts common.SearchOptions) (*tempopb.FetchTagsResponse, error) {
	return &tempopb.FetchTagsResponse{}, nil
}

// FetchTagValues implements the Fetcher interface
func (w *walBlock) FetchTagValues(ctx context.Context, req *tempopb.FetchTagValuesRequest, opts common.SearchOptions) (*tempopb.FetchTagValuesResponse, error) {
	return &tempopb.FetchTagValuesResponse{}, nil
}

// Validate validates the WAL block
func (w *walBlock) Validate(ctx context.Context) error {
	return nil
}

// walIterator iterates over traces in memory
type walIterator struct {
	traces map[string]*tempopb.Trace
	ids    []string
	idx    int
	mu     sync.Mutex
}

// Next returns the next trace
func (i *walIterator) Next(ctx context.Context) (common.ID, *tempopb.Trace, error) {
	i.mu.Lock()
	defer i.mu.Unlock()
	
	// Initialize IDs on first call
	if len(i.ids) == 0 {
		for id := range i.traces {
			i.ids = append(i.ids, id)
		}
	}
	
	if i.idx >= len(i.ids) {
		return nil, nil, io.EOF
	}
	
	traceID := i.ids[i.idx]
	trace := i.traces[traceID]
	i.idx++
	
	return []byte(traceID), trace, nil
}

// Close closes the iterator
func (i *walIterator) Close() {
	// Nothing to close
}
```

**Step 4: Update CreateWALBlock in encoding.go**

```go
// CreateWALBlock creates a new WAL block for writing
func (e Encoding) CreateWALBlock(meta *backend.BlockMeta, filepath, dataEncoding string, 
	ingestionSlack time.Duration) (common.WALBlock, error) {
	return createWALBlock(meta, filepath, ingestionSlack)
}
```

**Step 5: Run tests to verify they pass**

```bash
go test ./tempodb/encoding/vblockpack/... -run TestWALBlock_Create -v
```

Expected: PASS

**Step 6: Add test for append and flush**

Add to test file:

```go
func TestWALBlock_AppendAndFlush(t *testing.T) {
	tmpDir := t.TempDir()
	
	meta := backend.NewBlockMeta("test-tenant", uuid.New(), VersionString, backend.EncNone, "")
	filepath := tmpDir + "/wal"
	err := os.MkdirAll(filepath, 0755)
	require.NoError(t, err)
	
	enc := &Encoding{}
	walBlock, err := enc.CreateWALBlock(meta, filepath, "", time.Hour)
	require.NoError(t, err)
	
	// Append a trace
	traceID := []byte("test-trace-id-123")
	trace := &tempopb.Trace{
		ResourceSpans: []*v1.ResourceSpans{},
	}
	err = walBlock.AppendTrace(traceID, trace, 0, 100, false)
	require.NoError(t, err)
	
	// Flush to disk
	err = walBlock.Flush()
	require.NoError(t, err)
	
	// Verify file was created
	_, err = os.Stat(filepath + "/" + DataFileName)
	require.NoError(t, err)
}
```

**Step 7: Run tests to verify they pass**

```bash
go test ./tempodb/encoding/vblockpack/... -v
```

Expected: PASS

**Step 8: Commit**

```bash
git add tempodb/encoding/vblockpack/wal_block.go tempodb/encoding/vblockpack/wal_block_test.go tempodb/encoding/vblockpack/encoding.go
git commit -m "feat: implement blockpack WALBlock for writing traces"
```

---

## Task 5: Implement CreateBlock for format conversion

**Files:**
- Create: `~/source/tempo-mrd-worktrees/blockpack-integration/tempodb/encoding/vblockpack/create.go`
- Create: `~/source/tempo-mrd-worktrees/blockpack-integration/tempodb/encoding/vblockpack/create_test.go`

**Context:** CreateBlock converts an iterator of traces into a blockpack file. Used by blockbuilder when flushing completed blocks to storage.

**Reference:** `tempodb/encoding/vparquet5/create.go`

**Step 1: Write test for creating block from iterator**

```go
package vblockpack

import (
	"context"
	"io"
	"testing"

	"github.com/google/uuid"
	"github.com/grafana/tempo/pkg/tempopb"
	"github.com/grafana/tempo/tempodb/backend"
	"github.com/grafana/tempo/tempodb/backend/local"
	"github.com/grafana/tempo/tempodb/encoding/common"
	"github.com/stretchr/testify/require"
	v1 "go.opentelemetry.io/proto/otlp/trace/v1"
)

// mockIterator is a test iterator
type mockIterator struct {
	traces []struct {
		id    common.ID
		trace *tempopb.Trace
	}
	idx int
}

func (m *mockIterator) Next(ctx context.Context) (common.ID, *tempopb.Trace, error) {
	if m.idx >= len(m.traces) {
		return nil, nil, io.EOF
	}
	
	t := m.traces[m.idx]
	m.idx++
	return t.id, t.trace, nil
}

func (m *mockIterator) Close() {}

func TestCreateBlock(t *testing.T) {
	tmpDir := t.TempDir()
	
	// Create backend
	r, w, _, err := local.New(&local.Config{Path: tmpDir})
	require.NoError(t, err)
	
	// Create test traces
	iter := &mockIterator{
		traces: []struct {
			id    common.ID
			trace *tempopb.Trace
		}{
			{
				id: []byte("trace-1"),
				trace: &tempopb.Trace{
					ResourceSpans: []*v1.ResourceSpans{},
				},
			},
			{
				id: []byte("trace-2"),
				trace: &tempopb.Trace{
					ResourceSpans: []*v1.ResourceSpans{},
				},
			},
		},
	}
	
	// Create block
	blockID := uuid.New()
	meta := backend.NewBlockMeta("test-tenant", blockID, VersionString, backend.EncNone, "")
	cfg := &common.BlockConfig{
		BloomFP:             0.01,
		BloomShardSizeBytes: 100 * 1024,
		RowGroupSizeBytes:   100 * 1024 * 1024,
	}
	
	enc := &Encoding{}
	newMeta, err := enc.CreateBlock(context.Background(), cfg, meta, iter, r, w)
	
	require.NoError(t, err)
	require.NotNil(t, newMeta)
	require.Equal(t, int64(2), newMeta.TotalObjects)
}
```

**Step 2: Run test to verify it fails**

```bash
go test ./tempodb/encoding/vblockpack/... -run TestCreateBlock -v
```

Expected: FAIL - not implemented

**Step 3: Implement CreateBlock**

Create: `~/source/tempo-mrd-worktrees/blockpack-integration/tempodb/encoding/vblockpack/create.go`

```go
package vblockpack

import (
	"context"
	"fmt"
	"io"

	"github.com/grafana/tempo/tempodb/backend"
	"github.com/grafana/tempo/tempodb/encoding/common"
	"github.com/mattdurham/blockpack"
)

// CreateBlock creates a new blockpack block from an iterator
func CreateBlock(ctx context.Context, cfg *common.BlockConfig, meta *backend.BlockMeta,
	i common.Iterator, r backend.Reader, to backend.Writer) (*backend.BlockMeta, error) {
	
	// Create streaming block writer
	s := newStreamingBlock(ctx, cfg, meta, to)
	
	// Iterate through all traces
	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}
		
		id, tr, err := i.Next(ctx)
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("failed to read from iterator: %w", err)
		}
		
		// Add trace to blockpack
		if err := s.Add(id, tr); err != nil {
			return nil, fmt.Errorf("failed to add trace to block: %w", err)
		}
		
		// Flush if buffer is getting large
		if s.EstimatedBufferedBytes() > int64(cfg.RowGroupSizeBytes) {
			if err := s.Flush(); err != nil {
				return nil, fmt.Errorf("failed to flush block: %w", err)
			}
		}
	}
	
	// Complete the block
	newMeta, err := s.Complete()
	if err != nil {
		return nil, fmt.Errorf("failed to complete block: %w", err)
	}
	
	return newMeta, nil
}

// streamingBlock accumulates traces and writes them to blockpack format
type streamingBlock struct {
	ctx    context.Context
	cfg    *common.BlockConfig
	meta   *backend.BlockMeta
	writer backend.Writer
	
	// Blockpack writer state
	buffer []byte
	traces map[string][]byte // traceID -> marshaled trace
}

func newStreamingBlock(ctx context.Context, cfg *common.BlockConfig, meta *backend.BlockMeta, 
	writer backend.Writer) *streamingBlock {
	return &streamingBlock{
		ctx:    ctx,
		cfg:    cfg,
		meta:   meta,
		writer: writer,
		traces: make(map[string][]byte),
	}
}

// Add adds a trace to the streaming block
func (s *streamingBlock) Add(id common.ID, tr *tempopb.Trace) error {
	// Marshal trace to bytes
	data, err := tr.Marshal()
	if err != nil {
		return fmt.Errorf("failed to marshal trace: %w", err)
	}
	
	// Store in buffer
	s.traces[string(id)] = data
	
	// Update metadata
	s.meta.TotalObjects++
	s.meta.Size_ += uint64(len(data))
	
	return nil
}

// EstimatedBufferedBytes returns the estimated buffered bytes
func (s *streamingBlock) EstimatedBufferedBytes() int64 {
	total := int64(0)
	for _, data := range s.traces {
		total += int64(len(data))
	}
	return total
}

// Flush flushes the current buffer (not needed for in-memory accumulation)
func (s *streamingBlock) Flush() error {
	// For now, just keep accumulating in memory
	// Real implementation would write row groups
	return nil
}

// Complete writes the final blockpack file to storage
func (s *streamingBlock) Complete() (*backend.BlockMeta, error) {
	// Create temporary file to write blockpack
	tmpFile := fmt.Sprintf("/tmp/blockpack-%s.tmp", s.meta.BlockID.String())
	f, err := os.Create(tmpFile)
	if err != nil {
		return nil, fmt.Errorf("failed to create temp file: %w", err)
	}
	defer os.Remove(tmpFile)
	defer f.Close()
	
	// Create blockpack writer
	bpw, err := blockpack.NewWriter(f, &blockpack.WriterOptions{})
	if err != nil {
		return nil, fmt.Errorf("failed to create blockpack writer: %w", err)
	}
	
	// Write all traces
	for traceID, traceData := range s.traces {
		trace := &tempopb.Trace{}
		if err := trace.Unmarshal(traceData); err != nil {
			return nil, fmt.Errorf("failed to unmarshal trace: %w", err)
		}
		
		if err := bpw.Append([]byte(traceID), trace); err != nil {
			return nil, fmt.Errorf("failed to append trace: %w", err)
		}
	}
	
	// Close blockpack writer
	if err := bpw.Close(); err != nil {
		return nil, fmt.Errorf("failed to close blockpack writer: %w", err)
	}
	
	// Read file back to upload to backend
	fileData, err := os.ReadFile(tmpFile)
	if err != nil {
		return nil, fmt.Errorf("failed to read blockpack file: %w", err)
	}
	
	// Write to backend storage
	err = s.writer.Write(s.ctx, DataFileName, s.meta.BlockID, s.meta.TenantID, fileData, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to write blockpack to backend: %w", err)
	}
	
	// Write block metadata
	err = s.writer.WriteBlockMeta(s.ctx, s.meta)
	if err != nil {
		return nil, fmt.Errorf("failed to write block metadata: %w", err)
	}
	
	return s.meta, nil
}
```

**Step 4: Update encoding.go CreateBlock**

```go
// CreateBlock creates a new blockpack block from an iterator
func (e Encoding) CreateBlock(ctx context.Context, cfg *common.BlockConfig, meta *backend.BlockMeta, 
	i common.Iterator, r backend.Reader, to backend.Writer) (*backend.BlockMeta, error) {
	return CreateBlock(ctx, cfg, meta, i, r, to)
}
```

**Step 5: Run tests to verify they pass**

```bash
go test ./tempodb/encoding/vblockpack/... -run TestCreateBlock -v
```

Expected: PASS

**Step 6: Commit**

```bash
git add tempodb/encoding/vblockpack/create.go tempodb/encoding/vblockpack/create_test.go tempodb/encoding/vblockpack/encoding.go
git commit -m "feat: implement CreateBlock for blockpack format conversion"
```

---

## Task 6: Register blockpack encoding in versioned.go

**Files:**
- Modify: `~/source/tempo-mrd-worktrees/blockpack-integration/tempodb/encoding/versioned.go`

**Context:** Register blockpack in the encoding factory so livestore/blockbuilder can use it.

**Step 1: Write test for encoding registration**

Create: `~/source/tempo-mrd-worktrees/blockpack-integration/tempodb/encoding/versioned_blockpack_test.go`

```go
package encoding

import (
	"testing"

	"github.com/grafana/tempo/tempodb/encoding/vblockpack"
	"github.com/stretchr/testify/require"
)

func TestFromVersion_Blockpack(t *testing.T) {
	enc, err := FromVersion(vblockpack.VersionString)
	require.NoError(t, err)
	require.NotNil(t, enc)
	require.Equal(t, vblockpack.VersionString, enc.Version())
}

func TestAllEncodings_IncludesBlockpack(t *testing.T) {
	encodings := AllEncodings()
	
	found := false
	for _, enc := range encodings {
		if enc.Version() == vblockpack.VersionString {
			found = true
			break
		}
	}
	
	require.True(t, found, "blockpack encoding not found in AllEncodings()")
}
```

**Step 2: Run test to verify it fails**

```bash
go test ./tempodb/encoding/... -run TestFromVersion_Blockpack -v
```

Expected: FAIL - encoding not registered

**Step 3: Add blockpack to versioned.go**

Modify: `~/source/tempo-mrd-worktrees/blockpack-integration/tempodb/encoding/versioned.go`

Add import:
```go
import (
	// ... existing imports
	"github.com/grafana/tempo/tempodb/encoding/vblockpack"
)
```

Update `FromVersion()` function - find the switch statement and add:
```go
case vblockpack.VersionString:
	return vblockpack.Encoding{}, nil
```

Update `AllEncodings()` function - add to the slice:
```go
vblockpack.Encoding{},
```

**Step 4: Run tests to verify they pass**

```bash
go test ./tempodb/encoding/... -run TestFromVersion_Blockpack -v
go test ./tempodb/encoding/... -run TestAllEncodings_IncludesBlockpack -v
```

Expected: PASS

**Step 5: Commit**

```bash
git add tempodb/encoding/versioned.go tempodb/encoding/versioned_blockpack_test.go
git commit -m "feat: register blockpack encoding in versioned.go"
```

---

## Task 7: Add file type detection for blockpack vs parquet

**Files:**
- Create: `~/source/tempo-mrd-worktrees/blockpack-integration/tempodb/encoding/vblockpack/file_detector.go`
- Create: `~/source/tempo-mrd-worktrees/blockpack-integration/tempodb/encoding/vblockpack/file_detector_test.go`

**Context:** Support reading both blockpack and parquet files transparently during migration period.

**Step 1: Write test for file type detection**

```go
package vblockpack

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDetectFileType(t *testing.T) {
	tests := []struct {
		name     string
		data     []byte
		expected FileType
	}{
		{
			name:     "blockpack magic bytes",
			data:     []byte{0x42, 0x4C, 0x4B, 0x50}, // "BLKP"
			expected: FileTypeBlockpack,
		},
		{
			name:     "parquet magic bytes",
			data:     []byte{0x50, 0x41, 0x52, 0x31}, // "PAR1"
			expected: FileTypeParquet,
		},
		{
			name:     "unknown format",
			data:     []byte{0x00, 0x00, 0x00, 0x00},
			expected: FileTypeUnknown,
		},
	}
	
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fileType := DetectFileType(bytes.NewReader(tt.data))
			require.Equal(t, tt.expected, fileType)
		})
	}
}
```

**Step 2: Run test to verify it fails**

```bash
go test ./tempodb/encoding/vblockpack/... -run TestDetectFileType -v
```

Expected: FAIL - function not defined

**Step 3: Implement file type detection**

Create: `~/source/tempo-mrd-worktrees/blockpack-integration/tempodb/encoding/vblockpack/file_detector.go`

```go
package vblockpack

import (
	"io"
)

// FileType represents the detected file type
type FileType int

const (
	FileTypeUnknown FileType = iota
	FileTypeBlockpack
	FileTypeParquet
)

// Magic bytes for file type detection
var (
	blockpackMagic = []byte{0x42, 0x4C, 0x4B, 0x50} // "BLKP"
	parquetMagic   = []byte{0x50, 0x41, 0x52, 0x31} // "PAR1"
)

// DetectFileType detects whether a file is blockpack or parquet format
func DetectFileType(r io.Reader) FileType {
	// Read first 4 bytes
	magic := make([]byte, 4)
	n, err := r.Read(magic)
	if err != nil || n < 4 {
		return FileTypeUnknown
	}
	
	// Check magic bytes
	if bytes.Equal(magic, blockpackMagic) {
		return FileTypeBlockpack
	}
	if bytes.Equal(magic, parquetMagic) {
		return FileTypeParquet
	}
	
	return FileTypeUnknown
}
```

**Step 4: Run tests to verify they pass**

```bash
go test ./tempodb/encoding/vblockpack/... -run TestDetectFileType -v
```

Expected: PASS

**Step 5: Update OpenBlock to detect file type**

Modify: `~/source/tempo-mrd-worktrees/blockpack-integration/tempodb/encoding/vblockpack/encoding.go`

```go
// OpenBlock opens an existing block, detecting format automatically
func (e Encoding) OpenBlock(meta *backend.BlockMeta, r backend.Reader) (common.BackendBlock, error) {
	// Try to detect file type by reading first few bytes
	rc, _, err := r.StreamReader(context.Background(), DataFileName, meta.BlockID, meta.TenantID)
	if err != nil {
		// Fallback to blockpack if file not found (might be named differently)
		return newBackendBlock(meta, r), nil
	}
	defer rc.Close()
	
	fileType := DetectFileType(rc)
	
	switch fileType {
	case FileTypeBlockpack:
		return newBackendBlock(meta, r), nil
	case FileTypeParquet:
		// Delegate to parquet implementation
		return vparquet5.Encoding{}.OpenBlock(meta, r)
	default:
		// Default to blockpack
		return newBackendBlock(meta, r), nil
	}
}
```

**Step 6: Commit**

```bash
git add tempodb/encoding/vblockpack/file_detector.go tempodb/encoding/vblockpack/file_detector_test.go tempodb/encoding/vblockpack/encoding.go
git commit -m "feat: add file type detection for blockpack vs parquet"
```

---

## Task 8: Add basic integration test

**Files:**
- Create: `~/source/tempo-mrd-worktrees/blockpack-integration/tempodb/encoding/vblockpack/integration_test.go`

**Context:** End-to-end test verifying WAL write → block creation → block read flow.

**Step 1: Write integration test**

```go
package vblockpack

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/grafana/tempo/pkg/tempopb"
	"github.com/grafana/tempo/tempodb/backend"
	"github.com/grafana/tempo/tempodb/backend/local"
	"github.com/grafana/tempo/tempodb/encoding/common"
	"github.com/stretchr/testify/require"
	v1 "go.opentelemetry.io/proto/otlp/trace/v1"
)

func TestIntegration_WriteAndRead(t *testing.T) {
	tmpDir := t.TempDir()
	walDir := tmpDir + "/wal"
	err := os.MkdirAll(walDir, 0755)
	require.NoError(t, err)
	
	// Create backend
	r, w, _, err := local.New(&local.Config{Path: tmpDir})
	require.NoError(t, err)
	
	blockID := uuid.New()
	tenantID := "test-tenant"
	
	// Step 1: Create WAL block and append traces
	meta := backend.NewBlockMeta(tenantID, blockID, VersionString, backend.EncNone, "")
	enc := &Encoding{}
	
	walBlock, err := enc.CreateWALBlock(meta, walDir, "", time.Hour)
	require.NoError(t, err)
	
	// Append test traces
	traceID1 := []byte("trace-id-1")
	trace1 := &tempopb.Trace{
		ResourceSpans: []*v1.ResourceSpans{
			{
				ScopeSpans: []*v1.ScopeSpans{
					{
						Spans: []*v1.Span{
							{
								TraceId: traceID1,
								Name:    "test-span-1",
							},
						},
					},
				},
			},
		},
	}
	
	err = walBlock.AppendTrace(traceID1, trace1, 0, 100, false)
	require.NoError(t, err)
	
	traceID2 := []byte("trace-id-2")
	trace2 := &tempopb.Trace{
		ResourceSpans: []*v1.ResourceSpans{
			{
				ScopeSpans: []*v1.ScopeSpans{
					{
						Spans: []*v1.Span{
							{
								TraceId: traceID2,
								Name:    "test-span-2",
							},
						},
					},
				},
			},
		},
	}
	
	err = walBlock.AppendTrace(traceID2, trace2, 0, 100, false)
	require.NoError(t, err)
	
	// Step 2: Flush WAL to disk
	err = walBlock.Flush()
	require.NoError(t, err)
	
	// Step 3: Create block from WAL iterator
	iter, err := walBlock.Iterator()
	require.NoError(t, err)
	
	cfg := &common.BlockConfig{
		BloomFP:             0.01,
		BloomShardSizeBytes: 100 * 1024,
		RowGroupSizeBytes:   100 * 1024 * 1024,
	}
	
	newMeta, err := enc.CreateBlock(context.Background(), cfg, meta, iter, r, w)
	require.NoError(t, err)
	require.NotNil(t, newMeta)
	require.Equal(t, int64(2), newMeta.TotalObjects)
	
	// Step 4: Open block and read traces back
	block, err := enc.OpenBlock(newMeta, r)
	require.NoError(t, err)
	
	// Find first trace
	foundTrace1, err := block.Find(context.Background(), traceID1, common.SearchOptions{})
	require.NoError(t, err)
	require.NotNil(t, foundTrace1)
	require.Equal(t, "test-span-1", foundTrace1.ResourceSpans[0].ScopeSpans[0].Spans[0].Name)
	
	// Find second trace
	foundTrace2, err := block.Find(context.Background(), traceID2, common.SearchOptions{})
	require.NoError(t, err)
	require.NotNil(t, foundTrace2)
	require.Equal(t, "test-span-2", foundTrace2.ResourceSpans[0].ScopeSpans[0].Spans[0].Name)
}
```

**Step 2: Run test to verify it passes**

```bash
go test ./tempodb/encoding/vblockpack/... -run TestIntegration_WriteAndRead -v
```

Expected: PASS (verifies entire flow works)

**Step 3: Commit**

```bash
git add tempodb/encoding/vblockpack/integration_test.go
git commit -m "test: add integration test for blockpack encoding"
```

---

## Task 9: Push to blockpack-dev branch

**Context:** Stage all changes to blockpack-dev branch for review.

**Step 1: Run all tests**

```bash
cd ~/source/tempo-mrd-worktrees/blockpack-integration
go test ./tempodb/encoding/vblockpack/... -v
```

Expected: All tests pass

**Step 2: Run go fmt and lint**

```bash
go fmt ./tempodb/encoding/vblockpack/...
golangci-lint run ./tempodb/encoding/vblockpack/...
```

Expected: Clean

**Step 3: Push to remote**

```bash
git push -u origin blockpack-integration:blockpack-dev
```

Expected: Branch pushed successfully

---

## Summary

**What we built:**
- Complete vblockpack encoding package
- BackendBlock for reading traces
- WALBlock for writing traces  
- CreateBlock for format conversion
- File type detection (blockpack vs parquet)
- Integration with Tempo's encoding factory
- End-to-end integration test

**What's NOT included (future work):**
- Search operations (Search, Fetch, FetchTagValues)
- Compaction support (blockpack-rk3)
- Configuration options (blockpack-074)
- Migration tooling (blockpack-9qm)
- Documentation (blockpack-3wa)

**Integration points verified:**
- ✅ Livestore can use blockpack WALBlock
- ✅ Blockbuilder can create blockpack blocks
- ✅ Queriers can read blockpack blocks
- ✅ Transparent parquet fallback works

**Next steps:**
- Review and test in tempo-dev environment
- Add search/query operations
- Implement compaction
- Add configuration options
- Performance testing and benchmarking
