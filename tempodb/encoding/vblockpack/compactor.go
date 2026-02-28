package vblockpack

import (
	"context"
	"fmt"
	"io"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/google/uuid"

	"github.com/grafana/blockpack"
	"github.com/grafana/tempo/tempodb/backend"
	"github.com/grafana/tempo/tempodb/encoding/common"
)

// NewCompactor creates a new blockpack compactor with the given options.
func NewCompactor(opts common.CompactionOptions) *Compactor {
	return &Compactor{opts: opts}
}

// Compactor implements the common.Compactor interface for blockpack blocks.
// It delegates all span-level merging to blockpack.CompactBlocks, which operates
// directly on the columnar format without a round-trip through tempopb.Trace.
// Deletion of input blocks is handled by the caller (tempodb/compactor.go) via markCompacted.
type Compactor struct {
	opts common.CompactionOptions
}

// Compact merges multiple input blockpack blocks into one or more output blocks.
func (c *Compactor) Compact(ctx context.Context, l log.Logger, r backend.Reader, w backend.Writer, inputs []*backend.BlockMeta) ([]*backend.BlockMeta, error) {
	_, span := tracer.Start(ctx, "vblockpack.Compactor.Compact")
	defer span.End()

	if len(inputs) == 0 {
		return nil, nil
	}

	// Compute output block metadata fields from inputs.
	var (
		maxCompactionLevel uint32
		minBlockStart      time.Time
		maxBlockEnd        time.Time
		totalObjects       int64
	)
	for _, m := range inputs {
		if m.CompactionLevel > maxCompactionLevel {
			maxCompactionLevel = m.CompactionLevel
		}
		if minBlockStart.IsZero() || m.StartTime.Before(minBlockStart) {
			minBlockStart = m.StartTime
		}
		if m.EndTime.After(maxBlockEnd) {
			maxBlockEnd = m.EndTime
		}
		totalObjects += m.TotalObjects
	}

	first := inputs[0]

	// Build one ReaderProvider per input block.
	providers := make([]blockpack.ReaderProvider, len(inputs))
	for i, m := range inputs {
		providers[i] = &tempoBlockProvider{
			reader:   r,
			blockID:  uuid.UUID(m.BlockID),
			tenantID: m.TenantID,
		}
	}

	// WritableStorage receives the compacted output files.
	// Each call to Put() creates a new output block in the backend.
	out := &tempoOutputStorage{
		writer:            w,
		tenantID:          first.TenantID,
		replicationFactor: first.ReplicationFactor,
		dedicatedColumns:  first.DedicatedColumns,
		compactionLevel:   maxCompactionLevel + 1,
		startTime:         minBlockStart,
		endTime:           maxBlockEnd,
	}

	cfg := blockpack.CompactionConfig{
		MaxSpansPerBlock: maxSpansFromConfig(&c.opts.BlockConfig),
	}

	outputPaths, err := blockpack.CompactBlocks(ctx, providers, cfg, out)
	if err != nil {
		return nil, fmt.Errorf("blockpack.CompactBlocks: %w", err)
	}

	level.Info(l).Log(
		"msg", "blockpack compaction complete",
		"input_blocks", len(inputs),
		"output_blocks", len(outputPaths),
		"compaction_level", maxCompactionLevel+1,
	)

	if c.opts.ObjectsWritten != nil {
		c.opts.ObjectsWritten(int(maxCompactionLevel), int(totalObjects))
	}

	return out.metas, nil
}

// tempoBlockProvider implements blockpack.ReaderProvider for a single backend block.
// Size and ReadAt delegate to the backend reader.
// Delete is a no-op: the caller (tempodb/compactor.go) handles input block deletion
// via markCompacted after Compact returns.
type tempoBlockProvider struct {
	reader   backend.Reader
	blockID  uuid.UUID
	tenantID string
}

func (p *tempoBlockProvider) Size() (int64, error) {
	rc, size, err := p.reader.StreamReader(context.Background(), DataFileName, p.blockID, p.tenantID)
	if err != nil {
		return 0, err
	}
	_ = rc.Close()
	return size, nil
}

func (p *tempoBlockProvider) ReadAt(buf []byte, off int64, dataType blockpack.DataType) (int, error) {
	if off < 0 {
		return 0, fmt.Errorf("negative offset: %d", off)
	}
	err := p.reader.ReadRange(context.Background(), DataFileName, p.blockID, p.tenantID, uint64(off), buf, nil)
	if err != nil {
		size, sizeErr := p.Size()
		if sizeErr == nil && off >= size {
			return 0, io.EOF
		}
		return 0, err
	}
	return len(buf), nil
}

// Delete is a no-op. Input block lifecycle is managed by the outer compaction loop.
func (p *tempoBlockProvider) Delete() error {
	return nil
}

// tempoOutputStorage implements blockpack.WritableStorage.
// Each call to Put() writes one output blockpack file as a new block in the backend.
type tempoOutputStorage struct {
	writer            backend.Writer
	tenantID          string
	replicationFactor uint32
	dedicatedColumns  backend.DedicatedColumns
	compactionLevel   uint32
	startTime         time.Time
	endTime           time.Time

	metas []*backend.BlockMeta
}

func (s *tempoOutputStorage) Put(path string, data []byte) error {
	ctx := context.Background()
	newID := backend.NewUUID()

	if err := s.writer.Write(ctx, DataFileName, uuid.UUID(newID), s.tenantID, data, nil); err != nil {
		return fmt.Errorf("write blockpack data: %w", err)
	}

	// Count total spans by parsing the in-memory blockpack bytes. This avoids
	// an extra backend round-trip and sets TotalObjects so that the Tempo
	// compaction scheduler can prioritise blocks.
	var totalObjects int64
	if r, err := blockpack.NewReaderFromProvider(&bytesReaderProvider{data: data}); err == nil {
		for i := range r.BlockCount() {
			totalObjects += int64(r.BlockMeta(i).SpanCount)
		}
	}

	meta := &backend.BlockMeta{
		BlockID:           newID,
		TenantID:          s.tenantID,
		CompactionLevel:   s.compactionLevel,
		Version:           VersionString,
		ReplicationFactor: s.replicationFactor,
		DedicatedColumns:  s.dedicatedColumns,
		StartTime:         s.startTime,
		EndTime:           s.endTime,
		Size_:             uint64(len(data)),
		TotalObjects:      totalObjects,
	}

	if err := s.writer.WriteBlockMeta(ctx, meta); err != nil {
		return fmt.Errorf("write block meta: %w", err)
	}

	s.metas = append(s.metas, meta)
	return nil
}

// Size and ReadAt satisfy the blockpack.WritableStorage interface (extends Storage).
// Output storage is write-only; reads are not supported.
func (s *tempoOutputStorage) Size(path string) (int64, error) {
	return 0, fmt.Errorf("output storage does not support reads")
}

func (s *tempoOutputStorage) ReadAt(path string, p []byte, off int64, _ blockpack.DataType) (int, error) {
	return 0, fmt.Errorf("output storage does not support reads")
}

// Delete satisfies the blockpack.WritableStorage interface but is not used for output files.
func (s *tempoOutputStorage) Delete(path string) error {
	return nil
}

func maxSpansFromConfig(cfg *common.BlockConfig) int {
	if cfg.RowGroupSizeBytes <= 0 {
		return 0 // blockpack will use its default of 2000
	}
	n := cfg.RowGroupSizeBytes / 1024 // ~1KB per span estimate
	if n < 100 {
		return 100
	}
	if n > 10000 {
		return 10000
	}
	return n
}
