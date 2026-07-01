package vblockpack

import (
	"context"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/google/uuid"

	"github.com/grafana/blockpack"
	util_log "github.com/grafana/tempo/pkg/util/log"
	"github.com/grafana/tempo/tempodb/backend"
	"github.com/grafana/tempo/tempodb/encoding/common"
	"go.opentelemetry.io/otel/attribute"
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

	// Stamp input-side attributes now; output_blocks / compaction_level are added
	// after CompactBlocksStreaming returns (issue #465).
	span.SetAttributes(
		attribute.Int("input_blocks", len(inputs)),
		attribute.String("tenantID", inputs[0].TenantID),
	)

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

	// NOTE-463 (blockpack issue #382): Reorder inputs so the most mutually-similar blocks
	// are compacted in a contiguous, similarity-descending chain — the most-similar block is
	// fed FIRST so the writer's dictionary and sort baseline (sort key is
	// (resource.service.name, span:name, minHash)) are established from the dominant data
	// pattern before less-similar spans merge in. Denser identical-string runs ⇒ better dict
	// encoding + snappy ratio and fewer distinct dict values per output block. Best-effort:
	// reads only each block's intrinsic ToC (footer + section directory + two column blobs,
	// cheap ranged GETs — no full block download); on any read failure it leaves the original
	// time-window order untouched, so behaviour falls back to pre-NOTE-463.
	inputs = orderInputsBySimilarity(ctx, l, r, inputs)

	first := inputs[0]

	// NOTE-459: Stream one input block at a time, downloading each to a temp file
	// rather than holding it in memory. The factory downloads the block, writes it
	// to a local temp file, and immediately frees the in-memory download buffer.
	// CompactBlocksStreaming calls each factory just-in-time (not all upfront), so
	// at most one block's temp file exists on disk at once; the provider deletes it
	// when the reader is done. Peak memory = writer pending buffer (~1MB), not the
	// full block size. This allows max_input_blocks=8+ without OOMKills.
	providers := make([]blockpack.CompactionProviderFunc, len(inputs))
	for i, m := range inputs {
		i, m := i, m
		providers[i] = func() (blockpack.ReaderProvider, error) {
			data, err := r.Read(ctx, DataFileName, uuid.UUID(m.BlockID), m.TenantID, nil)
			if err != nil {
				return nil, fmt.Errorf("read block %s for compaction: %w", m.BlockID, err)
			}

			// Write to a temp file, then immediately release the in-memory buffer.
			// The fileBlockProvider reads from the temp file and deletes it on close.
			tmpFile, tmpErr := os.CreateTemp("", "blockpack-compact-*.blockpack")
			if tmpErr != nil {
				return nil, fmt.Errorf("create temp file for block %s: %w", m.BlockID, tmpErr)
			}
			if _, writeErr := tmpFile.Write(data); writeErr != nil {
				_ = tmpFile.Close()
				_ = os.Remove(tmpFile.Name())
				return nil, fmt.Errorf("write temp file for block %s: %w", m.BlockID, writeErr)
			}
			data = nil // release in-memory buffer immediately; GC can collect it
			return &fileBlockProvider{file: tmpFile}, nil
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

	// Initialize caches on first compaction (no-op if already initialized).
	ConfigureCache(
		c.opts.BlockConfig.Blockpack.FileCachePath,
		c.opts.BlockConfig.Blockpack.FileCacheMaxBytes,
		c.opts.BlockConfig.Blockpack.MemCacheServers,
		c.opts.BlockConfig.Blockpack.MemoryCacheBytes,
	)
	ConfigureLRU(c.opts.BlockConfig.Blockpack.LRUCacheBytes)

	// Always use the current config's dedicated columns (set by the caller from
	// per-tenant overrides via DedicatedColumnsForTenant). This ensures compaction
	// re-indexes output blocks according to the live config rather than blindly
	// copying from input block metas, which may reflect stale or different configs.
	compactDedicatedCols := c.opts.BlockConfig.DedicatedColumns
	if len(compactDedicatedCols) == 0 {
		compactDedicatedCols = first.DedicatedColumns
	}
	cfg := blockpack.CompactionConfig{
		MaxSpansPerBlock: maxSpansFromConfig(&c.opts.BlockConfig),
		DedicatedColumns: dedicatedColumnsToBlockpack(compactDedicatedCols),
		// blockpack NOTE-476 (issue #394): drop identity columns from compacted output's
		// IntrinsicTOC (~26% of L1 file size); identity is served from the SpanTree. Source
		// blocks written with this flag have their identity sourced from their own SpanTree
		// during recompaction. Matches the ingest writer (create.go).
		OmitIntrinsicIdentityColumns: true,
	}

	outputPaths, err := blockpack.CompactBlocksStreaming(ctx, providers, cfg, out)
	if err != nil {
		return nil, fmt.Errorf("blockpack.CompactBlocks: %w", err)
	}

	span.SetAttributes(
		attribute.Int("output_blocks", len(outputPaths)),
		attribute.Int("compaction_level", int(maxCompactionLevel)+1),
	)

	level.Info(l).Log(
		"msg", "blockpack compaction complete",
		"input_blocks", len(inputs),
		"output_blocks", len(outputPaths),
		"compaction_level", maxCompactionLevel+1,
	)

	if c.opts.ObjectsWritten != nil {
		c.opts.ObjectsWritten(int(maxCompactionLevel), int(totalObjects))
	}

	// Value-index L0 files are written synchronously inside tempoOutputStorage.Put
	// per output block (blockpack NOTE-VI-042, issue #464) — no async event publish.

	return out.metas, nil
}

// memoryBlockProvider implements blockpack.ReaderProvider using pre-downloaded block bytes.
// ReadAt is served entirely from memory — no per-column S3 requests during compaction.
type memoryBlockProvider struct {
	data []byte
}

func (p *memoryBlockProvider) Size() (int64, error) {
	return int64(len(p.data)), nil
}

func (p *memoryBlockProvider) ReadAt(buf []byte, off int64, _ blockpack.DataType) (int, error) {
	if off < 0 {
		return 0, fmt.Errorf("negative offset: %d", off)
	}
	if off >= int64(len(p.data)) {
		return 0, io.EOF
	}
	n := copy(buf, p.data[off:])
	if n < len(buf) {
		return n, io.EOF
	}
	return n, nil
}

func (p *memoryBlockProvider) Delete() error { return nil }

// orderInputsBySimilarity returns inputs reordered so contiguous blocks are as content-similar
// as possible, most-similar-pair first (NOTE-463). It reads each block's intrinsic ToC value
// sets (resource.service.name, span:name) via cheap ranged GETs and builds a greedy
// nearest-neighbour chain: start from the most-similar pair, then repeatedly append the
// unselected block most similar to the chain head.
//
// Best-effort and side-effect-free on failure: if fewer than three blocks, or any block's
// value sets cannot be read, the original (time-window) order is returned unchanged so
// compaction behaves exactly as it did before NOTE-463.
func orderInputsBySimilarity(ctx context.Context, l log.Logger, r backend.Reader, inputs []*backend.BlockMeta) []*backend.BlockMeta {
	// With 0–2 blocks every order produces the same single contiguous merge, so there is no
	// ordering decision to make — skip the ToC reads entirely.
	if len(inputs) < 3 {
		return inputs
	}

	sets := make([]blockpack.BlockValueSets, len(inputs))
	for i, m := range inputs {
		vs, err := readBlockValueSetsFromBackend(ctx, r, m)
		if err != nil {
			// Any failure means we cannot reliably score similarity for the whole set;
			// fall back to the original order rather than ordering on partial data.
			level.Debug(l).Log(
				"msg", "blockpack compaction: similarity ordering skipped, falling back to input order",
				"block", m.BlockID, "err", err,
			)
			return inputs
		}
		sets[i] = vs
	}

	order := greedySimilarityChain(sets)
	ordered := make([]*backend.BlockMeta, len(inputs))
	for newIdx, oldIdx := range order {
		ordered[newIdx] = inputs[oldIdx]
	}
	return ordered
}

// greedySimilarityChain returns a permutation of indices [0,len(sets)) ordered as a greedy
// nearest-neighbour chain: the most-similar pair seeds the chain, then each step appends the
// not-yet-placed block most similar to the current chain head. This keeps the most mutually
// similar blocks adjacent and front-loads the densest data pattern.
func greedySimilarityChain(sets []blockpack.BlockValueSets) []int {
	n := len(sets)
	// Seed with the most-similar pair.
	bestI, bestJ := 0, 1
	best := -1.0
	for i := 0; i < n; i++ {
		for j := i + 1; j < n; j++ {
			s := blockpack.BlockSimilarity(sets[i], sets[j])
			if s > best {
				best, bestI, bestJ = s, i, j
			}
		}
	}

	placed := make([]bool, n)
	order := make([]int, 0, n)
	order = append(order, bestI, bestJ)
	placed[bestI], placed[bestJ] = true, true

	for len(order) < n {
		head := order[len(order)-1]
		next := -1
		nextScore := -1.0
		for cand := 0; cand < n; cand++ {
			if placed[cand] {
				continue
			}
			s := blockpack.BlockSimilarity(sets[head], sets[cand])
			if s > nextScore {
				nextScore, next = s, cand
			}
		}
		order = append(order, next)
		placed[next] = true
	}
	return order
}

// readBlockValueSetsFromBackend reads a block's resource.service.name and span:name distinct
// value sets through a lean blockpack reader backed by ranged backend GETs — no full block
// download. The lean reader reads only the footer + section directory on open; the two
// intrinsic column blobs are fetched lazily by ReadBlockValueSets.
func readBlockValueSetsFromBackend(ctx context.Context, r backend.Reader, m *backend.BlockMeta) (blockpack.BlockValueSets, error) {
	prov, err := newRangeBlockProvider(ctx, r, m)
	if err != nil {
		return blockpack.BlockValueSets{}, err
	}
	reader, err := blockpack.NewLeanReaderFromProvider(prov)
	if err != nil {
		return blockpack.BlockValueSets{}, fmt.Errorf("open lean reader for %s: %w", m.BlockID, err)
	}
	return blockpack.ReadBlockValueSets(reader)
}

// rangeBlockProvider implements blockpack.ReaderProvider by issuing ranged GETs against the
// tempo backend, so a lean reader can read a block's ToC without downloading the whole block.
type rangeBlockProvider struct {
	ctx      context.Context
	r        backend.Reader
	blockID  uuid.UUID
	tenantID string
	size     int64
}

// newRangeBlockProvider resolves the block's object size (needed for footer reads from the
// end of the file) and returns a ranged-GET provider.
//
// NOTE-477 (blockpack issue #395): the size comes from the already-loaded block meta
// (BlockMeta.Size_, set by the compactor when writing the block). StreamReader is a FULL
// object download on the S3 backend (s3.Read -> readAll), and the previous code opened it
// purely to read the returned size, then discarded the body — downloading the entire (often
// multi-GB) block just to learn its length, once per input block, before compaction even
// started. We only fall back to StreamReader for legacy blocks written before size tracking
// (Size_ == 0).
func newRangeBlockProvider(ctx context.Context, r backend.Reader, m *backend.BlockMeta) (*rangeBlockProvider, error) {
	size := int64(m.Size_)
	if size == 0 {
		rc, s, err := r.StreamReader(ctx, DataFileName, uuid.UUID(m.BlockID), m.TenantID)
		if err != nil {
			return nil, fmt.Errorf("stat block %s: %w", m.BlockID, err)
		}
		_ = rc.Close()
		size = s
	}
	return &rangeBlockProvider{
		ctx:      ctx,
		r:        r,
		blockID:  uuid.UUID(m.BlockID),
		tenantID: m.TenantID,
		size:     size,
	}, nil
}

func (p *rangeBlockProvider) Size() (int64, error) { return p.size, nil }

func (p *rangeBlockProvider) ReadAt(buf []byte, off int64, _ blockpack.DataType) (int, error) {
	if off < 0 {
		return 0, fmt.Errorf("negative offset: %d", off)
	}
	if off >= p.size {
		return 0, io.EOF
	}
	n := len(buf)
	if int64(n) > p.size-off {
		n = int(p.size - off)
	}
	if err := p.r.ReadRange(p.ctx, DataFileName, p.blockID, p.tenantID, uint64(off), buf[:n], nil); err != nil {
		return 0, fmt.Errorf("read range [%d,%d) of block %s: %w", off, off+int64(n), p.blockID, err)
	}
	if n < len(buf) {
		return n, io.EOF
	}
	return n, nil
}

// fileBlockProvider implements blockpack.ReaderProvider using a local temp file.
// The download buffer is released immediately after writing to disk; ReadAt
// serves requests via file I/O. Delete removes the temp file when compaction
// is done with this block.
type fileBlockProvider struct {
	file *os.File
}

func (p *fileBlockProvider) Size() (int64, error) {
	info, err := p.file.Stat()
	if err != nil {
		return 0, err
	}
	return info.Size(), nil
}

func (p *fileBlockProvider) ReadAt(buf []byte, off int64, _ blockpack.DataType) (int, error) {
	return p.file.ReadAt(buf, off)
}

func (p *fileBlockProvider) Delete() error {
	name := p.file.Name()
	_ = p.file.Close()
	return os.Remove(name)
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

func (s *tempoOutputStorage) Put(_ string, data []byte) error {
	ctx := context.Background()
	newID := backend.NewUUID()

	if err := s.writer.Write(ctx, DataFileName, uuid.UUID(newID), s.tenantID, data, nil); err != nil {
		return fmt.Errorf("write blockpack data: %w", err)
	}

	// Parse the in-memory blockpack bytes to extract trace count and actual span
	// time range. TotalObjects must be trace count (not span count) to be consistent
	// with create.go and Tempo's compaction priority logic.
	// StartTime/EndTime are set from actual span timestamps so Tempo's block selector
	// can skip this block for queries outside its time range.
	var totalObjects int64
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
		// TotalRecords is set by setBlockTimeRange to the internal block count,
		// allowing the frontend sharder to split this file into sub-file jobs.
		// Must be >= 1 or the sharder skips the block.
		TotalRecords: 1, // default; overwritten by setBlockTimeRange below
	}
	if r, err := blockpack.NewReaderFromProvider(&bytesReaderProvider{data: data}); err == nil {
		totalObjects = int64(r.TraceCount())
		meta.TotalObjects = totalObjects
		setBlockTimeRange(meta, data)
	}

	if err := s.writer.WriteBlockMeta(ctx, meta); err != nil {
		return fmt.Errorf("write block meta: %w", err)
	}

	s.metas = append(s.metas, meta)

	// blockpack NOTE-VI-042 (issue #464): synchronously write per-column L0
	// value-index files for this compaction output block, with no Redis broker.
	// A fresh reader is opened from the in-memory output bytes (the same bytes
	// setBlockTimeRange already parsed). Best-effort — a failure is logged but
	// never fails compaction, since the index can be rebuilt from the source
	// block. Skipped entirely when value_index_enabled is false.
	if store, prefix := getValueIndexSink(); store != nil {
		sourceRef := blockObjectKey(s.tenantID, uuid.UUID(newID).String())
		if r, rerr := blockpack.NewReaderFromProvider(&bytesReaderProvider{data: data}); rerr != nil {
			level.Warn(util_log.Logger).Log("msg", "vblockpack: value-index L0 skipped (compaction): open reader failed", "block", sourceRef, "err", rerr)
		} else if werr := blockpack.WriteValueIndexL0(r, store, sourceRef, s.tenantID, prefix); werr != nil {
			level.Warn(util_log.Logger).Log("msg", "vblockpack: value-index L0 write failed (compaction)", "block", sourceRef, "err", werr)
		}
	}
	return nil
}

// Size and ReadAt satisfy the blockpack.WritableStorage interface (extends Storage).
// Output storage is write-only; reads are not supported.
func (s *tempoOutputStorage) Size(_ string) (int64, error) {
	return 0, fmt.Errorf("output storage does not support reads")
}

func (s *tempoOutputStorage) ReadAt(_ string, _ []byte, _ int64, _ blockpack.DataType) (int, error) {
	return 0, fmt.Errorf("output storage does not support reads")
}

// Delete satisfies the blockpack.WritableStorage interface but is not used for output files.
func (s *tempoOutputStorage) Delete(_ string) error {
	return nil
}

const (
	minSpansPerBlock = 100
	maxSpansPerBlock = 10000
)

func maxSpansFromConfig(cfg *common.BlockConfig) int {
	if cfg.RowGroupSizeBytes == 0 {
		return 0
	}
	n := cfg.RowGroupSizeBytes / 1024
	if n < minSpansPerBlock {
		n = minSpansPerBlock
	}
	if n > maxSpansPerBlock {
		n = maxSpansPerBlock
	}
	return n
}
