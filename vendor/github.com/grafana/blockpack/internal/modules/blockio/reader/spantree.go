package reader

// NOTE-462 (issue #381): reader side of the SpanTree structural index. Mirrors the writer
// section framing in writer/spantree_tempfile.go. A lookup range-reads the fixed header +
// chunk directory once, then range-reads + snappy-decodes only the single chunk that can
// contain a target trace ID. Within a decoded chunk the records are fixed stride
// (shared.SpanTreeRecordSize), so a trace's records are located by binary search on traceID
// without decoding the whole chunk. A trace's records never span two chunks (the writer never
// splits a trace across a chunk boundary), so one chunk read resolves any single trace.
//
// The DFS in/out counters on each record collapse ancestor/descendant checks to two integer
// comparisons (shared.IsDescendant). SpanTree subsumes the chunked trace index for trace-by-ID:
// every (blockIdx, rowIdx) for a trace is present in its records.

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"sort"
	"sync"

	"github.com/grafana/blockpack/internal/modules/blockio/shared"
	"github.com/grafana/blockpack/internal/modules/rw"
)

// spanTreeDirEntry locates one independently-compressed chunk within the SpanTree section.
type spanTreeDirEntry struct {
	firstID   [16]byte
	compOff   uint32 // absolute file offset is secOffset + compOff
	compLen   uint32
	spanCount uint32
}

// spanTreeParsePrefix is the size of the leading read used to parse the header + directory in
// one I/O. 64 KiB covers sections with thousands of chunks; larger directories trigger one
// extra read.
const spanTreeParsePrefix = 64 << 10

// spanTreeIndex is the parsed header + directory of a ToCSubTypeSpanTree section. Chunk bodies
// and the bloom are loaded lazily via range reads, memoized per-Reader (intra-query, L1).
type spanTreeIndex struct {
	chunkCache map[int][]byte
	dir        []spanTreeDirEntry
	bloom      []byte
	// NOTE-468 (issue #388): per-chunk span-ID bloom region, lazily range-read as one blob and
	// sliced per chunk. spanBloomStride == 0 means the file is v1 (no span-bloom region), in
	// which case SpanTreeChunksForSpan reports all chunks as candidates (vacuously true).
	spanBloomRegion []byte

	secOffset uint64

	bloomOnce     sync.Once
	spanBloomOnce sync.Once
	chunkMu       sync.Mutex

	traceCount uint32
	blockCount uint32
	spanCount  uint32
	bloomOff   uint32
	bloomLen   uint32

	spanBloomOff    uint32
	spanBloomStride uint32
}

// ensureSpanTreeSection lazily parses the SpanTree section header + directory on first call.
// Returns (nil, nil) when the file carries no SpanTree section (older format).
func (r *Reader) ensureSpanTreeSection() (*spanTreeIndex, error) {
	r.spanTreeOnce.Do(func() {
		entry, ok := r.tocMap[shared.ToCKey{Type: shared.ToCTypeMetadata, SubType: shared.ToCSubTypeSpanTree}]
		if !ok {
			return
		}
		si, perr := r.parseSpanTreeIndex(entry)
		if perr != nil {
			r.spanTreeErr = fmt.Errorf("ensureSpanTreeSection: %w", perr)
			return
		}
		r.spanTree = si
	})
	return r.spanTree, r.spanTreeErr
}

// HasSpanTree reports whether the file carries a SpanTree structural index.
func (r *Reader) HasSpanTree() bool {
	si, _ := r.ensureSpanTreeSection()
	return si != nil
}

// parseSpanTreeIndex reads the header and chunk directory of the section in a single range read
// (falling back to one extra read for very large directories). Chunk bodies are not read here.
func (r *Reader) parseSpanTreeIndex(entry shared.ToCEntry) (*spanTreeIndex, error) {
	if entry.Length < shared.SpanTreeHeaderSizeV1 {
		return nil, fmt.Errorf("span tree index: section too short (%d bytes)", entry.Length)
	}
	prefixLen := min(uint64(entry.Length), uint64(spanTreeParsePrefix))
	prefix, err := r.readRange(entry.Offset, prefixLen, rw.DataTypeMetadata)
	if err != nil {
		return nil, fmt.Errorf("span tree index: header: %w", err)
	}
	if binary.LittleEndian.Uint32(prefix[0:]) != shared.SpanTreeMagic {
		return nil, fmt.Errorf("span tree index: bad magic 0x%08X", binary.LittleEndian.Uint32(prefix[0:]))
	}
	version := prefix[4]
	if version != shared.SpanTreeVersion && version != shared.SpanTreeVersionV1 {
		return nil, fmt.Errorf("span tree index: unsupported version %d", version)
	}
	si := &spanTreeIndex{
		secOffset:  entry.Offset,
		blockCount: binary.LittleEndian.Uint32(prefix[8:]),
		traceCount: binary.LittleEndian.Uint32(prefix[12:]),
		spanCount:  binary.LittleEndian.Uint32(prefix[16:]),
		bloomOff:   binary.LittleEndian.Uint32(prefix[28:]),
		bloomLen:   binary.LittleEndian.Uint32(prefix[32:]),
	}
	// NOTE-468: v2 appends span_bloom_off[36:40] + span_bloom_stride[40:44]. A v1 file has no
	// span-bloom region; spanBloomStride stays 0 and SpanTreeChunksForSpan reports all chunks.
	if version == shared.SpanTreeVersion && entry.Length >= shared.SpanTreeHeaderSize {
		si.spanBloomOff = binary.LittleEndian.Uint32(prefix[36:])
		si.spanBloomStride = binary.LittleEndian.Uint32(prefix[40:])
	}
	chunkCount := binary.LittleEndian.Uint32(prefix[20:])
	dirOff := binary.LittleEndian.Uint32(prefix[24:])
	if chunkCount == 0 {
		return si, nil
	}
	dirBytes := uint64(chunkCount) * shared.SpanTreeDirEntrySize
	if uint64(dirOff)+dirBytes > uint64(entry.Length) {
		return nil, fmt.Errorf("span tree index: directory out of bounds")
	}

	var dirRaw []byte
	if uint64(dirOff)+dirBytes <= uint64(len(prefix)) {
		dirRaw = prefix[dirOff : uint64(dirOff)+dirBytes]
	} else {
		dirRaw, err = r.readRange(entry.Offset+uint64(dirOff), dirBytes, rw.DataTypeMetadata)
		if err != nil {
			return nil, fmt.Errorf("span tree index: directory: %w", err)
		}
	}

	si.dir = make([]spanTreeDirEntry, chunkCount)
	for i := range si.dir {
		base := i * shared.SpanTreeDirEntrySize
		copy(si.dir[i].firstID[:], dirRaw[base:base+16])
		si.dir[i].compOff = binary.LittleEndian.Uint32(dirRaw[base+16:])
		si.dir[i].compLen = binary.LittleEndian.Uint32(dirRaw[base+20:])
		si.dir[i].spanCount = binary.LittleEndian.Uint32(dirRaw[base+24:])
	}
	return si, nil
}

// chunkForTrace returns the index of the directory entry whose chunk may contain traceID, or
// -1 if traceID precedes the first chunk. Chunks are sorted ascending by firstID; the target
// chunk is the last one whose firstID <= traceID.
func (si *spanTreeIndex) chunkForTrace(traceID [16]byte) int {
	if len(si.dir) == 0 {
		return -1
	}
	// Find first dir entry with firstID > traceID; the target is the one before it.
	idx := sort.Search(len(si.dir), func(i int) bool {
		return bytes.Compare(si.dir[i].firstID[:], traceID[:]) > 0
	})
	return idx - 1
}

// spanTreeChunkBytes lazily range-reads + snappy-decodes one chunk's fixed-stride record blob,
// memoized per-Reader.
func (r *Reader) spanTreeChunkBytes(si *spanTreeIndex, chunkIdx int) ([]byte, error) {
	si.chunkMu.Lock()
	if cached, ok := si.chunkCache[chunkIdx]; ok {
		si.chunkMu.Unlock()
		return cached, nil
	}
	si.chunkMu.Unlock()

	ent := si.dir[chunkIdx]
	body, err := r.readRangeDecodeSnappy(
		si.secOffset+uint64(ent.compOff),
		uint64(ent.compLen),
		rw.DataTypeMetadata,
	)
	if err != nil {
		return nil, fmt.Errorf("span tree index: chunk read/snappy decode: %w", err)
	}

	si.chunkMu.Lock()
	if si.chunkCache == nil {
		si.chunkCache = make(map[int][]byte, 1)
	}
	si.chunkCache[chunkIdx] = body
	si.chunkMu.Unlock()
	return body, nil
}

// SpanTreeForTrace returns every SpanTree record for traceID, or nil if the trace is not
// present or the file carries no SpanTree section. Records are returned in stored order
// (dfsIn ascending — parent before children), so a single sequential scan reconstructs the
// tree. SpanTree subsumes the chunked trace index for trace-by-ID: every (BlockIdx, RowIdx)
// for the trace is present in the returned records.
func (r *Reader) SpanTreeForTrace(traceID [16]byte) ([]shared.SpanTreeRecord, error) {
	si, err := r.ensureSpanTreeSection()
	if err != nil {
		return nil, err
	}
	if si == nil {
		return nil, nil
	}
	// Bloom reject cheaply when present.
	if bloom := r.spanTreeBloom(si); len(bloom) > 0 && !shared.TestTraceIDBloom(bloom, traceID) {
		return nil, nil
	}
	chunkIdx := si.chunkForTrace(traceID)
	if chunkIdx < 0 {
		return nil, nil
	}
	body, err := r.spanTreeChunkBytes(si, chunkIdx)
	if err != nil {
		return nil, err
	}
	return scanSpanTreeChunk(body, traceID), nil
}

// scanSpanTreeChunk binary-searches a decoded chunk for the first record of traceID, then walks
// forward collecting all contiguous records with that traceID. Records within a chunk are sorted
// by (traceID, dfsIn), so a trace's records form one contiguous run.
func scanSpanTreeChunk(body []byte, traceID [16]byte) []shared.SpanTreeRecord {
	stride := shared.SpanTreeRecordSize
	n := len(body) / stride
	if n == 0 {
		return nil
	}
	// Binary search for the first record whose traceID >= target.
	lo := sort.Search(n, func(i int) bool {
		base := i * stride
		return bytes.Compare(body[base:base+16], traceID[:]) >= 0
	})
	if lo >= n {
		return nil
	}
	var out []shared.SpanTreeRecord
	for i := lo; i < n; i++ {
		base := i * stride
		if !bytes.Equal(body[base:base+16], traceID[:]) {
			break
		}
		out = append(out, shared.DecodeSpanTreeRecord(body[base:base+stride]))
	}
	return out
}

// SpanTreeRecordForSpan resolves an 8-byte span ID to its SpanTree record (which carries the
// owning traceID and exact (BlockIdx, RowIdx)). It probes the per-chunk span-ID blooms to find
// the candidate chunk(s) (SpanTreeChunksForSpan), decodes each, and linear-scans for the span.
// Returns ok=false when the span is absent or the file carries no SpanTree section. NOTE-468.
//
// Span IDs are unique within a file's span set, so at most one record matches; the bloom may
// admit a few false-positive chunks, which the per-chunk scan then rejects.
func (r *Reader) SpanTreeRecordForSpan(spanID [8]byte) (rec shared.SpanTreeRecord, ok bool, err error) {
	chunks, cerr := r.SpanTreeChunksForSpan(spanID)
	if cerr != nil {
		return shared.SpanTreeRecord{}, false, cerr
	}
	if len(chunks) == 0 {
		return shared.SpanTreeRecord{}, false, nil
	}
	si, _ := r.ensureSpanTreeSection()
	if si == nil {
		return shared.SpanTreeRecord{}, false, nil
	}
	for _, chunkIdx := range chunks {
		body, berr := r.spanTreeChunkBytes(si, chunkIdx)
		if berr != nil {
			return shared.SpanTreeRecord{}, false, berr
		}
		if found, hit := scanSpanTreeChunkForSpan(body, spanID); hit {
			return found, true, nil
		}
	}
	return shared.SpanTreeRecord{}, false, nil
}

// scanSpanTreeChunkForSpan linear-scans a decoded chunk for the record whose span ID matches.
// Records within a chunk are sorted by (traceID, dfsIn), not by spanID, so this is a full scan
// of the chunk — bounded by SpanTreeRecordsPerChunk. Returns ok=false if no record matches.
func scanSpanTreeChunkForSpan(body []byte, spanID [8]byte) (shared.SpanTreeRecord, bool) {
	stride := shared.SpanTreeRecordSize
	n := len(body) / stride
	for i := 0; i < n; i++ {
		base := i * stride
		// span_id occupies bytes [16:24] of a record (after the 16-byte trace_id).
		if bytes.Equal(body[base+16:base+24], spanID[:]) {
			return shared.DecodeSpanTreeRecord(body[base : base+stride]), true
		}
	}
	return shared.SpanTreeRecord{}, false
}

// spanTreeBloom lazily range-reads the section's trace-ID bloom filter. Returns nil if absent.
func (r *Reader) spanTreeBloom(si *spanTreeIndex) []byte {
	si.bloomOnce.Do(func() {
		if si.bloomLen == 0 {
			return
		}
		b, err := r.readRange(si.secOffset+uint64(si.bloomOff), uint64(si.bloomLen), rw.DataTypeMetadata)
		if err == nil {
			si.bloom = b
		}
	})
	return si.bloom
}

// ensureSpanBloomRegion lazily range-reads the entire per-chunk span-ID bloom region in one I/O
// and caches it on the index. Returns nil for v1 files (no region) or on read error (callers
// then treat all chunks as candidates, never producing a false negative). NOTE-468.
func (r *Reader) ensureSpanBloomRegion(si *spanTreeIndex) []byte {
	si.spanBloomOnce.Do(func() {
		if si.spanBloomStride == 0 || len(si.dir) == 0 {
			return
		}
		regionLen := uint64(si.spanBloomStride) * uint64(len(si.dir))
		b, err := r.readRange(si.secOffset+uint64(si.spanBloomOff), regionLen, rw.DataTypeMetadata)
		if err == nil {
			si.spanBloomRegion = b
		}
	})
	return si.spanBloomRegion
}

// SpanTreeChunksForSpan returns the indices of SpanTree chunks whose per-chunk span-ID bloom
// admits spanID — i.e. the chunks that may contain a record for that span. Expected result for
// a present span: 1 real chunk + ≈0 false positives (≈2% FPR per chunk).
//
// For a v1 file (no span-bloom region) or when the region cannot be read, every chunk is a
// candidate (a span ID is not located by traceID, so there is no cheap directory search) — this
// is vacuously correct and never produces a false negative. Returns nil if the file carries no
// SpanTree section. NOTE-468 (issue #388).
func (r *Reader) SpanTreeChunksForSpan(spanID [8]byte) ([]int, error) {
	si, err := r.ensureSpanTreeSection()
	if err != nil {
		return nil, err
	}
	if si == nil || len(si.dir) == 0 {
		return nil, nil
	}
	region := r.ensureSpanBloomRegion(si)
	if len(region) == 0 {
		// v1 file or unreadable region: every chunk is a candidate.
		out := make([]int, len(si.dir))
		for i := range out {
			out[i] = i
		}
		return out, nil
	}
	stride := int(si.spanBloomStride)
	var out []int
	for i := range si.dir {
		base := i * stride
		if base+stride > len(region) {
			break
		}
		if shared.TestSpanIDBloom(region[base:base+stride], spanID) {
			out = append(out, i)
		}
	}
	return out, nil
}

// SpanTreeIdentityForBlock returns a map from RowIdx to the full SpanTreeRecord for every
// span stored in the given block, scanning the SpanTree section once. It is the identity
// reverse-lookup source for blocks that no longer carry the trace:id/span:id/span:parent_id
// IntrinsicTOC columns (NOTE-476, issue #394). Returns (nil, nil) when the file carries no
// SpanTree section.
//
// The result is memoized on the Reader, keyed by blockIdx — the first request for any block
// decodes every chunk (records are sorted by (traceID, dfsIn), not by block, so a single
// full scan is required) and builds the per-block maps for ALL blocks at once, so subsequent
// per-block requests within the same query are free. Reader is constructed fresh per query
// per block, so this whole-section scan happens at most once per query.
func (r *Reader) SpanTreeIdentityForBlock(blockIdx uint16) (map[uint16]shared.SpanTreeRecord, error) {
	si, err := r.ensureSpanTreeSection()
	if err != nil {
		return nil, err
	}
	if si == nil {
		return nil, nil
	}

	r.spanTreeIdentityMu.Lock()
	defer r.spanTreeIdentityMu.Unlock()

	if r.spanTreeIdentityByBlock == nil {
		built, berr := r.buildSpanTreeIdentityMaps(si)
		if berr != nil {
			return nil, berr
		}
		r.spanTreeIdentityByBlock = built
	}
	return r.spanTreeIdentityByBlock[blockIdx], nil
}

// buildSpanTreeIdentityMaps decodes every SpanTree chunk once and groups the records into
// per-block RowIdx->record maps. Caller holds spanTreeIdentityMu. NOTE-476.
func (r *Reader) buildSpanTreeIdentityMaps(si *spanTreeIndex) (map[uint16]map[uint16]shared.SpanTreeRecord, error) {
	out := make(map[uint16]map[uint16]shared.SpanTreeRecord)
	stride := shared.SpanTreeRecordSize
	for chunkIdx := range si.dir {
		body, err := r.spanTreeChunkBytes(si, chunkIdx)
		if err != nil {
			return nil, fmt.Errorf("SpanTreeIdentity: chunk %d: %w", chunkIdx, err)
		}
		n := len(body) / stride
		for i := 0; i < n; i++ {
			rec := shared.DecodeSpanTreeRecord(body[i*stride : i*stride+stride])
			blockMap, ok := out[rec.BlockIdx]
			if !ok {
				blockMap = make(map[uint16]shared.SpanTreeRecord)
				out[rec.BlockIdx] = blockMap
			}
			blockMap[rec.RowIdx] = rec
		}
	}
	return out, nil
}
