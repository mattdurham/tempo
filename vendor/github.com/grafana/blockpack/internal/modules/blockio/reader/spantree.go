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

	secOffset uint64

	bloomOnce sync.Once
	chunkMu   sync.Mutex

	traceCount uint32
	blockCount uint32
	spanCount  uint32
	bloomOff   uint32
	bloomLen   uint32
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
	if entry.Length < shared.SpanTreeHeaderSize {
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
	if prefix[4] != shared.SpanTreeVersion {
		return nil, fmt.Errorf("span tree index: unsupported version %d", prefix[4])
	}
	si := &spanTreeIndex{
		secOffset:  entry.Offset,
		blockCount: binary.LittleEndian.Uint32(prefix[8:]),
		traceCount: binary.LittleEndian.Uint32(prefix[12:]),
		spanCount:  binary.LittleEndian.Uint32(prefix[16:]),
		bloomOff:   binary.LittleEndian.Uint32(prefix[28:]),
		bloomLen:   binary.LittleEndian.Uint32(prefix[32:]),
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
