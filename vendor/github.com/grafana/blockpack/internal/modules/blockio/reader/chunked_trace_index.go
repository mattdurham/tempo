package reader

// NOTE: Range-readable chunked trace index (issue #340). Mirrors the writer layout in
// writer/chunked_trace_index.go. A lookup range-reads the fixed header + directory once, then
// range-reads + snappy-decodes only the single chunk that can contain the target trace ID,
// instead of fetching and decompressing the whole trace index. Each chunk decompresses to a v2
// mini-body (fmt_version[1] + entry_count[4] + sorted entries), reusing the existing entry
// decode/scan helpers (traceEntryStride, decodeTraceEntryBlocks).

import (
	"encoding/binary"
	"fmt"
	"strconv"
	"strings"
	"sync"

	"github.com/grafana/blockpack/internal/modules/blockio/shared"
	"github.com/grafana/blockpack/internal/modules/rw"
)

// chunkDirEntry locates one independently-compressed chunk within the section.
type chunkDirEntry struct {
	firstID [16]byte
	compOff uint32 // absolute file offset is secOffset + compOff
	compLen uint32
}

// chunkedTraceParsePrefix is the size of the leading read used to parse the header and
// directory in a single I/O. The directory fits within it for files up to
// (chunkedTraceParsePrefix-header)/ChunkedTraceDirEntrySize chunks; larger directories trigger
// one extra read. 64 KiB covers files with millions of traces (≈2700 chunks) in one read.
const chunkedTraceParsePrefix = 64 << 10

// chunkedTraceIndex is the parsed header + directory of a ToCSubTypeTraceChunked section.
// The chunk bodies and bloom are loaded lazily via range reads.
type chunkedTraceIndex struct {
	chunkCache map[int][]byte
	dir        []chunkDirEntry
	bloom      []byte

	secOffset uint64

	bloomOnce sync.Once

	// chunkCache is the per-Reader (intra-query, L1) memoization of decompressed chunk
	// mini-bodies by directory index so repeated lookups that resolve to the same chunk
	// (and concurrent GetTraceByID callers) within ONE query avoid re-reading and
	// re-decompressing it. Guarded by chunkMu. NOTE-404: cross-query reuse is served by the
	// process-level parsedTraceChunkCache (L2), since a Reader is fresh per query.
	chunkMu    sync.Mutex
	traceCount uint32
	blockCount uint32
	bloomOff   uint32
	bloomLen   uint32

	entryFmt uint8
}

// NOTE-292: parseChunkedTraceIndex reads the header and chunk directory of the section in a single range
// read (falling back to one extra read only for very large directories) and returns the parsed
// index. Chunk bodies and the bloom are not read here.
func (r *Reader) parseChunkedTraceIndex(entry shared.ToCEntry) (*chunkedTraceIndex, error) {
	if entry.Length < shared.ChunkedTraceHeaderSize {
		return nil, fmt.Errorf("chunked trace index: section too short (%d bytes)", entry.Length)
	}
	prefixLen := min(uint64(entry.Length), uint64(chunkedTraceParsePrefix))
	prefix, err := r.readRange(entry.Offset, prefixLen, rw.DataTypeMetadata)
	if err != nil {
		return nil, fmt.Errorf("chunked trace index: header: %w", err)
	}
	if binary.LittleEndian.Uint32(prefix[0:]) != shared.ChunkedTraceMagic {
		return nil, fmt.Errorf("chunked trace index: bad magic 0x%08X", binary.LittleEndian.Uint32(prefix[0:]))
	}
	if prefix[4] != shared.ChunkedTraceVersion {
		return nil, fmt.Errorf("chunked trace index: unsupported version %d", prefix[4])
	}
	ci := &chunkedTraceIndex{
		secOffset:  entry.Offset,
		blockCount: binary.LittleEndian.Uint32(prefix[8:]),
		traceCount: binary.LittleEndian.Uint32(prefix[12:]),
		entryFmt:   prefix[20],
		bloomOff:   binary.LittleEndian.Uint32(prefix[28:]),
		bloomLen:   binary.LittleEndian.Uint32(prefix[32:]),
	}
	chunkCount := binary.LittleEndian.Uint32(prefix[16:])
	dirOff := binary.LittleEndian.Uint32(prefix[24:])
	if chunkCount == 0 {
		return ci, nil
	}
	dirBytes := uint64(chunkCount) * shared.ChunkedTraceDirEntrySize
	if uint64(dirOff)+dirBytes > uint64(entry.Length) {
		return nil, fmt.Errorf("chunked trace index: directory out of bounds")
	}

	var dirRaw []byte
	if uint64(dirOff)+dirBytes <= uint64(len(prefix)) {
		dirRaw = prefix[dirOff : uint64(dirOff)+dirBytes]
	} else {
		dirRaw, err = r.readRange(entry.Offset+uint64(dirOff), dirBytes, rw.DataTypeMetadata)
		if err != nil {
			return nil, fmt.Errorf("chunked trace index: directory: %w", err)
		}
	}
	ci.dir = make([]chunkDirEntry, chunkCount)
	for i := range ci.dir {
		base := i * shared.ChunkedTraceDirEntrySize
		copy(ci.dir[i].firstID[:], dirRaw[base:base+16])
		ci.dir[i].compOff = binary.LittleEndian.Uint32(dirRaw[base+16:])
		ci.dir[i].compLen = binary.LittleEndian.Uint32(dirRaw[base+20:])
	}
	return ci, nil
}

// lookup returns the block IDs for traceID, or nil if absent. It binary-searches the directory
// for the candidate chunk, range-reads + snappy-decodes only that chunk, and scans it.
func (r *Reader) chunkedLookup(ci *chunkedTraceIndex, traceID [16]byte) ([]uint16, error) {
	if len(ci.dir) == 0 {
		return nil, nil
	}
	targetHi := binary.BigEndian.Uint64(traceID[0:8])
	targetLo := binary.BigEndian.Uint64(traceID[8:16])

	// Find the last directory entry whose firstID <= target. Entries are sorted ascending,
	// so the target, if present, lies in that chunk.
	lo, hi := 0, len(ci.dir)
	for lo < hi {
		mid := int(uint(lo+hi) >> 1)
		ehi := binary.BigEndian.Uint64(ci.dir[mid].firstID[0:8])
		elo := binary.BigEndian.Uint64(ci.dir[mid].firstID[8:16])
		if ehi < targetHi || (ehi == targetHi && elo <= targetLo) {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	if lo == 0 {
		return nil, nil // target below the first chunk's first ID → absent
	}
	chunkIdx := lo - 1
	mini, err := r.chunkBytes(ci, chunkIdx)
	if err != nil {
		return nil, err
	}
	return scanMiniBody(mini, traceID), nil
}

// chunkBytes returns the decompressed mini-body for the directory entry at chunkIdx.
//
// Two-tier cache (NOTE-404):
//   - L1 (intra-query): the per-Reader ci.chunkCache (NOTE-292) memoizes within ONE query so
//     concurrent / repeat lookups resolving to the same chunk in this Reader avoid re-decode.
//   - L2 (cross-query): the process-level parsedTraceChunkCache holds the decoded mini-body
//     keyed by (fileID, section offset, chunkIdx) so a warm-file REPEAT lookup served by a
//     fresh Reader becomes a cache hit instead of a provider round-trip + snappy decode.
//
// The L2 cache is consulted only when the Reader has a fileID (the process cache key requires
// one); fileID-less Readers (e.g. some tests) fall back to read+decode and the per-Reader L1.
func (r *Reader) chunkBytes(ci *chunkedTraceIndex, chunkIdx int) ([]byte, error) {
	// L1: intra-query per-Reader memoization.
	ci.chunkMu.Lock()
	if cached, ok := ci.chunkCache[chunkIdx]; ok {
		ci.chunkMu.Unlock()
		return cached, nil
	}
	ci.chunkMu.Unlock()

	// L2: cross-query process-level cache.
	var procKey string
	if r.fileID != "" {
		procKey = r.traceChunkProcKey(ci, chunkIdx)
		if tc := parsedTraceChunkCache.Get(procKey); tc != nil {
			r.recordChunkL1(ci, chunkIdx, tc.body)
			return tc.body, nil
		}
	}

	ent := ci.dir[chunkIdx]
	// NOTE-366: read into pooled scratch, decode, recycle the compressed buffer.
	// SPEC-ROOT-012: decodeBoundedSnappy checks DecodedLen before allocating.
	mini, decErr := r.readRangeDecodeSnappy(
		ci.secOffset+uint64(ent.compOff),
		uint64(ent.compLen),
		rw.DataTypeTraceBloomFilter,
	)
	if decErr != nil {
		return nil, fmt.Errorf("chunked trace index: chunk read/snappy decode: %w", decErr)
	}

	// Populate L2 first (cache-safe: mini is the freshly allocated snappy.Decode dst, never
	// pooled/shared), then L1. A Put failure (over budget) is non-fatal — L1 still serves
	// this query.
	if procKey != "" {
		_ = parsedTraceChunkCache.Put(procKey, &traceChunk{body: mini})
	}
	r.recordChunkL1(ci, chunkIdx, mini)
	return mini, nil
}

// recordChunkL1 stores mini in the per-Reader L1 chunk cache under chunkIdx.
func (r *Reader) recordChunkL1(ci *chunkedTraceIndex, chunkIdx int, mini []byte) {
	ci.chunkMu.Lock()
	if ci.chunkCache == nil {
		ci.chunkCache = make(map[int][]byte, 1)
	}
	ci.chunkCache[chunkIdx] = mini
	ci.chunkMu.Unlock()
}

// traceChunkProcKey builds the process-cache key for a chunk. The section offset disambiguates
// multiple chunked-trace sections in one file; chunkIdx selects the directory entry.
func (r *Reader) traceChunkProcKey(ci *chunkedTraceIndex, chunkIdx int) string {
	var b strings.Builder
	b.Grow(len(r.fileID) + 32)
	b.WriteString(r.fileID)
	b.WriteString("/tracechunk/")
	b.WriteString(strconv.FormatUint(ci.secOffset, 10))
	b.WriteByte('/')
	b.WriteString(strconv.Itoa(chunkIdx))
	return b.String()
}

// scanMiniBody walks a decompressed v2 mini-body (fmt_version[1] + entry_count[4] + sorted
// entries) for an exact trace ID match and returns its block IDs, or nil. Entries are sorted
// ascending, so the walk stops once it passes the target.
func scanMiniBody(data []byte, traceID [16]byte) []uint16 {
	if len(data) < 5 {
		return nil
	}
	fmtVersion := data[0]
	if fmtVersion != shared.TraceIndexFmtVersion && fmtVersion != shared.TraceIndexFmtVersion2 {
		return nil
	}
	targetHi := binary.BigEndian.Uint64(traceID[0:8])
	targetLo := binary.BigEndian.Uint64(traceID[8:16])
	pos := 5
	for pos+16 <= len(data) {
		entryHi := binary.BigEndian.Uint64(data[pos : pos+8])
		entryLo := binary.BigEndian.Uint64(data[pos+8 : pos+16])
		if entryHi == targetHi && entryLo == targetLo {
			return decodeTraceEntryBlocks(data, pos, fmtVersion)
		}
		if entryHi > targetHi || (entryHi == targetHi && entryLo > targetLo) {
			return nil
		}
		stride, ok := traceEntryStride(data, pos, fmtVersion)
		if !ok {
			return nil
		}
		pos += stride
	}
	return nil
}

// traceBloom lazily range-reads the section's trace ID bloom filter (used by MayContainTraceID
// and TraceBloomRaw). Returns nil if the section carries no bloom.
func (r *Reader) chunkedTraceBloom(ci *chunkedTraceIndex) []byte {
	ci.bloomOnce.Do(func() {
		if ci.bloomLen == 0 {
			return
		}
		b, err := r.readRange(ci.secOffset+uint64(ci.bloomOff), uint64(ci.bloomLen), rw.DataTypeTraceBloomFilter)
		if err == nil {
			ci.bloom = b
		}
	})
	return ci.bloom
}
