package writer

// NOTE: Range-readable chunked trace index (issue #340). The whole section is written RAW
// (not snappy-compressed by writeToCEntry); each chunk is independently snappy-compressed so a
// reader can range-read + decompress only the one chunk that can contain a target trace ID,
// instead of fetching and decompressing the entire trace index. Each chunk decompresses to a
// valid v2 mini-body (fmt_version[1] + entry_count[4] + sorted entries) so the reader reuses the
// existing entry decode/scan logic.

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
	"slices"

	"github.com/grafana/blockpack/internal/modules/blockio/shared"
	"github.com/klauspost/compress/snappy"
)

// NOTE-223: buildChunkedTraceIndex serializes the chunked trace index section (ToCSubTypeTraceChunked).
//
// Layout (all offsets relative to section start; section stored uncompressed):
//
//	header[ChunkedTraceHeaderSize]:
//	  magic[4] + version[1] + reserved[3] + block_count[4] + trace_count[4] +
//	  chunk_count[4] + entry_fmt[1] + reserved2[3] + dir_off[4] + bloom_off[4] + bloom_len[4]
//	directory[chunk_count × ChunkedTraceDirEntrySize]:
//	  first_trace_id[16] + comp_off[4] + comp_len[4]
//	chunks: chunk_count × snappy(fmt_version[1] + entry_count[4] + entries)
//	  entry := trace_id[16] + block_ref_count[2 LE] + block_id[2 LE] × N
//	bloom[bloom_len]: trace ID bloom filter (same encoding as the compact index v2 bloom)
//
// The block_count field is informational only; block file offsets come from the block_index
// section, so no block table is duplicated here.
func buildChunkedTraceIndex(blockMetas []shared.BlockMeta, traceIndex map[[16]byte][]uint16) ([]byte, error) {
	traceIDs := make([][16]byte, 0, len(traceIndex))
	for tid := range traceIndex {
		traceIDs = append(traceIDs, tid)
	}
	slices.SortFunc(traceIDs, func(a, b [16]byte) int { return bytes.Compare(a[:], b[:]) })

	perChunk := shared.ChunkedTraceEntriesPerChunk
	chunkCount := (len(traceIDs) + perChunk - 1) / perChunk

	type dirEnt struct {
		firstID [16]byte
		compOff uint32
		compLen uint32
	}
	dir := make([]dirEnt, 0, chunkCount)
	var chunkBuf bytes.Buffer // concatenated compressed chunks

	for start := 0; start < len(traceIDs); start += perChunk {
		end := min(start+perChunk, len(traceIDs))
		mini, err := encodeTraceMiniBody(traceIDs[start:end], traceIndex)
		if err != nil {
			return nil, err
		}
		compressed := snappy.Encode(nil, mini)
		dir = append(dir, dirEnt{
			firstID: traceIDs[start],
			compOff: 0,                       // backfilled once base offset is known
			compLen: uint32(len(compressed)), //nolint:gosec // bounded by chunk size
		})
		chunkBuf.Write(compressed)
	}

	// Bloom over all trace IDs (lets MayContainTraceID/TraceBloomRaw work without reading chunks).
	bloomSize := shared.TraceIDBloomSize(len(traceIndex))
	bloom := make([]byte, bloomSize)
	for tid := range traceIndex {
		shared.AddTraceIDToBloom(bloom, tid)
	}

	dirOff := shared.ChunkedTraceHeaderSize
	chunksOff := dirOff + chunkCount*shared.ChunkedTraceDirEntrySize
	bloomOff := chunksOff + chunkBuf.Len()

	// Backfill absolute chunk offsets now that the chunk region base is known.
	var running int
	for i := range dir {
		dir[i].compOff = uint32(chunksOff + running) //nolint:gosec
		running += int(dir[i].compLen)
	}

	var out bytes.Buffer
	out.Grow(bloomOff + bloomSize)
	var tmp4 [4]byte

	binary.LittleEndian.PutUint32(tmp4[:], shared.ChunkedTraceMagic)
	out.Write(tmp4[:])
	out.WriteByte(shared.ChunkedTraceVersion)
	out.Write([]byte{0, 0, 0})                                      // reserved
	binary.LittleEndian.PutUint32(tmp4[:], uint32(len(blockMetas))) //nolint:gosec
	out.Write(tmp4[:])
	binary.LittleEndian.PutUint32(tmp4[:], uint32(len(traceIDs))) //nolint:gosec
	out.Write(tmp4[:])
	binary.LittleEndian.PutUint32(tmp4[:], uint32(chunkCount)) //nolint:gosec
	out.Write(tmp4[:])
	out.WriteByte(shared.TraceIndexFmtVersion2)
	out.Write([]byte{0, 0, 0})                             // reserved2
	binary.LittleEndian.PutUint32(tmp4[:], uint32(dirOff)) //nolint:gosec
	out.Write(tmp4[:])
	binary.LittleEndian.PutUint32(tmp4[:], uint32(bloomOff)) //nolint:gosec
	out.Write(tmp4[:])
	binary.LittleEndian.PutUint32(tmp4[:], uint32(bloomSize)) //nolint:gosec
	out.Write(tmp4[:])

	for i := range dir {
		out.Write(dir[i].firstID[:])
		binary.LittleEndian.PutUint32(tmp4[:], dir[i].compOff)
		out.Write(tmp4[:])
		binary.LittleEndian.PutUint32(tmp4[:], dir[i].compLen)
		out.Write(tmp4[:])
	}
	out.Write(chunkBuf.Bytes())
	out.Write(bloom)

	return out.Bytes(), nil
}

// encodeTraceMiniBody encodes a sorted run of trace IDs as a v2 mini-body:
// fmt_version[1] + entry_count[4 LE] + per entry: trace_id[16] + block_ref_count[2 LE] +
// block_id[2 LE] × N. This is the same per-entry encoding used by the legacy trace index, so
// the reader's existing entry decode/scan walks it unchanged.
func encodeTraceMiniBody(ids [][16]byte, traceIndex map[[16]byte][]uint16) ([]byte, error) {
	var buf bytes.Buffer
	buf.WriteByte(shared.TraceIndexFmtVersion2)
	var tmp4 [4]byte
	binary.LittleEndian.PutUint32(tmp4[:], uint32(len(ids))) //nolint:gosec
	buf.Write(tmp4[:])

	var tmp2 [2]byte
	for _, tid := range ids {
		blockIDs := traceIndex[tid]
		if len(blockIDs) > math.MaxUint16 {
			return nil, fmt.Errorf(
				"chunked trace_index: trace %x has %d block refs, exceeds uint16 max",
				tid,
				len(blockIDs),
			)
		}
		buf.Write(tid[:])
		binary.LittleEndian.PutUint16(tmp2[:], uint16(len(blockIDs))) //nolint:gosec
		buf.Write(tmp2[:])
		for _, bid := range blockIDs {
			binary.LittleEndian.PutUint16(tmp2[:], bid)
			buf.Write(tmp2[:])
		}
	}
	return buf.Bytes(), nil
}
