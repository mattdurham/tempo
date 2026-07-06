package valueindex

// traceindex.go — TraceID index: parent/child span structure for trace
// reconstruction from the index alone (issue #428, NOTE-VI-038).
//
// A standard value index entry answers "which spans have column C = value V at
// time T". The TraceID index answers "given traceID T, what is the complete
// span tree" — so each span carries its SpanID, ParentSpanID, and a direct
// reference (SourceRef + BlockRef + RowIdx) to the data block holding it.
//
// The TraceID index is stored under the hash("trace:id") column directory and
// follows the standard column-type path convention (ColumnTypeUUID segment).
// This file owns only the inner TraceGroup/SpanEntry payload: encode, decode,
// merge (compaction), and tree assembly (querier). The outer file framing
// (header/blocks/TOC/footer) is shared with the standard value index.

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"sort"

	"github.com/golang/snappy"

	"github.com/grafana/blockpack/internal/modules/blockio/shared"
)

// SpanEntry is one span within a TraceGroup. It carries the parent/child link
// (ParentSpanID, zero for the root) plus a direct data-block reference so the
// span's fields can be materialized without scanning unrelated spans.
//
// SourceRef is stored via the string table on the wire (uint16 index) to
// deduplicate the repeated blockpack file path across spans of the same trace.
type SpanEntry struct {
	SourceRef    string   // S3 key of the data blockpack file (string-table interned)
	SpanID       [8]byte  // span identity
	ParentSpanID [8]byte  // zero if root span
	BlockRef     BlockRef // page-addressed reference to the data block
	RowIdx       uint16   // row within the block for O(1) span access
}

// IsRoot reports whether this span has no parent (ParentSpanID is all zero).
func (s SpanEntry) IsRoot() bool { return s.ParentSpanID == [8]byte{} }

// TraceGroup is the set of spans for a single trace, time-bucketed by the
// root span's start second. Spans may originate from multiple blockpack files.
type TraceGroup struct {
	Spans   []SpanEntry
	TimeSec uint64 // 1-second bucket (root span start)
	TraceID [16]byte
}

// spanEntryWireSize is the per-span byte size on the wire:
// span_id[8] + parent_span_id[8] + source_ref_idx[2] + block_ref[5] + row_idx[2].
const spanEntryWireSize = 8 + 8 + 2 + BlockRefSize + 2

// minGroupWireSize is the fixed per-group header cost on the wire, before any
// span data: time_sec[8] + trace_id[16] + span_count[4]. Used to bounds-check
// an untrusted on-disk groupCount against the remaining buffer before
// allocating -- the minimum any group could possibly cost.
const minGroupWireSize = 8 + 16 + 4

// TraceFileMagic identifies a v2 batched TraceGroup index file. ASCII "VTG2".
// It is checked at both the file header and footer so a partial ranged read
// (footer only) can validate it is a v2 file without fetching the body, and so
// DecodeTraceGroups can dispatch v2 vs the legacy flat-blob format (issue #476).
const TraceFileMagic uint32 = 0x56544732

// TraceFileVersion is the current batched TraceGroup file format version.
const TraceFileVersion uint8 = 0x02

// traceFooterSize is the fixed byte size of a v2 TraceGroup file footer.
//
//	magic[4] + block_index_off[8] + block_index_len[8] +
//	str_table_off[8] + str_table_len[8] +
//	min_time_sec[8] + max_time_sec[8] + version[1]
const traceFooterSize = 4 + 8 + 8 + 8 + 8 + 8 + 8 + 1

// TraceFooterSize is the exported fixed footer byte size, for callers performing
// a ranged tail read to fetch only the footer + block directory.
const TraceFooterSize = traceFooterSize

// traceBlockDirEntrySize is the fixed byte size of one entry in the v2 block
// directory: comp_off[8] + comp_len[8] + min_trace_id[16] + max_trace_id[16] +
// min_time_sec[8] + max_time_sec[8].
const traceBlockDirEntrySize = 8 + 8 + 16 + 16 + 8 + 8

// traceBlockDirEntry is one record in the v2 block directory (TOC). It lets a
// trace-by-id lookup skip whole blocks by TraceID range (the point-lookup key)
// or time range without decompressing the block body.
type traceBlockDirEntry struct {
	compOff    uint64
	compLen    uint64
	minTraceID [16]byte
	maxTraceID [16]byte
	minTimeSec uint64
	maxTimeSec uint64
}

// EncodeTraceGroups encodes a slice of TraceGroups into the v2 batched
// TraceGroup index format (magic "VTG2", issue #476). Groups are sorted by
// (TraceID ASC, TimeSec ASC) — TraceID first because it is the trace-by-id
// point-lookup key — and split into blocks of at most
// shared.ValueIndexTraceGroupsPerBlock groups. Each block carries its own
// min/max TraceID, min/max TimeSec, and a bloom over its trace IDs; the querier
// prunes at block granularity via the footer-addressed block directory, so only
// the surviving block(s) are ever fetched (see traceindexquery.go).
//
// File layout:
//
//	magic[4] version[1]                         -- header
//	[ block 0 ] ... [ block N ]                 -- snappy-compressed block bodies
//	string_table (EncodeStringTable)
//	block_index (TOC: one traceBlockDirEntry per block)
//	footer[traceFooterSize]                     -- fixed size, tail-addressable
//
// Block body (before snappy):
//
//	min_trace_id[16] max_trace_id[16]
//	min_time_sec[8] max_time_sec[8]
//	bloom_len[4] bloom[N]                        -- bloom over the block's trace IDs
//	group_count[4]
//	  for each group (sorted TraceID ASC, TimeSec ASC):
//	    time_sec[8] trace_id[16] span_count[4]
//	    for each span:
//	        span_id[8] parent_span_id[8] source_ref_idx[2] block_ref[5] row_idx[2]
//
// Returns ErrStringTableOverflow if the groups reference more than
// MaxStringTableEntries distinct SourceRefs (caller must split — see compaction).
func EncodeTraceGroups(groups []TraceGroup) ([]byte, error) {
	return encodeTraceGroups(groups, shared.ValueIndexTraceGroupsPerBlock)
}

// encodeTraceGroups is the parameterized encoder EncodeTraceGroups delegates to.
// perBlock is the max groups per block; EncodeTraceGroups passes the configured
// shared.ValueIndexTraceGroupsPerBlock, and tests pass small values to exercise
// multi-block pruning without building huge fixtures. A perBlock <= 0 is treated
// as 1 (a single group per block) rather than panicking on the modulo.
func encodeTraceGroups(groups []TraceGroup, perBlock int) ([]byte, error) {
	if perBlock < 1 {
		perBlock = 1
	}
	sorted := make([]TraceGroup, len(groups))
	copy(sorted, groups)
	sortTraceGroups(sorted)

	table := NewStringTable()
	// Pre-intern in deterministic order so the table layout is stable.
	for i := range sorted {
		for j := range sorted[i].Spans {
			if _, ok := table.Intern(sorted[i].Spans[j].SourceRef); !ok {
				return nil, ErrStringTableOverflow
			}
		}
	}

	// Header: magic[4] + version[1].
	out := make([]byte, 0, len(sorted)*spanEntryWireSize+256)
	out = binary.LittleEndian.AppendUint32(out, TraceFileMagic)
	out = append(out, TraceFileVersion)
	headerLen := uint64(len(out))

	dir := make([]traceBlockDirEntry, 0, (len(sorted)+perBlock-1)/perBlock)
	var fileMin, fileMax uint64
	haveTime := false

	for start := 0; start < len(sorted); start += perBlock {
		end := start + perBlock
		if end > len(sorted) {
			end = len(sorted)
		}
		block := sorted[start:end]
		raw := encodeTraceBlock(block, table)
		compressed := snappy.Encode(nil, raw)

		e := traceBlockDirEntry{
			compOff:    uint64(len(out)),
			compLen:    uint64(len(compressed)),
			minTraceID: block[0].TraceID,
			maxTraceID: block[len(block)-1].TraceID,
		}
		bMin, bMax := traceBlockTimeRange(block)
		e.minTimeSec, e.maxTimeSec = bMin, bMax
		if !haveTime || bMin < fileMin {
			fileMin = bMin
		}
		if !haveTime || bMax > fileMax {
			fileMax = bMax
		}
		haveTime = true
		dir = append(dir, e)
		out = append(out, compressed...)
	}

	// String table.
	strOff := uint64(len(out))
	strBytes := EncodeStringTable(table)
	out = append(out, strBytes...)
	strLen := uint64(len(strBytes))

	// Block index (TOC).
	blockIdxOff := uint64(len(out))
	out = appendTraceBlockIndex(out, dir)
	blockIdxLen := uint64(len(out)) - blockIdxOff

	// Footer.
	out = binary.LittleEndian.AppendUint32(out, TraceFileMagic)
	out = binary.LittleEndian.AppendUint64(out, blockIdxOff)
	out = binary.LittleEndian.AppendUint64(out, blockIdxLen)
	out = binary.LittleEndian.AppendUint64(out, strOff)
	out = binary.LittleEndian.AppendUint64(out, strLen)
	out = binary.LittleEndian.AppendUint64(out, fileMin)
	out = binary.LittleEndian.AppendUint64(out, fileMax)
	out = append(out, TraceFileVersion)

	_ = headerLen // dir offsets are already file-absolute (header written first).
	return out, nil
}

// traceBlockTimeRange returns the min/max TimeSec across a block's groups.
func traceBlockTimeRange(block []TraceGroup) (minSec, maxSec uint64) {
	for i := range block {
		t := block[i].TimeSec
		if i == 0 || t < minSec {
			minSec = t
		}
		if i == 0 || t > maxSec {
			maxSec = t
		}
	}
	return minSec, maxSec
}

// encodeTraceBlock serializes one block of groups (already sorted) into the
// uncompressed block-body wire form, computing the block's trace-ID bloom.
func encodeTraceBlock(block []TraceGroup, table *StringTable) []byte {
	buf := make([]byte, 0, 64+len(block)*(minGroupWireSize+spanEntryWireSize))

	bMin, bMax := traceBlockTimeRange(block)
	buf = append(buf, block[0].TraceID[:]...)
	buf = append(buf, block[len(block)-1].TraceID[:]...)
	buf = binary.LittleEndian.AppendUint64(buf, bMin)
	buf = binary.LittleEndian.AppendUint64(buf, bMax)

	bloom := make([]byte, ValueBloomSize(len(block)))
	for i := range block {
		AddValueToBloom(bloom, block[i].TraceID[:])
	}
	buf = binary.LittleEndian.AppendUint32(buf, uint32(len(bloom))) //nolint:gosec
	buf = append(buf, bloom...)

	buf = binary.LittleEndian.AppendUint32(buf, uint32(len(block))) //nolint:gosec
	for i := range block {
		g := &block[i]
		buf = binary.LittleEndian.AppendUint64(buf, g.TimeSec)
		buf = append(buf, g.TraceID[:]...)
		buf = binary.LittleEndian.AppendUint32(buf, uint32(len(g.Spans))) //nolint:gosec
		for j := range g.Spans {
			s := &g.Spans[j]
			idx, _ := table.Intern(s.SourceRef) // already interned by caller
			buf = append(buf, s.SpanID[:]...)
			buf = append(buf, s.ParentSpanID[:]...)
			buf = binary.LittleEndian.AppendUint16(buf, idx)
			buf = AppendBlockRef(buf, s.BlockRef)
			buf = binary.LittleEndian.AppendUint16(buf, s.RowIdx)
		}
	}
	return buf
}

// appendTraceBlockIndex serializes the block directory: count[4] then one
// fixed-size traceBlockDirEntry per block.
func appendTraceBlockIndex(out []byte, dir []traceBlockDirEntry) []byte {
	out = binary.LittleEndian.AppendUint32(out, uint32(len(dir))) //nolint:gosec // bounded by block count
	for i := range dir {
		d := &dir[i]
		out = binary.LittleEndian.AppendUint64(out, d.compOff)
		out = binary.LittleEndian.AppendUint64(out, d.compLen)
		out = append(out, d.minTraceID[:]...)
		out = append(out, d.maxTraceID[:]...)
		out = binary.LittleEndian.AppendUint64(out, d.minTimeSec)
		out = binary.LittleEndian.AppendUint64(out, d.maxTimeSec)
	}
	return out
}

// DecodeTraceGroups decodes a full TraceGroup index payload into every group it
// holds. It dispatches on format: a v2 batched file (magic "VTG2") is decoded
// block by block; a legacy flat-blob file (snappy stream leading with
// ValueIndexTraceVersion) is decoded via decodeLegacyTraceGroups so old files
// written before the v2 rollover (issue #476) still read (NOTE-VI-047 migration
// discipline). This whole-file decode is used by the compactor's merge path
// (which must read every group) and by tests; the trace-by-id read path uses the
// partial, block-pruning helpers in traceindexquery.go instead.
func DecodeTraceGroups(data []byte) ([]TraceGroup, error) {
	if isTraceV2(data) {
		return decodeTraceV2(data)
	}
	return decodeLegacyTraceGroups(data)
}

// isTraceV2 reports whether data is a v2 batched TraceGroup file: long enough to
// hold a header magic + footer, with both magics matching TraceFileMagic.
func isTraceV2(data []byte) bool {
	if len(data) < 5+traceFooterSize {
		return false
	}
	if binary.LittleEndian.Uint32(data[:4]) != TraceFileMagic {
		return false
	}
	footer := data[len(data)-traceFooterSize:]
	return binary.LittleEndian.Uint32(footer[:4]) == TraceFileMagic
}

// decodeTraceV2 parses the full v2 batched wire format into every group.
func decodeTraceV2(data []byte) ([]TraceGroup, error) {
	ft, err := DecodeTraceFooter(data)
	if err != nil {
		return nil, err
	}
	dataLen := uint64(len(data))
	if ft.StringTableOff > dataLen || ft.StringTableLen > dataLen ||
		ft.StringTableOff+ft.StringTableLen > dataLen ||
		ft.BlockIndexOff > dataLen || ft.BlockIndexLen > dataLen ||
		ft.BlockIndexOff+ft.BlockIndexLen > dataLen {
		return nil, fmt.Errorf("valueindex: trace index footer offsets out of bounds")
	}

	table, _, err := DecodeStringTable(data[ft.StringTableOff : ft.StringTableOff+ft.StringTableLen])
	if err != nil {
		return nil, fmt.Errorf("valueindex: trace index string table: %w", err)
	}
	dir, err := decodeTraceBlockIndex(data[ft.BlockIndexOff : ft.BlockIndexOff+ft.BlockIndexLen])
	if err != nil {
		return nil, err
	}

	var groups []TraceGroup
	for i := range dir {
		d := &dir[i]
		blk, derr := decodeTraceBlockAt(data, d, table)
		if derr != nil {
			return nil, fmt.Errorf("valueindex: trace index block %d: %w", i, derr)
		}
		groups = append(groups, blk...)
	}
	return groups, nil
}

// decodeTraceBlockAt decodes a single block's groups from the file bytes using
// its directory entry (compressed offset/len) and the file's string table.
func decodeTraceBlockAt(data []byte, d *traceBlockDirEntry, table *StringTable) ([]TraceGroup, error) {
	end := d.compOff + d.compLen
	if d.compOff > uint64(len(data)) || d.compLen > uint64(len(data)) || end > uint64(len(data)) {
		return nil, fmt.Errorf("block body out of bounds")
	}
	raw, err := snappy.Decode(nil, data[d.compOff:end])
	if err != nil {
		return nil, fmt.Errorf("snappy decode: %w", err)
	}
	return decodeTraceBlockBody(raw, table)
}

// decodeTraceBlockBody parses one uncompressed block body into its groups.
func decodeTraceBlockBody(raw []byte, table *StringTable) ([]TraceGroup, error) {
	pos := 0
	// min_trace_id[16] max_trace_id[16] min_time[8] max_time[8]
	if pos+16+16+8+8 > len(raw) {
		return nil, fmt.Errorf("block header truncated")
	}
	pos += 16 + 16 + 8 + 8 // block metadata is redundant with the directory; skip.

	if pos+4 > len(raw) {
		return nil, fmt.Errorf("block bloom_len truncated")
	}
	bloomLen := int(binary.LittleEndian.Uint32(raw[pos:]))
	pos += 4
	if bloomLen < 0 || pos+bloomLen > len(raw) {
		return nil, fmt.Errorf("block bloom truncated")
	}
	pos += bloomLen // bloom is a pruning aid only; not needed for a full decode.

	if pos+4 > len(raw) {
		return nil, fmt.Errorf("block group_count truncated")
	}
	groupCount := int(binary.LittleEndian.Uint32(raw[pos:]))
	pos += 4
	if groupCount < 0 || pos+groupCount*minGroupWireSize > len(raw) {
		return nil, fmt.Errorf(
			"block group count %d implausible for remaining %d bytes",
			groupCount, len(raw)-pos,
		)
	}

	groups := make([]TraceGroup, 0, groupCount)
	for gi := range groupCount {
		g, npos, err := decodeTraceGroupAt(raw, pos, table)
		if err != nil {
			return nil, fmt.Errorf("group %d: %w", gi, err)
		}
		pos = npos
		groups = append(groups, g)
	}
	return groups, nil
}

// decodeTraceGroupAt decodes one TraceGroup starting at pos in raw, returning
// the group and the new position. Shared by the v2 block decoder and the legacy
// flat-blob decoder so both paths bounds-check identically.
func decodeTraceGroupAt(raw []byte, pos int, table *StringTable) (TraceGroup, int, error) {
	if pos+8+16+4 > len(raw) {
		return TraceGroup{}, pos, fmt.Errorf("group header truncated")
	}
	var g TraceGroup
	g.TimeSec = binary.LittleEndian.Uint64(raw[pos : pos+8])
	pos += 8
	copy(g.TraceID[:], raw[pos:pos+16])
	pos += 16
	spanCount := int(binary.LittleEndian.Uint32(raw[pos : pos+4]))
	pos += 4
	if spanCount < 0 || pos+spanCount*spanEntryWireSize > len(raw) {
		return TraceGroup{}, pos, fmt.Errorf("spans truncated")
	}
	g.Spans = make([]SpanEntry, spanCount)
	for si := range spanCount {
		var s SpanEntry
		copy(s.SpanID[:], raw[pos:pos+8])
		pos += 8
		copy(s.ParentSpanID[:], raw[pos:pos+8])
		pos += 8
		idx := binary.LittleEndian.Uint16(raw[pos : pos+2])
		pos += 2
		s.SourceRef = table.Lookup(idx)
		s.BlockRef = DecodeBlockRef(raw, pos)
		pos += BlockRefSize
		s.RowIdx = binary.LittleEndian.Uint16(raw[pos : pos+2])
		pos += 2
		g.Spans[si] = s
	}
	return g, pos, nil
}

// decodeTraceBlockIndex parses the v2 block directory (TOC).
func decodeTraceBlockIndex(data []byte) ([]traceBlockDirEntry, error) {
	if len(data) < 4 {
		return nil, fmt.Errorf("valueindex: trace index block index too short")
	}
	count := int(binary.LittleEndian.Uint32(data[:4]))
	pos := 4
	if count < 0 || pos+count*traceBlockDirEntrySize > len(data) {
		return nil, fmt.Errorf("valueindex: trace index block index count %d implausible", count)
	}
	dir := make([]traceBlockDirEntry, 0, count)
	for range count {
		var d traceBlockDirEntry
		d.compOff = binary.LittleEndian.Uint64(data[pos:])
		d.compLen = binary.LittleEndian.Uint64(data[pos+8:])
		copy(d.minTraceID[:], data[pos+16:pos+32])
		copy(d.maxTraceID[:], data[pos+32:pos+48])
		d.minTimeSec = binary.LittleEndian.Uint64(data[pos+48:])
		d.maxTimeSec = binary.LittleEndian.Uint64(data[pos+56:])
		pos += traceBlockDirEntrySize
		dir = append(dir, d)
	}
	return dir, nil
}

// decodeLegacyTraceGroups decodes a v1 flat-blob TraceGroup payload (a single
// snappy stream leading with ValueIndexTraceVersion). Retained for the v2
// rollover window so files written before issue #476 still read.
func decodeLegacyTraceGroups(compressed []byte) ([]TraceGroup, error) {
	raw, err := snappy.Decode(nil, compressed)
	if err != nil {
		return nil, fmt.Errorf("valueindex: trace index snappy decode: %w", err)
	}
	if len(raw) < 1 {
		return nil, fmt.Errorf("valueindex: trace index payload empty")
	}
	ver := raw[0]
	if ver != shared.ValueIndexTraceVersion {
		return nil, fmt.Errorf("valueindex: trace index unsupported version %d", ver)
	}
	pos := 1

	table, consumed, err := DecodeStringTable(raw[pos:])
	if err != nil {
		return nil, fmt.Errorf("valueindex: trace index string table: %w", err)
	}
	pos += consumed

	if pos+4 > len(raw) {
		return nil, fmt.Errorf("valueindex: trace index truncated group count")
	}
	groupCount := int(binary.LittleEndian.Uint32(raw[pos : pos+4]))
	pos += 4

	if groupCount < 0 || pos+groupCount*minGroupWireSize > len(raw) {
		return nil, fmt.Errorf(
			"valueindex: trace index group count %d implausible for remaining %d bytes",
			groupCount, len(raw)-pos,
		)
	}

	groups := make([]TraceGroup, 0, groupCount)
	for gi := range groupCount {
		g, npos, gerr := decodeTraceGroupAt(raw, pos, table)
		if gerr != nil {
			return nil, fmt.Errorf("valueindex: trace index group %d: %w", gi, gerr)
		}
		pos = npos
		groups = append(groups, g)
	}
	return groups, nil
}

// sortTraceGroups sorts groups by (TraceID ASC, TimeSec ASC) and each group's
// spans by SpanID ASC for a deterministic layout. TraceID leads the sort because
// it is the trace-by-id point-lookup key: ordering by TraceID makes each block's
// [minTraceID, maxTraceID] range a tight, seekable bound (issue #476).
func sortTraceGroups(groups []TraceGroup) {
	sort.Slice(groups, func(i, j int) bool {
		if groups[i].TraceID != groups[j].TraceID {
			return less16(groups[i].TraceID, groups[j].TraceID)
		}
		return groups[i].TimeSec < groups[j].TimeSec
	})
	for i := range groups {
		sortSpanEntries(groups[i].Spans)
	}
}

func sortSpanEntries(spans []SpanEntry) {
	sort.Slice(spans, func(i, j int) bool {
		return less8(spans[i].SpanID, spans[j].SpanID)
	})
}

func less16(a, b [16]byte) bool {
	return bytes.Compare(a[:], b[:]) < 0
}

func less8(a, b [8]byte) bool {
	return bytes.Compare(a[:], b[:]) < 0
}

// MergeTraceGroups merges TraceGroups from multiple input files into a deduplicated
// output, suitable for compaction (issue #428 acceptance):
//
//   - Groups for the same TraceID are merged into one TraceGroup.
//   - (TraceID, SpanID) pairs are deduplicated; the first occurrence wins.
//   - A span whose SourceRef is no longer live (per checker) is dropped.
//   - A group whose every span is dropped is removed entirely.
//   - The merged group's TimeSec is the minimum TimeSec across its inputs
//     (the earliest bucket the trace was seen in).
//
// A nil checker skips the retention drop (keeps all spans). checker matches
// the RefChecker shape every other compaction code path in this package
// already uses (e.g. StreamCompactBucketFiles), rather than a bespoke
// func(string) bool, so callers thread the same context-aware, cacheable
// checker through both formats.
//
// Output is sorted by (TraceID ASC, TimeSec ASC) — TraceID leads because it is
// the trace-by-id point-lookup key (issue #476, NOTE-VI-075).
func MergeTraceGroups(
	ctx context.Context,
	checker RefChecker,
	inputs ...[]TraceGroup,
) ([]TraceGroup, error) {
	type merged struct {
		seenSpan map[[8]byte]struct{}
		spans    []SpanEntry
		timeSec  uint64
	}
	byTrace := make(map[[16]byte]*merged)
	// Preserve first-seen trace order for determinism before the final sort.
	for _, in := range inputs {
		for gi := range in {
			g := &in[gi]
			m := byTrace[g.TraceID]
			if m == nil {
				m = &merged{timeSec: g.TimeSec, seenSpan: make(map[[8]byte]struct{})}
				byTrace[g.TraceID] = m
			} else if g.TimeSec < m.timeSec {
				m.timeSec = g.TimeSec
			}
			for si := range g.Spans {
				s := g.Spans[si]
				if checker != nil {
					live, err := checker.IsLive(ctx, s.SourceRef)
					if err != nil {
						return nil, fmt.Errorf(
							"valueindex: MergeTraceGroups: RefChecker.IsLive(%q): %w",
							s.SourceRef,
							err,
						)
					}
					if !live {
						continue
					}
				}
				if _, dup := m.seenSpan[s.SpanID]; dup {
					continue
				}
				m.seenSpan[s.SpanID] = struct{}{}
				m.spans = append(m.spans, s)
			}
		}
	}

	out := make([]TraceGroup, 0, len(byTrace))
	for tid, m := range byTrace {
		if len(m.spans) == 0 {
			continue // every span dropped by retention
		}
		out = append(out, TraceGroup{
			TimeSec: m.timeSec,
			TraceID: tid,
			Spans:   m.spans,
		})
	}
	sortTraceGroups(out)
	return out, nil
}

// SpanNode is one node in an assembled trace tree.
type SpanNode struct {
	Children []*SpanNode
	Span     SpanEntry
}

// AssembledTrace is the result of AssembleTrace.
type AssembledTrace struct {
	// Roots are the top-level spans. A span is a root if it has no parent, or
	// if its ParentSpanID references a span not present in the group (an orphan
	// attached at root level — see Partial).
	Roots []*SpanNode
	// TraceID identifies the trace.
	TraceID [16]byte
	// Partial is true if any span referenced a ParentSpanID that does not exist
	// in the group (the root or an intermediate span arrived in a different,
	// not-yet-compacted L0 file, or was dropped by retention).
	Partial bool
}

// AssembleTrace builds the span tree for a TraceGroup. Spans with a present
// parent are attached as children; orphans (missing parent) are attached at the
// root level and the result is flagged Partial. Genuine root spans (zero
// ParentSpanID) are always roots and do not set Partial.
//
// The tree is deterministic: children are ordered by SpanID ASC and roots by
// SpanID ASC.
func AssembleTrace(g TraceGroup) AssembledTrace {
	nodes := make(map[[8]byte]*SpanNode, len(g.Spans))
	for i := range g.Spans {
		s := g.Spans[i]
		nodes[s.SpanID] = &SpanNode{Span: s}
	}

	result := AssembledTrace{TraceID: g.TraceID}
	for i := range g.Spans {
		s := g.Spans[i]
		node := nodes[s.SpanID]
		if s.IsRoot() {
			result.Roots = append(result.Roots, node)
			continue
		}
		parent, ok := nodes[s.ParentSpanID]
		if !ok {
			// Orphan: parent not in this group. Attach at root, flag partial.
			result.Partial = true
			result.Roots = append(result.Roots, node)
			continue
		}
		parent.Children = append(parent.Children, node)
	}

	sortNodes(result.Roots)
	return result
}

// sortNodes recursively orders nodes and their children by SpanID ASC.
func sortNodes(nodes []*SpanNode) {
	sort.Slice(nodes, func(i, j int) bool {
		return less8(nodes[i].Span.SpanID, nodes[j].Span.SpanID)
	})
	for _, n := range nodes {
		if len(n.Children) > 0 {
			sortNodes(n.Children)
		}
	}
}
