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

// EncodeTraceGroups encodes a slice of TraceGroups into a snappy-compressed
// payload with a leading string table. Groups are sorted by (TimeSec ASC,
// TraceID ASC) and each group's spans are sorted by SpanID ASC to give a
// deterministic, dedup-friendly wire layout.
//
// Wire (before snappy):
//
//	version[1]
//	string_table (EncodeStringTable)
//	group_count[4 LE]
//	for each group:
//	    time_sec[8 LE]
//	    trace_id[16]
//	    span_count[4 LE]
//	    for each span:
//	        span_id[8] parent_span_id[8] source_ref_idx[2] block_ref[5] row_idx[2 LE]
//
// Returns ErrStringTableOverflow if the groups reference more than
// MaxStringTableEntries distinct SourceRefs (caller must split — see compaction).
func EncodeTraceGroups(groups []TraceGroup) ([]byte, error) {
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

	tableBytes := EncodeStringTable(table)

	// version[1] + table + group_count[4]
	size := 1 + len(tableBytes) + 4
	for i := range sorted {
		size += 8 + 16 + 4 + len(sorted[i].Spans)*spanEntryWireSize
	}
	buf := make([]byte, 0, size)
	buf = append(buf, shared.ValueIndexTraceVersion)
	buf = append(buf, tableBytes...)
	buf = binary.LittleEndian.AppendUint32(buf, uint32(len(sorted))) //nolint:gosec

	for i := range sorted {
		g := &sorted[i]
		buf = binary.LittleEndian.AppendUint64(buf, g.TimeSec)
		buf = append(buf, g.TraceID[:]...)
		buf = binary.LittleEndian.AppendUint32(buf, uint32(len(g.Spans))) //nolint:gosec
		for j := range g.Spans {
			s := &g.Spans[j]
			idx, _ := table.Intern(s.SourceRef) // already interned above
			buf = append(buf, s.SpanID[:]...)
			buf = append(buf, s.ParentSpanID[:]...)
			buf = binary.LittleEndian.AppendUint16(buf, idx)
			buf = AppendBlockRef(buf, s.BlockRef)
			buf = binary.LittleEndian.AppendUint16(buf, s.RowIdx)
		}
	}

	return snappy.Encode(nil, buf), nil
}

// DecodeTraceGroups decodes a payload produced by EncodeTraceGroups.
func DecodeTraceGroups(compressed []byte) ([]TraceGroup, error) {
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

	groups := make([]TraceGroup, 0, groupCount)
	for gi := range groupCount {
		if pos+8+16+4 > len(raw) {
			return nil, fmt.Errorf("valueindex: trace index group %d header truncated", gi)
		}
		var g TraceGroup
		g.TimeSec = binary.LittleEndian.Uint64(raw[pos : pos+8])
		pos += 8
		copy(g.TraceID[:], raw[pos:pos+16])
		pos += 16
		spanCount := int(binary.LittleEndian.Uint32(raw[pos : pos+4]))
		pos += 4

		if pos+spanCount*spanEntryWireSize > len(raw) {
			return nil, fmt.Errorf("valueindex: trace index group %d spans truncated", gi)
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
		groups = append(groups, g)
	}
	return groups, nil
}

// sortTraceGroups sorts groups by (TimeSec ASC, TraceID ASC) and each group's
// spans by SpanID ASC for a deterministic layout.
func sortTraceGroups(groups []TraceGroup) {
	sort.Slice(groups, func(i, j int) bool {
		if groups[i].TimeSec != groups[j].TimeSec {
			return groups[i].TimeSec < groups[j].TimeSec
		}
		return less16(groups[i].TraceID, groups[j].TraceID)
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

// SourceExists reports whether a data blockpack SourceRef still exists. During
// compaction the compactor supplies this (one HEAD per unique SourceRef, cached)
// so spans whose source was deleted by retention are dropped rather than
// propagated. A nil checker keeps every span (no retention check).
type SourceExists func(sourceRef string) bool

// MergeTraceGroups merges TraceGroups from multiple input files into a deduplicated
// output, suitable for compaction (issue #428 acceptance):
//
//   - Groups for the same TraceID are merged into one TraceGroup.
//   - (TraceID, SpanID) pairs are deduplicated; the first occurrence wins.
//   - A span whose SourceRef no longer exists (per exists) is dropped.
//   - A group whose every span is dropped is removed entirely.
//   - The merged group's TimeSec is the minimum TimeSec across its inputs
//     (the earliest bucket the trace was seen in).
//
// A nil exists checker skips the retention drop (keeps all spans).
// Output is sorted by (TimeSec ASC, TraceID ASC).
func MergeTraceGroups(exists SourceExists, inputs ...[]TraceGroup) []TraceGroup {
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
				if exists != nil && !exists(s.SourceRef) {
					continue
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
	return out
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
