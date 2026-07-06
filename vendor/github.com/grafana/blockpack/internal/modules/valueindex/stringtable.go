package valueindex

// stringtable.go — string table for SourceRef deduplication (issue #432, NOTE-VI-028).
//
// A value-index file may contain thousands of entries that all share the same
// SourceRef (blockpack file path). Storing the path inline with every entry wastes
// ~50 bytes per entry. The string table deduplicates these strings and stores them
// once; each entry carries a uint16 index instead.
//
// Wire format (embedded in the VINX section, v3 format):
//
//	str_count[2 LE]  — number of strings in the table (max 65535)
//	for each string:
//	    str_len[2 LE]
//	    str_bytes[str_len]
//
// StringTable is zero-indexed: index 0 → first string, index 1 → second, etc.

import (
	"encoding/binary"
	"errors"
	"fmt"
)

// MaxStringTableEntries is the maximum number of distinct strings a single
// value-index file's string table can hold. SourceRefs are addressed by a
// uint16 index, so a file may reference at most 65535 distinct source files.
// Compaction splits its output when an output file would exceed this
// (NOTE-VI-028, issue #432).
const MaxStringTableEntries = 0xFFFF

// ErrStringTableOverflow is returned by the v4 encoder when more than
// MaxStringTableEntries distinct SourceRefs are interned into a single file.
// Callers (compaction) must split the output so each file stays within the
// uint16 SourceRef index space (NOTE-VI-028, issue #432).
var ErrStringTableOverflow = errors.New("valueindex: string table exceeds 65535 distinct source refs")

// StringTable maps uint16 indexes to string values. Used to deduplicate
// SourceRef strings in v3 value-index files (NOTE-VI-028, issue #432).
type StringTable struct {
	indexes map[string]uint16 // reverse map for encoding
	strs    []string
}

// NewStringTable creates an empty string table.
func NewStringTable() *StringTable {
	return &StringTable{
		indexes: make(map[string]uint16),
	}
}

// Intern returns the uint16 index for s, adding it to the table if not present.
// Returns (0, false) if the table is full (>= 65535 entries).
func (t *StringTable) Intern(s string) (uint16, bool) {
	if idx, ok := t.indexes[s]; ok {
		return idx, true
	}
	if len(t.strs) >= MaxStringTableEntries {
		return 0, false
	}
	idx := uint16(len(t.strs)) //nolint:gosec // bounded above
	t.strs = append(t.strs, s)
	t.indexes[s] = idx
	return idx, true
}

// Lookup returns the string at index idx, or "" if out of range.
func (t *StringTable) Lookup(idx uint16) string {
	if int(idx) >= len(t.strs) {
		return ""
	}
	return t.strs[idx]
}

// Len returns the number of strings in the table.
func (t *StringTable) Len() int { return len(t.strs) }

// EncodedSize returns the exact number of bytes EncodeStringTable would produce for t,
// without allocating the encoded buffer. Used by StreamCompactBucketFiles' output-size split
// heuristic (NOTE-VI-077) to project the eventual on-disk file size at a block boundary
// without serializing the tail on every check.
func (t *StringTable) EncodedSize() int {
	if t == nil || len(t.strs) == 0 {
		return 2 // empty table: str_count[2]
	}
	size := 2 // str_count[2]
	for _, s := range t.strs {
		size += 2 + len(s) // str_len[2] + str_bytes[N]
	}
	return size
}

// EncodeStringTable serializes the string table into the wire format.
// Returns nil if the table is empty.
func EncodeStringTable(t *StringTable) []byte {
	if t == nil || len(t.strs) == 0 {
		// Empty table: encode as 0 count, 2 bytes.
		return []byte{0, 0}
	}
	// Calculate total size.
	size := 2 // str_count[2]
	for _, s := range t.strs {
		size += 2 + len(s) // str_len[2] + str_bytes[N]
	}
	buf := make([]byte, 0, size)
	buf = binary.LittleEndian.AppendUint16(buf, uint16(len(t.strs))) //nolint:gosec
	for _, s := range t.strs {
		buf = binary.LittleEndian.AppendUint16(buf, uint16(len(s))) //nolint:gosec
		buf = append(buf, s...)
	}
	return buf
}

// DecodeStringTable deserializes a string table from wire bytes.
// Returns the decoded table and the number of bytes consumed, or an error.
func DecodeStringTable(data []byte) (*StringTable, int, error) {
	if len(data) < 2 {
		return nil, 0, fmt.Errorf("valueindex: string table too short (%d bytes)", len(data))
	}
	count := int(binary.LittleEndian.Uint16(data[0:2]))
	pos := 2
	t := &StringTable{
		strs:    make([]string, 0, count),
		indexes: make(map[string]uint16, count),
	}
	for i := range count {
		if pos+2 > len(data) {
			return nil, 0, fmt.Errorf("valueindex: string table entry %d: truncated", i)
		}
		strLen := int(binary.LittleEndian.Uint16(data[pos : pos+2]))
		pos += 2
		if pos+strLen > len(data) {
			return nil, 0, fmt.Errorf("valueindex: string table entry %d: value truncated", i)
		}
		s := string(data[pos : pos+strLen])
		pos += strLen
		idx := uint16(len(t.strs)) //nolint:gosec
		t.strs = append(t.strs, s)
		t.indexes[s] = idx
	}
	return t, pos, nil
}
