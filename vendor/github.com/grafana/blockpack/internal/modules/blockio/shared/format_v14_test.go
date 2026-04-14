package shared_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/grafana/blockpack/internal/modules/blockio/shared"
)

// TestV14FormatConstants verifies that all V14 format constants have the expected values.
// These are pinned wire-format constants — changing them breaks file compatibility.
func TestV14FormatConstants(t *testing.T) {
	assert.Equal(t, uint8(14), shared.VersionBlockV14, "VersionBlockV14 must be 14")
	assert.Equal(t, uint8(3), shared.VersionBlockEncV3, "VersionBlockEncV3 must be 3")
	assert.Equal(t, uint16(7), shared.FooterV7Version, "FooterV7Version must be 7")
	assert.Equal(t, uint(18), shared.FooterV7Size, "FooterV7Size must be 18 bytes")

	// All 6 section type constants must be distinct and nonzero.
	sections := []uint8{
		shared.SectionBlockIndex,
		shared.SectionRangeIndex,
		shared.SectionTraceIndex,
		shared.SectionTSIndex,
		shared.SectionSketchIndex,
		shared.SectionFileBloom,
	}

	for _, s := range sections {
		assert.NotZero(t, s, "section type constant must be nonzero")
	}

	// All section type constants must be distinct.
	seen := make(map[uint8]bool)
	for _, s := range sections {
		assert.False(t, seen[s], "section type constant 0x%02x appears more than once", s)
		seen[s] = true
	}

	// Verify specific expected values match the plan.
	assert.Equal(t, uint8(0x01), shared.SectionBlockIndex)
	assert.Equal(t, uint8(0x02), shared.SectionRangeIndex)
	assert.Equal(t, uint8(0x03), shared.SectionTraceIndex)
	assert.Equal(t, uint8(0x04), shared.SectionTSIndex)
	assert.Equal(t, uint8(0x05), shared.SectionSketchIndex)
	assert.Equal(t, uint8(0x06), shared.SectionFileBloom)

	// DirEntryKind constants must be distinct.
	assert.Equal(t, uint8(0x00), shared.DirEntryKindType)
	assert.Equal(t, uint8(0x01), shared.DirEntryKindName)
	assert.NotEqual(t, shared.DirEntryKindType, shared.DirEntryKindName)
}

// TestDirEntryTypeWireSize verifies that DirEntryType serializes to exactly 14 bytes.
// Wire format: entry_kind[1]=0x00 + section_type[1] + offset[8] + compressed_len[4] = 14 bytes.
func TestDirEntryTypeWireSize(t *testing.T) {
	const wantWireSize = 1 + 1 + 8 + 4 // entry_kind[1]+section_type[1]+offset[8]+compressed_len[4]
	assert.Equal(t, 14, wantWireSize, "DirEntryType wire size must be exactly 14 bytes per plan spec")
	assert.Equal(t, 14, shared.DirEntryTypeWireSize)

	entry := shared.DirEntryType{
		SectionType:   shared.SectionBlockIndex,
		Offset:        0x0102030405060708,
		CompressedLen: 0x01020304,
	}
	encoded := entry.Marshal()
	require.Equal(t, wantWireSize, len(encoded),
		"DirEntryType.Marshal() must produce exactly 14 bytes")

	// First byte must be DirEntryKindType.
	assert.Equal(t, shared.DirEntryKindType, encoded[0])
	// Second byte must be the section type.
	assert.Equal(t, shared.SectionBlockIndex, encoded[1])

	// Round-trip decode (UnmarshalDirEntryType takes data starting after the kind byte).
	decoded, err := shared.UnmarshalDirEntryType(encoded[1:])
	require.NoError(t, err)
	assert.Equal(t, entry.SectionType, decoded.SectionType)
	assert.Equal(t, entry.Offset, decoded.Offset)
	assert.Equal(t, entry.CompressedLen, decoded.CompressedLen)
}

// TestDirEntryNameWireSize verifies DirEntryName wire format and round-trip.
// Wire format: entry_kind[1]=0x01 + name_len[2] + name + offset[8] + compressed_len[4] = 15+len(name) bytes.
func TestDirEntryNameWireSize(t *testing.T) {
	name := "span:name"
	entry := shared.DirEntryName{
		Name:          name,
		Offset:        0xDEADBEEFCAFEBABE,
		CompressedLen: 0x00001234,
	}

	wantSize := 1 + 2 + len(name) + 8 + 4 // entry_kind[1]+name_len[2]+name+offset[8]+compressed_len[4]
	assert.Equal(t, wantSize, entry.WireSize())

	encoded := entry.Marshal()
	require.Equal(t, wantSize, len(encoded), "DirEntryName.Marshal() must match WireSize()")

	// First byte must be DirEntryKindName.
	assert.Equal(t, shared.DirEntryKindName, encoded[0])

	// Round-trip decode (UnmarshalDirEntryName takes data starting after the kind byte).
	decoded, n, err := shared.UnmarshalDirEntryName(encoded[1:])
	require.NoError(t, err)
	assert.Equal(t, wantSize-1, n, "UnmarshalDirEntryName must report correct bytes consumed")
	assert.Equal(t, entry.Name, decoded.Name)
	assert.Equal(t, entry.Offset, decoded.Offset)
	assert.Equal(t, entry.CompressedLen, decoded.CompressedLen)
}
