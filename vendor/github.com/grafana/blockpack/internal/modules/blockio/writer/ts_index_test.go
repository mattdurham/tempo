package writer

import (
	"encoding/binary"
	"testing"

	"github.com/grafana/blockpack/internal/modules/blockio/shared"
	"github.com/stretchr/testify/require"
)

func TestWriteTSIndexSection_Empty(t *testing.T) {
	t.Parallel()
	got := writeTSIndexSection(nil)
	// 4 (magic) + 1 (version) + 4 (count=0) = 9 bytes
	require.Len(t, got, 9)
	require.Equal(t, shared.TSIndexMagic, binary.LittleEndian.Uint32(got[0:]))
	require.Equal(t, shared.TSIndexVersion, got[4])
	require.Equal(t, uint32(0), binary.LittleEndian.Uint32(got[5:]))
}

func TestWriteTSIndexSection_SortedByMinTS(t *testing.T) {
	t.Parallel()
	metas := []shared.BlockMeta{
		{MinStart: 300, MaxStart: 400}, // block 0
		{MinStart: 100, MaxStart: 200}, // block 1
		{MinStart: 500, MaxStart: 600}, // block 2
	}
	got := writeTSIndexSection(metas)
	// 9 header bytes + 3 * 20 = 69 bytes
	require.Len(t, got, 69)
	require.Equal(t, uint32(3), binary.LittleEndian.Uint32(got[5:]))

	// Entry 0 should be block 1 (minTS=100, smallest).
	e0 := got[9:]
	require.Equal(t, uint64(100), binary.LittleEndian.Uint64(e0[0:]))
	require.Equal(t, uint64(200), binary.LittleEndian.Uint64(e0[8:]))
	require.Equal(t, uint32(1), binary.LittleEndian.Uint32(e0[16:]))

	// Entry 1 should be block 0 (minTS=300).
	e1 := got[29:]
	require.Equal(t, uint64(300), binary.LittleEndian.Uint64(e1[0:]))
	require.Equal(t, uint32(0), binary.LittleEndian.Uint32(e1[16:]))

	// Entry 2 should be block 2 (minTS=500).
	e2 := got[49:]
	require.Equal(t, uint64(500), binary.LittleEndian.Uint64(e2[0:]))
	require.Equal(t, uint32(2), binary.LittleEndian.Uint32(e2[16:]))
}
