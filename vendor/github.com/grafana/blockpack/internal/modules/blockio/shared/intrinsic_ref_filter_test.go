package shared

// NOTE: Any changes to this file must be reflected in the corresponding specs.md or NOTES.md.

import (
	"encoding/binary"
	"sync"
	"testing"

	"github.com/golang/snappy"
)

// buildFlatPageBlob builds a raw (uncompressed) flat page containing uint64 values and
// their refs, then snappy-compresses it. Returns compressed blob, minVal, maxVal.
// Used only by tests that need to manually assemble multi-page blobs.
func buildFlatPageBlob(vals []uint64, refs []BlockRef) ([]byte, string, string) {
	varintBuf := make([]byte, 0, len(vals)*2) // varint estimates 1-2 bytes per uint64 delta
	var varintTmp [10]byte
	var prev uint64
	for _, v := range vals {
		n := binary.PutUvarint(varintTmp[:], v-prev)
		varintBuf = append(varintBuf, varintTmp[:n]...)
		prev = v
	}

	raw := make([]byte, 0, 4+len(varintBuf)+len(refs)*4)
	var tmp4 [4]byte
	binary.LittleEndian.PutUint32(tmp4[:], uint32(len(varintBuf))) //nolint:gosec
	raw = append(raw, tmp4[:]...)
	raw = append(raw, varintBuf...)

	var tmp2 [2]byte
	for _, ref := range refs {
		binary.LittleEndian.PutUint16(tmp2[:], ref.BlockIdx)
		raw = append(raw, tmp2[:]...)
		binary.LittleEndian.PutUint16(tmp2[:], ref.RowIdx)
		raw = append(raw, tmp2[:]...)
	}

	var minStr, maxStr string
	if len(vals) > 0 {
		var tmp8 [8]byte
		binary.LittleEndian.PutUint64(tmp8[:], vals[0])
		minStr = string(tmp8[:])
		binary.LittleEndian.PutUint64(tmp8[:], vals[len(vals)-1])
		maxStr = string(tmp8[:])
	}
	return snappy.Encode(nil, raw), minStr, maxStr
}

// buildMultiPageFlatBlob assembles a v2 paged blob from three page slices.
// Each page has its own vals/refs. blockW and rowW are both 2 (uint16) for simplicity.
func buildMultiPageFlatBlob(pages []struct {
	vals []uint64
	refs []BlockRef
},
) ([]byte, error) {
	pageBlobs := make([][]byte, len(pages))
	pageMetas := make([]PageMeta, len(pages))
	var offset uint32
	for i, p := range pages {
		blob, minVal, maxVal := buildFlatPageBlob(p.vals, p.refs)
		pageMetas[i] = PageMeta{
			Offset:   offset,
			Length:   uint32(len(blob)),   //nolint:gosec
			RowCount: uint32(len(p.refs)), //nolint:gosec
			Min:      minVal,
			Max:      maxVal,
		}
		pageBlobs[i] = blob
		offset += uint32(len(blob)) //nolint:gosec
	}

	toc := PagedIntrinsicTOC{
		Pages:         pageMetas,
		BlockIdxWidth: 2, // uint16
		RowIdxWidth:   2, // uint16
		Format:        IntrinsicFormatFlat,
		ColType:       ColumnTypeUint64,
	}
	tocBlob, err := EncodePageTOC(toc)
	if err != nil {
		return nil, err
	}

	// Assemble: sentinel[1] + toc_len[4 LE] + toc_blob + page_blobs...
	total := 1 + 4 + len(tocBlob)
	for _, pb := range pageBlobs {
		total += len(pb)
	}
	out := make([]byte, 0, total)
	out = append(out, IntrinsicPagedVersion)
	var tmp4 [4]byte
	binary.LittleEndian.PutUint32(tmp4[:], uint32(len(tocBlob))) //nolint:gosec
	out = append(out, tmp4[:]...)
	out = append(out, tocBlob...)
	for _, pb := range pageBlobs {
		out = append(out, pb...)
	}
	return out, nil
}

// TestPageMetaFields verifies that PageMeta stores core fields correctly.
func TestPageMetaFields(t *testing.T) {
	bloom := make([]byte, 16)
	bloom[0] = 0xFF
	pm := PageMeta{
		Offset:   100,
		Length:   200,
		RowCount: 50,
		Min:      "minVal",
		Max:      "maxVal",
		Bloom:    bloom,
	}
	if pm.Offset != 100 {
		t.Errorf("Offset = %d, want 100", pm.Offset)
	}
	if pm.Length != 200 {
		t.Errorf("Length = %d, want 200", pm.Length)
	}
	if len(pm.Bloom) != 16 || pm.Bloom[0] != 0xFF {
		t.Errorf("Bloom = %v, want [0xFF, 0, ...]", pm.Bloom)
	}
}

// TestEncodeDecodeTOC_V1_RoundTrip verifies that EncodePageTOC writes version 0x01 and
// that DecodePageTOC recovers page metadata correctly (no ref-range fields since NOTE-007).
func TestEncodeDecodeTOC_V1_RoundTrip(t *testing.T) {
	bloom0 := make([]byte, 16)
	bloom0[3] = 0xAB

	toc := PagedIntrinsicTOC{
		Pages: []PageMeta{
			{
				Offset:   0,
				Length:   100,
				RowCount: 50,
				Min:      "minA",
				Max:      "maxA",
				Bloom:    bloom0,
			},
			{
				Offset:   100,
				Length:   200,
				RowCount: 100,
				Min:      "minB",
				Max:      "maxB",
				Bloom:    []byte{0x01, 0x02},
			},
		},
		BlockIdxWidth: 2,
		RowIdxWidth:   2,
		Format:        IntrinsicFormatFlat,
		ColType:       ColumnTypeUint64,
	}

	encoded, err := EncodePageTOC(toc)
	if err != nil {
		t.Fatalf("EncodePageTOC: %v", err)
	}

	decoded, err := DecodePageTOC(encoded)
	if err != nil {
		t.Fatalf("DecodePageTOC: %v", err)
	}

	if len(decoded.Pages) != 2 {
		t.Fatalf("Pages count = %d, want 2", len(decoded.Pages))
	}

	p0 := decoded.Pages[0]
	if p0.Min != "minA" {
		t.Errorf("Pages[0].Min = %q, want 'minA'", p0.Min)
	}
	if p0.Max != "maxA" {
		t.Errorf("Pages[0].Max = %q, want 'maxA'", p0.Max)
	}
	if len(p0.Bloom) != 16 || p0.Bloom[3] != 0xAB {
		t.Errorf("Pages[0].Bloom mismatch: %v", p0.Bloom)
	}

	p1 := decoded.Pages[1]
	if p1.Min != "minB" {
		t.Errorf("Pages[1].Min = %q, want 'minB'", p1.Min)
	}
	if len(p1.Bloom) != 2 {
		t.Errorf("Pages[1].Bloom len = %d, want 2", len(p1.Bloom))
	}
}

// TestEncodeDecodeTOC_V1_ManualBlob verifies that a manually-built v0x01 TOC blob
// is decoded correctly (no ref-range fields present since NOTE-007).
func TestEncodeDecodeTOC_V1_ManualBlob(t *testing.T) {
	// Build a v1 TOC manually.
	// Format: version[1]=0x01, page_count[4], block_idx_width[1], row_idx_width[1],
	//         format[1], col_type[1], per page: offset[4], length[4], row_count[4],
	//         min_len[2]+min, max_len[2]+max, bloom_len[2]+bloom.
	// version[1] + page_count[4] + blockW[1] + rowW[1] + format[1] + col_type[1] +
	// offset[4] + length[4] + row_count[4] + min_len[2] + max_len[2] + bloom_len[2]
	raw := make([]byte, 0, 1+4+2+4+4+4+2+2+2)
	raw = append(raw, 0x01) // version = 0x01 (old)
	var tmp4 [4]byte
	var tmp2 [2]byte
	binary.LittleEndian.PutUint32(tmp4[:], 1) // page_count = 1
	raw = append(raw, tmp4[:]...)
	raw = append(raw, 2, 2) // blockW=2, rowW=2
	raw = append(raw, IntrinsicFormatFlat)
	raw = append(raw, byte(ColumnTypeUint64))
	// Page 0:
	binary.LittleEndian.PutUint32(tmp4[:], 0) // offset
	raw = append(raw, tmp4[:]...)
	binary.LittleEndian.PutUint32(tmp4[:], 42) // length
	raw = append(raw, tmp4[:]...)
	binary.LittleEndian.PutUint32(tmp4[:], 10) // row_count
	raw = append(raw, tmp4[:]...)
	// min: ""
	binary.LittleEndian.PutUint16(tmp2[:], 0)
	raw = append(raw, tmp2[:]...)
	// max: ""
	binary.LittleEndian.PutUint16(tmp2[:], 0)
	raw = append(raw, tmp2[:]...)
	// bloom: empty
	binary.LittleEndian.PutUint16(tmp2[:], 0)
	raw = append(raw, tmp2[:]...)

	blob := snappy.Encode(nil, raw)
	decoded, err := DecodePageTOC(blob)
	if err != nil {
		t.Fatalf("DecodePageTOC: %v", err)
	}
	if len(decoded.Pages) != 1 {
		t.Fatalf("Pages count = %d, want 1", len(decoded.Pages))
	}
	p := decoded.Pages[0]
	if p.Length != 42 {
		t.Errorf("Length = %d, want 42", p.Length)
	}
	if p.RowCount != 10 {
		t.Errorf("RowCount = %d, want 10", p.RowCount)
	}
}

// TestDecodePagedColumnBlobFiltered_FlatColumn_DecodesAll verifies that all pages are
// decoded regardless of refFilter (page-skipping removed in NOTE-007).
func TestDecodePagedColumnBlobFiltered_FlatColumn_DecodesAll(t *testing.T) {
	// Build 3 pages, each with distinct refs.
	// Page 0: refs with BlockIdx=0, RowIdx=0..4
	// Page 1: refs with BlockIdx=1, RowIdx=0..4
	// Page 2: refs with BlockIdx=2, RowIdx=0..4
	page0 := struct {
		vals []uint64
		refs []BlockRef
	}{
		vals: []uint64{100, 200, 300, 400, 500},
		refs: []BlockRef{
			{BlockIdx: 0, RowIdx: 0},
			{BlockIdx: 0, RowIdx: 1},
			{BlockIdx: 0, RowIdx: 2},
			{BlockIdx: 0, RowIdx: 3},
			{BlockIdx: 0, RowIdx: 4},
		},
	}
	page1 := struct {
		vals []uint64
		refs []BlockRef
	}{
		vals: []uint64{600, 700, 800, 900, 1000},
		refs: []BlockRef{
			{BlockIdx: 1, RowIdx: 0},
			{BlockIdx: 1, RowIdx: 1},
			{BlockIdx: 1, RowIdx: 2},
			{BlockIdx: 1, RowIdx: 3},
			{BlockIdx: 1, RowIdx: 4},
		},
	}
	page2 := struct {
		vals []uint64
		refs []BlockRef
	}{
		vals: []uint64{1100, 1200, 1300, 1400, 1500},
		refs: []BlockRef{
			{BlockIdx: 2, RowIdx: 0},
			{BlockIdx: 2, RowIdx: 1},
			{BlockIdx: 2, RowIdx: 2},
			{BlockIdx: 2, RowIdx: 3},
			{BlockIdx: 2, RowIdx: 4},
		},
	}

	blob, err := buildMultiPageFlatBlob([]struct {
		vals []uint64
		refs []BlockRef
	}{page0, page1, page2})
	if err != nil {
		t.Fatalf("buildMultiPageFlatBlob: %v", err)
	}

	// Filter: only page 1's refs.
	refFilter := map[uint32]struct{}{
		uint32(1)<<16 | uint32(0): {},
		uint32(1)<<16 | uint32(2): {},
	}

	col, err := DecodePagedColumnBlobFiltered(blob, refFilter)
	if err != nil {
		t.Fatalf("DecodePagedColumnBlobFiltered: %v", err)
	}
	if col == nil {
		t.Fatal("result is nil, want non-nil column")
	}

	// All 15 rows from all 3 pages are returned (no page skipping since NOTE-007).
	if col.Count != 15 {
		t.Errorf("Count = %d, want 15 (all pages decoded)", col.Count)
	}
}

// TestDecodePagedColumnBlobFiltered_DictColumn_DecodesAll verifies all pages are decoded
// for dict columns regardless of refFilter (page-skipping removed in NOTE-007).
func TestDecodePagedColumnBlobFiltered_DictColumn_DecodesAll(t *testing.T) {
	// Build a 2-page dict blob manually.
	// Page 0: value "alpha" → refs with BlockIdx=0
	// Page 1: value "beta" → refs with BlockIdx=1

	// Page 0 blob (dict format).
	page0Raw := buildDictPageRaw([]dictTestEntry{
		{value: "alpha", refs: []BlockRef{{BlockIdx: 0, RowIdx: 0}, {BlockIdx: 0, RowIdx: 1}}},
	})
	page0Blob := snappy.Encode(nil, page0Raw)

	// Page 1 blob (dict format).
	page1Raw := buildDictPageRaw([]dictTestEntry{
		{value: "beta", refs: []BlockRef{{BlockIdx: 1, RowIdx: 0}, {BlockIdx: 1, RowIdx: 1}}},
	})
	page1Blob := snappy.Encode(nil, page1Raw)

	toc := PagedIntrinsicTOC{
		Pages: []PageMeta{
			{
				Offset:   0,
				Length:   uint32(len(page0Blob)), //nolint:gosec
				RowCount: 2,
			},
			{
				Offset:   uint32(len(page0Blob)), //nolint:gosec
				Length:   uint32(len(page1Blob)), //nolint:gosec
				RowCount: 2,
			},
		},
		BlockIdxWidth: 2,
		RowIdxWidth:   2,
		Format:        IntrinsicFormatDict,
		ColType:       ColumnTypeString,
	}
	tocBlob, err := EncodePageTOC(toc)
	if err != nil {
		t.Fatalf("EncodePageTOC: %v", err)
	}

	blob := assemblePaged(tocBlob, [][]byte{page0Blob, page1Blob})

	// Filter: only page 1 ref — but all pages are still decoded since NOTE-007.
	refFilter := map[uint32]struct{}{
		uint32(1)<<16 | uint32(0): {},
	}

	col, err := DecodePagedColumnBlobFiltered(blob, refFilter)
	if err != nil {
		t.Fatalf("DecodePagedColumnBlobFiltered: %v", err)
	}
	if col == nil {
		t.Fatal("result is nil")
	}

	// Both pages are decoded: "alpha" and "beta" entries should both be present.
	values := make(map[string]bool)
	for _, e := range col.DictEntries {
		values[e.Value] = true
	}
	if !values["alpha"] || !values["beta"] {
		t.Errorf("DictEntries = %v, want both 'alpha' and 'beta' (all pages decoded)", col.DictEntries)
	}
}

// TestDecodePagedColumnBlobFiltered_NilFilter_DecodesAll verifies that nil refFilter
// returns all rows (equivalent to full decode).
func TestDecodePagedColumnBlobFiltered_NilFilter_DecodesAll(t *testing.T) {
	page0 := struct {
		vals []uint64
		refs []BlockRef
	}{
		vals: []uint64{10, 20},
		refs: []BlockRef{{BlockIdx: 0, RowIdx: 0}, {BlockIdx: 0, RowIdx: 1}},
	}
	page1 := struct {
		vals []uint64
		refs []BlockRef
	}{
		vals: []uint64{30, 40},
		refs: []BlockRef{{BlockIdx: 1, RowIdx: 0}, {BlockIdx: 1, RowIdx: 1}},
	}

	blob, err := buildMultiPageFlatBlob([]struct {
		vals []uint64
		refs []BlockRef
	}{page0, page1})
	if err != nil {
		t.Fatalf("buildMultiPageFlatBlob: %v", err)
	}

	col, err := DecodePagedColumnBlobFiltered(blob, nil)
	if err != nil {
		t.Fatalf("DecodePagedColumnBlobFiltered(nil): %v", err)
	}
	if col == nil {
		t.Fatal("result is nil")
	}
	if col.Count != 4 {
		t.Errorf("Count = %d, want 4 (all rows)", col.Count)
	}
}

// TestDecodePagedColumnBlobFiltered_NonNilFilter_DecodesAll verifies that a non-nil refFilter
// still returns all rows (page-skipping removed in NOTE-007; refFilter retained for API compat).
func TestDecodePagedColumnBlobFiltered_NonNilFilter_DecodesAll(t *testing.T) {
	page0 := struct {
		vals []uint64
		refs []BlockRef
	}{
		vals: []uint64{10},
		refs: []BlockRef{{BlockIdx: 0, RowIdx: 0}},
	}

	blob, err := buildMultiPageFlatBlob([]struct {
		vals []uint64
		refs []BlockRef
	}{page0})
	if err != nil {
		t.Fatalf("buildMultiPageFlatBlob: %v", err)
	}

	// Filter ref does not exist in the blob — but all pages are still decoded.
	refFilter := map[uint32]struct{}{
		uint32(99)<<16 | uint32(99): {},
	}

	col, err := DecodePagedColumnBlobFiltered(blob, refFilter)
	if err != nil {
		t.Fatalf("DecodePagedColumnBlobFiltered: %v", err)
	}
	// All rows are returned regardless of refFilter (page-skipping removed in NOTE-007).
	if col == nil {
		t.Fatal("result is nil, want non-nil column")
	}
	if col.Count != 1 {
		t.Errorf("Count = %d, want 1 (all pages decoded)", col.Count)
	}
}

// TestLookupRef_FlatColumn_Found verifies that LookupRef finds a matching uint64 value.
func TestLookupRef_FlatColumn_Found(t *testing.T) {
	col := &IntrinsicColumn{
		Format:       IntrinsicFormatFlat,
		Type:         ColumnTypeUint64,
		Uint64Values: []uint64{100, 200, 300},
		BlockRefs: []BlockRef{
			{BlockIdx: 0, RowIdx: 5},
			{BlockIdx: 1, RowIdx: 3},
			{BlockIdx: 2, RowIdx: 7},
		},
		Count: 3,
	}

	target := uint32(1)<<16 | uint32(3) // BlockIdx=1, RowIdx=3
	val, found := col.LookupRef(target)
	if !found {
		t.Fatal("LookupRef: expected found=true, got false")
	}
	v, ok := val.(uint64)
	if !ok || v != 200 {
		t.Errorf("LookupRef: val = %v, want uint64(200)", val)
	}
}

// TestLookupRef_FlatColumn_NotFound verifies that LookupRef returns false for absent ref.
func TestLookupRef_FlatColumn_NotFound(t *testing.T) {
	col := &IntrinsicColumn{
		Format:       IntrinsicFormatFlat,
		Type:         ColumnTypeUint64,
		Uint64Values: []uint64{100},
		BlockRefs:    []BlockRef{{BlockIdx: 0, RowIdx: 0}},
		Count:        1,
	}

	target := uint32(99)<<16 | uint32(99)
	_, found := col.LookupRef(target)
	if found {
		t.Fatal("LookupRef: expected found=false, got true")
	}
}

// TestLookupRef_DictColumn_Found verifies LookupRef for a dict string column.
func TestLookupRef_DictColumn_Found(t *testing.T) {
	col := &IntrinsicColumn{
		Format: IntrinsicFormatDict,
		Type:   ColumnTypeString,
		DictEntries: []IntrinsicDictEntry{
			{
				Value:     "hello",
				BlockRefs: []BlockRef{{BlockIdx: 0, RowIdx: 1}, {BlockIdx: 0, RowIdx: 2}},
			},
			{
				Value:     "world",
				BlockRefs: []BlockRef{{BlockIdx: 1, RowIdx: 0}},
			},
		},
		Count: 3,
	}

	target := uint32(1)<<16 | uint32(0) // BlockIdx=1, RowIdx=0 → "world"
	val, found := col.LookupRef(target)
	if !found {
		t.Fatal("LookupRef dict: expected found=true, got false")
	}
	s, ok := val.(string)
	if !ok || s != "world" {
		t.Errorf("LookupRef dict: val = %v, want string(\"world\")", val)
	}
}

// TestLookupRef_DictColumn_NotFound verifies LookupRef returns false for absent dict ref.
func TestLookupRef_DictColumn_NotFound(t *testing.T) {
	col := &IntrinsicColumn{
		Format: IntrinsicFormatDict,
		Type:   ColumnTypeString,
		DictEntries: []IntrinsicDictEntry{
			{Value: "hello", BlockRefs: []BlockRef{{BlockIdx: 0, RowIdx: 0}}},
		},
		Count: 1,
	}

	target := uint32(99)<<16 | uint32(99)
	_, found := col.LookupRef(target)
	if found {
		t.Fatal("LookupRef dict: expected found=false, got true")
	}
}

// TestLookupRef_EmptyColumn verifies LookupRef on zero-row column.
func TestLookupRef_EmptyColumn(t *testing.T) {
	col := &IntrinsicColumn{
		Format: IntrinsicFormatFlat,
		Type:   ColumnTypeUint64,
		Count:  0,
	}
	_, found := col.LookupRef(0x00010001)
	if found {
		t.Fatal("LookupRef on empty column: expected found=false, got true")
	}
}

// TestDecodeIntrinsicColumnBlob_V1_Error verifies that a legacy v1 blob returns an error.
func TestDecodeIntrinsicColumnBlob_V1_Error(t *testing.T) {
	// Build a minimal v1 blob: format_version=0x01, format=0x01, col_type=0x02, row_count=0.
	raw := make([]byte, 0, 9)                 // format_version[1]+format[1]+col_type[1]+row_count[4]+blockW[1]+rowW[1]
	raw = append(raw, 0x01)                   // format_version
	raw = append(raw, 0x01)                   // format = flat
	raw = append(raw, 0x02)                   // col_type = ColumnTypeUint64
	raw = append(raw, 0x00, 0x00, 0x00, 0x00) // row_count = 0
	raw = append(raw, 0x01, 0x01)             // blockW=1, rowW=1

	blob := snappy.Encode(nil, raw)

	_, err := DecodeIntrinsicColumnBlob(blob)
	if err == nil {
		t.Fatal("expected error for v1 blob, got nil")
	}
	// Error should mention unsupported format.
	if err.Error() == "" {
		t.Fatal("error message is empty")
	}
}

// --- Helpers for dict test blob building ---

type dictTestEntry struct {
	value string
	refs  []BlockRef
}

// buildDictPageRaw builds an uncompressed dict page blob with uint16 ref widths.
func buildDictPageRaw(entries []dictTestEntry) []byte {
	var raw []byte
	var tmp4 [4]byte
	var tmp2 [2]byte
	binary.LittleEndian.PutUint32(tmp4[:], uint32(len(entries))) //nolint:gosec
	raw = append(raw, tmp4[:]...)
	for _, e := range entries {
		binary.LittleEndian.PutUint16(tmp2[:], uint16(len(e.value))) //nolint:gosec
		raw = append(raw, tmp2[:]...)
		raw = append(raw, []byte(e.value)...)
		binary.LittleEndian.PutUint32(tmp4[:], uint32(len(e.refs))) //nolint:gosec
		raw = append(raw, tmp4[:]...)
		for _, ref := range e.refs {
			binary.LittleEndian.PutUint16(tmp2[:], ref.BlockIdx)
			raw = append(raw, tmp2[:]...)
			binary.LittleEndian.PutUint16(tmp2[:], ref.RowIdx)
			raw = append(raw, tmp2[:]...)
		}
	}
	return raw
}

// assemblePaged assembles a v2 paged blob from a TOC blob and page blobs.
func assemblePaged(tocBlob []byte, pageBlobs [][]byte) []byte {
	total := 1 + 4 + len(tocBlob)
	for _, pb := range pageBlobs {
		total += len(pb)
	}
	out := make([]byte, 0, total)
	out = append(out, IntrinsicPagedVersion)
	var tmp4 [4]byte
	binary.LittleEndian.PutUint32(tmp4[:], uint32(len(tocBlob))) //nolint:gosec
	out = append(out, tmp4[:]...)
	out = append(out, tocBlob...)
	for _, pb := range pageBlobs {
		out = append(out, pb...)
	}
	return out
}

// --- EnsureRefIndex / LookupRefFast tests ---

// TestEnsureRefIndex_Flat builds a flat IntrinsicColumn with known uint64 values and
// refs, calls EnsureRefIndex, then verifies LookupRefFast returns correct values for
// known refs and (nil, false) for unknown refs.
func TestEnsureRefIndex_Flat(t *testing.T) {
	col := &IntrinsicColumn{
		Format: IntrinsicFormatFlat,
		Type:   ColumnTypeUint64,
		Uint64Values: []uint64{
			1000,
			2000,
			3000,
		},
		BlockRefs: []BlockRef{
			{BlockIdx: 0, RowIdx: 5},
			{BlockIdx: 1, RowIdx: 10},
			{BlockIdx: 2, RowIdx: 15},
		},
		Count: 3,
	}

	col.EnsureRefIndex()

	tests := []struct {
		name      string
		blockIdx  uint16
		rowIdx    uint16
		wantVal   uint64
		wantFound bool
	}{
		{"first ref", 0, 5, 1000, true},
		{"middle ref", 1, 10, 2000, true},
		{"last ref", 2, 15, 3000, true},
		{"unknown ref", 9, 9, 0, false},
		{"wrong row", 0, 6, 0, false},
		{"wrong block", 3, 5, 0, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			packed := uint32(tt.blockIdx)<<16 | uint32(tt.rowIdx)
			val, found := col.LookupRefFast(packed)
			if found != tt.wantFound {
				t.Fatalf("LookupRefFast: found=%v, want %v", found, tt.wantFound)
			}
			if tt.wantFound {
				v, ok := val.(uint64)
				if !ok {
					t.Fatalf("val type = %T, want uint64", val)
				}
				if v != tt.wantVal {
					t.Errorf("val = %d, want %d", v, tt.wantVal)
				}
			}
		})
	}
}

// TestEnsureRefIndex_Dict builds a dict IntrinsicColumn with known string entries,
// calls EnsureRefIndex, then verifies LookupRefFast returns correct dict values.
func TestEnsureRefIndex_Dict(t *testing.T) {
	col := &IntrinsicColumn{
		Format: IntrinsicFormatDict,
		Type:   ColumnTypeString,
		DictEntries: []IntrinsicDictEntry{
			{
				Value:     "alpha",
				BlockRefs: []BlockRef{{BlockIdx: 0, RowIdx: 1}, {BlockIdx: 0, RowIdx: 2}},
			},
			{
				Value:     "beta",
				BlockRefs: []BlockRef{{BlockIdx: 1, RowIdx: 0}},
			},
			{
				Value:     "gamma",
				BlockRefs: []BlockRef{{BlockIdx: 2, RowIdx: 3}, {BlockIdx: 2, RowIdx: 4}},
			},
		},
		Count: 5,
	}

	col.EnsureRefIndex()

	tests := []struct {
		name      string
		blockIdx  uint16
		rowIdx    uint16
		wantVal   string
		wantFound bool
	}{
		{"alpha ref 0", 0, 1, "alpha", true},
		{"alpha ref 1", 0, 2, "alpha", true},
		{"beta ref", 1, 0, "beta", true},
		{"gamma ref 0", 2, 3, "gamma", true},
		{"gamma ref 1", 2, 4, "gamma", true},
		{"unknown", 9, 9, "", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			packed := uint32(tt.blockIdx)<<16 | uint32(tt.rowIdx)
			val, found := col.LookupRefFast(packed)
			if found != tt.wantFound {
				t.Fatalf("LookupRefFast: found=%v, want %v", found, tt.wantFound)
			}
			if tt.wantFound {
				s, ok := val.(string)
				if !ok {
					t.Fatalf("val type = %T, want string", val)
				}
				if s != tt.wantVal {
					t.Errorf("val = %q, want %q", s, tt.wantVal)
				}
			}
		})
	}
}

// TestEnsureRefIndex_Nil verifies that EnsureRefIndex on a nil column is a no-op
// and LookupRefFast on nil returns (nil, false).
func TestEnsureRefIndex_Nil(t *testing.T) {
	var col *IntrinsicColumn
	col.EnsureRefIndex() // must not panic

	val, found := col.LookupRefFast(0x00010001)
	if found {
		t.Errorf("LookupRefFast on nil: found=true, want false")
	}
	if val != nil {
		t.Errorf("LookupRefFast on nil: val=%v, want nil", val)
	}
}

// TestEnsureRefIndex_Concurrent calls EnsureRefIndex from 10 goroutines simultaneously
// on the same column and verifies no panic and correct results after all goroutines finish.
func TestEnsureRefIndex_Concurrent(t *testing.T) {
	col := &IntrinsicColumn{
		Format:       IntrinsicFormatFlat,
		Type:         ColumnTypeUint64,
		Uint64Values: []uint64{100, 200, 300},
		BlockRefs: []BlockRef{
			{BlockIdx: 0, RowIdx: 0},
			{BlockIdx: 0, RowIdx: 1},
			{BlockIdx: 0, RowIdx: 2},
		},
		Count: 3,
	}

	const goroutines = 10
	var wg sync.WaitGroup
	wg.Add(goroutines)

	for range goroutines {
		go func() {
			defer wg.Done()
			col.EnsureRefIndex()
		}()
	}
	wg.Wait()

	// After all goroutines complete, index must be valid and lookups must succeed.
	packed := uint32(0)<<16 | uint32(1)
	val, found := col.LookupRefFast(packed)
	if !found {
		t.Fatal("LookupRefFast after concurrent EnsureRefIndex: not found")
	}
	v, ok := val.(uint64)
	if !ok || v != 200 {
		t.Errorf("LookupRefFast: val=%v, want uint64(200)", val)
	}
}
