package shared

// NOTE: Any changes to this file must be reflected in the corresponding specs.md or NOTES.md.

// NOTE-258: unzeroed backing-array allocation for full-overwrite, pointer-free decode slices.
//
// decodePagedColumnBlob pre-sizes the merged column's backing arrays to the exact total row
// count (Uint64Values / BlockRefs), then the page-decode loop writes EVERY slot before the
// column is observed. The make() that allocates these arrays nonetheless memclr-zeroes the
// whole span (runtime.memclrNoHeapPointersChunked was the #2 runtime self-time frame on the
// M1/M4 unfiltered-rate path, ~2.4% of querier CPU) — pure waste, since the zeros are
// immediately overwritten by the decode.
//
// makeNoZeroUint64 / makeNoZeroBlockRef allocate via runtime.mallocgc with needzero=false,
// skipping that clear. This is sound ONLY because:
//   - Both element types are pointer-free (uint64; BlockRef is {uint16,uint16}), so the GC
//     never scans the contents — uninitialised bytes can never be misread as a heap pointer.
//   - The caller guarantees a full overwrite of [0:n) before any read (decodePagedColumnBlob
//     reslices to [:totalRows] and every append helper fills exactly its RowCount region;
//     the union covers [0:totalRows) exactly).
// The []byte-values slice ([][]byte) is intentionally NOT covered here: its elements contain
// pointers and a partial/garbage fill would be unsafe for the GC to scan.

import "unsafe"

//go:linkname mallocgc runtime.mallocgc
func mallocgc(size uintptr, typ unsafe.Pointer, needzero bool) unsafe.Pointer

// makeNoZeroUint64 returns a []uint64 of length and capacity n whose backing array is NOT
// zeroed. The caller MUST write all n elements before reading any. Returns nil for n <= 0.
func makeNoZeroUint64(n int) []uint64 {
	if n <= 0 {
		return nil
	}
	p := mallocgc(uintptr(n)*unsafe.Sizeof(uint64(0)), nil, false)
	//nolint:gosec // G103: unzeroed pointer-free alloc, fully overwritten before read (NOTE-258)
	return unsafe.Slice((*uint64)(p), n)
}

// makeNoZeroBlockRef returns a []BlockRef of length and capacity n whose backing array is NOT
// zeroed. The caller MUST write all n elements before reading any. Returns nil for n <= 0.
// BlockRef is pointer-free ({uint16,uint16}), so the unscanned garbage is GC-safe.
func makeNoZeroBlockRef(n int) []BlockRef {
	if n <= 0 {
		return nil
	}
	p := mallocgc(uintptr(n)*unsafe.Sizeof(BlockRef{}), nil, false)
	//nolint:gosec // G103: unzeroed pointer-free alloc, fully overwritten before read (NOTE-258)
	return unsafe.Slice((*BlockRef)(p), n)
}
