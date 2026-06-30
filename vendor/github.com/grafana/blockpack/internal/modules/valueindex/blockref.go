package valueindex

// NOTE: Any changes to this file must be reflected in the corresponding NOTES.md.

// PageSize is the page granularity (bytes) for page-addressed block references.
//
// A v2 blockpack aligns every inner block to a 4 096-byte page boundary, so a
// block's byte position is always an exact multiple of PageSize. Storing the
// position as a page number (uint24) and the length in pages (uint16) keeps the
// wire size to 5 bytes — one byte more than the legacy BlockID uint32 — while
// enabling a direct S3 ranged GET from a single value-index hit without a prior
// TOC fetch:
//
//	Range: bytes=PageNum*PageSize, length=LenPages*PageSize
//
// Limits:
//   - Max file offset: 2^24 * 4096 = 64 GiB
//   - Max block size:  2^16 * 4096 = 256 MiB
const PageSize = 4096

// BlockRef is a direct page-addressed reference to one inner block in a v2
// blockpack file (NOTE-VI-027, issue #417). It encodes a (page number, page
// count) pair.
type BlockRef struct {
	// PageNum is the block's start offset divided by PageSize (uint24 on the wire).
	PageNum uint32
	// LenPages is the block's byte length rounded up to PageSize, divided by
	// PageSize (uint16 on the wire).
	LenPages uint16
}

// ByteOffset returns the block's absolute byte offset in the file.
func (r BlockRef) ByteOffset() int64 { return int64(r.PageNum) * PageSize }

// ByteLen returns the padded block length in bytes (LenPages * PageSize).
func (r BlockRef) ByteLen() int64 { return int64(r.LenPages) * PageSize }

// BlockRefSize is the wire byte size of an encoded BlockRef.
const BlockRefSize = 3 + 2 // PageNum[3LE] + LenPages[2LE]

// AppendBlockRef encodes r into buf as 5 bytes (3-byte LE page num + 2-byte LE
// page count) and returns the extended slice.
func AppendBlockRef(buf []byte, r BlockRef) []byte {
	// Each byte() is an intentional little-endian mask (the encoding), not a lossy overflow.
	buf = append(
		buf,
		byte(r.PageNum),     //nolint:gosec // LE byte 0
		byte(r.PageNum>>8),  //nolint:gosec // LE byte 1
		byte(r.PageNum>>16), //nolint:gosec // LE byte 2 (uint24)
	)
	buf = append(
		buf,
		byte(r.LenPages),    //nolint:gosec // LE byte 0
		byte(r.LenPages>>8), //nolint:gosec // LE byte 1
	)
	return buf
}

// DecodeBlockRef reads 5 bytes from b starting at off and returns the BlockRef.
// The caller must ensure len(b[off:]) >= BlockRefSize.
func DecodeBlockRef(b []byte, off int) BlockRef {
	p := uint32(b[off]) | uint32(b[off+1])<<8 | uint32(b[off+2])<<16
	l := uint16(b[off+3]) | uint16(b[off+4])<<8
	return BlockRef{PageNum: p, LenPages: l}
}

// BlockRefFromByteRange constructs a BlockRef from a raw byte offset and byte
// length. byteOffset must be a multiple of PageSize; byteLen is rounded up to
// the next PageSize multiple automatically.
//
// Returns ok=false if byteOffset is not page-aligned, if byteOffset / PageSize
// overflows uint24, or if the rounded-up length overflows uint16.
func BlockRefFromByteRange(byteOffset, byteLen int64) (BlockRef, bool) {
	if byteOffset%PageSize != 0 {
		return BlockRef{}, false
	}
	pageNum := byteOffset / PageSize
	if pageNum > 0xFF_FFFF { // uint24 max
		return BlockRef{}, false
	}
	// Round byte length up to next page boundary.
	pages := (byteLen + PageSize - 1) / PageSize
	if pages > 0xFFFF { // uint16 max
		return BlockRef{}, false
	}
	return BlockRef{PageNum: uint32(pageNum), LenPages: uint16(pages)}, true //nolint:gosec
}
