package shared

import (
	"encoding/binary"
	"fmt"
)

// BlockRef is an in-memory reference to a single row within an inner block:
// which block (BlockIdx) and which row inside it (RowIdx). It is used by the
// value-index/intrinsic posting lists and is NOT a serialized file-position
// reference. For the v2 page-aligned on-disk block locator see BlockFileRef.
type BlockRef struct {
	BlockIdx uint16
	RowIdx   uint16
}

// BlockFileRef is the v2 file-format locator for an inner block (NOTE-V2-001).
// The v2 lean format (#417) drops the IntrinsicTOC and resolves blocks directly
// from value-index hits; a value-index entry therefore needs to point at the
// block's byte range in the file. To keep that pointer compact (5 bytes vs a
// full uint64 offset+uint32 length = 12 bytes) we store it in 4 KB page units:
// inner blocks are padded to a 4096-byte boundary (#419) so a page index plus a
// page count fully describes the block's byte range and supports a direct
// ranged S3 GET without any TOC fetch.
//
// Wire layout (BlockFileRefWireSize = 5 bytes, little-endian):
//
//	bytes 0..2  Page   as uint24 (max 16,777,215 pages = 64 GB file)
//	bytes 3..4  Length as uint16 (max 65,535 pages = 256 MB block)
//
// The name intentionally differs from BlockRef above (an in-memory row locator)
// to avoid conflating the two distinct concepts.
type BlockFileRef struct {
	Page   uint32 // uint24 on the wire: page index of the block start (page = 4096 bytes)
	Length uint16 // block length in 4 KB pages (padded; #419)
}

// BlockFileRefPageSize is the page granularity for BlockFileRef in bytes.
const BlockFileRefPageSize = 4096

// BlockFileRefWireSize is the on-wire size of an encoded BlockFileRef:
// 3 bytes for Page (LE uint24) + 2 bytes for Length (LE uint16).
const BlockFileRefWireSize = 5

// blockFileRefMaxPage is the largest representable Page (uint24 ceiling).
const blockFileRefMaxPage = 0xFFFFFF

// Bytes returns the block's byte offset within the file: Page * 4096.
func (r BlockFileRef) Bytes() uint64 {
	return uint64(r.Page) * BlockFileRefPageSize
}

// LengthBytes returns the block's padded byte length: Length * 4096.
func (r BlockFileRef) LengthBytes() uint32 {
	return uint32(r.Length) * BlockFileRefPageSize
}

// EncodeBlockFileRef writes r into dst in BlockFileRefWireSize little-endian
// bytes. dst must be at least BlockFileRefWireSize long. It returns an error if
// Page exceeds the uint24 maximum (overflow would silently truncate).
func EncodeBlockFileRef(dst []byte, r BlockFileRef) error {
	if len(dst) < BlockFileRefWireSize {
		return fmt.Errorf("blockfileref: dst too small: have %d want %d", len(dst), BlockFileRefWireSize)
	}
	if r.Page > blockFileRefMaxPage {
		return fmt.Errorf("blockfileref: page %d exceeds uint24 max %d", r.Page, blockFileRefMaxPage)
	}
	// uint24 little-endian: low byte first. Page is bounded above by the uint24
	// max check, so the masked conversions cannot lose information.
	dst[0] = byte(r.Page & 0xFF)
	dst[1] = byte((r.Page >> 8) & 0xFF)
	dst[2] = byte((r.Page >> 16) & 0xFF)
	binary.LittleEndian.PutUint16(dst[3:5], r.Length)
	return nil
}

// DecodeBlockFileRef reads a BlockFileRef from the first BlockFileRefWireSize
// bytes of src. It returns an error if src is too short.
func DecodeBlockFileRef(src []byte) (BlockFileRef, error) {
	if len(src) < BlockFileRefWireSize {
		return BlockFileRef{}, fmt.Errorf(
			"blockfileref: src too small: have %d want %d",
			len(src),
			BlockFileRefWireSize,
		)
	}
	page := uint32(src[0]) | uint32(src[1])<<8 | uint32(src[2])<<16
	length := binary.LittleEndian.Uint16(src[3:5])
	return BlockFileRef{Page: page, Length: length}, nil
}
