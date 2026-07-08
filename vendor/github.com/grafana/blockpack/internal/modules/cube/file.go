package cube

// NOTE: SPEC-CUBE-003, SPEC-CUBE-004, SPEC-CUBE-005 — File header (36B) and footer (40B).
// Magic 0x43554245 ("CUBE" ASCII), version 0x02, all fields LittleEndian.

import (
	"encoding/binary"
	"fmt"
)

// Cube file format constants.
const (
	MagicCube = uint32(0x43554245) // "CUBE" in ASCII
	// VersionCube 1->2 (E-3, #491): buf[5] repurposed from reserved to NumAggAttrs; a v1 file
	// is rejected by DecodeHeader with a typed error rather than silently misread (SPEC-ROOT-013
	// "typed error for old-format files, no dual reader" — no task in this phase adds a v1 decode
	// path).
	VersionCube = uint8(2)
	HeaderSize  = 36
	FooterSize  = 40
)

// Header is the fixed leading 36 bytes of a cube file.
type Header struct {
	Magic       uint32   // 0x43554245
	Version     uint8    // 0x02
	NumAggAttrs uint8    // count of per-aggAttr 540-byte records per cell (0 = pure-count format)
	CubeID      [16]byte // ULID or hash of cube definition
	MinMinute   uint32   // earliest cell minute in file
	MaxMinute   uint32   // latest cell minute in file
	Resolution  uint32   // 1=L0, 60=L1, 1440=L2
}

// EncodeHeader serializes a Header to 36 bytes.
func EncodeHeader(h Header) []byte {
	buf := make([]byte, HeaderSize)
	binary.LittleEndian.PutUint32(buf[0:4], h.Magic)
	buf[4] = h.Version
	buf[5] = h.NumAggAttrs
	// buf[6:8] = reserved (zero)
	copy(buf[8:24], h.CubeID[:])
	binary.LittleEndian.PutUint32(buf[24:28], h.MinMinute)
	binary.LittleEndian.PutUint32(buf[28:32], h.MaxMinute)
	binary.LittleEndian.PutUint32(buf[32:36], h.Resolution)
	return buf
}

// DecodeHeader parses a 36-byte buffer into a Header. Rejects any file whose Version does not
// match VersionCube — closes a latent gap where DecodeHeader previously performed no version
// validation at all (NOTE-CUBE-013), which would otherwise misread a v1 file's reserved byte as a
// garbage NumAggAttrs count.
func DecodeHeader(buf []byte) (Header, error) {
	if len(buf) < HeaderSize {
		return Header{}, fmt.Errorf("cube: header buffer too short (%d bytes)", len(buf))
	}
	magic := binary.LittleEndian.Uint32(buf[0:4])
	if magic != MagicCube {
		return Header{}, fmt.Errorf("cube: invalid magic 0x%08X (expected 0x%08X)", magic, MagicCube)
	}
	version := buf[4]
	if version != VersionCube {
		return Header{}, fmt.Errorf(
			"cube: unsupported file version %d (expected %d) — no dual reader, re-write the file",
			version,
			VersionCube,
		)
	}
	h := Header{
		Magic:       magic,
		Version:     version,
		NumAggAttrs: buf[5],
		MinMinute:   binary.LittleEndian.Uint32(buf[24:28]),
		MaxMinute:   binary.LittleEndian.Uint32(buf[28:32]),
		Resolution:  binary.LittleEndian.Uint32(buf[32:36]),
	}
	copy(h.CubeID[:], buf[8:24])
	return h, nil
}

// Footer is the trailing 40 bytes of a cube file.
type Footer struct {
	Magic        uint32 // 0x43554245
	Version      uint8  // 0x01
	CellCount    uint64 // total cells across all chunks
	DictOffset   uint64 // byte offset of dictionary section
	ChunksOffset uint64 // byte offset of cells section start
	DirOffset    uint64 // byte offset of chunk directory
}

// EncodeFooter serializes a Footer to 40 bytes.
func EncodeFooter(f Footer) []byte {
	buf := make([]byte, FooterSize)
	binary.LittleEndian.PutUint32(buf[0:4], f.Magic)
	buf[4] = f.Version
	// buf[5:8] = reserved (zero)
	binary.LittleEndian.PutUint64(buf[8:16], f.CellCount)
	binary.LittleEndian.PutUint64(buf[16:24], f.DictOffset)
	binary.LittleEndian.PutUint64(buf[24:32], f.ChunksOffset)
	binary.LittleEndian.PutUint64(buf[32:40], f.DirOffset)
	return buf
}

// DecodeFooter parses a 40-byte buffer into a Footer.
func DecodeFooter(buf []byte) (Footer, error) {
	if len(buf) < FooterSize {
		return Footer{}, fmt.Errorf("cube: footer buffer too short (%d bytes)", len(buf))
	}
	magic := binary.LittleEndian.Uint32(buf[0:4])
	if magic != MagicCube {
		return Footer{}, fmt.Errorf("cube: invalid footer magic 0x%08X", magic)
	}
	return Footer{
		Magic:        magic,
		Version:      buf[4],
		CellCount:    binary.LittleEndian.Uint64(buf[8:16]),
		DictOffset:   binary.LittleEndian.Uint64(buf[16:24]),
		ChunksOffset: binary.LittleEndian.Uint64(buf[24:32]),
		DirOffset:    binary.LittleEndian.Uint64(buf[32:40]),
	}, nil
}
