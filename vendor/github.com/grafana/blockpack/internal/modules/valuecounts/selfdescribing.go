package valuecounts

// NOTE: Any changes to this file must be reflected in the corresponding NOTES.md.
//
// selfdescribing.go — self-describing VCNT file format (NOTE-VC-005). EncodeRecords
// returns its chunk directory as a separate value; vcntwriter.go's write path today discards
// it, leaving every VCNT object undecodable by anything except a single-chunk reconstruction
// that only happens to work because no object has ever exceeded one chunk. EncodeVCNTFile
// embeds that directory directly in the file (a trailing footer, mirroring section.go's
// length-prefixed, little-endian binary conventions) so any consumer can decode a VCNT object
// from object storage without a side channel.

import (
	"encoding/binary"
	"errors"
	"fmt"
)

const vcntFileMagic uint32 = 0x56434E31 // "VCN1"

// ErrNotSelfDescribing is returned by DecodeVCNTFile when data's trailing magic doesn't
// match, distinguishing "this isn't a self-describing VCNT file" from a generic decode
// error (a malformed trailer/directory on data that IS self-describing) — errors.Is-comparable.
var ErrNotSelfDescribing = errors.New("valuecounts: not a self-describing VCNT file")

// vcntTrailerSize is the fixed byte size of the self-describing file trailer:
// dirCount[4] + bodyLen[4] + magic[4].
const vcntTrailerSize = 4 + 4 + 4

// EncodeVCNTFile encodes records into a self-describing VCNT file: the same snappy-chunked
// body EncodeRecords produces, followed by an embedded chunk directory and a fixed 12-byte
// trailer (dirCount[4] + bodyLen[4] + magic[4]), so any consumer can decode the file directly
// from object storage without a side-channel directory (fixes the vcntwriter.go gap where
// EncodeRecords' returned []ChunkDirEntry is discarded — see NOTES.md).
// SPEC-VC-2: self-describing file format contract (see DecodeVCNTFile).
func EncodeVCNTFile(records []Record, perChunk int) []byte {
	body, dir := EncodeRecords(records, perChunk)
	buf := make([]byte, 0, len(body)+dirEncodedSize(dir)+vcntTrailerSize)
	buf = append(buf, body...)
	for i := range dir {
		buf = appendDirEntry(buf, &dir[i])
	}
	buf = binary.LittleEndian.AppendUint32(buf, uint32(len(dir)))  //nolint:gosec // bounded by chunk count
	buf = binary.LittleEndian.AppendUint32(buf, uint32(len(body))) //nolint:gosec // bounded by output size
	buf = binary.LittleEndian.AppendUint32(buf, vcntFileMagic)
	return buf
}

// DecodeVCNTFile decodes a file produced by EncodeVCNTFile. Returns ErrNotSelfDescribing
// (wrapped, errors.Is-comparable) if data's trailing magic doesn't match.
func DecodeVCNTFile(data []byte) ([]Record, error) {
	if len(data) < vcntTrailerSize {
		return nil, fmt.Errorf("valuecounts: %w: file too short (%d bytes)", ErrNotSelfDescribing, len(data))
	}
	trailer := data[len(data)-vcntTrailerSize:]
	if binary.LittleEndian.Uint32(trailer[8:12]) != vcntFileMagic {
		return nil, fmt.Errorf("valuecounts: %w", ErrNotSelfDescribing)
	}
	dirCount := int(binary.LittleEndian.Uint32(trailer[0:4]))
	bodyLen := int(binary.LittleEndian.Uint32(trailer[4:8]))

	if bodyLen < 0 || bodyLen > len(data)-vcntTrailerSize {
		return nil, fmt.Errorf("valuecounts: self-describing VCNT file: body length %d out of bounds", bodyLen)
	}
	body := data[:bodyLen]
	dirBytes := data[bodyLen : len(data)-vcntTrailerSize]

	dir, err := decodeDirEntries(dirBytes, dirCount)
	if err != nil {
		return nil, fmt.Errorf("valuecounts: self-describing VCNT file: %w", err)
	}
	return DecodeAll(body, dir)
}

// DecodeVCNTObject decodes a VCNT object. Since #490 A-3, EncodeVCNTFile's self-describing
// format is the only format ever written (A-Tempo-1 switched tempo's vcntwriter.go off the
// legacy EncodeVCNTRecords path), so this is now a direct call to DecodeVCNTFile — kept as
// its own function because it has real callers (valuecountscompactor and
// VCNTBuildSectionFromObjects) that shouldn't need to change on a future format addition.
// This is the compactor's per-input-file decode entry point.
func DecodeVCNTObject(data []byte) ([]Record, error) {
	return DecodeVCNTFile(data)
}

// dirEncodedSize returns the exact byte length appendDirEntry will produce for dir.
func dirEncodedSize(dir []ChunkDirEntry) int {
	n := 0
	for i := range dir {
		n += 2 + len(dir[i].MinColumn) + 8 + 4 + 4
	}
	return n
}

// appendDirEntry appends one ChunkDirEntry in the same length-prefixed, little-endian
// conventions as section.go: min_column_len[2] + min_column[N] + min_time_start[8] +
// comp_off[4] + comp_len[4].
func appendDirEntry(buf []byte, d *ChunkDirEntry) []byte {
	buf = binary.LittleEndian.AppendUint16(buf, uint16(len(d.MinColumn))) //nolint:gosec // bounded
	buf = append(buf, d.MinColumn...)
	buf = binary.LittleEndian.AppendUint64(buf, d.MinTimeStart)
	buf = binary.LittleEndian.AppendUint32(buf, d.CompOff)
	buf = binary.LittleEndian.AppendUint32(buf, d.CompLen)
	return buf
}

// minDirEntrySize is the smallest possible encoded size of one directory entry: an empty
// MinColumn (min_column_len[2] + 0 bytes) + min_time_start[8] + comp_off[4] + comp_len[4].
const minDirEntrySize = 2 + 8 + 4 + 4

// decodeDirEntries parses count ChunkDirEntry records from data, mirroring appendDirEntry's
// layout. count is a file-trailer-supplied value and must be validated against the actual
// size of data before being used as a make() capacity hint — an unvalidated, corrupted or
// malicious dirCount (e.g. 0xFFFFFFFF) would otherwise trigger an unrecoverable OOM
// runtime.throw (not a catchable panic) rather than a clean decode error (SPEC-ROOT-001,
// mirroring SPEC-ROOT-012's decompression-bomb-guard principle).
func decodeDirEntries(data []byte, count int) ([]ChunkDirEntry, error) {
	if count < 0 {
		return nil, fmt.Errorf("negative dir count %d", count)
	}
	if count > len(data)/minDirEntrySize {
		return nil, fmt.Errorf(
			"dir count %d exceeds what %d bytes could possibly hold (min %d bytes/entry)",
			count, len(data), minDirEntrySize,
		)
	}
	dir := make([]ChunkDirEntry, 0, count)
	pos := 0
	for i := range count {
		if pos+2 > len(data) {
			return nil, fmt.Errorf("dir entry %d: truncated at min_column_len", i)
		}
		colLen := int(binary.LittleEndian.Uint16(data[pos:]))
		pos += 2
		if pos+colLen > len(data) {
			return nil, fmt.Errorf("dir entry %d: min_column truncated", i)
		}
		col := string(data[pos : pos+colLen])
		pos += colLen

		if pos+16 > len(data) {
			return nil, fmt.Errorf("dir entry %d: truncated at min_time_start/comp_off/comp_len", i)
		}
		minTimeStart := binary.LittleEndian.Uint64(data[pos:])
		pos += 8
		compOff := binary.LittleEndian.Uint32(data[pos:])
		pos += 4
		compLen := binary.LittleEndian.Uint32(data[pos:])
		pos += 4

		dir = append(dir, ChunkDirEntry{
			MinColumn:    col,
			MinTimeStart: minTimeStart,
			CompOff:      compOff,
			CompLen:      compLen,
		})
	}
	return dir, nil
}
