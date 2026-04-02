package reader

// NOTE: Any changes to this file must be reflected in the corresponding specs.md or NOTES.md.
// NOTE-45: FileBloom provides file-level bloom filtering for resource.service.name.
// Callers can cache FileBloom.Raw() bytes and reconstruct with ParseFileBloom.

import (
	"encoding/binary"
	"fmt"

	"github.com/grafana/blockpack/internal/modules/blockio/shared"
	"github.com/grafana/blockpack/internal/modules/sketch"
)

const fileBloomMinLen = 9 // magic[4] + version[1] + col_count[4]

// FileBloom holds parsed file-level bloom filters for fast per-file rejection.
// Obtain from Reader.FileBloom() or ParseFileBloom.
type FileBloom struct {
	columns map[string]*sketch.BinaryFuse8
	raw     []byte
}

// MayContainString returns false only when val is definitely absent from column col.
// Returns true (conservative) when the column is not tracked or has no filter.
func (fb *FileBloom) MayContainString(col, val string) bool {
	if fb == nil {
		return true
	}
	f := fb.columns[col]
	if f == nil {
		return true
	}
	return f.Contains(sketch.HashForFuse(val))
}

// Raw returns the raw bytes of this FileBloom section (suitable for caching by the caller).
// Returns nil if the file has no FileBloom section.
func (fb *FileBloom) Raw() []byte {
	if fb == nil {
		return nil
	}
	return fb.raw
}

// ParseFileBloom reconstructs a FileBloom from its previously cached Raw() bytes.
// Returns nil, nil for nil or empty input (old files — degrade gracefully).
func ParseFileBloom(b []byte) (*FileBloom, error) {
	if len(b) == 0 {
		return nil, nil
	}
	fb, _, err := parseFileBloomSection(b)
	return fb, err
}

// parseFileBloomSection parses the FileBloom section from data.
// Returns (nil, 0, nil) when data does not start with the FBLM magic (graceful degradation).
// Returns (*FileBloom, bytesConsumed, error) on success.
func parseFileBloomSection(data []byte) (*FileBloom, int, error) {
	if len(data) < fileBloomMinLen {
		return nil, 0, nil
	}
	magic := binary.LittleEndian.Uint32(data[0:])
	if magic != shared.FileBloomMagic {
		return nil, 0, nil // not a FileBloom section — graceful degradation
	}
	// version[1]
	version := data[4]
	if version != shared.FileBloomVersion {
		return nil, 0, fmt.Errorf("fileBloom: unsupported version %d", version)
	}
	// col_count[4]
	colCount := int(binary.LittleEndian.Uint32(data[5:]))
	pos := 9

	const (
		minBytesPerCol = 2 + 4 // name_len[2] + fuse_len[4]
		maxCols        = 1_000
	)
	remaining := len(data) - pos
	if colCount > remaining/minBytesPerCol || colCount > maxCols {
		return nil, 0, fmt.Errorf("fileBloom: col_count %d exceeds bounds", colCount)
	}

	columns := make(map[string]*sketch.BinaryFuse8, colCount)
	for range colCount {
		if pos+2 > len(data) {
			return nil, 0, fmt.Errorf("fileBloom: truncated at name_len")
		}
		nameLen := int(binary.LittleEndian.Uint16(data[pos:]))
		pos += 2
		if pos+nameLen > len(data) {
			return nil, 0, fmt.Errorf("fileBloom: truncated at name")
		}
		name := string(data[pos : pos+nameLen])
		pos += nameLen

		if pos+4 > len(data) {
			return nil, 0, fmt.Errorf("fileBloom: col %q: truncated at fuse_len", name)
		}
		fuseLen := int(binary.LittleEndian.Uint32(data[pos:]))
		pos += 4

		var f *sketch.BinaryFuse8
		if fuseLen > 0 {
			if pos+fuseLen > len(data) {
				return nil, 0, fmt.Errorf("fileBloom: col %q: truncated at fuse_data (need %d)", name, fuseLen)
			}
			f = &sketch.BinaryFuse8{}
			if err := f.UnmarshalBinary(data[pos : pos+fuseLen]); err != nil {
				return nil, 0, fmt.Errorf("fileBloom: col %q: fuse8: %w", name, err)
			}
			pos += fuseLen
		}
		columns[name] = f // nil f means empty (fuse_len == 0)
	}

	total := pos
	fb := &FileBloom{
		raw:     data[:total],
		columns: columns,
	}
	return fb, total, nil
}
