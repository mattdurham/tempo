package cube

// NOTE: SPEC-CUBE-002 — Dictionary maps dimension values (strings) to uint16 IDs (0-65535).
// Per-file scope (not global). Wire format: count[2] + (len[2] + bytes)* per dimension.

import (
	"encoding/binary"
	"fmt"
)

// Dictionary maps dimension values to uint16 IDs.
type Dictionary struct {
	dim1Map    map[string]uint16
	dim2Map    map[string]uint16
	Dim1Values []string
	Dim2Values []string
}

// NewDictionary creates an empty dictionary.
func NewDictionary() *Dictionary {
	return &Dictionary{
		Dim1Values: []string{},
		Dim2Values: []string{},
		dim1Map:    make(map[string]uint16),
		dim2Map:    make(map[string]uint16),
	}
}

// InternDim1 returns the uint16 ID for dim1 value, allocating a new ID if needed.
func (d *Dictionary) InternDim1(value string) (uint16, error) {
	if id, ok := d.dim1Map[value]; ok {
		return id, nil
	}
	if len(d.Dim1Values) >= 65535 {
		return 0, fmt.Errorf("cube: dim1 dictionary exhausted (>65535 values)")
	}
	id := uint16(len(d.Dim1Values)) //nolint:gosec // len is bounded by 65535 check above
	d.Dim1Values = append(d.Dim1Values, value)
	d.dim1Map[value] = id
	return id, nil
}

// InternDim2 returns the uint16 ID for dim2 value, allocating a new ID if needed.
func (d *Dictionary) InternDim2(value string) (uint16, error) {
	if id, ok := d.dim2Map[value]; ok {
		return id, nil
	}
	if len(d.Dim2Values) >= 65535 {
		return 0, fmt.Errorf("cube: dim2 dictionary exhausted (>65535 values)")
	}
	id := uint16(len(d.Dim2Values)) //nolint:gosec // len is bounded by 65535 check above
	d.Dim2Values = append(d.Dim2Values, value)
	d.dim2Map[value] = id
	return id, nil
}

// LookupID1 returns the uint16 ID for a dim1 value via the reverse map (O(1)),
// or (0, false) if the value is not present.
func (d *Dictionary) LookupID1(value string) (uint16, bool) {
	id, ok := d.dim1Map[value]
	return id, ok
}

// LookupID2 returns the uint16 ID for a dim2 value via the reverse map (O(1)),
// or (0, false) if the value is not present.
func (d *Dictionary) LookupID2(value string) (uint16, bool) {
	id, ok := d.dim2Map[value]
	return id, ok
}

// LookupDim1 returns the string value for a dim1 ID, or ("", false) if not found.
func (d *Dictionary) LookupDim1(id uint16) (string, bool) {
	if int(id) >= len(d.Dim1Values) {
		return "", false
	}
	return d.Dim1Values[id], true
}

// LookupDim2 returns the string value for a dim2 ID, or ("", false) if not found.
func (d *Dictionary) LookupDim2(id uint16) (string, bool) {
	if int(id) >= len(d.Dim2Values) {
		return "", false
	}
	return d.Dim2Values[id], true
}

// EncodeDictionary serializes a Dictionary to bytes (LittleEndian).
// Wire: dim1_count[2] + (len[2] + bytes)* + dim2_count[2] + (len[2] + bytes)*
func EncodeDictionary(d *Dictionary) []byte {
	buf := make([]byte, 0, 1024)
	buf = binary.LittleEndian.AppendUint16(buf, uint16(len(d.Dim1Values))) //nolint:gosec // len is bounded by 65535
	for _, v := range d.Dim1Values {
		buf = binary.LittleEndian.AppendUint16(buf, uint16(len(v))) //nolint:gosec // string len bounded by uint16
		buf = append(buf, v...)
	}
	buf = binary.LittleEndian.AppendUint16(buf, uint16(len(d.Dim2Values))) //nolint:gosec // len is bounded by 65535
	for _, v := range d.Dim2Values {
		buf = binary.LittleEndian.AppendUint16(buf, uint16(len(v))) //nolint:gosec // string len bounded by uint16
		buf = append(buf, v...)
	}
	return buf
}

// DecodeDictionary parses a Dictionary from bytes.
func DecodeDictionary(buf []byte) (*Dictionary, error) {
	if len(buf) < 2 {
		return nil, fmt.Errorf("cube: dictionary buffer too short")
	}
	d := NewDictionary()
	pos := 0

	// Decode dim1
	count1 := int(binary.LittleEndian.Uint16(buf[pos:]))
	pos += 2
	for i := 0; i < count1; i++ {
		if pos+2 > len(buf) {
			return nil, fmt.Errorf("cube: dim1 entry %d truncated", i)
		}
		vlen := int(binary.LittleEndian.Uint16(buf[pos:]))
		pos += 2
		if pos+vlen > len(buf) {
			return nil, fmt.Errorf("cube: dim1 entry %d value truncated", i)
		}
		d.Dim1Values = append(d.Dim1Values, string(buf[pos:pos+vlen]))
		pos += vlen
	}

	// Decode dim2
	if pos+2 > len(buf) {
		return nil, fmt.Errorf("cube: dim2 count truncated")
	}
	count2 := int(binary.LittleEndian.Uint16(buf[pos:]))
	pos += 2
	for i := 0; i < count2; i++ {
		if pos+2 > len(buf) {
			return nil, fmt.Errorf("cube: dim2 entry %d truncated", i)
		}
		vlen := int(binary.LittleEndian.Uint16(buf[pos:]))
		pos += 2
		if pos+vlen > len(buf) {
			return nil, fmt.Errorf("cube: dim2 entry %d value truncated", i)
		}
		d.Dim2Values = append(d.Dim2Values, string(buf[pos:pos+vlen]))
		pos += vlen
	}

	// Rebuild reverse maps
	for i, v := range d.Dim1Values {
		d.dim1Map[v] = uint16(i) //nolint:gosec // i bounded by uint16 count1 decoded above
	}
	for i, v := range d.Dim2Values {
		d.dim2Map[v] = uint16(i) //nolint:gosec // i bounded by uint16 count2 decoded above
	}

	return d, nil
}
