package cube

// NOTE: SPEC-CUBE-001 — AggCell is the SOLE cell type (E-3 APPENDIX 2, #491): a 12-byte sparse
// base record (minute, dim1_id, dim2_id, count) with base fields FLATTENED directly onto the
// struct, PLUS zero or more 540-byte per-aggAttr records (len(Aggs) == the owning file's
// NumAggAttrs; empty/nil is wire-tolerated but never produced by any real Definition/TryCreate
// path, per E-4's validateDefinition). Sort order: minute ASC, dim1_id ASC, dim2_id ASC. All
// fields LittleEndian. There is deliberately no separate, narrower "Cell" type — a prior revision
// of this file had one (EncodeCell/DecodeCell/CompareCell + a nested Cell field on AggCell); it
// was DELETED, not deprecated-and-kept, per team-lead's binding "no coexistence, one-current-
// version" ruling — do not reintroduce it.

import (
	"cmp"
	"encoding/binary"
	"fmt"
	"math"
)

// AggAttrRecordWidth is the fixed byte width of one aggAttr's record within an AggCell.
// recordWidthFor (chunk.go) is the one place the "12 + n*540" full-record-width arithmetic lives.
const AggAttrRecordWidth = 4 + 8 + 8 + 8 + BucketCount*8 // SampleCount+Sum+Min+Max+Buckets

// AggAttrValues is one materialized aggregate attribute's accumulated state for one cell.
// SampleCount == 0 means no valid sample was ever observed for this attribute at this cell —
// the signal downstream merge/response code uses to avoid leaking the Min/Max sentinel-init
// values (math.MaxFloat64 / -math.MaxFloat64) into output (E-4/E-5's convention).
type AggAttrValues struct {
	SampleCount uint32
	Sum         float64
	Min         float64
	Max         float64
	Buckets     [BucketCount]uint64 // only populated for Int64/Duration-typed attrs (ruling 1)
}

// EncodeAggAttrValues serializes an AggAttrValues to AggAttrRecordWidth (540) bytes.
func EncodeAggAttrValues(v AggAttrValues) []byte {
	buf := make([]byte, AggAttrRecordWidth)
	binary.LittleEndian.PutUint32(buf[0:4], v.SampleCount)
	binary.LittleEndian.PutUint64(buf[4:12], math.Float64bits(v.Sum))
	binary.LittleEndian.PutUint64(buf[12:20], math.Float64bits(v.Min))
	binary.LittleEndian.PutUint64(buf[20:28], math.Float64bits(v.Max))
	for i, b := range v.Buckets {
		off := 28 + i*8
		binary.LittleEndian.PutUint64(buf[off:off+8], b)
	}
	return buf
}

// DecodeAggAttrValues parses an AggAttrRecordWidth-byte buffer into an AggAttrValues.
func DecodeAggAttrValues(buf []byte) (AggAttrValues, error) {
	if len(buf) < AggAttrRecordWidth {
		return AggAttrValues{}, fmt.Errorf("cube: aggAttr record buffer too short (%d bytes)", len(buf))
	}
	v := AggAttrValues{
		SampleCount: binary.LittleEndian.Uint32(buf[0:4]),
		Sum:         math.Float64frombits(binary.LittleEndian.Uint64(buf[4:12])),
		Min:         math.Float64frombits(binary.LittleEndian.Uint64(buf[12:20])),
		Max:         math.Float64frombits(binary.LittleEndian.Uint64(buf[20:28])),
	}
	for i := range v.Buckets {
		off := 28 + i*8
		v.Buckets[i] = binary.LittleEndian.Uint64(buf[off : off+8])
	}
	return v, nil
}

// AggCell is the sole cell type: base fields flattened directly on the struct (not nested in a
// separate Cell type) plus zero or more per-aggAttr records.
type AggCell struct {
	Aggs   []AggAttrValues
	Minute uint32 // unix timestamp / 60
	Count  uint32 // exact span count
	Dim1ID uint16 // dictionary ID for dimension 1 (e.g., service.name)
	Dim2ID uint16 // dictionary ID for dimension 2 (e.g., http.status_code)
}

// EncodeAggCell serializes an AggCell to its base 12 bytes followed by len(Aggs)*540 bytes.
// When Aggs is empty, the output is the exact pre-#491 12-byte format — ruling 2's degenerate
// case (NumAggAttrs==0 reproduces today's exact format).
func EncodeAggCell(ac AggCell) []byte {
	buf := make([]byte, 12, 12+len(ac.Aggs)*AggAttrRecordWidth)
	binary.LittleEndian.PutUint32(buf[0:4], ac.Minute)
	binary.LittleEndian.PutUint16(buf[4:6], ac.Dim1ID)
	binary.LittleEndian.PutUint16(buf[6:8], ac.Dim2ID)
	binary.LittleEndian.PutUint32(buf[8:12], ac.Count)
	for _, agg := range ac.Aggs {
		buf = append(buf, EncodeAggAttrValues(agg)...)
	}
	return buf
}

// DecodeAggCell parses a recordWidthFor(numAggAttrs)-byte buffer into an AggCell.
func DecodeAggCell(buf []byte, numAggAttrs int) (AggCell, error) {
	width := recordWidthFor(numAggAttrs)
	if len(buf) < width {
		return AggCell{}, fmt.Errorf(
			"cube: aggCell buffer too short (%d bytes, need %d for %d aggAttrs)",
			len(buf),
			width,
			numAggAttrs,
		)
	}
	ac := AggCell{
		Minute: binary.LittleEndian.Uint32(buf[0:4]),
		Dim1ID: binary.LittleEndian.Uint16(buf[4:6]),
		Dim2ID: binary.LittleEndian.Uint16(buf[6:8]),
		Count:  binary.LittleEndian.Uint32(buf[8:12]),
	}
	if numAggAttrs > 0 {
		ac.Aggs = make([]AggAttrValues, numAggAttrs)
		offset := 12
		for i := 0; i < numAggAttrs; i++ {
			agg, decErr := DecodeAggAttrValues(buf[offset : offset+AggAttrRecordWidth])
			if decErr != nil {
				return AggCell{}, fmt.Errorf("cube: aggCell attr %d: %w", i, decErr)
			}
			ac.Aggs[i] = agg
			offset += AggAttrRecordWidth
		}
	}
	return ac, nil
}

// CompareAggCell returns -1 if a < b, 0 if a == b, +1 if a > b.
// Sort order: minute ASC, dim1_id ASC, dim2_id ASC (Aggs never participates in ordering — replaces
// the deleted CompareCell with identical base-field semantics).
func CompareAggCell(a, b AggCell) int {
	if a.Minute != b.Minute {
		return cmp.Compare(a.Minute, b.Minute)
	}
	if a.Dim1ID != b.Dim1ID {
		return cmp.Compare(a.Dim1ID, b.Dim1ID)
	}
	return cmp.Compare(a.Dim2ID, b.Dim2ID)
}
