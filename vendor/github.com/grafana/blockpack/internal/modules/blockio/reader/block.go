// Package reader implements the blockpack file reader.
package reader

// NOTE: Any changes to this file must be reflected in the corresponding specs.md or NOTES.md.

import "github.com/grafana/blockpack/internal/modules/blockio/shared"

// Column holds a decoded column ready for query evaluation.
type Column struct {
	Name string

	// Dictionary fields — MUST be heap-allocated (never arena) per NOTES §10.
	StringDict  []string
	StringIdx   []uint32
	Int64Dict   []int64
	Int64Idx    []uint32
	Uint64Dict  []uint64
	Uint64Idx   []uint32
	Float64Dict []float64
	Float64Idx  []uint32
	BoolDict    []uint8
	BoolIdx     []uint32
	BytesDict   [][]byte
	BytesIdx    []uint32

	// Inline bytes (kinds 3/4) — no dictionary.
	BytesInline [][]byte

	// Presence bitset — MAY be arena-allocated.
	Present []byte

	// Total span count this column covers (including nulls).
	SpanCount int
	Type      shared.ColumnType
}

// IsPresent reports whether span at idx has a value.
func (c *Column) IsPresent(idx int) bool {
	if c.Present == nil {
		return true
	}

	return shared.IsPresent(c.Present, idx)
}

// StringValue returns the string value at idx and whether it is present.
func (c *Column) StringValue(idx int) (string, bool) {
	if !c.IsPresent(idx) {
		return "", false
	}

	if len(c.StringIdx) > idx {
		di := int(c.StringIdx[idx])
		if di < len(c.StringDict) {
			return c.StringDict[di], true
		}
	}

	return "", false
}

// Int64Value returns the int64 value at idx and whether it is present.
func (c *Column) Int64Value(idx int) (int64, bool) {
	if !c.IsPresent(idx) {
		return 0, false
	}

	if len(c.Int64Idx) > idx {
		di := int(c.Int64Idx[idx])
		if di < len(c.Int64Dict) {
			return c.Int64Dict[di], true
		}
	}

	return 0, false
}

// Uint64Value returns the uint64 value at idx and whether it is present.
func (c *Column) Uint64Value(idx int) (uint64, bool) {
	if !c.IsPresent(idx) {
		return 0, false
	}

	if len(c.Uint64Idx) > idx {
		di := int(c.Uint64Idx[idx])
		if di < len(c.Uint64Dict) {
			return c.Uint64Dict[di], true
		}
	}

	return 0, false
}

// Float64Value returns the float64 value at idx and whether it is present.
func (c *Column) Float64Value(idx int) (float64, bool) {
	if !c.IsPresent(idx) {
		return 0, false
	}

	if len(c.Float64Idx) > idx {
		di := int(c.Float64Idx[idx])
		if di < len(c.Float64Dict) {
			return c.Float64Dict[di], true
		}
	}

	return 0, false
}

// BoolValue returns the bool value at idx and whether it is present.
func (c *Column) BoolValue(idx int) (bool, bool) {
	if !c.IsPresent(idx) {
		return false, false
	}

	if len(c.BoolIdx) > idx {
		di := int(c.BoolIdx[idx])
		if di < len(c.BoolDict) {
			return c.BoolDict[di] != 0, true
		}
	}

	return false, false
}

// BytesValue returns the bytes value at idx and whether it is present.
func (c *Column) BytesValue(idx int) ([]byte, bool) {
	if !c.IsPresent(idx) {
		return nil, false
	}

	if c.BytesInline != nil {
		if idx < len(c.BytesInline) {
			return c.BytesInline[idx], true
		}

		return nil, false
	}

	if len(c.BytesIdx) > idx {
		di := int(c.BytesIdx[idx])
		if di < len(c.BytesDict) {
			return c.BytesDict[di], true
		}
	}

	return nil, false
}

// Block holds decoded columns for a single block.
type Block struct {
	columns   map[string]*Column
	meta      shared.BlockMeta
	spanCount int
}

// NewBlockForParsing creates a Block with an empty columns map, for use with AddColumnsToBlock.
func NewBlockForParsing(meta shared.BlockMeta) *Block {
	return &Block{
		columns:   make(map[string]*Column),
		meta:      meta,
		spanCount: int(meta.SpanCount),
	}
}

// SpanCount returns the number of spans in the block.
func (b *Block) SpanCount() int { return b.spanCount }

// GetColumn returns the named column or nil if absent.
func (b *Block) GetColumn(name string) *Column { return b.columns[name] }

// Columns returns the full column map.
func (b *Block) Columns() map[string]*Column { return b.columns }

// Meta returns the block metadata.
func (b *Block) Meta() shared.BlockMeta { return b.meta }

// BlockWithBytes bundles a decoded Block with its raw bytes for AddColumnsToBlock.
type BlockWithBytes struct {
	Block    *Block
	RawBytes []byte
}
