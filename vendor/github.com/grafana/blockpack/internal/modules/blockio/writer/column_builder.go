// Package writer implements the blockpack file writer.
package writer

// NOTE: Any changes to this file must be reflected in the corresponding specs.md or NOTES.md.

import (
	"github.com/grafana/blockpack/internal/modules/blockio/shared"
	"github.com/klauspost/compress/zstd"
)

// zstdEncoder wraps a *zstd.Encoder to compress data into a reusable buffer.
// Per NOTES §5, a single encoder is reused for all blocks to bound memory usage.
type zstdEncoder struct {
	enc *zstd.Encoder
	buf []byte
}

// newZstdEncoder creates a zstdEncoder with SpeedDefault and concurrency 1.
func newZstdEncoder() (*zstdEncoder, error) {
	enc, err := zstd.NewWriter(nil,
		zstd.WithEncoderConcurrency(1),
		zstd.WithEncoderLevel(zstd.SpeedDefault),
	)
	if err != nil {
		return nil, err
	}

	return &zstdEncoder{enc: enc}, nil
}

// compress encodes src using zstd and returns a copy of the compressed bytes.
// The internal buf is reused across calls; callers must not retain the previous result.
func (z *zstdEncoder) compress(src []byte) ([]byte, error) {
	z.buf = z.enc.EncodeAll(src, z.buf[:0])

	result := make([]byte, len(z.buf))
	copy(result, z.buf)

	return result, nil
}

// columnBuilder accumulates values for one column and encodes them to the wire format.
// Each method corresponds to one value type; implementations ignore types that don't
// match the column's declared type. The present flag distinguishes null from non-null rows.
type columnBuilder interface {
	addString(val string, present bool)
	addInt64(val int64, present bool)
	addUint64(val uint64, present bool)
	addFloat64(val float64, present bool)
	addBool(val bool, present bool)
	addBytes(val []byte, present bool)
	rowCount() int
	nullCount() int
	colType() shared.ColumnType
	// buildData returns the wire-format column blob:
	// enc_version[1]=2 + encoding_kind[1] + payload (per SPECS §8.4).
	buildData(enc *zstdEncoder) ([]byte, error)
}

// newColumnBuilder creates the appropriate columnBuilder for the given column type and name.
// initCap is the initial capacity for the value and present slices; pass 0 to use the
// default (nil slice, grows on demand). Passing the expected span count eliminates growslice
// calls during the per-span append loop.
func newColumnBuilder(typ shared.ColumnType, colName string, initCap int) columnBuilder {
	switch typ {
	case shared.ColumnTypeString, shared.ColumnTypeRangeString:
		return &stringColumnBuilder{
			colName: colName,
			values:  make([]string, 0, initCap),
			present: make([]bool, 0, initCap),
		}
	case shared.ColumnTypeInt64, shared.ColumnTypeRangeInt64, shared.ColumnTypeRangeDuration:
		return &int64ColumnBuilder{
			values:  make([]int64, 0, initCap),
			present: make([]bool, 0, initCap),
		}
	case shared.ColumnTypeUint64, shared.ColumnTypeRangeUint64:
		return &uint64ColumnBuilder{
			colName: colName,
			values:  make([]uint64, 0, initCap),
			present: make([]bool, 0, initCap),
		}
	case shared.ColumnTypeFloat64, shared.ColumnTypeRangeFloat64:
		return &float64ColumnBuilder{
			values:  make([]float64, 0, initCap),
			present: make([]bool, 0, initCap),
		}
	case shared.ColumnTypeBool:
		return &boolColumnBuilder{
			values:  make([]bool, 0, initCap),
			present: make([]bool, 0, initCap),
		}
	case shared.ColumnTypeBytes, shared.ColumnTypeRangeBytes:
		return &bytesColumnBuilder{
			colName: colName,
			values:  make([][]byte, 0, initCap),
			present: make([]bool, 0, initCap),
		}
	default:
		return &stringColumnBuilder{
			colName: colName,
			values:  make([]string, 0, initCap),
			present: make([]bool, 0, initCap),
		}
	}
}
