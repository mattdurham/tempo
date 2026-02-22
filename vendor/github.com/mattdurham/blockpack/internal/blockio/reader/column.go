package reader

import (
	"fmt"

	"github.com/klauspost/compress/zstd"
	"github.com/mattdurham/blockpack/internal/arena"
	arenslice "github.com/mattdurham/blockpack/internal/arena/slice"
	"github.com/mattdurham/blockpack/internal/blockio/shared"
)

// zstdDecoder is shared across all column decoders for memory efficiency.
// CONCURRENCY SAFE: DecodeAll() with nil destination buffer is safe for
// concurrent use as it allocates new buffers per call. See:
// https://pkg.go.dev/github.com/klauspost/compress/zstd#Decoder.DecodeAll
var zstdDecoder = mustNewZstdDecoder()

func mustNewZstdDecoder() *zstd.Decoder {
	decoder, err := zstd.NewReader(nil)
	if err != nil {
		panic(fmt.Sprintf("failed to create zstd decoder: %v", err))
	}
	return decoder
}

// makeSlice allocates a slice, using arena if provided, otherwise heap.
// Returns a regular Go slice regardless of allocation source.
func makeSlice[T any](a *arena.Arena, n int) []T {
	if a == nil {
		// Heap allocation (backward compatibility)
		return make([]T, n)
	}
	// Arena allocation
	arenaSlice := arenslice.Make[T](a, n)
	return arenaSlice.Raw()
}

type columnEncodingFlags struct {
	isSparse          bool
	isInline          bool
	isRLE             bool
	isDelta           bool
	isXOR             bool
	isPrefix          bool
	isDeltaDictionary bool // Dictionary with delta-encoded indices (trace:id optimization)
}

func parseColumnReuse(
	name string,
	typ ColumnType,
	expectedRows int,
	data []byte,
	statsData []byte,
	reusable *Column,
	a *arena.Arena,
) (*Column, error) {
	rd := &sliceReader{data: data}
	col := reuseColumn(name, typ, reusable)

	flags, err := readColumnEncoding(rd, name)
	if err != nil {
		return nil, err
	}
	if err := validateColumnEncoding(name, typ, flags); err != nil {
		return nil, err
	}

	switch {
	case flags.isDelta:
		return decodeDeltaUint64Column(name, rd, statsData, col, a)
	case flags.isDeltaDictionary:
		return decodeDeltaDictionaryBytesColumn(name, rd, statsData, col, flags, a)
	case flags.isXOR:
		return decodeXORBytesColumn(name, rd, statsData, col, a)
	case flags.isPrefix:
		return decodePrefixBytesColumn(name, rd, statsData, col, a)
	default:
		return decodeDictionaryColumn(name, typ, expectedRows, rd, statsData, col, flags, a)
	}
}

func reuseColumn(name string, typ ColumnType, reusable *Column) *Column {
	if reusable != nil && reusable.Type == typ {
		reusable.Name = name
		return reusable
	}
	return &Column{
		Name: name,
		Type: typ,
	}
}

func readColumnEncoding(rd *sliceReader, name string) (columnEncodingFlags, error) {
	encVersion, err := rd.readUint8()
	if err != nil {
		return columnEncodingFlags{}, fmt.Errorf("column %s: %w", name, err)
	}
	if encVersion != columnEncodingVersion {
		return columnEncodingFlags{}, fmt.Errorf("column %s: unsupported encoding version %d", name, encVersion)
	}
	encodingKind, err := rd.readUint8()
	if err != nil {
		return columnEncodingFlags{}, fmt.Errorf("column %s: %w", name, err)
	}

	flags := columnEncodingFlags{}
	switch encodingKind {
	case encodingKindDictionary:
	case encodingKindSparseDictionary:
		flags.isSparse = true
	case encodingKindInlineBytes:
		flags.isInline = true
	case encodingKindSparseInlineBytes:
		flags.isInline = true
		flags.isSparse = true
	case encodingKindDeltaUint64:
		flags.isDelta = true
	case encodingKindXORBytes:
		flags.isXOR = true
	case encodingKindSparseXORBytes:
		flags.isXOR = true
		flags.isSparse = true
	case encodingKindPrefixBytes:
		flags.isPrefix = true
	case encodingKindSparsePrefixBytes:
		flags.isPrefix = true
		flags.isSparse = true
	case encodingKindRLEIndexes:
		flags.isRLE = true
	case encodingKindSparseRLEIndexes:
		flags.isRLE = true
		flags.isSparse = true
	case encodingKindDeltaDictionary:
		flags.isDeltaDictionary = true
	case encodingKindSparseDeltaDictionary:
		flags.isDeltaDictionary = true
		flags.isSparse = true
	default:
		return columnEncodingFlags{}, fmt.Errorf("column %s: unsupported encoding kind %d", name, encodingKind)
	}

	return flags, nil
}

func validateColumnEncoding(name string, typ ColumnType, flags columnEncodingFlags) error {
	if flags.isInline && typ != ColumnTypeBytes {
		return fmt.Errorf("column %s: inline encoding unsupported for type %d", name, typ)
	}
	if flags.isDelta && typ != ColumnTypeUint64 {
		return fmt.Errorf("column %s: delta encoding only supported for uint64", name)
	}
	if flags.isXOR && typ != ColumnTypeBytes {
		return fmt.Errorf("column %s: XOR encoding only supported for bytes", name)
	}
	if flags.isPrefix && typ != ColumnTypeBytes {
		return fmt.Errorf("column %s: prefix encoding only supported for bytes", name)
	}
	return nil
}

// BOT: these decodes are ripe to move to their own folder.
func decodeDeltaUint64Column(
	name string,
	rd *sliceReader,
	statsData []byte,
	col *Column,
	a *arena.Arena,
) (*Column, error) {
	// BOT: added comment in slice reader, but this would be great if it was rd.readSpanCount(), and the same for below.
	spanCount, err := rd.readUint32()
	if err != nil {
		return nil, fmt.Errorf("column %s: %w", name, err)
	}
	presenceLen, err := rd.readUint32()
	if err != nil {
		return nil, fmt.Errorf("column %s: %w", name, err)
	}
	presenceRLE, err := rd.readBytes(int(presenceLen))
	if err != nil {
		return nil, fmt.Errorf("column %s: %w", name, err)
	}
	presence, err := shared.DecodePresenceRLE(presenceRLE, int(spanCount))
	if err != nil {
		return nil, fmt.Errorf("column %s: failed to decode presence: %w", name, err)
	}

	// Read base timestamp (minimum value in block)
	baseTimestamp, err := rd.readUint64()
	if err != nil {
		return nil, fmt.Errorf("column %s: %w", name, err)
	}

	// Read width
	width, err := rd.readUint8()
	if err != nil {
		return nil, fmt.Errorf("column %s: %w", name, err)
	}

	// If width is 0, no values present
	if width == 0 {
		col.SetUint64Data(nil, makeSlice[uint32](a, int(spanCount)), presence)
		if err := parseColumnStatsIfPresent(name, ColumnTypeUint64, statsData, col, true); err != nil { //nolint:govet
			return nil, err
		}
		return col, nil
	}

	// Read compressed offsets
	offsetLen, err := rd.readUint32()
	if err != nil {
		return nil, fmt.Errorf("column %s: %w", name, err)
	}
	compressedOffsets, err := rd.readBytes(int(offsetLen))
	if err != nil {
		return nil, fmt.Errorf("column %s: %w", name, err)
	}
	offsetBytes, err := zstdDecoder.DecodeAll(compressedOffsets, nil)
	if err != nil {
		return nil, fmt.Errorf("column %s: failed to decompress offsets: %w", name, err)
	}

	offsetReader := &sliceReader{data: offsetBytes}
	uintIndexes := makeSlice[uint32](a, int(spanCount))
	// Pre-allocate for worst case (all present), will slice at end
	uintValues := makeSlice[uint64](a, int(spanCount))
	valueIdx := uint32(0)

	for i := 0; i < int(spanCount); i++ {
		if shared.IsBitSet(presence, i) {
			// Read offset based on width
			var offset uint64
			switch width {
			case 1:
				b, err := offsetReader.readUint8() //nolint:govet
				if err != nil {
					return nil, fmt.Errorf("column %s: %w", name, err)
				}
				//nolint:gosec
				offset = uint64(b)
			case 2:
				w, err := offsetReader.readUint16() //nolint:govet
				if err != nil {
					return nil, fmt.Errorf("column %s: %w", name, err)
				}
				//nolint:gosec
				offset = uint64(w)
			case 4:
				d, err := offsetReader.readUint32() //nolint:govet
				if err != nil {
					return nil, fmt.Errorf("column %s: %w", name, err)
				}
				//nolint:gosec
				offset = uint64(d)
			case 8:
				offset, err = offsetReader.readUint64()
				if err != nil {
					return nil, fmt.Errorf("column %s: %w", name, err)
				}
			default:
				return nil, fmt.Errorf("column %s: invalid width %d", name, width)
			}

			value := baseTimestamp + offset
			uintIndexes[i] = valueIdx
			uintValues[valueIdx] = value
			valueIdx++
		}
	}
	uintValues = uintValues[:valueIdx]

	col.SetUint64Data(uintValues, uintIndexes, presence)
	if err := parseColumnStatsIfPresent(name, ColumnTypeUint64, statsData, col, true); err != nil {
		return nil, err
	}
	return col, nil
}

func decodeXORBytesColumn(
	name string,
	rd *sliceReader,
	statsData []byte,
	col *Column,
	a *arena.Arena,
) (*Column, error) {
	spanCount, err := rd.readUint32()
	if err != nil {
		return nil, fmt.Errorf("column %s: %w", name, err)
	}
	presenceLen, err := rd.readUint32()
	if err != nil {
		return nil, fmt.Errorf("column %s: %w", name, err)
	}
	presenceData, err := rd.readBytes(int(presenceLen))
	if err != nil {
		return nil, fmt.Errorf("column %s: %w", name, err)
	}
	presence, err := shared.DecodePresenceRLE(presenceData, int(spanCount))
	if err != nil {
		return nil, fmt.Errorf("column %s: %w", name, err)
	}

	xorLen, err := rd.readUint32()
	if err != nil {
		return nil, fmt.Errorf("column %s: %w", name, err)
	}
	compressedXOR, err := rd.readBytes(int(xorLen))
	if err != nil {
		return nil, fmt.Errorf("column %s: %w", name, err)
	}
	xorBytes, err := zstdDecoder.DecodeAll(compressedXOR, nil)
	if err != nil {
		return nil, fmt.Errorf("column %s: failed to decompress XOR data: %w", name, err)
	}

	xorReader := &sliceReader{data: xorBytes}
	bytesIndexes := makeSlice[uint32](a, int(spanCount))
	// Pre-allocate for worst case (all present), will slice at end
	bytesValues := makeSlice[[]byte](a, int(spanCount))
	var prevValue []byte
	valueIdx := uint32(0)

	for i := 0; i < int(spanCount); i++ {
		if shared.IsBitSet(presence, i) {
			valLen, err := xorReader.readUint32()
			if err != nil {
				return nil, fmt.Errorf("column %s: %w", name, err)
			}
			xorData, err := xorReader.readBytes(int(valLen))
			if err != nil {
				return nil, fmt.Errorf("column %s: %w", name, err)
			}

			var value []byte
			if prevValue == nil {
				value = xorData
			} else {
				value = makeSlice[byte](a, len(xorData))
				for j := 0; j < len(xorData); j++ {
					if j < len(prevValue) {
						value[j] = xorData[j] ^ prevValue[j]
					} else {
						value[j] = xorData[j]
					}
				}
			}

			bytesIndexes[i] = valueIdx
			bytesValues[valueIdx] = value
			prevValue = value
			valueIdx++
		}
	}
	bytesValues = bytesValues[:valueIdx]

	col.SetBytesDictData(bytesValues, bytesIndexes, presence)
	if err := parseColumnStatsIfPresent(name, ColumnTypeBytes, statsData, col, true); err != nil {
		return nil, err
	}
	return col, nil
}

func decodePrefixBytesColumn(
	name string,
	rd *sliceReader,
	statsData []byte,
	col *Column,
	a *arena.Arena,
) (*Column, error) {
	spanCount, err := rd.readUint32()
	if err != nil {
		return nil, fmt.Errorf("column %s: %w", name, err)
	}
	presenceLen, err := rd.readUint32()
	if err != nil {
		return nil, fmt.Errorf("column %s: %w", name, err)
	}
	presenceData, err := rd.readBytes(int(presenceLen))
	if err != nil {
		return nil, fmt.Errorf("column %s: %w", name, err)
	}
	presence, err := shared.DecodePresenceRLE(presenceData, int(spanCount))
	if err != nil {
		return nil, fmt.Errorf("column %s: %w", name, err)
	}

	prefixList, err := readPrefixDictionary(rd, name, a)
	if err != nil {
		return nil, err
	}
	prefixIndexWidth, suffixReader, err := readSuffixReader(rd, name)
	if err != nil {
		return nil, err
	}

	bytesIndexes := makeSlice[uint32](a, int(spanCount))
	// Pre-allocate for worst case (all present), will slice at end
	bytesValues := makeSlice[[]byte](a, int(spanCount))
	valueIdx := uint32(0)

	for i := 0; i < int(spanCount); i++ {
		if shared.IsBitSet(presence, i) {
			var prefixIdx uint32
			switch prefixIndexWidth {
			case 1:
				b, err := suffixReader.readUint8()
				if err != nil {
					return nil, fmt.Errorf("column %s: %w", name, err)
				}
				prefixIdx = uint32(b)
			case 2:
				idx, err := suffixReader.readUint16()
				if err != nil {
					return nil, fmt.Errorf("column %s: %w", name, err)
				}
				prefixIdx = uint32(idx)
			case 4:
				idx, err := suffixReader.readUint32()
				if err != nil {
					return nil, fmt.Errorf("column %s: %w", name, err)
				}
				prefixIdx = idx
			default:
				return nil, fmt.Errorf("column %s: invalid prefix index width %d", name, prefixIndexWidth)
			}

			suffixLen, err := suffixReader.readUint32()
			if err != nil {
				return nil, fmt.Errorf("column %s: %w", name, err)
			}
			suffixData, err := suffixReader.readBytes(int(suffixLen))
			if err != nil {
				return nil, fmt.Errorf("column %s: %w", name, err)
			}

			var value []byte
			if prefixIdx != 0xFFFFFFFF && int(prefixIdx) < len(prefixList) {
				prefix := prefixList[prefixIdx]
				value = makeSlice[byte](a, len(prefix)+len(suffixData))
				copy(value, []byte(prefix))
				copy(value[len(prefix):], suffixData)
			} else {
				value = suffixData
			}

			bytesIndexes[i] = valueIdx
			bytesValues[valueIdx] = value
			valueIdx++
		}
	}
	bytesValues = bytesValues[:valueIdx]

	col.SetBytesDictData(bytesValues, bytesIndexes, presence)
	if err := parseColumnStatsIfPresent(name, ColumnTypeBytes, statsData, col, true); err != nil {
		return nil, err
	}
	return col, nil
}

func decodeDictionaryColumn(
	name string,
	typ ColumnType,
	expectedRows int,
	rd *sliceReader,
	statsData []byte,
	col *Column,
	flags columnEncodingFlags,
	a *arena.Arena,
) (*Column, error) {
	switch typ {
	case ColumnTypeString:
		return decodeStringDictionaryColumn(name, expectedRows, rd, statsData, col, flags, a)
	case ColumnTypeInt64:
		return decodeInt64DictionaryColumn(name, expectedRows, rd, statsData, col, flags, a)
	case ColumnTypeUint64:
		return decodeUint64DictionaryColumn(name, expectedRows, rd, statsData, col, flags, a)
	case ColumnTypeBool:
		return decodeBoolDictionaryColumn(name, expectedRows, rd, statsData, col, flags, a)
	case ColumnTypeFloat64:
		return decodeFloat64DictionaryColumn(name, expectedRows, rd, statsData, col, flags, a)
	case ColumnTypeBytes:
		if flags.isInline {
			return decodeInlineBytesColumn(name, expectedRows, rd, statsData, col, flags.isSparse, a)
		}
		return decodeBytesDictionaryColumn(name, expectedRows, rd, statsData, col, flags, a)
	default:
		return nil, fmt.Errorf("column %s unknown type %d", name, typ)
	}
}

func decodeStringDictionaryColumn(
	name string,
	expectedRows int,
	rd *sliceReader,
	statsData []byte,
	col *Column,
	flags columnEncodingFlags,
	a *arena.Arena,
) (*Column, error) {
	width, err := readIndexWidth(rd, name)
	if err != nil {
		return nil, err
	}
	dictBytes, err := readCompressedDict(rd, name)
	if err != nil {
		return nil, err
	}
	dictReader := &sliceReader{data: dictBytes}
	dictCountRaw, err := dictReader.readUint32()
	if err != nil {
		return nil, fmt.Errorf("column %s: %w", name, err)
	}

	dictCount, err := shared.SafeUint32ToInt(dictCountRaw)
	if err != nil {
		return nil, fmt.Errorf("column %s: invalid dictionary count: %w", name, err)
	}

	if err := shared.ValidateAllocationSize(dictCount, shared.MaxAllocDictionarySize, fmt.Sprintf("column %s dictionary", name)); err != nil { //nolint:govet
		return nil, err
	}

	// CRITICAL: Dictionary must use heap allocation, NOT arena allocation.
	// The dictionary is stored in the Column and must outlive the arena,
	// which gets reset between queries. Arena-allocated dictionaries cause
	// corruption when the arena is recycled.
	dict := make([]string, dictCount)
	for i := 0; i < dictCount; i++ {
		strLenRaw, err := dictReader.readUint32() //nolint:govet
		if err != nil {
			return nil, fmt.Errorf("column %s dict entry %d: %w", name, i, err)
		}

		strLen, err := shared.SafeUint32ToInt(strLenRaw)
		if err != nil {
			return nil, fmt.Errorf("column %s dict entry %d: invalid string length: %w", name, i, err)
		}

		if err := shared.ValidateAllocationSize(strLen, shared.MaxAllocStringLen, fmt.Sprintf("column %s dict string", name)); err != nil { //nolint:govet
			return nil, err
		}

		strBytes, err := dictReader.readBytes(strLen)
		if err != nil {
			return nil, fmt.Errorf("column %s dict entry %d: %w", name, i, err)
		}
		dict[i] = string(strBytes)
	}

	rowCount, present, err := readRowCountAndPresence(rd, name)
	if err != nil {
		return nil, err
	}
	values := col.ReuseIndexSlice(rowCount)
	if err := readIndexValues(rd, name, width, rowCount, present, flags.isRLE, flags.isSparse, values); err != nil {
		return nil, err
	}
	col.SetStringData(dict, values, present)
	if int(rowCount) != expectedRows {
		return nil, fmt.Errorf("column %s row count %d mismatch expected %d", name, rowCount, expectedRows)
	}
	if err := parseColumnStatsIfPresent(name, ColumnTypeString, statsData, col, false); err != nil {
		return nil, err
	}
	return col, nil
}

//
//nolint:dupl // Int64/Uint64 dictionary decoders are intentionally similar but type-specific
func decodeInt64DictionaryColumn(
	name string,
	expectedRows int,
	rd *sliceReader,
	statsData []byte,
	col *Column,
	flags columnEncodingFlags,
	a *arena.Arena,
) (*Column, error) {
	width, err := readIndexWidth(rd, name)
	if err != nil {
		return nil, err
	}
	dictBytes, err := readCompressedDict(rd, name)
	if err != nil {
		return nil, err
	}
	dictReader := &sliceReader{data: dictBytes}
	dictCountRaw, err := dictReader.readUint32()
	if err != nil {
		return nil, fmt.Errorf("column %s: %w", name, err)
	}

	dictCount, err := shared.SafeUint32ToInt(dictCountRaw)
	if err != nil {
		return nil, fmt.Errorf("column %s: invalid dictionary count: %w", name, err)
	}

	if err := shared.ValidateAllocationSize(dictCount, shared.MaxAllocDictionarySize, fmt.Sprintf("column %s dictionary", name)); err != nil { //nolint:govet
		return nil, err
	}

	// CRITICAL: Dictionary must use heap allocation, NOT arena allocation.
	// The dictionary is stored in the Column and must outlive the arena.
	dict := make([]int64, dictCount)
	for i := 0; i < dictCount; i++ {
		val, err := dictReader.readInt64() //nolint:govet
		if err != nil {
			return nil, fmt.Errorf("column %s: %w", name, err)
		}
		dict[i] = val
	}

	rowCount, present, err := readRowCountAndPresence(rd, name)
	if err != nil {
		return nil, err
	}
	values := col.ReuseIndexSlice(rowCount)
	if err := readIndexValues(rd, name, width, rowCount, present, flags.isRLE, flags.isSparse, values); err != nil {
		return nil, err
	}
	col.SetInt64Data(dict, values, present)
	if int(rowCount) != expectedRows {
		return nil, fmt.Errorf("column %s row count %d mismatch expected %d", name, rowCount, expectedRows)
	}
	if err := parseColumnStatsIfPresent(name, ColumnTypeInt64, statsData, col, false); err != nil {
		return nil, err
	}
	return col, nil
}

//
//nolint:dupl // Type-specific dictionary decoding; duplication is intentional for performance
func decodeUint64DictionaryColumn(
	name string,
	expectedRows int,
	rd *sliceReader,
	statsData []byte,
	col *Column,
	flags columnEncodingFlags,
	a *arena.Arena,
) (*Column, error) {
	width, err := readIndexWidth(rd, name)
	if err != nil {
		return nil, err
	}
	dictBytes, err := readCompressedDict(rd, name)
	if err != nil {
		return nil, err
	}
	dictReader := &sliceReader{data: dictBytes}
	dictCountRaw, err := dictReader.readUint32()
	if err != nil {
		return nil, fmt.Errorf("column %s: %w", name, err)
	}

	dictCount, err := shared.SafeUint32ToInt(dictCountRaw)
	if err != nil {
		return nil, fmt.Errorf("column %s: invalid dictionary count: %w", name, err)
	}

	if err := shared.ValidateAllocationSize(dictCount, shared.MaxAllocDictionarySize, fmt.Sprintf("column %s dictionary", name)); err != nil { //nolint:govet
		return nil, err
	}

	// CRITICAL: Dictionary must use heap allocation, NOT arena allocation.
	// The dictionary is stored in the Column and must outlive the arena.
	dict := make([]uint64, dictCount)
	for i := 0; i < dictCount; i++ {
		val, err := dictReader.readUint64() //nolint:govet
		if err != nil {
			return nil, fmt.Errorf("column %s: %w", name, err)
		}
		dict[i] = val
	}

	rowCount, present, err := readRowCountAndPresence(rd, name)
	if err != nil {
		return nil, err
	}
	values := col.ReuseIndexSlice(rowCount)
	if err := readIndexValues(rd, name, width, rowCount, present, flags.isRLE, flags.isSparse, values); err != nil {
		return nil, err
	}
	col.SetUint64Data(dict, values, present)
	if int(rowCount) != expectedRows {
		return nil, fmt.Errorf("column %s row count %d mismatch expected %d", name, rowCount, expectedRows)
	}
	if err := parseColumnStatsIfPresent(name, ColumnTypeUint64, statsData, col, false); err != nil {
		return nil, err
	}
	return col, nil
}

//
//nolint:dupl // Type-specific dictionary decoding; duplication is intentional for performance
func decodeBoolDictionaryColumn(
	name string,
	expectedRows int,
	rd *sliceReader,
	statsData []byte,
	col *Column,
	flags columnEncodingFlags,
	a *arena.Arena,
) (*Column, error) {
	width, err := readIndexWidth(rd, name)
	if err != nil {
		return nil, err
	}
	dictBytes, err := readCompressedDict(rd, name)
	if err != nil {
		return nil, err
	}
	dictReader := &sliceReader{data: dictBytes}
	dictCountRaw, err := dictReader.readUint32()
	if err != nil {
		return nil, fmt.Errorf("column %s: %w", name, err)
	}

	dictCount, err := shared.SafeUint32ToInt(dictCountRaw)
	if err != nil {
		return nil, fmt.Errorf("column %s: invalid dictionary count: %w", name, err)
	}

	if err := shared.ValidateAllocationSize(dictCount, shared.MaxAllocDictionarySize, fmt.Sprintf("column %s dictionary", name)); err != nil { //nolint:govet
		return nil, err
	}

	// CRITICAL: Dictionary must use heap allocation, NOT arena allocation.
	// The dictionary is stored in the Column and must outlive the arena.
	dict := make([]uint8, dictCount)
	for i := 0; i < dictCount; i++ {
		val, err := dictReader.readUint8() //nolint:govet
		if err != nil {
			return nil, fmt.Errorf("column %s: %w", name, err)
		}
		dict[i] = val
	}

	rowCount, present, err := readRowCountAndPresence(rd, name)
	if err != nil {
		return nil, err
	}
	values := col.ReuseIndexSlice(rowCount)
	if err := readIndexValues(rd, name, width, rowCount, present, flags.isRLE, flags.isSparse, values); err != nil {
		return nil, err
	}
	col.SetBoolData(dict, values, present)
	if int(rowCount) != expectedRows {
		return nil, fmt.Errorf("column %s row count %d mismatch expected %d", name, rowCount, expectedRows)
	}
	if err := parseColumnStatsIfPresent(name, ColumnTypeBool, statsData, col, false); err != nil {
		return nil, err
	}
	return col, nil
}

//
//nolint:dupl // Type-specific dictionary decoding; duplication is intentional for performance
func decodeFloat64DictionaryColumn(
	name string,
	expectedRows int,
	rd *sliceReader,
	statsData []byte,
	col *Column,
	flags columnEncodingFlags,
	a *arena.Arena,
) (*Column, error) {
	width, err := readIndexWidth(rd, name)
	if err != nil {
		return nil, err
	}
	dictBytes, err := readCompressedDict(rd, name)
	if err != nil {
		return nil, err
	}
	dictReader := &sliceReader{data: dictBytes}
	dictCountRaw, err := dictReader.readUint32()
	if err != nil {
		return nil, fmt.Errorf("column %s: %w", name, err)
	}

	dictCount, err := shared.SafeUint32ToInt(dictCountRaw)
	if err != nil {
		return nil, fmt.Errorf("column %s: invalid dictionary count: %w", name, err)
	}

	if err := shared.ValidateAllocationSize(dictCount, shared.MaxAllocDictionarySize, fmt.Sprintf("column %s dictionary", name)); err != nil { //nolint:govet
		return nil, err
	}

	// CRITICAL: Dictionary must use heap allocation, NOT arena allocation.
	// The dictionary is stored in the Column and must outlive the arena.
	dict := make([]float64, dictCount)
	for i := 0; i < dictCount; i++ {
		val, err := dictReader.readFloat64() //nolint:govet
		if err != nil {
			return nil, fmt.Errorf("column %s: %w", name, err)
		}
		dict[i] = val
	}

	rowCount, present, err := readRowCountAndPresence(rd, name)
	if err != nil {
		return nil, err
	}
	values := col.ReuseIndexSlice(rowCount)
	if err := readIndexValues(rd, name, width, rowCount, present, flags.isRLE, flags.isSparse, values); err != nil {
		return nil, err
	}
	col.SetFloat64Data(dict, values, present)
	if int(rowCount) != expectedRows {
		return nil, fmt.Errorf("column %s row count %d mismatch expected %d", name, rowCount, expectedRows)
	}
	if err := parseColumnStatsIfPresent(name, ColumnTypeFloat64, statsData, col, false); err != nil {
		return nil, err
	}
	return col, nil
}

func decodeInlineBytesColumn(
	name string,
	expectedRows int,
	rd *sliceReader,
	statsData []byte,
	col *Column,
	isSparse bool,
	a *arena.Arena,
) (*Column, error) {
	rowCount, present, err := readRowCountAndPresence(rd, name)
	if err != nil {
		return nil, err
	}
	values := makeSlice[[]byte](a, int(rowCount))
	if isSparse {
		presentCount, err := rd.readUint32()
		if err != nil {
			return nil, fmt.Errorf("column %s: %w", name, err)
		}
		readCount := 0
		for i := 0; i < int(rowCount); i++ {
			if !shared.IsBitSet(present, i) {
				continue
			}
			l, err := rd.readUint32()
			if err != nil {
				return nil, fmt.Errorf("column %s: %w", name, err)
			}
			var val []byte
			if l > 0 {
				val, err = rd.readBytes(int(l))
				if err != nil {
					return nil, fmt.Errorf("column %s: %w", name, err)
				}
				cp := makeSlice[byte](a, len(val))
				copy(cp, val)
				val = cp
			}
			values[i] = val
			readCount++
		}
		if readCount != int(presentCount) {
			return nil, fmt.Errorf(
				"column %s: sparse inline count mismatch: read %d expected %d",
				name,
				readCount,
				presentCount,
			)
		}
	} else {
		for i := 0; i < int(rowCount); i++ {
			l, err := rd.readUint32()
			if err != nil {
				return nil, fmt.Errorf("column %s: %w", name, err)
			}
			var val []byte
			if l > 0 {
				val, err = rd.readBytes(int(l))
				if err != nil {
					return nil, fmt.Errorf("column %s: %w", name, err)
				}
				cp := makeSlice[byte](a, len(val))
				copy(cp, val)
				val = cp
			}
			values[i] = val
		}
	}

	col.SetBytesInlineData(values, present)
	if int(rowCount) != expectedRows {
		return nil, fmt.Errorf("column %s row count %d mismatch expected %d", name, rowCount, expectedRows)
	}
	if err := parseColumnStatsIfPresent(name, ColumnTypeBytes, statsData, col, false); err != nil {
		return nil, err
	}
	return col, nil
}

func decodeBytesDictionaryColumn(
	name string,
	expectedRows int,
	rd *sliceReader,
	statsData []byte,
	col *Column,
	flags columnEncodingFlags,
	a *arena.Arena,
) (*Column, error) {
	width, err := readIndexWidth(rd, name)
	if err != nil {
		return nil, err
	}
	dictBytes, err := readCompressedDict(rd, name)
	if err != nil {
		return nil, err
	}
	dictReader := &sliceReader{data: dictBytes}
	dictCountRaw, err := dictReader.readUint32()
	if err != nil {
		return nil, fmt.Errorf("column %s: %w", name, err)
	}

	dictCount, err := shared.SafeUint32ToInt(dictCountRaw)
	if err != nil {
		return nil, fmt.Errorf("column %s: invalid dictionary count: %w", name, err)
	}

	if err := shared.ValidateAllocationSize(dictCount, shared.MaxAllocDictionarySize, fmt.Sprintf("column %s dictionary", name)); err != nil { //nolint:govet
		return nil, err
	}

	// CRITICAL: Dictionary must use heap allocation, NOT arena allocation.
	// The dictionary is stored in the Column and must outlive the arena.
	dict := make([][]byte, dictCount)
	for i := 0; i < dictCount; i++ {
		lengthRaw, err := dictReader.readUint32() //nolint:govet
		if err != nil {
			return nil, fmt.Errorf("column %s dict entry %d: %w", name, i, err)
		}

		length, err := shared.SafeUint32ToInt(lengthRaw)
		if err != nil {
			return nil, fmt.Errorf("column %s dict entry %d: invalid bytes length: %w", name, i, err)
		}

		if err := shared.ValidateAllocationSize(length, shared.MaxAllocStringLen, fmt.Sprintf("column %s dict bytes", name)); err != nil { //nolint:govet
			return nil, err
		}

		valBytes, err := dictReader.readBytes(length)
		if err != nil {
			return nil, fmt.Errorf("column %s dict entry %d: %w", name, i, err)
		}
		// Both the dict slice and individual byte slices must be heap-allocated
		cp := make([]byte, len(valBytes))
		copy(cp, valBytes)
		dict[i] = cp
	}

	rowCount, present, err := readRowCountAndPresence(rd, name)
	if err != nil {
		return nil, err
	}
	values := col.ReuseIndexSlice(rowCount)
	if err := readIndexValues(rd, name, width, rowCount, present, flags.isRLE, flags.isSparse, values); err != nil {
		return nil, err
	}
	col.SetBytesDictData(dict, values, present)
	if int(rowCount) != expectedRows {
		return nil, fmt.Errorf("column %s row count %d mismatch expected %d", name, rowCount, expectedRows)
	}
	if err := parseColumnStatsIfPresent(name, ColumnTypeBytes, statsData, col, false); err != nil {
		return nil, err
	}
	return col, nil
}

func readIndexWidth(rd *sliceReader, name string) (uint8, error) {
	width, err := rd.readUint8()
	if err != nil {
		return 0, fmt.Errorf("column %s: %w", name, err)
	}
	if width != 1 && width != 2 && width != 4 {
		return 0, fmt.Errorf("column %s: invalid index width %d", name, width)
	}
	return width, nil
}

func readCompressedDict(rd *sliceReader, name string) ([]byte, error) {
	dictLen, err := rd.readUint32()
	if err != nil {
		return nil, fmt.Errorf("column %s: %w", name, err)
	}
	if int(dictLen) > rd.remaining() {
		return nil, fmt.Errorf(
			"column %s: compressed dictionary length %d exceeds remaining %d",
			name,
			dictLen,
			rd.remaining(),
		)
	}
	compressed, err := rd.readBytes(int(dictLen))
	if err != nil {
		return nil, fmt.Errorf("column %s: %w", name, err)
	}
	dictBytes, err := zstdDecoder.DecodeAll(compressed, nil)
	if err != nil {
		return nil, fmt.Errorf("column %s: failed to decompress dictionary: %w", name, err)
	}
	return dictBytes, nil
}

func readRowCountAndPresence(rd *sliceReader, name string) (uint32, []byte, error) {
	rowCount, err := rd.readUint32()
	if err != nil {
		return 0, nil, fmt.Errorf("column %s: %w", name, err)
	}
	present, err := readPresence(rd, int(rowCount))
	if err != nil {
		return 0, nil, fmt.Errorf("column %s: %w", name, err)
	}
	return rowCount, present, nil
}

func readIndexValues(
	rd *sliceReader,
	name string,
	width uint8,
	rowCount uint32,
	present []byte,
	isRLE bool,
	isSparse bool,
	values []uint32,
) error {
	if isRLE {
		indexCount, err := rd.readUint32()
		if err != nil {
			return fmt.Errorf("column %s: %w", name, err)
		}
		rleLen, err := rd.readUint32()
		if err != nil {
			return fmt.Errorf("column %s: %w", name, err)
		}
		rleBytes, err := rd.readBytes(int(rleLen))
		if err != nil {
			return fmt.Errorf("column %s: %w", name, err)
		}
		decodedIndexes, err := shared.DecodeIndexRLE(rleBytes, int(indexCount))
		if err != nil {
			return fmt.Errorf("column %s: RLE decode error: %w", name, err)
		}
		if isSparse {
			idx := 0
			for i := 0; i < int(rowCount); i++ {
				if shared.IsBitSet(present, i) {
					if idx >= len(decodedIndexes) {
						return fmt.Errorf("column %s: not enough RLE indices", name)
					}
					values[i] = decodedIndexes[idx]
					idx++
				}
			}
			return nil
		}
		copy(values, decodedIndexes)
		return nil
	}

	if isSparse {
		presentCount, err := rd.readUint32()
		if err != nil {
			return fmt.Errorf("column %s: %w", name, err)
		}
		readCount := 0
		for i := 0; i < int(rowCount); i++ {
			if !shared.IsBitSet(present, i) {
				continue
			}
			val, err := rd.readFixedWidth(width)
			if err != nil {
				return fmt.Errorf("column %s: %w", name, err)
			}
			values[i] = val
			readCount++
		}
		if readCount != int(presentCount) {
			return fmt.Errorf(
				"column %s: sparse index count mismatch: read %d expected %d",
				name,
				readCount,
				presentCount,
			)
		}
		return nil
	}

	for i := 0; i < int(rowCount); i++ {
		val, err := rd.readFixedWidth(width)
		if err != nil {
			return fmt.Errorf("column %s: %w", name, err)
		}
		values[i] = val
	}
	return nil
}

func parseColumnStatsIfPresent(name string, typ ColumnType, statsData []byte, col *Column, statsLabel bool) error {
	if statsData == nil {
		return nil
	}
	stats, err := parseColumnStats(typ, statsData)
	if err != nil {
		if statsLabel {
			return fmt.Errorf("column %s stats: %w", name, err)
		}
		return fmt.Errorf("column %s: %w", name, err)
	}
	col.Stats = stats
	return nil
}

func readPrefixDictionary(rd *sliceReader, name string, a *arena.Arena) ([]string, error) {
	prefixDictLen, err := rd.readUint32()
	if err != nil {
		return nil, fmt.Errorf("column %s: %w", name, err)
	}
	compressedPrefixDict, err := rd.readBytes(int(prefixDictLen))
	if err != nil {
		return nil, fmt.Errorf("column %s: %w", name, err)
	}
	prefixDictBytes, err := zstdDecoder.DecodeAll(compressedPrefixDict, nil)
	if err != nil {
		return nil, fmt.Errorf("column %s: failed to decompress prefix dict: %w", name, err)
	}

	prefixReader := &sliceReader{data: prefixDictBytes}
	prefixCount, err := prefixReader.readUint32()
	if err != nil {
		return nil, fmt.Errorf("column %s: %w", name, err)
	}
	prefixList := makeSlice[string](a, int(prefixCount))
	for i := 0; i < int(prefixCount); i++ {
		prefixLen, err := prefixReader.readUint32()
		if err != nil {
			return nil, fmt.Errorf("column %s: %w", name, err)
		}
		prefixData, err := prefixReader.readBytes(int(prefixLen))
		if err != nil {
			return nil, fmt.Errorf("column %s: %w", name, err)
		}
		prefixList[i] = string(prefixData)
	}
	return prefixList, nil
}

func readSuffixReader(rd *sliceReader, name string) (uint8, *sliceReader, error) {
	suffixDataLen, err := rd.readUint32()
	if err != nil {
		return 0, nil, fmt.Errorf("column %s: %w", name, err)
	}
	compressedSuffixes, err := rd.readBytes(int(suffixDataLen))
	if err != nil {
		return 0, nil, fmt.Errorf("column %s: %w", name, err)
	}
	suffixBytes, err := zstdDecoder.DecodeAll(compressedSuffixes, nil)
	if err != nil {
		return 0, nil, fmt.Errorf("column %s: failed to decompress suffixes: %w", name, err)
	}

	suffixReader := &sliceReader{data: suffixBytes}
	prefixIndexWidth, err := suffixReader.readUint8()
	if err != nil {
		return 0, nil, fmt.Errorf("column %s: %w", name, err)
	}
	return prefixIndexWidth, suffixReader, nil
}

// decodeDeltaDictionaryBytesColumn decodes a bytes column with dictionary + delta-encoded indices.
// This is optimized for columns like trace:id where spans are sorted by value.
func decodeDeltaDictionaryBytesColumn(
	name string,
	rd *sliceReader,
	statsData []byte,
	col *Column,
	flags columnEncodingFlags,
	a *arena.Arena,
) (*Column, error) {
	// Read dictionary width (not used for delta encoding, but needed to advance reader)
	_, err := readIndexWidth(rd, name)
	if err != nil {
		return nil, err
	}

	// Read and decompress dictionary
	dictBytes, err := readCompressedDict(rd, name)
	if err != nil {
		return nil, err
	}
	dictReader := &sliceReader{data: dictBytes}
	dictCountRaw, err := dictReader.readUint32()
	if err != nil {
		return nil, fmt.Errorf("column %s: %w", name, err)
	}

	dictCount, err := shared.SafeUint32ToInt(dictCountRaw)
	if err != nil {
		return nil, fmt.Errorf("column %s: invalid dictionary count: %w", name, err)
	}

	if err := shared.ValidateAllocationSize(dictCount, shared.MaxAllocDictionarySize, fmt.Sprintf("column %s dictionary", name)); err != nil { //nolint:govet
		return nil, err
	}

	// CRITICAL: Dictionary must use heap allocation, NOT arena allocation
	dict := make([][]byte, dictCount)
	for i := 0; i < dictCount; i++ {
		lengthRaw, err := dictReader.readUint32() //nolint:govet
		if err != nil {
			return nil, fmt.Errorf("column %s dict entry %d: %w", name, i, err)
		}

		length, err := shared.SafeUint32ToInt(lengthRaw)
		if err != nil {
			return nil, fmt.Errorf("column %s dict entry %d: invalid bytes length: %w", name, i, err)
		}

		if err := shared.ValidateAllocationSize(length, shared.MaxAllocStringLen, fmt.Sprintf("column %s dict bytes", name)); err != nil { //nolint:govet
			return nil, err
		}

		valBytes, err := dictReader.readBytes(length)
		if err != nil {
			return nil, fmt.Errorf("column %s dict entry %d: %w", name, i, err)
		}
		cp := make([]byte, len(valBytes))
		copy(cp, valBytes)
		dict[i] = cp
	}

	// Read row count and presence
	rowCount, present, err := readRowCountAndPresence(rd, name)
	if err != nil {
		return nil, err
	}

	// Read and decompress delta-encoded indices
	deltaLen, err := rd.readUint32()
	if err != nil {
		return nil, fmt.Errorf("column %s: %w", name, err)
	}
	compressedDeltas, err := rd.readBytes(int(deltaLen))
	if err != nil {
		return nil, fmt.Errorf("column %s: %w", name, err)
	}
	deltaBytes, err := zstdDecoder.DecodeAll(compressedDeltas, nil)
	if err != nil {
		return nil, fmt.Errorf("column %s: failed to decompress deltas: %w", name, err)
	}

	// Decode delta-encoded indices
	deltaReader := &sliceReader{data: deltaBytes}
	indexes := makeSlice[uint32](a, int(rowCount))

	if flags.isSparse {
		// Sparse: only present values have delta-encoded indices
		var prevIdx int32
		for i := 0; i < int(rowCount); i++ {
			if shared.IsBitSet(present, i) {
				delta, err := deltaReader.readInt32()
				if err != nil {
					return nil, fmt.Errorf("column %s: %w", name, err)
				}
				currIdx := prevIdx + delta
				// Validate accumulated index is non-negative and within dictionary bounds
				if currIdx < 0 {
					return nil, fmt.Errorf("column %s: invalid negative index %d at position %d", name, currIdx, i)
				}
				if currIdx >= int32(dictCount) { //nolint:gosec
					return nil, fmt.Errorf(
						"column %s: index %d exceeds dictionary size %d at position %d",
						name,
						currIdx,
						dictCount,
						i,
					)
				}
				indexes[i] = uint32(currIdx)
				prevIdx = currIdx
			}
		}
	} else {
		// Dense: all values (including nulls) have delta-encoded indices
		var prevIdx int32
		for i := 0; i < int(rowCount); i++ {
			delta, err := deltaReader.readInt32()
			if err != nil {
				return nil, fmt.Errorf("column %s: %w", name, err)
			}
			currIdx := prevIdx + delta
			// Validate accumulated index is non-negative and within dictionary bounds
			if currIdx < 0 {
				return nil, fmt.Errorf("column %s: invalid negative index %d at position %d", name, currIdx, i)
			}
			if currIdx >= int32(dictCount) { //nolint:gosec
				return nil, fmt.Errorf("column %s: index %d exceeds dictionary size %d at position %d", name, currIdx, dictCount, i)
			}
			indexes[i] = uint32(currIdx)
			prevIdx = currIdx
		}
	}

	col.SetBytesDictData(dict, indexes, present)
	if err := parseColumnStatsIfPresent(name, ColumnTypeBytes, statsData, col, false); err != nil {
		return nil, err
	}
	return col, nil
}
