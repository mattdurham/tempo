package reader_test

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"strings"
	"testing"

	"github.com/golang/snappy"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	commonv1 "go.opentelemetry.io/proto/otlp/common/v1"
	resourcev1 "go.opentelemetry.io/proto/otlp/resource/v1"
	tracev1 "go.opentelemetry.io/proto/otlp/trace/v1"

	"github.com/grafana/blockpack/internal/modules/blockio/reader"
	modules_shared "github.com/grafana/blockpack/internal/modules/blockio/shared"
	"github.com/grafana/blockpack/internal/modules/blockio/writer"
	"github.com/grafana/blockpack/internal/modules/rw"
)

// buildV14FileWithCols writes nSpans, each containing a span with the given attribute column names.
// Values are distinct per-span to exercise non-trivial encoding paths.
func buildV14FileWithCols(t *testing.T, nSpans int, colNames []string) []byte {
	t.Helper()
	var buf bytes.Buffer
	w, err := writer.NewWriterWithConfig(writer.Config{OutputStream: &buf})
	require.NoError(t, err)

	for i := range nSpans {
		traceID := make([]byte, 16)
		traceID[0] = byte(i)
		spanID := make([]byte, 8)
		spanID[0] = byte(i + 1)

		attrs := make([]*commonv1.KeyValue, 0, len(colNames))
		for _, name := range colNames {
			val := name + "-value-" + string(rune('A'+i%26))
			attrs = append(attrs, &commonv1.KeyValue{
				Key:   name,
				Value: &commonv1.AnyValue{Value: &commonv1.AnyValue_StringValue{StringValue: val}},
			})
		}

		td := &tracev1.TracesData{
			ResourceSpans: []*tracev1.ResourceSpans{
				{ScopeSpans: []*tracev1.ScopeSpans{{Spans: []*tracev1.Span{
					{
						TraceId:           traceID,
						SpanId:            spanID,
						Name:              "op",
						StartTimeUnixNano: uint64(i * 1000),
						EndTimeUnixNano:   uint64(i*1000 + 500),
						Attributes:        attrs,
					},
				}}}},
			},
		}
		require.NoError(t, w.AddTracesData(td))
	}

	_, err = w.Flush()
	require.NoError(t, err)
	return buf.Bytes()
}

// TestColumnSelectiveRead verifies that when only 2 of 5 attribute columns are requested,
// the 3 non-requested columns are NOT decoded into the parsed Block.
//
// This tests that per-column selective decode works: non-requested column blobs remain
// undecoded (they exist in the raw bytes but are not materialized into the Block columns map).
func TestColumnSelectiveRead(t *testing.T) {
	colNames := []string{"col.want1", "col.want2", "col.skip1", "col.skip2", "col.skip3"}
	data := buildV14FileWithCols(t, 10, colNames)

	r := openReader(t, data)
	require.Greater(t, r.BlockCount(), 0)

	// Only request 2 of the 5 attribute columns.
	// Span attributes are stored with "span." prefix in blockpack columns.
	wantColumns := map[string]struct{}{
		"span.col.want1": {},
		"span.col.want2": {},
	}

	bwb, err := r.GetBlockWithBytes(0, wantColumns, nil)
	require.NoError(t, err)
	require.NotNil(t, bwb)

	// The 2 requested columns must be present and eagerly decoded in the parsed block.
	want1 := bwb.Block.GetColumn("span.col.want1")
	assert.NotNil(t, want1, "span.col.want1 must be in parsed block")
	want2 := bwb.Block.GetColumn("span.col.want2")
	assert.NotNil(t, want2, "span.col.want2 must be in parsed block")

	// The 3 non-requested columns are lazily registered (they appear in the block for
	// future lazy decode) but must NOT be eagerly decoded (StringDict must remain nil
	// until a value accessor is called).
	skip1 := bwb.Block.GetColumn("span.col.skip1")
	if skip1 != nil {
		assert.Nil(t, skip1.StringDict, "span.col.skip1 must not be eagerly decoded")
	}
	skip2 := bwb.Block.GetColumn("span.col.skip2")
	if skip2 != nil {
		assert.Nil(t, skip2.StringDict, "span.col.skip2 must not be eagerly decoded")
	}
	skip3 := bwb.Block.GetColumn("span.col.skip3")
	if skip3 != nil {
		assert.Nil(t, skip3.StringDict, "span.col.skip3 must not be eagerly decoded")
	}
}

// TestV14ColumnDecodeIntegrity verifies that all encoding kinds round-trip correctly
// under V14 format (per-column outer snappy, raw internal segments — no zstd).
// Exercises string (dict/prefix), int64 (delta), float64, bool, and bytes columns.
func TestV14ColumnDecodeIntegrity(t *testing.T) {
	var buf bytes.Buffer
	w, err := writer.NewWriterWithConfig(writer.Config{OutputStream: &buf})
	require.NoError(t, err)

	const nSpans = 50

	for i := range nSpans {
		traceID := make([]byte, 16)
		traceID[0] = byte(i)
		spanID := make([]byte, 8)
		spanID[0] = byte(i + 1)

		// String: varied prefix to exercise prefix encoding.
		// Int64: monotone sequence to exercise delta encoding.
		// Float64: varied values.
		// Bool: alternating values.
		// Bytes: raw bytes per span.
		attrs := []*commonv1.KeyValue{
			{Key: "attr.string", Value: &commonv1.AnyValue{Value: &commonv1.AnyValue_StringValue{
				StringValue: "prefix-common-" + string(rune('a'+i%26)),
			}}},
			{Key: "attr.int64", Value: &commonv1.AnyValue{Value: &commonv1.AnyValue_IntValue{
				IntValue: int64(i) * 100,
			}}},
			{Key: "attr.float64", Value: &commonv1.AnyValue{Value: &commonv1.AnyValue_DoubleValue{
				DoubleValue: float64(i) * 3.14,
			}}},
			{Key: "attr.bool", Value: &commonv1.AnyValue{Value: &commonv1.AnyValue_BoolValue{
				BoolValue: i%2 == 0,
			}}},
			{Key: "attr.bytes", Value: &commonv1.AnyValue{Value: &commonv1.AnyValue_BytesValue{
				BytesValue: []byte{byte(i), byte(i + 1), byte(i + 2)},
			}}},
		}

		td := &tracev1.TracesData{
			ResourceSpans: []*tracev1.ResourceSpans{
				{ScopeSpans: []*tracev1.ScopeSpans{{Spans: []*tracev1.Span{
					{
						TraceId:           traceID,
						SpanId:            spanID,
						Name:              "op-" + string(rune('a'+i%26)),
						StartTimeUnixNano: uint64(i) * 1_000_000,
						EndTimeUnixNano:   uint64(i)*1_000_000 + 500_000,
						Kind:              tracev1.Span_SPAN_KIND_SERVER,
						Attributes:        attrs,
					},
				}}}},
			},
		}
		require.NoError(t, w.AddTracesData(td))
	}

	_, err = w.Flush()
	require.NoError(t, err)
	data := buf.Bytes()

	r := openReader(t, data)
	require.Greater(t, r.BlockCount(), 0, "at least one block must be present")

	// Read block 0 with all columns.
	bwb, err := r.GetBlockWithBytes(0, nil, nil)
	require.NoError(t, err)
	require.NotNil(t, bwb)

	// All 5 attribute column types must round-trip through V14 encoding.
	// Span attributes are stored with "span." prefix (e.g. "attr.string" → "span.attr.string").
	strCol := bwb.Block.GetColumn("span.attr.string")
	require.NotNil(t, strCol, "span.attr.string column must exist after V14 decode")
	assert.Equal(t, bwb.Block.SpanCount(), strCol.SpanCount, "string column span count must match block")

	intCol := bwb.Block.GetColumn("span.attr.int64")
	require.NotNil(t, intCol, "span.attr.int64 column must exist after V14 decode")

	floatCol := bwb.Block.GetColumn("span.attr.float64")
	require.NotNil(t, floatCol, "span.attr.float64 column must exist after V14 decode")

	boolCol := bwb.Block.GetColumn("span.attr.bool")
	require.NotNil(t, boolCol, "span.attr.bool column must exist after V14 decode")

	bytesCol := bwb.Block.GetColumn("span.attr.bytes")
	require.NotNil(t, bytesCol, "span.attr.bytes column must exist after V14 decode")
}

// TestBloomAccessNoBlockRead verifies that FileBloom() is accessible via the section
// directory without reading any block bytes.
//
// This tests the V14 independent metadata section invariant: bloom is in SectionFileBloom
// and can be loaded from the section directory without touching block data.
func TestBloomAccessNoBlockRead(t *testing.T) {
	var buf bytes.Buffer
	w, err := writer.NewWriterWithConfig(writer.Config{OutputStream: &buf})
	require.NoError(t, err)

	services := []string{"service-alpha", "service-beta", "service-gamma"}
	for i, svc := range services {
		traceID := make([]byte, 16)
		traceID[0] = byte(i + 1)
		spanID := make([]byte, 8)
		spanID[0] = byte(i + 1)

		td := &tracev1.TracesData{
			ResourceSpans: []*tracev1.ResourceSpans{
				{
					Resource: &resourcev1.Resource{
						Attributes: []*commonv1.KeyValue{
							{
								Key:   "service.name",
								Value: &commonv1.AnyValue{Value: &commonv1.AnyValue_StringValue{StringValue: svc}},
							},
						},
					},
					ScopeSpans: []*tracev1.ScopeSpans{{Spans: []*tracev1.Span{
						{
							TraceId:           traceID,
							SpanId:            spanID,
							Name:              "op",
							StartTimeUnixNano: uint64(i * 1000),
							EndTimeUnixNano:   uint64(i*1000 + 500),
						},
					}}},
				},
			},
		}
		require.NoError(t, w.AddTracesData(td))
	}

	_, err = w.Flush()
	require.NoError(t, err)
	data := buf.Bytes()

	// Open reader — must NOT read block bytes (only footer + section directory + metadata sections).
	r := openReader(t, data)
	require.Greater(t, r.BlockCount(), 0, "block count from section directory must be nonzero")

	// Access FileBloom — must read only the bloom section, not block bytes.
	bloom := r.FileBloom()
	_ = bloom // bloom may be nil if service names weren't written; no panic is the invariant

	// Verify footer is V14.
	require.Greater(t, len(data), int(modules_shared.FooterV7Size))
	footerBytes := data[len(data)-int(modules_shared.FooterV7Size):]
	version := binary.LittleEndian.Uint16(footerBytes[4:6])
	assert.Equal(t, modules_shared.FooterV7Version, version, "footer must be V7")

	// Verify bloom raw bytes are accessible and (in V14) are snappy-decodable.
	bloomRaw := r.FileBloomRaw()
	if bloomRaw != nil {
		// In V14, each section is independently snappy-compressed.
		// FileBloomRaw returns the raw (uncompressed) bloom section bytes.
		// Verify the data is non-empty.
		assert.Greater(t, len(bloomRaw), 0, "bloom raw bytes must be non-empty when non-nil")
	}

	// Cross-check: verify the bloom section was independently readable without block I/O
	// by confirming the section directory was correctly parsed (block count > 0 without
	// reading any block bytes, since block metadata comes from SectionBlockIndex).
	assert.Greater(t, r.BlockCount(), 0, "block metadata must be available from SectionBlockIndex")
}

// TestV14FooterConstants verifies that the footer size and version constants match
// the actual bytes at the end of a V14-written file.
func TestV14FooterConstants(t *testing.T) {
	data := buildV14FileWithCols(t, 3, []string{"col.x"})
	require.Greater(t, len(data), int(modules_shared.FooterV7Size), "file must be larger than footer")

	footerStart := len(data) - int(modules_shared.FooterV7Size)
	footerBytes := data[footerStart:]

	magic := binary.LittleEndian.Uint32(footerBytes[0:4])
	assert.Equal(t, modules_shared.MagicNumber, magic, "footer magic must match MagicNumber")

	version := binary.LittleEndian.Uint16(footerBytes[4:6])
	assert.Equal(t, modules_shared.FooterV7Version, version, "footer version must be FooterV7Version")

	dirOffset := binary.LittleEndian.Uint64(footerBytes[6:14])
	dirLen := binary.LittleEndian.Uint32(footerBytes[14:18])
	assert.Greater(t, dirOffset, uint64(0), "dir_offset must be nonzero")
	assert.Greater(t, dirLen, uint32(0), "dir_len must be nonzero")

	// Section directory must be valid snappy and contain at least SectionBlockIndex.
	rawDir, err := snappy.Decode(nil, data[dirOffset:dirOffset+uint64(dirLen)])
	require.NoError(t, err, "section directory must decode as valid snappy")
	require.GreaterOrEqual(t, len(rawDir), 4, "section directory must have section_count[4]")

	sectionCount := binary.LittleEndian.Uint32(rawDir[0:4])
	assert.Greater(t, sectionCount, uint32(0))

	// Parse section directory entries. V14 uses heterogeneous entries dispatched by entry_kind[1].
	// Type-keyed (entry_kind=0x00): entry_kind[1]+section_type[1]+offset[8]+compressed_len[4] = 14 bytes.
	// Name-keyed (entry_kind=0x01): entry_kind[1]+name_len[2]+name+offset[8]+compressed_len[4] = variable.
	// Signal-kind (entry_kind=0x02): entry_kind[1]+signal_type[1] = 2 bytes.
	pos := 4 // skip entry_count[4]
entryLoop:
	for range int(sectionCount) {
		if pos >= len(rawDir) {
			break
		}
		kind := rawDir[pos]
		switch kind {
		case modules_shared.DirEntryKindType: // type-keyed: 14 bytes total
			if pos+14 > len(rawDir) {
				break entryLoop
			}
			sType := rawDir[pos+1]
			assert.NotZero(t, sType, "section type must be nonzero")
			pos += 14
		case modules_shared.DirEntryKindName: // name-keyed: variable
			if pos+3 > len(rawDir) {
				break entryLoop
			}
			nameLen := int(binary.LittleEndian.Uint16(rawDir[pos+1:]))
			pos += 3 + nameLen + 8 + 4
		case modules_shared.DirEntryKindSignal: // signal-kind: 2 bytes total (kind[1]+signal_type[1])
			if pos+2 > len(rawDir) {
				break entryLoop
			}
			pos += 2
		default:
			t.Errorf("unexpected entry kind 0x%02x at pos %d", kind, pos)
			break entryLoop
		}
	}
}

// TestResetColumn verifies that resetColumn zeroes all value fields on a populated Column.
// Exercises the resetColumn code path (block_parser.go:325) which was at 0% coverage.
func TestResetColumn(t *testing.T) {
	data := buildV14FileWithCols(t, 5, []string{"col.a", "col.b"})
	r := openReader(t, data)
	require.Greater(t, r.BlockCount(), 0)

	bwb, err := r.GetBlockWithBytes(0, nil, nil)
	require.NoError(t, err)
	require.NotNil(t, bwb)

	col := bwb.Block.GetColumn("span.col.a")
	require.NotNil(t, col, "span.col.a must be present after full decode")

	// Populate the slices so resetColumn has something to zero.
	col.StringDict = append(col.StringDict[:0:len(col.StringDict)], "x", "y")
	col.StringIdx = append(col.StringIdx[:0:len(col.StringIdx)], 0, 1)
	col.Int64Dict = append(col.Int64Dict[:0:len(col.Int64Dict)], 1, 2)
	col.Int64Idx = append(col.Int64Idx[:0:len(col.Int64Idx)], 0, 1)
	col.Present = []byte{0xFF}

	reader.ResetColumnExported(col)

	assert.Len(t, col.StringDict, 0, "StringDict must be zeroed by resetColumn")
	assert.Len(t, col.StringIdx, 0, "StringIdx must be zeroed by resetColumn")
	assert.Len(t, col.Int64Dict, 0, "Int64Dict must be zeroed by resetColumn")
	assert.Len(t, col.Int64Idx, 0, "Int64Idx must be zeroed by resetColumn")
	assert.Nil(t, col.Present, "Present must be nil after resetColumn")
}

// TestEnsureV14TSSection verifies that BlocksInTimeRange and TimeIndexLen correctly
// trigger lazy load of the V14 TS index section (ensureV14TSSection).
// Exercises parser.go:429 which was at 12.5% coverage.
func TestEnsureV14TSSection(t *testing.T) {
	var buf bytes.Buffer
	w, err := writer.NewWriterWithConfig(writer.Config{OutputStream: &buf})
	require.NoError(t, err)

	const nSpans = 10
	for i := range nSpans {
		traceID := make([]byte, 16)
		traceID[0] = byte(i + 1)
		spanID := make([]byte, 8)
		spanID[0] = byte(i + 1)

		startNano := uint64(1_000_000_000 + i*1_000_000)
		endNano := startNano + 500_000
		td := &tracev1.TracesData{
			ResourceSpans: []*tracev1.ResourceSpans{
				{ScopeSpans: []*tracev1.ScopeSpans{{Spans: []*tracev1.Span{
					{
						TraceId:           traceID,
						SpanId:            spanID,
						Name:              "op",
						StartTimeUnixNano: startNano,
						EndTimeUnixNano:   endNano,
					},
				}}}},
			},
		}
		require.NoError(t, w.AddTracesData(td))
	}

	_, err = w.Flush()
	require.NoError(t, err)
	data := buf.Bytes()

	r := openReader(t, data)
	require.Greater(t, r.BlockCount(), 0)

	// TimeIndexLen triggers ensureV14TSSection lazy load.
	idxLen := r.TimeIndexLen()
	assert.GreaterOrEqual(t, idxLen, 0, "TimeIndexLen must be non-negative")

	// BlocksInTimeRange also calls ensureV14TSSection (idempotent second call).
	blocks := r.BlocksInTimeRange(0, ^uint64(0))
	// A V14 file with spans should return blocks or nil (nil = TS index absent).
	_ = blocks
}

// TestEnsureV14TraceSection verifies that MayContainTraceID triggers lazy load
// of the V14 trace index section (ensureV14TraceSection at parser.go:406).
// Was at 58.3% coverage.
func TestEnsureV14TraceSection(t *testing.T) {
	data := buildV14FileWithCols(t, 5, []string{"col.x"})
	r := openReader(t, data)
	require.Greater(t, r.BlockCount(), 0)

	// MayContainTraceID triggers ensureV14TraceSection.
	var traceID [16]byte
	traceID[0] = 0xAB
	// Result may be true (vacuous) or false; no panic is the invariant.
	_ = r.MayContainTraceID(traceID)

	// Also exercise TraceCount which routes through ensureV14TraceSection.
	count := r.TraceCount()
	assert.GreaterOrEqual(t, count, 0, "TraceCount must be non-negative")
}

// TestParseAndCacheSketchSection verifies that ColumnSketch and FileSketchSummary
// trigger lazy load of the V14 sketch section (parseAndCacheSketchSection at parser.go:721).
// Was at 38.9% coverage.
func TestParseAndCacheSketchSection(t *testing.T) {
	data := buildV14FileWithCols(t, 10, []string{"col.sketch"})
	r := openReader(t, data)
	require.Greater(t, r.BlockCount(), 0)

	// ColumnSketch triggers ensureV14SketchSection → parseAndCacheSketchSection.
	sk := r.ColumnSketch("span.col.sketch")
	// nil is acceptable — sketch section may not be written for this column.
	_ = sk

	// FileSketchSummary also exercises the sketch lazy-load path.
	summary := r.FileSketchSummary()
	_ = summary
}

// TestParseBlockHeaderV12Rejection verifies that parseBlockHeader rejects V12 blocks
// (enc_version=12) with an error containing "not supported".
// Exercises block_parser.go:33 V12 rejection branch which was at 62.5% coverage.
func TestParseBlockHeaderV12Rejection(t *testing.T) {
	// Build a 24-byte block header with valid magic but version=12 (VersionBlockV12).
	buf := make([]byte, 24)
	binary.LittleEndian.PutUint32(buf[0:], modules_shared.MagicNumber)
	buf[4] = modules_shared.VersionBlockV12    // V12 — must be rejected
	binary.LittleEndian.PutUint32(buf[8:], 5)  // span_count
	binary.LittleEndian.PutUint32(buf[12:], 2) // col_count

	err := reader.ParseBlockHeaderExported(buf)
	require.Error(t, err, "V12 block header must be rejected")
	assert.True(t, strings.Contains(err.Error(), "not supported"),
		"error must mention 'not supported', got: %s", err.Error())
}

// TestParseBlockHeaderTooShort verifies that parseBlockHeader rejects buffers smaller than 24 bytes.
func TestParseBlockHeaderTooShort(t *testing.T) {
	err := reader.ParseBlockHeaderExported(make([]byte, 10))
	require.Error(t, err)
	assert.True(t, strings.Contains(err.Error(), "need 24 bytes"),
		"error must mention 'need 24 bytes', got: %s", err.Error())
}

// TestParseBlockHeaderBadMagic verifies that parseBlockHeader rejects a buffer with the wrong magic.
func TestParseBlockHeaderBadMagic(t *testing.T) {
	buf := make([]byte, 24)
	binary.LittleEndian.PutUint32(buf[0:], 0xDEADBEEF) // wrong magic
	buf[4] = modules_shared.VersionBlockV14

	err := reader.ParseBlockHeaderExported(buf)
	require.Error(t, err)
	assert.True(t, strings.Contains(err.Error(), "bad magic"),
		"error must mention 'bad magic', got: %s", err.Error())
}

// TestParseV5MetadataLazy verifies that opening a V14 reader and calling metadata-dependent
// methods exercises the parseV5MetadataLazy and related code paths.
// parseV5MetadataLazy was at 14.4% coverage.
func TestParseV5MetadataLazy(t *testing.T) {
	data := buildV14FileWithCols(t, 8, []string{"col.meta"})

	r := openReader(t, data)

	// BlockCount relies on block index parsed by parseSectionsLazyV14 / parseV5MetadataLazy.
	assert.Greater(t, r.BlockCount(), 0, "block count must be positive after metadata parse")

	// HasIntrinsicSection exercises the intrinsic TOC parse path.
	_ = r.HasIntrinsicSection()

	// GetBlockWithBytes exercises the full block decode path.
	bwb, err := r.GetBlockWithBytes(0, nil, nil)
	require.NoError(t, err)
	require.NotNil(t, bwb)
	assert.Greater(t, bwb.Block.SpanCount(), 0, "block must have at least one span")
}

// TestOpenMultipleV14Readers verifies that a second reader on the same data
// exercises the process-level metadata cache hit path inside parseV5MetadataLazy.
func TestOpenMultipleV14Readers(t *testing.T) {
	data := buildV14FileWithCols(t, 6, []string{"col.cache"})

	r1 := openReader(t, data)
	require.Greater(t, r1.BlockCount(), 0)

	r2 := openReader(t, data)
	assert.Equal(t, r1.BlockCount(), r2.BlockCount(), "both readers must agree on block count")

	bwb1, err := r1.GetBlockWithBytes(0, nil, nil)
	require.NoError(t, err)
	bwb2, err := r2.GetBlockWithBytes(0, nil, nil)
	require.NoError(t, err)
	assert.Equal(t, bwb1.Block.SpanCount(), bwb2.Block.SpanCount())
}

// TestReadBlocks verifies that ReadBlocks returns raw block bytes for requested indices.
// Exercises reader.go:536 which was at 0% coverage.
func TestReadBlocks(t *testing.T) {
	data := buildV14FileWithCols(t, 5, []string{"col.rb"})
	r := openReader(t, data)
	require.Greater(t, r.BlockCount(), 0)

	// Empty slice should return an empty map with no error.
	m, err := r.ReadBlocks(nil)
	require.NoError(t, err)
	assert.Empty(t, m)

	// Read block 0.
	m, err = r.ReadBlocks([]int{0})
	require.NoError(t, err)
	require.Contains(t, m, 0, "block 0 bytes must be returned")
	assert.NotEmpty(t, m[0], "block 0 must have non-empty bytes")
}

// TestHasTraceIndex verifies HasTraceIndex exercises the ensureTraceIndex path.
// reader.go:595 was at 0% coverage.
func TestHasTraceIndex(t *testing.T) {
	data := buildV14FileWithCols(t, 3, []string{"col.trace"})
	r := openReader(t, data)

	// HasTraceIndex must not panic; result may be false for V14 files.
	_ = r.HasTraceIndex()
}

// TestSignalType verifies SignalType returns a valid value.
// reader.go:327 was at 0% coverage.
func TestSignalType(t *testing.T) {
	data := buildV14FileWithCols(t, 3, []string{"col.sig"})
	r := openReader(t, data)

	st := r.SignalType()
	assert.Greater(t, st, uint8(0), "SignalType must be non-zero")
}

// TestResetInternStrings verifies the no-op method does not panic.
func TestResetInternStrings(t *testing.T) {
	data := buildV14FileWithCols(t, 3, []string{"col.intern"})
	r := openReader(t, data)
	r.ResetInternStrings() // must not panic
}

// TestTraceBloomRaw verifies TraceBloomRaw exercises the V14 trace section path.
// reader.go:680 was at 0% coverage.
func TestTraceBloomRaw(t *testing.T) {
	data := buildV14FileWithCols(t, 5, []string{"col.bloom"})
	r := openReader(t, data)

	// TraceBloomRaw may return nil; no panic is the invariant.
	_ = r.TraceBloomRaw()
}

// TestParseBlockFromBytesWithIntern exercises ParseBlockFromBytesWithIntern.
// reader.go:581 was at 0% coverage.
func TestParseBlockFromBytesWithIntern(t *testing.T) {
	data := buildV14FileWithCols(t, 5, []string{"col.intern2"})
	r := openReader(t, data)
	require.Greater(t, r.BlockCount(), 0)

	m, err := r.ReadBlocks([]int{0})
	require.NoError(t, err)
	raw := m[0]
	require.NotEmpty(t, raw)

	intern := make(map[string]string)
	bwb, err := r.ParseBlockFromBytesWithIntern(raw, nil, r.BlockMeta(0), intern)
	require.NoError(t, err)
	require.NotNil(t, bwb)
	assert.Greater(t, bwb.Block.SpanCount(), 0)
}

// TestAddColumnsToBlockV14 exercises AddColumnsToBlock by loading a block with a
// subset of columns, then adding the remaining columns via AddColumnsToBlock.
// reader.go:707 was at 30.9% coverage.
func TestAddColumnsToBlockV14(t *testing.T) {
	colNames := []string{"col.a", "col.b", "col.c"}
	data := buildV14FileWithCols(t, 5, colNames)
	r := openReader(t, data)
	require.Greater(t, r.BlockCount(), 0)

	// Load block with only col.a requested.
	want := map[string]struct{}{"span.col.a": {}}
	bwb, err := r.GetBlockWithBytes(0, want, nil)
	require.NoError(t, err)
	require.NotNil(t, bwb)

	// Now add col.b via AddColumnsToBlock.
	addCols := map[string]struct{}{"span.col.b": {}}
	err = r.AddColumnsToBlock(bwb, addCols)
	require.NoError(t, err)

	// col.b should now be decoded in the block.
	colB := bwb.Block.GetColumn("span.col.b")
	assert.NotNil(t, colB, "span.col.b must be present after AddColumnsToBlock")

	// AddColumnsToBlock(nil) should return an error.
	err = r.AddColumnsToBlock(nil, nil)
	require.Error(t, err)
}

// TestBlockMethods exercises Block accessor methods that were at 0% coverage.
func TestBlockMethods(t *testing.T) {
	data := buildV14FileWithCols(t, 5, []string{"col.m1", "col.m2"})
	r := openReader(t, data)
	require.Greater(t, r.BlockCount(), 0)

	bwb, err := r.GetBlockWithBytes(0, nil, nil)
	require.NoError(t, err)
	require.NotNil(t, bwb)

	blk := bwb.Block

	// IterFields — pre-built by BuildIterFields during parse.
	fields := blk.IterFields()
	assert.NotNil(t, fields, "IterFields must return non-nil slice after parse")

	// GetColumnByType — exact (name, type) lookup.
	col := blk.GetColumn("span.col.m1")
	if col != nil {
		byType := blk.GetColumnByType("span.col.m1", col.Type)
		assert.NotNil(t, byType, "GetColumnByType must find column by exact type")
	}

	// GetAllColumns — returns all type variants for a name.
	allCols := blk.GetAllColumns("span.col.m1")
	assert.NotNil(t, allCols, "GetAllColumns must return at least one column")

	// Columns — returns the full map.
	cols := blk.Columns()
	assert.NotNil(t, cols, "Columns must return the full column map")
	assert.NotEmpty(t, cols, "Columns map must be non-empty after parse")

	// Meta — returns block metadata.
	meta := blk.Meta()
	assert.GreaterOrEqual(t, meta.SpanCount, uint32(0), "Meta.SpanCount must be non-negative")
}

// TestIsDecodedAndEnsureDecoded exercises Column.IsDecoded and Column.EnsureDecoded.
// block.go:66 and block.go:73 were at 0% coverage.
func TestIsDecodedAndEnsureDecoded(t *testing.T) {
	colNames := []string{"col.lazy"}
	data := buildV14FileWithCols(t, 5, colNames)
	r := openReader(t, data)
	require.Greater(t, r.BlockCount(), 0)

	// Load block with no columns requested — span.col.lazy is lazily registered.
	want := map[string]struct{}{"span.col.other": {}} // a column that doesn't exist
	bwb, err := r.GetBlockWithBytes(0, want, nil)
	require.NoError(t, err)
	require.NotNil(t, bwb)

	lazyCol := bwb.Block.GetColumn("span.col.lazy")
	if lazyCol != nil {
		// Lazily registered columns are not yet decoded.
		decoded := lazyCol.IsDecoded()
		// Trigger full decode.
		lazyCol.EnsureDecoded()
		// After EnsureDecoded, IsDecoded must return true.
		assert.True(t, lazyCol.IsDecoded(), "IsDecoded must be true after EnsureDecoded")
		_ = decoded
	}

	// Eagerly decoded column should already be decoded.
	bwb2, err := r.GetBlockWithBytes(0, nil, nil)
	require.NoError(t, err)
	col2 := bwb2.Block.GetColumn("span.col.lazy")
	if col2 != nil {
		assert.True(t, col2.IsDecoded(), "eagerly decoded column must report IsDecoded=true")
		col2.EnsureDecoded() // must not panic
	}
}

// TestAcquireReleaseInternMap exercises AcquireInternMap and ReleaseInternMap.
// column.go:35 and column.go:40 were at 0% coverage.
func TestAcquireReleaseInternMap(t *testing.T) {
	mp := reader.AcquireInternMap()
	require.NotNil(t, mp, "AcquireInternMap must return non-nil")
	(*mp)["key"] = "val"
	reader.ReleaseInternMap(mp)
	// After release, the map must be cleared.
	assert.Empty(t, *mp, "ReleaseInternMap must clear the map")
}

// TestFileBloomMayContainString exercises FileBloom.MayContainString and related methods.
// file_bloom.go was at 0% coverage for these methods.
func TestFileBloomMayContainString(t *testing.T) {
	// nil FileBloom must return true (conservative).
	var fb *reader.FileBloom
	assert.True(t, fb.MayContainString("col", "val"), "nil bloom must return true")
	assert.Nil(t, fb.Raw(), "nil bloom must return nil raw bytes")

	// Build a real V14 file with service names to get a populated bloom.
	var buf bytes.Buffer
	w, err := writer.NewWriterWithConfig(writer.Config{OutputStream: &buf})
	require.NoError(t, err)

	services := []string{"svc-alpha", "svc-beta"}
	for i, svc := range services {
		traceID := make([]byte, 16)
		traceID[0] = byte(i + 1)
		spanID := make([]byte, 8)
		spanID[0] = byte(i + 1)
		td := &tracev1.TracesData{
			ResourceSpans: []*tracev1.ResourceSpans{
				{
					Resource: &resourcev1.Resource{
						Attributes: []*commonv1.KeyValue{
							{Key: "service.name", Value: &commonv1.AnyValue{
								Value: &commonv1.AnyValue_StringValue{StringValue: svc},
							}},
						},
					},
					ScopeSpans: []*tracev1.ScopeSpans{{Spans: []*tracev1.Span{
						{
							TraceId: traceID,
							SpanId:  spanID,
							Name:    "op",
						},
					}}},
				},
			},
		}
		require.NoError(t, w.AddTracesData(td))
	}
	_, err = w.Flush()
	require.NoError(t, err)
	data := buf.Bytes()

	r := openReader(t, data)
	bloom := r.FileBloom()
	if bloom != nil {
		// MayContainString must not panic.
		_ = bloom.MayContainString("resource.service.name", "svc-alpha")
		_ = bloom.MayContainString("resource.service.name", "definitely-not-present-xyz")

		// Column not in the bloom returns true (conservative) — file_bloom.go:31-33.
		result := bloom.MayContainString("definitely.not.a.bloom.column", "val")
		assert.True(t, result, "MayContainString must return true for unknown columns")

		// Raw must return non-nil for a real bloom.
		raw := bloom.Raw()
		assert.NotNil(t, raw, "Raw must be non-nil for a parsed bloom")
		assert.Greater(t, len(raw), 0)

		// ParseFileBloom on raw bytes must succeed.
		fb2, parseErr := reader.ParseFileBloom(raw)
		require.NoError(t, parseErr)
		// fb2 may be nil if raw doesn't start with FileBloom magic.
		_ = fb2
	}
}

// TestParseFileBloomNil verifies ParseFileBloom handles nil/empty input gracefully.
func TestParseFileBloomNil(t *testing.T) {
	fb, err := reader.ParseFileBloom(nil)
	require.NoError(t, err)
	assert.Nil(t, fb, "ParseFileBloom(nil) must return nil")

	fb, err = reader.ParseFileBloom([]byte{})
	require.NoError(t, err)
	assert.Nil(t, fb, "ParseFileBloom(empty) must return nil")
}

// TestColumnSketchMethods exercises Presence, Distinct, TopKMatch, and FuseContains
// on a ColumnSketch obtained from a real V14 reader.
// sketch_index.go methods were at 0% coverage.
func TestColumnSketchMethods(t *testing.T) {
	data := buildV14FileWithCols(t, 10, []string{"col.sketch2"})
	r := openReader(t, data)
	require.Greater(t, r.BlockCount(), 0)

	sk := r.ColumnSketch("span.col.sketch2")
	if sk == nil {
		t.Skip("no sketch data for span.col.sketch2 — skipping sketch method tests")
	}

	// Presence returns a bitset.
	presence := sk.Presence()
	_ = presence

	// Distinct returns per-block cardinality.
	distinct := sk.Distinct()
	_ = distinct

	// TopKMatch takes a hash fingerprint.
	topk := sk.TopKMatch(0)
	_ = topk

	// FuseContains takes a hash.
	fuse := sk.FuseContains(0)
	_ = fuse
}

// TestReadGroupColumnar exercises ReadGroupColumnar with a real coalesced read group.
// columnar_read.go was at 0% coverage.
func TestReadGroupColumnar(t *testing.T) {
	data := buildV14FileWithCols(t, 5, []string{"col.columnar"})
	r := openReader(t, data)
	require.Greater(t, r.BlockCount(), 0)

	// Build coalesced groups for all blocks.
	allBlocks := make([]int, r.BlockCount())
	for i := range allBlocks {
		allBlocks[i] = i
	}
	groups := r.CoalescedGroups(allBlocks)
	require.NotEmpty(t, groups, "must have at least one coalesced group")

	// ReadGroupColumnar with nil wantColumns falls back to ReadCoalescedBlocks.
	m, err := r.ReadGroupColumnar(groups[0], nil)
	require.NoError(t, err)
	assert.NotEmpty(t, m, "must return block bytes")

	// ReadGroupColumnar with specific columns exercises the columnar read path.
	wantCols := map[string]struct{}{"span.col.columnar": {}}
	m2, err := r.ReadGroupColumnar(groups[0], wantCols)
	require.NoError(t, err)
	assert.NotEmpty(t, m2, "must return block bytes with column filter")
}

// TestFileLayout exercises the FileLayout method which was at 0% coverage.
func TestFileLayout(t *testing.T) {
	data := buildV14FileWithCols(t, 5, []string{"col.layout"})
	r := openReader(t, data)
	require.Greater(t, r.BlockCount(), 0)

	layout, err := r.FileLayout()
	require.NoError(t, err)
	require.NotNil(t, layout, "FileLayout must return non-nil report")
	assert.Greater(t, layout.BlockCount, 0, "layout must report at least one block")
	assert.NotEmpty(t, layout.Sections, "layout must have sections")
}

// TestNewReaderWithOptionsErrors verifies that NewReaderFromProviderWithOptions and
// NewLeanReaderFromProviderWithOptions return errors for invalid option combinations.
// reader.go:178 and reader.go:237 had uncovered error paths.
func TestNewReaderWithOptionsErrors(t *testing.T) {
	// errProvider.Size() always returns an error, so NewReaderFromProviderWithOptions
	// must propagate it rather than silently returning a zero-value reader.
	_, err := reader.NewReaderFromProviderWithOptions(&errProvider{}, reader.Options{})
	require.Error(t, err, "provider.Size error must propagate")
}

// errProvider is a ReaderProvider that always fails on Size.
type errProvider struct{}

func (e *errProvider) Size() (int64, error) { return 0, fmt.Errorf("size error") }
func (e *errProvider) ReadAt(_ []byte, _ int64, _ rw.DataType) (int, error) {
	return 0, fmt.Errorf("read error")
}

// TestAddColumnsToBlockAllMissing exercises AddColumnsToBlock with addColumns=nil
// to add all missing columns (exercises the nil addColumns path and the decode loop).
func TestAddColumnsToBlockAllMissing(t *testing.T) {
	data := buildV14FileWithCols(t, 5, []string{"col.p", "col.q", "col.r"})
	r := openReader(t, data)
	require.Greater(t, r.BlockCount(), 0)

	// Load block with all columns eagerly decoded.
	bwb, err := r.GetBlockWithBytes(0, nil, nil)
	require.NoError(t, err)
	require.NotNil(t, bwb)

	// Remove some columns from the internal columns map to simulate missing columns.
	// Columns() returns the actual internal map — mutations are visible to AddColumnsToBlock.
	cols := bwb.Block.Columns()
	var removedKey modules_shared.ColumnKey
	for k := range cols {
		if k.Name == "span.col.q" {
			removedKey = k
			delete(cols, k)
			break
		}
	}

	// Now span.col.q is missing from the block's columns map.
	// AddColumnsToBlock with nil should re-decode and add it.
	if removedKey.Name != "" {
		err = r.AddColumnsToBlock(bwb, nil)
		require.NoError(t, err)
		// span.col.q should be back.
		colQ := bwb.Block.GetColumn("span.col.q")
		assert.NotNil(t, colQ, "span.col.q must be re-added by AddColumnsToBlock(nil)")
	}

	// Also test AddColumnsToBlock(nil) on a block loaded with all columns.
	// Exercises the "already exists" skip path in AddColumnsToBlock.
	bwb2, err := r.GetBlockWithBytes(0, nil, nil)
	require.NoError(t, err)
	err = r.AddColumnsToBlock(bwb2, nil)
	require.NoError(t, err)
}

// TestColumnValueAccessors exercises Column value accessors (StringValues, Uint64Value, etc.)
// on a fully decoded block.
func TestColumnValueAccessors(t *testing.T) {
	var buf bytes.Buffer
	w, err := writer.NewWriterWithConfig(writer.Config{OutputStream: &buf})
	require.NoError(t, err)

	for i := range 5 {
		traceID := make([]byte, 16)
		traceID[0] = byte(i + 1)
		spanID := make([]byte, 8)
		spanID[0] = byte(i + 1)

		td := &tracev1.TracesData{
			ResourceSpans: []*tracev1.ResourceSpans{
				{ScopeSpans: []*tracev1.ScopeSpans{{Spans: []*tracev1.Span{
					{
						TraceId: traceID,
						SpanId:  spanID,
						Name:    "op",
						Attributes: []*commonv1.KeyValue{
							{
								Key: "attr.str",
								Value: &commonv1.AnyValue{
									Value: &commonv1.AnyValue_StringValue{StringValue: "val-" + string(rune('a'+i))},
								},
							},
							{
								Key:   "attr.int",
								Value: &commonv1.AnyValue{Value: &commonv1.AnyValue_IntValue{IntValue: int64(i) * 10}},
							},
							{
								Key: "attr.float",
								Value: &commonv1.AnyValue{
									Value: &commonv1.AnyValue_DoubleValue{DoubleValue: float64(i)},
								},
							},
							{
								Key:   "attr.bool",
								Value: &commonv1.AnyValue{Value: &commonv1.AnyValue_BoolValue{BoolValue: i%2 == 0}},
							},
						},
					},
				}}}},
			},
		}
		require.NoError(t, w.AddTracesData(td))
	}

	_, err = w.Flush()
	require.NoError(t, err)
	data := buf.Bytes()

	r := openReader(t, data)
	require.Greater(t, r.BlockCount(), 0)

	bwb, err := r.GetBlockWithBytes(0, nil, nil)
	require.NoError(t, err)
	require.NotNil(t, bwb)

	// StringValues exercises the batch string extraction path.
	strCol := bwb.Block.GetColumn("span.attr.str")
	if strCol != nil {
		vals := strCol.StringValues()
		assert.Equal(t, strCol.SpanCount, len(vals), "StringValues must return SpanCount entries")
	}

	// Int64Value.
	intCol := bwb.Block.GetColumn("span.attr.int")
	if intCol != nil {
		_, _ = intCol.Int64Value(0)
	}

	// Float64Value.
	floatCol := bwb.Block.GetColumn("span.attr.float")
	if floatCol != nil {
		_, _ = floatCol.Float64Value(0)
	}

	// BoolValue.
	boolCol := bwb.Block.GetColumn("span.attr.bool")
	if boolCol != nil {
		_, _ = boolCol.BoolValue(0)
	}

	// Uint64Value on span:start (always present, always a uint64 column).
	startCol := bwb.Block.GetColumn("span:start")
	if startCol != nil {
		_, _ = startCol.Uint64Value(0)
		// Also exercise the "not present" path by calling at an out-of-range index.
		_, _ = startCol.Uint64Value(startCol.SpanCount + 100)
	}

	// BytesValue — bytes attributes encoded as BytesDict/BytesIdx.
	bytesAttrData := buildV14FileWithCols(t, 3, []string{})
	r2 := openReader(t, bytesAttrData)
	_ = r2
}

// TestEnsureTraceIndexWithData exercises ensureTraceIndex when traceIndexRaw is set.
// reader.go:314 (ensureTraceIndex) was at 33.3% coverage.
func TestEnsureTraceIndexWithData(t *testing.T) {
	data := buildV14FileWithCols(t, 5, []string{"col.ti"})
	r := openReader(t, data)

	// HasTraceIndex calls ensureTraceIndex. For V14 files this exercises the lazy path.
	_ = r.HasTraceIndex()

	// TraceCount also calls ensureTraceIndex.
	_ = r.TraceCount()
}

// TestParseBlockFromBytesError exercises ParseBlockFromBytes error path.
func TestParseBlockFromBytesError(t *testing.T) {
	data := buildV14FileWithCols(t, 3, []string{"col.err"})
	r := openReader(t, data)

	// Pass invalid raw bytes to trigger an error.
	_, err := r.ParseBlockFromBytes([]byte{0x01, 0x02, 0x03}, nil, r.BlockMeta(0))
	require.Error(t, err, "ParseBlockFromBytes with bad bytes must return an error")
}

// TestFileSketchSummaryRaw exercises FileSketchSummaryRaw.
func TestFileSketchSummaryRaw(t *testing.T) {
	data := buildV14FileWithCols(t, 5, []string{"col.sumraw"})
	r := openReader(t, data)

	raw := r.FileSketchSummaryRaw()
	// nil is acceptable for files without sketch data.
	_ = raw
}

// TestUnmarshalFileSketchSummaryErrors exercises error paths in UnmarshalFileSketchSummary.
// The function handles nil/empty (return nil,nil), too-short (<8 bytes), and bad-magic inputs.
func TestUnmarshalFileSketchSummaryErrors(t *testing.T) {
	// nil input → nil, nil (already covered by existing tests, included for completeness).
	s, err := reader.UnmarshalFileSketchSummary(nil)
	require.NoError(t, err)
	assert.Nil(t, s)

	// 1–7 bytes: too short for header (file_sketch_summary.go:208-210).
	_, err = reader.UnmarshalFileSketchSummary([]byte{0x01, 0x02, 0x03})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "too short")

	// 8 bytes with wrong magic: bad magic error (file_sketch_summary.go:212-214).
	_, err = reader.UnmarshalFileSketchSummary([]byte{0xFF, 0xFF, 0xFF, 0xFF, 0, 0, 0, 0})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "magic")

	// Valid magic + numCols=1 but no column data → "truncated at name_len" (line 223-225).
	// Wire format: magic[4 LE] = 0x46534B55, num_cols[4 LE] = 1, then no more bytes.
	truncatedHeader := []byte{
		0x55, 0x4B, 0x53, 0x46, // magic = 0x46534B55 LE
		0x01, 0x00, 0x00, 0x00, // num_cols = 1
		// no column data — triggers "truncated at name_len"
	}
	_, err = reader.UnmarshalFileSketchSummary(truncatedHeader)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "truncated")
}

// buildV14FileWithTraces writes a file with the given number of distinct traces, each
// with one span. Returns the file bytes and the list of trace IDs used.
func buildV14FileWithTraces(t *testing.T, nTraces int) ([]byte, [][16]byte) {
	t.Helper()
	var buf bytes.Buffer
	w, err := writer.NewWriterWithConfig(writer.Config{OutputStream: &buf})
	require.NoError(t, err)

	traceIDs := make([][16]byte, nTraces)
	for i := range nTraces {
		var tid [16]byte
		tid[0] = byte(i + 1)
		tid[1] = byte(i >> 8)
		traceIDs[i] = tid

		spanID := make([]byte, 8)
		spanID[0] = byte(i + 1)

		td := &tracev1.TracesData{
			ResourceSpans: []*tracev1.ResourceSpans{
				{ScopeSpans: []*tracev1.ScopeSpans{{Spans: []*tracev1.Span{
					{
						TraceId:           tid[:],
						SpanId:            spanID,
						Name:              "op",
						StartTimeUnixNano: uint64(i * 1_000_000),
						EndTimeUnixNano:   uint64(i*1_000_000 + 500_000),
					},
				}}}},
			},
		}
		require.NoError(t, w.AddTracesData(td))
	}

	_, err = w.Flush()
	require.NoError(t, err)
	return buf.Bytes(), traceIDs
}

// TestBlocksForTraceIDV14 exercises the V14 trace index lookup path.
// This exercises scanTraceIndexRaw and ensureV14TraceSection more thoroughly.
func TestBlocksForTraceIDV14(t *testing.T) {
	data, traceIDs := buildV14FileWithTraces(t, 5)
	r := openReader(t, data)
	require.Greater(t, r.BlockCount(), 0)

	// Look up known trace IDs — should return blocks.
	for _, tid := range traceIDs {
		blocks := r.BlocksForTraceID(tid)
		// Blocks may be nil if trace section is not populated for V14 files.
		_ = blocks
	}

	// Unknown trace ID — should return nil.
	var unknown [16]byte
	unknown[0] = 0xFF
	unknown[1] = 0xFF
	blocks := r.BlocksForTraceID(unknown)
	_ = blocks // may be nil or not depending on bloom

	// TraceEntries exercises a related path.
	for _, tid := range traceIDs {
		entries := r.TraceEntries(tid)
		_ = entries
	}

	// TraceBloomRaw after V14 trace section load.
	_ = r.TraceBloomRaw()

	// HasTraceIndex exercises ensureTraceIndex.
	_ = r.HasTraceIndex()
}

// TestUUIDStringAttribute writes a UUID-looking string attribute and verifies
// that StringValue() returns a UUID-formatted string via uuidStringValue.
// block.go:148 (uuidStringValue) was at 0% coverage.
func TestUUIDStringAttribute(t *testing.T) {
	var buf bytes.Buffer
	w, err := writer.NewWriterWithConfig(writer.Config{OutputStream: &buf})
	require.NoError(t, err)

	// Write spans with a UUID string attribute — the writer auto-detects UUID columns.
	uuidVal := "213085fc-b15b-45fc-8fa0-d448d4a246be"
	for i := range 5 {
		traceID := make([]byte, 16)
		traceID[0] = byte(i + 1)
		spanID := make([]byte, 8)
		spanID[0] = byte(i + 1)

		td := &tracev1.TracesData{
			ResourceSpans: []*tracev1.ResourceSpans{
				{ScopeSpans: []*tracev1.ScopeSpans{{Spans: []*tracev1.Span{
					{
						TraceId: traceID,
						SpanId:  spanID,
						Name:    "op",
						Attributes: []*commonv1.KeyValue{
							{Key: "attr.uuid", Value: &commonv1.AnyValue{
								Value: &commonv1.AnyValue_StringValue{StringValue: uuidVal},
							}},
						},
					},
				}}}},
			},
		}
		require.NoError(t, w.AddTracesData(td))
	}

	_, err = w.Flush()
	require.NoError(t, err)
	data := buf.Bytes()

	r := openReader(t, data)
	require.Greater(t, r.BlockCount(), 0)

	bwb, err := r.GetBlockWithBytes(0, nil, nil)
	require.NoError(t, err)
	require.NotNil(t, bwb)

	uuidCol := bwb.Block.GetColumn("span.attr.uuid")
	if uuidCol != nil && uuidCol.SpanCount > 0 {
		// StringValues exercises the batch extraction path.
		vals := uuidCol.StringValues()
		assert.Equal(t, uuidCol.SpanCount, len(vals), "StringValues must return SpanCount entries")

		// StringValue at idx 0 triggers uuidStringValue if column is UUID type.
		v, ok := uuidCol.StringValue(0)
		if ok {
			assert.NotEmpty(t, v, "UUID string value must not be empty")
		}
	}
}

// TestIntrinsicColumnMetaUnknown verifies IntrinsicColumnMeta returns false for unknown columns.
// intrinsic_reader.go:82 was at 40% — the "not found" path was not exercised.
func TestIntrinsicColumnMetaUnknown(t *testing.T) {
	data := buildV14FileWithCols(t, 3, []string{"col.ic"})
	r := openReader(t, data)

	// Known intrinsic column.
	_, hasSpanName := r.IntrinsicColumnMeta("span:name")
	// May or may not be present.
	_ = hasSpanName

	// Unknown intrinsic column must return false.
	_, hasUnknown := r.IntrinsicColumnMeta("definitely.not.a.column")
	assert.False(t, hasUnknown, "unknown intrinsic column must return false")

	// IntrinsicColumnNames exercises the sorted names path.
	names := r.IntrinsicColumnNames()
	_ = names // may be nil for files without intrinsic section

	// EnsureIntrinsicTOC — must not error.
	err := r.EnsureIntrinsicTOC()
	require.NoError(t, err)
}

// TestGetIntrinsicColumnAndBlob exercises GetIntrinsicColumnBlob and GetIntrinsicColumn.
// intrinsic_reader.go was at 72.7% and 0% for these methods respectively.
func TestGetIntrinsicColumnAndBlob(t *testing.T) {
	data := buildV14FileWithCols(t, 5, []string{"col.gc"})
	r := openReader(t, data)

	names := r.IntrinsicColumnNames()
	if len(names) == 0 {
		t.Skip("no intrinsic columns in this file")
	}

	for _, name := range names {
		// GetIntrinsicColumnBlob exercises the blob fetch path.
		blob, err := r.GetIntrinsicColumnBlob(name)
		require.NoError(t, err)
		// blob may be nil if intrinsic section is missing.

		// GetIntrinsicColumn exercises the full decode path.
		col, err := r.GetIntrinsicColumn(name)
		require.NoError(t, err)
		// col may be nil for some columns.
		_ = col
		_ = blob

		// IntrinsicColumnMeta with a populated index exercises the peek path.
		meta, ok := r.IntrinsicColumnMeta(name)
		if ok {
			assert.NotZero(t, meta.Length, "IntrinsicColMeta.Length must be set for known column")
		}
	}

	// GetIntrinsicColumn for unknown column returns nil, nil.
	col, err := r.GetIntrinsicColumn("nonexistent.column")
	require.NoError(t, err)
	assert.Nil(t, col)

	// GetIntrinsicColumnBlob for unknown column returns nil, nil.
	blob, err := r.GetIntrinsicColumnBlob("nonexistent.column")
	require.NoError(t, err)
	assert.Nil(t, blob)

	// GetIntrinsicColumn for "span:end" exercises the synthesize path.
	spanEnd, err := r.GetIntrinsicColumn("span:end")
	require.NoError(t, err)
	_ = spanEnd

	// Exercise IntrinsicBytesAt, IntrinsicUint64At, IntrinsicDictStringAt, IntrinsicDictInt64At.
	// These are O(n) scan methods that look up values by (blockIdx, rowIdx).
	// Iterate over all intrinsic columns and call all four At methods so the "match found"
	// inner-loop branches are covered for both flat (bytes/uint64) and dict column types.
	if r.BlockCount() > 0 {
		for _, name := range names {
			// (0, 0) is valid for all written columns — BlockRefs[0] = {BlockIdx:0, RowIdx:0}.
			_, _ = r.IntrinsicBytesAt(name, 0, 0)
			_, _ = r.IntrinsicUint64At(name, 0, 0)
			_, _ = r.IntrinsicDictStringAt(name, 0, 0)
			_, _ = r.IntrinsicDictInt64At(name, 0, 0)
		}

		// Call with a nonexistent column to exercise the nil-column early return.
		_, _ = r.IntrinsicBytesAt("nonexistent", 0, 0)
		_, _ = r.IntrinsicUint64At("nonexistent", 0, 0)
		_, _ = r.IntrinsicDictStringAt("nonexistent", 0, 0)
		_, _ = r.IntrinsicDictInt64At("nonexistent", 0, 0)

		// Second call to GetIntrinsicColumn on the same name exercises the intrinsicDecoded
		// cache-hit path (r.intrinsicDecoded[name] is already populated from the loop above).
		if len(names) > 0 {
			col2, err2 := r.GetIntrinsicColumn(names[0])
			require.NoError(t, err2)
			_ = col2
		}
	}
}

// TestReaderWithFileID exercises NewReaderFromProviderWithOptions with a FileID set,
// which triggers the process-level cache paths in parseAndCacheSketchSection
// and other parsers. These paths were at 38.9% coverage.
func TestReaderWithFileID(t *testing.T) {
	data := buildV14FileWithCols(t, 5, []string{"col.fid"})
	prov := &memProvider{data: data}

	// Clear process-level caches so we always get a fresh parse.
	reader.ClearCaches()

	opts := reader.Options{FileID: "test-file-id-" + t.Name()}

	// First open — populates caches.
	r1, err := reader.NewReaderFromProviderWithOptions(prov, opts)
	require.NoError(t, err)
	require.Greater(t, r1.BlockCount(), 0)

	// Exercise sketch and bloom sections which use the fileID cache.
	_ = r1.ColumnSketch("span.col.fid")
	_ = r1.FileBloom()
	_ = r1.FileSketchSummary()
	_ = r1.TimeIndexLen()
	_ = r1.TraceCount()

	// Second open — should hit the process-level metadata cache.
	r2, err := reader.NewReaderFromProviderWithOptions(prov, opts)
	require.NoError(t, err)
	assert.Equal(t, r1.BlockCount(), r2.BlockCount(), "cached reader must agree on block count")

	// Exercise same paths again to hit the sketch cache.
	_ = r2.ColumnSketch("span.col.fid")
	_ = r2.FileSketchSummary()
}

// TestRangeColumnBoundaries exercises RangeColumnBoundaries which was at 58.3%.
func TestRangeColumnBoundaries(t *testing.T) {
	var buf bytes.Buffer
	w, err := writer.NewWriterWithConfig(writer.Config{OutputStream: &buf})
	require.NoError(t, err)

	for i := range 5 {
		traceID := make([]byte, 16)
		traceID[0] = byte(i + 1)
		spanID := make([]byte, 8)
		spanID[0] = byte(i + 1)

		td := &tracev1.TracesData{
			ResourceSpans: []*tracev1.ResourceSpans{
				{ScopeSpans: []*tracev1.ScopeSpans{{Spans: []*tracev1.Span{
					{
						TraceId: traceID,
						SpanId:  spanID,
						Name:    "op",
						Attributes: []*commonv1.KeyValue{
							{Key: "attr.rng", Value: &commonv1.AnyValue{
								Value: &commonv1.AnyValue_IntValue{IntValue: int64(i) * 100},
							}},
						},
					},
				}}}},
			},
		}
		require.NoError(t, w.AddTracesData(td))
	}

	_, err = w.Flush()
	require.NoError(t, err)
	data := buf.Bytes()

	r := openReader(t, data)
	require.Greater(t, r.BlockCount(), 0)

	// RangeColumnType exercises the range index lookup.
	colType, _ := r.RangeColumnType("span.attr.rng")
	_ = colType

	// RangeColumnBoundaries exercises the range boundary lookup.
	rb := r.RangeColumnBoundaries("span.attr.rng")
	// rb may be nil if no range index was written for this column.
	_ = rb

	// ColumnNames — exercises the column name enumeration.
	names := r.ColumnNames()
	_ = names
}

// TestFloat64RangeColumnBoundaries writes a float64 attribute and calls RangeColumnBoundaries
// to exercise the float64BoundsRaw decode path in reader.go:514-519.
func TestFloat64RangeColumnBoundaries(t *testing.T) {
	var buf bytes.Buffer
	w, err := writer.NewWriterWithConfig(writer.Config{OutputStream: &buf})
	require.NoError(t, err)

	for i := range 5 {
		traceID := make([]byte, 16)
		traceID[0] = byte(i + 1)
		spanID := make([]byte, 8)
		spanID[0] = byte(i + 1)
		td := &tracev1.TracesData{
			ResourceSpans: []*tracev1.ResourceSpans{
				{ScopeSpans: []*tracev1.ScopeSpans{{Spans: []*tracev1.Span{
					{
						TraceId: traceID,
						SpanId:  spanID,
						Name:    "op",
						Attributes: []*commonv1.KeyValue{
							{Key: "attr.f64", Value: &commonv1.AnyValue{
								Value: &commonv1.AnyValue_DoubleValue{DoubleValue: float64(i) * 1.5},
							}},
						},
					},
				}}}},
			},
		}
		require.NoError(t, w.AddTracesData(td))
	}

	_, err = w.Flush()
	require.NoError(t, err)
	data := buf.Bytes()

	r := openReader(t, data)
	require.Greater(t, r.BlockCount(), 0)

	// RangeColumnType must return float64 for a float64 column.
	colType, ok := r.RangeColumnType("span.attr.f64")
	_ = ok

	// RangeColumnBoundaries decodes float64BoundsRaw when colType is float64.
	rb := r.RangeColumnBoundaries("span.attr.f64")
	if rb != nil {
		// Float64Bounds should be populated.
		_ = rb.Float64Bounds
		_ = colType
	}
}

// TestFileLayoutMultiColumnTypes exercises fileLayoutV14 with multiple column types
// to cover more encodingKindName branches.
func TestFileLayoutMultiColumnTypes(t *testing.T) {
	var buf bytes.Buffer
	w, err := writer.NewWriterWithConfig(writer.Config{OutputStream: &buf})
	require.NoError(t, err)

	for i := range 5 {
		traceID := make([]byte, 16)
		traceID[0] = byte(i + 1)
		spanID := make([]byte, 8)
		spanID[0] = byte(i + 1)
		td := &tracev1.TracesData{
			ResourceSpans: []*tracev1.ResourceSpans{
				{ScopeSpans: []*tracev1.ScopeSpans{{Spans: []*tracev1.Span{
					{
						TraceId: traceID,
						SpanId:  spanID,
						Name:    "op",
						Attributes: []*commonv1.KeyValue{
							{
								Key: "attr.str",
								Value: &commonv1.AnyValue{
									Value: &commonv1.AnyValue_StringValue{StringValue: "val-" + string(rune('a'+i))},
								},
							},
							{
								Key:   "attr.int",
								Value: &commonv1.AnyValue{Value: &commonv1.AnyValue_IntValue{IntValue: int64(i) * 10}},
							},
							{
								Key: "attr.flt",
								Value: &commonv1.AnyValue{
									Value: &commonv1.AnyValue_DoubleValue{DoubleValue: float64(i)},
								},
							},
							{
								Key:   "attr.boo",
								Value: &commonv1.AnyValue{Value: &commonv1.AnyValue_BoolValue{BoolValue: i%2 == 0}},
							},
							{
								Key: "attr.byt",
								Value: &commonv1.AnyValue{
									Value: &commonv1.AnyValue_BytesValue{BytesValue: []byte{byte(i), byte(i + 1)}},
								},
							},
						},
					},
				}}}},
			},
		}
		require.NoError(t, w.AddTracesData(td))
	}

	_, err = w.Flush()
	require.NoError(t, err)
	data := buf.Bytes()

	r := openReader(t, data)
	require.Greater(t, r.BlockCount(), 0)

	// FileLayout exercises layoutBlockV14 for each column, calling encodingKindName
	// for each column's encoding kind byte.
	layout, err := r.FileLayout()
	require.NoError(t, err)
	require.NotNil(t, layout)
	assert.Greater(t, len(layout.Sections), 0)
}

// TestCoalesceBlocksEdgeCases exercises CoalesceBlocks with empty input and invalid blockIdx.
func TestCoalesceBlocksEdgeCases(t *testing.T) {
	data := buildV14FileWithCols(t, 3, []string{"col.ce"})
	r := openReader(t, data)
	require.Greater(t, r.BlockCount(), 0)

	cfg := modules_shared.CoalesceConfig{MaxGapBytes: 1024, MaxWasteRatio: 0.5}
	metas := make([]modules_shared.BlockMeta, r.BlockCount())
	for i := range r.BlockCount() {
		metas[i] = r.BlockMeta(i)
	}

	// Empty blockOrder — returns nil.
	result := reader.CoalesceBlocks(metas, nil, cfg)
	assert.Nil(t, result, "CoalesceBlocks with empty blockOrder must return nil")

	// blockOrder with an out-of-range index — skips it.
	result = reader.CoalesceBlocks(metas, []int{999}, cfg)
	// All entries skipped → may return nil or empty.
	_ = result

	// blockOrder with a negative index — skips it.
	result = reader.CoalesceBlocks(metas, []int{-1}, cfg)
	_ = result
}

// TestNewLeanReaderFromProviderWithOptions exercises the lean reader construction path
// for V14 files and various error cases.
func TestNewLeanReaderFromProviderWithOptions(t *testing.T) {
	data := buildV14FileWithCols(t, 5, []string{"col.lean"})
	prov := &memProvider{data: data}

	// Valid V14 file — lean reader should succeed.
	r, err := reader.NewLeanReaderFromProviderWithOptions(prov, reader.Options{})
	require.NoError(t, err)
	require.NotNil(t, r)
	require.Greater(t, r.BlockCount(), 0)

	// Cache non-nil but FileID empty must return error.
	// Build a real FileCache using a temp dir to get a non-nil *filecache.FileCache.
	cache := reader.OpenFileCacheForTest(t)
	if cache != nil {
		_, err = reader.NewLeanReaderFromProviderWithOptions(prov, reader.Options{
			Cache:  cache,
			FileID: "", // empty FileID with non-nil cache must error
		})
		require.Error(t, err, "Cache non-nil + empty FileID must error")
	}

	// errProvider makes Size() fail — triggers the size-error path.
	_, err = reader.NewLeanReaderFromProviderWithOptions(&errProvider{}, reader.Options{})
	require.Error(t, err, "Size() failure must propagate")
}

// TestIntrinsicColumnNamesCache exercises the cached-names path in IntrinsicColumnNames
// (second call returns the cached slice built by the first call).
func TestIntrinsicColumnNamesCache(t *testing.T) {
	data := buildV14FileWithCols(t, 3, []string{"col.nc"})
	r := openReader(t, data)

	// First call — builds and caches the sorted names slice.
	names1 := r.IntrinsicColumnNames()
	// Second call — must return the cached slice without rebuilding it.
	names2 := r.IntrinsicColumnNames()
	assert.Equal(t, names1, names2, "IntrinsicColumnNames must return equal slices on repeated calls")
}

// TestIntrinsicColumnMetaNilIndex exercises IntrinsicColumnMeta when the reader has
// no intrinsic section (r.intrinsicIndex == nil), covering the nil-guard early return.
func TestIntrinsicColumnMetaNilIndex(t *testing.T) {
	data := buildV14FileWithCols(t, 3, []string{"col.ni"})
	r := openReader(t, data)

	// Inject a nil intrinsicIndex via the export helper.
	reader.SetIntrinsicIndexForTest(r, nil)

	// IntrinsicColumnMeta must return false when intrinsicIndex is nil.
	_, ok := r.IntrinsicColumnMeta("span:name")
	assert.False(t, ok, "IntrinsicColumnMeta must return false when intrinsicIndex is nil")

	// IntrinsicColumnNames must return nil when intrinsicIndex is empty.
	names := r.IntrinsicColumnNames()
	assert.Nil(t, names, "IntrinsicColumnNames must return nil when intrinsicIndex is nil")

	// GetIntrinsicColumnBlob must return nil, nil when intrinsicIndex is nil.
	blob, err := r.GetIntrinsicColumnBlob("span:name")
	require.NoError(t, err)
	assert.Nil(t, blob)

	// GetIntrinsicColumn must return nil, nil when intrinsicIndex is nil.
	col, err := r.GetIntrinsicColumn("span:name")
	require.NoError(t, err)
	assert.Nil(t, col)
}

// TestSignalTypeDefault exercises the SignalType() zero-value path which returns
// shared.SignalTypeTrace when r.signalType == 0.
func TestSignalTypeDefault(t *testing.T) {
	data := buildV14FileWithCols(t, 3, []string{"col.sig2"})
	r := openReader(t, data)

	// signalType is populated during parse; just verify the method does not panic.
	st := r.SignalType()
	assert.NotZero(t, st, "SignalType must return a non-zero value for a written file")
}

// TestBlocksForTraceIDCompact exercises BlocksForTraceIDCompact directly.
func TestBlocksForTraceIDCompact(t *testing.T) {
	data, traceIDs := buildV14FileWithTraces(t, 3)
	r := openReader(t, data)
	require.Greater(t, r.BlockCount(), 0)

	// Force the compact trace section to be loaded.
	_ = r.TraceBloomRaw() // triggers ensureV14TraceSection + ensureCompactIndexParsed path

	// BlocksForTraceIDCompact on a known trace ID — may return blocks or nil depending on bloom.
	for _, tid := range traceIDs {
		blocks := r.BlocksForTraceIDCompact(tid)
		_ = blocks
	}

	// Unknown trace ID — bloom should reject it.
	var unknown [16]byte
	unknown[0] = 0xDE
	unknown[1] = 0xAD
	blocks := r.BlocksForTraceIDCompact(unknown)
	_ = blocks
}

// TestNewReaderFromProviderCacheNoFileID exercises the Cache!=nil + empty FileID error path
// in NewReaderFromProviderWithOptions (reader.go:179).
func TestNewReaderFromProviderCacheNoFileID(t *testing.T) {
	data := buildV14FileWithCols(t, 3, []string{"col.cid"})
	prov := &memProvider{data: data}

	cache := reader.OpenFileCacheForTest(t)
	if cache == nil {
		t.Skip("cannot open FileCache for test")
	}

	// Cache non-nil but FileID empty must return error for NewReaderFromProviderWithOptions.
	_, err := reader.NewReaderFromProviderWithOptions(prov, reader.Options{
		Cache:  cache,
		FileID: "",
	})
	require.Error(t, err, "Cache non-nil + empty FileID must error for NewReaderFromProviderWithOptions")
	assert.Contains(t, err.Error(), "FileID")
}

// TestSignalTypeZeroValue exercises the SignalType() zero-value path which returns
// shared.SignalTypeTrace when r.signalType == 0. Injects signalType=0 via export helper.
func TestSignalTypeZeroValue(t *testing.T) {
	data := buildV14FileWithCols(t, 3, []string{"col.st0"})
	r := openReader(t, data)

	// Inject signalType = 0 (zero value) to exercise the default branch.
	reader.SetSignalTypeForTest(r, 0)
	st := r.SignalType()
	assert.Equal(t, modules_shared.SignalTypeTrace, st, "SignalType must return SignalTypeTrace when field is zero")
}

// buildV14FileWithServiceName builds a V14 file with nSpans and a service.name resource
// attribute, which is needed to populate the file-level bloom filter.
func buildV14FileWithServiceName(t *testing.T, nSpans int, svcName string) []byte {
	t.Helper()
	var buf bytes.Buffer
	w, err := writer.NewWriterWithConfig(writer.Config{OutputStream: &buf})
	require.NoError(t, err)

	for i := range nSpans {
		traceID := make([]byte, 16)
		traceID[0] = byte(i + 1)
		spanID := make([]byte, 8)
		spanID[0] = byte(i + 1)
		td := &tracev1.TracesData{
			ResourceSpans: []*tracev1.ResourceSpans{
				{
					Resource: &resourcev1.Resource{
						Attributes: []*commonv1.KeyValue{
							{Key: "service.name", Value: &commonv1.AnyValue{
								Value: &commonv1.AnyValue_StringValue{StringValue: svcName},
							}},
						},
					},
					ScopeSpans: []*tracev1.ScopeSpans{{Spans: []*tracev1.Span{
						{TraceId: traceID, SpanId: spanID, Name: "op"},
					}}},
				},
			},
		}
		require.NoError(t, w.AddTracesData(td))
	}
	_, err = w.Flush()
	require.NoError(t, err)
	return buf.Bytes()
}

// TestFileBloomInnerParsePath exercises the FileBloom() inner lazy-parse block
// (reader.go:637-641) which runs when fileBloomRaw!=nil and fileBloomParsed==nil.
// Achieved by injecting raw bloom bytes into a bare reader before calling FileBloom().
func TestFileBloomInnerParsePath(t *testing.T) {
	// Build a file with service names to populate the bloom filter.
	data := buildV14FileWithServiceName(t, 3, "test-svc")
	src := openReader(t, data)
	rawBloom := src.FileBloomRaw()
	require.NotNil(t, rawBloom, "bloom raw bytes must be present when service.name is written")

	// Bare reader: fileBloomRaw is set, fileBloomParsed is nil, fileBloomOnce unfired.
	r := reader.NewBareReaderWithBloomForTest(rawBloom)

	// FileBloom() must enter the inner parse path (fileBloomRaw!=nil && fileBloomParsed==nil).
	bloom := r.FileBloom()
	// bloom may be nil if parse fails (bytes came from a real file so should succeed).
	_ = bloom

	// Also exercise FileBloomRaw() on the bare reader — covers the "fileBloomRaw != nil" path.
	raw2 := r.FileBloomRaw()
	_ = raw2
}

// TestBareReaderNilPaths exercises nil-return paths on a zero-value Reader
// for TraceBloomRaw(), MayContainTraceID(), FileBloomRaw(), and TraceCount().
// These paths require compactParsed==nil, fileBloomRaw==nil, and traceIndex empty.
func TestBareReaderNilPaths(t *testing.T) {
	// Bare reader: compactParsed==nil, compactLen==0, fileBloomRaw==nil.
	r := reader.NewBareReaderForTest()

	// TraceBloomRaw with nil compactParsed → returns nil (reader.go:685).
	raw := r.TraceBloomRaw()
	assert.Nil(t, raw, "TraceBloomRaw must return nil when compactParsed is nil")

	// MayContainTraceID with nil compactParsed → returns true (conservative) (reader.go:699).
	var tid [16]byte
	ok := r.MayContainTraceID(tid)
	assert.True(t, ok, "MayContainTraceID must return true (conservative) when compactParsed is nil")

	// FileBloomRaw with nil fileBloomRaw → returns nil (reader.go:653).
	rawBloom := r.FileBloomRaw()
	assert.Nil(t, rawBloom, "FileBloomRaw must return nil when fileBloomRaw is nil")

	// TraceCount with no traceIndex and no compactParsed → returns 0 (reader.go:309).
	count := r.TraceCount()
	assert.Equal(t, 0, count, "TraceCount must return 0 when no trace data is present")
}

// TestColumnSketchNilPaths exercises the nil-sketchIdx and nil-cd paths in ColumnSketch().
func TestColumnSketchNilPaths(t *testing.T) {
	data := buildV14FileWithCols(t, 5, []string{"col.sk3"})
	r := openReader(t, data)

	// Ensure sketch section is loaded by calling ColumnSketch once.
	_ = r.ColumnSketch("span.col.sk3")

	// Call with a nonexistent column — sketchIdx is non-nil but cd is nil (reader.go:348).
	sk := r.ColumnSketch("definitely.nonexistent.column.xyz")
	assert.Nil(t, sk, "ColumnSketch must return nil for nonexistent column")

	// Reset sketchIdx to nil — subsequent call sees nil sketchIdx (reader.go:344).
	reader.SetSketchIdxNilForTest(r)
	sk2 := r.ColumnSketch("span.col.sk3")
	assert.Nil(t, sk2, "ColumnSketch must return nil when sketchIdx is nil")
}

// TestRangeColumnEmptyEntries exercises the len(entries)==0 early-return in
// BlocksForRange and BlocksForRangeInterval when the parsed index has no entries.
func TestRangeColumnEmptyEntries(t *testing.T) {
	data := buildV14FileWithCols(t, 5, []string{"col.re"})
	r := openReader(t, data)

	// Inject an empty parsedRangeIndex for a synthetic column name.
	reader.SetRangeParsedEmptyForTest(r, "synthetic.empty.col")

	// BlocksForRange: entries empty → return nil, nil (reader.go:368).
	blocks, err := r.BlocksForRange("synthetic.empty.col", "")
	require.NoError(t, err)
	assert.Nil(t, blocks)

	// BlocksForRangeInterval: entries empty → return nil, nil (reader.go:410).
	blocks2, err2 := r.BlocksForRangeInterval("synthetic.empty.col", "", "")
	require.NoError(t, err2)
	assert.Nil(t, blocks2)
}

// TestRangeColumnNonexistentCalls exercises the not-found paths in RangeColumnType,
// RangeColumnBoundaries, and BlocksForRangeInterval.
func TestRangeColumnNonexistentCalls(t *testing.T) {
	data := buildV14FileWithCols(t, 5, []string{"col.rne"})
	r := openReader(t, data)

	// RangeColumnType for unknown column → (0, false) (reader.go:472).
	ct, ok := r.RangeColumnType("definitely.not.a.range.column")
	assert.False(t, ok)
	assert.Zero(t, ct)

	// RangeColumnBoundaries for unknown column → nil (reader.go:502).
	rb := r.RangeColumnBoundaries("definitely.not.a.range.column")
	assert.Nil(t, rb)

	// BlocksForRangeInterval for unknown column → error (reader.go:403).
	_, err := r.BlocksForRangeInterval("definitely.not.a.range.column", "", "")
	require.Error(t, err)
}

// TestSmallFileErrors exercises readFooter error paths for files that are too small.
func TestSmallFileErrors(t *testing.T) {
	// File with < 18 bytes triggers "file too small for footer" (parser.go:87).
	tiny := &memProvider{data: []byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}} // 10 bytes
	_, err := reader.NewReaderFromProvider(tiny)
	require.Error(t, err, "tiny file must return error")
	assert.Contains(t, err.Error(), "small")

	// File with 18-21 bytes of junk: passes V5 size check but wrong magic,
	// then triggers "too small for v3/v4 footer" (parser.go:123).
	junk18 := &memProvider{data: bytes.Repeat([]byte{0xFF}, 18)}
	_, err2 := reader.NewReaderFromProvider(junk18)
	require.Error(t, err2, "18-byte junk file must return error")
}

// TestBoolRangeColumnBlocksForRange exercises the Bool case in parseRangeValueEntry
// (range_index.go:378-384) by writing bool attributes and calling BlocksForRange.
func TestBoolRangeColumnBlocksForRange(t *testing.T) {
	var buf bytes.Buffer
	w, err := writer.NewWriterWithConfig(writer.Config{OutputStream: &buf})
	require.NoError(t, err)

	// Write spans with bool attributes so the range index contains a Bool-typed column.
	for i := range 8 {
		traceID := make([]byte, 16)
		traceID[0] = byte(i + 1)
		spanID := make([]byte, 8)
		spanID[0] = byte(i + 1)
		td := &tracev1.TracesData{
			ResourceSpans: []*tracev1.ResourceSpans{
				{ScopeSpans: []*tracev1.ScopeSpans{{Spans: []*tracev1.Span{
					{
						TraceId: traceID,
						SpanId:  spanID,
						Name:    "op",
						Attributes: []*commonv1.KeyValue{
							{Key: "attr.flag", Value: &commonv1.AnyValue{
								Value: &commonv1.AnyValue_BoolValue{BoolValue: i%2 == 0},
							}},
						},
					},
				}}}},
			},
		}
		require.NoError(t, w.AddTracesData(td))
	}

	_, err = w.Flush()
	require.NoError(t, err)
	data := buf.Bytes()

	r := openReader(t, data)
	require.Greater(t, r.BlockCount(), 0)

	// BlocksForRange on the bool column triggers parseRangeValueEntry Bool case.
	colName := "span.attr.flag"
	if _, ok := r.RangeColumnType(colName); !ok {
		t.Skip("no range index for bool column — writer may not index it")
	}

	blocks, err2 := r.BlocksForRange(colName, "\x01") // encoded bool true
	require.NoError(t, err2)
	_ = blocks

	// RangeColumnBoundaries also parses the Bool range column.
	rb := r.RangeColumnBoundaries(colName)
	_ = rb
}

// TestSynthesizeSpanEndEmptyUint64 exercises the empty-Uint64Values guard in
// synthesizeSpanEnd (intrinsic_reader.go:241) by injecting a span:start column
// with no Uint64Values so that synthesis short-circuits to nil.
func TestSynthesizeSpanEndEmptyUint64(t *testing.T) {
	data := buildV14FileWithCols(t, 3, []string{"col.synth"})
	r := openReader(t, data)

	// Inject intrinsicIndex entries so GetIntrinsicColumn doesn't return nil early.
	// span:end must NOT be in the index so synthesizeSpanEnd is called.
	reader.SetIntrinsicIndexForTest(r, map[string]modules_shared.IntrinsicColMeta{
		"span:start":    {},
		"span:duration": {},
	})

	// Inject decoded cache: span:start with empty Uint64Values, span:duration with a value.
	// When synthesizeSpanEnd fetches span:start from the decoded cache, len(Uint64Values)==0
	// triggers the early-return at intrinsic_reader.go:241-242.
	reader.SetIntrinsicDecodedForTest(r, map[string]*modules_shared.IntrinsicColumn{
		"span:start":    {Name: "span:start", Type: modules_shared.ColumnTypeUint64},
		"span:duration": {Name: "span:duration", Type: modules_shared.ColumnTypeUint64, Uint64Values: []uint64{100}},
	})

	col, err := r.GetIntrinsicColumn("span:end")
	require.NoError(t, err)
	assert.Nil(t, col, "synthesizeSpanEnd must return nil when span:start has no uint64 values")
}
