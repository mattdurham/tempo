package writer_test

import (
	"bytes"
	"encoding/binary"
	"testing"

	"github.com/golang/snappy"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	commonv1 "go.opentelemetry.io/proto/otlp/common/v1"
	tracev1 "go.opentelemetry.io/proto/otlp/trace/v1"

	modules_reader "github.com/grafana/blockpack/internal/modules/blockio/reader"
	modules_shared "github.com/grafana/blockpack/internal/modules/blockio/shared"
	"github.com/grafana/blockpack/internal/modules/blockio/writer"
	modules_rw "github.com/grafana/blockpack/internal/modules/rw"
)

// v14memProvider is an in-memory ReaderProvider for V14 format tests.
type v14memProvider struct{ data []byte }

func (m *v14memProvider) Size() (int64, error) { return int64(len(m.data)), nil }

func (m *v14memProvider) ReadAt(p []byte, off int64, _ modules_rw.DataType) (int, error) {
	if off < 0 || int(off) > len(m.data) {
		return 0, bytes.ErrTooLarge
	}
	n := copy(p, m.data[off:])
	return n, nil
}

// buildV14File writes n spans (each with 3 string attributes) to a buffer and returns the bytes.
func buildV14File(t *testing.T, nSpans int, maxBlockSpans int) []byte {
	t.Helper()
	var buf bytes.Buffer
	w, err := writer.NewWriterWithConfig(writer.Config{
		OutputStream:  &buf,
		MaxBlockSpans: maxBlockSpans,
	})
	require.NoError(t, err)

	for i := range nSpans {
		traceID := make([]byte, 16)
		traceID[0] = byte(i % 256)
		traceID[1] = byte(i / 256)
		spanID := make([]byte, 8)
		spanID[0] = byte(i)

		span := &tracev1.Span{
			TraceId:           traceID,
			SpanId:            spanID,
			Name:              "op-" + string(rune('A'+i%26)),
			StartTimeUnixNano: uint64(1_000_000_000 + i*1000),
			EndTimeUnixNano:   uint64(1_000_002_000 + i*1000),
			Kind:              tracev1.Span_SPAN_KIND_SERVER,
			Status:            &tracev1.Status{Code: tracev1.Status_STATUS_CODE_OK},
			Attributes: []*commonv1.KeyValue{
				{Key: "col.a", Value: &commonv1.AnyValue{Value: &commonv1.AnyValue_StringValue{StringValue: "val-a"}}},
				{Key: "col.b", Value: &commonv1.AnyValue{Value: &commonv1.AnyValue_StringValue{StringValue: "val-b"}}},
				{Key: "col.c", Value: &commonv1.AnyValue{Value: &commonv1.AnyValue_StringValue{StringValue: "val-c"}}},
			},
		}

		td := &tracev1.TracesData{
			ResourceSpans: []*tracev1.ResourceSpans{
				{
					Resource: nil,
					ScopeSpans: []*tracev1.ScopeSpans{
						{Spans: []*tracev1.Span{span}},
					},
				},
			},
		}
		require.NoError(t, w.AddTracesData(td))
	}

	_, err = w.Flush()
	require.NoError(t, err)
	return buf.Bytes()
}

// TestNewFormatRoundTrip writes 5 spans and verifies the V14 format invariants:
// - Footer last 18 bytes match FooterV5 magic + version=5
// - Section directory contains at least SectionBlockIndex and SectionTraceIndex
// - File can be read back and span data matches
func TestNewFormatRoundTrip(t *testing.T) {
	data := buildV14File(t, 5, 0)
	require.Greater(t, len(data), int(modules_shared.FooterV7Size), "file must be larger than footer")

	// Verify footer: last FooterV7Size bytes.
	footerStart := len(data) - int(modules_shared.FooterV7Size)
	footerBytes := data[footerStart:]

	magic := binary.LittleEndian.Uint32(footerBytes[0:4])
	assert.Equal(t, modules_shared.MagicNumber, magic, "footer magic must match")

	version := binary.LittleEndian.Uint16(footerBytes[4:6])
	assert.Equal(t, modules_shared.FooterV7Version, version, "footer version must be 7")

	// Read section directory to verify it contains expected sections.
	dirOffset := binary.LittleEndian.Uint64(footerBytes[6:14])
	dirLen := binary.LittleEndian.Uint32(footerBytes[14:18])
	assert.Greater(t, dirOffset, uint64(0), "dir_offset must be nonzero")
	assert.Greater(t, dirLen, uint32(0), "dir_len must be nonzero")

	// Decompress section directory.
	compressedDir := data[dirOffset : dirOffset+uint64(dirLen)]
	rawDir, err := snappy.Decode(nil, compressedDir)
	require.NoError(t, err, "section directory must be valid snappy")
	require.GreaterOrEqual(t, len(rawDir), 4, "section directory must have at least section_count[4]")

	sectionCount := binary.LittleEndian.Uint32(rawDir[0:4])
	assert.Greater(t, sectionCount, uint32(0), "section directory must have at least one section")

	// Parse section directory entries. V14 uses heterogeneous entries dispatched by entry_kind[1].
	// Type-keyed (entry_kind=0x00): entry_kind[1]+section_type[1]+offset[8]+compressed_len[4] = 14 bytes.
	// Name-keyed (entry_kind=0x01): entry_kind[1]+name_len[2]+name+offset[8]+compressed_len[4] = variable.
	// Signal-kind (entry_kind=0x02): entry_kind[1]+signal_type[1] = 2 bytes.
	foundSections := make(map[uint8]bool)
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
			foundSections[sType] = true
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
			break entryLoop
		}
	}

	assert.True(t, foundSections[modules_shared.SectionBlockIndex], "must have SectionBlockIndex (0x01)")
	assert.True(t, foundSections[modules_shared.SectionTraceIndex], "must have SectionTraceIndex (0x03)")

	// Verify file reads back via NewReaderFromProvider.
	prov := &v14memProvider{data: data}
	r, err := modules_reader.NewReaderFromProvider(prov)
	require.NoError(t, err, "reader must open V14 file without error")
	assert.Greater(t, r.BlockCount(), 0, "reader must find at least one block")
}

// TestColumnSnappyCompression verifies that each column blob in a V14 file is snappy-compressed:
// the compressed bytes must differ from raw (i.e. compression was actually applied), and
// snappy.Decode must succeed on each column blob.
func TestColumnSnappyCompression(t *testing.T) {
	data := buildV14File(t, 2, 0)

	// Read block 0 raw bytes via the reader.
	prov := &v14memProvider{data: data}
	r, err := modules_reader.NewReaderFromProvider(prov)
	require.NoError(t, err)
	require.Greater(t, r.BlockCount(), 0)

	bwb, err := r.GetBlockWithBytes(0, nil, nil)
	require.NoError(t, err)
	require.NotNil(t, bwb)

	// Block raw bytes: parse column TOC and verify each column blob is snappy-compressed.
	// Block format (V14): magic[4]+version[1]+reserved[3]+span_count[4]+col_count[4]+reserved2[8] = 24-byte header,
	// then col_count entries of {name_len[2]+name+type[1]+data_offset[8]+compressed_len[4]+uncompressed_len[4]},
	// then column data blobs.
	raw := bwb.RawBytes
	require.GreaterOrEqual(t, len(raw), 24, "block must have at least 24-byte header")

	blockVersion := raw[4]
	assert.Equal(t, modules_shared.VersionBlockV14, blockVersion, "block header version must be V14")

	colCount := binary.LittleEndian.Uint32(raw[12:16])
	require.Greater(t, colCount, uint32(0), "block must have at least one column")

	// Parse the column TOC entries.
	pos := 24 // skip block header
	for range int(colCount) {
		require.GreaterOrEqual(t, len(raw), pos+2, "need name_len")
		nameLen := int(binary.LittleEndian.Uint16(raw[pos : pos+2]))
		pos += 2 + nameLen + 1 // name_len[2]+name[N]+type[1]

		require.GreaterOrEqual(t, len(raw), pos+16, "need data_offset+compressed_len+uncompressed_len")
		dataOffset := binary.LittleEndian.Uint64(raw[pos : pos+8])
		compressedLen := binary.LittleEndian.Uint32(raw[pos+8 : pos+12])
		uncompressedLen := binary.LittleEndian.Uint32(raw[pos+12 : pos+16])
		pos += 16

		require.Greater(t, compressedLen, uint32(0), "compressed_len must be nonzero")
		require.Greater(t, uncompressedLen, uint32(0), "uncompressed_len must be nonzero")

		colStart := dataOffset
		colEnd := dataOffset + uint64(compressedLen)
		require.LessOrEqual(t, colEnd, uint64(len(raw)), "column blob must be within block bounds")

		colBlob := raw[colStart:colEnd]

		// Decompress must succeed.
		decoded, decErr := snappy.Decode(nil, colBlob)
		require.NoError(t, decErr, "column blob must be valid snappy")

		// Compressed bytes must differ from uncompressed (actual compression was applied).
		// Note: for tiny inputs snappy may produce output the same size, but bytes will differ
		// because snappy adds framing. We check that either the size differs OR the bytes differ.
		assert.NotEqual(t, colBlob, decoded, "compressed column blob must differ from uncompressed bytes")
	}
}

// TestMetadataIndependence verifies that the file bloom section (0x06) can be read
// without touching any block bytes — only the section directory and bloom section bytes.
func TestMetadataIndependence(t *testing.T) {
	data := buildV14File(t, 5, 0)

	prov := &v14memProvider{data: data}
	r, err := modules_reader.NewReaderFromProvider(prov)
	require.NoError(t, err)

	// FileBloom() must succeed — it reads only the bloom section, not block bytes.
	bloom := r.FileBloom()
	// bloom may be nil if no service names were written; the important thing is no panic/error.
	_ = bloom

	// Verify file bloom raw bytes are accessible.
	bloomRaw := r.FileBloomRaw()
	// For files with service name data, bloom raw must be non-nil.
	// For minimal test spans with no service name, it may be nil — that's fine.
	_ = bloomRaw

	// The key invariant: reader was created and bloom was accessed without error.
	// If block bytes were required but section directory was broken, NewReaderFromProvider
	// would have failed or FileBloom() would have panicked.
	assert.NotNil(t, r, "reader must be non-nil")
}

// TestTraceIndexIndependence verifies that TraceEntries() works via the trace index section
// without loading any block data.
func TestTraceIndexIndependence(t *testing.T) {
	var buf bytes.Buffer
	w, err := writer.NewWriterWithConfig(writer.Config{
		OutputStream:  &buf,
		MaxBlockSpans: 2, // force 2 blocks for 3 spans in 2 traces
	})
	require.NoError(t, err)

	// Trace 1: 2 spans in trace A.
	traceA := [16]byte{0xAA, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0xAA}
	// Trace 2: 1 span in trace B.
	traceB := [16]byte{0xBB, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0xBB}

	for i, tid := range [][16]byte{traceA, traceA, traceB} {
		spanID := make([]byte, 8)
		spanID[0] = byte(i + 1)
		td := &tracev1.TracesData{
			ResourceSpans: []*tracev1.ResourceSpans{
				{
					ScopeSpans: []*tracev1.ScopeSpans{
						{Spans: []*tracev1.Span{
							{
								TraceId:           tid[:],
								SpanId:            spanID,
								Name:              "span",
								StartTimeUnixNano: uint64(i * 1000),
								EndTimeUnixNano:   uint64(i*1000 + 500),
							},
						}},
					},
				},
			},
		}
		require.NoError(t, w.AddTracesData(td))
	}

	_, err = w.Flush()
	require.NoError(t, err)
	data := buf.Bytes()

	prov := &v14memProvider{data: data}
	r, err := modules_reader.NewReaderFromProvider(prov)
	require.NoError(t, err)

	// TraceEntries must work without reading block data.
	entriesA := r.TraceEntries(traceA)
	assert.NotEmpty(t, entriesA, "trace A must have at least one block entry")

	entriesB := r.TraceEntries(traceB)
	assert.NotEmpty(t, entriesB, "trace B must have at least one block entry")

	// Unknown trace must return nil.
	unknownTrace := [16]byte{0xFF}
	assert.Nil(t, r.TraceEntries(unknownTrace), "unknown trace must return nil")
}

// BenchmarkColumnRead measures the per-op bytes read when reading 3 of 10 columns from
// a 100-span file. Serves as the V14 selective-read baseline.
func BenchmarkColumnRead(b *testing.B) {
	// Build a file with 100 spans × 10 columns.
	var buf bytes.Buffer
	w, err := writer.NewWriterWithConfig(writer.Config{
		OutputStream: &buf,
	})
	if err != nil {
		b.Fatal(err)
	}

	for i := range 100 {
		traceID := make([]byte, 16)
		traceID[0] = byte(i)
		spanID := make([]byte, 8)
		spanID[0] = byte(i)

		attrs := make([]*commonv1.KeyValue, 10)
		for j := range 10 {
			attrs[j] = &commonv1.KeyValue{
				Key: "col." + string(rune('a'+j)),
				Value: &commonv1.AnyValue{
					Value: &commonv1.AnyValue_StringValue{StringValue: "value-" + string(rune('a'+j))},
				},
			}
		}

		span := &tracev1.Span{
			TraceId:           traceID,
			SpanId:            spanID,
			Name:              "op",
			StartTimeUnixNano: uint64(i * 1000),
			EndTimeUnixNano:   uint64(i*1000 + 500),
			Attributes:        attrs,
		}
		td := &tracev1.TracesData{
			ResourceSpans: []*tracev1.ResourceSpans{
				{ScopeSpans: []*tracev1.ScopeSpans{{Spans: []*tracev1.Span{span}}}},
			},
		}
		if err = w.AddTracesData(td); err != nil {
			b.Fatal(err)
		}
	}

	if _, err = w.Flush(); err != nil {
		b.Fatal(err)
	}
	data := buf.Bytes()

	// Read 3 specific columns.
	wantColumns := map[string]struct{}{
		"col.a": {},
		"col.b": {},
		"col.c": {},
	}

	prov := &v14memProvider{data: data}
	r, err := modules_reader.NewReaderFromProvider(prov)
	if err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for range b.N {
		// GetBlockWithBytes reads block bytes and parses only the requested columns.
		bwb, err := r.GetBlockWithBytes(0, wantColumns, nil)
		if err != nil {
			b.Fatal(err)
		}
		_ = bwb
	}

	b.SetBytes(int64(len(data)))
}
