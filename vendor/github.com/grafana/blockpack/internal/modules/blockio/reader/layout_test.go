package reader_test

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"regexp"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	commonv1 "go.opentelemetry.io/proto/otlp/common/v1"
	tracev1 "go.opentelemetry.io/proto/otlp/trace/v1"

	"github.com/grafana/blockpack/internal/modules/blockio/reader"
)

// sectionNames returns a set of section names from a FileLayoutReport for quick lookup.
func sectionNames(r *reader.FileLayoutReport) map[string]bool {
	m := make(map[string]bool, len(r.Sections))
	for _, s := range r.Sections {
		m[s.Section] = true
	}
	return m
}

// TestFileLayout_ByteInvariant verifies that every byte in a file is accounted for,
// sections are sorted by offset, and JSON round-trip succeeds.
func TestFileLayout_ByteInvariant(t *testing.T) {
	var buf bytes.Buffer
	w := mustNewWriter(t, &buf, 0)

	traceID := [16]byte{0x01, 0x02, 0x03}
	for i := range 8 {
		span := makeSpan(
			traceID,
			fixedSpanID(byte(i)),
			"op.layout",
			uint64(1_000_000_000+i*1000),
			uint64(1_000_002_000+i*1000),
			tracev1.Span_SPAN_KIND_CLIENT,
			[]*commonv1.KeyValue{
				stringAttr("http.method", "GET"),
				int64Attr("http.status_code", 200),
				float64Attr("custom.latency", 1.5),
				boolAttr("error", false),
				bytesAttr("trace.bytes", []byte{0xDE, 0xAD}),
			},
		)
		addSpanToWriter(t, w, traceID, span, map[string]any{
			"service.name": "layout-test",
			"host.name":    "localhost",
		})
	}

	flushToBuffer(t, &buf, w)

	r := openReader(t, buf.Bytes())
	report, err := r.FileLayout()
	require.NoError(t, err)
	require.NotNil(t, report)
	require.Greater(t, len(report.Sections), 0)

	// Byte invariant: sum of physical (non-logical) section CompressedSize values must equal FileSize.
	// Logical sections (IsLogical=true) are sub-sections within the decompressed metadata buffer
	// for V12 files — they describe structure within the already-counted metadata.compressed blob
	// and must NOT be counted again toward the physical file size.
	var totalSize int64
	for _, s := range report.Sections {
		if s.IsLogical {
			continue
		}
		require.Greater(t, s.CompressedSize, int64(0), "section %q has zero or negative size", s.Section)
		totalSize += s.CompressedSize
	}

	require.Equal(t, report.FileSize, totalSize,
		"sum of all physical section CompressedSize values must equal FileSize")

	// Sections must be sorted by Offset ascending.
	// Only physical sections have file-relative offsets; logical sections have offsets
	// relative to the decompressed metadata buffer and must be excluded from this check.
	var physicalSections []reader.FileLayoutSection
	for _, s := range report.Sections {
		if !s.IsLogical {
			physicalSections = append(physicalSections, s)
		}
	}

	for i := 1; i < len(physicalSections); i++ {
		require.LessOrEqual(t, physicalSections[i-1].Offset, physicalSections[i].Offset,
			"sections[%d] offset %d must be <= sections[%d] offset %d",
			i-1, physicalSections[i-1].Offset, i, physicalSections[i].Offset)
	}

	// JSON round-trip.
	jsonData, err := json.Marshal(report)
	require.NoError(t, err)
	require.NotEmpty(t, jsonData)

	var decoded reader.FileLayoutReport
	require.NoError(t, json.Unmarshal(jsonData, &decoded))
	require.Equal(t, report.FileSize, decoded.FileSize)
	require.Equal(t, report.BlockCount, decoded.BlockCount)
	require.Equal(t, len(report.Sections), len(decoded.Sections))

	// Required section kinds.
	names := make(map[string]bool, len(report.Sections))
	for _, s := range report.Sections {
		names[s.Section] = true
	}

	require.True(t, names["footer"], "footer section must be present")
	require.True(t, names["file_header"], "file_header section must be present")
	require.True(t, names["metadata.compressed"], "metadata.compressed must be present")
	require.True(t, names["block[0].header"], "block[0].header must be present")

	// Column data sections must have encoding and type populated.
	for _, s := range report.Sections {
		if len(s.Section) > 0 && s.ColumnName != "" {
			require.NotEmpty(t, s.ColumnType, "section %q must have ColumnType", s.Section)
		}
	}

	// All column data sections must have Encoding populated.
	for _, s := range report.Sections {
		if len(s.Section) > 10 && s.Section[len(s.Section)-5:] == "].data" {
			require.NotEmpty(t, s.Encoding, "data section %q must have Encoding", s.Section)
		}
	}
}

// TestFileLayout_EmptyFile verifies that the byte invariant holds for a file with no spans.
func TestFileLayout_EmptyFile(t *testing.T) {
	var buf bytes.Buffer
	w := mustNewWriter(t, &buf, 0)
	flushToBuffer(t, &buf, w)

	r := openReader(t, buf.Bytes())
	require.Equal(t, 0, r.BlockCount())

	report, err := r.FileLayout()
	require.NoError(t, err)
	require.NotNil(t, report)
	require.Equal(t, 0, report.BlockCount)

	// Byte invariant must hold even for an empty file.
	// Only physical (non-logical) sections count toward disk usage.
	var totalSize int64
	for _, s := range report.Sections {
		if !s.IsLogical {
			totalSize += s.CompressedSize
		}
	}

	require.Equal(t, report.FileSize, totalSize,
		"byte invariant must hold for empty file: sum=%d FileSize=%d", totalSize, report.FileSize)

	// Must still have footer, file_header, metadata sections.
	names := make(map[string]bool)
	for _, s := range report.Sections {
		names[s.Section] = true
	}

	require.True(t, names["footer"])
	require.True(t, names["file_header"])
	require.True(t, names["metadata.compressed"])
}

// TestFileLayout_MultiBlock verifies the invariant with multiple blocks.
func TestFileLayout_MultiBlock(t *testing.T) {
	var buf bytes.Buffer
	const maxPerBlock = 3
	w := mustNewWriter(t, &buf, maxPerBlock)

	traceID := [16]byte{0xAB}
	for i := range 9 {
		span := makeSpan(
			traceID,
			fixedSpanID(byte(i)),
			"op.multi",
			uint64(1_000_000_000+i*1000),
			uint64(1_000_002_000+i*1000),
			tracev1.Span_SPAN_KIND_SERVER,
			[]*commonv1.KeyValue{stringAttr("svc", "multi-test")},
		)
		addSpanToWriter(t, w, traceID, span, map[string]any{"service.name": "multi"})
	}

	flushToBuffer(t, &buf, w)

	r := openReader(t, buf.Bytes())
	require.Greater(t, r.BlockCount(), 1, "expect multiple blocks")

	report, err := r.FileLayout()
	require.NoError(t, err)
	require.NotNil(t, report)

	// Byte invariant: only physical (non-logical) sections contribute to FileSize.
	var totalSize int64
	for _, s := range report.Sections {
		if !s.IsLogical {
			totalSize += s.CompressedSize
		}
	}

	require.Equal(t, report.FileSize, totalSize)
	require.Equal(t, r.BlockCount(), report.BlockCount)

	// Every block must have a header section.
	for i := range r.BlockCount() {
		key := fmt.Sprintf("block[%d].header", i)
		found := false
		for _, s := range report.Sections {
			if s.Section == key {
				found = true
				break
			}
		}
		require.True(t, found, "expected section %q", key)
	}
}

// TestFileLayout_LargeScale_ByteAccounting builds 100 traces with 500 spans each
// (50,000 total spans), runs the full layout analysis, and verifies:
//   - Every byte in the file is accounted for (sum of CompressedSize == FileSize)
//   - Sections are sorted by offset with no gaps or overlaps
//   - Column data sections report both CompressedSize and UncompressedSize
//   - UncompressedSize > 0 for all column data sections
//   - JSON round-trip preserves all fields
//   - Block structure is correct (headers, metadata, column data present per block)
func TestFileLayout_LargeScale_ByteAccounting(t *testing.T) {
	const (
		numTraces       = 100
		spansPerTrace   = 500
		totalSpans      = numTraces * spansPerTrace
		maxSpansPerBlk  = 2000
		expectedMinBlks = totalSpans / maxSpansPerBlk
	)

	var buf bytes.Buffer
	w := mustNewWriter(t, &buf, maxSpansPerBlk)

	// Realistic service names, HTTP methods, and status codes for varied attribute values.
	services := []string{
		"frontend", "cart", "checkout", "payment", "shipping",
		"productcatalog", "currency", "recommendation", "ad-service", "redis",
	}
	methods := []string{"GET", "POST", "PUT", "DELETE"}
	statusCodes := []int64{200, 201, 204, 301, 400, 403, 404, 500, 502, 503}
	spanKinds := []tracev1.Span_SpanKind{
		tracev1.Span_SPAN_KIND_CLIENT,
		tracev1.Span_SPAN_KIND_SERVER,
		tracev1.Span_SPAN_KIND_PRODUCER,
		tracev1.Span_SPAN_KIND_CONSUMER,
		tracev1.Span_SPAN_KIND_INTERNAL,
	}

	for traceIdx := range numTraces {
		traceID := [16]byte{}
		binary.BigEndian.PutUint64(traceID[0:], uint64(traceIdx+1))
		binary.BigEndian.PutUint64(traceID[8:], uint64(traceIdx*7+3))

		svc := services[traceIdx%len(services)]
		baseTime := uint64(1_700_000_000_000_000_000) + uint64(traceIdx)*1_000_000_000

		for spanIdx := range spansPerTrace {
			globalIdx := traceIdx*spansPerTrace + spanIdx
			method := methods[globalIdx%len(methods)]
			code := statusCodes[globalIdx%len(statusCodes)]
			kind := spanKinds[globalIdx%len(spanKinds)]

			span := makeSpan(
				traceID,
				fixedSpanID(byte(globalIdx%256)),
				fmt.Sprintf("%s.op_%d", svc, spanIdx%20),
				baseTime+uint64(spanIdx)*2_000_000,
				baseTime+uint64(spanIdx)*2_000_000+uint64(1_000_000+globalIdx%5_000_000),
				kind,
				[]*commonv1.KeyValue{
					stringAttr("http.method", method),
					int64Attr("http.status_code", code),
					float64Attr("http.response_time_ms", float64(globalIdx%1000)/10.0),
					boolAttr("error", code >= 500),
					bytesAttr("span.fingerprint", []byte{
						byte(globalIdx % 256), byte((globalIdx >> 8) % 256), //nolint:gosec
						byte((globalIdx >> 16) % 256), byte((globalIdx >> 24) % 256), //nolint:gosec
					}),
					stringAttr("http.url", fmt.Sprintf("/%s/api/v1/resource/%d", svc, spanIdx%100)),
					stringAttr("net.peer.name", fmt.Sprintf("%s-%d.internal.svc", svc, spanIdx%5)),
					int64Attr("net.peer.port", int64(8080+spanIdx%4)),
				},
			)
			addSpanToWriter(t, w, traceID, span, map[string]any{
				"service.name":    svc,
				"host.name":       fmt.Sprintf("host-%d.region-1", traceIdx%10),
				"deployment.env":  []string{"prod", "staging", "dev"}[traceIdx%3],
				"service.version": fmt.Sprintf("v1.%d.0", traceIdx%5),
			})
		}
	}

	flushToBuffer(t, &buf, w)
	fileBytes := buf.Bytes()

	r := openReader(t, fileBytes)
	require.GreaterOrEqual(t, r.BlockCount(), expectedMinBlks,
		"expected at least %d blocks for %d spans", expectedMinBlks, totalSpans)

	report, err := r.FileLayout()
	require.NoError(t, err)
	require.NotNil(t, report)

	t.Logf("file_size=%d blocks=%d sections=%d", report.FileSize, report.BlockCount, len(report.Sections))

	// --- Invariant 1: every physical byte accounted for ---
	// Logical sections (IsLogical=true) are sub-sections within the decompressed V12 metadata
	// buffer. They describe structure inside the already-counted metadata.compressed blob and
	// must not be counted again toward disk size.
	var totalCompressed int64
	for _, s := range report.Sections {
		if s.IsLogical {
			continue
		}
		require.Greater(t, s.CompressedSize, int64(0),
			"section %q has zero or negative CompressedSize", s.Section)
		totalCompressed += s.CompressedSize
	}

	require.Equal(t, report.FileSize, totalCompressed,
		"byte invariant: sum(CompressedSize)=%d != FileSize=%d", totalCompressed, report.FileSize)

	// --- Invariant 2: sorted by offset, no overlaps (physical sections only) ---
	var physicalSections []reader.FileLayoutSection
	for _, s := range report.Sections {
		if !s.IsLogical {
			physicalSections = append(physicalSections, s)
		}
	}

	for i := 1; i < len(physicalSections); i++ {
		prev := physicalSections[i-1]
		curr := physicalSections[i]
		prevEnd := prev.Offset + prev.CompressedSize
		require.LessOrEqual(t, prev.Offset, curr.Offset,
			"sections[%d].Offset=%d > sections[%d].Offset=%d", i-1, prev.Offset, i, curr.Offset)
		require.LessOrEqual(t, prevEnd, curr.Offset,
			"sections[%d] [%d,%d) overlaps sections[%d] [%d,%d+%d)",
			i-1, prev.Offset, prevEnd, i, curr.Offset, curr.Offset, curr.CompressedSize)
	}

	// --- Invariant 3: column data sections have compressed + uncompressed sizes ---
	// Note: UncompressedSize may be less than CompressedSize for small columns where
	// zstd frame overhead (~13 bytes) exceeds the compression savings.
	var columnDataSections int
	for _, s := range report.Sections {
		if !strings.HasSuffix(s.Section, "].data") {
			continue
		}

		columnDataSections++

		assert.Greater(t, s.CompressedSize, int64(0),
			"column data %q: CompressedSize must be > 0", s.Section)
		assert.Greater(t, s.UncompressedSize, int64(0),
			"column data %q: UncompressedSize must be > 0", s.Section)
		assert.NotEmpty(t, s.Encoding,
			"column data %q: Encoding must be populated", s.Section)
		assert.NotEmpty(t, s.ColumnType,
			"column data %q: ColumnType must be populated", s.Section)
		assert.NotEmpty(t, s.ColumnName,
			"column data %q: ColumnName must be populated", s.Section)
	}

	require.Greater(t, columnDataSections, 0, "expected column data sections in layout")
	t.Logf("column_data_sections=%d", columnDataSections)

	// --- Invariant 4: block structure ---
	require.Equal(t, r.BlockCount(), report.BlockCount)

	for blockIdx := range report.BlockCount {
		prefix := fmt.Sprintf("block[%d]", blockIdx)

		// Each block must have a header.
		hasHeader := false
		hasColumnData := false
		hasColumnMeta := false

		for _, s := range report.Sections {
			switch {
			case s.Section == prefix+".header":
				hasHeader = true
			case s.Section == prefix+".column_metadata":
				hasColumnMeta = true
			case strings.HasPrefix(s.Section, prefix+".column[") && strings.HasSuffix(s.Section, "].data"):
				hasColumnData = true
			}
		}

		assert.True(t, hasHeader, "block %d: missing header section", blockIdx)
		assert.True(t, hasColumnMeta, "block %d: missing column_metadata section", blockIdx)
		assert.True(t, hasColumnData, "block %d: missing column data sections", blockIdx)
	}

	// --- Invariant 5: required metadata sections ---
	sectionNames := make(map[string]bool, len(report.Sections))
	for _, s := range report.Sections {
		sectionNames[s.Section] = true
	}

	require.True(t, sectionNames["footer"], "missing footer")
	require.True(t, sectionNames["file_header"], "missing file_header")
	// V12+ files (snappy-compressed metadata) expose a single "metadata.compressed" section
	// instead of individual sub-sections; both paths satisfy the byte invariant.
	if report.FileVersion >= 12 {
		require.True(t, sectionNames["metadata.compressed"], "V12+: missing metadata.compressed")
	} else {
		require.True(t, sectionNames["metadata.block_index"], "missing metadata.block_index")
		require.True(t, sectionNames["metadata.trace_index"], "missing metadata.trace_index")
		require.True(t, sectionNames["metadata.column_index"], "missing metadata.column_index")
	}

	// --- Invariant 6: JSON round-trip preserves all fields ---
	jsonData, err := json.Marshal(report)
	require.NoError(t, err)

	var decoded reader.FileLayoutReport
	require.NoError(t, json.Unmarshal(jsonData, &decoded))

	require.Equal(t, report.FileSize, decoded.FileSize)
	require.Equal(t, report.BlockCount, decoded.BlockCount)
	require.Equal(t, report.FileVersion, decoded.FileVersion)
	require.Equal(t, len(report.Sections), len(decoded.Sections))

	// Verify UncompressedSize survives JSON round-trip for column data sections.
	for i, orig := range report.Sections {
		dec := decoded.Sections[i]
		assert.Equal(t, orig.CompressedSize, dec.CompressedSize,
			"section %q: CompressedSize mismatch after JSON round-trip", orig.Section)
		assert.Equal(t, orig.UncompressedSize, dec.UncompressedSize,
			"section %q: UncompressedSize mismatch after JSON round-trip", orig.Section)
	}

	// --- Summary logging ---
	var totalUncompressed int64
	for _, s := range report.Sections {
		if s.UncompressedSize > 0 {
			totalUncompressed += s.UncompressedSize
		}
	}

	t.Logf("total_compressed=%d total_uncompressed=%d ratio=%.2fx",
		totalCompressed, totalUncompressed,
		float64(totalUncompressed)/float64(totalCompressed))
}

// TestFileLayout_V13Only_NoLegacySubsections verifies that layoutMetadata() never emits
// legacy section names.
func TestFileLayout_V13Only_NoLegacySubsections(t *testing.T) {
	var buf bytes.Buffer
	w := mustNewWriter(t, &buf, 0)
	traceID := [16]byte{0x01, 0x02, 0x03}
	for i := range 4 {
		span := makeSpan(traceID, fixedSpanID(byte(i)), "op",
			1_000_000_000+uint64(i)*1000, 1_000_000_000+uint64(i)*1000+1000,
			tracev1.Span_SPAN_KIND_CLIENT, nil)
		addSpanToWriter(t, w, traceID, span, map[string]any{"service.name": "svc"})
	}
	flushToBuffer(t, &buf, w)

	r := openReader(t, buf.Bytes())

	report, err := r.FileLayout()
	require.NoError(t, err)

	require.GreaterOrEqual(t, report.FileVersion, uint8(12))

	legacy := []string{
		"metadata.block_index",
		"metadata.column_index",
		"metadata.trace_index",
	}
	names := sectionNames(report)
	for _, name := range legacy {
		assert.False(t, names[name], "V12 file must not emit legacy section %q", name)
	}
	for name := range names {
		if strings.HasPrefix(name, "metadata.range_index.column[") {
			// These are now logical sub-sections — they may exist, but only with IsLogical=true.
			for _, s := range report.Sections {
				if s.Section == name {
					assert.True(t, s.IsLogical, "range_index section %q must be logical in V12", name)
				}
			}
		}
	}
	assert.True(t, names["metadata.compressed"], "metadata.compressed must be present")
}

// TestFileLayout_IntrinsicPageBreakdown verifies that for a paged intrinsic column,
// FileLayout() emits one physical section per page with RowCount > 0.
func TestFileLayout_IntrinsicPageBreakdown(t *testing.T) {
	// IntrinsicPageSize is 10,000 — write >10k spans to trigger paged format.
	var buf bytes.Buffer
	w := mustNewWriter(t, &buf, 2000)
	for i := range 12_000 {
		traceID := [16]byte{}
		binary.BigEndian.PutUint64(traceID[0:], uint64(i+1))
		span := makeSpan(traceID, fixedSpanID(byte(i%256)), "op",
			1_000_000_000+uint64(i)*1000, 1_000_000_000+uint64(i)*1000+500,
			tracev1.Span_SPAN_KIND_CLIENT, nil)
		addSpanToWriter(t, w, traceID, span, map[string]any{"service.name": "paged-svc"})
	}
	flushToBuffer(t, &buf, w)

	r := openReader(t, buf.Bytes())
	report, err := r.FileLayout()
	require.NoError(t, err)

	pagePattern := regexp.MustCompile(`^intrinsic\.column\[(.+)\]\.page\[(\d+)\]$`)
	colPages := make(map[string][]reader.FileLayoutSection)
	for _, s := range report.Sections {
		if m := pagePattern.FindStringSubmatch(s.Section); m != nil {
			colPages[m[1]] = append(colPages[m[1]], s)
		}
	}

	require.NotEmpty(t, colPages,
		"12k spans must produce at least one paged intrinsic column (IntrinsicPageSize=10000)")

	for colName, pages := range colPages {
		require.GreaterOrEqual(t, len(pages), 2,
			"column %q: 12k spans should produce at least 2 pages", colName)
		for _, pg := range pages {
			assert.False(t, pg.IsLogical, "intrinsic page section must be physical: %s", pg.Section)
			assert.Greater(t, pg.CompressedSize, int64(0),
				"page section %q must have CompressedSize>0", pg.Section)
			assert.Greater(t, pg.RowCount, 0,
				"page section %q must have RowCount>0 for col %s", pg.Section, colName)
		}
		t.Logf("column %q: %d pages", colName, len(pages))
	}

	// Verify page_toc sections exist for paged columns.
	for colName := range colPages {
		tocSection := "intrinsic.column[" + colName + "].page_toc"
		found := false
		for _, s := range report.Sections {
			if s.Section == tocSection {
				found = true
				assert.Greater(t, s.CompressedSize, int64(0), "page_toc must have size")
				break
			}
		}
		assert.True(t, found, "paged column %q must have a page_toc section", colName)
	}

	// Byte invariant must still hold.
	var total int64
	for _, s := range report.Sections {
		if !s.IsLogical {
			total += s.CompressedSize
		}
	}
	assert.Equal(t, report.FileSize, total, "byte invariant must hold with page sections")
}

// TestFileLayout_SketchActualBytes verifies TotalBytes is populated and that
// ColumnSketchStat has CMSBytes and TopKBytes fields.
func TestFileLayout_SketchActualBytes(t *testing.T) {
	var buf bytes.Buffer
	w := mustNewWriter(t, &buf, 200)
	for i := range 400 {
		traceID := [16]byte{byte(i)}
		span := makeSpan(traceID, fixedSpanID(byte(i%256)), "op",
			1_000_000_000+uint64(i)*1000, 1_000_000_000+uint64(i)*1000+500,
			tracev1.Span_SPAN_KIND_CLIENT,
			[]*commonv1.KeyValue{stringAttr("env", "prod")})
		addSpanToWriter(t, w, traceID, span, map[string]any{"service.name": "svc"})
	}
	flushToBuffer(t, &buf, w)

	r := openReader(t, buf.Bytes())
	report, err := r.FileLayout()
	require.NoError(t, err)

	if report.SketchIndex == nil {
		t.Skip("no sketch index in this file")
	}

	si := report.SketchIndex
	assert.Greater(t, si.TotalBytes, 0, "TotalBytes must be positive")

	for blockIdx, block := range si.Blocks {
		for _, col := range block.Columns {
			assert.Greater(t, col.CMSBytes, 0,
				"block %d col %q: CMSBytes must be >0", blockIdx, col.ColumnName)
			_ = col.TopKBytes // field must exist (compile check)
		}
	}
}

// TestFileLayout_RangeIndexBucketBoundaries verifies that every RangeIndexBucket has End
// populated, and every RangeIndexColumn has BucketMin and BucketMax.
func TestFileLayout_RangeIndexBucketBoundaries(t *testing.T) {
	var buf bytes.Buffer
	w := mustNewWriter(t, &buf, 50)
	for i := range 200 {
		traceID := [16]byte{byte(i)}
		span := makeSpan(traceID, fixedSpanID(byte(i%256)), "op",
			1_000_000_000+uint64(i)*1_000_000,
			1_000_000_000+uint64(i)*1_000_000+uint64(i+1)*100_000,
			tracev1.Span_SPAN_KIND_CLIENT, nil)
		addSpanToWriter(t, w, traceID, span, map[string]any{"service.name": "svc"})
	}
	flushToBuffer(t, &buf, w)

	r := openReader(t, buf.Bytes())
	report, err := r.FileLayout()
	require.NoError(t, err)

	if len(report.RangeIndex) == 0 {
		t.Skip("no range index columns in this file")
	}

	// Only numeric range columns have BucketMin/BucketMax populated.
	numericTypes := map[string]bool{
		"RangeInt64": true, "RangeUint64": true, "RangeFloat64": true, "RangeDuration": true,
	}

	for _, col := range report.RangeIndex {
		if numericTypes[col.ColumnType] {
			assert.NotEmpty(t, col.BucketMin,
				"column %q: BucketMin must be populated for numeric type %q", col.ColumnName, col.ColumnType)
			assert.NotEmpty(t, col.BucketMax,
				"column %q: BucketMax must be populated for numeric type %q", col.ColumnName, col.ColumnType)
			for i, bucket := range col.Buckets {
				assert.NotEmpty(t, bucket.End,
					"column %q bucket[%d]: End must be populated", col.ColumnName, i)
			}
		}
	}
}

// TestFileLayout_FileBloomSection verifies that when a file has a FileBloom section,
// FileLayoutReport.FileBloom is populated with TotalBytes > 0 and at least one column entry.
func TestFileLayout_FileBloomSection(t *testing.T) {
	var buf bytes.Buffer
	w := mustNewWriter(t, &buf, 100)
	for i := range 300 {
		traceID := [16]byte{byte(i)}
		span := makeSpan(traceID, fixedSpanID(byte(i%256)), "op",
			1_000_000_000+uint64(i)*1000, 1_000_000_000+uint64(i)*1000+500,
			tracev1.Span_SPAN_KIND_CLIENT, nil)
		addSpanToWriter(t, w, traceID, span, map[string]any{
			"service.name": fmt.Sprintf("svc-%d", i%5),
		})
	}
	flushToBuffer(t, &buf, w)

	r := openReader(t, buf.Bytes())
	report, err := r.FileLayout()
	require.NoError(t, err)

	if report.FileBloom == nil {
		t.Skip("no FileBloom section in this file")
	}

	fb := report.FileBloom
	assert.Greater(t, fb.TotalBytes, 0, "FileBloom.TotalBytes must be > 0")
	assert.Greater(t, len(fb.Columns), 0, "FileBloom.Columns must be non-empty")

	for _, col := range fb.Columns {
		assert.NotEmpty(t, col.ColumnName, "FileBloom column name must be set")
	}

	// Byte invariant must still hold (FileBloom is logical — no double counting).
	var total int64
	for _, s := range report.Sections {
		if !s.IsLogical {
			total += s.CompressedSize
		}
	}
	assert.Equal(t, report.FileSize, total)
}
