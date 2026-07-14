package vblockpack

// vcntwriter_test.go — tests for vcntAccumulator's per-span minute bucketing
// (see Phase 4 of the minute-aligned-timestamp plan). These tests exercise the
// new bucket-aware accumulator API (inc/addAttrs taking a minute-bucket
// parameter, snapshot() for inspecting accumulated records without going
// through flush's store I/O, and flush()'s new two-window-free signature) —
// none of which exist yet on vcntAccumulator, so this file is expected to fail
// to compile until vcntwriter.go is restructured (TDD red step).

import (
	"path"
	"sync"
	"testing"

	"github.com/golang/snappy"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	blockpack "github.com/grafana/blockpack"
	tempopb "github.com/grafana/tempo/pkg/tempopb"
	tempocommon "github.com/grafana/tempo/pkg/tempopb/common/v1"
	temporesource "github.com/grafana/tempo/pkg/tempopb/resource/v1"
	tempotrace "github.com/grafana/tempo/pkg/tempopb/trace/v1"
)

// fakeVCNTStore captures Put calls so a test can decode the .vcnt objects a
// flush() call actually wrote, mirroring fakeVISink in valueindex_test.go.
type fakeVCNTStore struct {
	objs map[string][]byte
	mu   sync.Mutex
}

func newFakeVCNTStore() *fakeVCNTStore {
	return &fakeVCNTStore{objs: map[string][]byte{}}
}

func (f *fakeVCNTStore) Put(key string, data []byte) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	cp := make([]byte, len(data))
	copy(cp, data)
	f.objs[key] = cp
	return nil
}

func (f *fakeVCNTStore) recordsForColumn(colName string) []blockpack.VCNTRecord {
	f.mu.Lock()
	defer f.mu.Unlock()
	colHash := blockpack.VCNTColHash(colName)
	var out []blockpack.VCNTRecord
	for key, data := range f.objs {
		if !stringsContains(key, colHash) {
			continue
		}
		out = append(out, decodeVCNTObjectForTest(data)...)
	}
	return out
}

func stringsContains(s, substr string) bool {
	for i := 0; i+len(substr) <= len(s); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}

// vcntFileTrailerSize is the fixed byte size of the self-describing VCNT file trailer
// EncodeVCNTFile appends: dirCount[4] + bodyLen[4] + magic[4] (valuecounts/selfdescribing.go).
const vcntFileTrailerSize = 12

// vcntFileMagic mirrors valuecounts/selfdescribing.go's vcntFileMagic ("VCN1"); used to
// confirm decodeVCNTObjectForTest is actually parsing EncodeVCNTFile's self-describing
// trailer, not silently misparsing an unrelated tail as a bogus bodyLen.
const vcntFileMagic uint32 = 0x56434E31

// decodeVCNTObjectForTest decodes a .vcnt object written by vcntAccumulator.flush.
//
// flush() writes via blockpack.EncodeVCNTFile (NOTE-VC-005's self-describing VCNT file
// format, A-Tempo-1/#490): the same snappy-chunked body EncodeVCNTRecords/EncodeRecords
// produces, followed by an embedded chunk directory and a fixed 12-byte trailer
// (dirCount[4] + bodyLen[4] + magic[4]). This helper strips the trailer and directory and
// decodes only the body — for the small record counts these tests produce, that body is
// always a single snappy-compressed chunk, identical to the pre-#490 wire format.
// blockpack's public API (vcnt.go) exports CompactVCNTRecords (used below) but still has
// no decode-back-to-records function, and this test's package (vblockpack, module
// github.com/grafana/tempo) cannot import blockpack's internal valuecounts package
// directly (Go's internal-import-path rule scopes internal/ to importers whose path is
// rooted at github.com/grafana/blockpack/), so this helper decodes the wire format
// directly using the same encoding documented in valuecounts/section.go's
// encodeChunkPayload comment:
//
//	record_count[2] + (col_len[2] + col[N] + time_start[8] + time_end[8] +
//	val_len[4] + val[M] + count[8 signed])*
//
// using only golang/snappy, an already-vendored public dependency.
func decodeVCNTObjectForTest(data []byte) []blockpack.VCNTRecord {
	if len(data) < vcntFileTrailerSize {
		return nil
	}
	trailer := data[len(data)-vcntFileTrailerSize:]
	if le32(trailer[8:12]) != vcntFileMagic {
		return nil
	}
	bodyLen := int(le32(trailer[4:8]))
	if bodyLen < 0 || bodyLen > len(data)-vcntFileTrailerSize {
		return nil
	}
	body := data[:bodyLen]

	raw, err := snappy.Decode(nil, body)
	if err != nil {
		return nil
	}
	if len(raw) < 2 {
		return nil
	}
	count := int(le16(raw))
	pos := 2
	recs := make([]blockpack.VCNTRecord, 0, count)
	for range count {
		colLen := int(le16(raw[pos:]))
		pos += 2
		col := string(raw[pos : pos+colLen])
		pos += colLen
		timeStart := le64(raw[pos:])
		pos += 8
		timeEnd := le64(raw[pos:])
		pos += 8
		valLen := int(le32(raw[pos:]))
		pos += 4
		val := append([]byte(nil), raw[pos:pos+valLen]...)
		pos += valLen
		cnt := int64(le64(raw[pos:])) //nolint:gosec // signed round-trip, mirrors production wire format
		pos += 8
		recs = append(recs, blockpack.VCNTRecord{
			ColumnName: col,
			Value:      val,
			TimeStart:  timeStart,
			TimeEnd:    timeEnd,
			Count:      cnt,
		})
	}
	return recs
}

func le16(b []byte) uint16 { return uint16(b[0]) | uint16(b[1])<<8 }

func le32(b []byte) uint32 {
	return uint32(b[0]) | uint32(b[1])<<8 | uint32(b[2])<<16 | uint32(b[3])<<24
}

func le64(b []byte) uint64 {
	return uint64(le32(b)) | uint64(le32(b[4:]))<<32
}

// traceWithSpanAt builds a single-ResourceSpans trace with one span named
// spanName starting at startNano, and no resource attributes.
func traceWithSpanAt(startNano uint64, spanName string) *tempopb.Trace {
	return &tempopb.Trace{
		ResourceSpans: []*tempotrace.ResourceSpans{{
			ScopeSpans: []*tempotrace.ScopeSpans{{
				Spans: []*tempotrace.Span{{
					Name:              spanName,
					StartTimeUnixNano: startNano,
				}},
			}},
		}},
	}
}

// traceWithResourceAndSpansAt builds a single-ResourceSpans trace carrying
// resourceAttrs and one span per (startNano, spanName) pair, all under the
// same ResourceSpans/ScopeSpans block.
func traceWithResourceAndSpansAt(
	resourceAttrs []*tempocommon.KeyValue,
	spans ...struct {
		startNano uint64
		name      string
	},
) *tempopb.Trace {
	spanPBs := make([]*tempotrace.Span, 0, len(spans))
	for _, s := range spans {
		spanPBs = append(spanPBs, &tempotrace.Span{
			Name:              s.name,
			StartTimeUnixNano: s.startNano,
		})
	}
	return &tempopb.Trace{
		ResourceSpans: []*tempotrace.ResourceSpans{{
			Resource:   &temporesource.Resource{Attributes: resourceAttrs},
			ScopeSpans: []*tempotrace.ScopeSpans{{Spans: spanPBs}},
		}},
	}
}

// findRecord returns the first record in records matching column and value,
// failing the test if none is found.
func findRecord(t *testing.T, records []blockpack.VCNTRecord, column, value string) blockpack.VCNTRecord {
	t.Helper()
	for _, r := range records {
		if r.ColumnName == column && string(r.Value) == value {
			return r
		}
	}
	require.Fail(t, "record not found", "column=%q value=%q not present in %d records", column, value, len(records))
	return blockpack.VCNTRecord{}
}

// TestVCNTAccumulator_BucketsPerSpanMinute proves bucketing is per-span, not
// per-trace or per-block: two spans in the same ResourceSpans, one at 65s
// (minute bucket 60) and one at 130s (minute bucket 120), must land in
// different buckets.
func TestVCNTAccumulator_BucketsPerSpanMinute(t *testing.T) {
	trace := traceWithResourceAndSpansAt(nil,
		struct {
			startNano uint64
			name      string
		}{65 * 1_000_000_000, "op-a"},
		struct {
			startNano uint64
			name      string
		}{130 * 1_000_000_000, "op-b"},
	)

	acc := newVCNTAccumulator()
	acc.addTrace(trace)
	records := acc.snapshot()

	opA := findRecord(t, records, "span:name", "op-a")
	opB := findRecord(t, records, "span:name", "op-b")

	assert.Equal(t, uint64(60), opA.TimeStart, "65s must floor to minute bucket 60")
	assert.Equal(t, uint64(60), opA.TimeEnd, "point convention: TimeStart == TimeEnd == bucket")
	assert.Equal(t, uint64(120), opB.TimeStart, "130s must floor to minute bucket 120")
	assert.Equal(t, uint64(120), opB.TimeEnd, "point convention: TimeStart == TimeEnd == bucket")
}

// TestVCNTAccumulator_ResourceAttrsBucketedPerSpan proves Decision 2's
// per-span resource-attribute attribution: one ResourceSpans with
// service.name="svc-a" and two spans (65s, 130s) must produce TWO
// resource.service.name records, one per bucket, count 1 each — a deliberate
// over-count relative to "once per resource" (see vcntwriter.go's package doc
// for the rationale), not a bug.
func TestVCNTAccumulator_ResourceAttrsBucketedPerSpan(t *testing.T) {
	resourceAttrs := []*tempocommon.KeyValue{{
		Key:   "service.name",
		Value: &tempocommon.AnyValue{Value: &tempocommon.AnyValue_StringValue{StringValue: "svc-a"}},
	}}
	trace := traceWithResourceAndSpansAt(resourceAttrs,
		struct {
			startNano uint64
			name      string
		}{65 * 1_000_000_000, "op-a"},
		struct {
			startNano uint64
			name      string
		}{130 * 1_000_000_000, "op-b"},
	)

	acc := newVCNTAccumulator()
	acc.addTrace(trace)
	records := acc.snapshot()

	var bucket60, bucket120 []blockpack.VCNTRecord
	for _, r := range records {
		if r.ColumnName != "resource.service.name" || string(r.Value) != "svc-a" {
			continue
		}
		switch r.TimeStart {
		case 60:
			bucket60 = append(bucket60, r)
		case 120:
			bucket120 = append(bucket120, r)
		}
	}

	require.Len(t, bucket60, 1, "resource attr must appear exactly once in bucket 60 (span op-a's bucket)")
	require.Len(t, bucket120, 1, "resource attr must appear exactly once in bucket 120 (span op-b's bucket)")
	assert.Equal(t, int64(1), bucket60[0].Count)
	assert.Equal(t, int64(1), bucket120[0].Count)
}

// TestVCNTAccumulator_MinuteBoundary_FloorNotRound proves bucketing floors to
// the minute rather than rounding: a span 1ns under a minute boundary lands in
// the earlier bucket, and a span exactly on the boundary lands in the later one.
func TestVCNTAccumulator_MinuteBoundary_FloorNotRound(t *testing.T) {
	trace := traceWithResourceAndSpansAt(nil,
		struct {
			startNano uint64
			name      string
		}{59_999_999_999, "same-op"},
		struct {
			startNano uint64
			name      string
		}{60_000_000_000, "same-op"},
	)

	acc := newVCNTAccumulator()
	acc.addTrace(trace)
	records := acc.snapshot()

	var buckets []uint64
	for _, r := range records {
		if r.ColumnName == "span:name" && string(r.Value) == "same-op" {
			buckets = append(buckets, r.TimeStart)
		}
	}
	assert.ElementsMatch(t, []uint64{0, 60}, buckets,
		"59.999999999s must floor to bucket 0 and 60.0s must floor to bucket 60, not round to a shared bucket")
}

// TestVCNTFlush_CrossBlockMinuteCoalescing is the end-to-end proof that the
// pre-existing cross-block compaction defect is fixed: two independent
// accumulator+flush cycles (as two separate block-builder flushes would
// produce), each with one span landing in the same wall-clock minute, must
// produce .vcnt records that share the exact same (ColumnName, TimeStart,
// TimeEnd, Value) key and therefore compact into a single summed record.
func TestVCNTFlush_CrossBlockMinuteCoalescing(t *testing.T) {
	store := newFakeVCNTStore()

	accA := newVCNTAccumulator()
	accA.addTrace(traceWithSpanAt(65*1_000_000_000, "op-a")) // bucket 60
	accA.flush(store, "tenant1")

	accB := newVCNTAccumulator()
	accB.addTrace(traceWithSpanAt(68*1_000_000_000, "op-a")) // also bucket 60, independent flush
	accB.flush(store, "tenant1")

	decoded := store.recordsForColumn("span:name")
	require.NotEmpty(t, decoded, "flush must have written span:name records to the fake store")

	merged := blockpack.CompactVCNTRecords(decoded)
	require.Len(t, merged, 1,
		"two independent flushes landing in the same minute bucket must compact into exactly one record")
	assert.Equal(t, uint64(60), merged[0].TimeStart)
	assert.Equal(t, uint64(60), merged[0].TimeEnd)
	assert.Equal(t, int64(2), merged[0].Count)
}

// keyForColumn returns the single fakeVCNTStore key written for colName, failing the test if
// zero or more than one key matches.
func keyForColumn(t *testing.T, store *fakeVCNTStore, colName string) string {
	t.Helper()
	store.mu.Lock()
	defer store.mu.Unlock()
	colHash := blockpack.VCNTColHash(colName)
	var key string
	for k := range store.objs {
		if !stringsContains(k, colHash) {
			continue
		}
		require.Empty(t, key, "expected exactly one key written for column %q, found a second: %q", colName, k)
		key = k
	}
	require.NotEmpty(t, key, "expected a key to have been written for column %q", colName)
	return key
}

// TestVCNTFlush_WritesV2KeyFormatWithGenuineRange proves flush() writes a v2-format .vcnt
// key whose embedded [WallMinSec, WallMaxSec] is the genuine min/max across the flushed
// records — not hardcoded, not the first/last record in whatever map-iteration order flush()
// happens to build records in (issue #494, R5).
func TestVCNTFlush_WritesV2KeyFormatWithGenuineRange(t *testing.T) {
	store := newFakeVCNTStore()
	acc := newVCNTAccumulator()
	acc.addTrace(traceWithSpanAt(60*1_000_000_000, "op-a"))
	acc.addTrace(traceWithSpanAt(120*1_000_000_000, "op-b"))
	acc.addTrace(traceWithSpanAt(180*1_000_000_000, "op-c"))
	acc.flush(store, "tenant1")

	key := keyForColumn(t, store, "span:name")
	meta, err := blockpack.VCNTParseFilenameV2(path.Base(key))
	require.NoError(t, err)
	assert.Equal(t, 0, meta.Level)
	assert.Equal(t, uint64(60), meta.WallMinSec)
	assert.Equal(t, uint64(180), meta.WallMaxSec)
}

// durationBucketBoundsMillisForTest mirrors blockpack's finalized 16-entry
// DurationBucketBoundsMillis array (#205 plan §1) so tests can assert on the
// ABSENCE of records for every non-target bucket without importing blockpack's
// internal/modules/valuecounts package (not importable from this module tree).
// These are literal spec values, not a reimplementation of production logic.
var durationBucketBoundsMillisForTest = [16]uint64{
	0, 1, 5, 10, 50, 100, 500,
	1_000, 5_000, 10_000, 30_000,
	60_000, 300_000, 600_000,
	1_800_000, 3_600_000,
}

// durationSpan is one span's start/end timestamps for duration-histogram test fixtures.
type durationSpan struct {
	startNano uint64
	endNano   uint64
	name      string
}

// traceWithDurationSpansAt builds a single-ResourceSpans trace with one span per
// durationSpan, each carrying both StartTimeUnixNano and EndTimeUnixNano.
func traceWithDurationSpansAt(spans ...durationSpan) *tempopb.Trace {
	spanPBs := make([]*tempotrace.Span, 0, len(spans))
	for _, s := range spans {
		spanPBs = append(spanPBs, &tempotrace.Span{
			Name:              s.name,
			StartTimeUnixNano: s.startNano,
			EndTimeUnixNano:   s.endNano,
		})
	}
	return &tempopb.Trace{
		ResourceSpans: []*tempotrace.ResourceSpans{{
			ScopeSpans: []*tempotrace.ScopeSpans{{Spans: spanPBs}},
		}},
	}
}

// findHistogramRecord returns the record under histCol whose Value matches boundaryMillis, or
// nil if absent — used to assert absence (not just presence) for the discreteness pin.
func findHistogramRecord(records []blockpack.VCNTRecord, histCol string, boundaryMillis uint64) *blockpack.VCNTRecord {
	want := string(blockpack.VCNTDurationHistogramValue(boundaryMillis))
	for i, r := range records {
		if r.ColumnName == histCol && string(r.Value) == want {
			return &records[i]
		}
	}
	return nil
}

// TestVCNTAccumulator_DurationHistogram_BucketsBySpanDuration proves each span's duration is
// bucketed via VCNTDurationBucketBoundaryMillis and recorded under
// VCNTDurationHistogramColumnName("span:duration"), with exact (boundary, count) pairs — one
// span landing exactly on a boundary (10s, bucket 9), one past the 1hr catch-all (bucket 15),
// and one at a small exact boundary (50ms, bucket 4).
func TestVCNTAccumulator_DurationHistogram_BucketsBySpanDuration(t *testing.T) {
	const nsPerMs = 1_000_000
	trace := traceWithDurationSpansAt(
		durationSpan{startNano: 0, endNano: 10_000 * nsPerMs, name: "ten-seconds"},                // duration 10s -> boundary 10_000 (bucket 9)
		durationSpan{startNano: 0, endNano: (3_600_000 + 500) * nsPerMs, name: "past-one-hour"},    // duration 3,600.5s -> boundary 3_600_000 (bucket 15)
		durationSpan{startNano: 0, endNano: 50 * nsPerMs, name: "fifty-millis"},                   // duration 50ms -> boundary 50 (bucket 4)
	)

	acc := newVCNTAccumulator()
	acc.addTrace(trace)
	records := acc.snapshot()

	histCol := blockpack.VCNTDurationHistogramColumnName("span:duration")

	tenSec := findHistogramRecord(records, histCol, 10_000)
	require.NotNil(t, tenSec, "expected a histogram record for the 10s-exactly span's boundary")
	assert.Equal(t, int64(1), tenSec.Count)

	pastHour := findHistogramRecord(records, histCol, 3_600_000)
	require.NotNil(t, pastHour, "expected a histogram record for the past-1hr span's boundary (catch-all bucket 15)")
	assert.Equal(t, int64(1), pastHour.Count)

	fiftyMs := findHistogramRecord(records, histCol, 50)
	require.NotNil(t, fiftyMs, "expected a histogram record for the 50ms-exactly span's boundary")
	assert.Equal(t, int64(1), fiftyMs.Count)
}

// TestVCNTAccumulator_DurationHistogram_12ms_OnlyBucket3IncrementsOthersStayZero is the
// discreteness pin (#205 plan §1.1): a single 12ms-duration span must increment ONLY bucket
// index 3 (boundary 10ms, since 10 <= 12 < 50) — every other bucket must be absent from the
// snapshot entirely, not merely present-with-zero (the accumulator never creates a map entry
// for a bucket it didn't increment).
func TestVCNTAccumulator_DurationHistogram_12ms_OnlyBucket3IncrementsOthersStayZero(t *testing.T) {
	const nsPerMs = 1_000_000
	trace := traceWithDurationSpansAt(
		durationSpan{startNano: 0, endNano: 12 * nsPerMs, name: "twelve-millis"},
	)

	acc := newVCNTAccumulator()
	acc.addTrace(trace)
	records := acc.snapshot()

	histCol := blockpack.VCNTDurationHistogramColumnName("span:duration")

	target := findHistogramRecord(records, histCol, 10)
	require.NotNil(t, target, "expected exactly one histogram record at boundary 10 (bucket 3) for a 12ms span")
	assert.Equal(t, int64(1), target.Count)

	for _, boundary := range durationBucketBoundsMillisForTest {
		if boundary == 10 {
			continue
		}
		other := findHistogramRecord(records, histCol, boundary)
		assert.Nilf(t, other, "boundary %d must be entirely absent from the snapshot for a 12ms span, got %+v", boundary, other)
	}
}

// TestVCNTAccumulator_DurationHistogram_MalformedEndBeforeStart_ClampedToBucket0NotDropped
// proves the never-drop-records guard (#205 plan §6 point 2): a malformed span with
// EndTimeUnixNano < StartTimeUnixNano must still produce exactly one histogram record, clamped
// to bucket 0 (boundary 0), never zero records.
func TestVCNTAccumulator_DurationHistogram_MalformedEndBeforeStart_ClampedToBucket0NotDropped(t *testing.T) {
	trace := traceWithDurationSpansAt(
		durationSpan{startNano: 1_000_000_000, endNano: 500_000_000, name: "malformed"},
	)

	acc := newVCNTAccumulator()
	acc.addTrace(trace)
	records := acc.snapshot()

	histCol := blockpack.VCNTDurationHistogramColumnName("span:duration")

	target := findHistogramRecord(records, histCol, 0)
	require.NotNil(t, target, "malformed end-before-start span must clamp to bucket 0, not be dropped")
	assert.Equal(t, int64(1), target.Count)

	for _, boundary := range durationBucketBoundsMillisForTest {
		if boundary == 0 {
			continue
		}
		other := findHistogramRecord(records, histCol, boundary)
		assert.Nilf(t, other, "only bucket 0 should be incremented for a clamped malformed span, got %+v", other)
	}
}

// TestVCNTAccumulator_DurationHistogram_UnsetEndTime_ClampedToBucket0NotDropped is the sibling
// malformed-input case: EndTimeUnixNano == 0 (unset) must clamp identically to bucket 0.
func TestVCNTAccumulator_DurationHistogram_UnsetEndTime_ClampedToBucket0NotDropped(t *testing.T) {
	trace := traceWithDurationSpansAt(
		durationSpan{startNano: 1_000_000_000, endNano: 0, name: "unset-end"},
	)

	acc := newVCNTAccumulator()
	acc.addTrace(trace)
	records := acc.snapshot()

	histCol := blockpack.VCNTDurationHistogramColumnName("span:duration")

	target := findHistogramRecord(records, histCol, 0)
	require.NotNil(t, target, "unset EndTimeUnixNano span must clamp to bucket 0, not be dropped")
	assert.Equal(t, int64(1), target.Count)

	for _, boundary := range durationBucketBoundsMillisForTest {
		if boundary == 0 {
			continue
		}
		other := findHistogramRecord(records, histCol, boundary)
		assert.Nilf(t, other, "only bucket 0 should be incremented for an unset-end-time span, got %+v", other)
	}
}

// TestVCNTFlush_DurationHistogram_CrossBlockMinuteCoalescing mirrors
// TestVCNTFlush_CrossBlockMinuteCoalescing for histogram rows: two independent accumulator+flush
// cycles, each with one span landing in the same wall-clock minute AND the same duration bucket,
// must produce .vcnt records that share the exact same (ColumnName, TimeStart, TimeEnd, Value)
// key and compact into a single summed record under blockpack.CompactVCNTRecords.
func TestVCNTFlush_DurationHistogram_CrossBlockMinuteCoalescing(t *testing.T) {
	store := newFakeVCNTStore()
	const nsPerMs = 1_000_000
	const nsPerSec = 1_000_000_000

	accA := newVCNTAccumulator()
	accA.addTrace(traceWithDurationSpansAt(durationSpan{
		startNano: 65 * nsPerSec, endNano: 65*nsPerSec + 10_000*nsPerMs, name: "op-a",
	})) // bucket 60, duration 10s -> boundary 10_000
	accA.flush(store, "tenant1")

	accB := newVCNTAccumulator()
	accB.addTrace(traceWithDurationSpansAt(durationSpan{
		startNano: 68 * nsPerSec, endNano: 68*nsPerSec + 10_000*nsPerMs, name: "op-a",
	})) // also bucket 60, same duration bucket, independent flush
	accB.flush(store, "tenant1")

	histCol := blockpack.VCNTDurationHistogramColumnName("span:duration")
	decoded := store.recordsForColumn(histCol)
	require.NotEmpty(t, decoded, "flush must have written histogram records to the fake store")

	merged := blockpack.CompactVCNTRecords(decoded)
	require.Len(t, merged, 1,
		"two independent flushes landing in the same minute+bucket must compact into exactly one record")
	assert.Equal(t, uint64(60), merged[0].TimeStart)
	assert.Equal(t, uint64(60), merged[0].TimeEnd)
	assert.Equal(t, int64(2), merged[0].Count)
	assert.Equal(t, blockpack.VCNTDurationHistogramValue(10_000), merged[0].Value)
}

// TestVCNTFlush_KeyRangeCorrectDespiteMapIterationOrder is the mandatory adversarial test
// (issue #494, R5, mirrors R3's requirement applied to tempo's write call site): accumulator
// state is populated directly (bypassing addTrace) so the fixture doesn't depend on
// floor/bucket derivation. Go's map iteration order is randomized per-run, so the loop
// re-creates the map fresh each iteration and asserts the computed range is correct on every
// run, giving confidence flush()'s key range doesn't depend on iteration order.
func TestVCNTFlush_KeyRangeCorrectDespiteMapIterationOrder(t *testing.T) {
	for i := 0; i < 20; i++ {
		store := newFakeVCNTStore()
		acc := newVCNTAccumulator()
		acc.counts = map[string]map[uint64]map[string]int64{
			"span:name": {
				300:  {"c": 1},
				9999: {"b": 1},
				60:   {"a": 1},
			},
		}
		acc.flush(store, "tenant1")

		key := keyForColumn(t, store, "span:name")
		meta, err := blockpack.VCNTParseFilenameV2(path.Base(key))
		require.NoError(t, err)
		assert.Equal(t, uint64(60), meta.WallMinSec, "iteration %d", i)
		assert.Equal(t, uint64(9999), meta.WallMaxSec, "iteration %d", i)
	}
}
