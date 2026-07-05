package vblockpack

// vcntwriter_test.go — tests for vcntAccumulator's per-span minute bucketing
// (see Phase 4 of the minute-aligned-timestamp plan). These tests exercise the
// new bucket-aware accumulator API (inc/addAttrs taking a minute-bucket
// parameter, snapshot() for inspecting accumulated records without going
// through flush's store I/O, and flush()'s new two-window-free signature) —
// none of which exist yet on vcntAccumulator, so this file is expected to fail
// to compile until vcntwriter.go is restructured (TDD red step).

import (
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

// decodeVCNTObjectForTest decodes a .vcnt object written by vcntAccumulator.flush.
//
// flush() writes via blockpack.EncodeVCNTRecords, which is a thin re-export of
// blockpack's internal valuecounts.EncodeRecords — a single snappy-compressed
// chunk for the small record counts these tests produce, with NO directory
// persisted alongside it (that gap is tracked separately, NOTE-VC-005, and is
// out of scope here). blockpack's public API (vcnt.go) exports CompactVCNTRecords
// (used below) but still has no decode function, and this test's package
// (vblockpack, module github.com/grafana/tempo) cannot import blockpack's
// internal valuecounts package directly (Go's internal-import-path rule scopes
// internal/ to importers whose path is rooted at github.com/grafana/blockpack/),
// so this helper decodes the wire format directly using the same encoding
// documented in valuecounts/section.go's encodeChunkPayload comment:
//
//	record_count[2] + (col_len[2] + col[N] + time_start[8] + time_end[8] +
//	val_len[4] + val[M] + count[8 signed])*
//
// using only golang/snappy, an already-vendored public dependency.
func decodeVCNTObjectForTest(data []byte) []blockpack.VCNTRecord {
	raw, err := snappy.Decode(nil, data)
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
	accA.flush(store, "tenant1", "indexes")

	accB := newVCNTAccumulator()
	accB.addTrace(traceWithSpanAt(68*1_000_000_000, "op-a")) // also bucket 60, independent flush
	accB.flush(store, "tenant1", "indexes")

	decoded := store.recordsForColumn("span:name")
	require.NotEmpty(t, decoded, "flush must have written span:name records to the fake store")

	merged := blockpack.CompactVCNTRecords(decoded)
	require.Len(t, merged, 1,
		"two independent flushes landing in the same minute bucket must compact into exactly one record")
	assert.Equal(t, uint64(60), merged[0].TimeStart)
	assert.Equal(t, uint64(60), merged[0].TimeEnd)
	assert.Equal(t, int64(2), merged[0].Count)
}
