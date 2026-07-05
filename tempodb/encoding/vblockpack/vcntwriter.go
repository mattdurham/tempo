package vblockpack

// vcntwriter.go — VCNT value-count index writer for the block-builder.
//
// During CreateBlock, spans are iterated. vcntAccumulator counts how many
// spans carry each (column, value) pair, bucketed per-span by the span's own
// StartTimeUnixNano floored to the minute (minuteBucket) — not per-trace or
// per-block — so records from independent block flushes that land in the
// same wall-clock minute share an exact (ColumnName, TimeStart, TimeEnd,
// Value) key and coalesce under valuecounts.Compact (NOTE-VC-00x). After
// Flush(), the accumulated counts are encoded and PUT to S3 as one .vcnt
// file per column under:
//
//	<tenant>/indexes/unique_values/<colHash>/L0-<id>.vcnt

import (
	"encoding/binary"

	"github.com/go-kit/log/level"
	blockpack "github.com/grafana/blockpack"
	"github.com/grafana/tempo/pkg/tempopb"
	commonpbv1 "github.com/grafana/tempo/pkg/tempopb/common/v1"
	util_log "github.com/grafana/tempo/pkg/util/log"
)

// vcntAccumulator collects (column, value, minuteBucket) → count for one
// block-builder pass.
type vcntAccumulator struct {
	// counts: colName -> minuteBucket -> value_bytes_as_string -> count
	counts map[string]map[uint64]map[string]int64
}

func newVCNTAccumulator() *vcntAccumulator {
	return &vcntAccumulator{counts: make(map[string]map[uint64]map[string]int64)}
}

const secondsPerMinute = 60

// minuteBucket floors a span's StartTimeUnixNano to the minute, matching
// blockpack's VI write-side truncation exactly (buildSpanStartSecByRef,
// valueindex_extract.go:179) for consistency across the two sibling indexes,
// though there is no correctness coupling here the way there is for VI
// (VCNT has no per-record time-window search gate today).
func minuteBucket(startNano uint64) uint64 {
	const ns = 1_000_000_000
	return (startNano / ns) / secondsPerMinute * secondsPerMinute
}

// addTrace accumulates VCNT counts for every span in one trace. Resource
// attributes are attributed per-span (not once per ResourceSpans block): a
// resource with N spans in the same trace gets its resource attrs counted N
// times, once per span, each landing in that span's own minute bucket. This
// is a deliberate over-count — see plan.md Phase 3 Decision 2 for the
// rationale.
func (a *vcntAccumulator) addTrace(trace *tempopb.Trace) {
	if trace == nil {
		return
	}
	for _, rs := range trace.ResourceSpans {
		if rs == nil {
			continue
		}
		for _, ss := range rs.ScopeSpans {
			if ss == nil {
				continue
			}
			for _, span := range ss.Spans {
				if span == nil {
					continue
				}
				bucket := minuteBucket(span.StartTimeUnixNano)
				if rs.Resource != nil {
					a.addAttrs(bucket, "resource.", rs.Resource.Attributes)
				}
				a.addAttrs(bucket, "span.", span.Attributes)
				if span.Name != "" {
					a.inc(bucket, "span:name", []byte(span.Name))
				}
				if span.Kind != 0 {
					a.inc(bucket, "span:kind", encodeInt64VCNT(int64(span.Kind)))
				}
				if span.Status != nil && span.Status.Code != 0 {
					a.inc(bucket, "span:status", encodeInt64VCNT(int64(span.Status.Code)))
				}
			}
		}
	}
}

func (a *vcntAccumulator) addAttrs(bucket uint64, prefix string, attrs []*commonpbv1.KeyValue) {
	for _, kv := range attrs {
		if kv == nil || kv.Value == nil {
			continue
		}
		col := prefix + kv.Key
		switch v := kv.Value.Value.(type) {
		case *commonpbv1.AnyValue_StringValue:
			if v.StringValue != "" {
				a.inc(bucket, col, []byte(v.StringValue))
			}
		case *commonpbv1.AnyValue_IntValue:
			a.inc(bucket, col, encodeInt64VCNT(v.IntValue))
		case *commonpbv1.AnyValue_BoolValue:
			if v.BoolValue {
				a.inc(bucket, col, []byte{1})
			} else {
				a.inc(bucket, col, []byte{0})
			}
		}
	}
}

func (a *vcntAccumulator) inc(bucket uint64, col string, val []byte) {
	byBucket, ok := a.counts[col]
	if !ok {
		byBucket = make(map[uint64]map[string]int64)
		a.counts[col] = byBucket
	}
	m, ok := byBucket[bucket]
	if !ok {
		m = make(map[string]int64)
		byBucket[bucket] = m
	}
	m[string(val)]++
}

// snapshot returns the accumulated counts as VCNTRecords without going
// through flush's encode/store I/O — used by tests to inspect bucketing
// directly.
func (a *vcntAccumulator) snapshot() []blockpack.VCNTRecord {
	var records []blockpack.VCNTRecord
	for colName, byBucket := range a.counts {
		for bucket, vals := range byBucket {
			for val, count := range vals {
				records = append(records, blockpack.VCNTRecord{
					ColumnName: colName,
					Value:      []byte(val),
					TimeStart:  bucket,
					TimeEnd:    bucket,
					Count:      count,
				})
			}
		}
	}
	return records
}

// flush encodes accumulated per-minute-bucket counts and PUTs one .vcnt file
// per column. Each record's TimeStart/TimeEnd is its own minute bucket (point
// convention — see plan.md Phase 3 Decision 1), not a caller-supplied block
// window.
func (a *vcntAccumulator) flush(store blockpack.ObjectPutter, tenant, indexPrefix string) {
	if store == nil || len(a.counts) == 0 {
		return
	}
	for colName, byBucket := range a.counts {
		records := make([]blockpack.VCNTRecord, 0)
		for bucket, vals := range byBucket {
			for val, count := range vals {
				records = append(records, blockpack.VCNTRecord{
					ColumnName: colName,
					Value:      []byte(val),
					TimeStart:  bucket,
					TimeEnd:    bucket,
					Count:      count,
				})
			}
		}
		blockpack.SortVCNTRecords(records)
		data, _ := blockpack.EncodeVCNTRecords(records, 0)
		id := blockpack.VCNTNewID()
		key := blockpack.VCNTObjectKey(tenant, indexPrefix, colName, id)
		if putErr := store.Put(key, data); putErr != nil {
			level.Warn(util_log.Logger).Log(
				"msg", "vblockpack: vcnt write failed",
				"col", colName, "tenant", tenant, "err", putErr,
			)
		}
	}
}

// encodeInt64VCNT encodes an int64 as 8-byte little-endian for use as a VCNT value.
func encodeInt64VCNT(v int64) []byte {
	b := make([]byte, 8)
	binary.LittleEndian.PutUint64(b, uint64(v)) //nolint:gosec
	return b
}
