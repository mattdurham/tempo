package vblockpack

// vcntwriter.go — VCNT value-count index writer for the block-builder.
//
// During CreateBlock, spans are iterated. In parallel, vcntAccumulator counts
// how many spans carry each (column, value) pair within the block's time window.
// After Flush(), the accumulated counts are encoded and PUT to S3 as one .vcnt
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

// vcntAccumulator collects (column, value) → count for one block.
type vcntAccumulator struct {
	// counts: colName → value_bytes_as_string → count
	counts map[string]map[string]int64
}

func newVCNTAccumulator() *vcntAccumulator {
	return &vcntAccumulator{counts: make(map[string]map[string]int64)}
}

// addTrace accumulates VCNT counts for every span in one trace.
func (a *vcntAccumulator) addTrace(trace *tempopb.Trace) {
	if trace == nil {
		return
	}
	for _, rs := range trace.ResourceSpans {
		if rs == nil {
			continue
		}
		if rs.Resource != nil {
			a.addAttrs("resource.", rs.Resource.Attributes)
		}
		for _, ss := range rs.ScopeSpans {
			if ss == nil {
				continue
			}
			for _, span := range ss.Spans {
				if span == nil {
					continue
				}
				a.addAttrs("span.", span.Attributes)
				if span.Name != "" {
					a.inc("span:name", []byte(span.Name))
				}
				if span.Kind != 0 {
					a.inc("span:kind", encodeInt64VCNT(int64(span.Kind)))
				}
				if span.Status != nil && span.Status.Code != 0 {
					a.inc("span:status", encodeInt64VCNT(int64(span.Status.Code)))
				}
			}
		}
	}
}

func (a *vcntAccumulator) addAttrs(prefix string, attrs []*commonpbv1.KeyValue) {
	for _, kv := range attrs {
		if kv == nil || kv.Value == nil {
			continue
		}
		col := prefix + kv.Key
		switch v := kv.Value.Value.(type) {
		case *commonpbv1.AnyValue_StringValue:
			if v.StringValue != "" {
				a.inc(col, []byte(v.StringValue))
			}
		case *commonpbv1.AnyValue_IntValue:
			a.inc(col, encodeInt64VCNT(v.IntValue))
		case *commonpbv1.AnyValue_BoolValue:
			if v.BoolValue {
				a.inc(col, []byte{1})
			} else {
				a.inc(col, []byte{0})
			}
		}
	}
}

func (a *vcntAccumulator) inc(col string, val []byte) {
	m, ok := a.counts[col]
	if !ok {
		m = make(map[string]int64)
		a.counts[col] = m
	}
	m[string(val)]++
}

// flush encodes accumulated counts and PUTs one .vcnt file per column.
// timeStartSec and timeEndSec are the block's span time window in whole seconds.
func (a *vcntAccumulator) flush(store blockpack.ObjectPutter, tenant, indexPrefix string, timeStartSec, timeEndSec uint64) {
	if store == nil || len(a.counts) == 0 {
		return
	}
	for colName, vals := range a.counts {
		records := make([]blockpack.VCNTRecord, 0, len(vals))
		for val, count := range vals {
			records = append(records, blockpack.VCNTRecord{
				ColumnName: colName,
				Value:      []byte(val),
				TimeStart:  timeStartSec,
				TimeEnd:    timeEndSec,
				Count:      count,
			})
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
