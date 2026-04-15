// NOTE: Any changes to this file must be reflected in the corresponding specs.md or NOTES.md.

package compaction

// compaction/NOTES.md NOTE-37: CompactLogFile globally re-sorts a log blockpack file by
// (minHash[0..3], timestamp) to produce tight label-value boundaries per block.
// This enables the range index to prune effectively for label-based queries.

import (
	"bytes"
	"fmt"
	"io"
	"slices"
	"strings"

	commonv1 "go.opentelemetry.io/proto/otlp/common/v1"
	logsv1 "go.opentelemetry.io/proto/otlp/logs/v1"
	resourcev1 "go.opentelemetry.io/proto/otlp/resource/v1"

	modules_blockio "github.com/grafana/blockpack/internal/modules/blockio"
	modules_reader "github.com/grafana/blockpack/internal/modules/blockio/reader"
	"github.com/grafana/blockpack/internal/modules/blockio/shared"
	modules_rw "github.com/grafana/blockpack/internal/modules/rw"
)

// pendingLogRow holds a reconstructed log record with its sort key for global re-sorting.
type pendingLogRow struct {
	ld         *logsv1.LogsData
	minHashSig [4]uint64
	timestamp  uint64
}

// CompactLogFile reads a log-signal blockpack file, globally re-sorts all rows by
// (minHash[0..3], timestamp), and writes a new file to output. The MinHash is
// computed over "key=value" attribute pairs, which clusters similar label sets
// into contiguous blocks — enabling the range index to prune effectively.
//
// Returns an error if the input file is not a log-signal file (SignalTypeLog).
// Trace files must use the existing CompactBlocks function.
func CompactLogFile(input modules_rw.ReaderProvider, output io.Writer, cfg Config) error {
	r, err := modules_reader.NewReaderFromProvider(input)
	if err != nil {
		return fmt.Errorf("compact log: open reader: %w", err)
	}
	if r.SignalType() != shared.SignalTypeLog {
		return fmt.Errorf(
			"compact log: expected SignalTypeLog (0x%02x), got 0x%02x",
			shared.SignalTypeLog,
			r.SignalType(),
		)
	}

	totalBlocks := r.BlockCount()
	if totalBlocks == 0 {
		return writeEmptyLogFile(output, cfg)
	}

	// Phase 1: Read all blocks, reconstruct OTLP protos, compute sort keys.
	var rows []pendingLogRow
	for blockIdx := range totalBlocks {
		bwb, getErr := r.GetBlockWithBytes(blockIdx, nil)
		if getErr != nil {
			return fmt.Errorf("compact log: get block %d: %w", blockIdx, getErr)
		}
		if bwb == nil {
			continue
		}
		block := bwb.Block
		for rowIdx := range block.SpanCount() {
			ld := reconstructLogRecord(block, rowIdx)
			var sig [4]uint64
			ts := computeLogRowSortKey(block, rowIdx, &sig)
			rows = append(rows, pendingLogRow{
				ld:         ld,
				minHashSig: sig,
				timestamp:  ts,
			})
		}
	}

	if len(rows) == 0 {
		return writeEmptyLogFile(output, cfg)
	}

	// Phase 2: Global sort by (minHash[0..3], timestamp).
	sortLogRows(rows)

	// Phase 3: Write sorted rows to output via the standard log writer.
	// Set MaxBufferedSpans to totalRows+1 so auto-flush never triggers — the writer
	// buffers all rows and sorts globally in one Flush() call.
	maxSpans := cfg.MaxSpansPerBlock
	if maxSpans <= 0 {
		maxSpans = 2000
	}
	w, err := modules_blockio.NewWriterWithConfig(modules_blockio.WriterConfig{
		OutputStream:     output,
		MaxBlockSpans:    maxSpans,
		MaxBufferedSpans: len(rows) + 1,
	})
	if err != nil {
		return fmt.Errorf("compact log: new writer: %w", err)
	}

	for i := range rows {
		if addErr := w.AddLogsData(rows[i].ld); addErr != nil {
			return fmt.Errorf("compact log: add row %d: %w", i, addErr)
		}
	}

	if _, flushErr := w.Flush(); flushErr != nil {
		return fmt.Errorf("compact log: flush: %w", flushErr)
	}
	return nil
}

// writeEmptyLogFile writes a valid empty log blockpack file.
func writeEmptyLogFile(output io.Writer, cfg Config) error {
	maxSpans := cfg.MaxSpansPerBlock
	if maxSpans <= 0 {
		maxSpans = 2000
	}
	w, err := modules_blockio.NewWriterWithConfig(modules_blockio.WriterConfig{
		OutputStream:  output,
		MaxBlockSpans: maxSpans,
	})
	if err != nil {
		return fmt.Errorf("compact log: new empty writer: %w", err)
	}
	_, err = w.Flush()
	return err
}

// sortLogRows sorts by (minHash[0..3] ASC, timestamp ASC).
func sortLogRows(rows []pendingLogRow) {
	if len(rows) <= 1 {
		return
	}
	slices.SortFunc(rows, func(a, b pendingLogRow) int {
		for i := range 4 {
			if a.minHashSig[i] != b.minHashSig[i] {
				if a.minHashSig[i] < b.minHashSig[i] {
					return -1
				}
				return 1
			}
		}
		if a.timestamp < b.timestamp {
			return -1
		}
		if a.timestamp > b.timestamp {
			return 1
		}
		return 0
	})
}

// computeLogRowSortKey computes the MinHash signature and timestamp for a log row.
// Uses the same FNV-1a "key=value" min-heap as the writer's computeMinHashSigFromLog.
func computeLogRowSortKey(block *modules_reader.Block, rowIdx int, sig *[4]uint64) uint64 {
	*sig = [4]uint64{^uint64(0), ^uint64(0), ^uint64(0), ^uint64(0)}

	for ck, col := range block.Columns() {
		if !col.IsPresent(rowIdx) {
			continue
		}
		var attrKey string
		switch {
		case strings.HasPrefix(ck.Name, "log."):
			attrKey = ck.Name[4:]
		case strings.HasPrefix(ck.Name, "resource."):
			attrKey = ck.Name[9:]
		case strings.HasPrefix(ck.Name, "scope."):
			attrKey = ck.Name[6:]
		default:
			continue // intrinsic columns are not part of the label set
		}
		if sv, ok := col.StringValue(rowIdx); ok {
			shared.AddKVHashToMinHeap(attrKey, sv, sig)
		} else {
			shared.AddHashToMinHeap(attrKey, sig)
		}
	}

	var ts uint64
	if tsCol := block.GetColumn("log:timestamp"); tsCol != nil {
		ts, _ = tsCol.Uint64Value(rowIdx) // ok=false means no timestamp; zero value sorts first (acceptable fallback)
	}
	return ts
}

// reconstructLogRecord reads all columns for one log row and reconstructs an
// OTLP LogsData proto. This is the inverse of addLogRecordFromProto.
func reconstructLogRecord(block *modules_reader.Block, rowIdx int) *logsv1.LogsData {
	record := &logsv1.LogRecord{}
	var resourceAttrs []*commonv1.KeyValue
	var scopeAttrs []*commonv1.KeyValue

	for ck, col := range block.Columns() {
		if !col.IsPresent(rowIdx) {
			continue
		}
		switch ck.Name {
		case "log:timestamp":
			if v, ok := col.Uint64Value(rowIdx); ok {
				record.TimeUnixNano = v
			}
		case "log:observed_timestamp":
			if v, ok := col.Uint64Value(rowIdx); ok {
				record.ObservedTimeUnixNano = v
			}
		case "log:body":
			if v, ok := col.StringValue(rowIdx); ok {
				record.Body = &commonv1.AnyValue{
					Value: &commonv1.AnyValue_StringValue{StringValue: v},
				}
			}
		case "log:severity_number":
			if v, ok := col.Int64Value(rowIdx); ok {
				record.SeverityNumber = logsv1.SeverityNumber(v) //nolint:gosec // safe: severity number fits int32
			}
		case "log:severity_text":
			if v, ok := col.StringValue(rowIdx); ok {
				record.SeverityText = v
			}
		case "log:trace_id":
			if v, ok := col.BytesValue(rowIdx); ok {
				record.TraceId = slices.Clone(v)
			}
		case "log:span_id":
			if v, ok := col.BytesValue(rowIdx); ok {
				record.SpanId = slices.Clone(v)
			}
		case "log:flags":
			if v, ok := col.Uint64Value(rowIdx); ok {
				record.Flags = uint32(v) //nolint:gosec // safe: flags are uint32 widened to uint64
			}
		default:
			// SPEC-11.5: skip auto-parsed body columns (ColumnTypeRangeString under log.*)
			// — they will be re-derived from the body string by the writer.
			if strings.HasPrefix(ck.Name, "log.") && ck.Type == shared.ColumnTypeRangeString {
				continue
			}
			kv := reconstructAttribute(ck, col, rowIdx)
			if kv == nil {
				continue
			}
			switch {
			case strings.HasPrefix(ck.Name, "resource."):
				resourceAttrs = append(resourceAttrs, kv)
			case strings.HasPrefix(ck.Name, "scope."):
				scopeAttrs = append(scopeAttrs, kv)
			case strings.HasPrefix(ck.Name, "log."):
				record.Attributes = append(record.Attributes, kv)
			}
		}
	}

	rl := &logsv1.ResourceLogs{
		ScopeLogs: []*logsv1.ScopeLogs{{
			LogRecords: []*logsv1.LogRecord{record},
		}},
	}
	if len(resourceAttrs) > 0 {
		rl.Resource = &resourcev1.Resource{Attributes: resourceAttrs}
	}
	if len(scopeAttrs) > 0 {
		rl.ScopeLogs[0].Scope = &commonv1.InstrumentationScope{Attributes: scopeAttrs}
	}

	return &logsv1.LogsData{ResourceLogs: []*logsv1.ResourceLogs{rl}}
}

// reconstructAttribute creates a KeyValue proto from a dynamic column value.
func reconstructAttribute(ck shared.ColumnKey, col *modules_reader.Column, rowIdx int) *commonv1.KeyValue {
	// Strip the namespace prefix (resource., scope., log.) to get the bare attribute key.
	name := ck.Name
	switch {
	case strings.HasPrefix(name, "resource."):
		name = name[9:]
	case strings.HasPrefix(name, "scope."):
		name = name[6:]
	case strings.HasPrefix(name, "log."):
		name = name[4:]
	default:
		return nil
	}

	kv := &commonv1.KeyValue{Key: name}
	switch ck.Type {
	case shared.ColumnTypeString, shared.ColumnTypeRangeString:
		if v, ok := col.StringValue(rowIdx); ok {
			kv.Value = &commonv1.AnyValue{Value: &commonv1.AnyValue_StringValue{StringValue: v}}
		}
	case shared.ColumnTypeInt64, shared.ColumnTypeRangeInt64, shared.ColumnTypeRangeDuration:
		if v, ok := col.Int64Value(rowIdx); ok {
			kv.Value = &commonv1.AnyValue{Value: &commonv1.AnyValue_IntValue{IntValue: v}}
		}
	case shared.ColumnTypeFloat64, shared.ColumnTypeRangeFloat64:
		if v, ok := col.Float64Value(rowIdx); ok {
			kv.Value = &commonv1.AnyValue{Value: &commonv1.AnyValue_DoubleValue{DoubleValue: v}}
		}
	case shared.ColumnTypeBool:
		if v, ok := col.BoolValue(rowIdx); ok {
			kv.Value = &commonv1.AnyValue{Value: &commonv1.AnyValue_BoolValue{BoolValue: v}}
		}
	case shared.ColumnTypeBytes, shared.ColumnTypeRangeBytes:
		if v, ok := col.BytesValue(rowIdx); ok {
			kv.Value = &commonv1.AnyValue{Value: &commonv1.AnyValue_BytesValue{BytesValue: slices.Clone(v)}}
		}
	default:
		return nil
	}
	if kv.Value == nil {
		return nil
	}
	return kv
}

// CompactLogFileBytes is a convenience wrapper that reads from a byte slice and returns
// the compacted file as a byte slice. Useful for tests and single-file compaction.
func CompactLogFileBytes(input []byte, cfg Config) ([]byte, error) {
	provider := &bytesProvider{data: input}
	var buf bytes.Buffer
	if err := CompactLogFile(provider, &buf, cfg); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// bytesProvider implements modules_rw.ReaderProvider over a byte slice.
type bytesProvider struct {
	data []byte
}

func (p *bytesProvider) Size() (int64, error) { return int64(len(p.data)), nil }
func (p *bytesProvider) ReadAt(buf []byte, off int64, _ modules_rw.DataType) (int, error) {
	if off < 0 || off >= int64(len(p.data)) {
		return 0, io.EOF
	}
	n := copy(buf, p.data[off:])
	if n < len(buf) {
		return n, io.EOF
	}
	return n, nil
}
