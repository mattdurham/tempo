package parquetutil

import (
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/grafana/tempo/pkg/tempopb"
	"github.com/grafana/tempo/tempodb/backend"
	"github.com/grafana/tempo/tempodb/encoding/vparquet4"
	"github.com/grafana/tempo/tempodb/encoding/vparquet5"
	"github.com/mattdurham/blockpack/internal/otlpconvert"
	"github.com/parquet-go/parquet-go"
	tracev1 "go.opentelemetry.io/proto/otlp/trace/v1"
)

const defaultSpansPerBlock = 2000

type ConversionStats struct {
	Size       int64
	TraceCount int
	SpanCount  int
}

type SourceStats struct {
	TraceCount int
	SpanCount  int
}

func GetParquetStats(parquetPath string) (SourceStats, error) {
	tempoTraces, err := readTempoTracesFromParquet(parquetPath)
	if err != nil {
		return SourceStats{}, err
	}

	totalSpans := 0
	for _, tr := range tempoTraces {
		otlpTrace := tempoTraceToOTLP(tr)
		for _, rs := range otlpTrace.ResourceSpans {
			for _, ss := range rs.ScopeSpans {
				totalSpans += len(ss.Spans)
			}
		}
	}

	return SourceStats{
		TraceCount: len(tempoTraces),
		SpanCount:  totalSpans,
	}, nil
}

func ConvertParquetToBlockpack(parquetPath, outPath string) (ConversionStats, error) {
	blockpackBytes, traceCount, spanCount, err := ConvertParquetToBlockpackBytes(parquetPath)
	if err != nil {
		return ConversionStats{}, err
	}

	if err := os.WriteFile(outPath, blockpackBytes, 0o644); err != nil {
		return ConversionStats{}, err
	}

	outInfo, err := os.Stat(outPath)
	if err != nil {
		return ConversionStats{}, err
	}
	return ConversionStats{
		Size:       outInfo.Size(),
		TraceCount: traceCount,
		SpanCount:  spanCount,
	}, nil
}

// ConvertAndCompare reads parquet once, converts to blockpack, and compares the results.
// This is more efficient than calling ConvertParquetToBlockpack followed by DeepCompareParquetBlockpack
// because it reuses the parquet data instead of reading it twice.
func ConvertAndCompare(parquetPath, outPath string) (ConversionStats, *CompareResult, error) {
	// Read parquet once
	tempoTraces, err := readTempoTracesFromParquet(parquetPath)
	if err != nil {
		return ConversionStats{}, nil, err
	}

	// Convert to OTLP
	otlpTraces := make([]*tracev1.TracesData, 0, len(tempoTraces))
	totalSpans := 0
	for _, tr := range tempoTraces {
		otlpTrace := tempoTraceToOTLP(tr)
		otlpTraces = append(otlpTraces, otlpTrace)
		for _, rs := range otlpTrace.ResourceSpans {
			for _, ss := range rs.ScopeSpans {
				totalSpans += len(ss.Spans)
			}
		}
	}

	// Convert to blockpack bytes
	blockpackBytes, err := otlpconvert.WriteBlockpack(otlpTraces, defaultSpansPerBlock)
	if err != nil {
		return ConversionStats{}, nil, err
	}

	// Write blockpack file
	if err := os.WriteFile(outPath, blockpackBytes, 0o644); err != nil {
		return ConversionStats{}, nil, err
	}

	// Get file size
	outInfo, err := os.Stat(outPath)
	if err != nil {
		return ConversionStats{}, nil, err
	}

	stats := ConversionStats{
		Size:       outInfo.Size(),
		TraceCount: len(tempoTraces),
		SpanCount:  totalSpans,
	}

	// Now compare using already-loaded data
	// Extract spans from already-converted OTLP traces (parquet source)
	parquetSpans, parquetTraces := spansFromOTLPTraces(otlpTraces)

	// Extract spans from blockpack
	blockpackSpans, blockpackTraces, err := spansFromBlockpackBytes(blockpackBytes)
	if err != nil {
		return stats, nil, fmt.Errorf("extract blockpack spans: %w", err)
	}

	// Compare
	result := &CompareResult{
		SpanCountBlockpack:  len(blockpackSpans),
		SpanCountParquet:    len(parquetSpans),
		TraceCountBlockpack: len(blockpackTraces),
		TraceCountParquet:   len(parquetTraces),
	}

	missing, extra, diffs := compareSpanMaps(parquetSpans, blockpackSpans)
	result.MissingSpanCount = len(missing)
	result.ExtraSpanCount = len(extra)
	result.AttributeDiffCount = len(diffs)
	result.MissingSpans = trimStringList(missing, compareMaxSamples)
	result.ExtraSpans = trimStringList(extra, compareMaxSamples)
	result.AttributeDiffs = trimDiffs(diffs, compareMaxSamples)

	result.Match = (len(missing) == 0 && len(extra) == 0 && len(diffs) == 0)

	return stats, result, nil
}

func ConvertParquetToBlockpackBytes(parquetPath string) ([]byte, int, int, error) {
	tempoTraces, err := readTempoTracesFromParquet(parquetPath)
	if err != nil {
		return nil, 0, 0, err
	}

	otlpTraces := make([]*tracev1.TracesData, 0, len(tempoTraces))
	totalSpans := 0
	for _, tr := range tempoTraces {
		otlpTrace := tempoTraceToOTLP(tr)
		otlpTraces = append(otlpTraces, otlpTrace)
		// Count spans
		for _, rs := range otlpTrace.ResourceSpans {
			for _, ss := range rs.ScopeSpans {
				totalSpans += len(ss.Spans)
			}
		}
	}

	blockpackBytes, err := otlpconvert.WriteBlockpack(otlpTraces, defaultSpansPerBlock)
	if err != nil {
		return nil, 0, 0, err
	}
	return blockpackBytes, len(tempoTraces), totalSpans, nil
}

func readTempoTracesFromParquet(parquetPath string) ([]*tempopb.Trace, error) {
	// Try vparquet5 first
	traces, err := readTempoTracesWithSchema(parquetPath, true)
	if err == nil {
		return traces, nil
	}

	// If vparquet5 fails, try vparquet4
	traces, err4 := readTempoTracesWithSchema(parquetPath, false)
	if err4 == nil {
		return traces, nil
	}

	// Return the vparquet5 error as it's more likely to be the correct version
	return nil, fmt.Errorf("failed to read as vparquet5: %w (also tried vparquet4: %v)", err, err4)
}

func readTempoTracesWithSchema(parquetPath string, useV5 bool) ([]*tempopb.Trace, error) {
	f, err := os.Open(parquetPath)
	if err != nil {
		return nil, err
	}
	defer func() { _ = f.Close() }()

	info, err := f.Stat()
	if err != nil {
		return nil, err
	}

	pf, err := parquet.OpenFile(f, info.Size())
	if err != nil {
		return nil, err
	}

	var schema *parquet.Schema
	if useV5 {
		schema = parquet.SchemaOf(&vparquet5.Trace{})
	} else {
		schema = parquet.SchemaOf(&vparquet4.Trace{})
	}

	reader := parquet.NewReader(pf, schema)
	defer func() { _ = reader.Close() }()

	meta := &backend.BlockMeta{}
	tempoTraces := make([]*tempopb.Trace, 0, 1024)

	for {
		rows := []parquet.Row{make(parquet.Row, 0, len(schema.Columns()))}
		n, err := reader.ReadRows(rows)
		eof := errors.Is(err, io.EOF)
		if eof && n == 0 {
			break
		}
		if eof {
			err = nil
		}
		if err != nil {
			return nil, fmt.Errorf("read parquet: %w", err)
		}
		if n == 0 {
			continue
		}

		if useV5 {
			pqTrace := &vparquet5.Trace{}
			if err := schema.Reconstruct(pqTrace, rows[0]); err != nil {
				return nil, fmt.Errorf("decode parquet: %w", err)
			}
			tempoTraces = append(tempoTraces, vparquet5.ParquetTraceToTempopbTrace(meta, pqTrace))
		} else {
			pqTrace := &vparquet4.Trace{}
			if err := schema.Reconstruct(pqTrace, rows[0]); err != nil {
				return nil, fmt.Errorf("decode parquet: %w", err)
			}
			tempoTraces = append(tempoTraces, vparquet4.ParquetTraceToTempopbTrace(meta, pqTrace))
		}

		if eof {
			break
		}
	}

	if len(tempoTraces) == 0 {
		return nil, fmt.Errorf("no traces found in parquet")
	}
	return tempoTraces, nil
}
