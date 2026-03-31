// Package main provides a command-line tool for generating unified reports.
package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"html"
	"io"
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"time"

	"golang.org/x/perf/benchfmt"
	"golang.org/x/text/cases"
	"golang.org/x/text/language"
)

const (
	metricBytesRead          = "bytes_read"
	metricAllocsOp           = "allocs/op"
	metricBytesOp            = "B/op"
	metricSecOp              = "sec/op"
	metricIOOps              = "io_ops"
	metricCPUMs              = "cpuMs"
	metricCostS3Get          = "cost_s3_get"
	metricCostS3Xfer         = "cost_s3_xfer"
	metricCostLambdaCompute  = "cost_lambda_compute"
	metricCostLambdaRequests = "cost_lambda_requests"
	metricCostTotal          = "cost_total"

	formatAggregation = "aggregation"
	formatBlockpack   = "blockpack"
	formatParquet     = "parquet"

	winCellTie = `<td class="win-cell" style="color:#999; text-align:center">—</td>`
)

// slugifyID sanitizes a string for use as an HTML element ID.
// Only allows [A-Za-z0-9_-], replacing other characters with hyphens.
func slugifyID(parts ...string) string {
	raw := strings.Join(parts, "-")
	var sb strings.Builder
	for _, r := range raw {
		if (r >= 'A' && r <= 'Z') || (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') || r == '_' || r == '-' {
			sb.WriteRune(r)
		} else {
			sb.WriteRune('-')
		}
	}
	return sb.String()
}

// BenchmarkMetadata contains metadata about the benchmark run
type BenchmarkMetadata struct {
	Timestamp   string     `json:"timestamp"`
	GitHash     string     `json:"gitHash"`
	ResultsHash string     `json:"resultsHash"`
	System      SystemInfo `json:"system"`
}

// SystemInfo contains system information
type SystemInfo struct {
	CPUModel  string  `json:"cpuModel"`
	OS        string  `json:"os"`
	GoVersion string  `json:"goVersion"`
	RAM       float64 `json:"ram"` // GB
}

// BenchmarkReport contains full benchmark report with metadata
type BenchmarkReport struct {
	FileSizes    map[string]int64               `json:"fileSizes,omitempty"`
	Metadata     BenchmarkMetadata              `json:"metadata"`
	OTELDemo     []OTELDemoResult               `json:"otelDemo"`
	Comparisons  []ComparisonResult             `json:"comparisons"`
	Aggregations []AggregationComparisonResult  `json:"aggregations"`
	WritePath    []WritePathResult              `json:"writePath,omitempty"`
	TraceMetrics []TraceMetricsComparisonResult `json:"traceMetrics,omitempty"`
}

// WritePathResult represents a single format's result from a write path benchmark.
type WritePathResult struct {
	Scale        string // sub-benchmark scale label, e.g. "small"
	Format       string // "blockpack" or "parquet"
	TimeNs       int64
	TimeMs       float64
	CPUMs        float64
	BytesWritten int64
	BytesMB      float64
	MemoryBytes  int64
	MemoryMB     float64
	Allocs       int64
	SpansWritten int64
	NsPerSpan    float64
}

// OTELDemoResult represents a otel-demo-data benchmark result
type OTELDemoResult struct {
	QueryName          string
	Format             string // "blockpack" or "parquet"
	TimeNs             int64
	TimeMs             float64
	CPUMs              float64
	BytesRead          int64
	BytesReadMB        float64
	IOOps              int64
	BlocksScanned      int64
	MemoryBytes        int64
	MemoryMB           float64
	Allocs             int64
	Traces             int64
	Spans              int64
	CostS3Get          float64
	CostS3Xfer         float64
	CostLambdaCompute  float64
	CostLambdaRequests float64
	CostTotal          float64
}

// ComparisonResult represents a format comparison result
type ComparisonResult struct {
	QueryName          string
	Format             string // "blockpack" or "parquet"
	TimeNs             int64
	TimeMs             float64
	CPUMs              float64
	BytesRead          int64
	BytesReadMB        float64
	IOOps              int64
	MemoryBytes        int64
	MemoryMB           float64
	Allocs             int64
	Traces             int64
	Spans              int64
	CostS3Get          float64
	CostS3Xfer         float64
	CostLambdaCompute  float64
	CostLambdaRequests float64
	CostTotal          float64
}

// AggregationComparisonResult represents an aggregation comparison benchmark result
type AggregationComparisonResult struct {
	QueryName          string
	Format             string // "aggregation", "manual", or "parquet"
	TimeNs             int64
	TimeMs             float64
	BytesRead          int64
	BytesReadMB        float64
	IOOps              int64
	BytesPerIO         float64
	MemoryBytes        int64
	MemoryMB           float64
	Allocs             int64
	Groups             int64
	TimeValues         int64
	TotalSpans         int64
	CostS3Get          float64
	CostS3Xfer         float64
	CostLambdaCompute  float64
	CostLambdaRequests float64
	CostTotal          float64
}

// TraceMetricsComparisonResult represents a TraceQL metrics comparison benchmark result.
type TraceMetricsComparisonResult struct {
	QueryName          string
	Format             string // "blockpack" or "parquet"
	TimeNs             int64
	TimeMs             float64
	BytesRead          int64
	BytesReadMB        float64
	IOOps              int64
	BytesPerIO         float64
	MemoryBytes        int64
	MemoryMB           float64
	Allocs             int64
	Groups             int64
	TimeValues         int64
	CostS3Get          float64
	CostS3Xfer         float64
	CostLambdaCompute  float64
	CostLambdaRequests float64
	CostTotal          float64
}

// stripGOMAXPROCS removes the -N suffix from benchmark names (e.g., "blockpack-24" -> "blockpack")
func stripGOMAXPROCS(s string) string {
	if idx := strings.LastIndex(s, "-"); idx > 0 {
		return s[:idx]
	}
	return s
}

// parseBenchmarks reads all benchmarks from input using benchfmt
func parseBenchmarks(
	r io.Reader,
) ([]OTELDemoResult, []ComparisonResult, []AggregationComparisonResult, []WritePathResult, []TraceMetricsComparisonResult, error) {
	var otelDemo []OTELDemoResult
	var comparisons []ComparisonResult
	var aggregations []AggregationComparisonResult
	var writePaths []WritePathResult
	var traceMetrics []TraceMetricsComparisonResult

	reader := benchfmt.NewReader(r, "stdin")

	for reader.Scan() {
		rec := reader.Result()

		// Type assert to *Result (skip SyntaxError)
		result, ok := rec.(*benchfmt.Result)
		if !ok {
			continue
		}

		fullName := string(result.Name.Full())

		// Determine benchmark type and parse accordingly
		// Support both legacy names (RealWorldQueries) and new names (OTELDemo/ComprehensiveSuite)
		if strings.Contains(fullName, "RealWorldQueries/") ||
			strings.Contains(fullName, "ComprehensiveSuite/") ||
			strings.Contains(fullName, "GetByTraceID/") {
			if od := parseOTELDemoResult(result, fullName); od != nil {
				otelDemo = append(otelDemo, *od)
			}
		} else if strings.Contains(fullName, "FormatComparison/") {
			if comp := parseComparisonResult(result, fullName); comp != nil {
				comparisons = append(comparisons, *comp)
			}
		} else if strings.Contains(fullName, "AggregationComparison/") {
			if agg := parseAggregationComparisonResult(result, fullName); agg != nil {
				aggregations = append(aggregations, *agg)
			}
		} else if strings.Contains(fullName, "WritePathComparison/") {
			if wp := parseWritePathResult(result, fullName); wp != nil {
				writePaths = append(writePaths, *wp)
			}
		} else if strings.Contains(fullName, "TraceMetricsComparison/") {
			if tm := parseTraceMetricsComparisonResult(result, fullName); tm != nil {
				traceMetrics = append(traceMetrics, *tm)
			}
		}
	}

	if err := reader.Err(); err != nil {
		return nil, nil, nil, nil, nil, fmt.Errorf("error reading benchmarks: %w", err)
	}

	return otelDemo, comparisons, aggregations, writePaths, traceMetrics, nil
}

// parseOTELDemoResult parses a comprehensive benchmark result (110 diverse TraceQL queries)
func parseOTELDemoResult(result *benchfmt.Result, fullName string) *OTELDemoResult {
	var benchType, queryName, format string

	// Determine benchmark type - support both legacy and new names
	if strings.Contains(fullName, "RealWorldQueries/") {
		benchType = "realworld"
		// Extract after "RealWorldQueries/" or "RealWorldQueriesLimited/"
		prefix := "RealWorldQueries/"
		if strings.Contains(fullName, "RealWorldQueriesLimited/") {
			prefix = "RealWorldQueriesLimited/"
		}
		idx := strings.Index(fullName, prefix)
		if idx >= 0 {
			parts := strings.Split(fullName[idx+len(prefix):], "/")
			if len(parts) >= 2 {
				queryName = parts[0]
				format = stripGOMAXPROCS(parts[1])
			}
		}
	} else if strings.Contains(fullName, "ComprehensiveSuite/") {
		benchType = "comprehensive"
		idx := strings.Index(fullName, "ComprehensiveSuite/")
		if idx >= 0 {
			parts := strings.Split(fullName[idx+len("ComprehensiveSuite/"):], "/")
			if len(parts) >= 2 {
				queryName = parts[0]
				format = stripGOMAXPROCS(parts[1])
			}
		}
	} else if strings.Contains(fullName, "GetByTraceID/") {
		benchType = "trace"
		queryName = "trace-by-id"
		idx := strings.Index(fullName, "GetByTraceID/")
		if idx >= 0 {
			parts := strings.Split(fullName[idx+len("GetByTraceID/"):], "/")
			if len(parts) >= 1 {
				format = stripGOMAXPROCS(parts[0])
			}
		}
	} else {
		return nil
	}

	// Mark trace-by-id queries by adjusting format
	var resultFormat string
	switch benchType {
	case "realworld", "comprehensive":
		resultFormat = format
	case "trace":
		resultFormat = "trace-" + format
	default:
		resultFormat = format
	}

	od := &OTELDemoResult{
		QueryName: queryName,
		Format:    resultFormat,
	}

	// Extract all metrics
	for _, v := range result.Values {
		switch v.Unit {
		case metricSecOp:
			// benchfmt normalizes to metricSecOp, convert to ns/op
			od.TimeNs = int64(v.Value * 1_000_000_000)
			od.TimeMs = v.Value * 1000
		case metricBytesOp:
			od.MemoryBytes = int64(v.Value)
			od.MemoryMB = v.Value / (1024 * 1024)
		case metricAllocsOp:
			od.Allocs = int64(v.Value)
		case "blocks":
			od.BlocksScanned = int64(v.Value)
		case metricBytesRead:
			od.BytesRead = int64(v.Value)
			od.BytesReadMB = v.Value / (1024 * 1024)
		case metricIOOps:
			od.IOOps = int64(v.Value)
		case metricCPUMs:
			od.CPUMs = v.Value
		case "traces":
			od.Traces = int64(v.Value)
		case "spans":
			od.Spans = int64(v.Value)
		case "rows":
			// Legacy: some benchmarks report "rows" instead of "spans"
			if od.Spans == 0 {
				od.Spans = int64(v.Value)
			}
		case metricCostS3Get:
			od.CostS3Get = v.Value
		case metricCostS3Xfer:
			od.CostS3Xfer = v.Value
		case metricCostLambdaCompute:
			od.CostLambdaCompute = v.Value
		case metricCostLambdaRequests:
			od.CostLambdaRequests = v.Value
		case metricCostTotal:
			od.CostTotal = v.Value
		}
	}

	return od
}

// parseComparisonResult parses a format comparison benchmark result
func parseComparisonResult(result *benchfmt.Result, fullName string) *ComparisonResult {
	parts := strings.Split(strings.TrimPrefix(fullName, "FormatComparison/"), "/")
	if len(parts) < 2 {
		return nil
	}

	comp := &ComparisonResult{
		QueryName: parts[0],
		Format:    stripGOMAXPROCS(parts[1]),
	}

	// Extract all metrics
	for _, v := range result.Values {
		switch v.Unit {
		case metricSecOp:
			// benchfmt normalizes to metricSecOp, convert to ns/op
			comp.TimeNs = int64(v.Value * 1_000_000_000)
			comp.TimeMs = v.Value * 1000
		case metricBytesOp:
			comp.MemoryBytes = int64(v.Value)
			comp.MemoryMB = v.Value / (1024 * 1024)
		case metricAllocsOp:
			comp.Allocs = int64(v.Value)
		case metricBytesRead:
			comp.BytesRead = int64(v.Value)
			comp.BytesReadMB = v.Value / (1024 * 1024)
		case metricIOOps:
			comp.IOOps = int64(v.Value)
		case metricCPUMs:
			comp.CPUMs = v.Value
		case "traces":
			comp.Traces = int64(v.Value)
		case "spans":
			comp.Spans = int64(v.Value)
		case metricCostS3Get:
			comp.CostS3Get = v.Value
		case metricCostS3Xfer:
			comp.CostS3Xfer = v.Value
		case metricCostLambdaCompute:
			comp.CostLambdaCompute = v.Value
		case metricCostLambdaRequests:
			comp.CostLambdaRequests = v.Value
		case metricCostTotal:
			comp.CostTotal = v.Value
		}
	}

	return comp
}

// parseAggregationComparisonResult parses an aggregation comparison benchmark result
func parseAggregationComparisonResult(result *benchfmt.Result, fullName string) *AggregationComparisonResult {
	if !strings.HasPrefix(fullName, "AggregationComparison/") {
		return nil
	}

	parts := strings.Split(strings.TrimPrefix(fullName, "AggregationComparison/"), "/")
	if len(parts) < 2 {
		return nil
	}

	queryName := stripGOMAXPROCS(parts[0])
	format := stripGOMAXPROCS(parts[1])
	switch format {
	case formatBlockpack:
		format = formatAggregation
	case formatParquet:
		format = formatParquet
	default:
		return nil
	}

	agg := &AggregationComparisonResult{
		QueryName: queryName,
		Format:    format,
	}

	for _, v := range result.Values {
		switch v.Unit {
		case metricSecOp:
			agg.TimeNs = int64(v.Value * 1_000_000_000)
			agg.TimeMs = v.Value * 1000
		case metricBytesOp:
			agg.MemoryBytes = int64(v.Value)
			agg.MemoryMB = v.Value / (1024 * 1024)
		case metricAllocsOp:
			agg.Allocs = int64(v.Value)
		case metricBytesRead:
			agg.BytesRead = int64(v.Value)
			agg.BytesReadMB = v.Value / (1024 * 1024)
		case metricIOOps:
			agg.IOOps = int64(v.Value)
		case "bytes/io":
			agg.BytesPerIO = v.Value
		case "groups":
			agg.Groups = int64(v.Value)
		case "time_values":
			agg.TimeValues = int64(v.Value)
		case "total_spans":
			agg.TotalSpans = int64(v.Value)
		case metricCostS3Get:
			agg.CostS3Get = v.Value
		case metricCostS3Xfer:
			agg.CostS3Xfer = v.Value
		case metricCostLambdaCompute:
			agg.CostLambdaCompute = v.Value
		case metricCostLambdaRequests:
			agg.CostLambdaRequests = v.Value
		case metricCostTotal:
			agg.CostTotal = v.Value
		}
	}

	return agg
}

// parseWritePathResult parses a WritePathComparison benchmark result.
// Expected fullName format: "WritePathComparison/small/blockpack-N"
func parseWritePathResult(result *benchfmt.Result, fullName string) *WritePathResult {
	if !strings.HasPrefix(fullName, "WritePathComparison/") {
		return nil
	}

	parts := strings.Split(strings.TrimPrefix(fullName, "WritePathComparison/"), "/")
	if len(parts) < 2 {
		return nil
	}

	wp := &WritePathResult{
		Scale:  parts[0],
		Format: stripGOMAXPROCS(parts[1]),
	}

	for _, v := range result.Values {
		switch v.Unit {
		case metricSecOp:
			wp.TimeNs = int64(v.Value * 1_000_000_000)
			wp.TimeMs = v.Value * 1000
		case metricBytesOp:
			wp.MemoryBytes = int64(v.Value)
			wp.MemoryMB = v.Value / (1024 * 1024)
		case metricAllocsOp:
			wp.Allocs = int64(v.Value)
		case "bytes_written":
			wp.BytesWritten = int64(v.Value)
			wp.BytesMB = v.Value / (1024 * 1024)
		case "spans_written":
			wp.SpansWritten = int64(v.Value)
		case "ns/span":
			wp.NsPerSpan = v.Value
		case metricCPUMs:
			wp.CPUMs = v.Value
		}
	}

	return wp
}

// parseTraceMetricsComparisonResult parses a TraceQL metrics comparison benchmark result.
func parseTraceMetricsComparisonResult(result *benchfmt.Result, fullName string) *TraceMetricsComparisonResult {
	if !strings.HasPrefix(fullName, "TraceMetricsComparison/") {
		return nil
	}
	parts := strings.Split(strings.TrimPrefix(fullName, "TraceMetricsComparison/"), "/")
	if len(parts) < 2 {
		return nil
	}
	tm := &TraceMetricsComparisonResult{
		QueryName: parts[0],
		Format:    stripGOMAXPROCS(parts[1]),
	}
	for _, v := range result.Values {
		switch v.Unit {
		case metricSecOp:
			tm.TimeNs = int64(v.Value * 1_000_000_000)
			tm.TimeMs = v.Value * 1000
		case metricBytesOp:
			tm.MemoryBytes = int64(v.Value)
			tm.MemoryMB = v.Value / (1024 * 1024)
		case metricAllocsOp:
			tm.Allocs = int64(v.Value)
		case metricBytesRead:
			tm.BytesRead = int64(v.Value)
			tm.BytesReadMB = v.Value / (1024 * 1024)
		case metricIOOps:
			tm.IOOps = int64(v.Value)
		case "bytes/io":
			tm.BytesPerIO = v.Value
		case "groups":
			tm.Groups = int64(v.Value)
		case "time_values":
			tm.TimeValues = int64(v.Value)
		case metricCostS3Get:
			tm.CostS3Get = v.Value
		case metricCostS3Xfer:
			tm.CostS3Xfer = v.Value
		case metricCostLambdaCompute:
			tm.CostLambdaCompute = v.Value
		case metricCostLambdaRequests:
			tm.CostLambdaRequests = v.Value
		case metricCostTotal:
			tm.CostTotal = v.Value
		}
	}
	return tm
}

// validateOTELDemoResults validates OTEL Demo benchmark results
func validateOTELDemoResults(results []OTELDemoResult) error {
	for _, r := range results {
		// Validate IO bytes > 0
		if r.BytesRead <= 0 {
			return fmt.Errorf(
				"validation failed for OTEL Demo result '%s': IO bytes must be > 0 (got %d)",
				r.QueryName,
				r.BytesRead,
			)
		}

		// Validate IO ops > 0 (data was actually read)
		if r.IOOps <= 0 {
			return fmt.Errorf(
				"validation failed for OTEL Demo result '%s': IO ops must be > 0 (got %d)",
				r.QueryName,
				r.IOOps,
			)
		}
	}
	return nil
}

// validateComparisonResults validates format comparison results
func validateComparisonResults(results []ComparisonResult) error {
	// Group by query name
	queryMap := make(map[string][]ComparisonResult)
	for _, r := range results {
		queryMap[r.QueryName] = append(queryMap[r.QueryName], r)
	}

	// Validate each query has both formats
	for queryName, formats := range queryMap {
		var hasBlockpack, hasParquet bool
		var blockpackSpans, parquetSpans int64

		for _, f := range formats {
			// Validate IO bytes > 0
			if f.BytesRead <= 0 {
				return fmt.Errorf(
					"validation failed for comparison '%s' format '%s': IO bytes must be > 0 (got %d)",
					queryName,
					f.Format,
					f.BytesRead,
				)
			}

			// Validate IO ops > 0
			if f.IOOps <= 0 {
				return fmt.Errorf(
					"validation failed for comparison '%s' format '%s': IO ops must be > 0 (got %d)",
					queryName,
					f.Format,
					f.IOOps,
				)
			}

			switch f.Format {
			case formatBlockpack:
				hasBlockpack = true
				blockpackSpans = f.Spans
			case formatParquet:
				hasParquet = true
				parquetSpans = f.Spans
			}
		}

		// Check both formats present
		if !hasBlockpack {
			return fmt.Errorf("validation failed for comparison '%s': missing blockpack format", queryName)
		}
		if !hasParquet {
			return fmt.Errorf("validation failed for comparison '%s': missing parquet format", queryName)
		}

		// For exhaustive queries, span counts must match exactly
		if strings.Contains(strings.ToLower(queryName), "exhaustive") ||
			strings.Contains(strings.ToLower(queryName), "full") {
			if blockpackSpans != parquetSpans {
				return fmt.Errorf(
					"validation failed for comparison '%s': span count mismatch - blockpack: %d, parquet: %d (exhaustive queries must match exactly)",
					queryName,
					blockpackSpans,
					parquetSpans,
				)
			}
		}
	}

	return nil
}

// validateAggregationResults validates aggregation comparison results
func validateAggregationResults(results []AggregationComparisonResult) error {
	if len(results) == 0 {
		return nil
	}

	type pair struct {
		aggregation *AggregationComparisonResult
		parquet     *AggregationComparisonResult
	}

	byQuery := make(map[string]*pair)
	for i := range results {
		result := &results[i]
		if _, exists := byQuery[result.QueryName]; !exists {
			byQuery[result.QueryName] = &pair{}
		}

		switch result.Format {
		case formatAggregation:
			byQuery[result.QueryName].aggregation = result
		case formatParquet:
			byQuery[result.QueryName].parquet = result
		}
	}

	for queryName, set := range byQuery {
		if set.aggregation == nil || set.parquet == nil {
			return fmt.Errorf("aggregation comparison %q missing formats: aggregation=%t parquet=%t",
				queryName, set.aggregation != nil, set.parquet != nil)
		}

		formats := []*AggregationComparisonResult{set.aggregation, set.parquet}
		for _, format := range formats {
			// Allow Groups=0 and TimeValues=0 (valid for queries with no matching data)
			// Allow CostTotal=0 (valid for zero-cost operations)
			// Allow BytesRead=0 and IOOps=0 (valid for cached reads)
			// Require time, memory, and allocs to be > 0
			if format.TimeMs <= 0 || format.MemoryMB <= 0 || format.Allocs <= 0 {
				return fmt.Errorf(
					"aggregation comparison %q format %q missing required metrics (time=%.2fms bytes=%d metricIOOps=%d groups=%d time_values=%d memory=%.2fMB allocs=%d cost=%.6f)",
					queryName,
					format.Format,
					format.TimeMs,
					format.BytesRead,
					format.IOOps,
					format.Groups,
					format.TimeValues,
					format.MemoryMB,
					format.Allocs,
					format.CostTotal,
				)
			}
		}
	}

	return nil
}

// validateBenchmarkCompleteness checks that all expected benchmark types are present
func validateBenchmarkCompleteness(
	otelDemo []OTELDemoResult,
	comparisons []ComparisonResult,
	aggregations []AggregationComparisonResult,
) error {
	// Check that we have OTEL Demo results
	if len(otelDemo) == 0 {
		return fmt.Errorf(
			"no OTEL Demo (RealWorldQueries) benchmark results found - benchmark suite may have failed or not run",
		)
	}

	// Check that we have Format Comparison results
	if len(comparisons) == 0 {
		return fmt.Errorf(
			"no Format Comparison benchmark results found - BenchmarkFormatComparison may have failed or not run",
		)
	}

	// Aggregation comparisons are optional (may not exist in all benchmark runs)
	// but if they exist, they should be validated

	return nil
}

// validateOTELDemoDataQuality checks that OTEL Demo queries return meaningful results
func validateOTELDemoDataQuality(results []OTELDemoResult) error {
	// Track queries that should return data
	queriesWithZeroResults := []string{}

	for _, r := range results {
		// Check if this is a query that should return spans/traces
		// Queries like "grpc-calls", "database-operations" etc. should have results
		// Only "filter" queries might legitimately have 0 results if nothing matches
		isFilterQuery := strings.Contains(strings.ToLower(r.QueryName), "filter") ||
			strings.Contains(strings.ToLower(r.QueryName), "error")

		// If not a filter query and has zero results, flag it
		if !isFilterQuery && r.Spans == 0 && r.Traces == 0 {
			queriesWithZeroResults = append(queriesWithZeroResults, r.QueryName)
		}
	}

	if len(queriesWithZeroResults) > 0 {
		return fmt.Errorf(
			"OTEL Demo queries returned no results (0 spans, 0 traces): %v - this indicates corrupted test data or query failures",
			queriesWithZeroResults,
		)
	}

	return nil
}

// validateFormatComparisonMatching checks that blockpack and parquet return matching results
func validateFormatComparisonMatching(results []ComparisonResult) error {
	// Group by query name
	queryMap := make(map[string]map[string]*ComparisonResult)
	for i := range results {
		r := &results[i]
		if queryMap[r.QueryName] == nil {
			queryMap[r.QueryName] = make(map[string]*ComparisonResult)
		}
		queryMap[r.QueryName][r.Format] = r
	}

	var mismatches []string
	for queryName, formats := range queryMap {
		blockpack := formats[formatBlockpack]
		parquet := formats[formatParquet]

		if blockpack == nil || parquet == nil {
			continue // Already validated by validateComparisonResults
		}

		// Check for span count mismatches (allow up to 1% difference for rounding)
		spanDiff := float64(blockpack.Spans - parquet.Spans)
		spanPercent := 0.0
		if parquet.Spans > 0 {
			spanPercent = (spanDiff / float64(parquet.Spans)) * 100
		}

		if spanPercent > 1.0 || spanPercent < -1.0 {
			mismatches = append(mismatches, fmt.Sprintf("%s: blockpack=%d spans, parquet=%d spans (%.1f%% difference)",
				queryName, blockpack.Spans, parquet.Spans, spanPercent))
		}

		// Check for trace count mismatches
		traceDiff := float64(blockpack.Traces - parquet.Traces)
		tracePercent := 0.0
		if parquet.Traces > 0 {
			tracePercent = (traceDiff / float64(parquet.Traces)) * 100
		}

		if tracePercent > 1.0 || tracePercent < -1.0 {
			mismatches = append(
				mismatches,
				fmt.Sprintf("%s: blockpack=%d traces, parquet=%d traces (%.1f%% difference)",
					queryName, blockpack.Traces, parquet.Traces, tracePercent),
			)
		}
	}

	if len(mismatches) > 0 {
		return fmt.Errorf(
			"format comparison results don't match between blockpack and parquet:\n  %s",
			strings.Join(mismatches, "\n  "),
		)
	}

	return nil
}

// validateWritePathResults validates write path benchmark results.
// Returns nil if writePaths is empty (write benchmarks are optional).
func validateWritePathResults(writePaths []WritePathResult) error {
	if len(writePaths) == 0 {
		return nil
	}

	var hasBytesWritten, hasSpansWritten bool

	for _, wp := range writePaths {
		if wp.BytesWritten > 0 {
			hasBytesWritten = true
		}
		if wp.SpansWritten > 0 {
			hasSpansWritten = true
		}
	}

	if !hasBytesWritten {
		return fmt.Errorf(
			"validation failed for write path results: no entry has BytesWritten > 0 (data may be corrupt)",
		)
	}

	if !hasSpansWritten {
		return fmt.Errorf(
			"validation failed for write path results: no entry has SpansWritten > 0 (data may be corrupt)",
		)
	}

	// Group by scale and verify both blockpack and parquet formats are present for each scale.
	scaleMap := make(map[string]map[string]bool)
	for _, wp := range writePaths {
		if scaleMap[wp.Scale] == nil {
			scaleMap[wp.Scale] = make(map[string]bool)
		}
		scaleMap[wp.Scale][wp.Format] = true
	}

	for scale, formats := range scaleMap {
		if !formats[formatBlockpack] {
			return fmt.Errorf("validation failed for write path scale %q: missing blockpack format", scale)
		}
		if !formats[formatParquet] {
			return fmt.Errorf("validation failed for write path scale %q: missing parquet format", scale)
		}
	}

	return nil
}

// validateResults validates all benchmark results
func validateResults(
	otelDemo []OTELDemoResult,
	comparisons []ComparisonResult,
	aggregations []AggregationComparisonResult,
	writePaths []WritePathResult,
) error {
	// Check that all expected benchmark types are present
	if err := validateBenchmarkCompleteness(otelDemo, comparisons, aggregations); err != nil {
		return fmt.Errorf("benchmark completeness check failed: %w", err)
	}

	// Validate OTEL Demo results
	if err := validateOTELDemoResults(otelDemo); err != nil {
		return fmt.Errorf("OTEL Demo validation failed: %w", err)
	}

	// Check OTEL Demo data quality (queries should return results)
	if err := validateOTELDemoDataQuality(otelDemo); err != nil {
		return fmt.Errorf("OTEL Demo data quality check failed: %w", err)
	}

	// Validate format comparison results
	if err := validateComparisonResults(comparisons); err != nil {
		return fmt.Errorf("format comparison validation failed: %w", err)
	}

	// Check that blockpack and parquet results match
	if err := validateFormatComparisonMatching(comparisons); err != nil {
		return fmt.Errorf("format comparison matching failed: %w", err)
	}

	// Validate aggregation results
	if err := validateAggregationResults(aggregations); err != nil {
		return fmt.Errorf("aggregation validation failed: %w", err)
	}

	// Validate write path results
	if err := validateWritePathResults(writePaths); err != nil {
		return fmt.Errorf("write path validation failed: %w", err)
	}

	return nil
}

// collectSystemInfo gathers system information
func collectSystemInfo() SystemInfo {
	info := SystemInfo{
		GoVersion: runtime.Version(),
		OS:        runtime.GOOS,
	}

	// Get detailed OS info on Linux
	if runtime.GOOS == "linux" {
		if output, err := exec.Command("uname", "-r").Output(); err == nil {
			info.OS = "Linux " + strings.TrimSpace(string(output))
		}

		// Get CPU model from /proc/cpuinfo
		if data, err := os.ReadFile("/proc/cpuinfo"); err == nil {
			lines := strings.Split(string(data), "\n")
			for _, line := range lines {
				if strings.HasPrefix(line, "model name") {
					parts := strings.SplitN(line, ":", 2)
					if len(parts) == 2 {
						info.CPUModel = strings.TrimSpace(parts[1])
						break
					}
				}
			}
		}

		// Get RAM from /proc/meminfo
		if data, err := os.ReadFile("/proc/meminfo"); err == nil {
			lines := strings.Split(string(data), "\n")
			for _, line := range lines {
				if strings.HasPrefix(line, "MemTotal:") {
					var kb int64
					if _, err := fmt.Sscanf(line, "MemTotal: %d kB", &kb); err == nil {
						info.RAM = float64(kb) / (1024 * 1024) // Convert KB to GB
					}
					break
				}
			}
		}
	} else if runtime.GOOS == "darwin" {
		// macOS
		if output, err := exec.Command("sysctl", "-n", "machdep.cpu.brand_string").Output(); err == nil {
			info.CPUModel = strings.TrimSpace(string(output))
		}

		if output, err := exec.Command("sysctl", "-n", "hw.memsize").Output(); err == nil {
			var bytes int64
			if _, err := fmt.Sscanf(string(output), "%d", &bytes); err == nil {
				info.RAM = float64(bytes) / (1024 * 1024 * 1024) // Convert bytes to GB
			}
		}
	}

	return info
}

// getGitHash returns the current git commit hash (short)
func getGitHash() string {
	output, err := exec.Command("git", "rev-parse", "--short", "HEAD").Output()
	if err != nil {
		return "unknown"
	}
	return strings.TrimSpace(string(output))
}

// generateResultsHash creates a hash of the benchmark results
func generateResultsHash(data string) string {
	hash := sha256.Sum256([]byte(data))
	return hex.EncodeToString(hash[:])[:8] // First 8 characters
}

// generateFilename creates a timestamp-based filename
func generateFilename(timestamp time.Time, hash, extension string) string {
	// Format: 2026-02-11_215430_a3f9b2c1.json
	return fmt.Sprintf("%s_%s.%s",
		timestamp.Format("2006-01-02_150405"),
		hash,
		extension,
	)
}

// writeJSONReport writes the benchmark report as JSON (atomic write)
func writeJSONReport(filename string, report BenchmarkReport) error {
	// Create temp file in same directory for atomic write
	tmpFile := filename + ".tmp"

	// Marshal JSON with indentation
	data, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal JSON: %w", err)
	}

	// Write to temp file
	if err := os.WriteFile(tmpFile, data, 0o644); err != nil { //nolint:gosec
		return fmt.Errorf("failed to write temp file: %w", err)
	}

	// Atomic rename
	if err := os.Rename(tmpFile, filename); err != nil {
		if cleanupErr := os.Remove(tmpFile); cleanupErr != nil {
			fmt.Fprintf(os.Stderr, "Warning: cleanup failed: %v\n", cleanupErr)
		}
		return fmt.Errorf("failed to rename temp file: %w", err)
	}

	return nil
}

// writeHTMLReport writes the benchmark report as HTML
func writeHTMLReport(filename string, report BenchmarkReport, blockpackAnalysisPath string, timestamp time.Time) error {
	tmpFile := filename + ".tmp"

	f, err := os.Create(tmpFile) //nolint:gosec
	if err != nil {
		return fmt.Errorf("failed to create temp file: %w", err)
	}

	// Generate HTML using existing function
	titleCaser := cases.Title(language.English)
	fileSizes := parseFileSizes("")
	htmlContent := generateHTML(
		report.OTELDemo,
		report.Comparisons,
		report.Aggregations,
		report.WritePath,
		report.TraceMetrics,
		fileSizes,
		titleCaser,
		blockpackAnalysisPath,
		timestamp,
	)

	// If metadata is present, inject it after <body> tag
	if report.Metadata.Timestamp != "" {
		metadataHTML := fmt.Sprintf(`
<!-- Benchmark Metadata -->
<div style="background: #f5f5f5; padding: 20px; margin-bottom: 30px; border-radius: 8px; border-left: 4px solid #4CAF50;">
	<h2 style="margin-top: 0; color: #333;">📊 Benchmark Metadata</h2>
	<div style="display: grid; grid-template-columns: repeat(auto-fit, minmax(250px, 1fr)); gap: 15px;">
		<div>
			<strong>Timestamp:</strong><br>
			<code>%s</code>
		</div>
		<div>
			<strong>Git Hash:</strong><br>
			<code>%s</code>
		</div>
		<div>
			<strong>Results Hash:</strong><br>
			<code>%s</code>
		</div>
	</div>
	<div style="margin-top: 15px;">
		<strong>System Information:</strong><br>
		<ul style="margin: 5px 0; padding-left: 20px;">
			<li><strong>CPU:</strong> %s</li>
			<li><strong>RAM:</strong> %.1f GB</li>
			<li><strong>OS:</strong> %s</li>
			<li><strong>Go Version:</strong> %s</li>
		</ul>
	</div>
	<div style="margin-top: 15px;">
		<strong>📄 Data Export:</strong>
		<a href="%s" style="color: #1976D2; text-decoration: none;">
			Download JSON for programmatic comparison →
		</a>
	</div>
</div>

`,
			html.EscapeString(report.Metadata.Timestamp),
			html.EscapeString(report.Metadata.GitHash),
			html.EscapeString(report.Metadata.ResultsHash),
			html.EscapeString(report.Metadata.System.CPUModel),
			report.Metadata.System.RAM,
			html.EscapeString(report.Metadata.System.OS),
			html.EscapeString(report.Metadata.System.GoVersion),
			html.EscapeString(generateFilename(timestamp, report.Metadata.ResultsHash, "json")),
		)

		// Insert metadata after <body> tag
		bodyTag := "<body>"
		bodyIndex := strings.Index(htmlContent, bodyTag)
		if bodyIndex != -1 {
			htmlContent = htmlContent[:bodyIndex+len(bodyTag)] + metadataHTML + htmlContent[bodyIndex+len(bodyTag):]
		}
	}

	if _, err := f.WriteString(htmlContent); err != nil {
		if closeErr := f.Close(); closeErr != nil {
			fmt.Fprintf(os.Stderr, "Warning: cleanup failed: %v\n", closeErr)
		}
		if cleanupErr := os.Remove(tmpFile); cleanupErr != nil {
			fmt.Fprintf(os.Stderr, "Warning: cleanup failed: %v\n", cleanupErr)
		}
		return fmt.Errorf("failed to write HTML: %w", err)
	}

	if err := f.Close(); err != nil {
		if cleanupErr := os.Remove(tmpFile); cleanupErr != nil {
			fmt.Fprintf(os.Stderr, "Warning: cleanup failed: %v\n", cleanupErr)
		}
		return fmt.Errorf("failed to close temp file: %w", err)
	}

	// Atomic rename
	if err := os.Rename(tmpFile, filename); err != nil {
		if cleanupErr := os.Remove(tmpFile); cleanupErr != nil {
			fmt.Fprintf(os.Stderr, "Warning: cleanup failed: %v\n", cleanupErr)
		}
		return fmt.Errorf("failed to rename temp file: %w", err)
	}

	return nil
}

// updateSymlink updates a symlink to point to the target file
func updateSymlink(targetFile, symlinkPath string) error {
	// Get basename for relative symlink
	targetBase := filepath.Base(targetFile)

	// Remove existing symlink if it exists
	if err := os.Remove(symlinkPath); err != nil && !os.IsNotExist(err) {
		fmt.Fprintf(os.Stderr, "Warning: failed to remove old symlink: %v\n", err)
	}

	// Create new symlink (relative path)
	if err := os.Symlink(targetBase, symlinkPath); err != nil {
		return fmt.Errorf("failed to create symlink: %w", err)
	}

	return nil
}

// QueryDefinition holds query information
type QueryDefinition struct {
	Name        string
	TraceQL     string
	Description string
}

// getOTELDemoQueries returns otel-demo-data query definitions
func getOTELDemoQueries() map[string]QueryDefinition {
	return map[string]QueryDefinition{
		"frontend-proxy-spans": {
			TraceQL: `{ resource.service.name = "frontend-proxy" }`,
		},
		"grpc-calls": {
			TraceQL: `{ span.rpc.system = "grpc" }`,
		},
		"post-requests": {
			TraceQL: `{ span.http.method = "POST" }`,
		},
		"client-spans": {TraceQL: `{ kind = client }`},
		"server-spans": {TraceQL: `{ kind = server }`},
		"get-requests": {
			TraceQL: `{ span.http.method = "GET" }`,
		},
		"product-catalog-operations": {
			TraceQL: `{ name =~ ".*ProductCatalogService.*" }`,
		},
		"checkout-operations": {
			TraceQL: `{ name =~ ".*[Cc]heckout.*" }`,
		},
		"currency-service-spans": {
			TraceQL: `{ resource.service.name =~ ".*currency.*" }`,
		},
		"shipping-service-operations": {
			TraceQL: `{ resource.service.name =~ ".*shipping.*" }`,
		},
		"payment-operations": {
			TraceQL: `{ name =~ ".*[Pp]ayment.*" }`,
		},
		"redis-operations": {
			TraceQL: `{ resource.service.name =~ ".*(redis|valkey).*" }`,
		},
		"ad-service-operations": {
			TraceQL: `{ name =~ ".*[Aa]d.*" }`,
		},
		"http-errors": {
			TraceQL: `{ span.http.status_code >= 400 }`,
		},
		"slow-frontend-requests": {
			TraceQL: `{ resource.service.name = "frontend" && duration > 1s }`,
		},
		"slow-grpc-calls": {
			TraceQL: `{ span.rpc.system = "grpc" && duration > 500ms }`,
		},
		"long-running-operations": {
			TraceQL: `{ duration > 500ms }`,
		},
		"fast-operations": {
			TraceQL: `{ duration < 30ms }`,
		},
		"cart-service-errors": {
			TraceQL: `{ (resource.service.name = "cart" || resource.service.name = "cartservice") && span.http.status_code >= 500 }`,
		},
		"database-operations": {
			TraceQL: `{ span.db.system = "postgresql" }`,
		},
	}
}

// getComparisonQueries returns format comparison query definitions
func getComparisonQueries() map[string]QueryDefinition {
	return map[string]QueryDefinition{
		"full_scan": {
			TraceQL:     "{}",
			Description: "Full table scan - all spans",
		},
		"platinum_customers": {
			TraceQL:     `{ span.customer.tier = "platinum" }`,
			Description: "Platinum tier customers only",
		},
		"experiment_traces": {
			TraceQL:     `{ span.experiment.id = "exp-2024-q1" }`,
			Description: "Experimental feature traces",
		},
		"platinum_us_west": {
			TraceQL:     `{ span.customer.tier = "platinum" && span.deployment.region = "us-west" }`,
			Description: "Platinum customers in us-west",
		},
		"gold_customers": {
			TraceQL:     `{ span.customer.tier = "gold" }`,
			Description: "Gold tier customers",
		},
		"us_west_region": {
			TraceQL:     `{ span.deployment.region = "us-west" }`,
			Description: "US West region deployments",
		},
		"gold_us_west": {
			TraceQL:     `{ span.customer.tier = "gold" && span.deployment.region = "us-west" }`,
			Description: "Gold customers in us-west",
		},
		"filter_by_service_shop": {
			TraceQL:     `{ resource.service.name = "shop-backend" }`,
			Description: "Filter by shop-backend service",
		},
		"filter_by_service_auth": {
			TraceQL:     `{ resource.service.name = "auth-service" }`,
			Description: "Filter by auth-service",
		},
		"filter_authenticate_operation": {
			TraceQL:     `{ name = "authenticate" }`,
			Description: "Filter authenticate operations",
		},
		"filter_by_http_status_200": {
			TraceQL:     `{ span.http.status_code = 200 }`,
			Description: "Filter by HTTP 200 status",
		},
		"filter_by_http_status_403": {
			TraceQL:     `{ span.http.status_code = 403 }`,
			Description: "Filter auth failures - HTTP 403",
		},
		"filter_by_http_status_201": {
			TraceQL:     `{ span.http.status_code = 201 }`,
			Description: "Filter created resources - HTTP 201",
		},
		"filter_by_db_operations": {
			TraceQL:     `{ span.db.system = "postgresql" }`,
			Description: "Filter PostgreSQL database operations",
		},
		"filter_shop_namespace": {
			TraceQL:     `{ resource.service.namespace = "shop" }`,
			Description: "Filter by shop namespace",
		},
		"filter_auth_namespace": {
			TraceQL:     `{ resource.service.namespace = "auth" }`,
			Description: "Filter by auth namespace",
		},
		"filter_billing_service": {
			TraceQL:     `{ resource.service.name = "billing-service" }`,
			Description: "Filter by billing service",
		},
		"filter_by_http_method_post": {
			TraceQL:     `{ span.http.method = "POST" }`,
			Description: "Filter by HTTP POST method",
		},
		"filter_payment_operation": {
			TraceQL:     `{ name = "process_payment" }`,
			Description: "Filter payment operations",
		},
		"regex_article_operations": {
			TraceQL:     `{ name =~ ".*article.*" }`,
			Description: "Regex match for article operations",
		},
		"duration_gt_100ms": {
			TraceQL:     `{ duration > 100ms }`,
			Description: "Filter operations longer than 100ms",
		},
		"mixed_db_namespace_and_db": {
			TraceQL:     `{ resource.namespace = "db" && span.db.system = "postgresql" }`,
			Description: "Mixed filter: db namespace + PostgreSQL",
		},
	}
}

// formatTime formats time in human-readable form
func formatTime(ms float64) string {
	if ms < 1 {
		return fmt.Sprintf("%.2fµs", ms*1000)
	} else if ms < 1000 {
		return fmt.Sprintf("%.2fms", ms)
	}
	return fmt.Sprintf("%.2fs", ms/1000)
}

// formatMemory formats memory in human-readable form
func formatMemory(mb float64) string {
	if mb < 1 {
		return fmt.Sprintf("%.2f KB", mb*1024)
	} else if mb < 1024 {
		return fmt.Sprintf("%.2f MB", mb)
	}
	return fmt.Sprintf("%.2f GB", mb/1024)
}

// addCommas adds commas to a numeric string
func addCommas(s string) string {
	if len(s) <= 3 {
		return s
	}

	var result strings.Builder
	for i, c := range s {
		if i > 0 && (len(s)-i)%3 == 0 {
			result.WriteRune(',')
		}
		result.WriteRune(c)
	}
	return result.String()
}

// getWinnerClass returns the winner CSS class if the value wins, empty string otherwise
func getWinnerClass(wins bool, winnerClass string) string {
	if wins {
		return " " + winnerClass
	}
	return ""
}

// formatWithSpeedup formats a value with speedup indicator in grey if there's a comparison
func formatWithSpeedup(value string, speedup float64, isBetter bool) string {
	if speedup <= 1.0 {
		return value
	}
	return fmt.Sprintf(`%s <span style="color: #999; font-size: 0.85em;">(%.1fx)</span>`, value, speedup)
}

// formatCostWithTooltip formats cost with hover tooltip showing breakdown
func formatCostWithTooltip(total, s3Get, s3Xfer, lambdaCompute, lambdaRequests float64) string {
	if total == 0 {
		return "$0.00"
	}

	totalStr := formatCost(total)
	tooltip := fmt.Sprintf(
		"S3 GET: %s&#10;S3 Transfer: %s&#10;Lambda Compute: %s&#10;Lambda Requests: %s&#10;Total: %s",
		formatCost(s3Get),
		formatCost(s3Xfer),
		formatCost(lambdaCompute),
		formatCost(lambdaRequests),
		totalStr,
	)

	return fmt.Sprintf(`<span class="cost-tooltip" title="%s">%s</span>`, tooltip, totalStr)
}

// formatCost formats a cost value for display
func formatCost(cost float64) string {
	if cost < 0.01 {
		return fmt.Sprintf("$%.6f", cost)
	} else if cost < 1 {
		return fmt.Sprintf("$%.4f", cost)
	} else if cost < 100 {
		return fmt.Sprintf("$%.2f", cost)
	}
	return fmt.Sprintf("$%.0f", cost)
}

// safeSpeedup calculates speedup (parquet / blockpack) with division by zero protection
// Returns 1.0 if division by zero would occur
func safeSpeedup(parquetVal, blockpackVal float64) float64 {
	if blockpackVal == 0 {
		if parquetVal == 0 {
			return 1.0 // Both zero - no speedup
		}
		return 1.0 // Avoid Inf - treat as no speedup
	}
	return parquetVal / blockpackVal
}

// formatWinCell renders the winner cell for a comparison row.
// Shows winner name, combined wall+cpu speedup ratio, and the winning score.
func formatWinCell(blockpackWins bool, bothExist bool, cScore, pScore float64) string {
	if !bothExist {
		return winCellTie
	}
	if blockpackWins {
		var speedup float64
		if cScore > 0 {
			speedup = pScore / cScore
		}
		return fmt.Sprintf(
			`<td class="win-cell win-cell-blockpack">blockpack<br><small>(%.1fx faster)<br>score: %s</small></td>`,
			speedup, formatTime(cScore),
		)
	}
	var speedup float64
	if pScore > 0 {
		speedup = cScore / pScore
	}
	return fmt.Sprintf(
		`<td class="win-cell win-cell-parquet">parquet<br><small>(%.1fx faster)<br>score: %s</small></td>`,
		speedup, formatTime(pScore),
	)
}

// formatFileSizeBar renders a styled file size comparison bar for section headers.
// When sizes are unavailable (0), returns a neutral note instead of an empty string.
func formatFileSizeBar(blockpackMB, parquetMB float64) string {
	if blockpackMB == 0 && parquetMB == 0 {
		return `<div class="file-size-bar"><span class="file-size-ratio">File sizes not available for synthetic data</span></div>`
	}
	var ratioText string
	if blockpackMB > 0 && parquetMB > 0 {
		ratio := parquetMB / blockpackMB
		if ratio >= 1 {
			ratioText = fmt.Sprintf(`<span class="file-size-ratio">Parquet is <strong>%.1fx larger</strong></span>`, ratio)
		} else {
			ratioText = fmt.Sprintf(`<span class="file-size-ratio">Blockpack is <strong>%.1fx larger</strong></span>`, blockpackMB/parquetMB)
		}
	}
	return fmt.Sprintf(
		`<div class="file-size-bar">
	<span class="file-size-pill file-size-pill-blockpack">Blockpack %.1f MB</span>
	<span class="file-size-pill file-size-pill-parquet">Parquet %.1f MB</span>
	%s
</div>`,
		blockpackMB, parquetMB, ratioText,
	)
}

// fileExists checks if a file exists
func fileExists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}

// QueryComparison holds both blockpack and parquet results for a query
type QueryComparison struct {
	Blockpack *ComparisonResult
	Parquet   *ComparisonResult
	QueryName string
}

// OTELDemoComparison holds both blockpack and parquet results for a otel-demo-data query
type OTELDemoComparison struct {
	Blockpack *OTELDemoResult
	Parquet   *OTELDemoResult
	QueryName string
}

// AggregationComparison holds aggregation, manual, and parquet results for a query
type AggregationComparison struct {
	Aggregation *AggregationComparisonResult
	Parquet     *AggregationComparisonResult
	QueryName   string
}

// FileSize holds file size information per benchmark suite.
// Each suite writes to a dedicated path so sizes reflect the correct dataset.
type FileSize struct {
	// Format-comparison suite (BenchmarkFormatComparison): traces-format.*
	BlockpackMB float64
	ParquetMB   float64
	// Real-world suite (BenchmarkRealWorldQueries): traces-realworld.*
	RealWorldBlockpackMB float64
	RealWorldParquetMB   float64
	// Write-path suite (BenchmarkWritePathComparison): traces-writepath.*
	WritePathBlockpackMB float64
	WritePathParquetMB   float64
	// Hot/cold aggregation variants (legacy paths)
	BlockpackHotMB  float64
	BlockpackColdMB float64
}

// statFirst returns the first os.FileInfo that exists from the given candidate paths.
func statFirst(candidates ...string) (os.FileInfo, bool) {
	for _, p := range candidates {
		if info, err := os.Stat(p); err == nil {
			return info, true
		}
	}
	return nil, false
}

// readBlockpackMB returns the size in MB of the first matching blockpack file found,
// checking both the CWD and benchmark/ subdirectory.
func readBlockpackMB(stem string) float64 {
	if stat, ok := statFirst(stem+".blockpack", "benchmark/"+stem+".blockpack"); ok {
		return float64(stat.Size()) / (1024 * 1024)
	}
	return 0
}

// readParquetMB returns the total size in MB of the first matching parquet directory found,
// checking both the CWD and benchmark/ subdirectory.
func readParquetMB(stem string) float64 {
	for _, dir := range []string{stem + ".parquet", "benchmark/" + stem + ".parquet"} {
		var total int64
		_ = filepath.Walk(dir, func(_ string, info os.FileInfo, err error) error {
			if err == nil && !info.IsDir() {
				total += info.Size()
			}
			return nil
		})
		if total > 0 {
			return float64(total) / (1024 * 1024)
		}
	}
	return 0
}

// parseFileSizes reads actual file sizes from disk.
// Each benchmark suite writes to a dedicated path so sizes reflect the correct dataset.
// Checks both the repo root and the benchmark/ subdirectory since bench-report
// runs from the repo root but bench.test writes files into benchmark/.
func parseFileSizes(input string) FileSize {
	_ = input // reserved for future use
	return FileSize{
		BlockpackMB:          readBlockpackMB("traces-format"),
		ParquetMB:            readParquetMB("traces-format"),
		RealWorldBlockpackMB: readBlockpackMB("traces-realworld"),
		RealWorldParquetMB:   readParquetMB("traces-realworld"),
		WritePathBlockpackMB: readBlockpackMB("traces-writepath"),
		WritePathParquetMB:   readParquetMB("traces-writepath"),
		BlockpackHotMB:       readBlockpackMB("traces-hot"),
		BlockpackColdMB:      readBlockpackMB("traces-cold"),
		// Note: traces-structural and traces-metrics are written by their respective
		// benchmark suites but are not currently surfaced in the HTML report.
	}
}

// otelDemoRowMetrics holds pre-computed display strings for one OTELDemoComparison row.
type otelDemoRowMetrics struct {
	blockpackTime, parquetTime         string
	blockpackCPU, parquetCPU           string
	blockpackBytes, parquetBytes       string
	blockpackIOOps, parquetIOOps       string
	blockpackMemory, parquetMemory     string
	blockpackAllocs, parquetAllocs     string
	blockpackCostHTML, parquetCostHTML string
	winCell                            string
	cVal, pVal                         OTELDemoResult
	blockpackWins, bothExist           bool
}

// computeOTELDemoRowMetrics pre-computes all display strings for a single comparison row.
func computeOTELDemoRowMetrics(comp OTELDemoComparison) otelDemoRowMetrics {
	c := comp.Blockpack
	p := comp.Parquet

	var m otelDemoRowMetrics
	if c != nil {
		m.cVal = *c
	}
	if p != nil {
		m.pVal = *p
	}
	m.bothExist = c != nil && p != nil
	// Winner is determined by lowest wall+cpu score: faster wall time AND less CPU both matter.
	cScore := m.cVal.TimeMs + m.cVal.CPUMs
	pScore := m.pVal.TimeMs + m.pVal.CPUMs
	m.blockpackWins = m.bothExist && cScore < pScore
	tie := m.bothExist && math.Abs(cScore-pScore) < 1e-3 // <1µs difference: treat as tie
	if tie {
		m.winCell = winCellTie
	} else {
		m.winCell = formatWinCell(m.blockpackWins, m.bothExist, cScore, pScore)
	}

	timeSpeedup := safeSpeedup(m.pVal.TimeMs, m.cVal.TimeMs)
	cpuSpeedup := safeSpeedup(m.pVal.CPUMs, m.cVal.CPUMs)
	bytesSpeedup := safeSpeedup(float64(m.pVal.BytesRead), float64(m.cVal.BytesRead))
	ioOpsSpeedup := safeSpeedup(float64(m.pVal.IOOps), float64(m.cVal.IOOps))
	memorySpeedup := safeSpeedup(m.pVal.MemoryMB, m.cVal.MemoryMB)
	allocsSpeedup := safeSpeedup(float64(m.pVal.Allocs), float64(m.cVal.Allocs))
	costSpeedup := safeSpeedup(m.pVal.CostTotal, m.cVal.CostTotal)

	m.blockpackTime = formatWithSpeedup(formatTime(m.cVal.TimeMs), timeSpeedup, m.blockpackWins)
	m.parquetTime = formatWithSpeedup(formatTime(m.pVal.TimeMs), safeSpeedup(m.cVal.TimeMs, m.pVal.TimeMs), !m.blockpackWins)

	m.blockpackCPU = formatWithSpeedup(formatTime(m.cVal.CPUMs), cpuSpeedup, m.cVal.CPUMs < m.pVal.CPUMs)
	m.parquetCPU = formatWithSpeedup(formatTime(m.pVal.CPUMs), safeSpeedup(m.cVal.CPUMs, m.pVal.CPUMs), m.pVal.CPUMs < m.cVal.CPUMs)

	m.blockpackBytes = formatWithSpeedup(formatMemory(m.cVal.BytesReadMB), bytesSpeedup, m.cVal.BytesRead < m.pVal.BytesRead)
	m.parquetBytes = formatWithSpeedup(formatMemory(m.pVal.BytesReadMB), safeSpeedup(float64(m.cVal.BytesRead), float64(m.pVal.BytesRead)), m.pVal.BytesRead < m.cVal.BytesRead)

	m.blockpackIOOps = formatWithSpeedup(addCommas(fmt.Sprintf("%d", m.cVal.IOOps)), ioOpsSpeedup, m.cVal.IOOps < m.pVal.IOOps)
	m.parquetIOOps = formatWithSpeedup(addCommas(fmt.Sprintf("%d", m.pVal.IOOps)), safeSpeedup(float64(m.cVal.IOOps), float64(m.pVal.IOOps)), m.pVal.IOOps < m.cVal.IOOps)

	m.blockpackMemory = formatWithSpeedup(formatMemory(m.cVal.MemoryMB), memorySpeedup, m.cVal.MemoryMB < m.pVal.MemoryMB)
	m.parquetMemory = formatWithSpeedup(formatMemory(m.pVal.MemoryMB), safeSpeedup(m.cVal.MemoryMB, m.pVal.MemoryMB), m.pVal.MemoryMB < m.cVal.MemoryMB)

	m.blockpackAllocs = formatWithSpeedup(addCommas(fmt.Sprintf("%d", m.cVal.Allocs)), allocsSpeedup, m.cVal.Allocs < m.pVal.Allocs)
	m.parquetAllocs = formatWithSpeedup(addCommas(fmt.Sprintf("%d", m.pVal.Allocs)), safeSpeedup(float64(m.cVal.Allocs), float64(m.pVal.Allocs)), m.pVal.Allocs < m.cVal.Allocs)

	m.blockpackCostHTML = formatCostWithTooltip(m.cVal.CostTotal, m.cVal.CostS3Get, m.cVal.CostS3Xfer, m.cVal.CostLambdaCompute, m.cVal.CostLambdaRequests)
	m.parquetCostHTML = formatCostWithTooltip(m.pVal.CostTotal, m.pVal.CostS3Get, m.pVal.CostS3Xfer, m.pVal.CostLambdaCompute, m.pVal.CostLambdaRequests)

	if costSpeedup > 1.0 {
		m.blockpackCostHTML = formatWithSpeedup(m.blockpackCostHTML, costSpeedup, true)
	}
	if inverseCostSpeedup := safeSpeedup(m.cVal.CostTotal, m.pVal.CostTotal); inverseCostSpeedup > 1.0 {
		m.parquetCostHTML = formatWithSpeedup(m.parquetCostHTML, inverseCostSpeedup, true)
	}
	return m
}

// writeOTELDemoTableRow writes a single HTML table row for one OTELDemoComparison into sb.
// queryNameCell is the pre-rendered <td class="query-name">...</td> content (inner HTML only).
//
//nolint:gosec // G705: fmt.Fprintf writes to strings.Builder; format string is a static HTML template
func writeOTELDemoTableRow(sb *strings.Builder, queryNameCell string, m otelDemoRowMetrics) {
	cVal, pVal, bothExist := m.cVal, m.pVal, m.bothExist
	fmt.Fprintf(sb, `			<tr>
				<td class="query-name">%s</td>
				%s
				<td class="metric numeric%s">%s</td>
				<td class="metric numeric%s">%s</td>
				<td class="metric numeric%s">%s</td>
				<td class="metric numeric%s">%s</td>
				<td class="metric numeric%s">%s</td>
				<td class="metric numeric%s">%s</td>
				<td class="metric numeric%s">%s</td>
				<td class="metric numeric%s">%s</td>
				<td class="metric numeric%s">%s</td>
				<td class="metric numeric%s">%s</td>
				<td class="metric numeric%s">%s</td>
				<td class="metric numeric%s">%s</td>
				<td class="metric numeric%s">%s</td>
				<td class="metric numeric%s">%s</td>
				<td class="metric numeric%s">%s</td>
				<td class="metric numeric%s">%s</td>
				<td class="metric numeric%s">%s</td>
				<td class="metric numeric%s">%s</td>
			</tr>
`, queryNameCell, m.winCell,
		getWinnerClass(bothExist && cVal.TimeMs < pVal.TimeMs, "winner-blockpack"), m.blockpackTime,
		getWinnerClass(bothExist && cVal.CPUMs < pVal.CPUMs, "winner-blockpack"), m.blockpackCPU,
		getWinnerClass(bothExist && cVal.BytesRead < pVal.BytesRead, "winner-blockpack"), m.blockpackBytes,
		getWinnerClass(bothExist && cVal.IOOps < pVal.IOOps, "winner-blockpack"), m.blockpackIOOps,
		"", addCommas(fmt.Sprintf("%d", cVal.Traces)),
		"", addCommas(fmt.Sprintf("%d", cVal.Spans)),
		getWinnerClass(bothExist && cVal.MemoryMB < pVal.MemoryMB, "winner-blockpack"), m.blockpackMemory,
		getWinnerClass(bothExist && cVal.Allocs < pVal.Allocs, "winner-blockpack"), m.blockpackAllocs,
		getWinnerClass(bothExist && cVal.CostTotal < pVal.CostTotal, "winner-blockpack"), m.blockpackCostHTML,
		getWinnerClass(bothExist && pVal.TimeMs < cVal.TimeMs, "winner-parquet"), m.parquetTime,
		getWinnerClass(bothExist && pVal.CPUMs < cVal.CPUMs, "winner-parquet"), m.parquetCPU,
		getWinnerClass(bothExist && pVal.BytesRead < cVal.BytesRead, "winner-parquet"), m.parquetBytes,
		getWinnerClass(bothExist && pVal.IOOps < cVal.IOOps, "winner-parquet"), m.parquetIOOps,
		"", addCommas(fmt.Sprintf("%d", pVal.Traces)),
		"", addCommas(fmt.Sprintf("%d", pVal.Spans)),
		getWinnerClass(bothExist && pVal.MemoryMB < cVal.MemoryMB, "winner-parquet"), m.parquetMemory,
		getWinnerClass(bothExist && pVal.Allocs < cVal.Allocs, "winner-parquet"), m.parquetAllocs,
		getWinnerClass(bothExist && pVal.CostTotal < cVal.CostTotal, "winner-parquet"), m.parquetCostHTML)
}

// generateOTELDemoTableRows generates HTML table rows for OTELDemoComparison pairs
func generateOTELDemoTableRows(
	pairs []OTELDemoComparison,
	titleCaser cases.Caser,
	prefix string,
	includeAnchor bool,
) string {
	var sb strings.Builder

	for _, comp := range pairs {
		// Skip if both are nil (shouldn't happen, but be safe)
		if comp.Blockpack == nil && comp.Parquet == nil {
			continue
		}

		m := computeOTELDemoRowMetrics(comp)
		queryName := html.EscapeString(titleCaser.String(strings.ReplaceAll(comp.QueryName, "-", " ")))

		var queryNameCell string
		if includeAnchor {
			anchorID := slugifyID("query", prefix, comp.QueryName)
			queryNameCell = fmt.Sprintf(`<a href="#%s">%s</a>`, anchorID, queryName)
		} else {
			queryNameCell = queryName
		}
		writeOTELDemoTableRow(&sb, queryNameCell, m)
	}

	return sb.String()
}

func minFloat(values ...float64) float64 {
	if len(values) == 0 {
		return 0
	}
	min := values[0] //nolint:revive
	for _, v := range values[1:] {
		if v < min {
			min = v //nolint:revive
		}
	}
	return min
}

func speedupFromBest(best, val float64) float64 {
	if best <= 0 {
		return 1.0
	}
	return val / best
}

func generateAggregationComparisonRows(comparisons []AggregationComparison, titleCaser cases.Caser) string {
	var sb strings.Builder

	for _, comp := range comparisons {
		agg := comp.Aggregation
		parquet := comp.Parquet

		// Skip incomplete comparisons
		if agg == nil || parquet == nil {
			continue
		}

		queryName := html.EscapeString(titleCaser.String(strings.ReplaceAll(comp.QueryName, "-", " ")))

		bestTime := minFloat(agg.TimeMs, parquet.TimeMs)
		bestBytes := minFloat(float64(agg.BytesRead), float64(parquet.BytesRead))
		bestIOOps := minFloat(float64(agg.IOOps), float64(parquet.IOOps))
		bestMemory := minFloat(agg.MemoryMB, parquet.MemoryMB)
		bestAllocs := minFloat(float64(agg.Allocs), float64(parquet.Allocs))
		bestCost := minFloat(agg.CostTotal, parquet.CostTotal)

		aggTime := formatWithSpeedup(
			formatTime(agg.TimeMs),
			speedupFromBest(bestTime, agg.TimeMs),
			agg.TimeMs <= bestTime,
		)
		parquetTime := formatWithSpeedup(
			formatTime(parquet.TimeMs),
			speedupFromBest(bestTime, parquet.TimeMs),
			parquet.TimeMs <= bestTime,
		)

		aggBytes := formatWithSpeedup(
			formatMemory(agg.BytesReadMB),
			speedupFromBest(bestBytes, float64(agg.BytesRead)),
			agg.BytesRead <= int64(bestBytes),
		)
		parquetBytes := formatWithSpeedup(
			formatMemory(parquet.BytesReadMB),
			speedupFromBest(bestBytes, float64(parquet.BytesRead)),
			parquet.BytesRead <= int64(bestBytes),
		)

		aggIOOps := formatWithSpeedup(
			addCommas(fmt.Sprintf("%d", agg.IOOps)),
			speedupFromBest(bestIOOps, float64(agg.IOOps)),
			float64(agg.IOOps) <= bestIOOps,
		)
		parquetIOOps := formatWithSpeedup(
			addCommas(fmt.Sprintf("%d", parquet.IOOps)),
			speedupFromBest(bestIOOps, float64(parquet.IOOps)),
			float64(parquet.IOOps) <= bestIOOps,
		)

		aggMemory := formatWithSpeedup(
			formatMemory(agg.MemoryMB),
			speedupFromBest(bestMemory, agg.MemoryMB),
			agg.MemoryMB <= bestMemory,
		)
		parquetMemory := formatWithSpeedup(
			formatMemory(parquet.MemoryMB),
			speedupFromBest(bestMemory, parquet.MemoryMB),
			parquet.MemoryMB <= bestMemory,
		)

		aggAllocs := formatWithSpeedup(
			addCommas(fmt.Sprintf("%d", agg.Allocs)),
			speedupFromBest(bestAllocs, float64(agg.Allocs)),
			float64(agg.Allocs) <= bestAllocs,
		)
		parquetAllocs := formatWithSpeedup(
			addCommas(fmt.Sprintf("%d", parquet.Allocs)),
			speedupFromBest(bestAllocs, float64(parquet.Allocs)),
			float64(parquet.Allocs) <= bestAllocs,
		)

		aggCost := formatWithSpeedup(
			formatCostWithTooltip(
				agg.CostTotal,
				agg.CostS3Get,
				agg.CostS3Xfer,
				agg.CostLambdaCompute,
				agg.CostLambdaRequests,
			),
			speedupFromBest(bestCost, agg.CostTotal),
			agg.CostTotal <= bestCost,
		)
		parquetCost := formatWithSpeedup(
			formatCostWithTooltip(
				parquet.CostTotal,
				parquet.CostS3Get,
				parquet.CostS3Xfer,
				parquet.CostLambdaCompute,
				parquet.CostLambdaRequests,
			),
			speedupFromBest(bestCost, parquet.CostTotal),
			parquet.CostTotal <= bestCost,
		)

		//nolint:gosec // G705: fmt.Fprintf writes to strings.Builder; format string is a static HTML template
		fmt.Fprintf(&sb, `			<tr>
				<td class="query-name">%s</td>
				<td class="metric numeric%s">%s</td>
				<td class="metric numeric%s">%s</td>
				<td class="metric numeric%s">%s</td>
				<td class="metric numeric">%s</td>
				<td class="metric numeric">%s</td>
				<td class="metric numeric%s">%s</td>
				<td class="metric numeric%s">%s</td>
				<td class="metric numeric%s">%s</td>
				<td class="metric numeric parquet-start%s">%s</td>
				<td class="metric numeric%s">%s</td>
				<td class="metric numeric%s">%s</td>
				<td class="metric numeric">%s</td>
				<td class="metric numeric">%s</td>
				<td class="metric numeric%s">%s</td>
				<td class="metric numeric%s">%s</td>
				<td class="metric numeric%s">%s</td>
			</tr>
`, queryName,
			getWinnerClass(agg.TimeMs <= bestTime, "winner-aggregation"), aggTime,
			getWinnerClass(agg.BytesRead <= int64(bestBytes), "winner-aggregation"), aggBytes,
			getWinnerClass(float64(agg.IOOps) <= bestIOOps, "winner-aggregation"), aggIOOps,
			addCommas(fmt.Sprintf("%d", agg.Groups)),
			addCommas(fmt.Sprintf("%d", agg.TimeValues)),
			getWinnerClass(agg.MemoryMB <= bestMemory, "winner-aggregation"), aggMemory,
			getWinnerClass(float64(agg.Allocs) <= bestAllocs, "winner-aggregation"), aggAllocs,
			getWinnerClass(agg.CostTotal <= bestCost, "winner-aggregation"), aggCost,
			getWinnerClass(parquet.TimeMs <= bestTime, "winner-parquet"), parquetTime,
			getWinnerClass(parquet.BytesRead <= int64(bestBytes), "winner-parquet"), parquetBytes,
			getWinnerClass(float64(parquet.IOOps) <= bestIOOps, "winner-parquet"), parquetIOOps,
			addCommas(fmt.Sprintf("%d", parquet.Groups)),
			addCommas(fmt.Sprintf("%d", parquet.TimeValues)),
			getWinnerClass(parquet.MemoryMB <= bestMemory, "winner-parquet"), parquetMemory,
			getWinnerClass(float64(parquet.Allocs) <= bestAllocs, "winner-parquet"), parquetAllocs,
			getWinnerClass(parquet.CostTotal <= bestCost, "winner-parquet"), parquetCost)
	}

	return sb.String()
}

// preparedData holds grouped and sorted benchmark data
type preparedData struct {
	otelDemoPairs   []OTELDemoComparison
	limitedPairs    []OTELDemoComparison
	traceByIDPairs  []OTELDemoComparison
	comparisonPairs []QueryComparison
}

// prepareDataForHTML groups and sorts benchmark results for HTML generation
func prepareDataForHTML(otelDemo []OTELDemoResult, comparisons []ComparisonResult) preparedData {
	// Separate otel-demo-data results into regular, limited, and trace-by-id
	regularMap := make(map[string]*OTELDemoComparison)
	limitedMap := make(map[string]*OTELDemoComparison)
	traceByIDMap := make(map[string]*OTELDemoComparison)

	for i := range otelDemo {
		result := &otelDemo[i]

		// Check if this is a limited benchmark (format starts with "limited-")
		if strings.HasPrefix(result.Format, "limited-") {
			if _, exists := limitedMap[result.QueryName]; !exists {
				limitedMap[result.QueryName] = &OTELDemoComparison{
					QueryName: result.QueryName,
				}
			}
			if strings.HasSuffix(result.Format, "blockpack") {
				limitedMap[result.QueryName].Blockpack = result
			} else {
				limitedMap[result.QueryName].Parquet = result
			}
			continue
		}

		// Check if this is a trace-by-id benchmark (format starts with "trace-")
		if strings.HasPrefix(result.Format, "trace-") {
			if _, exists := traceByIDMap[result.QueryName]; !exists {
				traceByIDMap[result.QueryName] = &OTELDemoComparison{
					QueryName: result.QueryName,
				}
			}
			if strings.HasSuffix(result.Format, "blockpack") {
				traceByIDMap[result.QueryName].Blockpack = result
			} else {
				traceByIDMap[result.QueryName].Parquet = result
			}
			continue
		}

		// Regular otel-demo-data benchmarks
		if _, exists := regularMap[result.QueryName]; !exists {
			regularMap[result.QueryName] = &OTELDemoComparison{
				QueryName: result.QueryName,
			}
		}

		if result.Format == formatBlockpack {
			regularMap[result.QueryName].Blockpack = result
		} else {
			regularMap[result.QueryName].Parquet = result
		}
	}

	// Convert regular results to slice and sort
	var otelDemoPairs []OTELDemoComparison
	for _, comp := range regularMap {
		// Include results even if only blockpack or only parquet exists
		if comp.Blockpack != nil || comp.Parquet != nil {
			otelDemoPairs = append(otelDemoPairs, *comp)
		}
	}

	sort.Slice(otelDemoPairs, func(i, j int) bool {
		return otelDemoPairs[i].QueryName < otelDemoPairs[j].QueryName
	})

	// Convert trace-by-id results to slice
	var traceByIDPairs []OTELDemoComparison
	for _, comp := range traceByIDMap {
		// Include results even if only blockpack or only parquet exists
		if comp.Blockpack != nil || comp.Parquet != nil {
			traceByIDPairs = append(traceByIDPairs, *comp)
		}
	}

	// Convert limited results to slice and sort
	var limitedPairs []OTELDemoComparison
	for _, comp := range limitedMap {
		// Include results even if only blockpack or only parquet exists
		if comp.Blockpack != nil || comp.Parquet != nil {
			limitedPairs = append(limitedPairs, *comp)
		}
	}

	sort.Slice(limitedPairs, func(i, j int) bool {
		return limitedPairs[i].QueryName < limitedPairs[j].QueryName
	})

	// Group comparisons by query name
	queryMap := make(map[string]*QueryComparison)
	for i := range comparisons {
		result := &comparisons[i]
		if _, exists := queryMap[result.QueryName]; !exists {
			queryMap[result.QueryName] = &QueryComparison{
				QueryName: result.QueryName,
			}
		}

		if result.Format == formatBlockpack {
			queryMap[result.QueryName].Blockpack = result
		} else {
			queryMap[result.QueryName].Parquet = result
		}
	}

	// Convert to slice and sort
	var comparisonPairs []QueryComparison
	for _, comp := range queryMap {
		if comp.Blockpack != nil && comp.Parquet != nil {
			comparisonPairs = append(comparisonPairs, *comp)
		}
	}

	sort.Slice(comparisonPairs, func(i, j int) bool {
		return comparisonPairs[i].QueryName < comparisonPairs[j].QueryName
	})

	return preparedData{
		otelDemoPairs:   otelDemoPairs,
		limitedPairs:    limitedPairs,
		traceByIDPairs:  traceByIDPairs,
		comparisonPairs: comparisonPairs,
	}
}

func prepareAggregationComparisons(results []AggregationComparisonResult) []AggregationComparison {
	comparisonMap := make(map[string]*AggregationComparison)

	for i := range results {
		result := &results[i]
		if _, exists := comparisonMap[result.QueryName]; !exists {
			comparisonMap[result.QueryName] = &AggregationComparison{
				QueryName: result.QueryName,
			}
		}

		switch result.Format {
		case formatAggregation:
			comparisonMap[result.QueryName].Aggregation = result
		case formatParquet:
			comparisonMap[result.QueryName].Parquet = result
		}
	}

	var comparisons []AggregationComparison
	for _, comp := range comparisonMap {
		if comp.Aggregation != nil && comp.Parquet != nil {
			comparisons = append(comparisons, *comp)
		}
	}

	sort.Slice(comparisons, func(i, j int) bool {
		return comparisons[i].QueryName < comparisons[j].QueryName
	})

	return comparisons
}

// traceMetricsPair holds blockpack and parquet results for a single TraceQL metrics query.
type traceMetricsPair struct {
	Blockpack *TraceMetricsComparisonResult
	Parquet   *TraceMetricsComparisonResult
	QueryName string
}

// prepareTraceMetricsPairs groups TraceMetricsComparisonResults by query, preserving insertion order.
func prepareTraceMetricsPairs(results []TraceMetricsComparisonResult) []traceMetricsPair {
	type pair struct {
		bp *TraceMetricsComparisonResult
		pq *TraceMetricsComparisonResult
	}
	byQuery := make(map[string]*pair)
	order := make([]string, 0)
	for i := range results {
		r := &results[i]
		if _, exists := byQuery[r.QueryName]; !exists {
			byQuery[r.QueryName] = &pair{}
			order = append(order, r.QueryName)
		}
		switch r.Format {
		case formatBlockpack:
			byQuery[r.QueryName].bp = r
		case formatParquet:
			byQuery[r.QueryName].pq = r
		}
	}
	out := make([]traceMetricsPair, 0, len(order))
	for _, q := range order {
		p := byQuery[q]
		out = append(out, traceMetricsPair{QueryName: q, Blockpack: p.bp, Parquet: p.pq})
	}
	return out
}

// generateTraceMetricsSection generates the HTML section for TraceQL metrics comparison results.
// Returns an empty string if traceMetrics is empty.
//
//nolint:gosec // G705: fmt.Fprintf writes to strings.Builder; format string is a static HTML template
func generateTraceMetricsSection(traceMetrics []TraceMetricsComparisonResult) string {
	pairs := prepareTraceMetricsPairs(traceMetrics)
	if len(pairs) == 0 {
		return ""
	}

	var sb strings.Builder
	sb.WriteString(`
	<button type="button" class="section-header collapsible" id="trace-metrics-comparison" onclick="toggleSection(this)" aria-expanded="true" aria-controls="trace-metrics-comparison-content">
		<span class="chevron">▼</span> 📈 TraceQL Metrics Comparison: Blockpack vs Parquet
	</button>
	<div class="collapsible-content" id="trace-metrics-comparison-content" aria-hidden="false">
	<p>Performance of TraceQL metrics queries (count_over_time, rate, histogram) across formats.</p>
	<table>
		<thead>
			<tr>
				<th>Query</th>
				<th>Format</th>
				<th>Time (ms)</th>
				<th>I/O Ops</th>
				<th>Bytes Read</th>
				<th>Bytes/IO</th>
				<th>Groups</th>
				<th>Time Values</th>
				<th>Est. Cost ($)</th>
			</tr>
		</thead>
		<tbody>
`)
	for _, p := range pairs {
		for _, row := range []struct {
			r     *TraceMetricsComparisonResult
			label string
		}{
			{p.Blockpack, "blockpack"},
			{p.Parquet, "parquet"},
		} {
			if row.r == nil {
				fmt.Fprintf(&sb, `			<tr>
				<td class="query-name">%s</td>
				<td>%s</td>
				<td colspan="7" style="color: #999;">no data</td>
			</tr>
`, html.EscapeString(p.QueryName), html.EscapeString(row.label))
				continue
			}
			r := row.r
			costHTML := formatCostWithTooltip(r.CostTotal, r.CostS3Get, r.CostS3Xfer, r.CostLambdaCompute, r.CostLambdaRequests)
			fmt.Fprintf(&sb, `			<tr>
				<td class="query-name">%s</td>
				<td>%s</td>
				<td class="metric numeric">%s</td>
				<td class="metric numeric">%s</td>
				<td class="metric numeric">%s</td>
				<td class="metric numeric">%.0f</td>
				<td class="metric numeric">%s</td>
				<td class="metric numeric">%s</td>
				<td class="metric numeric">%s</td>
			</tr>
`,
				html.EscapeString(p.QueryName),
				html.EscapeString(r.Format),
				formatTime(r.TimeMs),
				addCommas(fmt.Sprintf("%d", r.IOOps)),
				formatMemory(r.BytesReadMB),
				r.BytesPerIO,
				addCommas(fmt.Sprintf("%d", r.Groups)),
				addCommas(fmt.Sprintf("%d", r.TimeValues)),
				costHTML,
			)
		}
	}
	sb.WriteString(`		</tbody>
	</table>
	</div>
`)
	return sb.String()
}

// generateWritePathSection generates the HTML section for write path comparison results.
// Returns an empty string if writePaths is empty.
func generateWritePathSection(writePaths []WritePathResult) string {
	if len(writePaths) == 0 {
		return `
	<button type="button" class="section-header collapsible" id="write-path" onclick="toggleSection(this)" aria-expanded="true" aria-controls="write-path-content">
		<span class="chevron">▼</span> 💾 Write Path Comparison (Blockpack vs Parquet)
	</button>
	<div class="collapsible-content" id="write-path-content" aria-hidden="false">
	<div style="text-align: center; color: #666; margin-bottom: 30px;">
		No write path benchmarks were run.
	</div>
	</div>
`
	}

	// Group by scale, then by format
	type scaleEntry struct {
		blockpack *WritePathResult
		parquet   *WritePathResult
	}

	scaleMap := make(map[string]*scaleEntry)

	for i := range writePaths {
		wp := &writePaths[i]

		if scaleMap[wp.Scale] == nil {
			scaleMap[wp.Scale] = &scaleEntry{}
		}

		switch wp.Format {
		case formatBlockpack:
			scaleMap[wp.Scale].blockpack = wp
		case formatParquet:
			scaleMap[wp.Scale].parquet = wp
		}
	}

	// Collect and sort scale keys for deterministic output
	scales := make([]string, 0, len(scaleMap))

	for s := range scaleMap {
		scales = append(scales, s)
	}

	sort.Strings(scales)

	var sb strings.Builder

	sb.WriteString(`
	<button type="button" class="section-header collapsible" id="write-path" onclick="toggleSection(this)" aria-expanded="true" aria-controls="write-path-content">
		<span class="chevron">▼</span> 💾 Write Path Comparison (Blockpack vs Parquet)
	</button>
	<div class="collapsible-content" id="write-path-content" aria-hidden="false">
	<div style="text-align: center; color: #666; margin-bottom: 30px;">
		Bulk write performance for a complete block
	</div>

	<table class="comparison-table">
		<thead>
			<tr>
				<th rowspan="2">Scale</th>
				<th colspan="6" class="blockpack-col" style="text-align: center;">Blockpack</th>
				<th colspan="6" class="parquet-col" style="text-align: center;">Parquet</th>
			</tr>
			<tr>
				<th class="numeric blockpack-col">Time</th>
				<th class="numeric blockpack-col">Output Size</th>
				<th class="numeric blockpack-col">Memory</th>
				<th class="numeric blockpack-col">Allocs</th>
				<th class="numeric blockpack-col">Spans Written</th>
				<th class="numeric blockpack-col">ns/span</th>
				<th class="numeric parquet-col">Time</th>
				<th class="numeric parquet-col">Output Size</th>
				<th class="numeric parquet-col">Memory</th>
				<th class="numeric parquet-col">Allocs</th>
				<th class="numeric parquet-col">Spans Written</th>
				<th class="numeric parquet-col">ns/span</th>
			</tr>
		</thead>
		<tbody>
`)

	for _, scale := range scales {
		entry, ok := scaleMap[scale]
		if !ok || entry == nil {
			continue
		}

		bp := entry.blockpack
		pq := entry.parquet

		if bp == nil || pq == nil {
			//nolint:gosec // G705: fmt.Fprintf writes to strings.Builder; format string is a static HTML template
			fmt.Fprintf(&sb,
				`		<tr>
			<td class="query-name">%s</td>
			<td class="metric numeric" colspan="6" style="text-align:center; color:#999;">—</td>
			<td class="metric numeric" colspan="6" style="text-align:center; color:#999;">—</td>
		</tr>
`, html.EscapeString(scale))
			continue
		}

		bpTimeWins := bp.TimeMs < pq.TimeMs
		bpSizeWins := bp.BytesMB < pq.BytesMB
		bpMemWins := bp.MemoryMB < pq.MemoryMB
		bpAllocsWins := bp.Allocs < pq.Allocs
		bpNsWins := bp.NsPerSpan < pq.NsPerSpan

		//nolint:gosec // G705: fmt.Fprintf writes to strings.Builder; format string is a static HTML template
		fmt.Fprintf(&sb,
			`		<tr>
			<td class="query-name">%s</td>
			<td class="metric numeric%s">%s</td>
			<td class="metric numeric%s">%s</td>
			<td class="metric numeric%s">%s</td>
			<td class="metric numeric%s">%s</td>
			<td class="metric numeric">%s</td>
			<td class="metric numeric%s">%s</td>
			<td class="metric numeric%s">%s</td>
			<td class="metric numeric%s">%s</td>
			<td class="metric numeric%s">%s</td>
			<td class="metric numeric%s">%s</td>
			<td class="metric numeric">%s</td>
			<td class="metric numeric%s">%s</td>
		</tr>
`,
			html.EscapeString(scale),
			getWinnerClass(bpTimeWins, "winner-blockpack"),
			formatWithSpeedup(formatTime(bp.TimeMs), safeSpeedup(pq.TimeMs, bp.TimeMs), bpTimeWins),
			getWinnerClass(bpSizeWins, "winner-blockpack"),
			formatWithSpeedup(formatMemory(bp.BytesMB), safeSpeedup(pq.BytesMB, bp.BytesMB), bpSizeWins),
			getWinnerClass(bpMemWins, "winner-blockpack"),
			formatWithSpeedup(formatMemory(bp.MemoryMB), safeSpeedup(pq.MemoryMB, bp.MemoryMB), bpMemWins),
			getWinnerClass(bpAllocsWins, "winner-blockpack"),
			formatWithSpeedup(
				addCommas(fmt.Sprintf("%d", bp.Allocs)),
				safeSpeedup(float64(pq.Allocs), float64(bp.Allocs)),
				bpAllocsWins,
			),
			addCommas(fmt.Sprintf("%d", bp.SpansWritten)),
			getWinnerClass(bpNsWins, "winner-blockpack"),
			formatWithSpeedup(fmt.Sprintf("%.1f", bp.NsPerSpan), safeSpeedup(pq.NsPerSpan, bp.NsPerSpan), bpNsWins),
			getWinnerClass(!bpTimeWins, "winner-parquet"),
			formatWithSpeedup(formatTime(pq.TimeMs), safeSpeedup(bp.TimeMs, pq.TimeMs), !bpTimeWins),
			getWinnerClass(!bpSizeWins, "winner-parquet"),
			formatWithSpeedup(formatMemory(pq.BytesMB), safeSpeedup(bp.BytesMB, pq.BytesMB), !bpSizeWins),
			getWinnerClass(!bpMemWins, "winner-parquet"),
			formatWithSpeedup(formatMemory(pq.MemoryMB), safeSpeedup(bp.MemoryMB, pq.MemoryMB), !bpMemWins),
			getWinnerClass(!bpAllocsWins, "winner-parquet"),
			formatWithSpeedup(
				addCommas(fmt.Sprintf("%d", pq.Allocs)),
				safeSpeedup(float64(bp.Allocs), float64(pq.Allocs)),
				!bpAllocsWins,
			),
			addCommas(fmt.Sprintf("%d", pq.SpansWritten)),
			getWinnerClass(!bpNsWins, "winner-parquet"),
			formatWithSpeedup(fmt.Sprintf("%.1f", pq.NsPerSpan), safeSpeedup(bp.NsPerSpan, pq.NsPerSpan), !bpNsWins),
		)
	}

	sb.WriteString(`		</tbody>
	</table>
	</div>
`)

	return sb.String()
}

// generateHTML generates unified HTML report
func generateHTML(
	otelDemo []OTELDemoResult,
	comparisons []ComparisonResult,
	aggregations []AggregationComparisonResult,
	writePaths []WritePathResult,
	traceMetrics []TraceMetricsComparisonResult,
	fileSizes FileSize,
	titleCaser cases.Caser,
	blockpackAnalysis string,
	timestamp time.Time,
) string {
	var sb strings.Builder

	// Prepare data
	data := prepareDataForHTML(otelDemo, comparisons)
	otelDemoPairs := data.otelDemoPairs
	limitedPairs := data.limitedPairs
	traceByIDPairs := data.traceByIDPairs
	comparisonPairs := data.comparisonPairs
	aggregationComparisons := prepareAggregationComparisons(aggregations)

	// Generate HTML header
	//nolint:gosec // G705: fmt.Fprintf writes to strings.Builder; format string is a static HTML template
	fmt.Fprintf(&sb, `<!DOCTYPE html>
<html>
<head>
	<meta charset="UTF-8">
	<title>Benchmark Report - OTEL Generator Demo Data & Format Comparison</title>
	<style>
		body {
			font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif;
			max-width: 1800px;
			margin: 40px auto;
			padding: 0 20px;
			background: #f5f5f5;
		}
		h1 {
			color: #333;
			text-align: center;
			margin-bottom: 20px;
		}
		.metadata {
			text-align: center;
			color: #666;
			margin-bottom: 30px;
			font-size: 14px;
		}
		.summary {
			background: white;
			padding: 20px;
			margin-bottom: 30px;
			border-radius: 4px;
			box-shadow: 0 2px 4px rgba(0,0,0,0.1);
		}
		.toc {
			background: white;
			padding: 20px;
			margin-bottom: 30px;
			border-radius: 4px;
			box-shadow: 0 2px 4px rgba(0,0,0,0.1);
		}
		.toc h2 {
			margin-top: 0;
			color: #333;
			font-size: 18px;
		}
		.toc ul {
			list-style: none;
			padding: 0;
			margin: 0;
		}
		.toc li {
			margin: 10px 0;
			padding-left: 20px;
		}
		.toc a {
			color: #2c3e50;
			text-decoration: none;
			font-weight: 500;
		}
		.toc a:hover {
			color: #3498db;
			text-decoration: underline;
		}
		.summary h2 {
			margin-top: 0;
			color: #333;
			font-size: 18px;
		}
		.summary-stat {
			margin: 12px 0;
			font-size: 15px;
		}
		.summary-stat strong {
			color: #2c3e50;
		}
		.section-header {
			background: #2c3e50;
			color: white;
			padding: 20px;
			margin: 50px -20px 20px -20px; /* Negative margins to break out of body padding */
			border-radius: 4px;
			text-align: center;
			font-size: 1.5rem;
			font-weight: 600;
			width: calc(100%% + 40px); /* Full width including body padding */
		}
		button.section-header {
			border: none;
			font-family: inherit;
			display: block;
		}
		.section-header.collapsible {
			cursor: pointer;
			user-select: none;
		}
		.section-header.collapsible:hover {
			background: #34495e;
		}
		.chevron {
			font-size: 0.8em;
			display: inline-block;
			transition: transform 0.2s;
		}
		.collapsible-content {
			display: block; /* Sections start expanded */
		}
		table {
			width: 100%%;
			border-collapse: collapse;
			background: white;
			box-shadow: 0 2px 4px rgba(0,0,0,0.1);
			margin-bottom: 30px;
		}
		th {
			background: #2c3e50;
			color: white;
			padding: 12px;
			text-align: left;
			font-weight: 600;
			font-size: 14px;
		}
		th.numeric {
			text-align: right;
		}
		th.blockpack-col {
			background: #27ae60;
		}
		th.aggregation-col {
			background: #27ae60;
		}
		th.parquet-col {
			background: #3498db;
			border-left: 2px solid #ddd;
		}
		/* Vertical line between Blockpack and Parquet sections */
		table.comparison-table tbody tr td:nth-child(10) {
			border-left: 2px solid #ddd;
		}
		table.aggregation-comparison th.parquet-start,
		table.aggregation-comparison td.parquet-start {
			border-left: 2px solid #ddd;
		}
		td {
			padding: 12px;
			border-bottom: 1px solid #eee;
		}
		td.numeric {
			text-align: right;
			font-family: 'Monaco', 'Courier New', monospace;
			font-size: 13px;
		}
		tr:hover {
			background: #f9f9f9;
		}
		.query-name {
			font-weight: 600;
			font-size: 15px;
		}
		.query-name a {
			color: #2c3e50;
			text-decoration: none;
		}
		.query-name a:hover {
			color: #3498db;
			text-decoration: underline;
		}
		.metric {
			font-family: 'Monaco', 'Courier New', monospace;
			font-size: 13px;
		}
		.category-title {
			font-size: 1.3rem;
			margin-top: 40px;
			margin-bottom: 16px;
			color: #333;
			padding: 10px;
			background: white;
			border-left: 4px solid #3498db;
			box-shadow: 0 2px 4px rgba(0,0,0,0.1);
		}
		.excellent {
			color: #27ae60;
			font-weight: 600;
		}
		.good {
			color: #2ecc71;
		}
		.okay {
			color: #f39c12;
		}
		.slow {
			color: #e74c3c;
		}
		.winner-blockpack {
			border-bottom: 2px solid #27ae60;
			font-weight: 600;
		}
		.winner-aggregation {
			border-bottom: 2px solid #27ae60;
			font-weight: 600;
		}
		.winner-parquet {
			border-bottom: 2px solid #3498db;
			font-weight: 600;
		}
		.win-cell {
			text-align: center;
			font-weight: 700;
			font-size: 0.85em;
			white-space: nowrap;
			vertical-align: middle;
		}
		.win-cell-blockpack {
			background-color: #27ae60;
			color: white;
		}
		.win-cell-parquet {
			background-color: #3498db;
			color: white;
		}
		.file-size-bar {
			display: flex;
			align-items: center;
			justify-content: center;
			gap: 12px;
			margin: 8px 0 20px 0;
			font-size: 14px;
		}
		.file-size-pill {
			display: inline-block;
			padding: 4px 14px;
			border-radius: 20px;
			font-weight: 600;
			color: white;
			font-size: 13px;
		}
		.file-size-pill-blockpack { background-color: #27ae60; }
		.file-size-pill-parquet  { background-color: #3498db; }
		.file-size-ratio {
			color: #555;
			font-size: 13px;
		}
		.speedup-blockpack {
			color: #27ae60;
			font-weight: 600;
		}
		.speedup-parquet {
			color: #3498db;
			font-weight: 600;
		}
		.legend {
			text-align: center;
			margin-top: 30px;
			color: #666;
			font-size: 14px;
			margin-bottom: 40px;
		}
		.query-definitions {
			margin-top: 50px;
			margin-bottom: 50px;
		}
		.query-def {
			background: white;
			padding: 20px;
			margin-bottom: 20px;
			border-radius: 4px;
			box-shadow: 0 2px 4px rgba(0,0,0,0.1);
		}
		.query-def h3 {
			margin-top: 0;
			color: #2c3e50;
			font-size: 16px;
			border-bottom: 2px solid #3498db;
			padding-bottom: 10px;
		}
		.query-def pre {
			background: #f8f9fa;
			padding: 12px;
			border-radius: 3px;
			overflow-x: auto;
			font-size: 13px;
			line-height: 1.5;
			margin: 10px 0;
		}
		.query-def .description {
			color: #666;
			font-style: italic;
			margin-bottom: 10px;
		}
		.cost-tooltip {
			cursor: help;
			text-decoration: underline dotted;
			text-decoration-color: #999;
		}
		.cost-tooltip:hover {
			font-weight: 600;
		}
	</style>
	<script>
		function toggleSection(header) {
			const content = header.nextElementSibling;
			const chevron = header.querySelector('.chevron');

			const isHidden = content.style.display === 'none';
			if (isHidden) {
				content.style.display = 'block';
				content.setAttribute('aria-hidden', 'false');
				chevron.textContent = '▼';
				header.setAttribute('aria-expanded', 'true');
			} else {
				content.style.display = 'none';
				content.setAttribute('aria-hidden', 'true');
				chevron.textContent = '▶';
				header.setAttribute('aria-expanded', 'false');
			}
		}

		function initCollapsibleSections() {
			// Find elements that invoke toggleSection via an inline onclick handler
			const headers = document.querySelectorAll('[onclick^="toggleSection"]');
			headers.forEach((header) => {
				// Make header behave like a button for assistive technologies
				header.setAttribute('role', 'button');
				if (!header.hasAttribute('tabindex')) {
					header.setAttribute('tabindex', '0');
				}

				const content = header.nextElementSibling;
				const isHidden = content && content.style.display === 'none';
				header.setAttribute('aria-expanded', isHidden ? 'false' : 'true');
				if (content) {
					content.setAttribute('aria-hidden', isHidden ? 'true' : 'false');
				}

				header.addEventListener('keydown', function (event) {
					const key = event.key || event.code;
					if (key === 'Enter' || key === ' ' || key === 'Spacebar') {
						event.preventDefault();
						toggleSection(header);
					}
				});
			});
		}

		if (document.readyState === 'loading') {
			document.addEventListener('DOMContentLoaded', initCollapsibleSections);
		} else {
			initCollapsibleSections();
		}
	</script>
</head>
<body>
	<h1>Comprehensive Benchmark Report</h1>
	<div class="metadata">
		OTEL Generator Demo Data Queries (Blockpack) + Format Comparison (Blockpack vs Parquet)<br>
		Generated %s
	</div>

	<div class="io-info" style="background: #fff3cd; border-left: 4px solid #ffc107; padding: 15px; margin-bottom: 20px; border-radius: 4px;">
		<h3 style="margin-top: 0; color: #856404;">⚠️ S3 Latency Simulation</h3>
		<p style="margin: 0; color: #856404;">
			<strong>All benchmarks include artificial 20ms latency per I/O operation</strong> to simulate otel-demo-data AWS S3 performance characteristics.
			This reflects typical first-byte latency (10-20ms) for S3 requests within the same region. Total execution time = processing time + (I/O ops × 20ms).
		</p>
	</div>

	<div class="summary">
		<h2>📊 Overview</h2>
		<div class="summary-stat"><strong>OTEL Generator Demo Data Queries:</strong> %d (actual trace data, blockpack vs parquet)</div>
		<div class="summary-stat"><strong>Limited Queries:</strong> %d (blockpack vs parquet, max 20 traces × 3 spans)</div>
		<div class="summary-stat"><strong>Trace-by-ID:</strong> %d (single trace retrieval, blockpack vs parquet)</div>
		<div class="summary-stat"><strong>Aggregation Comparisons:</strong> %d (aggregation vs manual vs parquet)</div>
		<div class="summary-stat"><strong>Format Comparisons:</strong> %d (synthetic data, blockpack vs parquet)</div>
	</div>
`, timestamp.Format("January 02, 2006 at 03:04 PM"), len(otelDemoPairs), len(limitedPairs), len(traceByIDPairs), len(aggregationComparisons), len(comparisonPairs))

	// Add blockpack analysis link if available
	if blockpackAnalysis != "" && fileExists(blockpackAnalysis) {
		//nolint:gosec // G705: fmt.Fprintf writes to strings.Builder; format string is a static HTML template
		fmt.Fprintf(&sb, `
	<div class="io-info">
		<h3>📦 Blockpack Structure Analysis</h3>
		<p>View detailed analysis of the blockpack file structure including service distribution, block layout, and column statistics:</p>
		<p><a href="%s" target="_blank">View Blockpack Analysis Report →</a></p>
	</div>
`, filepath.Base(blockpackAnalysis))
	}

	// Table of Contents
	sb.WriteString(`
	<div class="toc">
		<h2>📑 Table of Contents</h2>
		<ul>
			<li><a href="#otel-demo-data">🌍 OTEL Generator Demo Data Queries (Blockpack vs Parquet)</a></li>
			<li><a href="#limited">🎯 Limited Queries (Blockpack vs Parquet)</a></li>
			<li><a href="#trace-by-id">🔍 Get Trace by ID (Blockpack vs Parquet)</a></li>
			<li><a href="#aggregation-comparison">📊 Aggregation vs Parquet</a></li>
			<li><a href="#format-comparison">⚖️ Format Comparison (Blockpack vs Parquet)</a></li>
			<li><a href="#write-path">💾 Write Path (Blockpack vs Parquet)</a></li>
			<li><a href="#query-definitions">📝 Query Definitions</a></li>
		</ul>
	</div>
	`)

	// Section 1: OTEL Generator Demo Data Queries
	//nolint:gosec // G705: fmt.Fprintf writes to strings.Builder; format string is a static HTML template
	fmt.Fprintf(&sb, `
	<button type="button" class="section-header collapsible" id="otel-demo-data" onclick="toggleSection(this)" aria-expanded="true" aria-controls="otel-demo-data-content">
		<span class="chevron">▼</span> 🌍 OTEL Generator Demo Data Queries (Blockpack vs Parquet)
	</button>
	<div class="collapsible-content" id="otel-demo-data-content" aria-hidden="false">
	<div style="text-align: center; color: #666; margin-bottom: 30px;">
		Actual trace data from e-commerce demo • 669,886 spans
	</div>
	%s

	<table class="comparison-table">
		<thead>
			<tr>
				<th rowspan="2">Query</th>
				<th rowspan="2" style="text-align:center; min-width:90px">Win</th>
				<th colspan="9" class="blockpack-col" style="text-align: center;">Blockpack</th>
				<th colspan="9" class="parquet-col" style="text-align: center;">Parquet</th>
			</tr>
			<tr>
				<th class="numeric blockpack-col">Wall</th>
				<th class="numeric blockpack-col">CPU</th>
				<th class="numeric blockpack-col">Bytes Read</th>
				<th class="numeric blockpack-col">I/O Ops</th>
				<th class="numeric blockpack-col">Traces</th>
				<th class="numeric blockpack-col">Spans</th>
				<th class="numeric blockpack-col">Memory</th>
				<th class="numeric blockpack-col">Allocations</th>
				<th class="numeric blockpack-col">Cost</th>
				<th class="numeric parquet-col">Wall</th>
				<th class="numeric parquet-col">CPU</th>
				<th class="numeric parquet-col">Bytes Read</th>
				<th class="numeric parquet-col">I/O Ops</th>
				<th class="numeric parquet-col">Traces</th>
				<th class="numeric parquet-col">Spans</th>
				<th class="numeric parquet-col">Memory</th>
				<th class="numeric parquet-col">Allocations</th>
				<th class="numeric parquet-col">Cost</th>
			</tr>
		</thead>
		<tbody>
`, formatFileSizeBar(fileSizes.RealWorldBlockpackMB, fileSizes.RealWorldParquetMB))

	sb.WriteString(generateOTELDemoTableRows(otelDemoPairs, titleCaser, "rw", true))

	sb.WriteString(`		</tbody>
	</table>
`)
	sb.WriteString(`
	</div>
`)

	// Section 2: Limited Queries (if any)
	if len(limitedPairs) > 0 {
		//nolint:gosec // G705: fmt.Fprintf writes to strings.Builder; format string is a static HTML template
		fmt.Fprintf(&sb, `
	<button type="button" class="section-header collapsible" id="limited" onclick="toggleSection(this)" aria-expanded="true" aria-controls="limited-content">
		<span class="chevron">▼</span> 🎯 Limited Queries (Blockpack vs Parquet)
	</button>
	<div class="collapsible-content" id="limited-content" aria-hidden="false">
	<div style="text-align: center; color: #666; margin-bottom: 30px;">
		Same queries as above but limited to first 20 traces × 3 spans per trace<br>
		Tests early-exit optimization and pagination performance
	</div>
	%s

	<table class="comparison-table">
		<thead>
			<tr>
				<th rowspan="2">Query</th>
				<th rowspan="2" style="text-align:center; min-width:90px">Win</th>
				<th colspan="9" class="blockpack-col" style="text-align: center;">Blockpack</th>
				<th colspan="9" class="parquet-col" style="text-align: center;">Parquet</th>
			</tr>
			<tr>
				<th class="numeric blockpack-col">Wall</th>
				<th class="numeric blockpack-col">CPU</th>
				<th class="numeric blockpack-col">Bytes Read</th>
				<th class="numeric blockpack-col">I/O Ops</th>
				<th class="numeric blockpack-col">Traces</th>
				<th class="numeric blockpack-col">Spans</th>
				<th class="numeric blockpack-col">Memory</th>
				<th class="numeric blockpack-col">Allocations</th>
				<th class="numeric blockpack-col">Cost</th>
				<th class="numeric parquet-col">Wall</th>
				<th class="numeric parquet-col">CPU</th>
				<th class="numeric parquet-col">Bytes Read</th>
				<th class="numeric parquet-col">I/O Ops</th>
				<th class="numeric parquet-col">Traces</th>
				<th class="numeric parquet-col">Spans</th>
				<th class="numeric parquet-col">Memory</th>
				<th class="numeric parquet-col">Allocations</th>
				<th class="numeric parquet-col">Cost</th>
			</tr>
		</thead>
		<tbody>
`, formatFileSizeBar(fileSizes.RealWorldBlockpackMB, fileSizes.RealWorldParquetMB))

		sb.WriteString(generateOTELDemoTableRows(limitedPairs, titleCaser, "limited", false))
		sb.WriteString(`		</tbody>
	</table>
	</div>
`)
	}

	// Section 3: Trace-by-ID (if any)
	if len(traceByIDPairs) > 0 {
		for _, comp := range traceByIDPairs {
			c := comp.Blockpack
			p := comp.Parquet

			// Skip incomplete comparisons
			if c == nil || p == nil {
				continue
			}

			// Determine winner based on wall+cpu score.
			cScore := c.TimeMs + c.CPUMs
			pScore := p.TimeMs + p.CPUMs
			blockpackWins := cScore < pScore
			winCell := formatWinCell(blockpackWins, true, cScore, pScore)

			//nolint:gosec // G705: fmt.Fprintf writes to strings.Builder; format string is a static HTML template
			fmt.Fprintf(&sb,
				`
	<button type="button" class="section-header collapsible" id="trace-by-id" onclick="toggleSection(this)" aria-expanded="true" aria-controls="trace-by-id-content">
		<span class="chevron">▼</span> 🔍 Get Trace by ID (Blockpack vs Parquet)
	</button>
	<div class="collapsible-content" id="trace-by-id-content" aria-hidden="false">
	<div style="text-align: center; color: #666; margin-bottom: 30px;">
		Retrieve a single trace by trace ID • Tests point lookup performance
	</div>
	%s

	<table class="comparison-table">
		<thead>
			<tr>
				<th rowspan="2">Query</th>
				<th rowspan="2" style="text-align:center; min-width:90px">Win</th>
				<th colspan="9" class="blockpack-col" style="text-align: center;">Blockpack</th>
				<th colspan="9" class="parquet-col" style="text-align: center;">Parquet</th>
			</tr>
			<tr>
				<th class="numeric blockpack-col">Wall</th>
				<th class="numeric blockpack-col">CPU</th>
				<th class="numeric blockpack-col">Bytes Read</th>
				<th class="numeric blockpack-col">I/O Ops</th>
				<th class="numeric blockpack-col">Traces</th>
				<th class="numeric blockpack-col">Spans</th>
				<th class="numeric blockpack-col">Memory</th>
				<th class="numeric blockpack-col">Allocations</th>
				<th class="numeric blockpack-col">Cost</th>
				<th class="numeric parquet-col">Wall</th>
				<th class="numeric parquet-col">CPU</th>
				<th class="numeric parquet-col">Bytes Read</th>
				<th class="numeric parquet-col">I/O Ops</th>
				<th class="numeric parquet-col">Traces</th>
				<th class="numeric parquet-col">Spans</th>
				<th class="numeric parquet-col">Memory</th>
				<th class="numeric parquet-col">Allocations</th>
				<th class="numeric parquet-col">Cost</th>
			</tr>
		</thead>
		<tbody>
			<tr>
				<td class="query-name">Get Trace by ID</td>
				%s
				<td class="metric numeric%s">%s</td>
				<td class="metric numeric%s">%s</td>
				<td class="metric numeric%s">%s</td>
				<td class="metric numeric%s">%s</td>
				<td class="metric numeric%s">%s</td>
				<td class="metric numeric%s">%s</td>
				<td class="metric numeric%s">%s</td>
				<td class="metric numeric%s">%s</td>
				<td class="metric numeric%s">%s</td>
				<td class="metric numeric%s">%s</td>
				<td class="metric numeric%s">%s</td>
				<td class="metric numeric%s">%s</td>
				<td class="metric numeric%s">%s</td>
				<td class="metric numeric%s">%s</td>
				<td class="metric numeric%s">%s</td>
				<td class="metric numeric%s">%s</td>
				<td class="metric numeric%s">%s</td>
				<td class="metric numeric%s">%s</td>
			</tr>
		</tbody>
	</table>
`,
				formatFileSizeBar(fileSizes.RealWorldBlockpackMB, fileSizes.RealWorldParquetMB),
				winCell,
				getWinnerClass(
					blockpackWins,
					"winner-blockpack",
				),
				formatWithSpeedup(formatTime(c.TimeMs), safeSpeedup(p.TimeMs, c.TimeMs), blockpackWins),
				getWinnerClass(
					c.CPUMs < p.CPUMs,
					"winner-blockpack",
				),
				formatWithSpeedup(formatTime(c.CPUMs), safeSpeedup(p.CPUMs, c.CPUMs), c.CPUMs < p.CPUMs),
				getWinnerClass(
					c.BytesRead < p.BytesRead,
					"winner-blockpack",
				),
				formatWithSpeedup(
					formatMemory(c.BytesReadMB),
					safeSpeedup(float64(p.BytesRead), float64(c.BytesRead)),
					c.BytesRead < p.BytesRead,
				),
				getWinnerClass(
					c.IOOps < p.IOOps,
					"winner-blockpack",
				),
				formatWithSpeedup(
					addCommas(fmt.Sprintf("%d", c.IOOps)),
					safeSpeedup(float64(p.IOOps), float64(c.IOOps)),
					c.IOOps < p.IOOps,
				),
				"",
				addCommas(fmt.Sprintf("%d", c.Traces)),
				"",
				addCommas(fmt.Sprintf("%d", c.Spans)),
				getWinnerClass(
					c.MemoryMB < p.MemoryMB,
					"winner-blockpack",
				),
				formatWithSpeedup(
					formatMemory(c.MemoryMB),
					safeSpeedup(p.MemoryMB, c.MemoryMB),
					c.MemoryMB < p.MemoryMB,
				),
				getWinnerClass(
					c.Allocs < p.Allocs,
					"winner-blockpack",
				),
				formatWithSpeedup(
					addCommas(fmt.Sprintf("%d", c.Allocs)),
					safeSpeedup(float64(p.Allocs), float64(c.Allocs)),
					c.Allocs < p.Allocs,
				),
				getWinnerClass(
					c.CostTotal < p.CostTotal,
					"winner-blockpack",
				),
				formatCostWithTooltip(
					c.CostTotal,
					c.CostS3Get,
					c.CostS3Xfer,
					c.CostLambdaCompute,
					c.CostLambdaRequests,
				),
				getWinnerClass(
					!blockpackWins,
					"winner-parquet",
				),
				formatWithSpeedup(formatTime(p.TimeMs), safeSpeedup(c.TimeMs, p.TimeMs), !blockpackWins),
				getWinnerClass(
					p.CPUMs < c.CPUMs,
					"winner-parquet",
				),
				formatWithSpeedup(formatTime(p.CPUMs), safeSpeedup(c.CPUMs, p.CPUMs), p.CPUMs < c.CPUMs),
				getWinnerClass(
					p.BytesRead < c.BytesRead,
					"winner-parquet",
				),
				formatWithSpeedup(
					formatMemory(p.BytesReadMB),
					safeSpeedup(float64(c.BytesRead), float64(p.BytesRead)),
					p.BytesRead < c.BytesRead,
				),
				getWinnerClass(
					p.IOOps < c.IOOps,
					"winner-parquet",
				),
				formatWithSpeedup(
					addCommas(fmt.Sprintf("%d", p.IOOps)),
					safeSpeedup(float64(c.IOOps), float64(p.IOOps)),
					p.IOOps < c.IOOps,
				),
				"",
				addCommas(fmt.Sprintf("%d", p.Traces)),
				"",
				addCommas(fmt.Sprintf("%d", p.Spans)),
				getWinnerClass(
					p.MemoryMB < c.MemoryMB,
					"winner-parquet",
				),
				formatWithSpeedup(
					formatMemory(p.MemoryMB),
					safeSpeedup(c.MemoryMB, p.MemoryMB),
					p.MemoryMB < c.MemoryMB,
				),
				getWinnerClass(
					p.Allocs < c.Allocs,
					"winner-parquet",
				),
				formatWithSpeedup(
					addCommas(fmt.Sprintf("%d", p.Allocs)),
					safeSpeedup(float64(c.Allocs), float64(p.Allocs)),
					p.Allocs < c.Allocs,
				),
				getWinnerClass(
					p.CostTotal < c.CostTotal,
					"winner-parquet",
				),
				formatCostWithTooltip(
					p.CostTotal,
					p.CostS3Get,
					p.CostS3Xfer,
					p.CostLambdaCompute,
					p.CostLambdaRequests,
				),
			)
		}
		sb.WriteString(`
	</div>
`)
	}

	// Section 4: Aggregation vs Parquet
	aggregationSize := fileSizes.BlockpackHotMB
	if aggregationSize == 0 {
		aggregationSize = fileSizes.BlockpackMB
	}

	if len(aggregationComparisons) > 0 {
		//nolint:gosec // G705: fmt.Fprintf writes to strings.Builder; format string is a static HTML template
		fmt.Fprintf(&sb, `
	<button type="button" class="section-header collapsible" id="aggregation-comparison" onclick="toggleSection(this)" aria-expanded="true" aria-controls="aggregation-comparison-content">
		<span class="chevron">▼</span> 📊 Aggregation vs Parquet
	</button>
	<div class="collapsible-content" id="aggregation-comparison-content" aria-hidden="false">
	<div style="text-align: center; color: #666; margin-bottom: 30px;">
		Aggregation and analytics queries • Real trace data • 669,886 spans<br>
		Blockpack: %.1f MB • Parquet: %.1f MB
	</div>

	<table class="aggregation-comparison">
		<thead>
			<tr>
				<th rowspan="2">Query</th>
				<th colspan="8" class="aggregation-col" style="text-align: center;">Aggregation (Blockpack)</th>
				<th colspan="8" class="parquet-col parquet-start" style="text-align: center;">Parquet</th>
			</tr>
			<tr>
				<th class="numeric aggregation-col">Time</th>
				<th class="numeric aggregation-col">Bytes Read</th>
				<th class="numeric aggregation-col">I/O Ops</th>
				<th class="numeric aggregation-col">Groups</th>
				<th class="numeric aggregation-col">Time Buckets</th>
				<th class="numeric aggregation-col">Memory</th>
				<th class="numeric aggregation-col">Allocations</th>
				<th class="numeric aggregation-col">Cost</th>
				<th class="numeric parquet-col parquet-start">Time</th>
				<th class="numeric parquet-col">Bytes Read</th>
				<th class="numeric parquet-col">I/O Ops</th>
				<th class="numeric parquet-col">Groups</th>
				<th class="numeric parquet-col">Time Buckets</th>
				<th class="numeric parquet-col">Memory</th>
				<th class="numeric parquet-col">Allocations</th>
				<th class="numeric parquet-col">Cost</th>
			</tr>
		</thead>
		<tbody>
`, aggregationSize, fileSizes.ParquetMB)

		sb.WriteString(generateAggregationComparisonRows(aggregationComparisons, titleCaser))

		sb.WriteString(`		</tbody>
	</table>
	</div>
	`)
	} else {
		sb.WriteString(`
	<button type="button" class="section-header collapsible" id="aggregation-comparison" onclick="toggleSection(this)" aria-expanded="true" aria-controls="aggregation-comparison-content">
		<span class="chevron">▼</span> 📊 Aggregation vs Parquet
	</button>
	<div class="collapsible-content" id="aggregation-comparison-content" aria-hidden="false">
	<div style="text-align: center; color: #666; margin-bottom: 30px;">
		No aggregation comparison benchmarks were run.
	</div>
	</div>
	`)
	}

	// Section 5: Format Comparison
	//nolint:gosec // G705: fmt.Fprintf writes to strings.Builder; format string is a static HTML template
	fmt.Fprintf(&sb, `
	<button type="button" class="section-header collapsible" id="format-comparison" onclick="toggleSection(this)" aria-expanded="true" aria-controls="format-comparison-content">
		<span class="chevron">▼</span> ⚖️ Format Comparison (Blockpack vs Parquet)
	</button>
	<div class="collapsible-content" id="format-comparison-content" aria-hidden="false">
	<div style="text-align: center; color: #666; margin-bottom: 30px;">
		Synthetic test data • 250,000 traces • Side-by-side performance comparison
	</div>
	%s

	<table class="comparison-table">
		<thead>
			<tr>
				<th rowspan="2">Query</th>
				<th rowspan="2" style="text-align:center; min-width:90px">Win</th>
				<th colspan="9" class="blockpack-col" style="text-align: center;">Blockpack</th>
				<th colspan="9" class="parquet-col" style="text-align: center;">Parquet</th>
			</tr>
			<tr>
				<th class="numeric blockpack-col">Wall</th>
				<th class="numeric blockpack-col">CPU</th>
				<th class="numeric blockpack-col">Bytes Read</th>
				<th class="numeric blockpack-col">I/O Ops</th>
				<th class="numeric blockpack-col">Traces</th>
				<th class="numeric blockpack-col">Spans</th>
				<th class="numeric blockpack-col">Memory</th>
				<th class="numeric blockpack-col">Allocations</th>
				<th class="numeric blockpack-col">Cost</th>
				<th class="numeric parquet-col">Wall</th>
				<th class="numeric parquet-col">CPU</th>
				<th class="numeric parquet-col">Bytes Read</th>
				<th class="numeric parquet-col">I/O Ops</th>
				<th class="numeric parquet-col">Traces</th>
				<th class="numeric parquet-col">Spans</th>
				<th class="numeric parquet-col">Memory</th>
				<th class="numeric parquet-col">Allocations</th>
				<th class="numeric parquet-col">Cost</th>
			</tr>
		</thead>
		<tbody>
`, formatFileSizeBar(fileSizes.BlockpackMB, fileSizes.ParquetMB))

	for _, comp := range comparisonPairs {
		c := comp.Blockpack
		p := comp.Parquet

		// Winner is determined by lowest wall+cpu score: faster wall time AND less CPU both matter.
		cScore := c.TimeMs + c.CPUMs
		pScore := p.TimeMs + p.CPUMs
		blockpackWins := cScore < pScore
		var winCell string
		if math.Abs(cScore-pScore) < 1e-3 { // <1µs difference: treat as tie
			winCell = winCellTie
		} else {
			winCell = formatWinCell(blockpackWins, true, cScore, pScore)
		}

		queryName := html.EscapeString(titleCaser.String(strings.ReplaceAll(comp.QueryName, "_", " ")))
		anchorID := slugifyID("query-cmp", comp.QueryName)

		// Calculate speedups for each metric (with division by zero protection)
		timeSpeedup := safeSpeedup(p.TimeMs, c.TimeMs)
		cpuSpeedup := safeSpeedup(p.CPUMs, c.CPUMs)
		bytesSpeedup := safeSpeedup(float64(p.BytesRead), float64(c.BytesRead))
		ioOpsSpeedup := safeSpeedup(float64(p.IOOps), float64(c.IOOps))
		memorySpeedup := safeSpeedup(p.MemoryMB, c.MemoryMB)
		allocsSpeedup := safeSpeedup(float64(p.Allocs), float64(c.Allocs))
		costSpeedup := safeSpeedup(p.CostTotal, c.CostTotal)

		//nolint:gosec // G705: fmt.Fprintf writes to strings.Builder; format string is a static HTML template
		fmt.Fprintf(&sb,
			`			<tr>
				<td class="query-name"><a href="#%s">%s</a></td>
				%s
				<td class="metric numeric%s">%s</td>
				<td class="metric numeric%s">%s</td>
				<td class="metric numeric%s">%s</td>
				<td class="metric numeric%s">%s</td>
				<td class="metric numeric%s">%s</td>
				<td class="metric numeric%s">%s</td>
				<td class="metric numeric%s">%s</td>
				<td class="metric numeric%s">%s</td>
				<td class="metric numeric%s">%s</td>
				<td class="metric numeric%s">%s</td>
				<td class="metric numeric%s">%s</td>
				<td class="metric numeric%s">%s</td>
				<td class="metric numeric%s">%s</td>
				<td class="metric numeric%s">%s</td>
				<td class="metric numeric%s">%s</td>
				<td class="metric numeric%s">%s</td>
				<td class="metric numeric%s">%s</td>
				<td class="metric numeric%s">%s</td>
			</tr>
`,
			anchorID,
			queryName,
			winCell,
			getWinnerClass(c.TimeMs < p.TimeMs, "winner-blockpack"),
			formatWithSpeedup(formatTime(c.TimeMs), timeSpeedup, c.TimeMs < p.TimeMs),
			getWinnerClass(c.CPUMs < p.CPUMs, "winner-blockpack"),
			formatWithSpeedup(formatTime(c.CPUMs), cpuSpeedup, c.CPUMs < p.CPUMs),
			getWinnerClass(c.BytesRead < p.BytesRead, "winner-blockpack"),
			formatWithSpeedup(formatMemory(c.BytesReadMB), bytesSpeedup, c.BytesRead < p.BytesRead),
			getWinnerClass(c.IOOps < p.IOOps, "winner-blockpack"),
			formatWithSpeedup(addCommas(fmt.Sprintf("%d", c.IOOps)), ioOpsSpeedup, c.IOOps < p.IOOps),
			"",
			addCommas(fmt.Sprintf("%d", c.Traces)),
			"",
			addCommas(fmt.Sprintf("%d", c.Spans)),
			getWinnerClass(c.MemoryMB < p.MemoryMB, "winner-blockpack"),
			formatWithSpeedup(formatMemory(c.MemoryMB), memorySpeedup, c.MemoryMB < p.MemoryMB),
			getWinnerClass(c.Allocs < p.Allocs, "winner-blockpack"),
			formatWithSpeedup(addCommas(fmt.Sprintf("%d", c.Allocs)), allocsSpeedup, c.Allocs < p.Allocs),
			getWinnerClass(c.CostTotal < p.CostTotal, "winner-blockpack"),
			formatWithSpeedup(
				formatCostWithTooltip(c.CostTotal, c.CostS3Get, c.CostS3Xfer, c.CostLambdaCompute, c.CostLambdaRequests),
				costSpeedup,
				c.CostTotal < p.CostTotal,
			),
			getWinnerClass(p.TimeMs < c.TimeMs, "winner-parquet"),
			formatWithSpeedup(formatTime(p.TimeMs), safeSpeedup(c.TimeMs, p.TimeMs), p.TimeMs < c.TimeMs),
			getWinnerClass(p.CPUMs < c.CPUMs, "winner-parquet"),
			formatWithSpeedup(formatTime(p.CPUMs), safeSpeedup(c.CPUMs, p.CPUMs), p.CPUMs < c.CPUMs),
			getWinnerClass(p.BytesRead < c.BytesRead, "winner-parquet"),
			formatWithSpeedup(formatMemory(p.BytesReadMB), safeSpeedup(float64(c.BytesRead), float64(p.BytesRead)), p.BytesRead < c.BytesRead),
			getWinnerClass(p.IOOps < c.IOOps, "winner-parquet"),
			formatWithSpeedup(addCommas(fmt.Sprintf("%d", p.IOOps)), safeSpeedup(float64(c.IOOps), float64(p.IOOps)), p.IOOps < c.IOOps),
			"",
			addCommas(fmt.Sprintf("%d", p.Traces)),
			"",
			addCommas(fmt.Sprintf("%d", p.Spans)),
			getWinnerClass(p.MemoryMB < c.MemoryMB, "winner-parquet"),
			formatWithSpeedup(formatMemory(p.MemoryMB), safeSpeedup(c.MemoryMB, p.MemoryMB), p.MemoryMB < c.MemoryMB),
			getWinnerClass(p.Allocs < c.Allocs, "winner-parquet"),
			formatWithSpeedup(addCommas(fmt.Sprintf("%d", p.Allocs)), safeSpeedup(float64(c.Allocs), float64(p.Allocs)), p.Allocs < c.Allocs),
			getWinnerClass(p.CostTotal < c.CostTotal, "winner-parquet"),
			formatWithSpeedup(
				formatCostWithTooltip(p.CostTotal, p.CostS3Get, p.CostS3Xfer, p.CostLambdaCompute, p.CostLambdaRequests),
				safeSpeedup(c.CostTotal, p.CostTotal),
				p.CostTotal < c.CostTotal,
			),
		)
	}

	sb.WriteString(`		</tbody>
	</table>
	</div>
`)

	// Section 6: TraceQL Metrics Comparison
	sb.WriteString(generateTraceMetricsSection(traceMetrics))

	// Section 7: Write Path Comparison
	sb.WriteString(generateWritePathSection(writePaths))

	sb.WriteString(`
	<button type="button" class="section-header collapsible" id="query-definitions" onclick="toggleSection(this)" aria-expanded="true" aria-controls="query-definitions-content">
		<span class="chevron">▼</span> 📝 Query Definitions
	</button>

	<div class="collapsible-content" id="query-definitions-content" aria-hidden="false">
	<div class="query-definitions">
`)

	// Add otel-demo-data query definitions
	otelDemoQueries := getOTELDemoQueries()
	sb.WriteString(`		<h2 style="color: #333; margin-bottom: 20px;">OTEL Generator Demo Data Queries</h2>
`)
	for _, comp := range otelDemoPairs {
		if def, ok := otelDemoQueries[comp.QueryName]; ok {
			queryName := html.EscapeString(titleCaser.String(strings.ReplaceAll(comp.QueryName, "-", " ")))
			anchorID := slugifyID("query-rw", comp.QueryName)
			//nolint:gosec // G705: fmt.Fprintf writes to strings.Builder; format string is a static HTML template
			fmt.Fprintf(&sb, `		<div class="query-def" id="%s">
			<h3>%s</h3>
			<strong>TraceQL:</strong>
			<pre>%s</pre>
		</div>
`, anchorID, queryName, html.EscapeString(def.TraceQL))
		}
	}

	// Add comparison query definitions
	cmpQueries := getComparisonQueries()
	sb.WriteString(`
		<h2 style="color: #333; margin-top: 40px; margin-bottom: 20px;">Format Comparison Queries</h2>
`)
	for _, comp := range comparisonPairs {
		if def, ok := cmpQueries[comp.QueryName]; ok {
			queryName := html.EscapeString(titleCaser.String(strings.ReplaceAll(comp.QueryName, "_", " ")))
			anchorID := slugifyID("query-cmp", comp.QueryName)
			descHTML := ""
			if def.Description != "" {
				descHTML = fmt.Sprintf(`			<div class="description">%s</div>
`, html.EscapeString(def.Description))
			}
			//nolint:gosec // G705: fmt.Fprintf writes to strings.Builder; format string is a static HTML template
			fmt.Fprintf(&sb, `		<div class="query-def" id="%s">
			<h3>%s</h3>
%s			<strong>TraceQL:</strong>
			<pre>%s</pre>
		</div>
`, anchorID, queryName, descHTML, html.EscapeString(def.TraceQL))
		}
	}

	sb.WriteString(`	</div>

	<div class="legend">
		<p><span style="color: #27ae60; font-weight: 600;">Green highlight</span> = Aggregation wins · <span style="color: #f39c12; font-weight: 600;">Orange highlight</span> = Manual wins · <span style="color: #3498db; font-weight: 600;">Blue highlight</span> = Parquet wins</p>
		<p style="margin-top: 10px; font-size: 13px;">
			Hardware: 13th Gen Intel Core i7-13700K (24 threads) • OS: Linux
		</p>
	</div>
	</div>
</body>
</html>
`)

	return sb.String()
}

func main() {
	blockpackAnalysis := flag.String("blockpack-analysis", "", "Path to blockpack structure analysis HTML file")
	validate := flag.Bool("validate", false, "Enable validation (fail-fast on errors)")
	outputDir := flag.String("output-dir", "", "Output directory for versioned results (enables JSON+HTML output)")
	flag.Parse()

	// Create title caser for query names
	titleCaser := cases.Title(language.English)

	// Parse benchmarks using benchfmt
	otelDemo, comparisons, aggregations, writePaths, traceMetrics, err := parseBenchmarks(os.Stdin)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error parsing benchmarks: %v\n", err)
		os.Exit(1)
	}

	if len(otelDemo) == 0 && len(comparisons) == 0 && len(aggregations) == 0 && len(writePaths) == 0 && len(traceMetrics) == 0 {
		fmt.Fprintln(os.Stderr, "Error: No benchmark results found in input")
		os.Exit(1)
	}

	// Validate if requested
	if *validate {
		if err := validateResults(otelDemo, comparisons, aggregations, writePaths); err != nil {
			fmt.Fprintf(os.Stderr, "\n❌ Validation failed:\n%v\n\n", err)
			fmt.Fprintf(os.Stderr, "No output files generated (validation must pass before saving results)\n")
			os.Exit(1)
		}
	}

	// If output directory specified, generate versioned JSON+HTML output
	if *outputDir != "" {
		// Create output directory
		if err := os.MkdirAll(*outputDir, 0o755); err != nil { //nolint:gosec
			fmt.Fprintf(os.Stderr, "Error creating output directory: %v\n", err)
			os.Exit(1)
		}

		// Generate timestamp once for consistency
		timestamp := time.Now().UTC()

		// Collect metadata
		metadata := BenchmarkMetadata{
			Timestamp:   timestamp.Format(time.RFC3339),
			GitHash:     getGitHash(),
			ResultsHash: generateResultsHash(fmt.Sprintf("%v%v%v%v%v", otelDemo, comparisons, aggregations, writePaths, traceMetrics)),
			System:      collectSystemInfo(),
		}

		// Create report
		report := BenchmarkReport{
			Metadata:     metadata,
			OTELDemo:     otelDemo,
			Comparisons:  comparisons,
			Aggregations: aggregations,
			WritePath:    writePaths,
			TraceMetrics: traceMetrics,
		}

		// Generate filenames using the same timestamp
		jsonFilename := generateFilename(timestamp, metadata.ResultsHash, "json")
		htmlFilename := generateFilename(timestamp, metadata.ResultsHash, "html")

		jsonPath := filepath.Join(*outputDir, jsonFilename)
		htmlPath := filepath.Join(*outputDir, htmlFilename)
		latestPath := filepath.Join(*outputDir, "latest")

		// Write JSON
		if err := writeJSONReport(jsonPath, report); err != nil {
			fmt.Fprintf(os.Stderr, "Error writing JSON: %v\n", err)
			os.Exit(1)
		}

		// Write HTML
		if err := writeHTMLReport(htmlPath, report, *blockpackAnalysis, timestamp); err != nil {
			fmt.Fprintf(os.Stderr, "Error writing HTML: %v\n", err)
			os.Exit(1)
		}

		// Update symlink
		if err := updateSymlink(htmlPath, latestPath); err != nil {
			fmt.Fprintf(os.Stderr, "Warning: failed to update symlink: %v\n", err)
		}

		// Print success message
		fmt.Fprintf(os.Stderr, "\n✅ Benchmark results saved!\n\n")
		fmt.Fprintf(os.Stderr, "Files:\n")
		fmt.Fprintf(os.Stderr, "  JSON:   %s\n", jsonPath)
		fmt.Fprintf(os.Stderr, "  HTML:   %s\n", htmlPath)
		fmt.Fprintf(os.Stderr, "  Latest: %s (→ %s)\n", latestPath, htmlFilename)
		fmt.Fprintf(os.Stderr, "\nMetadata:\n")
		fmt.Fprintf(os.Stderr, "  Timestamp:  %s\n", metadata.Timestamp)
		fmt.Fprintf(os.Stderr, "  Git Hash:   %s\n", metadata.GitHash)
		fmt.Fprintf(os.Stderr, "  System:     %s, %.1f GB RAM, %s, %s\n",
			metadata.System.CPUModel, metadata.System.RAM, metadata.System.OS, metadata.System.GoVersion)

		return
	}

	// Otherwise, generate HTML to stdout (legacy behavior)
	// Parse file sizes from disk (not from benchmark input)
	fileSizes := parseFileSizes("")

	htmlOutput := generateHTML(
		otelDemo,
		comparisons,
		aggregations,
		writePaths,
		traceMetrics,
		fileSizes,
		titleCaser,
		*blockpackAnalysis,
		time.Now(),
	)
	fmt.Print(htmlOutput)
}
