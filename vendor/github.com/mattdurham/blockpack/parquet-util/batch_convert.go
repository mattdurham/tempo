package parquetutil

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
)

type ConversionResult struct {
	SourcePath       string
	DestinationPath  string
	MarkdownPath     string
	SourceSize       int64
	DestinationSize  int64
	SourceTraceCount int
	SourceSpanCount  int
	DestTraceCount   int
	DestSpanCount    int
	Error            error
	CompareResult    *CompareResult
}

type ProgressReport struct {
	Converted int
	Total     int
	Filename  string // Current file being processed (empty for summary reports)
}

type ConvertOptions struct {
	ProgressEvery int
	EmitStart     bool
	Progress      func(ProgressReport)
	Workers       int // Number of parallel workers. If 0, uses runtime.NumCPU()
}

func ConvertParquetFolder(inputDir, outputDir string) ([]ConversionResult, error) {
	return ConvertParquetFolderWithOptions(inputDir, outputDir, ConvertOptions{})
}

func ConvertParquetFolderWithOptions(inputDir, outputDir string, options ConvertOptions) ([]ConversionResult, error) {
	if inputDir == "" {
		return nil, fmt.Errorf("input directory is required")
	}
	if outputDir == "" {
		return nil, fmt.Errorf("output directory is required")
	}
	if err := os.MkdirAll(outputDir, 0o755); err != nil {
		return nil, err
	}

	parquetPaths := make([]string, 0)
	err := filepath.WalkDir(inputDir, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() {
			return nil
		}
		if filepath.Ext(path) != ".parquet" {
			return nil
		}
		parquetPaths = append(parquetPaths, path)
		return nil
	})
	if err != nil {
		return nil, err
	}

	total := len(parquetPaths)
	if options.EmitStart && options.Progress != nil {
		options.Progress(ProgressReport{Converted: 0, Total: total})
	}

	// Determine number of workers
	workers := options.Workers
	if workers <= 0 {
		workers = runtime.NumCPU()
	}

	// Job represents a conversion task
	type job struct {
		path       string
		outputDir  string
		parentName string
	}

	// Create job channel
	jobs := make(chan job, len(parquetPaths))
	resultChan := make(chan ConversionResult, len(parquetPaths))

	// Track progress
	var mu sync.Mutex
	converted := 0

	// Start workers
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := range jobs {
				// Report starting work on this file (only if ProgressEvery == 0 or 1)
				if options.Progress != nil && (options.ProgressEvery <= 1) {
					mu.Lock()
					currentConverted := converted
					mu.Unlock()
					options.Progress(ProgressReport{
						Converted: currentConverted,
						Total:     total,
						Filename:  j.parentName,
					})
				}

				outBlockpack := filepath.Join(j.outputDir, j.parentName+".blockpack")
				outMarkdown := filepath.Join(j.outputDir, j.parentName+".md")

				result := ConversionResult{
					SourcePath:      j.path,
					DestinationPath: outBlockpack,
					MarkdownPath:    outMarkdown,
				}

				// Get source file size
				sourceInfo, err := os.Stat(j.path)
				if err != nil {
					result.Error = fmt.Errorf("failed to stat source file: %w", err)
				} else {
					result.SourceSize = sourceInfo.Size()

					// Convert and compare in one operation (reads parquet only once)
					stats, compareResult, err := ConvertAndCompare(j.path, outBlockpack)
					if err != nil {
						result.Error = fmt.Errorf("conversion failed: %w", err)
					} else {
						result.DestinationSize = stats.Size
						result.SourceTraceCount = stats.TraceCount
						result.SourceSpanCount = stats.SpanCount
						result.DestTraceCount = stats.TraceCount
						result.DestSpanCount = stats.SpanCount

						// Always add comparison results (success or failure)
						result.CompareResult = compareResult
					}
				}

				// Always write markdown, even on error
				if err := os.WriteFile(outMarkdown, []byte(ConversionMarkdown(result)), 0o644); err != nil {
					// Log but don't fail the whole batch
					fmt.Fprintf(os.Stderr, "Warning: failed to write markdown for %s: %v\n", j.parentName, err)
				}

				resultChan <- result

				// Update progress
				mu.Lock()
				converted++
				currentConverted := converted
				mu.Unlock()

				if options.ProgressEvery > 0 && options.Progress != nil && currentConverted%options.ProgressEvery == 0 {
					options.Progress(ProgressReport{Converted: currentConverted, Total: total, Filename: ""})
				}
			}
		}()
	}

	// Send jobs
	for _, path := range parquetPaths {
		parentName := filepath.Base(filepath.Dir(path))
		if parentName == "." || parentName == string(filepath.Separator) {
			continue
		}
		jobs <- job{path: path, outputDir: outputDir, parentName: parentName}
	}
	close(jobs)

	// Wait for all workers to finish
	wg.Wait()
	close(resultChan)

	// Collect results
	results := make([]ConversionResult, 0, len(parquetPaths))
	for result := range resultChan {
		results = append(results, result)
	}

	return results, nil
}

func formatBytes(bytes int64) string {
	const unit = 1024
	if bytes < unit {
		return fmt.Sprintf("%d B", bytes)
	}
	div, exp := int64(unit), 0
	for n := bytes / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	units := []string{"KB", "MB", "GB", "TB", "PB"}
	return fmt.Sprintf("%.2f %s", float64(bytes)/float64(div), units[exp])
}

func ConversionMarkdown(result ConversionResult) string {
	var sb strings.Builder
	sb.WriteString(fmt.Sprintf(`# Parquet Conversion

## Files
- Source: %s
- Destination: %s

`, result.SourcePath, result.DestinationPath))

	// If conversion failed, show error and return
	if result.Error != nil {
		sb.WriteString(fmt.Sprintf(`## Status

**CONVERSION FAILED**

Error: %s

`, result.Error.Error()))

		// Show source info if we have it
		if result.SourceSize > 0 {
			sb.WriteString(fmt.Sprintf(`## Source File Info

- Size: %d bytes (%s)
`, result.SourceSize, formatBytes(result.SourceSize)))
		}

		if result.SourceTraceCount > 0 || result.SourceSpanCount > 0 {
			sb.WriteString(fmt.Sprintf(`- Traces: %d
- Spans: %d
`, result.SourceTraceCount, result.SourceSpanCount))
		}

		return sb.String()
	}

	// Success case - show full stats
	diff := result.DestinationSize - result.SourceSize
	var percentChange float64
	if result.SourceSize > 0 {
		percentChange = (float64(diff) / float64(result.SourceSize)) * 100
	}

	diffSign := ""
	if diff > 0 {
		diffSign = "+"
	}

	sb.WriteString(fmt.Sprintf(`## Data

### Source (Parquet)
- Traces: %d
- Spans: %d

### Destination (Blockpack)
- Traces: %d
- Spans: %d

## Sizes
- Source size: %d bytes (%s)
- Destination size: %d bytes (%s)
- Difference: %s%d bytes (%s) [%.2f%%]

`,
		result.SourceTraceCount,
		result.SourceSpanCount,
		result.DestTraceCount,
		result.DestSpanCount,
		result.SourceSize,
		formatBytes(result.SourceSize),
		result.DestinationSize,
		formatBytes(result.DestinationSize),
		diffSign,
		diff,
		formatBytes(abs(diff)),
		percentChange,
	))

	// Add blockpack analysis if available
	analysisMarkdown, err := analyzeBlockpackToMarkdown(result.DestinationPath)
	if err == nil {
		sb.WriteString(analysisMarkdown)
	}

	// Add deep comparison results if available
	if result.CompareResult != nil {
		sb.WriteString(CompareMarkdownSummary(result.CompareResult))
	}

	return sb.String()
}

func abs(n int64) int64 {
	if n < 0 {
		return -n
	}
	return n
}
