package ondiskio

import (
	"fmt"
	"runtime"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// IOLogEntry represents a single IO operation
type IOLogEntry struct {
	Offset    int64
	Length    int
	Timestamp time.Time
	Duration  time.Duration
	Caller    string // Caller function name for context
}

// IOCategory represents the type of data being read
type IOCategory int

const (
	IOCategoryUnknown IOCategory = iota
	IOCategoryFooter
	IOCategoryMetadata
	IOCategoryBlockIndex
	IOCategoryColumnIndex
	IOCategoryDedicatedIndex
	IOCategoryBlockData
	IOCategoryBloomFilter
	IOCategoryMetricStream
)

func (c IOCategory) String() string {
	switch c {
	case IOCategoryFooter:
		return "Footer"
	case IOCategoryMetadata:
		return "Metadata"
	case IOCategoryBlockIndex:
		return "BlockIndex"
	case IOCategoryColumnIndex:
		return "ColumnIndex"
	case IOCategoryDedicatedIndex:
		return "DedicatedIndex"
	case IOCategoryBlockData:
		return "BlockData"
	case IOCategoryBloomFilter:
		return "BloomFilter"
	case IOCategoryMetricStream:
		return "MetricStream"
	default:
		return "Unknown"
	}
}

// DetailedTrackingReader tracks IO operations with detailed logging
type DetailedTrackingReader struct {
	underlying   ReaderProvider
	bytesRead    int64
	ioOperations int64
	latency      int64

	// Detailed logging
	logEnabled bool
	logMu      sync.Mutex
	log        []IOLogEntry

	// File structure info for categorization
	footerOffset       int64
	metadataOffset     int64
	metadataLen        int64
	metricStreamOffset int64
	metricStreamLen    int64
	blockOffsets       map[int64]bool // Quick lookup for block data offsets
}

// NewDetailedTrackingReader creates a tracking reader with detailed IO logging
func NewDetailedTrackingReader(underlying ReaderProvider, logEnabled bool) *DetailedTrackingReader {
	return &DetailedTrackingReader{
		underlying:   underlying,
		logEnabled:   logEnabled,
		log:          make([]IOLogEntry, 0, 1000),
		blockOffsets: make(map[int64]bool),
	}
}

// NewDetailedTrackingReaderWithLatency creates a tracking reader with latency simulation
func NewDetailedTrackingReaderWithLatency(underlying ReaderProvider, latency time.Duration, logEnabled bool) *DetailedTrackingReader {
	return &DetailedTrackingReader{
		underlying:   underlying,
		latency:      int64(latency),
		logEnabled:   logEnabled,
		log:          make([]IOLogEntry, 0, 1000),
		blockOffsets: make(map[int64]bool),
	}
}

// SetFileStructure sets known file structure offsets for better categorization
func (d *DetailedTrackingReader) SetFileStructure(footerOffset, metadataOffset, metadataLen, metricStreamOffset, metricStreamLen int64, blockOffsets []uint64) {
	d.logMu.Lock()
	defer d.logMu.Unlock()

	d.footerOffset = footerOffset
	d.metadataOffset = metadataOffset
	d.metadataLen = metadataLen
	d.metricStreamOffset = metricStreamOffset
	d.metricStreamLen = metricStreamLen

	for _, offset := range blockOffsets {
		d.blockOffsets[int64(offset)] = true
	}
}

func (d *DetailedTrackingReader) Size() (int64, error) {
	return d.underlying.Size()
}

func (d *DetailedTrackingReader) ReadAt(p []byte, off int64) (int, error) {
	start := time.Now()

	// Simulate latency
	if latency := atomic.LoadInt64(&d.latency); latency > 0 {
		time.Sleep(time.Duration(latency))
	}

	n, err := d.underlying.ReadAt(p, off)

	duration := time.Since(start)

	// Track statistics
	atomic.AddInt64(&d.ioOperations, 1)
	if n > 0 {
		atomic.AddInt64(&d.bytesRead, int64(n))
	}

	// Log detailed entry if enabled
	if d.logEnabled && err == nil {
		caller := getCallerInfo(3) // Skip 3 frames: ReadAt -> getCallerInfo -> runtime.Callers
		d.logMu.Lock()
		d.log = append(d.log, IOLogEntry{
			Offset:    off,
			Length:    n,
			Timestamp: start,
			Duration:  duration,
			Caller:    caller,
		})
		d.logMu.Unlock()
	}

	return n, err
}

// BytesRead returns total bytes read
func (d *DetailedTrackingReader) BytesRead() int64 {
	return atomic.LoadInt64(&d.bytesRead)
}

// IOOperations returns number of ReadAt calls
func (d *DetailedTrackingReader) IOOperations() int64 {
	return atomic.LoadInt64(&d.ioOperations)
}

// BytesPerIO returns average bytes per operation
func (d *DetailedTrackingReader) BytesPerIO() float64 {
	ops := atomic.LoadInt64(&d.ioOperations)
	if ops == 0 {
		return 0
	}
	return float64(atomic.LoadInt64(&d.bytesRead)) / float64(ops)
}

// Reset resets all counters and clears log
func (d *DetailedTrackingReader) Reset() {
	atomic.StoreInt64(&d.bytesRead, 0)
	atomic.StoreInt64(&d.ioOperations, 0)
	d.logMu.Lock()
	d.log = d.log[:0]
	d.logMu.Unlock()
}

// GetLog returns a copy of the IO log
func (d *DetailedTrackingReader) GetLog() []IOLogEntry {
	d.logMu.Lock()
	defer d.logMu.Unlock()

	logCopy := make([]IOLogEntry, len(d.log))
	copy(logCopy, d.log)
	return logCopy
}

// CategorizeIO determines what type of data an IO operation is reading
func (d *DetailedTrackingReader) CategorizeIO(entry IOLogEntry) IOCategory {
	off := entry.Offset
	_ = off + int64(entry.Length) // end offset, not currently used

	// Footer is typically at the end
	if d.footerOffset > 0 && off >= d.footerOffset {
		return IOCategoryFooter
	}

	// Metadata section
	if d.metadataOffset > 0 && off >= d.metadataOffset && off < d.metadataOffset+d.metadataLen {
		return IOCategoryMetadata
	}

	// Metric stream section
	if d.metricStreamOffset > 0 && off >= d.metricStreamOffset && off < d.metricStreamOffset+d.metricStreamLen {
		return IOCategoryMetricStream
	}

	// Block data - check if offset matches known block offset
	if len(d.blockOffsets) > 0 && d.blockOffsets[off] {
		return IOCategoryBlockData
	}

	// Heuristics based on caller
	caller := strings.ToLower(entry.Caller)
	if strings.Contains(caller, "footer") {
		return IOCategoryFooter
	}
	if strings.Contains(caller, "metadata") {
		return IOCategoryMetadata
	}
	if strings.Contains(caller, "columnindex") || strings.Contains(caller, "column_index") {
		return IOCategoryColumnIndex
	}
	if strings.Contains(caller, "blockindex") || strings.Contains(caller, "block_index") {
		return IOCategoryBlockIndex
	}
	if strings.Contains(caller, "dedicated") {
		return IOCategoryDedicatedIndex
	}
	if strings.Contains(caller, "bloom") {
		return IOCategoryBloomFilter
	}
	if strings.Contains(caller, "block") || strings.Contains(caller, "coalesce") {
		return IOCategoryBlockData
	}

	return IOCategoryUnknown
}

// IOStats represents aggregated statistics
type IOStats struct {
	TotalIOs      int
	TotalBytes    int64
	TotalDuration time.Duration
	AvgBytesPerIO float64
	AvgDuration   time.Duration
	SmallestIO    int
	LargestIO     int
	ByCategory    map[IOCategory]CategoryStats
	OverlapBytes  int64 // Bytes read multiple times (overlapping ranges)
}

// CategoryStats represents stats for a specific IO category
type CategoryStats struct {
	Count      int
	TotalBytes int64
	AvgBytes   float64
	MinBytes   int
	MaxBytes   int
}

// AnalyzeIOLog performs detailed analysis of IO operations
func (d *DetailedTrackingReader) AnalyzeIOLog() IOStats {
	d.logMu.Lock()
	defer d.logMu.Unlock()

	if len(d.log) == 0 {
		return IOStats{}
	}

	stats := IOStats{
		TotalIOs:   len(d.log),
		SmallestIO: d.log[0].Length,
		LargestIO:  d.log[0].Length,
		ByCategory: make(map[IOCategory]CategoryStats),
	}

	// Categorize and aggregate
	for _, entry := range d.log {
		stats.TotalBytes += int64(entry.Length)
		stats.TotalDuration += entry.Duration

		if entry.Length < stats.SmallestIO {
			stats.SmallestIO = entry.Length
		}
		if entry.Length > stats.LargestIO {
			stats.LargestIO = entry.Length
		}

		category := d.CategorizeIO(entry)
		cs := stats.ByCategory[category]
		cs.Count++
		cs.TotalBytes += int64(entry.Length)
		if cs.MinBytes == 0 || entry.Length < cs.MinBytes {
			cs.MinBytes = entry.Length
		}
		if entry.Length > cs.MaxBytes {
			cs.MaxBytes = entry.Length
		}
		stats.ByCategory[category] = cs
	}

	// Calculate averages
	if stats.TotalIOs > 0 {
		stats.AvgBytesPerIO = float64(stats.TotalBytes) / float64(stats.TotalIOs)
		stats.AvgDuration = stats.TotalDuration / time.Duration(stats.TotalIOs)
	}

	for cat, cs := range stats.ByCategory {
		if cs.Count > 0 {
			cs.AvgBytes = float64(cs.TotalBytes) / float64(cs.Count)
			stats.ByCategory[cat] = cs
		}
	}

	// Detect overlapping reads
	stats.OverlapBytes = d.calculateOverlap()

	return stats
}

// calculateOverlap detects bytes read multiple times
func (d *DetailedTrackingReader) calculateOverlap() int64 {
	if len(d.log) == 0 {
		return 0
	}

	// Create sorted list of ranges
	type ioRange struct {
		start int64
		end   int64
	}

	ranges := make([]ioRange, len(d.log))
	for i, entry := range d.log {
		ranges[i] = ioRange{
			start: entry.Offset,
			end:   entry.Offset + int64(entry.Length),
		}
	}

	sort.Slice(ranges, func(i, j int) bool {
		return ranges[i].start < ranges[j].start
	})

	// Find overlaps
	var overlapBytes int64
	for i := 0; i < len(ranges)-1; i++ {
		for j := i + 1; j < len(ranges); j++ {
			if ranges[j].start >= ranges[i].end {
				break // No more overlaps with range i
			}
			// Calculate overlap
			overlapStart := maxInt64(ranges[i].start, ranges[j].start)
			overlapEnd := minInt64(ranges[i].end, ranges[j].end)
			if overlapEnd > overlapStart {
				overlapBytes += overlapEnd - overlapStart
			}
		}
	}

	return overlapBytes
}

// PrintIOAnalysis prints a human-readable analysis
func (d *DetailedTrackingReader) PrintIOAnalysis() string {
	stats := d.AnalyzeIOLog()

	var sb strings.Builder
	sb.WriteString("\n=== Detailed IO Analysis ===\n\n")

	sb.WriteString(fmt.Sprintf("Total IOs: %d\n", stats.TotalIOs))
	sb.WriteString(fmt.Sprintf("Total Bytes: %.2f MB\n", float64(stats.TotalBytes)/(1024*1024)))
	sb.WriteString(fmt.Sprintf("Total Duration: %v\n", stats.TotalDuration))
	sb.WriteString(fmt.Sprintf("Avg Bytes/IO: %.1f KB\n", stats.AvgBytesPerIO/1024))
	sb.WriteString(fmt.Sprintf("Avg Duration/IO: %v\n", stats.AvgDuration))
	sb.WriteString(fmt.Sprintf("IO Size Range: %d - %d bytes\n", stats.SmallestIO, stats.LargestIO))
	if stats.OverlapBytes > 0 {
		sb.WriteString(fmt.Sprintf("Overlapping Reads: %.2f MB (%.1f%% waste)\n",
			float64(stats.OverlapBytes)/(1024*1024),
			100.0*float64(stats.OverlapBytes)/float64(stats.TotalBytes)))
	}

	sb.WriteString("\n=== Breakdown by Category ===\n\n")

	// Sort categories by count
	type catWithStats struct {
		cat   IOCategory
		stats CategoryStats
	}
	cats := make([]catWithStats, 0, len(stats.ByCategory))
	for cat, cs := range stats.ByCategory {
		cats = append(cats, catWithStats{cat, cs})
	}
	sort.Slice(cats, func(i, j int) bool {
		return cats[i].stats.Count > cats[j].stats.Count
	})

	for _, c := range cats {
		pct := 100.0 * float64(c.stats.Count) / float64(stats.TotalIOs)
		sb.WriteString(fmt.Sprintf("%-20s: %4d IOs (%5.1f%%), %.2f MB, avg %.1f KB/IO\n",
			c.cat.String(),
			c.stats.Count,
			pct,
			float64(c.stats.TotalBytes)/(1024*1024),
			c.stats.AvgBytes/1024))
	}

	return sb.String()
}

// getCallerInfo extracts caller function name from stack
func getCallerInfo(skip int) string {
	pc := make([]uintptr, 1)
	n := runtime.Callers(skip, pc)
	if n == 0 {
		return "unknown"
	}

	frames := runtime.CallersFrames(pc)
	frame, _ := frames.Next()

	// Extract just the function name
	parts := strings.Split(frame.Function, ".")
	if len(parts) > 0 {
		return parts[len(parts)-1]
	}

	return frame.Function
}

func maxInt64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}

func minInt64(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}
