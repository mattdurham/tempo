// Command analyze reads all Loki chunk files and produces a comprehensive
// data-shape report suitable for informing a synthetic data generator.
//
// Usage:
//
//	go run ./cmd/analyze -chunks /tmp/loki-bench/chunks -max-mb 10240 -tenant 29
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/grafana/loki/v3/pkg/chunkenc"
	"github.com/grafana/loki/v3/pkg/logproto"
	"github.com/grafana/loki/v3/pkg/logql/log"
	"github.com/grafana/loki/v3/pkg/storage/chunk"
	"github.com/prometheus/prometheus/model/labels"
)

const (
	maxPatterns = 50_000
	maxSMValues = 10_000
)

var (
	reNumber = regexp.MustCompile(`\b[0-9]+(\.[0-9]+)?\b`)
	reUUID   = regexp.MustCompile(`[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}`)
	reHex    = regexp.MustCompile(`\b[0-9a-f]{8,}\b`)
)

type labelAgg struct {
	values  map[string]struct{}
	streams map[string]struct{}
}

type smAgg struct {
	values         map[string]struct{}
	entriesWithKey int64
}

type formatCounts struct {
	json   int64
	logfmt int64
	plain  int64
}

type streamProfile struct {
	labels     string
	count      int64
	minTs      int64 // nanoseconds
	maxTs      int64
	fmtCounts  formatCounts
	totalLen   int64
	sampleLine string
}

type patternStats struct {
	pattern string
	count   int64
	format  string
	samples []string // max 5
}

type analysisState struct {
	// Label analysis
	labelStats map[string]*labelAgg

	// SM analysis
	smStats          map[string]*smAgg
	entriesWithAnySM int64
	totalEntries     int64

	// Volume
	streamProfiles map[string]*streamProfile
	globalMinTs    int64
	globalMaxTs    int64

	// Rate: minute-epoch -> count
	rateBuckets map[int64]int64

	// Line lengths (all entries)
	// Memory note: one int32 per log line. ~200MB for 50M lines. Acceptable for one-shot tool.
	// Fallback if OOM: replace with fixed-size histogram (200 buckets, 0-5000+ bytes).
	lineLengths []int32

	// Pattern detection
	patterns  map[string]*patternStats
	fmtCounts formatCounts // global

	// JSON key frequency: key -> count
	jsonKeyFreq map[string]int64
	jsonSampled int64 // how many JSON lines were sampled for key extraction

	// Decode metadata
	decoded  int
	failed   int
	rawBytes int64
}

func newAnalysisState() *analysisState {
	return &analysisState{
		labelStats:     make(map[string]*labelAgg),
		smStats:        make(map[string]*smAgg),
		streamProfiles: make(map[string]*streamProfile),
		rateBuckets:    make(map[int64]int64),
		lineLengths:    make([]int32, 0, 1_000_000),
		patterns:       make(map[string]*patternStats),
		jsonKeyFreq:    make(map[string]int64),
		globalMinTs:    math.MaxInt64,
	}
}

func main() {
	chunksDir := flag.String("chunks", "/tmp/loki-bench/chunks", "directory containing Loki chunk objects")
	maxMB := flag.Int("max-mb", 10240, "max MB of chunk data to process")
	tenant := flag.String("tenant", "29", "tenant ID")
	flag.Parse()

	if _, err := os.Stat(*chunksDir); err != nil {
		fmt.Fprintf(os.Stderr, "Error: chunks directory %q not found: %v\n", *chunksDir, err)
		os.Exit(1)
	}

	fmt.Fprintf(os.Stderr, "=== Loki Chunk Analyzer ===\n")
	fmt.Fprintf(os.Stderr, "Source:  %s\n", *chunksDir)
	fmt.Fprintf(os.Stderr, "Tenant:  %s\n", *tenant)
	fmt.Fprintf(os.Stderr, "Limit:   %d MB\n\n", *maxMB)

	start := time.Now()
	st := analyzeChunks(*chunksDir, *tenant, int64(*maxMB)*1024*1024)
	fmt.Fprintf(os.Stderr, "\n\nAnalysis complete in %s\n", time.Since(start).Round(time.Second))
	fmt.Fprintf(os.Stderr, "Chunks decoded: %d, failed: %d, entries: %d\n",
		st.decoded, st.failed, st.totalEntries)

	printHeader(st)
	printLabelAnalysis(st)
	printSMAnalysis(st)
	printVolumeStats(st)
	printLineRateAnalysis(st)
	printLinePatterns(st)
	printStreamProfiles(st)
}

func analyzeChunks(dir, tenant string, maxBytes int64) *analysisState {
	st := newAnalysisState()

	//nolint:errcheck
	filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			st.failed++
			return nil
		}
		if info.IsDir() {
			return nil
		}
		if st.rawBytes >= maxBytes {
			return filepath.SkipAll
		}

		data, readErr := os.ReadFile(path)
		if readErr != nil {
			st.failed++
			return nil
		}
		st.rawBytes += int64(len(data))

		// Use filepath.Base for portable path parsing (Edge Case 7)
		fname := filepath.Base(path)
		fingerprint := filepath.Base(filepath.Dir(path))
		chunkKey := fmt.Sprintf("%s/%s/%s", tenant, fingerprint, fname)

		c, decErr := chunk.ParseExternalKey(tenant, chunkKey)
		if decErr != nil {
			st.failed++
			return nil
		}
		if decErr = c.Decode(chunk.NewDecodeContext(), data); decErr != nil {
			st.failed++
			return nil
		}

		// Parse labels once per chunk — labels are constant within a chunk
		lblsStr := c.Metric.String()
		c.Metric.Range(func(lbl labels.Label) {
			la, ok := st.labelStats[lbl.Name]
			if !ok {
				la = &labelAgg{
					values:  make(map[string]struct{}),
					streams: make(map[string]struct{}),
				}
				st.labelStats[lbl.Name] = la
			}
			la.values[lbl.Value] = struct{}{}
			la.streams[lblsStr] = struct{}{}
		})

		lokiChk, ok := c.Data.(*chunkenc.Facade)
		if !ok {
			st.failed++
			return nil
		}
		it, itErr := lokiChk.LokiChunk().Iterator(
			context.Background(),
			time.Unix(0, 0),
			time.Unix(1<<50, 0),
			logproto.FORWARD,
			log.NewNoopPipeline().ForStream(labels.EmptyLabels()),
		)
		if itErr != nil {
			st.failed++
			return nil
		}

		for it.Next() {
			processEntry(st, lblsStr, it.At())
		}
		_ = it.Close()

		st.decoded++
		if st.decoded%500 == 0 {
			fmt.Fprintf(os.Stderr, "\r  decoded %d chunks, %d entries, %.0f MB processed...",
				st.decoded, st.totalEntries, float64(st.rawBytes)/1024/1024)
		}
		return nil
	})

	return st
}

func processEntry(st *analysisState, lblsStr string, entry logproto.Entry) {
	ts := entry.Timestamp.UnixNano()

	// Update stream profile
	sp, ok := st.streamProfiles[lblsStr]
	if !ok {
		sp = &streamProfile{
			labels: lblsStr,
			minTs:  math.MaxInt64,
		}
		st.streamProfiles[lblsStr] = sp
	}
	sp.count++
	if ts < sp.minTs {
		sp.minTs = ts
	}
	if ts > sp.maxTs {
		sp.maxTs = ts
	}

	// Update global time range
	if ts < st.globalMinTs {
		st.globalMinTs = ts
	}
	if ts > st.globalMaxTs {
		st.globalMaxTs = ts
	}

	// Update rate bucket (minute granularity)
	minuteEpoch := entry.Timestamp.Unix() / 60
	st.rateBuckets[minuteEpoch]++

	// Classify line format
	line := entry.Line
	format := classifyLine(line)

	switch format {
	case "json":
		st.fmtCounts.json++
		sp.fmtCounts.json++
		// JSON key-set analysis: sample ~10% of JSON lines
		if st.totalEntries%10 == 0 {
			extractJSONKeys(st, line)
		}
	case "logfmt":
		st.fmtCounts.logfmt++
		sp.fmtCounts.logfmt++
	default:
		st.fmtCounts.plain++
		sp.fmtCounts.plain++
	}

	// Line length
	lineLen := int32(len(line))
	st.lineLengths = append(st.lineLengths, lineLen)
	sp.totalLen += int64(lineLen)
	if sp.sampleLine == "" && len(line) > 0 {
		sp.sampleLine = line
	}

	// Pattern
	norm := normalizeForPattern(line)
	updatePattern(st, line, norm, format)

	// Structured metadata
	if len(entry.StructuredMetadata) > 0 {
		st.entriesWithAnySM++
		processSM(st, entry.StructuredMetadata)
	}

	st.totalEntries++
}

func processSM(st *analysisState, sms []logproto.LabelAdapter) {
	for _, sm := range sms {
		sa, ok := st.smStats[sm.Name]
		if !ok {
			sa = &smAgg{values: make(map[string]struct{})}
			st.smStats[sm.Name] = sa
		}
		sa.entriesWithKey++
		if len(sa.values) < maxSMValues {
			sa.values[sm.Value] = struct{}{}
		}
	}
}

func extractJSONKeys(st *analysisState, line string) {
	var obj map[string]json.RawMessage
	if err := json.Unmarshal([]byte(line), &obj); err != nil {
		return
	}
	for k := range obj {
		st.jsonKeyFreq[k]++
	}
	st.jsonSampled++
}

func classifyLine(line string) string {
	line = strings.TrimSpace(line)
	if len(line) == 0 {
		return "plain"
	}
	if line[0] == '{' && line[len(line)-1] == '}' {
		return "json"
	}
	if isLogfmt(line) {
		return "logfmt"
	}
	return "plain"
}

func isLogfmt(line string) bool {
	count := 0
	for _, part := range strings.Fields(line) {
		idx := strings.IndexByte(part, '=')
		if idx > 0 && idx < len(part)-1 {
			count++
			if count >= 2 {
				return true
			}
		}
	}
	return false
}

func normalizeForPattern(line string) string {
	if len(line) > 80 {
		line = line[:80]
	}
	line = reUUID.ReplaceAllString(line, "<uuid>")
	line = reHex.ReplaceAllString(line, "<hex>")
	line = reNumber.ReplaceAllString(line, "<N>")
	return line
}

func updatePattern(st *analysisState, raw, normalized, format string) {
	ps, ok := st.patterns[normalized]
	if !ok {
		if len(st.patterns) >= maxPatterns {
			return // cap reached; skip new pattern registration
		}
		ps = &patternStats{pattern: normalized, format: format}
		st.patterns[normalized] = ps
	}
	ps.count++
	if len(ps.samples) < 5 {
		ps.samples = append(ps.samples, raw)
	}
}

func percentiles(sorted []int64, pcts []float64) []int64 {
	result := make([]int64, len(pcts))
	n := len(sorted)
	if n == 0 {
		return result
	}
	for i, p := range pcts {
		idx := int(math.Ceil(p/100.0*float64(n))) - 1
		if idx < 0 {
			idx = 0
		}
		if idx >= n {
			idx = n - 1
		}
		result[i] = sorted[idx]
	}
	return result
}

func sortedInt32ToInt64(src []int32) []int64 {
	sort.Slice(src, func(i, j int) bool { return src[i] < src[j] })
	out := make([]int64, len(src))
	for i, v := range src {
		out[i] = int64(v)
	}
	return out
}

func mean(vals []int32) float64 {
	if len(vals) == 0 {
		return 0
	}
	var sum int64
	for _, v := range vals {
		sum += int64(v)
	}
	return float64(sum) / float64(len(vals))
}

func dominantFormat(fc formatCounts) string {
	if fc.json >= fc.logfmt && fc.json >= fc.plain {
		return "json"
	}
	if fc.logfmt >= fc.plain {
		return "logfmt"
	}
	return "plain"
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "..."
}

func printHeader(st *analysisState) {
	fmt.Printf("# Loki Chunk Data Shape Report\n\n")
	fmt.Printf("Generated: %s\n\n", time.Now().UTC().Format(time.RFC3339))
	fmt.Printf("## Decode Summary\n\n")
	fmt.Printf("| Metric | Value |\n")
	fmt.Printf("|---|---|\n")
	fmt.Printf("| Chunks decoded | %d |\n", st.decoded)
	fmt.Printf("| Chunks failed | %d |\n", st.failed)
	fmt.Printf("| Total log entries | %d |\n", st.totalEntries)
	fmt.Printf("| Raw bytes processed | %.1f MB |\n", float64(st.rawBytes)/1024/1024)

	if st.globalMinTs < math.MaxInt64 && st.globalMaxTs > 0 {
		minT := time.Unix(0, st.globalMinTs).UTC()
		maxT := time.Unix(0, st.globalMaxTs).UTC()
		duration := maxT.Sub(minT).Round(time.Minute)
		fmt.Printf("| Time range | %s → %s (%s) |\n",
			minT.Format(time.RFC3339), maxT.Format(time.RFC3339), duration)
	}
	fmt.Printf("\n")
}

func printLabelAnalysis(st *analysisState) {
	fmt.Printf("## 1. Label Analysis\n\n")

	type labelInfo struct {
		name        string
		cardinality int
		streamCount int
		values      []string
	}

	infos := make([]labelInfo, 0, len(st.labelStats))
	for name, la := range st.labelStats {
		vals := make([]string, 0, len(la.values))
		for v := range la.values {
			vals = append(vals, v)
		}
		sort.Strings(vals)
		infos = append(infos, labelInfo{
			name:        name,
			cardinality: len(la.values),
			streamCount: len(la.streams),
			values:      vals,
		})
	}
	sort.Slice(infos, func(i, j int) bool {
		if infos[i].cardinality != infos[j].cardinality {
			return infos[i].cardinality > infos[j].cardinality
		}
		return infos[i].name < infos[j].name
	})

	fmt.Printf("| Label Name | Cardinality | Streams | Values |\n")
	fmt.Printf("|---|---|---|---|\n")
	for _, li := range infos {
		var valStr string
		if li.cardinality <= 50 {
			valStr = strings.Join(li.values, ", ")
		} else {
			top10 := li.values[:10]
			valStr = strings.Join(top10, ", ") + fmt.Sprintf(" ... (%d total)", li.cardinality)
		}
		fmt.Printf("| %s | %d | %d | %s |\n", li.name, li.cardinality, li.streamCount, valStr)
	}
	fmt.Printf("\n")
}

func printSMAnalysis(st *analysisState) {
	fmt.Printf("## 2. Structured Metadata Analysis\n\n")

	if st.totalEntries > 0 {
		pct := float64(st.entriesWithAnySM) / float64(st.totalEntries) * 100
		fmt.Printf("Entries with any SM: %d / %d total (%.1f%%)\n\n", st.entriesWithAnySM, st.totalEntries, pct)
	} else {
		fmt.Printf("No entries processed.\n\n")
	}

	type smInfo struct {
		name        string
		cardinality int
		coverage    float64
		values      []string
	}

	smInfos := make([]smInfo, 0, len(st.smStats))
	for name, sa := range st.smStats {
		vals := make([]string, 0, len(sa.values))
		for v := range sa.values {
			vals = append(vals, v)
		}
		sort.Strings(vals)
		var cov float64
		if st.totalEntries > 0 {
			cov = float64(sa.entriesWithKey) / float64(st.totalEntries) * 100
		}
		smInfos = append(smInfos, smInfo{
			name:        name,
			cardinality: len(sa.values),
			coverage:    cov,
			values:      vals,
		})
	}
	sort.Slice(smInfos, func(i, j int) bool {
		if smInfos[i].cardinality != smInfos[j].cardinality {
			return smInfos[i].cardinality > smInfos[j].cardinality
		}
		return smInfos[i].name < smInfos[j].name
	})

	fmt.Printf("| Key | Cardinality | Coverage | Values |\n")
	fmt.Printf("|---|---|---|---|\n")
	for _, si := range smInfos {
		var valStr string
		if si.cardinality <= 50 {
			valStr = strings.Join(si.values, ", ")
		} else {
			top10 := si.values[:10]
			valStr = strings.Join(top10, ", ") + fmt.Sprintf(" ... (%d total)", si.cardinality)
		}
		fmt.Printf("| %s | %d | %.1f%% | %s |\n", si.name, si.cardinality, si.coverage, valStr)
	}
	fmt.Printf("\n")
}

func printVolumeStats(st *analysisState) {
	fmt.Printf("## 3. Volume Statistics\n\n")

	fmt.Printf("- Total log lines: %d\n", st.totalEntries)
	fmt.Printf("- Total unique streams: %d\n", len(st.streamProfiles))

	if st.globalMinTs < math.MaxInt64 && st.globalMaxTs > 0 {
		minT := time.Unix(0, st.globalMinTs).UTC()
		maxT := time.Unix(0, st.globalMaxTs).UTC()
		duration := maxT.Sub(minT).Round(time.Minute)
		fmt.Printf("- Global time range: %s → %s (%s)\n",
			minT.Format(time.RFC3339), maxT.Format(time.RFC3339), duration)
	}
	fmt.Printf("\n")

	// Lines per stream distribution
	counts := make([]int64, 0, len(st.streamProfiles))
	for _, sp := range st.streamProfiles {
		counts = append(counts, sp.count)
	}
	sort.Slice(counts, func(i, j int) bool { return counts[i] < counts[j] })

	pctLabels := []float64{0, 10, 25, 50, 75, 90, 99, 100}
	pctNames := []string{"Min", "P10", "P25", "P50", "P75", "P90", "P99", "Max"}

	var pctVals []int64
	if len(counts) > 0 {
		pctVals = percentiles(counts, pctLabels)
	} else {
		pctVals = make([]int64, len(pctLabels))
	}

	var meanVal float64
	if len(counts) > 0 {
		var sum int64
		for _, c := range counts {
			sum += c
		}
		meanVal = float64(sum) / float64(len(counts))
	}

	fmt.Printf("### Lines Per Stream Distribution\n\n")
	fmt.Printf("| Metric | Value |\n")
	fmt.Printf("|---|---|\n")
	for i, name := range pctNames {
		fmt.Printf("| %s | %d |\n", name, pctVals[i])
	}
	fmt.Printf("| Mean | %.1f |\n", meanVal)
	fmt.Printf("\n")
}

func printLineRateAnalysis(st *analysisState) {
	fmt.Printf("## 4. Line Rate Analysis (by minute)\n\n")

	if len(st.rateBuckets) == 0 {
		fmt.Printf("No rate data available.\n\n")
		return
	}

	// Sort minute epochs
	epochs := make([]int64, 0, len(st.rateBuckets))
	for e := range st.rateBuckets {
		epochs = append(epochs, e)
	}
	sort.Slice(epochs, func(i, j int) bool { return epochs[i] < epochs[j] })

	// Find gaps > 5 minutes
	gapCount := 0
	var totalGapMin int64
	for i := 1; i < len(epochs); i++ {
		diff := epochs[i] - epochs[i-1]
		if diff > 5 {
			gapCount++
			totalGapMin += diff - 1
		}
	}

	// Time span
	spanMin := epochs[len(epochs)-1] - epochs[0]
	spanHours := float64(spanMin) / 60.0

	// Peak and stats
	var peakEpoch int64
	var peakCount int64
	var totalLines int64
	minRate := int64(math.MaxInt64)

	for _, e := range epochs {
		c := st.rateBuckets[e]
		totalLines += c
		if c > peakCount {
			peakCount = c
			peakEpoch = e
		}
		if c < minRate {
			minRate = c
		}
	}

	avgRate := float64(totalLines) / float64(len(epochs))
	peakTime := time.Unix(peakEpoch*60, 0).UTC().Format("2006-01-02 15:04")

	fmt.Printf("- Total time span: %.1f hours\n", spanHours)
	fmt.Printf("- Peak rate: %d lines/min (at %s UTC)\n", peakCount, peakTime)
	fmt.Printf("- Average rate: %.1f lines/min (over %d active minutes)\n", avgRate, len(epochs))
	fmt.Printf("- Min rate (active minutes): %d lines/min\n", minRate)
	fmt.Printf("- Time gaps > 5 min: %d gaps (total gap duration: %d min)\n\n", gapCount, totalGapMin)

	// Top 10 busiest minutes
	type epochCount struct {
		epoch int64
		count int64
	}
	all := make([]epochCount, 0, len(st.rateBuckets))
	for e, c := range st.rateBuckets {
		all = append(all, epochCount{e, c})
	}
	sort.Slice(all, func(i, j int) bool { return all[i].count > all[j].count })

	fmt.Printf("### Top 10 Busiest Minutes\n\n")
	fmt.Printf("| Minute (UTC) | Lines |\n")
	fmt.Printf("|---|---|\n")
	for i := 0; i < 10 && i < len(all); i++ {
		t := time.Unix(all[i].epoch*60, 0).UTC().Format("2006-01-02 15:04")
		fmt.Printf("| %s | %d |\n", t, all[i].count)
	}
	fmt.Printf("\n")
}

func printLinePatterns(st *analysisState) {
	fmt.Printf("## 5. Log Line Pattern Analysis\n\n")

	// Line length distribution
	fmt.Printf("### Line Length Distribution\n\n")

	sortedLens := sortedInt32ToInt64(st.lineLengths)
	pctLabels := []float64{0, 10, 25, 50, 75, 90, 99, 100}
	pctNames := []string{"Min", "P10", "P25", "P50", "P75", "P90", "P99", "Max"}

	var pctVals []int64
	if len(sortedLens) > 0 {
		pctVals = percentiles(sortedLens, pctLabels)
	} else {
		pctVals = make([]int64, len(pctLabels))
	}

	fmt.Printf("| Metric | Value |\n")
	fmt.Printf("|---|---|\n")
	for i, name := range pctNames {
		fmt.Printf("| %s | %d |\n", name, pctVals[i])
	}
	fmt.Printf("| Mean | %.1f |\n", mean(st.lineLengths))
	fmt.Printf("\n")

	// Format classification
	fmt.Printf("### Format Classification\n\n")
	total := st.fmtCounts.json + st.fmtCounts.logfmt + st.fmtCounts.plain
	fmt.Printf("| Format | Count | Percentage |\n")
	fmt.Printf("|---|---|---|\n")
	if total > 0 {
		fmt.Printf("| JSON | %d | %.1f%% |\n", st.fmtCounts.json, float64(st.fmtCounts.json)/float64(total)*100)
		fmt.Printf("| logfmt | %d | %.1f%% |\n", st.fmtCounts.logfmt, float64(st.fmtCounts.logfmt)/float64(total)*100)
		fmt.Printf("| plain | %d | %.1f%% |\n", st.fmtCounts.plain, float64(st.fmtCounts.plain)/float64(total)*100)
	}
	fmt.Printf("\n")

	// JSON key frequency
	if len(st.jsonKeyFreq) > 0 {
		fmt.Printf("### JSON Key Frequency (sampled from ~10%% of JSON lines, %d samples)\n\n", st.jsonSampled)

		type keyCount struct {
			key   string
			count int64
		}
		keyList := make([]keyCount, 0, len(st.jsonKeyFreq))
		for k, c := range st.jsonKeyFreq {
			keyList = append(keyList, keyCount{k, c})
		}
		sort.Slice(keyList, func(i, j int) bool { return keyList[i].count > keyList[j].count })

		fmt.Printf("| Key | Occurrences |\n")
		fmt.Printf("|---|---|\n")
		for i := 0; i < 30 && i < len(keyList); i++ {
			fmt.Printf("| %s | %d |\n", keyList[i].key, keyList[i].count)
		}
		fmt.Printf("\n")
	}

	// Top patterns
	type patternEntry struct {
		key   string
		stats *patternStats
	}
	patList := make([]patternEntry, 0, len(st.patterns))
	for k, ps := range st.patterns {
		patList = append(patList, patternEntry{k, ps})
	}
	sort.Slice(patList, func(i, j int) bool { return patList[i].stats.count > patList[j].stats.count })

	capNote := ""
	if len(st.patterns) >= maxPatterns {
		capNote = " (pattern cap reached — some patterns not tracked)"
	}
	fmt.Printf("### Top Patterns (by frequency, showing top 30)%s\n\n", capNote)

	for i := 0; i < 30 && i < len(patList); i++ {
		ps := patList[i].stats
		var pct float64
		if st.totalEntries > 0 {
			pct = float64(ps.count) / float64(st.totalEntries) * 100
		}
		fmt.Printf("#### Pattern %d (%s, %.1f%% of lines)\n\n", i+1, ps.format, pct)
		fmt.Printf("Normalized: `%s`\n\n", truncate(ps.pattern, 120))
		if len(ps.samples) > 0 {
			fmt.Printf("Samples:\n\n")
			for _, s := range ps.samples {
				fmt.Printf("  - `%s`\n", truncate(s, 120))
			}
		}
		fmt.Printf("\n")
	}
}

func printStreamProfiles(st *analysisState) {
	fmt.Printf("## 6. Per-Stream Profiles (Top 20 by Volume)\n\n")

	type spEntry struct {
		sp *streamProfile
	}
	spList := make([]spEntry, 0, len(st.streamProfiles))
	for _, sp := range st.streamProfiles {
		spList = append(spList, spEntry{sp})
	}
	sort.Slice(spList, func(i, j int) bool { return spList[i].sp.count > spList[j].sp.count })

	fmt.Printf("| Rank | Stream Labels | Entries | Time Range | Dominant Format | Avg Line Len | Sample Line |\n")
	fmt.Printf("|---|---|---|---|---|---|---|\n")

	for i := 0; i < 20 && i < len(spList); i++ {
		sp := spList[i].sp

		var timeRange string
		if sp.minTs < math.MaxInt64 && sp.maxTs > 0 {
			minT := time.Unix(0, sp.minTs).UTC().Format("01-02 15:04")
			maxT := time.Unix(0, sp.maxTs).UTC().Format("01-02 15:04")
			timeRange = minT + " → " + maxT
		}

		domFmt := dominantFormat(sp.fmtCounts)

		var avgLen int64
		if sp.count > 0 {
			avgLen = sp.totalLen / sp.count
		}

		lbls := truncate(sp.labels, 60)
		sample := truncate(sp.sampleLine, 80)

		fmt.Printf("| %d | %s | %d | %s | %s | %d | %s |\n",
			i+1, lbls, sp.count, timeRange, domFmt, avgLen, sample)
	}
	fmt.Printf("\n")
}
