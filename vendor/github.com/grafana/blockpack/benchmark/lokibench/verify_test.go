// verify_test.go — Result comparison for benchmark correctness verification.
//
// Compares LogQL query results from Loki chunks and blockpack to verify they
// produce identical outputs. Used by BenchmarkSynthetic and BenchmarkLargeScale
// to add a correctness verification step alongside performance measurement.
package lokibench

import (
	"fmt"
	"html"
	"math"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/grafana/loki/pkg/push"
	"github.com/grafana/loki/v3/pkg/logqlmodel"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/promql/parser"
)

const (
	// fuzzyMatchRatio is the minimum fraction of entries that must match.
	// With deterministic unique timestamps in the generator, both stores always
	// return identical result sets, so we require 100% exact content match.
	fuzzyMatchRatio = 1.0
	// fuzzyTimeTolerance is the maximum nanosecond span across all mismatching
	// entry timestamps. With fuzzyMatchRatio=1.0 this path is unreachable, but
	// kept for completeness.
	fuzzyTimeTolerance = int64(time.Second)
)

// verifyFromCaptured compares chunk and blockpack results that were captured
// during benchmark iterations. This avoids extra query executions — we just
// reuse data the benchmark already produced. Sets verified/verifyMatch/verifyDetail.
func verifyFromCaptured(res *synthResult, tb testing.TB) {
	if res.chunkData == nil && res.bpData == nil {
		return // neither store produced data (e.g. both errored)
	}
	res.verified = true
	if res.chunkData == nil {
		res.verifyDetail = "chunk: no data captured"
		return
	}
	if res.bpData == nil {
		res.verifyDetail = "blockpack: no data captured"
		return
	}

	if err := compareQueryData(res.chunkData, res.bpData, 1e-5); err != nil {
		res.verifyDetail = err.Error()
		tb.Logf("[VERIFY MISMATCH] %s: %v", res.name, err)
	} else {
		res.verifyMatch = true
	}
}

// compareQueryData compares two LogQL query results for content equality.
// Log streams: compared by (timestamp, line) multiset, ignoring label attribution
// differences from Loki schema v13 StructuredMetadata promotion.
// Metric results: compared element-by-element with floating-point tolerance.
func compareQueryData(expected, actual parser.Value, tolerance float64) error {
	switch exp := expected.(type) {
	case logqlmodel.Streams:
		act, ok := actual.(logqlmodel.Streams)
		if !ok {
			return fmt.Errorf("type mismatch: expected Streams, got %T", actual)
		}
		return compareStreams(exp, act)
	case promql.Vector:
		act, ok := actual.(promql.Vector)
		if !ok {
			return fmt.Errorf("type mismatch: expected Vector, got %T", actual)
		}
		return compareVectors(exp, act, tolerance)
	case promql.Matrix:
		act, ok := actual.(promql.Matrix)
		if !ok {
			return fmt.Errorf("type mismatch: expected Matrix, got %T", actual)
		}
		return compareMatrices(exp, act, tolerance)
	case promql.Scalar:
		act, ok := actual.(promql.Scalar)
		if !ok {
			return fmt.Errorf("type mismatch: expected Scalar, got %T", actual)
		}
		return compareScalars(exp, act, tolerance)
	default:
		return fmt.Errorf("unknown result type: %T", expected)
	}
}

type verifyEntry struct {
	ts   int64
	line string
	sm   string // sorted "k=v\nk=v" serialization of StructuredMetadata
}

func compareStreams(expected, actual logqlmodel.Streams) error {
	exp := flattenStreams(expected)
	act := flattenStreams(actual)

	total := max(len(exp), len(act))
	if total == 0 {
		return nil
	}

	// Collect mismatches: walk the common prefix element-by-element, then
	// count extra entries in the longer slice.
	common := min(len(exp), len(act))
	var firstMismatch int = -1
	var diffCount int
	var diffTSs []int64
	for i := range common {
		if exp[i].ts != act[i].ts || exp[i].line != act[i].line || exp[i].sm != act[i].sm {
			if firstMismatch < 0 {
				firstMismatch = i
			}
			diffCount++
			diffTSs = append(diffTSs, exp[i].ts, act[i].ts)
		}
	}
	for i := common; i < len(exp); i++ {
		diffCount++
		diffTSs = append(diffTSs, exp[i].ts)
	}
	for i := common; i < len(act); i++ {
		diffCount++
		diffTSs = append(diffTSs, act[i].ts)
	}

	if diffCount == 0 {
		return nil // exact match
	}

	matchPct := float64(total-diffCount) / float64(total)
	if matchPct < fuzzyMatchRatio {
		// Too many mismatches — report the most useful detail.
		if firstMismatch >= 0 {
			i := firstMismatch
			if exp[i].ts != act[i].ts {
				return fmt.Errorf(
					"entry[%d]: ts mismatch: chunk=%d bp=%d (%d/%d entries differ, %.1f%% mismatch)",
					i, exp[i].ts, act[i].ts, diffCount, total, (1-matchPct)*100,
				)
			}
			if exp[i].sm != act[i].sm {
				return fmt.Errorf(
					"entry[%d]: SM mismatch:\n  chunk: %s\n  bp:    %s\n(%d/%d entries differ, %.1f%% mismatch)",
					i, exp[i].sm, act[i].sm, diffCount, total, (1-matchPct)*100,
				)
			}
			return fmt.Errorf(
				"entry[%d]: line mismatch:\n  chunk: %q\n  bp:    %q\n(%d/%d entries differ, %.1f%% mismatch)",
				i, truncStr(exp[i].line, 80), truncStr(act[i].line, 80), diffCount, total, (1-matchPct)*100,
			)
		}
		return fmt.Errorf("entry count: chunk=%d blockpack=%d (%.1f%% mismatch)", len(exp), len(act), (1-matchPct)*100)
	}

	// ≥99% match. Accept if all mismatching timestamps span ≤1s (boundary tie-breaking).
	minTS, maxTS := diffTSs[0], diffTSs[0]
	for _, ts := range diffTSs[1:] {
		if ts < minTS {
			minTS = ts
		}
		if ts > maxTS {
			maxTS = ts
		}
	}
	if maxTS-minTS > fuzzyTimeTolerance {
		return fmt.Errorf(
			"%d/%d entries differ; mismatch timestamps span %s (> 1s tolerance)",
			diffCount, total, time.Duration(maxTS-minTS).Round(time.Millisecond),
		)
	}

	// Fuzzy match: ≥99% entries match and boundary divergence is within 1s.
	return nil
}

func flattenStreams(streams logqlmodel.Streams) []verifyEntry {
	var entries []verifyEntry
	for _, s := range streams {
		for _, e := range s.Entries {
			entries = append(entries, verifyEntry{
				ts: e.Timestamp.UnixNano(), line: e.Line, sm: serializeSM(e.StructuredMetadata),
			})
		}
	}
	sort.Slice(entries, func(i, j int) bool {
		if entries[i].ts != entries[j].ts {
			return entries[i].ts < entries[j].ts
		}
		if entries[i].line != entries[j].line {
			return entries[i].line < entries[j].line
		}
		return entries[i].sm < entries[j].sm
	})
	return entries
}

func compareVectors(expected, actual promql.Vector, tol float64) error {
	if len(expected) != len(actual) {
		return fmt.Errorf("vector length: chunk=%d blockpack=%d", len(expected), len(actual))
	}
	sortVec := func(v promql.Vector) {
		sort.Slice(v, func(i, j int) bool {
			return v[i].Metric.String() < v[j].Metric.String()
		})
	}
	sortVec(expected)
	sortVec(actual)
	for i := range expected {
		e, a := expected[i], actual[i]
		if e.Metric.String() != a.Metric.String() {
			return fmt.Errorf("vector[%d]: labels: chunk=%s bp=%s", i, e.Metric, a.Metric)
		}
		if e.T != a.T {
			return fmt.Errorf("vector[%d]: ts: chunk=%d bp=%d", i, e.T, a.T)
		}
		if math.Abs(e.F-a.F) > tol {
			return fmt.Errorf(
				"vector[%d]: value: chunk=%g bp=%g (delta=%g)",
				i, e.F, a.F, math.Abs(e.F-a.F),
			)
		}
	}
	return nil
}

func compareMatrices(expected, actual promql.Matrix, tol float64) error {
	if len(expected) != len(actual) {
		return fmt.Errorf("series count: chunk=%d blockpack=%d", len(expected), len(actual))
	}
	sortMat := func(m promql.Matrix) {
		sort.Slice(m, func(i, j int) bool {
			return m[i].Metric.String() < m[j].Metric.String()
		})
	}
	sortMat(expected)
	sortMat(actual)
	for i := range expected {
		e, a := expected[i], actual[i]
		if e.Metric.String() != a.Metric.String() {
			return fmt.Errorf("series[%d]: labels: chunk=%s bp=%s", i, e.Metric, a.Metric)
		}
		if len(e.Floats) != len(a.Floats) {
			return fmt.Errorf(
				"series[%d] %s: points: chunk=%d bp=%d",
				i, e.Metric, len(e.Floats), len(a.Floats),
			)
		}
		for j := range e.Floats {
			ep, ap := e.Floats[j], a.Floats[j]
			if ep.T != ap.T {
				return fmt.Errorf(
					"series[%d] point[%d]: ts: chunk=%d bp=%d", i, j, ep.T, ap.T,
				)
			}
			if math.Abs(ep.F-ap.F) > tol {
				return fmt.Errorf(
					"series[%d] point[%d]: value: chunk=%g bp=%g (delta=%g)",
					i, j, ep.F, ap.F, math.Abs(ep.F-ap.F),
				)
			}
		}
	}
	return nil
}

func compareScalars(expected, actual promql.Scalar, tol float64) error {
	if expected.T != actual.T {
		return fmt.Errorf("scalar ts: chunk=%d bp=%d", expected.T, actual.T)
	}
	if math.Abs(expected.V-actual.V) > tol {
		return fmt.Errorf(
			"scalar value: chunk=%g bp=%g (delta=%g)",
			expected.V, actual.V, math.Abs(expected.V-actual.V),
		)
	}
	return nil
}

// serializeSM encodes push.LabelsAdapter as a sorted "name=value\nname=value" string
// for stable equality comparison. Empty SM serializes to "".
func serializeSM(sm push.LabelsAdapter) string {
	if len(sm) == 0 {
		return ""
	}
	pairs := make([]string, len(sm))
	for i, lv := range sm {
		pairs[i] = lv.Name + "=" + lv.Value
	}
	sort.Strings(pairs)
	return strings.Join(pairs, "\n")
}

func truncStr(s string, maxLen int) string {
	if len(s) <= maxLen {
		return s
	}
	return s[:maxLen] + "..."
}

// --- HTML report helpers for verification ---

// verifyCell returns an HTML table cell for the verification status.
func verifyCell(r synthResult) string {
	if !r.verified {
		return `<td style="text-align: center; color: #999;">—</td>`
	}
	if r.verifyMatch {
		return `<td style="text-align: center; color: #27ae60; font-weight: bold;">` +
			`&#10003;</td>`
	}
	return fmt.Sprintf(
		`<td style="text-align: center; color: #e74c3c; font-weight: bold;" title="%s">`+
			`&#10007;</td>`,
		html.EscapeString(r.verifyDetail),
	)
}

// verifySummary counts matched, mismatched, and skipped queries.
func verifySummary(results []synthResult) (matched, mismatched, skipped int) {
	for _, r := range results {
		switch {
		case !r.verified:
			skipped++
		case r.verifyMatch:
			matched++
		default:
			mismatched++
		}
	}
	return
}

// writeVerifySummaryHTML writes the verification summary line into the report.
func writeVerifySummaryHTML(w func(string), results []synthResult) {
	matched, mismatched, skipped := verifySummary(results)
	total := matched + mismatched + skipped
	var status string
	if mismatched == 0 {
		status = fmt.Sprintf(
			`<span style="color: #27ae60; font-weight: bold;">%d/%d queries match</span>`,
			matched, total,
		)
	} else {
		status = fmt.Sprintf(
			`<span style="color: #27ae60;">%d matched</span>, `+
				`<span style="color: #e74c3c; font-weight: bold;">%d mismatched</span>`,
			matched, mismatched,
		)
	}
	if skipped > 0 {
		status += fmt.Sprintf(`, <span style="color: #999;">%d skipped</span>`, skipped)
	}
	w(fmt.Sprintf(
		`<div class="summary-stat"><strong>Verification:</strong> %s</div>`, status,
	))
}

// writeVerifyMismatchSection writes an HTML section listing verification mismatches.
func writeVerifyMismatchSection(w func(string), id string, results []synthResult) {
	var mismatches []synthResult
	for _, r := range results {
		if r.verified && !r.verifyMatch {
			mismatches = append(mismatches, r)
		}
	}
	if len(mismatches) == 0 {
		return
	}
	w(fmt.Sprintf(
		`<button type="button" class="section-header collapsible" id="%s"`+
			` onclick="toggleSection(this)" aria-expanded="false"`+
			` aria-controls="%s-content" style="background: #c0392b;">`+"\n"+
			"\t"+`<span class="chevron" style="transform:rotate(-90deg)">&#9660;</span>`+
			` Verification Mismatches (%d queries)`+"\n"+
			`</button>`+"\n"+
			`<div class="collapsible-content" id="%s-content" aria-hidden="true" style="display:none;">`+"\n",
		id, id, len(mismatches), id,
	))
	w("<table>\n<thead><tr>\n\t<th>Query</th><th>Kind</th><th>Detail</th>\n</tr></thead>\n<tbody>\n")
	for _, r := range mismatches {
		w(fmt.Sprintf(
			"<tr><td class=\"query-name\">%s</td><td>%s</td>"+
				"<td style=\"font-family: monospace; font-size: 12px; color: #e74c3c; "+
				"white-space: pre-wrap; max-width: 800px;\">%s</td></tr>\n",
			html.EscapeString(r.name),
			r.kind,
			html.EscapeString(r.verifyDetail),
		))
	}
	w("</tbody></table>\n</div>\n")
}
