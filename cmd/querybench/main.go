// querybench runs TraceQL search queries against two Tempo endpoints concurrently,
// comparing result correctness and reporting latency side-by-side.
package main

import (
	"cmp"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"slices"
	"sort"
	"strings"
	"sync"
	"time"
)

// queryDef matches the format of blockpack-queries.json.
type queryDef struct {
	ID         int    `json:"id"`
	Label      string `json:"label"`
	Complexity string `json:"complexity"`
	Query      string `json:"query"`
}

type queryFile struct {
	Start   int64         `json:"start"`
	End     int64         `json:"end"`
	Since   string        `json:"since"` // e.g. "1h", "30m" — if set, overrides Start/End with [now-since, now]
	Limit   int           `json:"limit"`
	Queries []queryDef    `json:"queries"`
}

// resolveTimeRange returns the start/end to use for queries.
// If Since is set it overrides the static Start/End fields.
func (qf *queryFile) resolveTimeRange() (start, end int64) {
	if qf.Since != "" {
		d, err := time.ParseDuration(qf.Since)
		if err == nil {
			end = time.Now().Unix()
			start = end - int64(d.Seconds())
			return
		}
	}
	return qf.Start, qf.End
}

// searchResponse is the subset of Tempo's /api/search response we care about.
type searchResponse struct {
	Traces []traceResult `json:"traces"`
}

type traceResult struct {
	TraceID           string `json:"traceID"`
	RootServiceName   string `json:"rootServiceName"`
	RootTraceName     string `json:"rootTraceName"`
	StartTimeUnixNano string `json:"startTimeUnixNano"`
	DurationMs        uint32 `json:"durationMs"`
}

type result struct {
	latency time.Duration
	resp    *searchResponse
	err     error
}

func query(endpoint, q string, start, end int64, limit int) result {
	u, _ := url.Parse(endpoint + "/api/search")
	params := url.Values{}
	params.Set("q", q)
	params.Set("start", fmt.Sprintf("%d", start))
	params.Set("end", fmt.Sprintf("%d", end))
	params.Set("limit", fmt.Sprintf("%d", limit))
	u.RawQuery = params.Encode()

	t0 := time.Now()
	resp, err := http.Get(u.String()) //nolint:noctx
	latency := time.Since(t0)
	if err != nil {
		return result{latency: latency, err: err}
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return result{latency: latency, err: fmt.Errorf("read body: %w", err)}
	}
	if resp.StatusCode != http.StatusOK {
		return result{latency: latency, err: fmt.Errorf("HTTP %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))}
	}

	var sr searchResponse
	if err := json.Unmarshal(body, &sr); err != nil {
		return result{latency: latency, err: fmt.Errorf("decode: %w", err)}
	}
	return result{latency: latency, resp: &sr}
}

func p50(times []time.Duration) time.Duration {
	s := make([]time.Duration, len(times))
	copy(s, times)
	sort.Slice(s, func(i, j int) bool { return s[i] < s[j] })
	return s[len(s)/2]
}

// traceIDs returns a sorted slice of trace IDs from a response.
func traceIDs(sr *searchResponse) []string {
	if sr == nil {
		return nil
	}
	ids := make([]string, len(sr.Traces))
	for i, t := range sr.Traces {
		ids[i] = t.TraceID
	}
	slices.Sort(ids)
	return ids
}

// startNano returns the StartTimeUnixNano for a trace as an int64 (0 on parse error).
func startNano(t traceResult) int64 {
	if t.StartTimeUnixNano == "" {
		return 0
	}
	// Parse as int64; ignore error (returns 0).
	var n int64
	fmt.Sscanf(t.StartTimeUnixNano, "%d", &n)
	return n
}

// oldestStartNano returns the oldest (minimum) start time across all traces, in nanoseconds.
func oldestStartNano(sr *searchResponse) int64 {
	if sr == nil || len(sr.Traces) == 0 {
		return 0
	}
	min := startNano(sr.Traces[0])
	for _, t := range sr.Traces[1:] {
		if n := startNano(t); n < min {
			min = n
		}
	}
	return min
}

// compareResults returns a human-readable diff summary, or "" if results match.
// When trace IDs differ, it also checks whether the disagreement is a legitimate tie
// (both engines returned traces with the same start time), a real ordering problem
// (one engine returned a newer trace the other missed — [GAP]), or a definite miss
// ([MISS]: B hit the limit but its newest result is older than a trace found only by A).
func compareResults(a, b *searchResponse, limit int) string {
	aIDs := traceIDs(a)
	bIDs := traceIDs(b)

	aCount := len(aIDs)
	bCount := len(bIDs)

	if slices.Equal(aIDs, bIDs) {
		return ""
	}

	// Build ID→trace maps for start-time lookup.
	aMap := make(map[string]traceResult, aCount)
	bMap := make(map[string]traceResult, bCount)
	if a != nil {
		for _, t := range a.Traces {
			aMap[t.TraceID] = t
		}
	}
	if b != nil {
		for _, t := range b.Traces {
			bMap[t.TraceID] = t
		}
	}

	// bNewest is the maximum startNano across all B results (0 if B empty or nil).
	var bNewest int64
	if b != nil {
		for _, t := range b.Traces {
			if n := startNano(t); n > bNewest {
				bNewest = n
			}
		}
	}

	aSet := make(map[string]bool, aCount)
	bSet := make(map[string]bool, bCount)
	for _, id := range aIDs {
		aSet[id] = true
	}
	for _, id := range bIDs {
		bSet[id] = true
	}

	var onlyA, onlyB []string
	for id := range aSet {
		if !bSet[id] {
			onlyA = append(onlyA, id)
		}
	}
	for id := range bSet {
		if !aSet[id] {
			onlyB = append(onlyB, id)
		}
	}
	slices.SortFunc(onlyA, func(x, y string) int {
		return cmp.Compare(startNano(aMap[x]), startNano(aMap[y]))
	})
	slices.SortFunc(onlyB, func(x, y string) int {
		return cmp.Compare(startNano(bMap[x]), startNano(bMap[y]))
	})

	// Determine if the diff is a tie (same start times) or a real ordering gap.
	// The oldest result in each set determines the "cutoff" — traces below both cutoffs
	// are within the legitimate tie zone. Traces above the other set's cutoff are real gaps.
	aOldest := oldestStartNano(a)
	bOldest := oldestStartNano(b)
	cutoff := max(aOldest, bOldest) // the higher of the two oldest — any result older than this in one set is a real miss

	realGap := false
	for _, id := range onlyA {
		if startNano(aMap[id]) >= cutoff {
			realGap = true
			break
		}
	}
	if !realGap {
		for _, id := range onlyB {
			if startNano(bMap[id]) >= cutoff {
				realGap = true
				break
			}
		}
	}

	// definiteMiss fires when B hit the limit but its newest result is older than a
	// trace found exclusively by A. This is an unambiguous correctness failure:
	// B had capacity for a newer trace but returned an older one instead.
	definiteMiss := false
	if limit > 0 && b != nil && len(b.Traces) >= limit && bNewest > 0 {
		for _, id := range onlyA {
			ts := startNano(aMap[id])
			if ts > 0 && ts > bNewest {
				definiteMiss = true
				break
			}
		}
	}

	diffKind := "tie"
	if definiteMiss {
		diffKind = "MISS"
	} else if realGap {
		diffKind = "GAP"
	}

	parts := []string{fmt.Sprintf("count: a=%d b=%d [%s]", aCount, bCount, diffKind)}

	fmtWithTime := func(ids []string, m map[string]traceResult) string {
		shown := ids
		suffix := ""
		if len(shown) > 2 {
			shown = shown[:2]
			suffix = fmt.Sprintf(" +%d more", len(ids)-2)
		}
		strs := make([]string, len(shown))
		for i, id := range shown {
			ns := startNano(m[id])
			prefix := id
		if len(prefix) > 16 {
			prefix = prefix[:16]
		}
		strs[i] = fmt.Sprintf("%s@%dms", prefix, ns/1e6)
		}
		return strings.Join(strs, ",") + suffix
	}

	if len(onlyA) > 0 {
		parts = append(parts, "only-a: "+fmtWithTime(onlyA, aMap))
	}
	if len(onlyB) > 0 {
		parts = append(parts, "only-b: "+fmtWithTime(onlyB, bMap))
	}
	return strings.Join(parts, "; ")
}

func main() {
	var (
		aEndpoint string
		bEndpoint string
		aLabel    string
		bLabel    string
		queryPath string
		runs      int
		warmup    bool
	)

	var mostRecent bool
	flag.StringVar(&aEndpoint, "a", "http://localhost:13200", "First Tempo endpoint (parquet)")
	flag.StringVar(&bEndpoint, "b", "http://localhost:13201", "Second Tempo endpoint (blockpack)")
	flag.StringVar(&aLabel, "a-label", "A", "Label for first endpoint")
	flag.StringVar(&bLabel, "b-label", "B", "Label for second endpoint")
	flag.StringVar(&queryPath, "queries", "", "Path to queries JSON file")
	flag.IntVar(&runs, "runs", 6, "Number of runs per query per endpoint")
	flag.BoolVar(&warmup, "warmup", true, "Run one warmup query before benchmarking")
	flag.BoolVar(&mostRecent, "most-recent", false, "Append 'with (most_recent=true)' to all queries")
	flag.Parse()

	if queryPath == "" {
		fmt.Fprintln(os.Stderr, "error: --queries is required")
		flag.Usage()
		os.Exit(1)
	}

	data, err := os.ReadFile(queryPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error reading query file: %v\n", err)
		os.Exit(1)
	}
	var qf queryFile
	if err := json.Unmarshal(data, &qf); err != nil {
		fmt.Fprintf(os.Stderr, "error parsing query file: %v\n", err)
		os.Exit(1)
	}

	if warmup {
		fmt.Printf("Warming up %s and %s...\n", aLabel, bLabel)
		wStart, wEnd := qf.resolveTimeRange()
		var wg sync.WaitGroup
		wg.Add(2)
		go func() { defer wg.Done(); query(aEndpoint, "{}", wStart, wEnd, 1) }()
		go func() { defer wg.Done(); query(bEndpoint, "{}", wStart, wEnd, 1) }()
		wg.Wait()
		time.Sleep(500 * time.Millisecond)
	}

	aLabelW := max(len(aLabel), 6)
	bLabelW := max(len(bLabel), 6)
	ratioW := 6

	header := fmt.Sprintf("%-4s %-12s %-44s | %*s | %*s | %*s | %s",
		"ID", "Complexity", "Label",
		aLabelW, aLabel,
		bLabelW, bLabel,
		ratioW, "B/A",
		"Match",
	)
	sep := strings.Repeat("-", len(header)+4)
	fmt.Println(header)
	fmt.Println(sep)

	mismatches := 0
	errors := 0

	for _, q := range qf.Queries {
		aTimes := make([]time.Duration, 0, runs)
		bTimes := make([]time.Duration, 0, runs)
		var lastA, lastB result

		qStr := q.Query
		if mostRecent && !strings.Contains(qStr, "most_recent") {
			qStr += " with (most_recent=true)"
		}

		for i := range runs {
			start, end := qf.resolveTimeRange()
			var ra, rb result
			var wg sync.WaitGroup
			wg.Add(2)
			go func() {
				defer wg.Done()
				ra = query(aEndpoint, qStr, start, end, qf.Limit)
			}()
			go func() {
				defer wg.Done()
				rb = query(bEndpoint, qStr, start, end, qf.Limit)
			}()
			wg.Wait()

			aTimes = append(aTimes, ra.latency)
			bTimes = append(bTimes, rb.latency)
			if i == runs-1 {
				lastA, lastB = ra, rb
			}
		}

		aP50 := p50(aTimes)
		bP50 := p50(bTimes)

		// Determine status
		matchStr := "ok"
		if lastA.err != nil || lastB.err != nil {
			matchStr = "ERR"
			errors++
		} else {
			diff := compareResults(lastA.resp, lastB.resp, qf.Limit)
			if diff != "" {
				matchStr = fmt.Sprintf("DIFF(%s)", diff)
				mismatches++
			}
		}

		ratio := float64(bP50) / float64(cmp.Or(aP50, 1))
		faster := ""
		if ratio < 1.0 {
			faster = " ←"
		}

		errInfo := ""
		if lastA.err != nil {
			errInfo += fmt.Sprintf(" a-err:%v", lastA.err)
		}
		if lastB.err != nil {
			errInfo += fmt.Sprintf(" b-err:%v", lastB.err)
		}

		fmt.Printf("%-4d %-12s %-44s | %*s | %*s | %*.2fx%s | %s%s\n",
			q.ID, q.Complexity, q.Label,
			aLabelW, fmt.Sprintf("%dms", aP50.Milliseconds()),
			bLabelW, fmt.Sprintf("%dms", bP50.Milliseconds()),
			ratioW-1, ratio,
			faster,
			matchStr,
			errInfo,
		)
	}

	fmt.Println(sep)
	fmt.Printf("Queries: %d  Mismatches: %d  Errors: %d\n", len(qf.Queries), mismatches, errors)
	if mismatches > 0 || errors > 0 {
		os.Exit(1)
	}
}
