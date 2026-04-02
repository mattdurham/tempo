// pprof_worst_test.go — CPU profiles for the worst-performing 1GB benchmark queries.
//
// These queries were identified by running BenchmarkLogsFull against the 1GB OTLP dataset
// and sorting by (bp_wall+bp_cpu) / (chunk_wall+chunk_cpu) ratio. The top losers fall
// into two clear categories: logfmt parse+filter chains (5–11x) and numeric pushdown (5–9x).
//
// # Usage
//
//	cd benchmark/lokibench
//	LOGFILTER_DIR=testdata/logfilter \
//	  go test -run TestPprofWorstCases -v -timeout 600s ./...
//
// Profiles are written to profiles/worst/. View with:
//
//	go tool pprof -http=:6060 profiles/worst/logfmt.cpu.pprof
//	go tool pprof -http=:6061 profiles/worst/numeric.cpu.pprof
//	go tool pprof -http=:6062 profiles/worst/multilabel.cpu.pprof
//
// # Using existing 1GB data from another worktree
//
//	LOGFILTER_DIR=/home/matt/source/blockpack-worktrees/perf_again/benchmark/lokibench/testdata/logfilter \
//	  go test -run TestPprofWorstCases -v -timeout 600s ./...
package lokibench

import (
	"os"
	"path/filepath"
	"runtime/pprof"
	"testing"
	"time"

	"github.com/grafana/blockpack"
)

// worstQuery is a named query identified as a worst-case loser vs Loki chunks.
type worstQuery struct {
	name  string
	query string
}

// Worst-performing queries from the 1GB BenchmarkLogsFull run.
// Ratios are (bp Wall+CPU ms) / (chunk Wall+CPU ms).
var (
	// logfmtWorstQueries: T7/T8/T9/T10 — logfmt parse + field filter chains.
	// Ratio range: 3.7x – 10.6x.
	logfmtWorstQueries = []worstQuery{
		{
			"T10/Q68/namespace+env+logfmt+component-api",
			`{namespace="namespace-0", env="prod"} | logfmt | component="api"`,
		}, // 10.6x
		{
			"T7/Q54/env+region+logfmt+component-auth",
			`{env="prod", region="us-east-1"} | logfmt | component="auth"`,
		}, // 6.7x
		{
			"T7/Q53/cluster+logfmt+component-cache",
			`{cluster="cluster-0"} | logfmt | component="cache"`,
		}, // 5.9x
		{
			"T10/Q70/namespace-or+env+logfmt+component-db+level-error",
			`{namespace=~"namespace-0|namespace-1", env="prod"} | logfmt | component="db" | level="error"`,
		}, // 5.7x
		{
			"T7/Q55/dc+logfmt+component-scheduler",
			`{datacenter="dc1"} | logfmt | component="scheduler"`,
		}, // 4.8x
		{
			"T9/Q65/env+region+logfmt+component-regex+level-error",
			`{env="prod", region="us-east-1"} | logfmt | component=~"api|auth|proxy" | level="error"`,
		}, // 5.0x
		{
			"T8/Q59/cluster+logfmt+component-cache+level-warn",
			`{cluster="cluster-0"} | logfmt | component="cache" | level="warn"`,
		}, // 4.1x
	}

	// numericWorstQueries: T12 — numeric field pushdown (latency_ms comparisons).
	// Ratio range: 4.9x – 8.9x.
	numericWorstQueries = []worstQuery{
		{
			"T12/Q89/numeric/env+region+latency-gt-4000",
			`{env="prod", region="us-east-1"} | latency_ms > 4000`,
		}, // 8.9x
		{
			"T12/Q86/numeric/env+latency-gt-4500",
			`{env="prod"} | latency_ms > 4500`,
		}, // 8.4x
		{
			"T12/Q90/numeric/env+latency-gt-4000+detected-error",
			`{env="prod"} | latency_ms > 4000 | detected_level="error"`,
		}, // 5.1x
		{
			"T12/Q88/numeric/cluster+latency-gt-4000",
			`{cluster="cluster-0"} | latency_ms > 4000`,
		}, // 4.9x
	}

	// multiLabelWorstQueries: T2/T3 — basic multi-label selectors and content filters.
	// These should be fast but are still 3.5–5x slower; profiling may reveal selector overhead.
	multiLabelWorstQueries = []worstQuery{
		{"T2/Q14/env+region+datacenter", `{env="staging", region="us-west-2", datacenter="dc2"}`}, // 5.1x
		{"T3/Q18/env+region+error", `{env="prod", region="us-east-1"} |= "error"`},                // 5.4x
	}
)

// TestPprofWorstCases profiles the worst-performing blockpack queries against the 1GB dataset.
// Requires RUN_PPROF_WORST=1 and the dataset to exist at LOGFILTER_DIR (default: testdata/logfilter).
// Profiles are written to profiles/worst/.
func TestPprofWorstCases(t *testing.T) {
	if os.Getenv("RUN_PPROF_WORST") != "1" {
		t.Skip("set RUN_PPROF_WORST=1 to run CPU profiling harness")
	}
	dir := os.Getenv("LOGFILTER_DIR")
	if dir == "" {
		dir = logfilterDefaultDir
	}
	bpPath := filepath.Join(dir, "blockpack.logs")
	if _, err := os.Stat(bpPath); os.IsNotExist(err) {
		t.Skipf("1GB blockpack not found at %s — set LOGFILTER_DIR or run BenchmarkLogsFull first", bpPath)
	}

	profileDir := "profiles/worst"
	if err := os.MkdirAll(profileDir, 0o755); err != nil {
		t.Fatalf("create profile dir: %v", err)
	}

	// Load without IO latency — we want pure CPU cost, not simulated network.
	bpReader, err := openBlockpackReader(bpPath)
	if err != nil {
		t.Fatalf("open blockpack reader: %v", err)
	}
	t.Logf("Loaded 1GB blockpack: %d blocks, file=%s", bpReader.BlockCount(), bpPath)

	queryStart := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	queryEnd := queryStart.Add(logfilterDefaultWindow)
	bpOpts := blockpack.LogQueryOptions{
		StartNano: uint64(queryStart.UnixNano()), //nolint:gosec
		EndNano:   uint64(queryEnd.UnixNano()),   //nolint:gosec
	}

	groups := []struct {
		name    string
		queries []worstQuery
	}{
		{"logfmt", logfmtWorstQueries},
		{"numeric", numericWorstQueries},
		{"multilabel", multiLabelWorstQueries},
	}

	for _, g := range groups {
		g := g
		t.Run(g.name, func(t *testing.T) {
			profilePath := filepath.Join(profileDir, g.name+".cpu.pprof")
			f, err := os.Create(profilePath)
			if err != nil {
				t.Fatalf("create profile file: %v", err)
			}
			defer f.Close() //nolint:errcheck

			if err := pprof.StartCPUProfile(f); err != nil {
				t.Fatalf("start cpu profile: %v", err)
			}

			for _, q := range g.queries {
				t.Logf("running %s: %s", q.name, q.query)
				var lines int64
				// Run each query 3x to get stable profile samples.
				for range 3 {
					matches, err := blockpack.QueryLogQL(bpReader, q.query, bpOpts)
					if err != nil {
						t.Errorf("query %s failed: %v", q.name, err)
					}
					lines = int64(len(matches)) //nolint:gosec
				}
				t.Logf("  → %d lines", lines)
			}

			pprof.StopCPUProfile()
			t.Logf("Profile written to %s", profilePath)
		})
	}
}
