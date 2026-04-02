package lokibench

// BenchmarkLogFilter: 80 pure LogQL filter queries (no metrics, no unwrap) in
// increasing complexity against OTLP-native log data.
//
// Tiers:
//   - T1 (Q01–Q08):  single-label stream selector
//   - T2 (Q09–Q14):  multi-label AND stream selector
//   - T3 (Q15–Q22):  stream selector + content filter (|=, !=, |~)
//   - T4 (Q23–Q32):  stream selector + field filter (detected_level, level)
//   - T5 (Q33–Q40):  OR stream selectors
//   - T6 (Q41–Q50):  nested AND/OR + multi-stage pipelines
//   - T7 (Q51–Q56):  logfmt + single body-field filter (component)
//   - T8 (Q57–Q63):  logfmt + two body-field AND (component + level, high selectivity)
//   - T9 (Q64–Q67):  logfmt + three-stage pipeline chains (maximum selectivity)
//   - T10 (Q68–Q70): namespace stream selector + body-field filters (high-cardinality resource dim)
//   - T11 (Q71–Q80): instance_id high-cardinality field (200 values, ~0.5% selectivity)
//
// Data is generated as OTLP and written directly to blockpack (no conversion).
// For the Loki chunk store comparison, data is converted OTLP→Loki at generation time.
// Data is persisted to testdata/logfilter/ (configurable via LOGFILTER_DIR).
// On subsequent runs the existing data is reused — delete the directory to regenerate.
//
// # Usage
//
//	cd benchmark/lokibench
//	go test -run '^$' -bench BenchmarkLogFilter -benchtime=1x -timeout 1800s -count=1 -v ./...
//
// # Tuning via environment variables
//
//	LOGFILTER_DIR=testdata/logfilter    # data directory
//	LOGFILTER_REPORT=path.html          # HTML report output path
//	LOGFILTER_IO_LATENCY=10ms           # per-IO latency simulation (default: 0 = no latency)
//
// Metrics reported per sub-benchmark:
//
//	ios           — actual object-storage read calls (from latencyProvider.IOCount / latencyObjectClient.IOCount)
//	linesReturned — log lines returned after all pipeline filters
//	cpuMs         — LogQL engine CPU time in ms

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/blockpack"
	"github.com/grafana/dskit/user"
	"github.com/grafana/loki/pkg/push"
	"github.com/grafana/loki/v3/pkg/logproto"
	"github.com/grafana/loki/v3/pkg/logql"
	"github.com/grafana/loki/v3/pkg/logql/bench"
	"github.com/grafana/loki/v3/pkg/logqlmodel"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

const (
	logfilterReportEnv     = "LOGFILTER_REPORT"
	logfilterReportDefault = "../lokibench-logs.html"

	logfilterDefaultDir    = "testdata/logfilter"
	logfilterDefaultDataMB = 1024
	logfilterDefaultWindow = 24 * time.Hour
)

// logfilterIOLatency returns the per-IO latency for filter benchmarks.
// Reads LOGFILTER_IO_LATENCY env var; defaults to 0 (no latency) so GHA
// correctness runs complete quickly. Set LOGFILTER_IO_LATENCY=10ms for
// production-like bench-logs-full runs.
func logfilterIOLatency() time.Duration {
	if v := os.Getenv("LOGFILTER_IO_LATENCY"); v != "" {
		if d, err := time.ParseDuration(v); err == nil && d >= 0 {
			return d
		}
	}
	return 0
}

// logFilterQuery is a named LogQL filter query for the benchmark.
type logFilterQuery struct {
	name  string
	query string
}

// lokiQueryOverrides maps benchmark query names to their Loki-compatible
// equivalents. Entries here are used for the Loki chunk-store sub-benchmark
// instead of the blockpack query string when blockpack uses syntax that Loki
// does not support (e.g. pipe-OR "| a=x or a=y" vs Loki regex "| a=~"x|y"").
var lokiQueryOverrides = map[string]string{
	"T12/Q81/or-pushdown/detected-error-or-warn":         `{env="prod"} | detected_level=~"error|warn"`,
	"T12/Q82/or-pushdown/level-error-or-warn":            `{env="prod"} | level=~"error|warn"`,
	"T12/Q83/or-pushdown/cluster+detected-error-or-warn": `{cluster="cluster-0"} | detected_level=~"error|warn"`,
	"T12/Q84/or-pushdown/env+region+level-error-or-warn": `{env="prod", region="us-east-1"} | level=~"error|warn"`,
	"T12/Q85/or-pushdown/detected-info-or-debug":         `{env="prod"} | detected_level=~"info|debug"`,

	// component is a logfmt body field (not a LogRecord attribute), so Loki requires
	// | logfmt to parse it from the body before a field filter can match it.
	// blockpack stores component as log.component column and evaluates it natively.
	"T4/Q32/env+cluster+level+nonempty":                 `{env="prod", cluster="cluster-0"} | level="error" | logfmt | component!=""`,
	"T11/Q76/env+instance-100+component-api":            `{env="prod"} | instance_id="instance-100" | logfmt | component="api"`,
	"T11/Q78/env+instance-042+level-error+component-db": `{env="prod"} | instance_id="instance-042" | level="error" | logfmt | component="db"`,
}

// logFilterQueries holds 80 pure filter queries in increasing complexity.
// Label values match bench.DefaultOpt() generated data:
//
//	cluster:    cluster-0 … cluster-4
//	env:        prod, staging, dev
//	region:     us-east-1, us-west-2, eu-west-1, ap-southeast-1
//	datacenter: dc1, dc2, dc3
//
// Content patterns:
//
//	detected_level: structured metadata set on every line (error/warn/info/debug)
//	logfmt level:   present on logfmt-format lines
var logFilterQueries = []logFilterQuery{
	// --- Tier 1: single-label stream selector ---
	{"T1/Q01/env=prod", `{env="prod"}`},
	{"T1/Q02/env=staging", `{env="staging"}`},
	{"T1/Q03/region=us-east-1", `{region="us-east-1"}`},
	{"T1/Q04/region=ap-southeast-1", `{region="ap-southeast-1"}`},
	{"T1/Q05/cluster=cluster-0", `{cluster="cluster-0"}`},
	{"T1/Q06/cluster=cluster-4", `{cluster="cluster-4"}`},
	{"T1/Q07/datacenter=dc1", `{datacenter="dc1"}`},
	{"T1/Q08/datacenter=dc3", `{datacenter="dc3"}`},

	// --- Tier 2: multi-label AND stream selector ---
	{"T2/Q09/env+region", `{env="prod", region="us-east-1"}`},
	{"T2/Q10/env+cluster", `{env="prod", cluster="cluster-0"}`},
	{"T2/Q11/env+region-eu", `{env="staging", region="eu-west-1"}`},
	{"T2/Q12/cluster+datacenter", `{cluster="cluster-0", datacenter="dc1"}`},
	{"T2/Q13/env+region+cluster", `{env="prod", region="us-east-1", cluster="cluster-0"}`},
	{"T2/Q14/env+region+datacenter", `{env="staging", region="us-west-2", datacenter="dc2"}`},

	// --- Tier 3: stream selector + content filter ---
	{"T3/Q15/env-prod+error", `{env="prod"} |= "error"`},
	{"T3/Q16/env-prod-not-debug", `{env="prod"} != "debug"`},
	{"T3/Q17/cluster+error", `{cluster="cluster-0"} |= "error"`},
	{"T3/Q18/env+region+error", `{env="prod", region="us-east-1"} |= "error"`},
	{"T3/Q19/env+regex-error-warn", `{env="prod"} |~ "error|warn"`},
	{"T3/Q20/env+case-insensitive-error", `{env="prod"} |~ "(?i)error"`},
	{"T3/Q21/cluster+error-not-timeout", `{cluster="cluster-0"} |= "error" != "timeout"`},
	{"T3/Q22/env+cluster+two-content", `{env="prod", cluster="cluster-0"} |= "error" |= "failed"`},

	// --- Tier 4: stream selector + field filter ---
	// detected_level and level are stored as LogRecord attributes (log.detected_level,
	// log.level columns) so no parse stage is needed — both are native column scans.
	{"T4/Q23/env+detected-error", `{env="prod"} | detected_level="error"`},
	{"T4/Q24/env+detected-warn", `{env="prod"} | detected_level="warn"`},
	{"T4/Q25/cluster+detected-error", `{cluster="cluster-0"} | detected_level="error"`},
	{"T4/Q26/env+region+detected-error", `{env="prod", region="us-east-1"} | detected_level="error"`},
	{"T4/Q27/env+detected-error-or-warn", `{env="prod"} | detected_level=~"error|warn"`},
	{"T4/Q28/env+level-error", `{env="prod"} | level="error"`},
	{"T4/Q29/cluster+level-error", `{cluster="cluster-0"} | level="error"`},
	{"T4/Q30/env+region+level-error", `{env="prod", region="us-east-1"} | level="error"`},
	{"T4/Q31/env+level-error-or-warn", `{env="prod"} | level=~"error|warn"`},
	{"T4/Q32/env+cluster+level+nonempty", `{env="prod", cluster="cluster-0"} | level="error" | component!=""`},

	// --- Tier 5: OR stream selectors ---
	{"T5/Q33/env-or-2", `{env=~"prod|staging"}`},
	{"T5/Q34/region-or-2", `{region=~"us-east-1|us-west-2"}`},
	{"T5/Q35/cluster-or-2", `{cluster=~"cluster-0|cluster-1"}`},
	{"T5/Q36/env-or-3-all", `{env=~"prod|staging|dev"}`},
	{"T5/Q37/cluster-or-3", `{cluster=~"cluster-0|cluster-1|cluster-2"}`},
	{"T5/Q38/env-or+error", `{env=~"prod|staging"} |= "error"`},
	{"T5/Q39/region-or+detected-error", `{region=~"us-east-1|us-west-2"} | detected_level="error"`},
	{"T5/Q40/cluster-or+env+detected-error", `{cluster=~"cluster-0|cluster-1", env="prod"} | detected_level="error"`},

	// --- Tier 6: complex nested AND/OR + multi-stage ---
	{"T6/Q41/env-or+region-or", `{env=~"prod|staging", region=~"us-east-1|us-west-2"}`},
	{"T6/Q42/cluster-or+env+logfmt-error", `{cluster=~"cluster-0|cluster-1", env="prod"} | logfmt | level="error"`},
	{
		"T6/Q43/env-or+region+content+detected",
		`{env=~"prod|staging", region="us-east-1"} |= "error" | detected_level="error"`,
	},
	{
		"T6/Q44/cluster-or-3+env+detected-or",
		`{cluster=~"cluster-0|cluster-1|cluster-2", env="prod"} | detected_level=~"error|warn"`,
	},
	{
		"T6/Q45/env+region-or-3+detected-error",
		`{env="prod", region=~"us-east-1|us-west-2|eu-west-1"} | detected_level="error"`,
	},
	{
		"T6/Q46/env-or+region-or+detected+logfmt",
		`{env=~"prod|staging", region=~"us-east-1|us-west-2"} | detected_level="error" | logfmt | level="error"`,
	},
	{
		"T6/Q47/cluster-or+env-or+content+detected",
		`{cluster=~"cluster-0|cluster-1", env=~"prod|staging"} |= "error" | detected_level="error"`,
	},
	{
		"T6/Q48/env-or-3+region-or+cluster+detected",
		`{env=~"prod|staging|dev", region=~"us-east-1|us-west-2", cluster="cluster-0"} | detected_level="error"`,
	},
	{
		"T6/Q49/cluster-or-4+env-or+detected-or",
		`{cluster=~"cluster-0|cluster-1|cluster-2|cluster-3", env=~"prod|staging"} | detected_level=~"error|warn"`,
	},
	{
		"T6/Q50/all-or+detected-or-max",
		`{cluster=~"cluster-0|cluster-1|cluster-2|cluster-3|cluster-4", env=~"prod|staging|dev", region=~"us-east-1|us-west-2|eu-west-1|ap-southeast-1"} | detected_level=~"error|warn"`,
	},

	// --- Tier 7: logfmt + single body-field filter ---
	// Body is logfmt-formatted: level=X component=Y msg="..." latency_ms=N
	// blockpack: body auto-parsed at ingest → log.component stored column → block-level bloom +
	//   range pruning; logfmt stage is a no-op (keys already in labels map).
	// Loki: must parse every body string with logfmt before the field filter can run; no block pruning.
	// component has 8 values → ~12.5% selectivity per query.
	{"T7/Q51/env+logfmt+component-api", `{env="prod"} | logfmt | component="api"`},
	{"T7/Q52/env+logfmt+component-db", `{env="prod"} | logfmt | component="db"`},
	{"T7/Q53/cluster+logfmt+component-cache", `{cluster="cluster-0"} | logfmt | component="cache"`},
	{"T7/Q54/env+region+logfmt+component-auth", `{env="prod", region="us-east-1"} | logfmt | component="auth"`},
	{"T7/Q55/dc+logfmt+component-scheduler", `{datacenter="dc1"} | logfmt | component="scheduler"`},
	{"T7/Q56/env+cluster+logfmt+component-proxy", `{env="prod", cluster="cluster-0"} | logfmt | component="proxy"`},

	// --- Tier 8: logfmt + two body-field AND (high selectivity) ---
	// component (8 values) AND level (4 values) → ~3% selectivity.
	// blockpack: both log.component and log.level participate in presence-based block pruning
	//   (via unscoped column blooms); blocks missing either column can be skipped, but blocks
	//   with those columns and non-matching values are still scanned.
	// Loki: full body parse required; no additional block pruning on extracted fields.
	{"T8/Q57/env+logfmt+component-api+level-error", `{env="prod"} | logfmt | component="api" | level="error"`},
	{"T8/Q58/env+logfmt+component-db+level-error", `{env="prod"} | logfmt | component="db" | level="error"`},
	{
		"T8/Q59/cluster+logfmt+component-cache+level-warn",
		`{cluster="cluster-0"} | logfmt | component="cache" | level="warn"`,
	},
	{
		"T8/Q60/env+region+logfmt+component-api+level-error",
		`{env="prod", region="us-east-1"} | logfmt | component="api" | level="error"`,
	},
	{
		"T8/Q61/env+cluster+logfmt+component-worker+level-error",
		`{env="prod", cluster="cluster-0"} | logfmt | component="worker" | level="error"`,
	},
	{
		"T8/Q62/env+logfmt+component-regex-api-auth+level-error",
		`{env="prod"} | logfmt | component=~"api|auth" | level="error"`,
	},
	{
		"T8/Q63/cluster-or+logfmt+component-db+level-error",
		`{cluster=~"cluster-0|cluster-1"} | logfmt | component="db" | level="error"`,
	},

	// --- Tier 9: three-stage pipeline chains (maximum selectivity) ---
	// Combining stream selector pruning, body-field column pruning, and detected_level column pruning.
	// blockpack: three independent predicate pushdowns; only blocks containing matching values for
	//   ALL three predicates are read. Extremely few blocks survive.
	// Loki: full body parse + sequential label filter on every matched log line.
	{
		"T9/Q64/env+logfmt+component-api+level-error+detected-error",
		`{env="prod"} | logfmt | component="api" | level="error" | detected_level="error"`,
	},
	{
		"T9/Q65/env+region+logfmt+component-regex+level-error",
		`{env="prod", region="us-east-1"} | logfmt | component=~"api|auth|proxy" | level="error"`,
	},
	{
		"T9/Q66/env-or+logfmt+component-ingester+level-error",
		`{env=~"prod|staging"} | logfmt | component="ingester" | level="error"`,
	},
	{
		"T9/Q67/cluster+dc+logfmt+component-db+level-warn",
		`{cluster="cluster-0", datacenter="dc1"} | logfmt | component="db" | level="warn"`,
	},

	// --- Tier 10: namespace stream selector + body-field filters ---
	// namespace has 10 values → high-cardinality resource dimension not used in T1-T9.
	// blockpack: namespace bloom pruning (stream selector) + log.component column pruning (body-field).
	// Loki: namespace is a stream label → chunk-level pruning; body still parsed per line.
	{"T10/Q68/namespace+env+logfmt+component-api", `{namespace="namespace-0", env="prod"} | logfmt | component="api"`},
	{
		"T10/Q69/namespace+env+logfmt+component-api+level-error",
		`{namespace="namespace-0", env="prod"} | logfmt | component="api" | level="error"`,
	},
	{
		"T10/Q70/namespace-or+env+logfmt+component-db+level-error",
		`{namespace=~"namespace-0|namespace-1", env="prod"} | logfmt | component="db" | level="error"`,
	},

	// --- Tier 11: high-cardinality instance_id filters (200 unique values, ~0.5% selectivity) ---
	// instance_id is a LogRecord attribute stored as log.instance_id (200 values).
	// Assigned round-robin per resource group → some block-level clustering.
	// blockpack: log.instance_id range-index pruning + bloom filter → few blocks survive.
	// Loki: no column index on record attributes → must scan all matching chunks.
	{"T11/Q71/env+instance-042", `{env="prod"} | instance_id="instance-042"`},
	{"T11/Q72/env+instance-100", `{env="prod"} | instance_id="instance-100"`},
	{"T11/Q73/cluster+instance-010", `{cluster="cluster-0"} | instance_id="instance-010"`},
	{"T11/Q74/env+region+instance-150", `{env="prod", region="us-east-1"} | instance_id="instance-150"`},
	{"T11/Q75/env+instance-042+level-error", `{env="prod"} | instance_id="instance-042" | level="error"`},
	{"T11/Q76/env+instance-100+component-api", `{env="prod"} | instance_id="instance-100" | component="api"`},
	{"T11/Q77/env+instance-regex-0xx", `{env="prod"} | instance_id=~"instance-0[0-9][0-9]"`},
	{
		"T11/Q78/env+instance-042+level-error+component-db",
		`{env="prod"} | instance_id="instance-042" | level="error" | component="db"`,
	},
	{"T11/Q79/namespace+instance-005", `{namespace="namespace-0", env="prod"} | instance_id="instance-005"`},
	{
		"T11/Q80/cluster+env+instance-195+level-warn",
		`{cluster="cluster-0", env="prod"} | instance_id="instance-195" | level="warn"`,
	},

	// --- Tier 12: OR pushdown and numeric pushdown (new predicate pushdown paths) ---
	//
	// OR pushdown (Q81–Q85): blockpack uses pipe-OR syntax ("| a=x or a=y") which is
	// pushed down to ColumnPredicate as a union of two equality scans. Loki does not
	// support this syntax, so lokiQuery holds the regex equivalent ("| a=~"x|y"") which
	// produces identical results. Both forms scan the same native columns; the OR pushdown
	// enables block-level bloom pruning for each alternative independently.
	//
	// Numeric pushdown (Q86–Q90): latency_ms is stored as a native LogRecord attribute
	// (log.latency_ms string column, values 0–4999). blockpack pushes "> N" down to
	// ColumnPredicate: rowCompare parses the string as float64 and applies the comparison
	// directly during the column scan, matching LabelFilterStage semantics. Loki evaluates
	// the same filter against structured metadata. Both stores return identical rows.
	{"T12/Q81/or-pushdown/detected-error-or-warn", `{env="prod"} | detected_level="error" or detected_level="warn"`},
	{"T12/Q82/or-pushdown/level-error-or-warn", `{env="prod"} | level="error" or level="warn"`},
	{
		"T12/Q83/or-pushdown/cluster+detected-error-or-warn",
		`{cluster="cluster-0"} | detected_level="error" or detected_level="warn"`,
	},
	{
		"T12/Q84/or-pushdown/env+region+level-error-or-warn",
		`{env="prod", region="us-east-1"} | level="error" or level="warn"`,
	},
	{"T12/Q85/or-pushdown/detected-info-or-debug", `{env="prod"} | detected_level="info" or detected_level="debug"`},
	{"T12/Q86/numeric-pushdown/env+latency-gt-4500", `{env="prod"} | latency_ms > 4500`},
	{"T12/Q87/numeric-pushdown/env+latency-gt-4000", `{env="prod"} | latency_ms > 4000`},
	{"T12/Q88/numeric-pushdown/cluster+latency-gt-4000", `{cluster="cluster-0"} | latency_ms > 4000`},
	{"T12/Q89/numeric-pushdown/env+region+latency-gt-4000", `{env="prod", region="us-east-1"} | latency_ms > 4000`},
	{
		"T12/Q90/numeric-pushdown/env+latency-gt-4000+detected-error",
		`{env="prod"} | latency_ms > 4000 | detected_level="error"`,
	},
}

// logFilterConfig holds configuration for a single log filter benchmark dataset.
type logFilterConfig struct {
	name      string        // display name in the HTML report
	dir       string        // data directory (persistent across runs)
	dataMB    int           // target dataset size in MB
	window    time.Duration // time window for log distribution
	ioLatency time.Duration // per-IO latency simulation
}

// BenchmarkLogFilter benchmarks 80 pure LogQL filter queries against a single
// dataset (default 1 GB). For a combined multi-dataset report use BenchmarkLogsFull.
//
// Each sub-benchmark reports ios, linesReturned, and cpuMs for both stores.
func BenchmarkLogFilter(b *testing.B) {
	dir := logfilterDefaultDir
	if v := os.Getenv("LOGFILTER_DIR"); v != "" {
		dir = v
	}

	reportPath := logfilterReportDefault
	if v := os.Getenv(logfilterReportEnv); v != "" {
		reportPath = v
	}

	cfg := logFilterConfig{
		name:      fmt.Sprintf("%d MB OTLP Filter Queries", logfilterDefaultDataMB),
		dir:       dir,
		dataMB:    logfilterDefaultDataMB,
		window:    logfilterDefaultWindow,
		ioLatency: logfilterIOLatency(),
	}

	ds := runFilterDataset(b, cfg)
	writeCombinedHTMLReport(reportPath, []datasetResult{ds})
	b.Logf("HTML report written to %s", reportPath)
}

// BenchmarkLogsFull runs the 50 filter queries at two dataset sizes (100 MB + 1 GB)
// and writes a single combined HTML report — the canonical unified log benchmark.
//
// Data is persisted to testdata/logfilter-{size}/ and reused on subsequent runs.
// Delete those directories to regenerate.
//
// # Usage
//
//	go test -run '^$' -bench BenchmarkLogsFull -benchtime=1x -timeout 3600s -count=1 -v ./...
//
// # Tuning
//
//	LOGFILTER_REPORT=path.html    HTML output path
//	LOGFILTER_IO_LATENCY=10ms     per-IO latency (default: 0)
func BenchmarkLogsFull(b *testing.B) {
	reportPath := logfilterReportDefault
	if v := os.Getenv(logfilterReportEnv); v != "" {
		reportPath = v
	}

	datasets := []logFilterConfig{
		{
			name:      "100 MB OTLP Filter Queries",
			dir:       "testdata/logfilter-100mb",
			dataMB:    100,
			window:    logfilterDefaultWindow,
			ioLatency: logfilterIOLatency(),
		},
		{
			name:      "1 GB OTLP Filter Queries",
			dir:       logfilterDefaultDir,
			dataMB:    logfilterDefaultDataMB,
			window:    logfilterDefaultWindow,
			ioLatency: logfilterIOLatency(),
		},
	}

	var results []datasetResult
	for _, cfg := range datasets {
		cfg := cfg
		b.Run(cfg.name, func(b *testing.B) {
			ds := runFilterDataset(b, cfg)
			results = append(results, ds)
		})
	}

	writeCombinedHTMLReport(reportPath, results)
	b.Logf("Unified HTML report written to %s (%d datasets)", reportPath, len(results))
}

// runFilterDataset generates (or reuses) OTLP log data and runs all 50 filter
// queries against both the Loki chunk store and blockpack native StreamLogQL.
func runFilterDataset(b *testing.B, cfg logFilterConfig) datasetResult {
	b.Helper()

	prometheus.DefaultRegisterer = prometheus.NewRegistry()
	require.NoError(b, os.MkdirAll(cfg.dir, 0o755))

	bpPath := filepath.Join(cfg.dir, "blockpack.logs")
	queryStart := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	queryEnd := queryStart.Add(cfg.window)

	var genDuration time.Duration

	if _, err := os.Stat(bpPath); err == nil {
		b.Logf("[%s] Reusing existing data at %s (delete to regenerate)", cfg.name, cfg.dir)
	} else {
		b.Logf("[%s] Generating %d MB OTLP log data (%s window)...", cfg.name, cfg.dataMB, cfg.window)
		genStart := time.Now()

		genCfg := OTLPGenConfig{
			DataMB: int64(cfg.dataMB),
			Window: cfg.window,
			Start:  queryStart,
		}
		ld := GenerateOTLP(genCfg)

		// Write blockpack directly — OTLP native, no conversion.
		var bpBuf bytes.Buffer
		bpWriter, wErr := blockpack.NewWriter(&bpBuf, 0)
		require.NoError(b, wErr)
		require.NoError(b, bpWriter.AddLogsData(ld))
		_, err := bpWriter.Flush()
		require.NoError(b, err)
		require.NoError(b, os.WriteFile(bpPath, bpBuf.Bytes(), 0o644))

		// Convert OTLP → Loki for the chunk store comparison.
		chunkStore, err := bench.NewChunkStore(cfg.dir, testTenant)
		require.NoError(b, err)
		streams := OTLPToLokiStreams(ld)
		var lokiTotal int64
		for _, s := range streams {
			lokiTotal += int64(len(s.Entries))
		}
		const batchSize = 500
		for i := 0; i < len(streams); i += batchSize {
			end := min(i+batchSize, len(streams))
			require.NoError(b, chunkStore.Write(context.Background(), streams[i:end]))
		}
		require.NoError(b, chunkStore.Close())

		// Persist the Loki entry count so cache-hit runs can display it.
		lokiCountPath := filepath.Join(cfg.dir, "loki_count.txt")
		require.NoError(b, os.WriteFile(lokiCountPath, []byte(strconv.FormatInt(lokiTotal, 10)), 0o644))

		genDuration = time.Since(genStart)
		b.Logf("[%s] Data generation took %s", cfg.name, genDuration.Round(time.Second))
	}

	// --- Read phase ---
	prometheus.DefaultRegisterer = prometheus.NewRegistry()
	chunkStoreRead, err := newLatencyChunkStore(cfg.dir, testTenant, cfg.ioLatency)
	require.NoError(b, err)

	bpReader, bpProvider, err := openBlockpackReaderWithLatency(bpPath, cfg.ioLatency)
	require.NoError(b, err)

	bpStat, err := os.Stat(bpPath)
	require.NoError(b, err)

	var totalDirBytes int64
	_ = filepath.Walk(cfg.dir, func(_ string, info os.FileInfo, walkErr error) error {
		if walkErr == nil && !info.IsDir() {
			totalDirBytes += info.Size()
		}
		return walkErr
	})
	chunkSizeMB := float64(totalDirBytes-bpStat.Size()) / 1024 / 1024

	var totalLogs int64
	for i := range bpReader.BlockCount() {
		totalLogs += int64(bpReader.BlockMeta(i).SpanCount)
	}

	// Read the Loki entry count saved during generation.
	var lokiTotal int64
	lokiCountPath := filepath.Join(cfg.dir, "loki_count.txt")
	if raw, readErr := os.ReadFile(lokiCountPath); readErr == nil {
		lokiTotal, _ = strconv.ParseInt(strings.TrimSpace(string(raw)), 10, 64)
	}

	countNote := ""
	if lokiTotal > 0 && lokiTotal != totalLogs {
		countNote = fmt.Sprintf(" *** COUNT MISMATCH: loki=%d bp=%d ***", lokiTotal, totalLogs)
	}
	b.Logf("[%s] Dataset: %d blocks, bp=%d loki=%d total logs, blockpack %.1f MB, chunks %.1f MB, IO latency %s%s",
		cfg.name, bpReader.BlockCount(), totalLogs, lokiTotal,
		float64(bpStat.Size())/1024/1024, chunkSizeMB, cfg.ioLatency, countNote,
	)

	logger := level.NewFilter(log.NewNopLogger(), level.AllowWarn())
	chunkQuerier, err := chunkStoreRead.Querier()
	require.NoError(b, err)
	chunkEngine := logql.NewEngine(logql.EngineOpts{}, chunkQuerier, logql.NoLimits, logger)
	ctx := user.InjectOrgID(context.Background(), testTenant)

	bpOpts := blockpack.LogQueryOptions{
		StartNano: uint64(queryStart.UnixNano()), //nolint:gosec
		EndNano:   uint64(queryEnd.UnixNano()),   //nolint:gosec
		Limit:     1000,                          // match Loki's default limit for apples-to-apples comparison
		Forward:   true,                          // FORWARD matches logproto.FORWARD so both stores return the same oldest-first 1000 entries
	}

	var results []synthResult

	for _, q := range logFilterQueries {
		q := q

		res := synthResult{
			name:  q.name,
			kind:  "filter",
			query: q.query,
		}

		// Use the Loki-compatible query override when the blockpack query uses
		// syntax Loki does not support (e.g. pipe-OR). Both produce the same rows.
		lokiQ := q.query
		if override, ok := lokiQueryOverrides[q.name]; ok {
			lokiQ = override
		}
		params, pErr := logql.NewLiteralParams(
			lokiQ, queryStart, queryEnd, 0, 0, logproto.FORWARD, 1000, nil, nil,
		)
		require.NoError(b, pErr)

		require.NoError(b, chunkStoreRead.ClearCache())
		chunkStoreRead.ResetIO()
		b.Run(fmt.Sprintf("query=%s/store=chunk", q.name), func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()

			for b.Loop() {
				var m1, m2 runtime.MemStats
				chunkStoreRead.ResetIO()
				runtime.ReadMemStats(&m1)
				cpuBefore := readCPUSecs()
				start := time.Now()

				r, runErr := chunkEngine.Query(params).Exec(ctx)
				elapsed := time.Since(start)
				cpuAfter := readCPUSecs()
				runtime.ReadMemStats(&m2)

				require.NoError(b, runErr)

				res.chunkWallMs = float64(elapsed.Nanoseconds()) / 1e6
				res.chunkCPUMs = (cpuAfter.user - cpuBefore.user + cpuAfter.gc - cpuBefore.gc) * 1000
				res.chunkIOs = chunkStoreRead.IOCount()
				res.chunkLines = countLokiLines(r.Data)
				res.chunkBytes = int64(r.Statistics.Summary.TotalBytesProcessed)
				res.chunkAllocs = int64(m2.Mallocs - m1.Mallocs)
				res.chunkAllocB = int64(m2.TotalAlloc - m1.TotalAlloc)
				if res.chunkData == nil {
					res.chunkData = r.Data // capture first iteration for deep compare
				}

				b.ReportMetric(float64(res.chunkIOs), "ios")
				b.ReportMetric(res.chunkCPUMs, "cpuMs")
				b.ReportMetric(float64(res.chunkLines), "linesReturned")
			}
		})

		// Verification pass: unlimited query to get globally sorted entries for
		// comparison against Loki's globally-sorted result. This runs once before
		// the timing loop so the timing loop is not affected.
		if res.bpData == nil {
			verifyOpts := bpOpts
			verifyOpts.Limit = 0 // unlimited — collect all to sort globally
			var verifyEntries []push.Entry
			verifyMatches, verifyErr := blockpack.QueryLogQL(bpReader, q.query, verifyOpts)
			if verifyErr != nil {
				res.verifyDetail = "blockpack QueryLogQL error: " + verifyErr.Error()
				res.verified = true
				continue
			}
			for _, match := range verifyMatches {
				if match.Fields == nil {
					continue
				}
				var tsNano uint64
				var line string
				if v, ok := match.Fields.GetField("log:timestamp"); ok {
					if ts, ok := v.(uint64); ok {
						tsNano = ts
					}
				}
				if v, ok := match.Fields.GetField("log:body"); ok {
					if s, ok := v.(string); ok {
						line = s
					}
				}
				verifyEntries = append(verifyEntries, push.Entry{
					Timestamp:          time.Unix(0, int64(tsNano)), //nolint:gosec
					Line:               line,
					StructuredMetadata: push.LabelsAdapter(extractStructuredMetadata(match.Fields)),
				})
			}
			// Stable sort with tie-breakers (line, SM) for deterministic ordering
			// when multiple entries share the same nanosecond timestamp.
			sort.SliceStable(verifyEntries, func(i, j int) bool {
				ei, ej := verifyEntries[i], verifyEntries[j]
				if !ei.Timestamp.Equal(ej.Timestamp) {
					return ei.Timestamp.Before(ej.Timestamp)
				}
				if ei.Line != ej.Line {
					return ei.Line < ej.Line
				}
				return serializeSM(ei.StructuredMetadata) < serializeSM(ej.StructuredMetadata)
			})
			// Cap to same limit as Loki for apples-to-apples comparison.
			const verifyLimit = 1000
			if len(verifyEntries) > verifyLimit {
				verifyEntries = verifyEntries[:verifyLimit]
			}
			res.bpData = logqlmodel.Streams{push.Stream{Labels: `{}`, Entries: verifyEntries}}
		}

		b.Run(fmt.Sprintf("query=%s/store=blockpack", q.name), func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()

			for b.Loop() {
				var m1, m2 runtime.MemStats
				bpProvider.Reset()
				runtime.ReadMemStats(&m1)
				cpuBefore := readCPUSecs()
				start := time.Now()

				var bpLines int64
				bpOpts.OnStats = func(s blockpack.LogQueryStats) {
					res.bpExplain = s.Explain
				}
				bpMatches, queryErr := blockpack.QueryLogQL(bpReader, q.query, bpOpts)
				elapsed := time.Since(start)
				cpuAfter := readCPUSecs()
				runtime.ReadMemStats(&m2)

				require.NoError(b, queryErr)
				bpLines = int64(len(bpMatches))

				res.bpWallMs = float64(elapsed.Nanoseconds()) / 1e6
				res.bpCPUMs = (cpuAfter.user - cpuBefore.user + cpuAfter.gc - cpuBefore.gc) * 1000
				res.bpIOs = bpProvider.IOCount()
				res.bpLines = bpLines
				res.bpBytes = 0
				res.bpAllocs = int64(m2.Mallocs - m1.Mallocs)
				res.bpAllocB = int64(m2.TotalAlloc - m1.TotalAlloc)

				b.ReportMetric(float64(res.bpIOs), "ios")
				b.ReportMetric(res.bpCPUMs, "cpuMs")
				b.ReportMetric(float64(bpLines), "linesReturned")
			}
		})

		// Deep compare: timestamp + line content for every returned entry.
		verifyFromCaptured(&res, b)

		results = append(results, res)
	}

	// Stream count = cartesian product of all resource label dimensions.
	nStreams := len(otlpClusters) * len(otlpEnvs) * len(otlpRegions) * len(otlpDatacenters) * len(otlpNamespaces)
	return datasetResult{
		cfg: largescaleConfig{
			name:      cfg.name,
			dir:       cfg.dir,
			dataMB:    cfg.dataMB,
			streams:   nStreams,
			window:    cfg.window,
			ioLatency: cfg.ioLatency,
		},
		results:       results,
		bpSizeMB:      float64(bpStat.Size()) / 1024 / 1024,
		chunkSizeMB:   chunkSizeMB,
		nBlocks:       bpReader.BlockCount(),
		bpTotalLogs:   totalLogs,
		lokiTotalLogs: lokiTotal,
		genDuration:   genDuration,
	}
}

// countLokiLines counts the actual log entries returned in a Loki query result.
// Uses the result data directly rather than TotalPostFilterLines, which counts
// all matching lines before the limit is applied.
func countLokiLines(data interface{}) int64 {
	streams, ok := data.(logqlmodel.Streams)
	if !ok {
		return 0
	}
	var n int64
	for _, s := range streams {
		n += int64(len(s.Entries))
	}
	return n
}
