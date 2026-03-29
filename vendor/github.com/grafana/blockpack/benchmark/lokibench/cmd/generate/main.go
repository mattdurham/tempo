// Command generate produces realistic synthetic Loki log data matching production
// patterns from loki-dev-005 (tenant 29). It writes to both a Loki chunk store and
// a blockpack file, then optionally verifies both stores contain the same data.
//
// Usage:
//
//	go run ./cmd/generate/ -out /tmp/loki-bench-gen -entries 1000000 -streams 100
//
// Flags:
//
//	-out        output directory (default: /tmp/loki-bench-gen)
//	-entries    total log entries to generate (default: 1000000)
//	-streams    number of unique log streams (default: 100)
//	-start      start time RFC3339 (default: now-2h)
//	-end        end time RFC3339 (default: now)
//	-tenant     tenant ID (default: 29)
//	-seed       random seed for reproducibility (default: 42)
//	-cardinality label cardinality multiplier (default: 1.0)
//	-verify     run verification after generation (default: true)
package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"math"
	"math/rand"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/blockpack"
	lokibench "github.com/grafana/blockpack/benchmark/lokibench"
	"github.com/grafana/dskit/user"
	"github.com/grafana/loki/v3/pkg/logproto"
	"github.com/grafana/loki/v3/pkg/logql"
	"github.com/grafana/loki/v3/pkg/logql/bench"
	"github.com/grafana/loki/v3/pkg/logqlmodel"
	"github.com/prometheus/client_golang/prometheus"
)

// ---------------------------------------------------------------------------
// Static label value tables (realistic but fictional, modeled on production shapes)
// ---------------------------------------------------------------------------

var (
	realClusters = []string{
		"staging-us-central-0", "staging-eu-south-0", "staging-eu-west-2", "staging-eu-west-3",
		"staging-eu-west-4", "staging-eu-west-5", "staging-us-central-1", "staging-us-east-0",
		"staging-us-east-1", "staging-us-east-2", "staging-us-east-3",
		"edge-aws-oregon-0", "edge-aws-oregon-1", "edge-aws-paris",
		"edge-aws-spain-1",
	}

	realNamespaces = []string{
		"logging", "metrics", "tracing", "dashboards", "collector-logs", "collector-otlp",
		"profiling", "agent-logs", "auth", "assertions", "aws-logs-staging-005",
		"incident-mgmt", "adaptive-profiles-cd", "assistant", "infra-monitoring",
		"cicd-o11y", "machine-learning", "loadtest-cloud", "oncall",
		"metrics-staging-10", "logging-staging", "tracing-staging", "dashboards-staging",
		"kube-system", "monitoring", "velero", "cert-manager",
		"external-dns", "ingress-nginx", "metrics-server",
	}

	realContainers = []string{
		"ingester", "distributor", "ruler", "compactor", "querier",
		"query-frontend", "store-gateway", "alloy", "grafana",
		"alertmanager", "query-scheduler", "index-gateway",
		"bloom-compactor", "bloom-gateway", "partition-ingester",
		"memcached", "memcached-frontend", "memcached-index-queries",
		"agent", "cortex-gw", "loki-canary", "promtail",
	}

	realCallerFiles = []string{
		"evaluator.go", "ingester.go", "distributor.go", "querier.go",
		"compactor.go", "ruler.go", "store.go", "chunk.go",
		"handler.go", "server.go", "client.go", "ring.go",
		"scheduler.go", "frontend.go", "gateway.go",
	}

	realLogMessages = []string{
		"evaluation done", "chunk flushed", "query received", "stream ingested",
		"index written", "compaction started", "rule evaluated", "flush complete",
		"connection established", "request completed", "cache miss", "cache hit",
		"ring changed", "member joined", "member left", "heartbeat sent",
		"block written", "block pruned", "tenant created", "tenant deleted",
		"query timeout", "chunk skipped", "stream matched", "index updated",
	}

	realComponents = []string{
		"ingester", "distributor", "compactor", "querier", "ruler",
		"frontend", "gateway", "scheduler", "ring", "store", "cache",
	}

	samplingRates = []string{
		"0.01", "0.05", "0.10", "0.20", "0.50",
		"1.00", "2.00", "5.00", "10.00", "12.00",
	}
)

const (
	hexChars   = "0123456789abcdef"
	alphaChars = "abcdefghijklmnopqrstuvwxyz"
)

// ---------------------------------------------------------------------------
// config and CLI
// ---------------------------------------------------------------------------

type config struct {
	outDir      string
	numEntries  int
	numStreams  int
	startStr    string
	endStr      string
	start       time.Time
	end         time.Time
	tenant      string
	seed        int64
	cardinality float64
	verify      bool
}

func parseFlags() config {
	var cfg config
	flag.StringVar(&cfg.outDir, "out", "/tmp/loki-bench-gen", "output directory")
	flag.IntVar(&cfg.numEntries, "entries", 1_000_000, "total log entries to generate")
	flag.IntVar(&cfg.numStreams, "streams", 100, "number of unique log streams")
	flag.StringVar(&cfg.startStr, "start", "", "start time RFC3339 (default: now-2h)")
	flag.StringVar(&cfg.endStr, "end", "", "end time RFC3339 (default: now)")
	flag.StringVar(&cfg.tenant, "tenant", "29", "tenant ID")
	flag.Int64Var(&cfg.seed, "seed", 42, "random seed for reproducibility")
	flag.Float64Var(&cfg.cardinality, "cardinality", 1.0, "label cardinality multiplier")
	flag.BoolVar(&cfg.verify, "verify", true, "run verification after generation")
	flag.Parse()

	now := time.Now().UTC()
	if cfg.startStr == "" {
		cfg.start = now.Add(-2 * time.Hour)
	} else {
		t, err := time.Parse(time.RFC3339, cfg.startStr)
		if err != nil {
			fmt.Fprintf(os.Stderr, "invalid -start: %v\n", err)
			os.Exit(1)
		}
		cfg.start = t
	}
	if cfg.endStr == "" {
		cfg.end = now
	} else {
		t, err := time.Parse(time.RFC3339, cfg.endStr)
		if err != nil {
			fmt.Fprintf(os.Stderr, "invalid -end: %v\n", err)
			os.Exit(1)
		}
		cfg.end = t
	}
	if cfg.numStreams < 1 {
		fmt.Fprintf(os.Stderr, "error: -streams must be >= 1\n")
		os.Exit(1)
	}
	if cfg.numEntries < 1 {
		fmt.Fprintf(os.Stderr, "error: -entries must be >= 1\n")
		os.Exit(1)
	}
	if !cfg.start.Before(cfg.end) {
		fmt.Fprintf(os.Stderr, "error: -start must be before -end (got %s >= %s)\n",
			cfg.start.Format(time.RFC3339), cfg.end.Format(time.RFC3339))
		os.Exit(1)
	}
	if cfg.numEntries < cfg.numStreams {
		fmt.Fprintf(os.Stderr, "error: -entries (%d) must be >= -streams (%d)\n",
			cfg.numEntries, cfg.numStreams)
		os.Exit(1)
	}
	return cfg
}

// ---------------------------------------------------------------------------
// Stream specification
// ---------------------------------------------------------------------------

type streamSpec struct {
	labels  string
	podName string // pod name for SM generation
	format  int    // 0=logfmt (96%), 1=json (2%), 2=plain (2%)
	count   int
}

func buildStreamSpecs(cfg config, rng *rand.Rand) []streamSpec {
	zipf := rand.NewZipf(rng, 1.5, 1.0, uint64(cfg.numStreams*10)) //nolint:gosec
	rawCounts := make([]uint64, cfg.numStreams)
	var rawSum uint64
	for i := range rawCounts {
		v := zipf.Uint64()
		if v == 0 {
			v = 1
		}
		rawCounts[i] = v
		rawSum += v
	}

	scale := float64(cfg.numEntries) / float64(rawSum)
	specs := make([]streamSpec, cfg.numStreams)
	var totalCount int
	for i := range specs {
		cnt := int(float64(rawCounts[i]) * scale)
		if cnt < 1 {
			cnt = 1
		}
		labels, podName := buildStreamLabels(i, cfg, rng)
		specs[i] = streamSpec{
			labels:  labels,
			podName: podName,
			count:   cnt,
		}
		totalCount += cnt

		r := rng.Float64()
		switch {
		case r < 0.963:
			specs[i].format = 0 // logfmt
		case r < 0.983:
			specs[i].format = 1 // json
		default:
			specs[i].format = 2 // plain
		}
	}

	// Adjust stream 0 to ensure sum == cfg.numEntries.
	diff := cfg.numEntries - totalCount
	specs[0].count += diff
	if specs[0].count < 1 {
		specs[0].count = 1
	}

	return specs
}

func buildStreamLabels(i int, cfg config, rng *rand.Rand) (string, string) {
	cluster := realClusters[i%len(realClusters)]
	namespace := realNamespaces[i%len(realNamespaces)]
	container := realContainers[i%len(realContainers)]
	pod := randomPodName(container, rng, cfg.cardinality)
	job := namespace + "/" + container
	streamVal := "stdout"
	if i%2 != 0 {
		streamVal = "stderr"
	}
	svcName := namespace + "/" + container
	labels := fmt.Sprintf(
		`{__name__="logs", cluster=%q, container=%q, job=%q, namespace=%q, pod=%q, service_name=%q, stream=%q}`,
		cluster, container, job, namespace, pod, svcName, streamVal,
	)
	return labels, pod
}

func randomPodName(container string, rng *rand.Rand, cardinality float64) string {
	n := 5
	if cardinality > 1.0 {
		extra := int(math.Ceil(cardinality))
		n += extra
	}
	hash := make([]byte, n)
	suffix := make([]byte, 5)
	for i := range hash {
		hash[i] = hexChars[rng.Intn(len(hexChars))]
	}
	for i := range suffix {
		suffix[i] = alphaChars[rng.Intn(len(alphaChars))]
	}
	return container + "-" + string(hash) + "-" + string(suffix)
}

// ---------------------------------------------------------------------------
// Log entry generation
// ---------------------------------------------------------------------------

func generateStreams(cfg config, specs []streamSpec, rng *rand.Rand) []logproto.Stream {
	streams := make([]logproto.Stream, 0, len(specs))
	startNs := cfg.start.UnixNano()
	rangeNs := cfg.end.UnixNano() - startNs
	if rangeNs <= 0 {
		rangeNs = 1
	}

	for _, spec := range specs {
		if spec.count == 0 {
			continue
		}
		tsNanos := make([]int64, spec.count)
		for i := range tsNanos {
			tsNanos[i] = startNs + rng.Int63n(rangeNs+1)
		}
		sort.Slice(tsNanos, func(i, j int) bool { return tsNanos[i] < tsNanos[j] })

		entries := make([]logproto.Entry, spec.count)
		for i, tsNano := range tsNanos {
			ts := time.Unix(0, tsNano)
			line := generateLogLine(spec.format, ts, rng)
			sm := generateStructuredMetadata(rng, spec.podName)
			entries[i] = logproto.Entry{
				Timestamp:          ts,
				Line:               line,
				StructuredMetadata: sm,
			}
		}

		streams = append(streams, logproto.Stream{
			Labels:  spec.labels,
			Entries: entries,
		})
	}
	return streams
}

func generateLogLine(format int, ts time.Time, rng *rand.Rand) string {
	switch format {
	case 1:
		return generateJSONLine(ts, rng)
	case 2:
		return generatePlainLine(rng)
	default:
		return generateLogfmtLine(ts, rng)
	}
}

func generateLogfmtLine(ts time.Time, rng *rand.Rand) string {
	lvl := pickLevel(rng)
	caller := realCallerFiles[rng.Intn(len(realCallerFiles))]
	callerLine := 10 + rng.Intn(500)
	msg := realLogMessages[rng.Intn(len(realLogMessages))]
	component := realComponents[rng.Intn(len(realComponents))]

	base := fmt.Sprintf(
		`ts=%s caller=%s:%d level=%s msg=%q component=%s`,
		ts.UTC().Format(time.RFC3339Nano),
		caller, callerLine, lvl, msg, component,
	)

	targetLen := pickTargetLen(rng)
	var sb strings.Builder
	sb.WriteString(base)

	extraKeys := []string{
		"user", "org", "traceID", "duration", "requestID",
		"method", "path", "status", "bytes", "host",
	}
	kvIndex := 0
	for sb.Len() < targetLen {
		appendKV(&sb, extraKeys[kvIndex%len(extraKeys)], rng)
		kvIndex++
	}
	return sb.String()
}

func appendKV(sb *strings.Builder, key string, rng *rand.Rand) {
	switch key {
	case "user":
		fmt.Fprintf(sb, " user=%d", rng.Intn(100000))
	case "org":
		fmt.Fprintf(sb, " org=%d", rng.Intn(10000))
	case "traceID":
		fmt.Fprintf(sb, " traceID=%016x", rng.Uint64())
	case "duration":
		fmt.Fprintf(sb, " duration=%dms", rng.Intn(10000))
	case "requestID":
		fmt.Fprintf(sb, " requestID=%016x", rng.Uint64())
	case "method":
		methods := []string{"GET", "POST", "PUT", "DELETE"}
		fmt.Fprintf(sb, " method=%s", methods[rng.Intn(len(methods))])
	case "path":
		fmt.Fprintf(sb, " path=/api/v1/%s/%d",
			realComponents[rng.Intn(len(realComponents))], rng.Intn(1000))
	case "status":
		statuses := []string{"200", "201", "400", "404", "500", "503"}
		fmt.Fprintf(sb, " status=%s", statuses[rng.Intn(len(statuses))])
	case "bytes":
		fmt.Fprintf(sb, " bytes=%d", rng.Intn(1000000))
	case "host":
		fmt.Fprintf(sb, " host=node-%d.cluster.local", rng.Intn(100))
	}
}

func pickTargetLen(rng *rand.Rand) int {
	r := rng.Float64()
	switch {
	case r < 0.50:
		return 150 + rng.Intn(200) // 150–350
	case r < 0.75:
		return 350 + rng.Intn(150) // 350–500
	case r < 0.90:
		return 500 + rng.Intn(400) // 500–900
	default:
		return 900 + rng.Intn(700) // 900–1600
	}
}

func pickLevel(rng *rand.Rand) string {
	r := rng.Float64()
	switch {
	case r < 0.60:
		return "info"
	case r < 0.80:
		return "debug"
	case r < 0.95:
		return "warn"
	default:
		return "error"
	}
}

func generateJSONLine(ts time.Time, rng *rand.Rand) string {
	lvl := pickLevel(rng)
	msg := realLogMessages[rng.Intn(len(realLogMessages))]
	hostname := fmt.Sprintf("grafana-com-session-expiration-%d-%s",
		rng.Intn(99999999), randomSuffix(rng, 5))
	return fmt.Sprintf(
		`{"time":%q,"level":%q,"msg":%q,"hostname":%q,"pid":1,"git_commit":%q,"go_os":"linux","num_vcpus":%d}`,
		ts.UTC().Format(time.RFC3339),
		lvl, msg, hostname,
		fmt.Sprintf("%08x", rng.Uint32()),
		1+rng.Intn(32),
	)
}

func generatePlainLine(rng *rand.Rand) string {
	switch rng.Intn(3) {
	case 0:
		lvl := pickLevel(rng)
		msg := realLogMessages[rng.Intn(len(realLogMessages))]
		return fmt.Sprintf("[%s] %s", strings.ToUpper(lvl), msg)
	case 1:
		return "io scheduler bfq registered"
	default:
		return fmt.Sprintf("EXT4-fs (sda1): re-mounted %s.",
			fmt.Sprintf("%08x-%04x-%04x-%04x-%012x",
				rng.Uint32(), rng.Uint32()&0xffff, rng.Uint32()&0xffff,
				rng.Uint32()&0xffff, rng.Uint64()&0xffffffffffff))
	}
}

func generateStructuredMetadata(rng *rand.Rand, podName string) []logproto.LabelAdapter {
	if rng.Float64() > 0.988 {
		return nil // 1.2% of entries have no SM
	}
	sm := []logproto.LabelAdapter{
		{Name: "detected_level", Value: pickLevel(rng)},
	}
	// __adaptive_logs_sampled__: 9.1% of entries.
	if rng.Float64() < 0.091 {
		rate := samplingRates[rng.Intn(len(samplingRates))]
		sm = append(sm, logproto.LabelAdapter{Name: "__adaptive_logs_sampled__", Value: rate})
	}
	// pod as SM: 2.3% of entries (k8s pod name duplicated into SM).
	if rng.Float64() < 0.023 {
		sm = append(sm, logproto.LabelAdapter{Name: "pod", Value: podName})
	}
	// pod_template_hash as SM: 2.2% of entries.
	if rng.Float64() < 0.022 {
		sm = append(sm, logproto.LabelAdapter{
			Name: "pod_template_hash", Value: fmt.Sprintf("%010x", rng.Uint64()&0xffffffffff),
		})
	}
	// resource_version as SM: 2.2% of entries.
	if rng.Float64() < 0.022 {
		sm = append(sm, logproto.LabelAdapter{
			Name: "resource_version", Value: fmt.Sprintf("%d", 10000000+rng.Intn(90000000)),
		})
	}
	// span_id: ~0.03% of entries (rare, tracing context).
	if rng.Float64() < 0.003 {
		sm = append(sm, logproto.LabelAdapter{
			Name: "span_id", Value: fmt.Sprintf("%016x", rng.Uint64()),
		})
	}
	// trace_id: ~0.03% of entries (rare, tracing context).
	if rng.Float64() < 0.003 {
		sm = append(sm, logproto.LabelAdapter{
			Name: "trace_id", Value: fmt.Sprintf("%016x%016x", rng.Uint64(), rng.Uint64()),
		})
	}
	// Sort by Name for consistent comparison with chunk store.
	sort.Slice(sm, func(i, j int) bool { return sm[i].Name < sm[j].Name })
	return sm
}

func randomSuffix(rng *rand.Rand, n int) string {
	b := make([]byte, n)
	for i := range b {
		b[i] = alphaChars[rng.Intn(len(alphaChars))]
	}
	return string(b)
}

// ---------------------------------------------------------------------------
// Write to chunk store
// ---------------------------------------------------------------------------

func writeChunkStore(ctx context.Context, cfg config, streams []logproto.Stream) error {
	dir := filepath.Join(cfg.outDir, "chunks")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("mkdir chunks: %w", err)
	}
	// Reset Prometheus registry to avoid MustRegister panics from bench.NewChunkStore.
	prometheus.DefaultRegisterer = prometheus.NewRegistry()
	store, err := bench.NewChunkStore(dir, cfg.tenant)
	if err != nil {
		return fmt.Errorf("bench.NewChunkStore: %w", err)
	}
	if err := store.Write(ctx, streams); err != nil {
		return fmt.Errorf("chunk store write: %w", err)
	}
	return store.Close()
}

// ---------------------------------------------------------------------------
// Write to blockpack
// ---------------------------------------------------------------------------

func writeBlockpack(ctx context.Context, cfg config, streams []logproto.Stream) (string, error) {
	dir := filepath.Join(cfg.outDir, "blockpack")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return "", fmt.Errorf("mkdir blockpack: %w", err)
	}
	store, err := lokibench.NewBlockpackStore(dir)
	if err != nil {
		return "", fmt.Errorf("NewBlockpackStore: %w", err)
	}

	const batchSize = 50_000
	var batch []logproto.Stream
	var batchEntries int

	for _, s := range streams {
		batch = append(batch, s)
		batchEntries += len(s.Entries)
		if batchEntries >= batchSize {
			if writeErr := store.Write(ctx, batch); writeErr != nil {
				return "", fmt.Errorf("blockpack write: %w", writeErr)
			}
			batch = batch[:0]
			batchEntries = 0
		}
	}
	if len(batch) > 0 {
		if writeErr := store.Write(ctx, batch); writeErr != nil {
			return "", fmt.Errorf("blockpack write final: %w", writeErr)
		}
	}
	if err := store.Close(); err != nil {
		return "", fmt.Errorf("blockpack close: %w", err)
	}
	return store.Path(), nil
}

// ---------------------------------------------------------------------------
// Verification
// ---------------------------------------------------------------------------

// bytesProvider implements blockpack.ReaderProvider over an in-memory byte slice.
// Replicated from querier.go since that package's openBlockpackReader is unexported.
type bytesProvider struct{ data []byte }

func (p *bytesProvider) Size() (int64, error) { return int64(len(p.data)), nil }

func (p *bytesProvider) ReadAt(buf []byte, off int64, _ blockpack.DataType) (int, error) {
	if off < 0 || off >= int64(len(p.data)) {
		return 0, io.EOF
	}
	n := copy(buf, p.data[off:])
	if n < len(buf) {
		return n, io.EOF
	}
	return n, nil
}

func verify(ctx context.Context, cfg config, bpPath string) (int64, int64, error) {
	// Open chunk store for reading (reset registry to avoid duplicate registration panic).
	prometheus.DefaultRegisterer = prometheus.NewRegistry()
	chunksDir := filepath.Join(cfg.outDir, "chunks")
	chunkStore, err := bench.NewChunkStore(chunksDir, cfg.tenant)
	if err != nil {
		return 0, 0, fmt.Errorf("verify: open chunk store: %w", err)
	}
	defer chunkStore.Close()
	chunkQuerier, err := chunkStore.Querier()
	if err != nil {
		return 0, 0, fmt.Errorf("verify: chunk querier: %w", err)
	}

	// Open blockpack for reading.
	data, err := os.ReadFile(bpPath)
	if err != nil {
		return 0, 0, fmt.Errorf("verify: read blockpack: %w", err)
	}
	bpReader, err := blockpack.NewReaderFromProvider(&bytesProvider{data: data})
	if err != nil {
		return 0, 0, fmt.Errorf("verify: blockpack reader: %w", err)
	}

	logger := level.NewFilter(log.NewNopLogger(), level.AllowWarn())
	chunkEngine := logql.NewEngine(logql.EngineOpts{}, chunkQuerier, logql.NoLimits, logger)
	bpQuerier := lokibench.NewLokiConverter(bpReader)
	bpEngine := logql.NewEngine(logql.EngineOpts{}, bpQuerier, logql.NoLimits, logger)

	ctx = user.InjectOrgID(ctx, cfg.tenant)

	params, err := logql.NewLiteralParams(
		`{__name__="logs"}`,
		cfg.start, cfg.end,
		0, 0,
		logproto.FORWARD, math.MaxUint32, nil, nil,
	)
	if err != nil {
		return 0, 0, fmt.Errorf("verify: build params: %w", err)
	}

	verifyCtx, cancel := context.WithTimeout(ctx, 5*time.Minute)
	defer cancel()

	chunkResult, err := chunkEngine.Query(params).Exec(verifyCtx)
	if err != nil {
		return 0, 0, fmt.Errorf("verify: chunk query: %w", err)
	}
	bpResult, err := bpEngine.Query(params).Exec(verifyCtx)
	if err != nil {
		return 0, 0, fmt.Errorf("verify: blockpack query: %w", err)
	}

	chunkCount := countStreamEntries(chunkResult)
	bpCount := countStreamEntries(bpResult)

	// Fast-path count check with 0.1% tolerance.
	diff := chunkCount - bpCount
	if diff < 0 {
		diff = -diff
	}
	threshold := chunkCount / 1000
	if threshold == 0 {
		threshold = 1
	}
	if diff > threshold {
		return chunkCount, bpCount, fmt.Errorf("count mismatch: chunk=%d bp=%d", chunkCount, bpCount)
	}

	// Content comparison: collect (timestamp, line) pairs from both stores, sort, and
	// compare element-by-element. Reports the first mismatching pair found.
	chunkPairs := collectEntryPairs(chunkResult)
	bpPairs := collectEntryPairs(bpResult)

	if len(chunkPairs) != len(bpPairs) {
		return chunkCount, bpCount, fmt.Errorf("content count mismatch: chunk=%d bp=%d",
			len(chunkPairs), len(bpPairs))
	}

	const maxMismatches = 5
	var mismatches []string
	for i := range chunkPairs {
		cp, bp := chunkPairs[i], bpPairs[i]
		if cp.tsNano != bp.tsNano {
			mismatches = append(mismatches,
				fmt.Sprintf("  entry[%d]: timestamp mismatch chunk=%d bp=%d", i, cp.tsNano, bp.tsNano))
		} else if cp.line != bp.line {
			mismatches = append(mismatches,
				fmt.Sprintf("  entry[%d] ts=%d: line mismatch\n    chunk:     %q\n    blockpack: %q",
					i, cp.tsNano, cp.line, bp.line))
		}
		if len(mismatches) >= maxMismatches {
			break
		}
	}
	if len(mismatches) > 0 {
		return chunkCount, bpCount, fmt.Errorf("content mismatch (%d shown):\n%s",
			len(mismatches), strings.Join(mismatches, "\n"))
	}

	return chunkCount, bpCount, nil
}

// entryPair holds a (timestamp, line) pair for content comparison.
type entryPair struct {
	tsNano int64
	line   string
}

// collectEntryPairs flattens a query result into a sorted slice of (timestamp, line) pairs.
func collectEntryPairs(result logqlmodel.Result) []entryPair {
	streams, ok := result.Data.(logqlmodel.Streams)
	if !ok {
		return nil
	}
	var pairs []entryPair
	for _, s := range streams {
		for _, e := range s.Entries {
			pairs = append(pairs, entryPair{tsNano: e.Timestamp.UnixNano(), line: e.Line})
		}
	}
	sort.Slice(pairs, func(i, j int) bool {
		if pairs[i].tsNano != pairs[j].tsNano {
			return pairs[i].tsNano < pairs[j].tsNano
		}
		return pairs[i].line < pairs[j].line
	})
	return pairs
}

func countStreamEntries(result logqlmodel.Result) int64 {
	streams, ok := result.Data.(logqlmodel.Streams)
	if !ok {
		return 0
	}
	var total int64
	for _, s := range streams {
		total += int64(len(s.Entries))
	}
	return total
}

// ---------------------------------------------------------------------------
// Summary helpers
// ---------------------------------------------------------------------------

func commaFmt(n int64) string {
	if n < 0 {
		return "-" + commaFmt(-n)
	}
	s := fmt.Sprintf("%d", n)
	if len(s) <= 3 {
		return s
	}
	var b strings.Builder
	mod := len(s) % 3
	for i, ch := range s {
		if i > 0 && (i-mod)%3 == 0 {
			b.WriteByte(',')
		}
		b.WriteRune(ch)
	}
	return b.String()
}

func dirSizeMB(dir string) float64 {
	var total int64
	_ = filepath.Walk(dir, func(_ string, info os.FileInfo, err error) error {
		if err == nil && !info.IsDir() {
			total += info.Size()
		}
		return nil
	})
	return float64(total) / 1024 / 1024
}

// ---------------------------------------------------------------------------
// main
// ---------------------------------------------------------------------------

func main() {
	cfg := parseFlags()
	ctx := context.Background()

	if err := os.MkdirAll(cfg.outDir, 0o755); err != nil {
		fmt.Fprintf(os.Stderr, "mkdir %s: %v\n", cfg.outDir, err)
		os.Exit(1)
	}

	// Phase 1: Generate streams.
	rng := rand.New(rand.NewSource(cfg.seed)) //nolint:gosec
	fmt.Printf("Building %d stream specs with Zipf distribution...\n", cfg.numStreams)
	specs := buildStreamSpecs(cfg, rng)

	fmt.Printf("Generating log entries...\n")
	streams := generateStreams(cfg, specs, rng)

	var totalGenerated int
	for _, s := range streams {
		totalGenerated += len(s.Entries)
	}
	fmt.Printf("Generated %d entries across %d streams\n", totalGenerated, len(streams))

	// Phase 2: Write chunk store.
	fmt.Printf("Writing chunk store...\n")
	t0 := time.Now()
	if err := writeChunkStore(ctx, cfg, streams); err != nil {
		fmt.Fprintf(os.Stderr, "chunk store error: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("Chunk store written (%.1fs)\n", time.Since(t0).Seconds())

	// Phase 3: Write blockpack.
	fmt.Printf("Writing blockpack...\n")
	t0 = time.Now()
	bpPath, err := writeBlockpack(ctx, cfg, streams)
	if err != nil {
		fmt.Fprintf(os.Stderr, "blockpack error: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("Blockpack written (%.1fs)\n", time.Since(t0).Seconds())

	// Phase 4: Print summary.
	chunksDir := filepath.Join(cfg.outDir, "chunks")
	chunksMB := dirSizeMB(chunksDir)
	bpStat, _ := os.Stat(bpPath)
	var bpMB float64
	if bpStat != nil {
		bpMB = float64(bpStat.Size()) / 1024 / 1024
	}

	fmt.Printf("\n=== Generation Summary ===\n")
	fmt.Printf("Streams:       %s\n", commaFmt(int64(len(streams))))
	fmt.Printf("Total entries: %s\n", commaFmt(int64(totalGenerated)))
	fmt.Printf("Time range:    %s -> %s\n",
		cfg.start.UTC().Format(time.RFC3339),
		cfg.end.UTC().Format(time.RFC3339))
	fmt.Printf("Chunk store:   %s (%.1f MB)\n", chunksDir, chunksMB)
	fmt.Printf("Blockpack:     %s (%.1f MB)\n", bpPath, bpMB)

	// Phase 5: Verify (optional).
	if !cfg.verify {
		return
	}

	fmt.Printf("\nRunning verification...\n")
	chunkCount, bpCount, verifyErr := verify(ctx, cfg, bpPath)
	fmt.Printf("\n=== Verification ===\n")
	if verifyErr != nil {
		fmt.Printf("Match: FAILED (%v)\n", verifyErr)
		os.Exit(1)
	}
	fmt.Printf("Chunk store entries: %s\n", commaFmt(chunkCount))
	fmt.Printf("Blockpack entries:   %s\n", commaFmt(bpCount))
	fmt.Printf("Match: OK\n")
}
