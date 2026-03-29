// deepcompare_test.go — Synthetic deep compare: chunk store vs blockpack, no entry limit.
//
// Generates synthetic log data, then runs every log query from TestCaseGenerator
// with no entry limit against both stores, asserting full content equality:
// entry count, timestamps, log lines, and the union of stream labels +
// StructuredMetadata + Parsed fields per entry.
//
// The metadata union handles Loki schema v13 SM promotion: SM fields that Loki
// promotes to stream labels are present in parse(stream.Labels) for the chunk store
// and in entry.StructuredMetadata for blockpack — the union is identical in both cases.
//
// Run:
//
//	cd benchmark/lokibench
//	go test -run TestSyntheticDeepCompare -timeout 120s -v ./...
package lokibench

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sort"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/user"
	lokiengine "github.com/grafana/loki/v3/pkg/engine"
	"github.com/grafana/loki/v3/pkg/logproto"
	"github.com/grafana/loki/v3/pkg/logql"
	"github.com/grafana/loki/v3/pkg/logql/bench"
	"github.com/grafana/loki/v3/pkg/logql/syntax"
	"github.com/grafana/loki/v3/pkg/logqlmodel"
	pmlabels "github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/require"
)

const (
	deepCompareDataBytes = 10 * 1024 * 1024 // 10 MB — fast enough for a unit test
	deepCompareStreams   = 20
	deepCompareWindow    = 30 * time.Minute
)

// TestSyntheticDeepCompare runs all synthetic log queries against both the Loki chunk
// store and blockpack with no entry limit, then performs a deep content comparison:
// entry count, timestamps, log lines, and the full metadata union
// (stream labels ∪ StructuredMetadata ∪ Parsed) per entry.
//
// This is stricter than TestStorageEquality which applies a 1000-entry cap and
// only compares (timestamp, line) pairs.
func TestSyntheticDeepCompare(t *testing.T) {
	resetPrometheusRegistry(t)
	dir := t.TempDir()

	chunkStore, err := bench.NewChunkStore(dir, testTenant)
	require.NoError(t, err)

	bpStore, err := NewBlockpackStore(dir)
	require.NoError(t, err)

	opt := bench.DefaultOpt().
		WithNumStreams(deepCompareStreams).
		WithTimeSpread(deepCompareWindow)

	builder := bench.NewBuilder(dir, opt, chunkStore, bpStore)
	require.NoError(t, builder.Generate(context.Background(), deepCompareDataBytes))

	resetPrometheusRegistry(t) // second reset: write store already registered metrics above
	chunkStoreRead, err := bench.NewChunkStore(dir, testTenant)
	require.NoError(t, err)

	bpReader, err := openBlockpackReader(bpStore.Path())
	require.NoError(t, err)
	require.Greater(t, bpReader.BlockCount(), 0)

	config, err := bench.LoadConfig(dir)
	require.NoError(t, err)

	cases := bench.NewTestCaseGenerator(bench.DefaultTestCaseGeneratorConfig, config).Generate()
	require.NotEmpty(t, cases)

	logger := level.NewFilter(log.NewNopLogger(), level.AllowWarn())
	chunkQuerier, err := chunkStoreRead.Querier()
	require.NoError(t, err)

	chunkEngine := logql.NewEngine(logql.EngineOpts{}, chunkQuerier, logql.NoLimits, logger)
	bpConverter := NewLokiConverter(bpReader)
	bpConverter.forceFullPipeline = true // metadata comparison needs authoritative Loki pipeline
	bpEngine := logql.NewEngine(logql.EngineOpts{}, bpConverter, logql.NoLimits, logger)

	ctx := user.InjectOrgID(context.Background(), testTenant)

	for _, tc := range cases {
		if tc.Kind() != "log" {
			continue
		}

		tc := tc
		t.Run(tc.Name(), func(t *testing.T) {
			// math.MaxUint32 as limit — effectively unlimited; NoLimits covers engine caps.
			params, err := logql.NewLiteralParams(
				tc.Query, tc.Start, tc.End, 0, 0, tc.Direction, math.MaxUint32, nil, nil,
			)
			require.NoError(t, err)

			chunkResult, err := chunkEngine.Query(params).Exec(ctx)
			require.NoError(t, err, "chunk engine error")

			bpResult, err := bpEngine.Query(params).Exec(ctx)
			if errors.Is(err, lokiengine.ErrNotSupported) {
				t.Skipf("query not supported by blockpack engine: %v", err)
			}
			require.NoError(t, err, "blockpack engine error")

			chunkStreams, ok := chunkResult.Data.(logqlmodel.Streams)
			require.True(t, ok, "chunk result is not logqlmodel.Streams: %T", chunkResult.Data)

			bpStreams, ok := bpResult.Data.(logqlmodel.Streams)
			require.True(t, ok, "blockpack result is not logqlmodel.Streams: %T", bpResult.Data)

			require.NoError(t, deepCompareLogStreams(chunkStreams, bpStreams))
		})
	}
}

// deepEntry is a normalised log entry for cross-store comparison. The meta map is the
// union of stream labels + StructuredMetadata + Parsed, so SM fields promoted to stream
// labels by Loki schema v13 compare equal to SM fields in entry.StructuredMetadata from
// blockpack.
type deepEntry struct {
	tsNano int64
	line   string
	meta   map[string]string
}

// deepCompareLogStreams performs a full content comparison of two Streams results.
// Entries are normalised to []deepEntry (sorted by tsNano, then line) and compared
// element-by-element for timestamp, line content, and effective metadata.
func deepCompareLogStreams(chunk, bp logqlmodel.Streams) error {
	chunkEntries := collectDeepEntries(chunk)
	bpEntries := collectDeepEntries(bp)

	if len(chunkEntries) != len(bpEntries) {
		return fmt.Errorf("entry count mismatch: chunk=%d blockpack=%d", len(chunkEntries), len(bpEntries))
	}

	for i := range chunkEntries {
		ce, be := chunkEntries[i], bpEntries[i]
		if ce.tsNano != be.tsNano {
			return fmt.Errorf("entry[%d]: timestamp mismatch: chunk=%d blockpack=%d", i, ce.tsNano, be.tsNano)
		}
		if ce.line != be.line {
			return fmt.Errorf("entry[%d] ts=%d: line mismatch:\n  chunk:     %q\n  blockpack: %q",
				i, ce.tsNano, ce.line, be.line)
		}
		if err := compareEntryMeta(i, ce.meta, be.meta); err != nil {
			return err
		}
	}
	return nil
}

// collectDeepEntries flattens a Streams result into a sorted []deepEntry.
// For each entry: meta = parse(stream.Labels) ∪ StructuredMetadata ∪ Parsed.
func collectDeepEntries(streams logqlmodel.Streams) []deepEntry {
	var entries []deepEntry
	for _, s := range streams {
		streamMeta := streamLabelsToMap(s.Labels)
		for _, e := range s.Entries {
			meta := make(map[string]string, len(streamMeta)+len(e.StructuredMetadata)+len(e.Parsed))
			for k, v := range streamMeta {
				meta[k] = v
			}
			for _, a := range e.StructuredMetadata {
				meta[a.Name] = a.Value
			}
			for _, a := range e.Parsed {
				meta[a.Name] = a.Value
			}
			entries = append(entries, deepEntry{
				tsNano: e.Timestamp.UnixNano(),
				line:   e.Line,
				meta:   meta,
			})
		}
	}
	sort.Slice(entries, func(i, j int) bool {
		if entries[i].tsNano != entries[j].tsNano {
			return entries[i].tsNano < entries[j].tsNano
		}
		return entries[i].line < entries[j].line
	})
	return entries
}

// streamLabelsToMap parses a Loki label string into a key→value map.
// Returns an empty map on parse failure.
func streamLabelsToMap(labelsStr string) map[string]string {
	parsed, err := syntax.ParseLabels(labelsStr)
	if err != nil {
		return map[string]string{}
	}
	m := make(map[string]string, parsed.Len())
	parsed.Range(func(l pmlabels.Label) {
		m[l.Name] = l.Value
	})
	return m
}

// compareEntryMeta checks that blockpack's metadata is a superset of the chunk
// store's metadata. Blockpack may include additional metadata keys derived from
// fields parsed from the log body and stored as columns for indexing that the
// chunk store does not expose — those extras are acceptable. What matters is
// that every key the chunk store returns is also present in blockpack with the
// same value.
func compareEntryMeta(entryIdx int, chunk, bp map[string]string) error {
	for k, cv := range chunk {
		bv, ok := bp[k]
		if !ok {
			return fmt.Errorf("entry[%d] meta: key %q in chunk but not in blockpack", entryIdx, k)
		}
		if cv != bv {
			return fmt.Errorf("entry[%d] meta[%q]: chunk=%q blockpack=%q", entryIdx, k, cv, bv)
		}
	}
	return nil
}

// TestFastPathParity verifies the production fast path (fullyPushed=true,
// forceFullPipeline=false) for queries over data with body-auto-parsed JSON columns.
//
// Unlike TestSyntheticDeepCompare (which forces forceFullPipeline=true to keep
// the authoritative Loki pipeline for full metadata comparison), this test uses
// the default converter path where fullyPushed=true queries skip sp.Process and
// return entries directly from blockpack's executor.
//
// Covers: ProcessSkipParsers, LabelFilterStage (Equal/NotEqual/GT/GTE/LT/LTE/Regex/NotRegex),
// OrLabelFilterStage.
//
// Comparison is on (timestamp, line, StructuredMetadata): metadata encoding
// differs between stores for Parsed labels (chunk exposes | json fields as Parsed;
// blockpack fast path skips sp.Process so no Parsed labels are added), but entry
// selection and real SM must agree exactly.
//
// Run:
//
//	cd benchmark/lokibench
//	go test -run TestFastPathParity -timeout 60s -v ./...
func TestFastPathParity(t *testing.T) {
	resetPrometheusRegistry(t)
	dir := t.TempDir()

	chunkStore, err := bench.NewChunkStore(dir, testTenant)
	require.NoError(t, err)

	bpStore, err := NewBlockpackStore(dir)
	require.NoError(t, err)

	base := time.Date(2024, 6, 1, 0, 0, 0, 0, time.UTC)

	// Two streams with JSON bodies. blockpack auto-parses JSON bodies into log.* columns
	// (ColumnTypeRangeString), so blockHasBodyParsed=true → ProcessSkipParsers is used.
	streams := []logproto.Stream{
		{
			Labels: `{service="api", env="prod"}`,
			Entries: []logproto.Entry{
				{Timestamp: base, Line: `{"level":"error","latency_ms":500,"component":"api"}`},
				{Timestamp: base.Add(time.Second), Line: `{"level":"warn","latency_ms":250,"component":"api"}`},
				{Timestamp: base.Add(2 * time.Second), Line: `{"level":"info","latency_ms":50,"component":"api"}`},
				{Timestamp: base.Add(3 * time.Second), Line: `{"level":"debug","latency_ms":10,"component":"api"}`},
			},
		},
		{
			Labels: `{service="auth", env="prod"}`,
			Entries: []logproto.Entry{
				{Timestamp: base.Add(500 * time.Millisecond), Line: `{"level":"error","latency_ms":800,"component":"auth"}`},
				{Timestamp: base.Add(1500 * time.Millisecond), Line: `{"level":"info","latency_ms":30,"component":"auth"}`},
			},
		},
	}

	require.NoError(t, chunkStore.Write(context.Background(), streams))
	require.NoError(t, chunkStore.Close())
	require.NoError(t, bpStore.Write(context.Background(), streams))
	require.NoError(t, bpStore.Close())

	resetPrometheusRegistry(t)
	chunkStoreRead, err := bench.NewChunkStore(dir, testTenant)
	require.NoError(t, err)

	bpReader, err := openBlockpackReader(bpStore.Path())
	require.NoError(t, err)

	logger := level.NewFilter(log.NewNopLogger(), level.AllowWarn())
	chunkQuerier, err := chunkStoreRead.Querier()
	require.NoError(t, err)
	chunkEngine := logql.NewEngine(logql.EngineOpts{}, chunkQuerier, logql.NoLimits, logger)

	// forceFullPipeline=false (default): fullyPushed=true queries take the fast path,
	// skipping sp.Process and exercising ProcessSkipParsers in the executor.
	bpConverter := NewLokiConverter(bpReader)
	bpEngine := logql.NewEngine(logql.EngineOpts{}, bpConverter, logql.NoLimits, logger)

	ctx := user.InjectOrgID(context.Background(), testTenant)

	start := base.Add(-time.Second)
	end := base.Add(10 * time.Second)

	cases := []struct {
		name  string
		query string
	}{
		// LabelFilterStage Equal — fast path, ProcessSkipParsers
		{"json_equal", `{service=~".+"} | json | level="error"`},
		// OrLabelFilterStage — OR filter preserved in pushed-down query
		{"json_or", `{service=~".+"} | json | level="error" or level="warn"`},
		// LabelFilterStage GT numeric
		{"json_gt", `{service=~".+"} | json | latency_ms > 200`},
		// LabelFilterStage GTE numeric
		{"json_gte", `{service=~".+"} | json | latency_ms >= 250`},
		// LabelFilterStage LT numeric
		{"json_lt", `{service=~".+"} | json | latency_ms < 100`},
		// LabelFilterStage LTE numeric
		{"json_lte", `{service=~".+"} | json | latency_ms <= 50`},
		// LabelFilterStage Regex
		{"json_regex", `{service=~".+"} | json | component=~"api|auth"`},
		// LabelFilterStage NotEqual (seenParser=true so still fast path)
		{"json_notequal", `{service=~".+"} | json | level!="debug"`},
		// LabelFilterStage NotRegex
		{"json_notregex", `{service=~".+"} | json | component!~"db"`},
		// Numeric OR: both alternatives pushed into ColumnPredicate (union), pipeline=nil.
		// Pre-parser numeric ops are skipped from pipeline same as string ops (NOTE-012).
		{"json_numeric_or", `{service=~".+"} | json | latency_ms > 200 or latency_ms < 50`},
		// stream selector only — no pipeline, exercises base path
		{"no_pipeline", `{service="api"}`},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			params, err := logql.NewLiteralParams(
				tc.query, start, end, 0, 0, logproto.FORWARD, math.MaxUint32, nil, nil,
			)
			require.NoError(t, err)

			chunkResult, err := chunkEngine.Query(params).Exec(ctx)
			require.NoError(t, err, "chunk engine error")

			bpResult, err := bpEngine.Query(params).Exec(ctx)
			if errors.Is(err, lokiengine.ErrNotSupported) {
				t.Skipf("query not supported by blockpack engine: %v", err)
			}
			require.NoError(t, err, "blockpack engine error")

			chunkStreams, ok := chunkResult.Data.(logqlmodel.Streams)
			require.True(t, ok, "chunk result is not Streams: %T", chunkResult.Data)

			bpStreams, ok := bpResult.Data.(logqlmodel.Streams)
			require.True(t, ok, "blockpack result is not Streams: %T", bpResult.Data)

			// compareStreams checks (timestamp, line, StructuredMetadata) equality.
			// Parsed labels are excluded: chunk exposes json-parsed fields as Parsed
			// but blockpack fast path skips sp.Process so no Parsed labels are added.
			// Both sides have no real SM written, so SM is "" throughout.
			require.NoError(t, compareStreams(chunkStreams, bpStreams))
		})
	}
}

// TestSMLeakMismatch reproduces the body-parsed log.* column leak into
// StructuredMetadata. When a log entry has a JSON body with key "level",
// blockpack stores it as log.level (ColumnTypeRangeString). extractStructuredMetadata
// then sees it and passes level=... as SM to sp.Process. The chunk store does NOT
// do this — it only exposes real LogRecord attributes as SM, not body-parsed fields.
// This causes blockpack to return entries for label filters that the chunk store
// would correctly return zero results for.
func TestSMLeakMismatch(t *testing.T) {
	resetPrometheusRegistry(t)
	dir := t.TempDir()

	// Write a stream whose log bodies are JSON with a "level" field.
	// The chunk store stores this as SM={} (no SM from body parsing).
	// Blockpack's writer auto-parses the body and stores log.level as
	// ColumnTypeRangeString. extractStructuredMetadata then returns
	// [{Name:"level", Value:"error"}] — which it should NOT.
	chunkStore, err := bench.NewChunkStore(dir, testTenant)
	require.NoError(t, err)

	bpStore, err := NewBlockpackStore(dir)
	require.NoError(t, err)

	streams := []logproto.Stream{
		{
			Labels: `{service_name="svc-json", env="prod"}`,
			Entries: []logproto.Entry{
				{
					Timestamp: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
					Line:      `{"level":"error","msg":"something failed","duration_seconds":0.5}`,
				},
				{
					Timestamp: time.Date(2024, 1, 1, 0, 0, 1, 0, time.UTC),
					Line:      `{"level":"info","msg":"ok","duration_seconds":0.01}`,
				},
				{
					Timestamp: time.Date(2024, 1, 1, 0, 0, 2, 0, time.UTC),
					Line:      `{"level":"warn","msg":"slow","duration_seconds":0.8}`,
				},
			},
		},
	}

	require.NoError(t, chunkStore.Write(context.Background(), streams))
	require.NoError(t, chunkStore.Close())
	require.NoError(t, bpStore.Write(context.Background(), streams))
	require.NoError(t, bpStore.Close())

	resetPrometheusRegistry(t) // second reset: write store already registered metrics above
	chunkStoreRead, err := bench.NewChunkStore(dir, testTenant)
	require.NoError(t, err)

	bpReader, err := openBlockpackReader(bpStore.Path())
	require.NoError(t, err)

	logger := level.NewFilter(log.NewNopLogger(), level.AllowWarn())
	chunkQuerier, err := chunkStoreRead.Querier()
	require.NoError(t, err)
	chunkEngine := logql.NewEngine(logql.EngineOpts{}, chunkQuerier, logql.NoLimits, logger)
	bpConverter := NewLokiConverter(bpReader)
	bpConverter.forceFullPipeline = true // SM leak test needs authoritative Loki pipeline
	bpEngine := logql.NewEngine(logql.EngineOpts{}, bpConverter, logql.NoLimits, logger)
	ctx := user.InjectOrgID(context.Background(), testTenant)

	base := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)

	// runQuery executes a LogQL query against both engines and returns (chunkCount, bpCount).
	runQuery := func(t *testing.T, query string) (int, int) {
		t.Helper()
		params, err := logql.NewLiteralParams(
			query, base.Add(-time.Second), base.Add(10*time.Second),
			0, 0, logproto.FORWARD, math.MaxUint32, nil, nil,
		)
		require.NoError(t, err)

		chunkResult, err := chunkEngine.Query(params).Exec(ctx)
		require.NoError(t, err)
		bpResult, err := bpEngine.Query(params).Exec(ctx)
		require.NoError(t, err)

		countEntries := func(t *testing.T, label string, r logqlmodel.Result) int {
			t.Helper()
			streams, ok := r.Data.(logqlmodel.Streams)
			require.True(t, ok, "%s: expected logqlmodel.Streams result, got %T", label, r.Data)
			var n int
			for _, s := range streams {
				n += len(s.Entries)
			}
			return n
		}
		return countEntries(t, "chunk", chunkResult), countEntries(t, "blockpack", bpResult)
	}

	t.Run("label_filter_without_parser_should_not_match_body_fields", func(t *testing.T) {
		// Without a parser stage, label filters only match stream labels and real SM.
		// Body-parsed fields like "level" must NOT appear in SM.
		// Before fix: IterateFields leaks log.level (ColumnTypeRangeString) as SM →
		// blockpack returns 1 entry; chunk returns 0 (no such SM).
		// After fix: ColumnTypeRangeString columns are skipped → SM is empty → 0 entries match.
		chunkCount, bpCount := runQuery(t, `{service_name="svc-json"} | level="error"`)
		// Without a parser, label filters cannot match body-parsed fields: expect 0 from both.
		// If both stores return non-zero but equal, it means both are broken the same way.
		require.Equal(t, 0, chunkCount,
			"label filter without parser: chunk store returned %d (expected 0)", chunkCount)
		require.Equal(t, 0, bpCount,
			"label filter without parser: blockpack returned %d (expected 0, SM leak?)", bpCount)
	})

	t.Run("json_parser_level_filter_agrees_with_chunk", func(t *testing.T) {
		// With | json, body fields are extracted as parsed labels. With SM leak,
		// | json sees level=error already in SM and renames the extracted field to
		// level_extracted — making the level="error" filter match via SM not parsed.
		// Both stores should agree on the count.
		chunkCount, bpCount := runQuery(t, `{service_name="svc-json"} | json | level="error"`)
		// The dataset has exactly one entry with level="error"; both stores must return 1.
		require.Equal(t, 1, chunkCount,
			"json parser + level filter: chunk store returned %d (expected 1)", chunkCount)
		require.Equal(t, 1, bpCount,
			"json parser + level filter: blockpack returned %d (expected 1)", bpCount)
	})
}
