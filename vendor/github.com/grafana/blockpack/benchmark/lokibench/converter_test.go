package lokibench

import (
	"context"
	"testing"
	"time"

	"github.com/grafana/loki/v3/pkg/logproto"
	"github.com/grafana/loki/v3/pkg/logql"
	"github.com/grafana/loki/v3/pkg/logql/syntax"
	"github.com/grafana/loki/v3/pkg/querier/plan"
	"github.com/stretchr/testify/require"
)

// buildSmallBlockpack writes a test blockpack file with two known streams and returns
// the file path and the base timestamp. Used by all querier tests.
//
// Stream A: {service_name="svc-a", env="prod"} — entries at base, base+1s, base+2s
// Stream B: {service_name="svc-b", env="staging"} — entries at base+500ms, base+1500ms
func buildSmallBlockpack(t *testing.T, dir string) (path string, base time.Time) {
	t.Helper()
	base = time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)

	store, err := NewBlockpackStore(dir)
	require.NoError(t, err)

	streams := []logproto.Stream{
		{
			Labels: `{service_name="svc-a", env="prod"}`,
			Entries: []logproto.Entry{
				{
					Timestamp:          base,
					Line:               "a1",
					StructuredMetadata: []logproto.LabelAdapter{{Name: "level", Value: "info"}},
				},
				{Timestamp: base.Add(time.Second), Line: "a2"},
				{Timestamp: base.Add(2 * time.Second), Line: "a3"},
			},
		},
		{
			Labels: `{service_name="svc-b", env="staging"}`,
			Entries: []logproto.Entry{
				{Timestamp: base.Add(500 * time.Millisecond), Line: "b1"},
				{Timestamp: base.Add(1500 * time.Millisecond), Line: "b2"},
			},
		},
	}
	require.NoError(t, store.Write(context.Background(), streams))
	require.NoError(t, store.Close())
	return store.Path(), base
}

// buildMultilabelBlockpack writes a test blockpack file with multiple streams having 3+ labels.
func buildMultilabelBlockpack(t *testing.T, dir string) (path string, base time.Time) {
	t.Helper()
	base = time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)

	store, err := NewBlockpackStore(dir)
	require.NoError(t, err)

	// Create multiple streams with 3+ labels
	streams := []logproto.Stream{
		{
			Labels: `{service_name="grafana", env="prod", region="us-west-2"}`,
			Entries: []logproto.Entry{
				{Timestamp: base, Line: "log 1"},
				{Timestamp: base.Add(time.Second), Line: "log 2"},
				{Timestamp: base.Add(2 * time.Second), Line: "log 3"},
			},
		},
		{
			Labels: `{service_name="prometheus", env="prod", region="us-west-2"}`,
			Entries: []logproto.Entry{
				{Timestamp: base.Add(500 * time.Millisecond), Line: "log 4"},
				{Timestamp: base.Add(1500 * time.Millisecond), Line: "log 5"},
			},
		},
		{
			Labels: `{service_name="grafana", env="staging", region="us-east-1"}`,
			Entries: []logproto.Entry{
				{Timestamp: base.Add(3 * time.Second), Line: "log 6"},
			},
		},
	}
	require.NoError(t, store.Write(context.Background(), streams))
	require.NoError(t, store.Close())
	return store.Path(), base
}

// makeLogParams constructs a SelectLogParams with the given log selector, time range, and direction.
func makeLogParams(logSel syntax.LogSelectorExpr, start, end time.Time, dir logproto.Direction) logql.SelectLogParams {
	return logql.SelectLogParams{
		QueryRequest: &logproto.QueryRequest{
			Selector:  logSel.String(),
			Start:     start,
			End:       end,
			Direction: dir,
			Plan:      &plan.QueryPlan{AST: logSel},
		},
	}
}

func TestLokiConverter_SelectLogs_MatchAll(t *testing.T) {
	dir := t.TempDir()
	path, base := buildSmallBlockpack(t, dir)

	r, err := openBlockpackReader(path)
	require.NoError(t, err)
	q := NewLokiConverter(r)

	logSel, err := syntax.ParseLogSelector(`{service_name=~".+"}`, true)
	require.NoError(t, err)
	params := makeLogParams(logSel, base.Add(-time.Second), base.Add(10*time.Second), logproto.FORWARD)

	it, err := q.SelectLogs(context.Background(), params)
	require.NoError(t, err)
	var lines []string
	for it.Next() {
		lines = append(lines, it.At().Line)
	}
	require.NoError(t, it.Err())
	require.NoError(t, it.Close())

	// All 5 entries, FORWARD: a1 at base is earliest
	require.Len(t, lines, 5)
	require.Equal(t, "a1", lines[0])
}

func TestLokiConverter_SelectLogs_LabelFilter(t *testing.T) {
	dir := t.TempDir()
	path, base := buildSmallBlockpack(t, dir)

	r, err := openBlockpackReader(path)
	require.NoError(t, err)
	q := NewLokiConverter(r)

	logSel, err := syntax.ParseLogSelector(`{service_name="svc-a"}`, true)
	require.NoError(t, err)
	params := makeLogParams(logSel, base.Add(-time.Second), base.Add(10*time.Second), logproto.FORWARD)

	it, err := q.SelectLogs(context.Background(), params)
	require.NoError(t, err)
	var lines []string
	for it.Next() {
		lines = append(lines, it.At().Line)
	}
	require.NoError(t, it.Close())
	require.Equal(t, []string{"a1", "a2", "a3"}, lines)
}

func TestLokiConverter_SelectLogs_EmptyResult(t *testing.T) {
	dir := t.TempDir()
	path, base := buildSmallBlockpack(t, dir)

	r, err := openBlockpackReader(path)
	require.NoError(t, err)
	q := NewLokiConverter(r)

	logSel, err := syntax.ParseLogSelector(`{service_name="nonexistent"}`, true)
	require.NoError(t, err)
	params := makeLogParams(logSel, base, base.Add(10*time.Second), logproto.FORWARD)

	it, err := q.SelectLogs(context.Background(), params)
	require.NoError(t, err)
	require.False(t, it.Next())
	require.NoError(t, it.Close())
}

func TestLokiConverter_SelectLogs_StructuredMetadata(t *testing.T) {
	dir := t.TempDir()
	path, base := buildSmallBlockpack(t, dir)

	r, err := openBlockpackReader(path)
	require.NoError(t, err)
	q := NewLokiConverter(r)

	logSel, err := syntax.ParseLogSelector(`{service_name="svc-a"}`, true)
	require.NoError(t, err)
	// Small time window: only first entry (a1 at base) is in range
	params := makeLogParams(logSel, base.Add(-time.Millisecond), base.Add(time.Millisecond), logproto.FORWARD)

	it, err := q.SelectLogs(context.Background(), params)
	require.NoError(t, err)
	require.True(t, it.Next())
	entry := it.At()
	require.Equal(t, "a1", entry.Line)

	var foundLevel bool
	for _, sm := range entry.StructuredMetadata {
		if sm.Name == "level" && sm.Value == "info" {
			foundLevel = true
		}
	}
	require.True(t, foundLevel, "expected level=info in StructuredMetadata")
	require.NoError(t, it.Close())
}

// TestSelectLogsWithLabelFilterPreservesStructuredMetadata is the regression
// guard for Risk 1 in plan.md: when buildPushdownQuery re-emits a label filter
// stage (e.g. | level="info") as a native StreamLogQL predicate, log.* columns
// must still appear in StructuredMetadata. This verifies that pushed-down label
// filter predicates do NOT cause StreamLogQL to strip log.* fields from
// SpanMatch.Fields.
func TestSelectLogsWithLabelFilterPreservesStructuredMetadata(t *testing.T) {
	dir := t.TempDir()
	path, base := buildSmallBlockpack(t, dir)

	r, err := openBlockpackReader(path)
	require.NoError(t, err)
	q := NewLokiConverter(r)

	// Query with a pushed-down label filter: | level="info".
	// buildPushdownQuery will re-emit this as a native predicate.
	// The first entry (a1 at base) has level="info" SM; others have no level.
	logSel, err := syntax.ParseLogSelector(`{service_name="svc-a"} | level="info"`, true)
	require.NoError(t, err)
	params := makeLogParams(logSel, base.Add(-time.Millisecond), base.Add(10*time.Second), logproto.FORWARD)

	it, err := q.SelectLogs(context.Background(), params)
	require.NoError(t, err)

	// Only the entry with level="info" should be returned.
	require.True(t, it.Next(), "expected at least one entry matching | level=\"info\"")
	entry := it.At()
	require.Equal(t, "a1", entry.Line)

	// log.* columns must still be accessible in StructuredMetadata even though
	// | level="info" was pushed down as a native column predicate.
	var foundLevel bool
	for _, sm := range entry.StructuredMetadata {
		if sm.Name == "level" && sm.Value == "info" {
			foundLevel = true
		}
	}
	require.True(t, foundLevel, "level=info must appear in StructuredMetadata even with label filter pushdown")
	require.False(t, it.Next(), "expected exactly one matching entry")
	require.NoError(t, it.Close())
}

func TestLokiConverter_SelectLogs_BackwardDirection(t *testing.T) {
	dir := t.TempDir()
	path, base := buildSmallBlockpack(t, dir)

	r, err := openBlockpackReader(path)
	require.NoError(t, err)
	q := NewLokiConverter(r)

	// Only svc-a (3 entries) for a clear ordering test
	logSel, err := syntax.ParseLogSelector(`{service_name="svc-a"}`, true)
	require.NoError(t, err)
	params := makeLogParams(logSel, base.Add(-time.Second), base.Add(10*time.Second), logproto.BACKWARD)

	it, err := q.SelectLogs(context.Background(), params)
	require.NoError(t, err)
	var lines []string
	for it.Next() {
		lines = append(lines, it.At().Line)
	}
	require.NoError(t, it.Close())
	// BACKWARD: a3 (latest) first
	require.Len(t, lines, 3)
	require.Equal(t, "a3", lines[0])
	require.Equal(t, "a1", lines[2])
}

// TestBuildPushdownQuery verifies that buildPushdownQuery:
// - Preserves stream matchers in {…}
// - Preserves line filters (|=, !=, |~, !~)
// - Re-emits simple StageLabelFilter stages as native predicates
// - Drops parser stages (logfmt, json) but keeps subsequent label filters
// - Stops at mutating barrier stages (label_format, drop, keep, line_format)
// - Skips negation ops (unsafe for bloom pruning)
// - Handles OR label filter stages correctly
// - Falls back to original string on parse error
// - Reports fullyPushed=true when all stages handled, false otherwise
func TestBuildPushdownQuery(t *testing.T) {
	tests := []struct {
		name        string
		input       string
		want        string
		fullyPushed bool
	}{
		{
			name:        "stream only — no pipeline",
			input:       `{env="prod"}`,
			want:        `{env="prod"}`,
			fullyPushed: true,
		},
		{
			name:        "stream + line filter — preserved",
			input:       `{env="prod"} |= "error"`,
			want:        `{env="prod"} |= "error"`,
			fullyPushed: true,
		},
		{
			name:        "stream + label filter — pushed but not fully (no parser seen)",
			input:       `{env="prod"} | detected_level="error"`,
			want:        `{env="prod"} | detected_level="error"`,
			fullyPushed: false,
		},
		{
			name:        "stream + logfmt + label filter — label filter preserved, logfmt dropped",
			input:       `{env="prod"} | logfmt | component="api"`,
			want:        `{env="prod"} | component="api"`,
			fullyPushed: true,
		},
		{
			name:        "stream + multi label filter AND (no parser)",
			input:       `{env="prod"} | detected_level="error" | level="error"`,
			want:        `{env="prod"} | detected_level="error" | level="error"`,
			fullyPushed: false,
		},
		{
			name:        "stream + line filter + label filter (no parser)",
			input:       `{env="prod"} |= "error" | detected_level="error"`,
			want:        `{env="prod"} |= "error" | detected_level="error"`,
			fullyPushed: false,
		},
		{
			name:        "stream + label filter stops at label_format barrier",
			input:       `{env="prod"} | detected_level="error" | label_format foo=bar | level="error"`,
			want:        `{env="prod"} | detected_level="error"`,
			fullyPushed: false,
		},
		{
			name:        "logfmt + component regex",
			input:       `{env="prod"} | logfmt | component=~"api|auth"`,
			want:        `{env="prod"} | component=~"api|auth"`,
			fullyPushed: true,
		},
		{
			name:        "numeric pushdown preserved (no parser)",
			input:       `{env="prod"} | latency_ms > 4000`,
			want:        `{env="prod"} | latency_ms > 4000`,
			fullyPushed: false,
		},
		{
			name:        "instance_id equality pushed down (no parser)",
			input:       `{env="prod"} | instance_id="instance-042"`,
			want:        `{env="prod"} | instance_id="instance-042"`,
			fullyPushed: false,
		},
		{
			name:        "OR label filter preserved (no parser)",
			input:       `{env="prod"} | detected_level="error" or detected_level="warn"`,
			want:        `{env="prod"} | detected_level="error" or detected_level="warn"`,
			fullyPushed: false,
		},
		{
			name:        "not-equal negation pushed (no parser)",
			input:       `{env="prod"} | level!="debug"`,
			want:        `{env="prod"} | level!="debug"`,
			fullyPushed: false,
		},
		{
			name:        "parse error falls back to original string",
			input:       `not valid logql {{{{`,
			want:        `not valid logql {{{{`,
			fullyPushed: false,
		},
		{
			name:        "not-regex negation pushed (no parser)",
			input:       `{env="prod"} | level!~"debug|trace"`,
			want:        `{env="prod"} | level!~"debug|trace"`,
			fullyPushed: false,
		},
		{
			name:        "less-than numeric (no parser)",
			input:       `{env="prod"} | latency_ms < 100`,
			want:        `{env="prod"} | latency_ms < 100`,
			fullyPushed: false,
		},
		{
			name:        "less-than-or-equal numeric (no parser)",
			input:       `{env="prod"} | latency_ms <= 100`,
			want:        `{env="prod"} | latency_ms <= 100`,
			fullyPushed: false,
		},
		{
			name:        "greater-than-or-equal numeric (no parser)",
			input:       `{env="prod"} | latency_ms >= 4000`,
			want:        `{env="prod"} | latency_ms >= 4000`,
			fullyPushed: false,
		},
		{
			name:        "line_format is a barrier stage",
			input:       `{env="prod"} | detected_level="error" | line_format "{{.msg}}" | level="info"`,
			want:        `{env="prod"} | detected_level="error"`,
			fullyPushed: false,
		},
		{
			name:        "drop is transparent — positive filters after it are pushed",
			input:       `{env="prod"} | detected_level="error" | drop __error__ | level="info"`,
			want:        `{env="prod"} | detected_level="error" | level="info"`,
			fullyPushed: false,
		},
		{
			name:        "negation after drop is NOT pushed (dropped label absence = match-all)",
			input:       `{env="prod"} | drop level | level!="debug"`,
			want:        `{env="prod"}`,
			fullyPushed: false,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, fullyPushed := buildPushdownQuery(tc.input)
			require.Equal(t, tc.want, got)
			require.Equal(t, tc.fullyPushed, fullyPushed, "fullyPushed mismatch")
		})
	}
}

func TestLokiConverter_SelectLogs_MultiLabelEquality(t *testing.T) {
	dir := t.TempDir()
	path, base := buildMultilabelBlockpack(t, dir)

	r, err := openBlockpackReader(path)
	require.NoError(t, err)
	q := NewLokiConverter(r)

	t.Run("three_labels_equality", func(t *testing.T) {
		logSel, err := syntax.ParseLogSelector(`{service_name="grafana", env="prod", region="us-west-2"}`, true)
		require.NoError(t, err)
		params := makeLogParams(logSel, base.Add(-time.Second), base.Add(10*time.Second), logproto.FORWARD)

		it, err := q.SelectLogs(context.Background(), params)
		require.NoError(t, err)
		var lines []string
		for it.Next() {
			lines = append(lines, it.At().Line)
		}
		require.NoError(t, it.Close())
		require.Len(
			t,
			lines,
			3,
			"Expected 3 entries for {service_name=\"grafana\", env=\"prod\", region=\"us-west-2\"}, got %d: %v",
			len(lines),
			lines,
		)
		require.Equal(t, []string{"log 1", "log 2", "log 3"}, lines)
	})

	t.Run("three_labels_with_regex", func(t *testing.T) {
		logSel, err := syntax.ParseLogSelector(`{service_name=~"(?i)grafana", env="prod", region="us-west-2"}`, true)
		require.NoError(t, err)
		params := makeLogParams(logSel, base.Add(-time.Second), base.Add(10*time.Second), logproto.FORWARD)

		it, err := q.SelectLogs(context.Background(), params)
		require.NoError(t, err)
		var lines []string
		for it.Next() {
			lines = append(lines, it.At().Line)
		}
		require.NoError(t, it.Close())
		require.Len(
			t,
			lines,
			3,
			"Expected 3 entries for {service_name=~\"(?i)grafana\", env=\"prod\", region=\"us-west-2\"}, got %d: %v",
			len(lines),
			lines,
		)
	})

	t.Run("two_labels_equality", func(t *testing.T) {
		logSel, err := syntax.ParseLogSelector(`{service_name="grafana", env="prod"}`, true)
		require.NoError(t, err)
		params := makeLogParams(logSel, base.Add(-time.Second), base.Add(10*time.Second), logproto.FORWARD)

		it, err := q.SelectLogs(context.Background(), params)
		require.NoError(t, err)
		var lines []string
		for it.Next() {
			lines = append(lines, it.At().Line)
		}
		require.NoError(t, it.Close())
		require.Len(
			t,
			lines,
			3,
			"Expected 3 entries for {service_name=\"grafana\", env=\"prod\"}, got %d: %v",
			len(lines),
			lines,
		)
	})
}
