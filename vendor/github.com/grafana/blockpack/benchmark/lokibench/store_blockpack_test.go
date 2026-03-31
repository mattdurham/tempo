package lokibench

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/grafana/loki/v3/pkg/logproto"
	"github.com/stretchr/testify/require"
	commonv1 "go.opentelemetry.io/proto/otlp/common/v1"
)

func TestConvertStreamsToLogsData_Empty(t *testing.T) {
	ld := convertStreamsToLogsData(nil)
	require.NotNil(t, ld)
	require.Empty(t, ld.ResourceLogs)
}

func TestConvertStreamsToLogsData_SingleStream(t *testing.T) {
	ts := time.Unix(1000, 0)
	streams := []logproto.Stream{
		{
			Labels: `{cluster="c-0", service_name="api-server"}`,
			Entries: []logproto.Entry{
				{
					Timestamp:          ts,
					Line:               "hello world",
					StructuredMetadata: []logproto.LabelAdapter{{Name: "level", Value: "info"}},
				},
			},
		},
	}

	ld := convertStreamsToLogsData(streams)

	require.Len(t, ld.ResourceLogs, 1)
	rl := ld.ResourceLogs[0]

	// Individual label attributes and __loki_labels__ should be present.
	resourceAttrs := attrMapFromKV(rl.Resource.Attributes)
	require.Equal(t, "c-0", resourceAttrs["cluster"])
	require.Equal(t, "api-server", resourceAttrs["service_name"])
	require.Equal(t, `{cluster="c-0", service_name="api-server"}`, resourceAttrs["__loki_labels__"])

	require.Len(t, rl.ScopeLogs, 1)
	require.Len(t, rl.ScopeLogs[0].LogRecords, 1)
	rec := rl.ScopeLogs[0].LogRecords[0]
	require.Equal(t, uint64(ts.UnixNano()), rec.TimeUnixNano) //nolint:gosec
	require.Equal(t, "hello world", rec.Body.GetStringValue())

	logAttrs := attrMapFromKV(rec.Attributes)
	require.Equal(t, "info", logAttrs["level"])
}

func TestConvertStreamsToLogsData_MultipleStreams(t *testing.T) {
	ts := time.Unix(2000, 0)
	streams := []logproto.Stream{
		{Labels: `{service_name="svc-a"}`, Entries: []logproto.Entry{{Timestamp: ts, Line: "a"}}},
		{Labels: `{service_name="svc-b"}`, Entries: []logproto.Entry{{Timestamp: ts, Line: "b"}}},
	}
	ld := convertStreamsToLogsData(streams)
	require.Len(t, ld.ResourceLogs, 2)
}

func TestBlockpackStore_WriteAndClose(t *testing.T) {
	dir := t.TempDir()
	store, err := NewBlockpackStore(dir)
	require.NoError(t, err)
	require.Equal(t, "blockpack", store.Name())

	ts := time.Unix(3000, 0)
	streams := []logproto.Stream{
		{
			Labels:  `{service_name="test"}`,
			Entries: []logproto.Entry{{Timestamp: ts, Line: "test log line"}},
		},
	}
	require.NoError(t, store.Write(context.Background(), streams))
	require.NoError(t, store.Close())

	require.NotEmpty(t, store.Path())
	stat, err := os.Stat(store.Path())
	require.NoError(t, err)
	require.Greater(t, stat.Size(), int64(0))
}

// attrMapFromKV converts an OTLP KeyValue slice to map[key]stringValue for test assertions.
func attrMapFromKV(kvs []*commonv1.KeyValue) map[string]string {
	m := make(map[string]string, len(kvs))
	for _, kv := range kvs {
		m[kv.Key] = kv.Value.GetStringValue()
	}
	return m
}
