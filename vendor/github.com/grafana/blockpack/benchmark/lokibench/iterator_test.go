package lokibench

import (
	"testing"
	"time"

	"github.com/grafana/loki/v3/pkg/logproto"
	"github.com/stretchr/testify/require"
)

func TestSliceEntryIterator_Empty(t *testing.T) {
	it := newSliceEntryIterator("{}", 0, nil)
	require.False(t, it.Next())
	require.NoError(t, it.Err())
	require.Equal(t, "{}", it.Labels())
	require.Equal(t, uint64(0), it.StreamHash())
	require.NoError(t, it.Close())
}

func TestSliceEntryIterator_SingleEntry(t *testing.T) {
	ts := time.Unix(1000, 0)
	entries := []logproto.Entry{{Timestamp: ts, Line: "hello world"}}
	it := newSliceEntryIterator(`{service="foo"}`, 42, entries)
	require.True(t, it.Next())
	got := it.At()
	require.Equal(t, ts, got.Timestamp)
	require.Equal(t, "hello world", got.Line)
	require.Equal(t, `{service="foo"}`, it.Labels())
	require.Equal(t, uint64(42), it.StreamHash())
	require.False(t, it.Next())
	require.NoError(t, it.Err())
}

func TestSliceEntryIterator_MultipleEntries(t *testing.T) {
	base := time.Unix(1000, 0)
	entries := []logproto.Entry{
		{Timestamp: base, Line: "first"},
		{Timestamp: base.Add(time.Second), Line: "second"},
		{Timestamp: base.Add(2 * time.Second), Line: "third"},
	}
	it := newSliceEntryIterator("{}", 0, entries)
	var got []string
	for it.Next() {
		got = append(got, it.At().Line)
	}
	require.Equal(t, []string{"first", "second", "third"}, got)
	require.NoError(t, it.Err())
}

func TestSliceEntryIterator_StructuredMetadata(t *testing.T) {
	ts := time.Unix(1000, 0)
	sm := []logproto.LabelAdapter{
		{Name: "level", Value: "info"},
		{Name: "trace_id", Value: "abc123"},
	}
	entries := []logproto.Entry{{Timestamp: ts, Line: "msg", StructuredMetadata: sm}}
	it := newSliceEntryIterator("{}", 0, entries)
	require.True(t, it.Next())
	got := it.At().StructuredMetadata
	require.Len(t, got, 2)
	require.Equal(t, "level", got[0].Name)
	require.Equal(t, "info", got[0].Value)
	require.Equal(t, "trace_id", got[1].Name)
	require.Equal(t, "abc123", got[1].Value)
}
