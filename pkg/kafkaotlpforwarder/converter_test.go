package kafkaotlpforwarder

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/grafana/tempo/pkg/tempopb"
	v1_common "github.com/grafana/tempo/pkg/tempopb/common/v1"
	v1_resource "github.com/grafana/tempo/pkg/tempopb/resource/v1"
	v1_trace "github.com/grafana/tempo/pkg/tempopb/trace/v1"
)

func TestConvertToOTLP_EmptyRequest(t *testing.T) {
	req := &tempopb.PushBytesRequest{
		Traces: []tempopb.PreallocBytes{},
		Ids:    [][]byte{},
	}

	tracesData, err := ConvertToOTLP(req)
	require.NoError(t, err)
	require.NotNil(t, tracesData)
	require.Len(t, tracesData.ResourceSpans, 0)
}

func TestConvertToOTLP_SingleTrace(t *testing.T) {
	// Create a valid OTLP ResourceSpans structure
	resourceSpans := &v1_trace.ResourceSpans{
		Resource: &v1_resource.Resource{
			Attributes: []*v1_common.KeyValue{
				{
					Key: "service.name",
					Value: &v1_common.AnyValue{
						Value: &v1_common.AnyValue_StringValue{
							StringValue: "test-service",
						},
					},
				},
			},
		},
		ScopeSpans: []*v1_trace.ScopeSpans{
			{
				Spans: []*v1_trace.Span{
					{
						TraceId: []byte("trace-id-1234567"),
						SpanId:  []byte("span-id-12"),
						Name:    "test-span",
					},
				},
			},
		},
	}

	// Marshal ResourceSpans to bytes (this is what Tempo stores in Kafka)
	sliceData, err := resourceSpans.Marshal()
	require.NoError(t, err)

	// Create PushBytesRequest with the marshaled data
	req := &tempopb.PushBytesRequest{
		Traces: []tempopb.PreallocBytes{
			{Slice: sliceData},
		},
		Ids: [][]byte{[]byte("trace-id-1234567")},
	}

	// Convert to OTLP
	tracesData, err := ConvertToOTLP(req)
	require.NoError(t, err)
	require.NotNil(t, tracesData)
	require.Len(t, tracesData.ResourceSpans, 1)

	// Verify the data matches
	rs := tracesData.ResourceSpans[0]
	require.Equal(t, "test-service", rs.Resource.Attributes[0].Value.GetStringValue())
	require.Len(t, rs.ScopeSpans, 1)
	require.Len(t, rs.ScopeSpans[0].Spans, 1)
	require.Equal(t, "test-span", rs.ScopeSpans[0].Spans[0].Name)
}

func TestConvertToOTLP_MultipleTraces(t *testing.T) {
	// Create two different traces
	rs1 := &v1_trace.ResourceSpans{
		ScopeSpans: []*v1_trace.ScopeSpans{
			{
				Spans: []*v1_trace.Span{
					{Name: "span-1"},
				},
			},
		},
	}

	rs2 := &v1_trace.ResourceSpans{
		ScopeSpans: []*v1_trace.ScopeSpans{
			{
				Spans: []*v1_trace.Span{
					{Name: "span-2"},
				},
			},
		},
	}

	slice1, _ := rs1.Marshal()
	slice2, _ := rs2.Marshal()

	req := &tempopb.PushBytesRequest{
		Traces: []tempopb.PreallocBytes{
			{Slice: slice1},
			{Slice: slice2},
		},
	}

	tracesData, err := ConvertToOTLP(req)
	require.NoError(t, err)
	require.Len(t, tracesData.ResourceSpans, 2)
	require.Equal(t, "span-1", tracesData.ResourceSpans[0].ScopeSpans[0].Spans[0].Name)
	require.Equal(t, "span-2", tracesData.ResourceSpans[1].ScopeSpans[0].Spans[0].Name)
}

func TestConvertToOTLP_InvalidData(t *testing.T) {
	req := &tempopb.PushBytesRequest{
		Traces: []tempopb.PreallocBytes{
			{Slice: []byte("invalid protobuf data")},
		},
	}

	_, err := ConvertToOTLP(req)
	require.Error(t, err)
	require.Contains(t, err.Error(), "unmarshal")
}

func TestConvertToOTLP_NilRequest(t *testing.T) {
	_, err := ConvertToOTLP(nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "nil")
}

func TestDecodePushBytesRequest(t *testing.T) {
	// Create a valid ResourceSpans
	resourceSpans := &v1_trace.ResourceSpans{
		ScopeSpans: []*v1_trace.ScopeSpans{
			{
				Spans: []*v1_trace.Span{
					{Name: "test-span"},
				},
			},
		},
	}

	sliceData, err := resourceSpans.Marshal()
	require.NoError(t, err)

	// Create and marshal PushBytesRequest
	req := &tempopb.PushBytesRequest{
		Traces: []tempopb.PreallocBytes{
			{Slice: sliceData},
		},
	}

	data, err := req.Marshal()
	require.NoError(t, err)

	// Test helper function
	tracesData, err := DecodePushBytesRequest(data)
	require.NoError(t, err)
	require.NotNil(t, tracesData)
	require.Len(t, tracesData.ResourceSpans, 1)
	require.Equal(t, "test-span", tracesData.ResourceSpans[0].ScopeSpans[0].Spans[0].Name)
}

func TestDecodePushBytesRequest_InvalidData(t *testing.T) {
	_, err := DecodePushBytesRequest([]byte("invalid data"))
	require.Error(t, err)
	require.Contains(t, err.Error(), "unmarshal")
}
