package kafkaotlpforwarder

import (
	"fmt"

	"github.com/grafana/tempo/pkg/tempopb"
	v1_trace "github.com/grafana/tempo/pkg/tempopb/trace/v1"
)

// ConvertToOTLP converts a tempopb.PushBytesRequest to OTLP TracesData format.
// The Trace.Slice field contains pre-encoded OTLP ResourceSpans that need to be
// unmarshaled and aggregated.
func ConvertToOTLP(req *tempopb.PushBytesRequest) (*v1_trace.TracesData, error) {
	if req == nil {
		return nil, fmt.Errorf("nil request")
	}

	tracesData := &v1_trace.TracesData{
		ResourceSpans: make([]*v1_trace.ResourceSpans, 0, len(req.Traces)),
	}

	for i, trace := range req.Traces {
		if len(trace.Slice) == 0 {
			continue
		}

		var resourceSpans v1_trace.ResourceSpans
		if err := resourceSpans.Unmarshal(trace.Slice); err != nil {
			return nil, fmt.Errorf("unmarshal trace %d: %w", i, err)
		}

		tracesData.ResourceSpans = append(tracesData.ResourceSpans, &resourceSpans)
	}

	return tracesData, nil
}
