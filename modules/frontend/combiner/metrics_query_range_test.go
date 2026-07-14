package combiner

import (
	"math"
	"math/rand/v2"
	"strconv"
	"testing"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/grafana/tempo/modules/frontend/shardtracker"
	"github.com/grafana/tempo/pkg/api"
	"github.com/grafana/tempo/pkg/tempopb"
	v1 "github.com/grafana/tempo/pkg/tempopb/common/v1"
	"github.com/grafana/tempo/pkg/traceql"
	"github.com/stretchr/testify/require"
)

// TestQueryRangeCombiner_PropagatesPartialFromOneJobWithoutDiscardingOthers (#217 task 1.3) is
// the single most important test in Phase 1: it is the literal proof of the brainstorm-cited
// scenario — "some jobs succeed, one job hits a coverage gap, final response contains the
// successful jobs' results" AND carries PartialStatus=PARTIAL with a non-empty message. Feeds
// the combiner M real-data 200s and 1 coverage-gap-tolerant 200/PARTIAL/empty (exactly the shape
// backend_block.go's QueryRange now returns per Phase 1.1/slice_partial_response.go), then
// asserts the final response contains ALL of the real series AND Status=PARTIAL.
func TestQueryRangeCombiner_PropagatesPartialFromOneJobWithoutDiscardingOthers(t *testing.T) {
	start := uint64(1100 * time.Second)
	end := uint64(1300 * time.Second)
	step := traceql.DefaultQueryRangeStep(start, end)
	bar := &v1.AnyValue{Value: &v1.AnyValue_StringValue{StringValue: "bar"}}

	req := &tempopb.QueryRangeRequest{
		Query: "{} | rate()",
		Start: start,
		End:   end,
		Step:  step,
	}

	c, err := NewTypedQueryRange(req, 100)
	require.NoError(t, err)

	// M=2 jobs with real data.
	resp1 := &tempopb.QueryRangeResponse{
		Series: []*tempopb.TimeSeries{
			{
				Labels:  []v1.KeyValue{{Key: "foo", Value: bar}},
				Samples: []tempopb.Sample{{TimestampMs: 1200_000, Value: 2}},
			},
		},
	}
	resp2 := &tempopb.QueryRangeResponse{
		Series: []*tempopb.TimeSeries{
			{
				Labels:  []v1.KeyValue{{Key: "boo", Value: bar}},
				Samples: []tempopb.Sample{{TimestampMs: 1200_000, Value: 3}},
			},
		},
	}
	require.NoError(t, c.AddResponse(toHTTPResponseWithFormat(t, resp1, 200, 0, api.HeaderAcceptJSON)))
	require.NoError(t, c.AddResponse(toHTTPResponseWithFormat(t, resp2, 200, 0, api.HeaderAcceptJSON)))

	// The 1 coverage-gap-tolerant job: a normal 200 carrying Status=PARTIAL and a message, no
	// series at all — exactly what backend_block.go's QueryRange now returns for an indexOnly
	// slice job that tolerated a coverage-gap decline (Phase 1.1).
	partialResp := &tempopb.QueryRangeResponse{
		Status:  tempopb.PartialStatus_PARTIAL,
		Message: "no value-index coverage for this slice's window",
	}
	require.NoError(t, c.AddResponse(toHTTPResponseWithFormat(t, partialResp, 200, 0, api.HeaderAcceptJSON)))

	final, err := c.GRPCFinal()
	require.NoError(t, err)
	require.Len(t, final.Series, 2, "both real jobs' series must survive the partial job's contribution")
	require.Equal(t, tempopb.PartialStatus_PARTIAL, final.Status, "the final response must carry PARTIAL forward from the one declined job")
	require.NotEmpty(t, final.Message, "the final response must carry a non-empty message explaining the gap")
}

// TestQueryRangeCombiner_PartialMessagesDeduped_MultipleGapsSameReason (#217 task 1.3) pins the
// dedupe half of the plan's own stated requirement ("dedupe identical messages, cap total
// message length defensively"): N jobs reporting the IDENTICAL partial message must not repeat
// it N times in the final message.
func TestQueryRangeCombiner_PartialMessagesDeduped_MultipleGapsSameReason(t *testing.T) {
	start := uint64(1100 * time.Second)
	end := uint64(1300 * time.Second)
	step := traceql.DefaultQueryRangeStep(start, end)

	req := &tempopb.QueryRangeRequest{Query: "{} | rate()", Start: start, End: end, Step: step}
	c, err := NewTypedQueryRange(req, 100)
	require.NoError(t, err)

	const msg = "no value-index coverage for this slice's window"
	for i := 0; i < 3; i++ {
		partialResp := &tempopb.QueryRangeResponse{Status: tempopb.PartialStatus_PARTIAL, Message: msg}
		require.NoError(t, c.AddResponse(toHTTPResponseWithFormat(t, partialResp, 200, 0, api.HeaderAcceptJSON)))
	}

	final, err := c.GRPCFinal()
	require.NoError(t, err)
	require.Equal(t, tempopb.PartialStatus_PARTIAL, final.Status)
	require.Equal(t, msg, final.Message, "identical messages from multiple declined jobs must be deduplicated, not repeated")
}

// TestQueryRangeCombiner_CoverageGapPartial_DoesNotTriggerEarlyQuit (#217 task 1.3, a finding
// surfaced by TDD-ing task 1.3's own test): pkg/traceql.QueryRangeCombiner.Combine treats ANY
// incoming resp.Status==PARTIAL as evidence of series-count truncation and flips
// maxSeriesReached, which gates this combiner's `quit` early-stop optimization
// (modules/frontend/combiner/metrics_query_range.go). Left unguarded, a single benign
// coverage-gap-tolerant job (Phase 1.1) arriving early would incorrectly make the frontend STOP
// waiting for the remaining real-data shards — truncating a correct answer instead of merely
// annotating it partial, exactly the over-declining failure mode #217's own Phase 1.5 boundary
// test is concerned with. This test pins that a coverage-gap PARTIAL job, even alongside shard
// completion metadata, does NOT trip ShouldQuit() on its own — only a genuine max-series
// overflow may.
func TestQueryRangeCombiner_CoverageGapPartial_DoesNotTriggerEarlyQuit(t *testing.T) {
	start := uint64(1100 * time.Second)
	end := uint64(1300 * time.Second)
	step := traceql.DefaultQueryRangeStep(start, end)

	req := &tempopb.QueryRangeRequest{Query: "{} | rate()", Start: start, End: end, Step: step, MaxSeries: 100}
	c, err := NewQueryRange(req, 100)
	require.NoError(t, err)

	// Shard completion metadata IS present, and TotalJobs:1 means the single job response
	// below (shardIdx 0, the default responseData toHTTPResponseWithFormat assigns) completes
	// the shard immediately, making completedThrough != Unknown right away — the OTHER half of
	// quit's condition — isolating this test to prove the coverage-gap signal alone isn't
	// sufficient to trip quit.
	metadata := &QueryRangeJobResponse{
		JobMetadata: shardtracker.JobMetadata{
			TotalJobs: 1,
			Shards:    []shardtracker.Shard{{TotalJobs: 1, CompletedThroughSeconds: 1200}},
		},
	}
	require.NoError(t, c.AddResponse(metadata))

	partialResp := &tempopb.QueryRangeResponse{
		Status:  tempopb.PartialStatus_PARTIAL,
		Message: "no value-index coverage for this slice's window",
	}
	require.NoError(t, c.AddResponse(toHTTPResponseWithFormat(t, partialResp, 200, 0, api.HeaderAcceptJSON)))

	require.False(t, c.ShouldQuit(),
		"a coverage-gap-tolerant PARTIAL job must not trigger the max-series early-quit optimization on its own")
}

// TestQueryRangeCombiner_GenuineErrorStillFailsWholeQuery (#217 task 1.4, the safety-net
// regression test explicitly required before Phase 1 is considered done): a real, NON-coverage
// 500 from one job alongside successful jobs must still fail the WHOLE query, unchanged by
// Phase 1's new PartialStatus-propagation logic. Phase 1's scope is narrow — tolerating a
// coverage-gap decline that arrives as an ordinary 200 — and must never be mistaken for
// widening tolerance to any other error class.
func TestQueryRangeCombiner_GenuineErrorStillFailsWholeQuery(t *testing.T) {
	start := uint64(1100 * time.Second)
	end := uint64(1300 * time.Second)
	step := traceql.DefaultQueryRangeStep(start, end)
	bar := &v1.AnyValue{Value: &v1.AnyValue_StringValue{StringValue: "bar"}}

	req := &tempopb.QueryRangeRequest{Query: "{} | rate()", Start: start, End: end, Step: step}
	c, err := NewTypedQueryRange(req, 100)
	require.NoError(t, err)

	resp1 := &tempopb.QueryRangeResponse{
		Series: []*tempopb.TimeSeries{
			{Labels: []v1.KeyValue{{Key: "foo", Value: bar}}, Samples: []tempopb.Sample{{TimestampMs: 1200_000, Value: 2}}},
		},
	}
	require.NoError(t, c.AddResponse(toHTTPResponseWithFormat(t, resp1, 200, 0, api.HeaderAcceptJSON)))
	require.NoError(t, c.AddResponse(toHTTPResponseWithFormat(t, &tempopb.QueryRangeResponse{}, 500, 0, api.HeaderAcceptJSON)))

	_, err = c.GRPCFinal()
	require.Error(t, err, "a genuine 500 from one job must still fail the whole query, exactly as before Phase 1")
}

func TestAttachExemplars(t *testing.T) {
	start := uint64(10 * time.Second)
	end := uint64(20 * time.Second)
	step := traceql.DefaultQueryRangeStep(start, end)

	req := &tempopb.QueryRangeRequest{
		Start: start,
		End:   end,
		Step:  step,
	}

	tcs := []struct {
		name    string
		include func(i int) bool
	}{
		{
			name:    "include all",
			include: func(_ int) bool { return true },
		},
		{
			name:    "include none",
			include: func(_ int) bool { return false },
		},
		{
			name:    "include every other",
			include: func(i int) bool { return i%2 == 0 },
		},
		{
			name:    "include rando",
			include: func(_ int) bool { return rand.Int()%2 == 0 },
		},
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			resp, expectedSeries := buildSeriesForExemplarTest(start, end, step, tc.include)

			attachExemplars(req, resp)
			require.Equal(t, expectedSeries, resp.Series)
		})
	}
}

func BenchmarkAttachExemplars(b *testing.B) {
	start := uint64(1 * time.Second)
	end := uint64(10000 * time.Second)
	step := uint64(time.Second)

	req := &tempopb.QueryRangeRequest{
		Start: start,
		End:   end,
		Step:  step,
	}

	resp, _ := buildSeriesForExemplarTest(start, end, step, func(_ int) bool { return true })

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		attachExemplars(req, resp)
	}
}

func buildSeriesForExemplarTest(start, end, step uint64, include func(i int) bool) (*tempopb.QueryRangeResponse, []*tempopb.TimeSeries) {
	resp := &tempopb.QueryRangeResponse{
		Series: []*tempopb.TimeSeries{
			{},
		},
	}

	expectedSeries := []*tempopb.TimeSeries{
		{},
	}

	// populate series and expected series based on step
	idx := 0
	for i := start; i < end; i += step {
		idx++
		tsMS := int64(i / uint64(time.Millisecond))
		val := float64(idx)

		sample := tempopb.Sample{
			TimestampMs: tsMS,
			Value:       val,
		}
		nanExemplar := tempopb.Exemplar{
			TimestampMs: tsMS,
			Value:       math.NaN(),
		}
		valExamplar := tempopb.Exemplar{
			TimestampMs: tsMS,
			Value:       val,
		}

		includeExemplar := include(idx)

		// copy the sample and nan exemplar into the response. the nan exemplar should be overwritten
		resp.Series[0].Samples = append(resp.Series[0].Samples, sample)
		if includeExemplar {
			resp.Series[0].Exemplars = append(resp.Series[0].Exemplars, nanExemplar)
		}

		// copy the sample and val exemplar into the expected response
		expectedSeries[0].Samples = append(expectedSeries[0].Samples, sample)
		if includeExemplar {
			expectedSeries[0].Exemplars = append(expectedSeries[0].Exemplars, valExamplar)
		}
	}

	return resp, expectedSeries
}

func TestSegmentQueryRangeResponseToMaxPacketSize(t *testing.T) {
	input := &tempopb.QueryRangeResponse{
		Metrics: &tempopb.SearchMetrics{},
		Series: []*tempopb.TimeSeries{
			{
				Labels: []v1.KeyValue{
					{Key: "name", Value: &v1.AnyValue{Value: &v1.AnyValue_StringValue{StringValue: "series0"}}},
				},
				Samples: []tempopb.Sample{{TimestampMs: 1000, Value: 1}},
			},
			{
				Labels: []v1.KeyValue{
					{Key: "name", Value: &v1.AnyValue{Value: &v1.AnyValue_StringValue{StringValue: "series1"}}},
				},
				Samples: []tempopb.Sample{{TimestampMs: 2000, Value: 2}},
			},
			{
				Labels: []v1.KeyValue{
					{Key: "name", Value: &v1.AnyValue{Value: &v1.AnyValue_StringValue{StringValue: "series2"}}},
				},
				Samples: []tempopb.Sample{{TimestampMs: 3000, Value: 3}},
			},
		},
	}

	t.Run("fits", func(t *testing.T) {
		// Generate a test packet size that is large enough for only part of the data.
		maxSize := (&tempopb.QueryRangeResponse{
			Metrics: input.Metrics,
			Series:  input.Series[:2],
		}).Size()
		out := segmentQueryRangeResponseToMaxPacketSize(input, maxSize)

		require.Len(t, out, 2, "expected 2 responses")
		requireProtoSegmentsFit(t, out, maxSize)
		require.Len(t, out[0].Series, 2)
		require.Len(t, out[1].Series, 1)
		require.Equal(t, input.Series[0], out[0].Series[0])
		require.Equal(t, input.Series[1], out[0].Series[1])
		require.Equal(t, input.Series[2], out[1].Series[0])
		// Metrics repeated in each segment
		require.Equal(t, input.Metrics, out[0].Metrics)
		require.Equal(t, input.Metrics, out[1].Metrics)
	})

	t.Run("at least one", func(t *testing.T) {
		// This is smaller than every item but will test logic to ensure we always send at least one item even if too big
		const maxSize = 1
		out := segmentQueryRangeResponseToMaxPacketSize(input, maxSize)

		require.Len(t, out, 3, "expected 3 responses")
		require.Len(t, out[0].Series, 1)
		require.Len(t, out[1].Series, 1)
		require.Len(t, out[2].Series, 1)
		require.Equal(t, input.Series[0], out[0].Series[0])
		require.Equal(t, input.Series[1], out[1].Series[0])
		require.Equal(t, input.Series[2], out[2].Series[0])
		require.Equal(t, input.Metrics, out[0].Metrics)
		require.Equal(t, input.Metrics, out[1].Metrics)
		require.Equal(t, input.Metrics, out[2].Metrics)
	})
}

func TestQueryRangemaxSeriesShouldQuit(t *testing.T) {
	start := uint64(1100 * time.Second)
	end := uint64(1300 * time.Second)
	step := traceql.DefaultQueryRangeStep(start, end)
	bar := &v1.AnyValue{Value: &v1.AnyValue_StringValue{StringValue: "bar"}}

	req := &tempopb.QueryRangeRequest{
		Query:     "{} | rate()",
		Start:     start,
		End:       end,
		Step:      step,
		MaxSeries: 4,
	}

	queryRangeCombiner, err := NewQueryRange(req, 4)
	require.NoError(t, err)

	// Add metadata that establishes shard information
	metadata := &QueryRangeJobResponse{
		JobMetadata: shardtracker.JobMetadata{
			TotalBlocks: 10,
			TotalJobs:   2,
			TotalBytes:  1000,
			Shards: []shardtracker.Shard{
				{
					TotalJobs:               2,
					CompletedThroughSeconds: 1200,
				},
			},
		},
	}

	err = queryRangeCombiner.AddResponse(metadata)
	require.NoError(t, err)

	// add 3 series, should not quit
	resp := &tempopb.QueryRangeResponse{
		Metrics: &tempopb.SearchMetrics{
			InspectedTraces: 1,
			InspectedBytes:  1,
		},
		Series: []*tempopb.TimeSeries{
			{
				Labels: []v1.KeyValue{
					{Key: "foo", Value: bar},
				},
				Samples: []tempopb.Sample{
					{
						TimestampMs: 1200_000,
						Value:       2,
					},
				},
			},
			{
				Labels: []v1.KeyValue{
					{Key: "boo", Value: bar},
				},
				Samples: []tempopb.Sample{
					{
						TimestampMs: 1200_000,
						Value:       2,
					},
				},
			},
			{
				Labels: []v1.KeyValue{
					{Key: "moo", Value: bar},
				},
				Samples: []tempopb.Sample{
					{
						TimestampMs: 1200_000,
						Value:       2,
					},
				},
			},
		},
	}

	err = queryRangeCombiner.AddResponse(toHTTPResponseWithFormat(t, resp, 200, 0, api.HeaderAcceptJSON))
	require.NoError(t, err)
	require.False(t, queryRangeCombiner.ShouldQuit())

	// add 4th & 5th series, should quit after shard completes
	secondResp := &tempopb.QueryRangeResponse{
		Metrics: &tempopb.SearchMetrics{
			InspectedTraces: 1,
			InspectedBytes:  1,
		},
		Series: []*tempopb.TimeSeries{
			{
				Labels: []v1.KeyValue{
					{Key: "woo", Value: bar},
				},
				Samples: []tempopb.Sample{
					{
						TimestampMs: 1200_000,
						Value:       2,
					},
				},
			},
			{
				Labels: []v1.KeyValue{
					{Key: "zoo", Value: bar},
				},
				Samples: []tempopb.Sample{
					{
						TimestampMs: 1200_000,
						Value:       2,
					},
				},
			},
		},
	}

	err = queryRangeCombiner.AddResponse(toHTTPResponseWithFormat(t, secondResp, 200, 0, api.HeaderAcceptJSON))
	require.NoError(t, err)
	require.True(t, queryRangeCombiner.ShouldQuit())
}

func TestQueryRangeMaxSeriesQuitRequiresCompletedShards(t *testing.T) {
	start := uint64(1100 * time.Second)
	end := uint64(1300 * time.Second)
	step := traceql.DefaultQueryRangeStep(start, end)
	bar := &v1.AnyValue{Value: &v1.AnyValue_StringValue{StringValue: "bar"}}

	req := &tempopb.QueryRangeRequest{
		Query:     "{} | rate()",
		Start:     start,
		End:       end,
		Step:      step,
		MaxSeries: 2,
	}

	t.Run("max series reached but no shards completed should not quit", func(t *testing.T) {
		queryRangeCombiner, err := NewQueryRange(req, 2)
		require.NoError(t, err)

		// Add responses that exceed max series limit but without any metadata about completed shards
		// This simulates the case where we've received enough data to hit the limit
		// but haven't completed any shards yet
		resp := &tempopb.QueryRangeResponse{
			Metrics: &tempopb.SearchMetrics{
				InspectedTraces: 1,
				InspectedBytes:  1,
			},
			Series: []*tempopb.TimeSeries{
				{
					Labels: []v1.KeyValue{
						{Key: "series1", Value: bar},
					},
					Samples: []tempopb.Sample{
						{TimestampMs: 1200_000, Value: 1.0},
					},
				},
				{
					Labels: []v1.KeyValue{
						{Key: "series2", Value: bar},
					},
					Samples: []tempopb.Sample{
						{TimestampMs: 1200_000, Value: 2.0},
					},
				},
				{
					Labels: []v1.KeyValue{
						{Key: "series3", Value: bar},
					},
					Samples: []tempopb.Sample{
						{TimestampMs: 1200_000, Value: 3.0},
					},
				},
			},
		}

		err = queryRangeCombiner.AddResponse(toHTTPResponse(t, resp, 200))
		require.NoError(t, err)

		// Even though we've exceeded max series (3 > 2), we should NOT quit
		// because no shards have been marked as completed yet
		require.False(t, queryRangeCombiner.ShouldQuit())
	})

	t.Run("max series reached with completed shards should quit", func(t *testing.T) {
		queryRangeCombiner, err := NewQueryRange(req, 2)
		require.NoError(t, err)

		// First, add metadata that establishes shard information
		metadata := &QueryRangeJobResponse{
			JobMetadata: shardtracker.JobMetadata{
				TotalBlocks: 10,
				TotalJobs:   5,
				TotalBytes:  1000,
				Shards: []shardtracker.Shard{
					{
						TotalJobs:               2,
						CompletedThroughSeconds: 1200,
					},
					{
						TotalJobs:               3,
						CompletedThroughSeconds: 1250,
					},
				},
			},
		}

		err = queryRangeCombiner.AddResponse(metadata)
		require.NoError(t, err)

		// Add responses for the first shard with shard index to simulate completion
		resp1 := &tempopb.QueryRangeResponse{
			Metrics: &tempopb.SearchMetrics{
				InspectedTraces: 1,
				InspectedBytes:  1,
			},
			Series: []*tempopb.TimeSeries{
				{
					Labels: []v1.KeyValue{
						{Key: "series1", Value: bar},
					},
					Samples: []tempopb.Sample{
						{TimestampMs: 1200_000, Value: 1.0},
					},
				},
			},
		}

		// Add first response for shard 0
		err = queryRangeCombiner.AddResponse(toHTTPResponseWithFormat(t, resp1, 200, 0, api.HeaderAcceptJSON))
		require.NoError(t, err)
		require.False(t, queryRangeCombiner.ShouldQuit())

		// Add second response for shard 0 with more series, pushing us over the limit
		resp2 := &tempopb.QueryRangeResponse{
			Metrics: &tempopb.SearchMetrics{
				InspectedTraces: 1,
				InspectedBytes:  1,
			},
			Series: []*tempopb.TimeSeries{
				{
					Labels: []v1.KeyValue{
						{Key: "series2", Value: bar},
					},
					Samples: []tempopb.Sample{
						{TimestampMs: 1200_000, Value: 2.0},
					},
				},
				{
					Labels: []v1.KeyValue{
						{Key: "series3", Value: bar},
					},
					Samples: []tempopb.Sample{
						{TimestampMs: 1200_000, Value: 3.0},
					},
				},
			},
		}

		err = queryRangeCombiner.AddResponse(toHTTPResponseWithFormat(t, resp2, 200, 0, api.HeaderAcceptJSON))
		require.NoError(t, err)

		// Now we've exceeded max series (3 > 2) AND completed shard 0 (2 jobs received),
		// so we SHOULD quit
		require.True(t, queryRangeCombiner.ShouldQuit())
	})
}

func BenchmarkMarshalOnly(b *testing.B) {
	_, curr := seriesWithTenPercentDiff()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := proto.Marshal(curr)
		require.NoError(b, err)
	}
}

func seriesWithTenPercentDiff() (*tempopb.QueryRangeResponse, *tempopb.QueryRangeResponse) {
	a := &tempopb.QueryRangeResponse{}
	b := &tempopb.QueryRangeResponse{}

	numSeries := 1000
	numSamples := 1000

	for s := range numSeries {
		aSamples := make([]tempopb.Sample, numSamples)
		bSamples := make([]tempopb.Sample, numSamples)

		for i := range 1000 {
			aSamples[i] = tempopb.Sample{
				TimestampMs: int64(i) * 1000,
				Value:       rand.Float64(),
			}

			// 10% of samples are different
			if i%10 == 0 {
				bSamples[i] = tempopb.Sample{
					TimestampMs: int64(i) * 1000,
					Value:       rand.Float64(),
				}
			} else {
				bSamples[i] = aSamples[i]
			}
		}

		a.Series = append(a.Series, ts(aSamples, nil, "foo"+strconv.Itoa(s), "bar"))
		b.Series = append(b.Series, ts(bSamples, nil, "foo"+strconv.Itoa(s), "bar"))

	}

	return a, b
}

func ts(samples []tempopb.Sample, exemplars []tempopb.Exemplar, kvs ...string) *tempopb.TimeSeries {
	ts := &tempopb.TimeSeries{
		Samples:   samples,
		Exemplars: exemplars,
		Labels:    []v1.KeyValue{},
	}

	for i := 0; i < len(kvs); i += 2 {
		ts.Labels = append(ts.Labels, v1.KeyValue{
			Key: kvs[i],
			Value: &v1.AnyValue{
				Value: &v1.AnyValue_StringValue{
					StringValue: kvs[i+1],
				},
			},
		})
	}

	if samples == nil {
		ts.Samples = []tempopb.Sample{}
	}
	if exemplars == nil {
		ts.Exemplars = []tempopb.Exemplar{}
	}

	return ts
}

func TestTrimSeriesToCompletedWindow(t *testing.T) {
	tests := []struct {
		name                     string
		inputSamples             []tempopb.Sample
		inputExemplars           []tempopb.Exemplar
		lastCompletedThroughSecs uint32
		completedThroughSecs     uint32
		expectedSamples          []tempopb.Sample
		expectedExemplars        []tempopb.Exemplar
	}{
		{
			name: "basic window filtering",
			inputSamples: []tempopb.Sample{
				{TimestampMs: 5000, Value: 1.0},
				{TimestampMs: 10000, Value: 2.0},
				{TimestampMs: 15000, Value: 3.0},
				{TimestampMs: 20000, Value: 4.0},
				{TimestampMs: 25000, Value: 5.0},
			},
			inputExemplars: []tempopb.Exemplar{
				{TimestampMs: 5000, Value: 1.0},
				{TimestampMs: 15000, Value: 3.0},
				{TimestampMs: 25000, Value: 5.0},
			},
			lastCompletedThroughSecs: 20, // 20000ms
			completedThroughSecs:     10, // 10000ms
			expectedSamples: []tempopb.Sample{
				{TimestampMs: 15000, Value: 3.0},
				{TimestampMs: 20000, Value: 4.0}, // completedThrough is inclusive
			},
			expectedExemplars: []tempopb.Exemplar{
				{TimestampMs: 15000, Value: 3.0},
			},
		},
		{
			name: "no samples in window",
			inputSamples: []tempopb.Sample{
				{TimestampMs: 5000, Value: 1.0},
				{TimestampMs: 10000, Value: 2.0},
				{TimestampMs: 30000, Value: 4.0},
			},
			inputExemplars:           []tempopb.Exemplar{},
			lastCompletedThroughSecs: 20,
			completedThroughSecs:     10,
			expectedSamples:          []tempopb.Sample{},
			expectedExemplars:        []tempopb.Exemplar{},
		},
		{
			name: "all samples in window",
			inputSamples: []tempopb.Sample{
				{TimestampMs: 11000, Value: 1.0},
				{TimestampMs: 15000, Value: 2.0},
				{TimestampMs: 19000, Value: 3.0},
			},
			inputExemplars: []tempopb.Exemplar{
				{TimestampMs: 12000, Value: 1.0},
				{TimestampMs: 18000, Value: 2.0},
			},
			lastCompletedThroughSecs: 20,
			completedThroughSecs:     10,
			expectedSamples: []tempopb.Sample{
				{TimestampMs: 11000, Value: 1.0},
				{TimestampMs: 15000, Value: 2.0},
				{TimestampMs: 19000, Value: 3.0},
			},
			expectedExemplars: []tempopb.Exemplar{
				{TimestampMs: 12000, Value: 1.0},
				{TimestampMs: 18000, Value: 2.0},
			},
		},
		{
			name:                     "empty series",
			inputSamples:             []tempopb.Sample{},
			inputExemplars:           []tempopb.Exemplar{},
			lastCompletedThroughSecs: 20,
			completedThroughSecs:     10,
			expectedSamples:          []tempopb.Sample{},
			expectedExemplars:        []tempopb.Exemplar{},
		},
		{
			name: "boundary conditions - samples at exact timestamps",
			inputSamples: []tempopb.Sample{
				{TimestampMs: 10000, Value: 1.0}, // exactly at lastCompletedThrough (exclusive)
				{TimestampMs: 15000, Value: 2.0},
				{TimestampMs: 20000, Value: 3.0}, // exactly at completedThrough (inclusive)
				{TimestampMs: 25000, Value: 4.0},
			},
			inputExemplars:           []tempopb.Exemplar{},
			lastCompletedThroughSecs: 20,
			completedThroughSecs:     10,
			expectedSamples: []tempopb.Sample{
				{TimestampMs: 15000, Value: 2.0},
				{TimestampMs: 20000, Value: 3.0}, // completedThrough is inclusive
			},
			expectedExemplars: []tempopb.Exemplar{},
		},
		{
			name: "zero timestamps",
			inputSamples: []tempopb.Sample{
				{TimestampMs: 0, Value: 1.0},
				{TimestampMs: 5000, Value: 2.0},
				{TimestampMs: 10000, Value: 3.0},
			},
			inputExemplars:           []tempopb.Exemplar{},
			lastCompletedThroughSecs: 8,
			completedThroughSecs:     0,
			expectedSamples: []tempopb.Sample{
				{TimestampMs: 5000, Value: 2.0},
			},
			expectedExemplars: []tempopb.Exemplar{},
		},
		{
			name: "multiple series",
			inputSamples: []tempopb.Sample{
				{TimestampMs: 5000, Value: 1.0},
				{TimestampMs: 15000, Value: 2.0},
				{TimestampMs: 25000, Value: 3.0},
			},
			inputExemplars: []tempopb.Exemplar{
				{TimestampMs: 5000, Value: 1.0},
				{TimestampMs: 15000, Value: 2.0},
				{TimestampMs: 25000, Value: 3.0},
			},
			lastCompletedThroughSecs: 20,
			completedThroughSecs:     10,
			expectedSamples: []tempopb.Sample{
				{TimestampMs: 15000, Value: 2.0},
			},
			expectedExemplars: []tempopb.Exemplar{
				{TimestampMs: 15000, Value: 2.0},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			series := []*tempopb.TimeSeries{
				ts(tt.inputSamples, tt.inputExemplars, "test", "value"),
			}

			trimSeriesToCompletedWindow(series, tt.lastCompletedThroughSecs, tt.completedThroughSecs)

			require.Equal(t, tt.expectedSamples, series[0].Samples, "samples mismatch")
			require.Equal(t, tt.expectedExemplars, series[0].Exemplars, "exemplars mismatch")
		})
	}
}

func TestTrimSeriesToCompletedWindow_MultipleSeries(t *testing.T) {
	series := []*tempopb.TimeSeries{
		ts([]tempopb.Sample{
			{TimestampMs: 5000, Value: 1.0},
			{TimestampMs: 15000, Value: 2.0},
			{TimestampMs: 25000, Value: 3.0},
		}, []tempopb.Exemplar{
			{TimestampMs: 15000, Value: 2.0},
		}, "series", "one"),
		ts([]tempopb.Sample{
			{TimestampMs: 8000, Value: 4.0},
			{TimestampMs: 12000, Value: 5.0},
			{TimestampMs: 18000, Value: 6.0},
		}, []tempopb.Exemplar{
			{TimestampMs: 12000, Value: 5.0},
			{TimestampMs: 18000, Value: 6.0},
		}, "series", "two"),
	}

	trimSeriesToCompletedWindow(series, 20, 10)

	// First series should have only the sample at 15000
	require.Equal(t, []tempopb.Sample{
		{TimestampMs: 15000, Value: 2.0},
	}, series[0].Samples)
	require.Equal(t, []tempopb.Exemplar{
		{TimestampMs: 15000, Value: 2.0},
	}, series[0].Exemplars)

	// Second series should have samples at 12000 and 18000
	require.Equal(t, []tempopb.Sample{
		{TimestampMs: 12000, Value: 5.0},
		{TimestampMs: 18000, Value: 6.0},
	}, series[1].Samples)
	require.Equal(t, []tempopb.Exemplar{
		{TimestampMs: 12000, Value: 5.0},
		{TimestampMs: 18000, Value: 6.0},
	}, series[1].Exemplars)
}
