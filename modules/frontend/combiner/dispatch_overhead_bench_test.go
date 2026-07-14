package combiner

// dispatch_overhead_bench_test.go — BENCH-QP-010 (blockpack issue #217, Phase 2.3/4.4): measures
// the frontend-side per-job overhead of combining N slice-job responses, as a proxy for the cost
// forcing literal one-minute dispatch (#217) imposes on the query-frontend itself. This is
// deliberately scoped to the COMBINE step (marshal/unmarshal + AddResponse bookkeeping), not real
// network I/O — the frontend's actual HTTP/gRPC round-trip latency to a querier is dominated by
// the querier's own compute time and network RTT, neither of which scales with slice COUNT in a
// way this in-process benchmark could honestly represent. What DOES scale with slice count,
// purely as a function of the frontend process's own work, is exactly what this benchmark
// measures: unmarshaling each job's response body and folding it into the running combined
// result. Reusing this project's existing BenchmarkDeduper100/1000/10000/100000 convention
// (trace_by_id_deduper_test.go) for the job-count scaling shape.
import (
	"io"
	"net/http"
	"strconv"
	"strings"
	"testing"

	"github.com/gogo/protobuf/proto"
	"github.com/grafana/tempo/pkg/api"
	"github.com/grafana/tempo/pkg/tempopb"
)

// benchPipelineResponse is a minimal PipelineResponse for benchmark use (mirrors
// common_test.go's testPipelineResponse, duplicated here rather than shared across
// _test.go/_bench_test.go files' differing build tag needs is unnecessary here since both are
// plain _test.go files in the same package — reusing testPipelineResponse directly).
func benchSearchJobResponse(traceID string) PipelineResponse {
	pb := &tempopb.SearchResponse{
		Traces: []*tempopb.TraceSearchMetadata{{TraceID: traceID}},
	}
	body, _ := proto.Marshal(pb)
	return &testPipelineResponse{
		r: &http.Response{
			Body:       io.NopCloser(strings.NewReader(string(body))),
			StatusCode: 200,
			Header: http.Header{
				api.HeaderContentType: {string(api.MarshallingFormatProtobuf)},
			},
		},
	}
}

// benchmarkSearchCombinerNJobs feeds n synthetic single-trace 200-OK search job responses
// through a fresh NewSearch combiner's AddResponse, timing the whole fan-in. ns/op divided by n
// is the effective per-job overhead BENCH-QP-010 is measuring.
func benchmarkSearchCombinerNJobs(b *testing.B, n int) {
	jobs := make([]PipelineResponse, n)
	for i := 0; i < n; i++ {
		jobs[i] = benchSearchJobResponse(strconv.Itoa(i))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		c := NewSearch(0, false, api.MarshallingFormatProtobuf, false)
		for _, j := range jobs {
			if err := c.AddResponse(j); err != nil {
				b.Fatalf("AddResponse: %v", err)
			}
			// A real testPipelineResponse's Body is a one-shot io.Reader; rebuild a fresh one
			// each b.N iteration so every iteration replays the SAME n jobs from scratch.
		}
		// Rebuild jobs' bodies for the next b.N iteration (Body is drained above).
		for idx := range jobs {
			jobs[idx] = benchSearchJobResponse(strconv.Itoa(idx))
		}
	}
}

// BenchmarkSearchCombinerDispatchOverhead_10/100/500/2000/10000/50000 (BENCH-QP-010): slice
// counts spanning the plan's originally-requested {10,100,500,2000} plus the #217-raised
// maxSlicesPerPlan ceiling (50000) and its midpoint (10000), so the recorded numbers cover both
// the plan's own requested range and the actual new ceiling this task shipped.
func BenchmarkSearchCombinerDispatchOverhead_10(b *testing.B)    { benchmarkSearchCombinerNJobs(b, 10) }
func BenchmarkSearchCombinerDispatchOverhead_100(b *testing.B)   { benchmarkSearchCombinerNJobs(b, 100) }
func BenchmarkSearchCombinerDispatchOverhead_500(b *testing.B)   { benchmarkSearchCombinerNJobs(b, 500) }
func BenchmarkSearchCombinerDispatchOverhead_2000(b *testing.B)  { benchmarkSearchCombinerNJobs(b, 2000) }
func BenchmarkSearchCombinerDispatchOverhead_10000(b *testing.B) { benchmarkSearchCombinerNJobs(b, 10000) }
func BenchmarkSearchCombinerDispatchOverhead_50000(b *testing.B) { benchmarkSearchCombinerNJobs(b, 50000) }
