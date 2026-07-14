package frontend

import (
	"context"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"github.com/go-kit/log"
	"github.com/gogo/protobuf/jsonpb"
	"github.com/google/uuid"
	"github.com/grafana/blockpack"
	"github.com/grafana/dskit/user"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/model"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/grafana/tempo/modules/frontend/combiner"
	"github.com/grafana/tempo/modules/frontend/pipeline"
	"github.com/grafana/tempo/modules/frontend/shardtracker"
	"github.com/grafana/tempo/modules/overrides"
	"github.com/grafana/tempo/pkg/api"
	"github.com/grafana/tempo/pkg/tempopb"
	"github.com/grafana/tempo/pkg/traceql"
	"github.com/grafana/tempo/tempodb"
	"github.com/grafana/tempo/tempodb/backend"
	"github.com/grafana/tempo/tempodb/blocklist"
	"github.com/grafana/tempo/tempodb/encoding/common"
	"github.com/grafana/tempo/tempodb/encoding/vblockpack"
)

var _ tempodb.Reader = (*mockReader)(nil)

// implements tempodb.Reader interface
type mockReader struct {
	metas   []*backend.BlockMeta
	tenants []string
}

func (m *mockReader) SearchTags(context.Context, *backend.BlockMeta, *tempopb.SearchTagsBlockRequest, common.SearchOptions) (*tempopb.SearchTagsV2Response, error) {
	return nil, nil
}

func (m *mockReader) SearchTagValues(context.Context, *backend.BlockMeta, *tempopb.SearchTagValuesBlockRequest, common.SearchOptions) (*tempopb.SearchTagValuesResponse, error) {
	return nil, nil
}

func (m *mockReader) SearchTagValuesV2(context.Context, *backend.BlockMeta, *tempopb.SearchTagValuesRequest, common.SearchOptions) (*tempopb.SearchTagValuesV2Response, error) {
	return nil, nil
}

func (m *mockReader) FetchTagValues(context.Context, *backend.BlockMeta, traceql.FetchTagValuesRequest, traceql.FetchTagValuesCallback, common.MetricsCallback, common.SearchOptions) error {
	return nil
}

func (m *mockReader) Find(context.Context, string, common.ID, string, string, time.Time, time.Time, common.SearchOptions) ([]*tempopb.TraceByIDResponse, []error, error) {
	return nil, nil, nil
}

func (m *mockReader) BlockMeta(context.Context, string, backend.UUID) (*backend.BlockMeta, *backend.CompactedBlockMeta, error) {
	return nil, nil, nil
}

func (m *mockReader) BlockMetas(string) []*backend.BlockMeta {
	return m.metas
}

func (m *mockReader) Tenants() []string {
	return m.tenants
}

func (m *mockReader) Search(context.Context, *backend.BlockMeta, *tempopb.SearchRequest, common.SearchOptions) (*tempopb.SearchResponse, error) {
	return nil, nil
}

func (m *mockReader) Fetch(context.Context, *backend.BlockMeta, traceql.FetchSpansRequest, common.SearchOptions) (traceql.FetchSpansResponse, error) {
	return traceql.FetchSpansResponse{}, nil
}

func (m *mockReader) FetchSpans(context.Context, *backend.BlockMeta, traceql.FetchSpansRequest, common.SearchOptions) (traceql.FetchSpansOnlyResponse, error) {
	return traceql.FetchSpansOnlyResponse{}, nil
}

func (m *mockReader) FetchTagNames(context.Context, *backend.BlockMeta, traceql.FetchTagsRequest, traceql.FetchTagsCallback, common.MetricsCallback, common.SearchOptions) error {
	return nil
}

func (m *mockReader) QueryRange(_ context.Context, _ *backend.BlockMeta, _ *tempopb.QueryRangeRequest, _ common.SearchOptions) (*tempopb.QueryRangeResponse, error) {
	return nil, nil
}
func (m *mockReader) EnablePolling(context.Context, blocklist.JobSharder, bool) {}
func (m *mockReader) PollNow(context.Context)                                   {}
func (m *mockReader) PollNotification(context.Context) <-chan struct{}          { return nil }
func (m *mockReader) Shutdown()                                                 {}

//nolint:all deprecated

func TestBuildBackendRequests(t *testing.T) {
	tests := []struct {
		targetBytesPerRequest int
		metas                 []*backend.BlockMeta
		expectedURIs          []string
	}{
		{
			expectedURIs: []string{},
		},
		// block with no size
		{
			metas: []*backend.BlockMeta{
				{
					BlockID: backend.MustParse("00000000-0000-0000-0000-000000000000"),
				},
			},
			expectedURIs: []string{},
		},
		// block with no records
		{
			metas: []*backend.BlockMeta{
				{
					Size_:   1000,
					BlockID: backend.MustParse("00000000-0000-0000-0000-000000000000"),
				},
			},
			expectedURIs: []string{},
		},
		// meta.json fields
		{
			targetBytesPerRequest: 1000,
			metas: []*backend.BlockMeta{
				{
					Size_:         1000,
					TotalRecords:  100,
					BlockID:       backend.MustParse("00000000-0000-0000-0000-000000000000"),
					IndexPageSize: 13,
					Version:       "glarg",
				},
			},
			expectedURIs: []string{
				"/querier?blockID=00000000-0000-0000-0000-000000000000&encoding=none&end=20&footerSize=0&indexPageSize=13&pagesToSearch=100&size=1000&spss=3&start=10&startPage=0&totalRecords=100&version=glarg",
			},
		},
		// meta.json with dedicated columns
		{
			targetBytesPerRequest: 1000,
			metas: []*backend.BlockMeta{
				{
					Size_:         1000,
					TotalRecords:  10,
					BlockID:       backend.MustParse("00000000-0000-0000-0000-000000000000"),
					IndexPageSize: 13,
					Version:       "vParquet3",
					DedicatedColumns: backend.DedicatedColumns{
						{Scope: "span", Name: "net.sock.host.addr", Type: "string"},
					},
				},
			},
			expectedURIs: []string{
				"/querier?blockID=00000000-0000-0000-0000-000000000000&dc=%5B%7B%22name%22%3A%22net.sock.host.addr%22%7D%5D&encoding=none&end=20&footerSize=0&indexPageSize=13&pagesToSearch=10&size=1000&spss=3&start=10&startPage=0&totalRecords=10&version=vParquet3",
			},
		},
		// bytes/per request is too small for the page size
		{
			targetBytesPerRequest: 1,
			metas: []*backend.BlockMeta{
				{
					Size_:        1000,
					TotalRecords: 3,
					BlockID:      backend.MustParse("00000000-0000-0000-0000-000000000000"),
				},
			},
			expectedURIs: []string{
				"/querier?blockID=00000000-0000-0000-0000-000000000000&encoding=none&end=20&footerSize=0&indexPageSize=0&pagesToSearch=1&size=1000&spss=3&start=10&startPage=0&totalRecords=3&version=",
				"/querier?blockID=00000000-0000-0000-0000-000000000000&encoding=none&end=20&footerSize=0&indexPageSize=0&pagesToSearch=1&size=1000&spss=3&start=10&startPage=1&totalRecords=3&version=",
				"/querier?blockID=00000000-0000-0000-0000-000000000000&encoding=none&end=20&footerSize=0&indexPageSize=0&pagesToSearch=1&size=1000&spss=3&start=10&startPage=2&totalRecords=3&version=",
			},
		},
		// 100 pages, 10 bytes per page, 1k allowed per request
		{
			targetBytesPerRequest: 1000,
			metas: []*backend.BlockMeta{
				{
					Size_:        1000,
					TotalRecords: 100,
					BlockID:      backend.MustParse("00000000-0000-0000-0000-000000000000"),
				},
			},
			expectedURIs: []string{
				"/querier?blockID=00000000-0000-0000-0000-000000000000&encoding=none&end=20&footerSize=0&indexPageSize=0&pagesToSearch=100&size=1000&spss=3&start=10&startPage=0&totalRecords=100&version=",
			},
		},
		// 100 pages, 10 bytes per page, 900 allowed per request
		{
			targetBytesPerRequest: 900,
			metas: []*backend.BlockMeta{
				{
					Size_:        1000,
					TotalRecords: 100,
					BlockID:      backend.MustParse("00000000-0000-0000-0000-000000000000"),
				},
			},
			expectedURIs: []string{
				"/querier?blockID=00000000-0000-0000-0000-000000000000&encoding=none&end=20&footerSize=0&indexPageSize=0&pagesToSearch=90&size=1000&spss=3&start=10&startPage=0&totalRecords=100&version=",
				"/querier?blockID=00000000-0000-0000-0000-000000000000&encoding=none&end=20&footerSize=0&indexPageSize=0&pagesToSearch=90&size=1000&spss=3&start=10&startPage=90&totalRecords=100&version=",
			},
		},
		// two blocks
		{
			targetBytesPerRequest: 900,
			metas: []*backend.BlockMeta{
				{
					Size_:        1000,
					TotalRecords: 100,
					BlockID:      backend.MustParse("00000000-0000-0000-0000-000000000000"),
				},
				{
					Size_:        1000,
					TotalRecords: 200,
					BlockID:      backend.MustParse("00000000-0000-0000-0000-000000000001"),
				},
			},
			expectedURIs: []string{
				"/querier?blockID=00000000-0000-0000-0000-000000000000&encoding=none&end=20&footerSize=0&indexPageSize=0&pagesToSearch=90&size=1000&spss=3&start=10&startPage=0&totalRecords=100&version=",
				"/querier?blockID=00000000-0000-0000-0000-000000000000&encoding=none&end=20&footerSize=0&indexPageSize=0&pagesToSearch=90&size=1000&spss=3&start=10&startPage=90&totalRecords=100&version=",
				"/querier?blockID=00000000-0000-0000-0000-000000000001&encoding=none&end=20&footerSize=0&indexPageSize=0&pagesToSearch=180&size=1000&spss=3&start=10&startPage=0&totalRecords=200&version=",
				"/querier?blockID=00000000-0000-0000-0000-000000000001&encoding=none&end=20&footerSize=0&indexPageSize=0&pagesToSearch=180&size=1000&spss=3&start=10&startPage=180&totalRecords=200&version=",
			},
		},
	}

	for _, tc := range tests {
		req := httptest.NewRequest("GET", "/?k=test&v=test&start=10&end=20", nil)
		searchReq, err := api.ParseSearchRequest(req)
		require.NoError(t, err)

		ctx, cancelCause := context.WithCancelCause(context.Background())
		reqCh := make(chan pipeline.Request)
		iterFn := backendJobsFunc(tc.metas, tc.targetBytesPerRequest, defaultMostRecentShards, math.MaxUint32)

		go func() {
			buildBackendRequests(ctx, "test", pipeline.NewHTTPRequest(req), searchReq, 0, iterFn, reqCh, cancelCause)
		}()

		actualURIs := []string{}
		for r := range reqCh {
			if r != nil {
				actualURIs = append(actualURIs, r.HTTPRequest().RequestURI)
			}
		}

		assert.NoError(t, ctx.Err())

		urisEqual(t, tc.expectedURIs, actualURIs)
	}
}

func TestBuildBackendRequestsShardNumbers(t *testing.T) {
	// Test that firstShard parameter correctly offsets the shard numbers
	tests := []struct {
		name                  string
		targetBytesPerRequest int
		metas                 []*backend.BlockMeta
		firstShard            int
		expectedShardNumbers  []int
	}{
		{
			name:                  "single block, firstShard=0",
			targetBytesPerRequest: 1000,
			metas: []*backend.BlockMeta{
				{
					Size_:         1000,
					TotalRecords:  10,
					BlockID:       backend.MustParse("00000000-0000-0000-0000-000000000000"),
					IndexPageSize: 13,
					Version:       "glarg",
				},
			},
			firstShard:           0,
			expectedShardNumbers: []int{0},
		},
		{
			name:                  "single block, firstShard=5",
			targetBytesPerRequest: 1000,
			metas: []*backend.BlockMeta{
				{
					Size_:         1000,
					TotalRecords:  10,
					BlockID:       backend.MustParse("00000000-0000-0000-0000-000000000000"),
					IndexPageSize: 13,
					Version:       "glarg",
				},
			},
			firstShard:           5,
			expectedShardNumbers: []int{5},
		},
		{
			name:                  "multiple blocks across different shards, firstShard=3",
			targetBytesPerRequest: 900,
			metas: []*backend.BlockMeta{
				{
					Size_:        1000,
					TotalRecords: 100,
					BlockID:      backend.MustParse("00000000-0000-0000-0000-000000000000"),
				},
				{
					Size_:        1000,
					TotalRecords: 200,
					BlockID:      backend.MustParse("00000000-0000-0000-0000-000000000001"),
				},
			},
			firstShard:           3,
			expectedShardNumbers: []int{3, 3, 4, 4}, // blocks split across backend shards 0 and 1, offset by firstShard=3
		},
		{
			name:                  "edge case: firstShard=100 with single block",
			targetBytesPerRequest: 1000,
			metas: []*backend.BlockMeta{
				{
					Size_:        1000,
					TotalRecords: 10,
					BlockID:      backend.MustParse("00000000-0000-0000-0000-000000000000"),
				},
			},
			firstShard:           100,
			expectedShardNumbers: []int{100},
		},
		{
			name:                  "three blocks with small pages to force multiple shards",
			targetBytesPerRequest: 1,
			metas: []*backend.BlockMeta{
				{
					Size_:        1000,
					TotalRecords: 3,
					BlockID:      backend.MustParse("00000000-0000-0000-0000-000000000000"),
				},
				{
					Size_:        1000,
					TotalRecords: 3,
					BlockID:      backend.MustParse("00000000-0000-0000-0000-000000000001"),
				},
				{
					Size_:        1000,
					TotalRecords: 3,
					BlockID:      backend.MustParse("00000000-0000-0000-0000-000000000002"),
				},
			},
			firstShard:           2,
			expectedShardNumbers: []int{2, 2, 2, 3, 3, 3, 4, 4, 4}, // 3 jobs per block across 3 shards (0,1,2) + firstShard offset (2)
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest("GET", "/?k=test&v=test&start=10&end=20", nil)
			searchReq, err := api.ParseSearchRequest(req)
			require.NoError(t, err)

			ctx, cancelCause := context.WithCancelCause(context.Background())
			reqCh := make(chan pipeline.Request, 10)
			iterFn := backendJobsFunc(tc.metas, tc.targetBytesPerRequest, defaultMostRecentShards, math.MaxUint32)

			go func() {
				buildBackendRequests(ctx, "test", pipeline.NewHTTPRequest(req), searchReq, tc.firstShard, iterFn, reqCh, cancelCause)
			}()

			actualShardNumbers := []int{}
			for r := range reqCh {
				if r != nil {
					shardNum := r.ResponseData().(int)
					actualShardNumbers = append(actualShardNumbers, shardNum)
				}
			}

			require.NoError(t, ctx.Err())
			assert.Equal(t, tc.expectedShardNumbers, actualShardNumbers)
		})
	}
}

func TestBackendRequests(t *testing.T) {
	bm := backend.NewBlockMeta("test", uuid.New(), "wdwad")
	bm.StartTime = time.Unix(100, 0)
	bm.EndTime = time.Unix(200, 0)
	bm.Size_ = defaultTargetBytesPerRequest * 2
	bm.TotalRecords = 2

	s := &asyncSearchSharder{
		cfg: SearchSharderConfig{
			MostRecentShards: defaultMostRecentShards,
		},
		reader: &mockReader{metas: []*backend.BlockMeta{bm}},
	}

	tests := []struct {
		name               string
		request            string
		expectedReqsURIs   []string
		expectedJobs       int
		expectedBlocks     int
		expectedBlockBytes uint64
	}{
		{
			name:    "start and end same as block",
			request: "/?tags=foo%3Dbar&minDuration=10ms&maxDuration=30ms&limit=50&start=100&end=200",
			expectedReqsURIs: []string{
				"/querier?blockID=" + bm.BlockID.String() + "&encoding=none&end=200&footerSize=0&indexPageSize=0&limit=50&maxDuration=30ms&minDuration=10ms&pagesToSearch=1&size=209715200&spss=3&start=100&startPage=0&tags=foo%3Dbar&totalRecords=2&version=wdwad",
				"/querier?blockID=" + bm.BlockID.String() + "&encoding=none&end=200&footerSize=0&indexPageSize=0&limit=50&maxDuration=30ms&minDuration=10ms&pagesToSearch=1&size=209715200&spss=3&start=100&startPage=1&tags=foo%3Dbar&totalRecords=2&version=wdwad",
			},
			expectedJobs:       2,
			expectedBlocks:     1,
			expectedBlockBytes: defaultTargetBytesPerRequest * 2,
		},
		{
			name:    "start and end in block",
			request: "/?tags=foo%3Dbar&minDuration=10ms&maxDuration=30ms&limit=50&start=110&end=150",
			expectedReqsURIs: []string{
				"/querier?blockID=" + bm.BlockID.String() + "&encoding=none&end=150&footerSize=0&indexPageSize=0&limit=50&maxDuration=30ms&minDuration=10ms&pagesToSearch=1&size=209715200&spss=3&start=110&startPage=0&tags=foo%3Dbar&totalRecords=2&version=wdwad",
				"/querier?blockID=" + bm.BlockID.String() + "&encoding=none&end=150&footerSize=0&indexPageSize=0&limit=50&maxDuration=30ms&minDuration=10ms&pagesToSearch=1&size=209715200&spss=3&start=110&startPage=1&tags=foo%3Dbar&totalRecords=2&version=wdwad",
			},
			expectedJobs:       2,
			expectedBlocks:     1,
			expectedBlockBytes: defaultTargetBytesPerRequest * 2,
		},
		{
			name:    "skip_ast_transformations propagated to backend block requests",
			request: "/?start=100&end=200&skip_ast_transformations=or_to_in",
			expectedReqsURIs: []string{
				"/querier?blockID=" + bm.BlockID.String() + "&encoding=none&end=200&footerSize=0&indexPageSize=0&pagesToSearch=1&size=209715200&skip_ast_transformations=or_to_in&spss=3&start=100&startPage=0&totalRecords=2&version=wdwad",
				"/querier?blockID=" + bm.BlockID.String() + "&encoding=none&end=200&footerSize=0&indexPageSize=0&pagesToSearch=1&size=209715200&skip_ast_transformations=or_to_in&spss=3&start=100&startPage=1&totalRecords=2&version=wdwad",
			},
			expectedJobs:       2,
			expectedBlocks:     1,
			expectedBlockBytes: defaultTargetBytesPerRequest * 2,
		},
		{
			name:             "start and end out of block",
			request:          "/?tags=foo%3Dbar&minDuration=10ms&maxDuration=30ms&limit=50&start=10&end=20",
			expectedReqsURIs: make([]string, 0),
		},
		{
			name:             "no start and end",
			request:          "/?tags=foo%3Dbar&minDuration=10ms&maxDuration=30ms&limit=50",
			expectedReqsURIs: make([]string, 0),
		},
		{
			name:             "only tags",
			request:          "/?tags=foo%3Dbar",
			expectedReqsURIs: make([]string, 0),
		},
		{
			name:             "no params",
			request:          "/",
			expectedReqsURIs: make([]string, 0),
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			r := httptest.NewRequest("GET", tc.request, nil)
			searchReq, err := api.ParseSearchRequest(r)
			require.NoError(t, err)

			stopCh := make(chan struct{})
			defer close(stopCh)
			reqCh := make(chan pipeline.Request)

			ctx, cancelCause := context.WithCancelCause(context.Background())
			pipelineRequest := pipeline.NewHTTPRequest(r)

			searchJobResponse := &combiner.SearchJobResponse{}
			s.backendRequests(ctx, "test", pipelineRequest, searchReq, searchJobResponse, nil, false, reqCh, cancelCause)
			require.Equal(t, tc.expectedJobs, searchJobResponse.TotalJobs)
			require.Equal(t, tc.expectedBlocks, searchJobResponse.TotalBlocks)
			require.Equal(t, tc.expectedBlockBytes, searchJobResponse.TotalBytes)

			actualReqURIs := []string{}
			for r := range reqCh {
				if r != nil {
					actualReqURIs = append(actualReqURIs, r.HTTPRequest().RequestURI)
				}
			}
			require.NoError(t, ctx.Err())
			urisEqual(t, tc.expectedReqsURIs, actualReqURIs)
		})
	}
}

// TestSearchSharder_TimeSlicedDispatch_UsesQueryPlanSlicesNotBlockPaging pins #487's
// time-sliced dispatch: given a QueryPlan with Strategy: DispatchTimeSliced and a non-empty
// Slices, backendRequests must emit one job per (block, slice) pair with IndexOnly=true and
// SearchReq.Start/End overridden to the slice's bounds — NOT the pagesPerRequest-computed
// page range backendJobsFunc would otherwise use.
func TestSearchSharder_TimeSlicedDispatch_UsesQueryPlanSlicesNotBlockPaging(t *testing.T) {
	bm := backend.NewBlockMeta("test", uuid.New(), "wdwad")
	bm.StartTime = time.Unix(100, 0)
	bm.EndTime = time.Unix(200, 0)
	bm.Size_ = defaultTargetBytesPerRequest * 2
	bm.TotalRecords = 2

	s := &asyncSearchSharder{
		cfg:    SearchSharderConfig{MostRecentShards: defaultMostRecentShards},
		reader: &mockReader{metas: []*backend.BlockMeta{bm}},
	}

	r := httptest.NewRequest("GET", "/?tags=foo%3Dbar&limit=50&start=100&end=200", nil)
	searchReq, err := api.ParseSearchRequest(r)
	require.NoError(t, err)

	plan := &blockpack.QueryPlan{
		Strategy: blockpack.DispatchTimeSliced,
		Slices: []blockpack.TimeSlice{
			{Start: 150, End: 200},
			{Start: 100, End: 150},
		},
	}

	reqCh := make(chan pipeline.Request)
	ctx, cancelCause := context.WithCancelCause(context.Background())
	pipelineRequest := pipeline.NewHTTPRequest(r)
	searchJobResponse := &combiner.SearchJobResponse{}

	go s.backendRequests(ctx, "test", pipelineRequest, searchReq, searchJobResponse, plan, false, reqCh, cancelCause)

	var gotReqs []*tempopb.SearchBlockRequest
	for pr := range reqCh {
		parsed, err := api.ParseSearchBlockRequest(pr.HTTPRequest())
		require.NoError(t, err)
		gotReqs = append(gotReqs, parsed)
	}
	require.NoError(t, ctx.Err())

	require.Equal(t, 2, searchJobResponse.TotalJobs, "one job per (block, slice) pair for the single block")
	require.Len(t, gotReqs, 2)

	sort.Slice(gotReqs, func(i, j int) bool { return gotReqs[i].SearchReq.Start < gotReqs[j].SearchReq.Start })

	require.Equal(t, uint32(100), gotReqs[0].SearchReq.Start)
	require.Equal(t, uint32(150), gotReqs[0].SearchReq.End)
	require.True(t, gotReqs[0].IndexOnly, "a time-sliced job must set IndexOnly=true")

	require.Equal(t, uint32(150), gotReqs[1].SearchReq.Start)
	require.Equal(t, uint32(200), gotReqs[1].SearchReq.End)
	require.True(t, gotReqs[1].IndexOnly, "a time-sliced job must set IndexOnly=true")
}

// TestSearchSharder_BlockShardedDispatch_UnchangedWhenStrategyIsBlockSharded pins the
// parity-and-fallback-first discipline: a nil plan, and a plan whose Strategy is the
// zero-value DispatchBlockSharded, must both produce byte-identical output to today's
// block-sharded backendJobsFunc-only path — including IndexOnly=false (the zero value) on
// every job.
func TestSearchSharder_BlockShardedDispatch_UnchangedWhenStrategyIsBlockSharded(t *testing.T) {
	bm := backend.NewBlockMeta("test", uuid.New(), "wdwad")
	bm.StartTime = time.Unix(100, 0)
	bm.EndTime = time.Unix(200, 0)
	bm.Size_ = defaultTargetBytesPerRequest * 2
	bm.TotalRecords = 2

	newSharder := func() *asyncSearchSharder {
		return &asyncSearchSharder{
			cfg:    SearchSharderConfig{MostRecentShards: defaultMostRecentShards},
			reader: &mockReader{metas: []*backend.BlockMeta{bm}},
		}
	}

	runWithPlan := func(t *testing.T, plan *blockpack.QueryPlan) (int, []string) {
		t.Helper()
		r := httptest.NewRequest("GET", "/?tags=foo%3Dbar&minDuration=10ms&maxDuration=30ms&limit=50&start=100&end=200", nil)
		searchReq, err := api.ParseSearchRequest(r)
		require.NoError(t, err)

		reqCh := make(chan pipeline.Request)
		ctx, cancelCause := context.WithCancelCause(context.Background())
		pipelineRequest := pipeline.NewHTTPRequest(r)
		searchJobResponse := &combiner.SearchJobResponse{}

		go newSharder().backendRequests(ctx, "test", pipelineRequest, searchReq, searchJobResponse, plan, false, reqCh, cancelCause)

		var uris []string
		for pr := range reqCh {
			uris = append(uris, pr.HTTPRequest().RequestURI)
			parsed, err := api.ParseSearchBlockRequest(pr.HTTPRequest())
			require.NoError(t, err)
			require.False(t, parsed.IndexOnly, "block-sharded jobs must have IndexOnly=false")
		}
		require.NoError(t, ctx.Err())
		return searchJobResponse.TotalJobs, uris
	}

	nilJobs, nilURIs := runWithPlan(t, nil)
	blockShardedJobs, blockShardedURIs := runWithPlan(t, &blockpack.QueryPlan{Strategy: blockpack.DispatchBlockSharded})

	require.Equal(t, 2, nilJobs)
	require.Equal(t, nilJobs, blockShardedJobs)
	require.Equal(t, nilURIs, blockShardedURIs)
}

// twoBlockGroupsForTimeSlicedShardTest returns two blocks (a recent one and an older
// one) plus a sharder configured so timeSlicedJobsFunc rolls them into two separate
// shards (MostRecentShards=2 -> blocksPerShard=1) — the fixture the T4 (#487 brainstorm
// risk 1) tests below share, so all three exercise the exact same shard-array shape T2's
// production code produces.
func twoBlockGroupsForTimeSlicedShardTest() (*asyncSearchSharder, *http.Request, *tempopb.SearchRequest) {
	bmRecent := backend.NewBlockMeta("test", uuid.New(), "wdwad")
	bmRecent.StartTime = time.Unix(200, 0)
	bmRecent.EndTime = time.Unix(300, 0)
	bmRecent.Size_ = 100
	bmRecent.TotalRecords = 1

	bmOlder := backend.NewBlockMeta("test", uuid.New(), "wdwad")
	bmOlder.StartTime = time.Unix(100, 0)
	bmOlder.EndTime = time.Unix(200, 0)
	bmOlder.Size_ = 100
	bmOlder.TotalRecords = 1

	s := &asyncSearchSharder{
		cfg:    SearchSharderConfig{MostRecentShards: 2},
		reader: &mockReader{metas: []*backend.BlockMeta{bmRecent, bmOlder}},
	}

	r := httptest.NewRequest("GET", "/?tags=foo%3Dbar&limit=50&start=100&end=300", nil)
	searchReq, err := api.ParseSearchRequest(r)
	if err != nil {
		panic(err) // fixture construction only; a parse failure here is a test-writing bug
	}

	return s, r, searchReq
}

// runTimeSlicedBackendRequests drives backendRequests to completion for the given plan
// and returns the resulting shard array plus each dispatched job's (Start, End) pair in
// the exact order jobs were sent to reqCh (buildTimeSlicedBackendRequests iterates
// blocks/slices on a single goroutine, so channel receive order == send order).
func runTimeSlicedBackendRequests(t *testing.T, s *asyncSearchSharder, r *http.Request, searchReq *tempopb.SearchRequest, plan *blockpack.QueryPlan) ([]shardtracker.Shard, [][2]uint32) {
	t.Helper()
	reqCh := make(chan pipeline.Request)
	ctx, cancelCause := context.WithCancelCause(context.Background())
	resp := &combiner.SearchJobResponse{}

	go s.backendRequests(ctx, "test", pipeline.NewHTTPRequest(r), searchReq, resp, plan, false, reqCh, cancelCause)

	var order [][2]uint32
	for pr := range reqCh {
		parsed, err := api.ParseSearchBlockRequest(pr.HTTPRequest())
		require.NoError(t, err)
		order = append(order, [2]uint32{parsed.SearchReq.Start, parsed.SearchReq.End})
	}
	require.NoError(t, ctx.Err())

	return resp.Shards, order
}

// TestTimeSlicedDispatch_ShardArrayOrderIsChronologicalNotPriority pins the #487
// brainstorm's headline risk (plan-c.md T4): the []shardtracker.Shard array
// timeSlicedJobsFunc/backendRequests builds must depend only on block chronology
// (blockMetasForSearch's most-recent-first order), never on TimeSlice.EstMatches or the
// order Slices happen to be given in. A recent, low-EstMatches slice and an older,
// high-EstMatches slice (non-monotonic with time, exactly the brainstorm's scenario) must
// not perturb the shard array — feeding Slices in chronological order versus a
// hypothetical (incorrect) EstMatches-DESC dispatch-priority order must produce a
// byte-identical shard array, since shard construction never inspects EstMatches at all.
func TestTimeSlicedDispatch_ShardArrayOrderIsChronologicalNotPriority(t *testing.T) {
	s, r, searchReq := twoBlockGroupsForTimeSlicedShardTest()

	// chronological is exactly BuildTimeSlices' guaranteed output shape: most-recent-first,
	// with EstMatches deliberately NOT monotonic with time (recent=low, older=high).
	chronological := []blockpack.TimeSlice{
		{Start: 200, End: 300, EstMatches: 5, EstKnown: true},   // most recent, low priority
		{Start: 100, End: 200, EstMatches: 500, EstKnown: true}, // older, high priority
	}
	// priorityOrdered is what a hypothetical (incorrect) EstMatches-DESC caller might feed
	// into shard construction instead — the exact "helpful refactor" this test guards
	// against ever reaching shard-array construction.
	priorityOrdered := []blockpack.TimeSlice{chronological[1], chronological[0]}

	chronoShards, _ := runTimeSlicedBackendRequests(t, s, r, searchReq,
		&blockpack.QueryPlan{Strategy: blockpack.DispatchTimeSliced, Slices: chronological})
	priorityShards, _ := runTimeSlicedBackendRequests(t, s, r, searchReq,
		&blockpack.QueryPlan{Strategy: blockpack.DispatchTimeSliced, Slices: priorityOrdered})

	// The known-correct shape: two blocks (MostRecentShards=2 -> blocksPerShard=1) roll into
	// two BLOCK-driven shards. Shard 0's boundary is the most-recent block group's EndTime
	// (300); shard 1 is the final/older block group's sentinel boundary (1). Asserting this
	// exact shape (not just self-consistency between the two Slices orderings) is what
	// actually catches a "helpful" refactor that shards by slice-priority-group instead of
	// block-group — such a refactor would still be internally self-consistent (both
	// orderings would normalize to the same, but WRONG, shard values) without this explicit
	// expected-value check.
	//
	// TotalJobs is 1 and 2, not 2 and 2 (team-lead's layer-(a) overlap filter,
	// blockOverlapsSlice): bmRecent is [200,300] (closed) and the older slice is [100,200)
	// (half-open) — they share no second at all (the slice's exclusive end, 200, is exactly
	// where bmRecent's inclusive start begins), so bmRecent gets only the recent slice's job.
	// bmOlder is [100,200] (closed) and the recent slice is [200,300) (half-open) — these DO
	// share second 200 (the slice's inclusive start falls inside bmOlder's inclusive end), so
	// bmOlder still gets both slices' jobs. This asymmetric touching-boundary behavior is the
	// expected, principled consequence of TimeSlice's own half-open [Start, End) contract
	// against a block's closed [StartTime, EndTime] convention — not an inconsistency.
	wantShards := []shardtracker.Shard{
		{TotalJobs: 1, CompletedThroughSeconds: 300},
		{TotalJobs: 2, CompletedThroughSeconds: 1},
	}
	require.Equal(t, wantShards, chronoShards,
		"the shard array must be block-chronology-driven (matching backendJobsFunc's own "+
			"block-group shard shape), never slice-priority-driven")
	require.Equal(t, chronoShards, priorityShards,
		"the shard array must depend only on block chronology, never on Slices' order/EstMatches — "+
			"a dispatch-priority reordering of Slices must not change shard-array construction")
}

// TestTimeSlicedDispatch_CompletedThroughSecondsAdvancesEvenWhenHighPrioritySliceIsSlow is
// the regression guard directly modeling the brainstorm risk: the highest-EstMatches
// slice's job (bundled into the OLDER block group's shard, per T2's per-block-group
// sharding) never completes, while the chronologically-earlier (array index 0, most
// recent block group) shard's jobs all complete promptly. CompletedThroughSeconds must
// still advance past that completed prefix — CompletionTracker itself is untouched; this
// only exercises it against a shard array built by the real production shard-array
// construction (twoBlockGroupsForTimeSlicedShardTest / runTimeSlicedBackendRequests).
func TestTimeSlicedDispatch_CompletedThroughSecondsAdvancesEvenWhenHighPrioritySliceIsSlow(t *testing.T) {
	s, r, searchReq := twoBlockGroupsForTimeSlicedShardTest()
	slices := []blockpack.TimeSlice{
		{Start: 200, End: 300, EstMatches: 5, EstKnown: true},   // most recent, low priority
		{Start: 100, End: 200, EstMatches: 500, EstKnown: true}, // older, high priority
	}
	shards, _ := runTimeSlicedBackendRequests(t, s, r, searchReq,
		&blockpack.QueryPlan{Strategy: blockpack.DispatchTimeSliced, Slices: slices})
	require.Len(t, shards, 2, "two blocks with MostRecentShards=2 must roll into two shards")

	var tracker shardtracker.CompletionTracker
	tracker.AddShards(shards)

	// Shard 0 (most-recent block group) carries only the low-EstMatches recent slice's job —
	// the older slice does not overlap bmRecent at all (blockOverlapsSlice's half-open/closed
	// boundary rule; see TestTimeSlicedDispatch_ShardArrayOrderIsChronologicalNotPriority's
	// wantShards comment). That one job completes promptly.
	got := tracker.AddShardIdx(0)
	require.Equal(t, shards[0].CompletedThroughSeconds, got,
		"CompletedThroughSeconds must advance to shard 0's boundary once shard 0's job completes")

	// Shard 1 (older block group) carries the highest-EstMatches slice's job — it never
	// completes at all (simulating a permanently slow high-priority job). The completed
	// prefix must not regress or block on it.
	require.Equal(t, shards[0].CompletedThroughSeconds, tracker.CompletedThroughSeconds(),
		"the completed prefix must not wait on the highest-EstMatches slice's (still-pending) shard")
}

// TestTimeSlicedDispatch_UniformFallbackDispatchOrderEqualsChronological pins the settled
// #487 degradation rule (plan-c.md's team-lead final settlement, queryplan's
// TimeSlice.EstKnown/BuildTimeSlices doc comments): when every slice has EstKnown=false
// (the plan-wide uniform-width fallback, no VCNT signal to prioritize by), the OBSERVED
// dispatch order must equal the chronological block/slice iteration order exactly — no
// est-matches-based reordering exists or should be attempted for an all-EstKnown=false
// plan.
func TestTimeSlicedDispatch_UniformFallbackDispatchOrderEqualsChronological(t *testing.T) {
	s, r, searchReq := twoBlockGroupsForTimeSlicedShardTest()

	// Uniform fallback: EstKnown is the zero value (false) on every slice, EstMatches=0 —
	// no signal to sort by, per BuildTimeSlices' contract.
	slices := []blockpack.TimeSlice{
		{Start: 200, End: 300}, // most recent
		{Start: 100, End: 200}, // older
	}
	_, order := runTimeSlicedBackendRequests(t, s, r, searchReq,
		&blockpack.QueryPlan{Strategy: blockpack.DispatchTimeSliced, Slices: slices})

	// Expected: for each block in its natural most-recent-first order, for each slice in
	// Slices' given (already-chronological) order that genuinely overlaps that block —
	// exactly the shard-array/chronological order, unchanged by any priority reordering
	// since none applies here. bmRecent[200,300] does not overlap the older slice [100,200)
	// at all (see TestTimeSlicedDispatch_ShardArrayOrderIsChronologicalNotPriority's wantShards
	// comment for why), so only its recent-slice job is dispatched; bmOlder[100,200] overlaps
	// both slices (the recent slice touches bmOlder's inclusive end at second 200).
	wantOrder := [][2]uint32{
		{200, 300},             // recent block: recent slice only (older slice does not overlap it)
		{200, 300}, {100, 200}, // older block: recent slice, then older slice
	}
	require.Equal(t, wantOrder, order,
		"with EstKnown=false on every slice, dispatch order must equal the chronological "+
			"block/slice iteration order exactly")
}

// TestBlockOverlapsSlice pins blockOverlapsSlice's boundary semantics directly (team-lead's
// layer-(a) completion for search's #487 dispatch/count filter): a block's [StartTime, EndTime]
// is closed on both ends, a TimeSlice's [Start, End) is half-open, so a slice whose exclusive
// end lands exactly on a block's inclusive start does NOT overlap, while a slice whose
// inclusive start lands exactly on a block's inclusive end DOES overlap.
func TestBlockOverlapsSlice(t *testing.T) {
	m := backend.NewBlockMeta("test", uuid.New(), "wdwad")
	m.StartTime = time.Unix(200, 0)
	m.EndTime = time.Unix(300, 0)

	tests := []struct {
		name  string
		slice blockpack.TimeSlice
		want  bool
	}{
		{"slice fully inside block", blockpack.TimeSlice{Start: 220, End: 280}, true},
		{"slice fully contains block", blockpack.TimeSlice{Start: 100, End: 400}, true},
		{"slice equals block bounds", blockpack.TimeSlice{Start: 200, End: 300}, true},
		{"slice touches block's inclusive end at its own inclusive start", blockpack.TimeSlice{Start: 300, End: 350}, true},
		{"slice's exclusive end touches block's inclusive start: no shared second", blockpack.TimeSlice{Start: 100, End: 200}, false},
		{"slice entirely before block", blockpack.TimeSlice{Start: 0, End: 100}, false},
		{"slice entirely after block", blockpack.TimeSlice{Start: 400, End: 500}, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, blockOverlapsSlice(m, tc.slice))
		})
	}
}

// TestSearchSharder_TimeSlicedDispatch_SkipsNonOverlappingBlockSlicePairs mirrors the
// metrics sharder's own regression test for the same fix (#161): a (block, slice) pair
// with no time overlap must be silently skipped in BOTH the job count
// (SearchJobResponse.TotalJobs / Shard.TotalJobs) and the actual jobs sent to reqCh — the
// count can never drift out of sync with what is actually dispatched, or
// shardtracker.CompletionTracker's exact-TotalJobs completion check can never be
// satisfied, stalling completion tracking forever (see blockOverlapsSlice's doc comment).
func TestSearchSharder_TimeSlicedDispatch_SkipsNonOverlappingBlockSlicePairs(t *testing.T) {
	blockA := backend.NewBlockMeta("test", uuid.New(), "wdwad")
	blockA.StartTime = time.Unix(100, 0)
	blockA.EndTime = time.Unix(150, 0)
	blockA.Size_ = defaultTargetBytesPerRequest
	blockA.TotalRecords = 1

	blockB := backend.NewBlockMeta("test", uuid.New(), "wdwad")
	blockB.StartTime = time.Unix(500, 0)
	blockB.EndTime = time.Unix(600, 0)
	blockB.Size_ = defaultTargetBytesPerRequest
	blockB.TotalRecords = 1

	s := &asyncSearchSharder{
		cfg:    SearchSharderConfig{MostRecentShards: defaultMostRecentShards},
		reader: &mockReader{metas: []*backend.BlockMeta{blockA, blockB}},
	}

	r := httptest.NewRequest("GET", "/?tags=foo%3Dbar&limit=50&start=100&end=600", nil)
	searchReq, err := api.ParseSearchRequest(r)
	require.NoError(t, err)

	// slice1 overlaps ONLY blockA; slice2 overlaps ONLY blockB. The cross product therefore
	// includes two genuinely disjoint (block, slice) pairs: (blockA, slice2), (blockB, slice1).
	plan := &blockpack.QueryPlan{
		Strategy: blockpack.DispatchTimeSliced,
		Slices: []blockpack.TimeSlice{
			{Start: 500, End: 600}, // slice2
			{Start: 100, End: 150}, // slice1
		},
	}

	reqCh := make(chan pipeline.Request)
	ctx, cancelCause := context.WithCancelCause(context.Background())
	pipelineRequest := pipeline.NewHTTPRequest(r)
	resp := &combiner.SearchJobResponse{}

	go s.backendRequests(ctx, "test", pipelineRequest, searchReq, resp, plan, false, reqCh, cancelCause)

	var gotReqs []*tempopb.SearchBlockRequest
	for pr := range reqCh {
		parsed, err := api.ParseSearchBlockRequest(pr.HTTPRequest())
		require.NoError(t, err)
		gotReqs = append(gotReqs, parsed)
	}
	require.NoError(t, ctx.Err())

	// Only the two genuinely overlapping (block, slice) pairs must produce a job; the two
	// disjoint pairs must be silently skipped, never sent as wasted no-results jobs.
	require.Len(t, gotReqs, 2)

	sort.Slice(gotReqs, func(i, j int) bool { return gotReqs[i].SearchReq.Start < gotReqs[j].SearchReq.Start })
	require.Equal(t, uint32(100), gotReqs[0].SearchReq.Start)
	require.Equal(t, uint32(150), gotReqs[0].SearchReq.End)
	require.Equal(t, uint32(500), gotReqs[1].SearchReq.Start)
	require.Equal(t, uint32(600), gotReqs[1].SearchReq.End)

	// Fix for #161 (search-side gap): before blockOverlapsSlice was wired in, timeSlicedJobsFunc
	// counted the full, unfiltered cross product (4) into TotalJobs/Shard.TotalJobs, even though
	// the two disjoint pairs above are silently skipped and only 2 jobs are ever dispatched.
	// shardtracker.CompletionTracker compares foundResponses[shard] against exactly this
	// TotalJobs value to decide a shard is complete (tracker.go) — an inflated count can never
	// be reached, stalling completion tracking. TotalJobs (and the sum of every Shard's own
	// TotalJobs) must equal the number of requests actually sent to reqCh, not the raw block x
	// slice cross product.
	require.Equal(t, len(gotReqs), resp.TotalJobs, "TotalJobs must match jobs actually dispatched, not the raw block x slice cross product")
	var sumShardJobs int
	for _, sh := range resp.Shards {
		sumShardJobs += int(sh.TotalJobs)
	}
	require.Equal(t, resp.TotalJobs, sumShardJobs, "sum of per-shard TotalJobs must equal the overall TotalJobs")
}

// TestSearchSharder_TimeSlicedDispatch_CacheKeyUsesWholeQueryWindowNotSliceWindow pins the
// holistic-review HIGH fix: buildTimeSlicedBackendRequests must compute the job cache key from
// the whole, unnarrowed query window (searchReq.Start/End), not the slice-narrowed subReq.Start/
// End. cacheKey's own validity rule (cache_keys.go) requires [start, end) to strictly
// encapsulate the block — a slice's window is by construction never wider than the block it
// overlaps, so using the slice window there would make every sliced job's cache key empty
// ("can't cache"), silently disabling caching for virtually all time-sliced search jobs. Using a
// block whose bounds exactly equal the slice's own bounds (so the slice-narrowed computation
// would fail the "strictly encapsulates" check) but sits strictly inside the WHOLE query window
// (100-300) makes this the sharpest possible regression guard: the two computations disagree in
// exactly this case.
func TestSearchSharder_TimeSlicedDispatch_CacheKeyUsesWholeQueryWindowNotSliceWindow(t *testing.T) {
	bm := backend.NewBlockMeta("test", uuid.New(), "wdwad")
	bm.StartTime = time.Unix(150, 0)
	bm.EndTime = time.Unix(160, 0)
	bm.Size_ = defaultTargetBytesPerRequest
	bm.TotalRecords = 1

	s := &asyncSearchSharder{
		cfg:    SearchSharderConfig{MostRecentShards: defaultMostRecentShards},
		reader: &mockReader{metas: []*backend.BlockMeta{bm}},
	}

	// The whole query window (100-300) strictly encapsulates the block (150-160), so the FIXED
	// cache-key computation (using this window) must produce a non-empty key. The slice's own
	// window exactly equals the block's bounds (150-160) — the BUGGY computation (using the
	// slice window) would fail cacheKey's strict "start.Before(block.Start)" check and produce
	// an empty key instead.
	// q= (not tags=) is required: hashForSearchRequest returns 0 for an empty Query, and
	// cacheKey's own "if queryHash == 0" guard would produce an empty key regardless of the
	// start/end fix this test is pinning — a non-zero query hash is a precondition, not the
	// thing under test here.
	r := httptest.NewRequest("GET", "/?q=%7B%7D&limit=50&start=100&end=300", nil)
	searchReq, err := api.ParseSearchRequest(r)
	require.NoError(t, err)

	plan := &blockpack.QueryPlan{
		Strategy: blockpack.DispatchTimeSliced,
		Slices:   []blockpack.TimeSlice{{Start: 150, End: 160}},
	}

	reqCh := make(chan pipeline.Request)
	ctx, cancelCause := context.WithCancelCause(context.Background())
	pipelineRequest := pipeline.NewHTTPRequest(r)
	resp := &combiner.SearchJobResponse{}

	go s.backendRequests(ctx, "test", pipelineRequest, searchReq, resp, plan, false, reqCh, cancelCause)

	var gotKeys []string
	for pr := range reqCh {
		gotKeys = append(gotKeys, pr.CacheKey())
	}
	require.NoError(t, ctx.Err())

	require.Len(t, gotKeys, 1)
	require.NotEmpty(t, gotKeys[0],
		"a time-sliced job's cache key must be computed from the whole query window, not the "+
			"slice-narrowed window — an empty key here means caching regressed to disabled")
}

func TestIngesterRequests(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		nownow := time.Now()

		now := int(nownow.Unix())

		ago := func(d string) int {
			duration, err := time.ParseDuration(d)
			require.NoError(t, err)
			return int(nownow.Add(-duration).Unix())
		}
		tenMinutesAgo := int(nownow.Add(-10 * time.Minute).Unix())
		fifteenMinutesAgo := int(nownow.Add(-15 * time.Minute).Unix())
		twentyMinutesAgo := int(nownow.Add(-20 * time.Minute).Unix())

		tests := []struct {
			request           string
			queryBackendAfter time.Duration
			ingesterShards    int
			expectedURI       []string
			expectedError     error
		}{
			// start/end is outside queryBackendAfter
			{
				request:           "/?tags=foo%3Dbar&minDuration=10ms&maxDuration=30ms&limit=50&start=10&end=20",
				queryBackendAfter: 10 * time.Minute,
				expectedURI:       []string{},
				ingesterShards:    1,
			},
			// start/end is inside queryBackendAfter
			{
				request:           "/?tags=foo%3Dbar&minDuration=10ms&maxDuration=30ms&limit=50&start=" + strconv.Itoa(tenMinutesAgo) + "&end=" + strconv.Itoa(now),
				queryBackendAfter: 30 * time.Minute,
				expectedURI:       []string{"/querier?end=" + strconv.Itoa(now) + "&limit=50&maxDuration=30ms&minDuration=10ms&spss=3&start=" + strconv.Itoa(tenMinutesAgo) + "&tags=foo%3Dbar"},
				ingesterShards:    1,
			},
			// queryBackendAfter = 0 results in no ingester query
			{
				request:           "/?tags=foo%3Dbar&minDuration=10ms&maxDuration=30ms&limit=50&start=" + strconv.Itoa(tenMinutesAgo) + "&end=" + strconv.Itoa(now),
				queryBackendAfter: 0,
				expectedURI:       []string{},
				ingesterShards:    1,
			},
			// start/end = 20 - 10 mins ago - break across query backend after
			//  ingester start/End = 15 - 10 mins ago
			{
				request:           "/?tags=foo%3Dbar&minDuration=10ms&maxDuration=30ms&limit=50&start=" + strconv.Itoa(twentyMinutesAgo) + "&end=" + strconv.Itoa(tenMinutesAgo),
				queryBackendAfter: 15 * time.Minute,
				expectedURI:       []string{"/querier?end=" + strconv.Itoa(tenMinutesAgo) + "&limit=50&maxDuration=30ms&minDuration=10ms&spss=3&start=" + strconv.Itoa(fifteenMinutesAgo) + "&tags=foo%3Dbar"},
				ingesterShards:    1,
			},
			// start/end = 10 - now mins ago - break across query backend after
			//  ingester start/End = 10 - now mins ago
			//  backend start/End = 15 - 10 mins ago
			{
				request:           "/?tags=foo%3Dbar&minDuration=10ms&maxDuration=30ms&limit=50&start=" + strconv.Itoa(tenMinutesAgo) + "&end=" + strconv.Itoa(now),
				queryBackendAfter: 15 * time.Minute,
				expectedURI:       []string{"/querier?end=" + strconv.Itoa(now) + "&limit=50&maxDuration=30ms&minDuration=10ms&spss=3&start=" + strconv.Itoa(tenMinutesAgo) + "&tags=foo%3Dbar"},
				ingesterShards:    1,
			},
			// start/end = 20 - now mins ago - break across query backend after
			//  ingester start/End = 15 - now mins ago
			//  backend start/End = 20 - 5 mins ago
			{
				request:           "/?tags=foo%3Dbar&minDuration=10ms&maxDuration=30ms&limit=50&start=" + strconv.Itoa(twentyMinutesAgo) + "&end=" + strconv.Itoa(now),
				queryBackendAfter: 15 * time.Minute,
				expectedURI:       []string{"/querier?end=" + strconv.Itoa(now) + "&limit=50&maxDuration=30ms&minDuration=10ms&spss=3&start=" + strconv.Itoa(fifteenMinutesAgo) + "&tags=foo%3Dbar"},
				ingesterShards:    1,
			},
			{
				request:           "/?tags=foo%3Dbar&minDuration=10ms&maxDuration=30ms&limit=50",
				queryBackendAfter: 15 * time.Minute,
				expectedURI:       []string{"minDuration=10ms&maxDuration=30ms&limit=50&spss=3&tags=foo%3Dbar"},
				ingesterShards:    1,
			},
			{
				request:           "/?limit=50",
				queryBackendAfter: 15 * time.Minute,
				expectedURI:       []string{"limit=50&spss=3"},
				ingesterShards:    1,
			},
			// start/end = 20 - 10 mins ago - break across query backend after
			//  ingester start/End = 15 - 10 mins ago -- 5 minutes split in 2 shards.
			{
				request:           "/?tags=foo%3Dbar&minDuration=12ms&maxDuration=30ms&limit=50&start=" + strconv.Itoa(ago("20m")) + "&end=" + strconv.Itoa(ago("10m")),
				queryBackendAfter: 15 * time.Minute,
				expectedURI: []string{
					"/querier?end=" + strconv.Itoa(ago("12.5m")) + "&limit=50&maxDuration=30ms&minDuration=12ms&spss=3&start=" + strconv.Itoa(ago("15m")) + "&tags=foo%3Dbar",
					"/querier?end=" + strconv.Itoa(ago("10m")) + "&limit=50&maxDuration=30ms&minDuration=12ms&spss=3&start=" + strconv.Itoa(ago("12.5m")) + "&tags=foo%3Dbar",
				},
				ingesterShards: 2,
			},
			// start/end when entirely within the ingester search window when split across 3 shards.
			{
				request:           "/?tags=foo%3Dbar&minDuration=11ms&maxDuration=30ms&limit=50&start=" + strconv.Itoa(ago("15m")) + "&end=" + strconv.Itoa(ago("0s")),
				queryBackendAfter: 15 * time.Minute,
				expectedURI: []string{
					"/querier?end=" + strconv.Itoa(ago("10m")) + "&limit=50&maxDuration=30ms&minDuration=11ms&spss=3&start=" + strconv.Itoa(ago("15m")) + "&tags=foo%3Dbar",
					"/querier?end=" + strconv.Itoa(ago("5m")) + "&limit=50&maxDuration=30ms&minDuration=11ms&spss=3&start=" + strconv.Itoa(ago("10m")) + "&tags=foo%3Dbar",
					"/querier?end=" + strconv.Itoa(now) + "&limit=50&maxDuration=30ms&minDuration=11ms&spss=3&start=" + strconv.Itoa(ago("5m")) + "&tags=foo%3Dbar",
				},
				ingesterShards: 3,
			},
			// start/end when entirely within ingeste search window, but check that we don't shard too much.
			{
				request:           "/?tags=foo%3Dbar&minDuration=11ms&maxDuration=30ms&limit=50&start=" + strconv.Itoa(ago("15m")) + "&end=" + strconv.Itoa(ago("0s")),
				queryBackendAfter: 5*time.Minute + 10*time.Second,
				expectedURI: []string{
					"/querier?end=" + strconv.Itoa(ago("4m10s")) + "&limit=50&maxDuration=30ms&minDuration=11ms&spss=3&start=" + strconv.Itoa(ago("5m10s")) + "&tags=foo%3Dbar",
					"/querier?end=" + strconv.Itoa(ago("3m10s")) + "&limit=50&maxDuration=30ms&minDuration=11ms&spss=3&start=" + strconv.Itoa(ago("4m10s")) + "&tags=foo%3Dbar",
					"/querier?end=" + strconv.Itoa(ago("2m10s")) + "&limit=50&maxDuration=30ms&minDuration=11ms&spss=3&start=" + strconv.Itoa(ago("3m10s")) + "&tags=foo%3Dbar",
					"/querier?end=" + strconv.Itoa(ago("1m10s")) + "&limit=50&maxDuration=30ms&minDuration=11ms&spss=3&start=" + strconv.Itoa(ago("2m10s")) + "&tags=foo%3Dbar",
					"/querier?end=" + strconv.Itoa(ago("10s")) + "&limit=50&maxDuration=30ms&minDuration=11ms&spss=3&start=" + strconv.Itoa(ago("1m10s")) + "&tags=foo%3Dbar",
					"/querier?end=" + strconv.Itoa(now) + "&limit=50&maxDuration=30ms&minDuration=11ms&spss=3&start=" + strconv.Itoa(ago("10s")) + "&tags=foo%3Dbar",
				},
				ingesterShards: 6,
			},
			// start/end when entirely within ingeste search window, but check that we don't shard too much with a large number of shards for a small window.
			{
				request:           "/?tags=foo%3Dbar&minDuration=11ms&maxDuration=30ms&limit=50&start=" + strconv.Itoa(ago("15m")) + "&end=" + strconv.Itoa(ago("0s")),
				queryBackendAfter: 5 * time.Minute,
				expectedURI: []string{
					"/querier?end=" + strconv.Itoa(ago("4m")) + "&limit=50&maxDuration=30ms&minDuration=11ms&spss=3&start=" + strconv.Itoa(ago("5m")) + "&tags=foo%3Dbar",
					"/querier?end=" + strconv.Itoa(ago("3m")) + "&limit=50&maxDuration=30ms&minDuration=11ms&spss=3&start=" + strconv.Itoa(ago("4m")) + "&tags=foo%3Dbar",
					"/querier?end=" + strconv.Itoa(ago("2m")) + "&limit=50&maxDuration=30ms&minDuration=11ms&spss=3&start=" + strconv.Itoa(ago("3m")) + "&tags=foo%3Dbar",
					"/querier?end=" + strconv.Itoa(ago("1m")) + "&limit=50&maxDuration=30ms&minDuration=11ms&spss=3&start=" + strconv.Itoa(ago("2m")) + "&tags=foo%3Dbar",
					"/querier?end=" + strconv.Itoa(now) + "&limit=50&maxDuration=30ms&minDuration=11ms&spss=3&start=" + strconv.Itoa(ago("1m")) + "&tags=foo%3Dbar",
				},
				ingesterShards: 30,
			},
			{
				request:           "/?tags=foo%3Dbar&minDuration=11ms&maxDuration=30ms&limit=50&start=" + strconv.Itoa(ago("15m")) + "&end=" + strconv.Itoa(ago("0s")),
				queryBackendAfter: 350 * time.Second,
				expectedURI: []string{
					"/querier?end=" + strconv.Itoa(ago("3m54s")) + "&limit=50&maxDuration=30ms&minDuration=11ms&spss=3&start=" + strconv.Itoa(ago("5m50s")) + "&tags=foo%3Dbar",
					"/querier?end=" + strconv.Itoa(ago("1m58s")) + "&limit=50&maxDuration=30ms&minDuration=11ms&spss=3&start=" + strconv.Itoa(ago("3m54s")) + "&tags=foo%3Dbar",
					"/querier?end=" + strconv.Itoa(now) + "&limit=50&maxDuration=30ms&minDuration=11ms&spss=3&start=" + strconv.Itoa(ago("1m58s")) + "&tags=foo%3Dbar",
				},
				ingesterShards: 3,
			},
		}

		for i, tc := range tests {
			t.Logf("test case %d", i)
			require.Greater(t, tc.ingesterShards, 0)
			s := &asyncSearchSharder{
				cfg: SearchSharderConfig{
					QueryBackendAfter: tc.queryBackendAfter,
					IngesterShards:    tc.ingesterShards,
				},
			}
			req := httptest.NewRequest("GET", tc.request, nil)

			searchReq, err := api.ParseSearchRequest(req)
			require.NoError(t, err)

			reqChan := make(chan pipeline.Request, tc.ingesterShards)
			defer close(reqChan)

			pr := pipeline.NewHTTPRequest(req)
			pr.SetWeight(2)
			actualSearchResponse, err := s.ingesterRequests("test", pr, *searchReq, reqChan)
			if tc.expectedError != nil {
				require.Equal(t, tc.expectedError, err)
				continue
			}
			require.NoError(t, err)
			require.Equal(t, len(tc.expectedURI), len(reqChan))
			require.Equal(t, len(tc.expectedURI), actualSearchResponse.TotalJobs)
			if len(tc.expectedURI) > 0 {
				require.Equal(t, len(tc.expectedURI), int(actualSearchResponse.Shards[0].TotalJobs))
				expectedCompletedThrough := math.MaxUint32      // normal ingester shard completes no time on purpose
				if searchReq.Start == 0 && searchReq.End == 0 { // ingester only search completes all time on purpose
					expectedCompletedThrough = 1
				}
				require.Equal(t, expectedCompletedThrough, int(actualSearchResponse.Shards[0].CompletedThroughSeconds))
			} else {
				require.Equal(t, 0, len(actualSearchResponse.Shards))
			}

			// drain the channel and check the URIs
			for _, expectedURI := range tc.expectedURI {
				req := <-reqChan
				require.NotNil(t, req)

				values := req.HTTPRequest().URL.Query()
				expectedQueryStringValues, err := url.ParseQuery(expectedURI)
				require.NoError(t, err)

				for k, v := range expectedQueryStringValues {
					key := k

					// Due the way the query string is parse, we need to ensure that
					// the first query param is captured.  Split the key on the first ? and
					// use the second part as the key.
					if strings.Contains(k, "?") {
						parts := strings.Split(k, "?")
						require.Equal(t, 2, len(parts))
						key = parts[1]
					}

					if key == "start" || key == "end" {
						// check the time difference between the expected and actual
						// start/end times is within a tolerance for the use of time.Now()
						// in the code compared to when the tests check the values.
						// Use 2s tolerance because timestamps are truncated to Unix seconds,
						// so crossing a second boundary causes a 1s difference.
						const tolerance = 2 * time.Second

						actual := timeFrom(t, values[key][0])
						expected := timeFrom(t, v[0])

						diff := expected.Sub(actual)
						assert.LessOrEqual(t, diff, tolerance)

						diff = actual.Sub(expected)
						assert.LessOrEqual(t, diff, tolerance)

						continue
					}

					require.Equal(t, v, values[k])
					require.Equal(t, 2, req.Weight())
				}
			}
		}
	})
}

func timeFrom(t *testing.T, n string) time.Time {
	i, err := strconv.ParseInt(n, 10, 32)
	require.NoError(t, err)
	return time.Unix(i, 0)
}

func TestBackendRange(t *testing.T) {
	now := int(time.Now().Unix())
	fiveMinutesAgo := int(time.Now().Add(-5 * time.Minute).Unix())
	tenMinutesAgo := int(time.Now().Add(-10 * time.Minute).Unix())
	fifteenMinutesAgo := int(time.Now().Add(-15 * time.Minute).Unix())
	twentyMinutesAgo := int(time.Now().Add(-20 * time.Minute).Unix())

	tests := []struct {
		request           string
		queryBackendAfter time.Duration
		expectedStart     uint32
		expectedEnd       uint32
	}{
		// start/end is outside queryBackendAfter
		{
			request:           "/?tags=foo%3Dbar&minDuration=10ms&maxDuration=30ms&limit=50&start=10&end=20",
			queryBackendAfter: time.Minute,
			expectedStart:     10,
			expectedEnd:       20,
		},
		// start/end is inside queryBackendAfter
		{
			request:           "/?tags=foo%3Dbar&minDuration=10ms&maxDuration=30ms&limit=50&start=" + strconv.Itoa(tenMinutesAgo) + "&end=" + strconv.Itoa(now),
			queryBackendAfter: 15 * time.Minute,
			expectedStart:     uint32(fifteenMinutesAgo),
			expectedEnd:       uint32(fifteenMinutesAgo),
		},
		// queryBackendAfter = 0 results in no ingester query
		{
			request:           "/?tags=foo%3Dbar&minDuration=10ms&maxDuration=30ms&limit=50&start=" + strconv.Itoa(tenMinutesAgo) + "&end=" + strconv.Itoa(now),
			queryBackendAfter: 0,
			expectedStart:     uint32(tenMinutesAgo),
			expectedEnd:       uint32(now),
		},
		// start/end = 20 - 10 mins ago - break across query backend after
		//  ingester start/End = 15 - 10 mins ago
		//  backend start/End = 20 - 10 mins ago
		{
			request:           "/?tags=foo%3Dbar&minDuration=10ms&maxDuration=30ms&limit=50&start=" + strconv.Itoa(twentyMinutesAgo) + "&end=" + strconv.Itoa(tenMinutesAgo),
			queryBackendAfter: 5 * time.Minute,
			expectedStart:     uint32(twentyMinutesAgo),
			expectedEnd:       uint32(tenMinutesAgo),
		},
		// start/end = 10 - now mins ago - break across query backend after
		//  ingester start/End = 10 - now mins ago
		//  backend start/End = 15 - 10 mins ago
		{
			request:           "/?tags=foo%3Dbar&minDuration=10ms&maxDuration=30ms&limit=50&start=" + strconv.Itoa(tenMinutesAgo) + "&end=" + strconv.Itoa(now),
			queryBackendAfter: 5 * time.Minute,
			expectedStart:     uint32(tenMinutesAgo),
			expectedEnd:       uint32(fiveMinutesAgo),
		},
		// start/end = 20 - now mins ago - break across query backend after
		//  ingester start/End = 15 - now mins ago
		//  backend start/End = 20 - 5 mins ago
		{
			request:           "/?tags=foo%3Dbar&minDuration=10ms&maxDuration=30ms&limit=50&start=" + strconv.Itoa(twentyMinutesAgo) + "&end=" + strconv.Itoa(now),
			queryBackendAfter: 5 * time.Minute,
			expectedStart:     uint32(twentyMinutesAgo),
			expectedEnd:       uint32(fiveMinutesAgo),
		},
		// request without start and end should return start and end as 0
		{
			request:           "/?tags=foo%3Dbar&minDuration=10ms&maxDuration=30ms&limit=50",
			queryBackendAfter: 5 * time.Minute,
			expectedStart:     0,
			expectedEnd:       0,
		},
	}

	for _, tc := range tests {
		req := httptest.NewRequest("GET", tc.request, nil)

		searchReq, err := api.ParseSearchRequest(req)
		require.NoError(t, err)

		actualStart, actualEnd := backendRange(searchReq.Start, searchReq.End, tc.queryBackendAfter)
		assert.Equal(t, int(tc.expectedStart), int(actualStart))
		assert.Equal(t, int(tc.expectedEnd), int(actualEnd))
	}
}

func TestTotalJobsIncludesIngester(t *testing.T) {
	next := pipeline.AsyncRoundTripperFunc[combiner.PipelineResponse](func(_ pipeline.Request) (pipeline.Responses[combiner.PipelineResponse], error) {
		resString, err := (&jsonpb.Marshaler{}).MarshalToString(&tempopb.SearchResponse{
			Metrics: &tempopb.SearchMetrics{},
		})
		require.NoError(t, err)

		return pipeline.NewHTTPToAsyncResponse(&http.Response{
			Body:       io.NopCloser(strings.NewReader(resString)),
			StatusCode: 200,
		}), nil
	})

	o, err := overrides.NewOverrides(overrides.Config{}, nil, prometheus.DefaultRegisterer)
	require.NoError(t, err)

	blockTime := time.Now().Add(-10 * time.Minute).Unix()

	sharder := newAsyncSearchSharder(&mockReader{
		metas: []*backend.BlockMeta{ // one block with 2 records that are each the target bytes per request will force 2 sub queries
			{
				StartTime:    time.Unix(blockTime, 0),
				EndTime:      time.Unix(blockTime, 0),
				Size_:        defaultTargetBytesPerRequest * 2,
				TotalRecords: 2,
				BlockID:      backend.MustParse("00000000-0000-0000-0000-000000000000"),
			},
		},
	}, o, SearchSharderConfig{
		QueryBackendAfter:     5 * time.Minute,
		ConcurrentRequests:    1, // 1 concurrent request to force order
		TargetBytesPerRequest: defaultTargetBytesPerRequest,
		MostRecentShards:      defaultMostRecentShards,
		IngesterShards:        1,
	}, nil, newJobsPerQueryHistogram(), log.NewNopLogger())
	testRT := sharder.Wrap(next)

	// query range straddles the QueryBackendAfter boundary so both backend and ingester are queried
	path := fmt.Sprintf("/?start=%d&end=%d", blockTime-1, time.Now().Unix())
	req := httptest.NewRequest("GET", path, nil)
	ctx := req.Context()
	ctx = user.InjectOrgID(ctx, "blerg")
	req = req.WithContext(ctx)

	resps, err := testRT.RoundTrip(pipeline.NewHTTPRequest(req))
	require.NoError(t, err)
	// find a response with total jobs > . this is the metadata response

	totalJobs := 0
	for {
		res, done, err := resps.Next(context.Background())

		if res.IsMetadata() {
			searchJobResponse := res.(*combiner.SearchJobResponse)
			totalJobs += searchJobResponse.TotalJobs

			break
		}

		require.NoError(t, err)
		require.False(t, done)
	}

	// 2 jobs for the meta + 1 for the ingester
	assert.Equal(t, 3, totalJobs)
}

func TestSearchSharderRoundTripBadRequest(t *testing.T) {
	next := pipeline.AsyncRoundTripperFunc[combiner.PipelineResponse](func(_ pipeline.Request) (pipeline.Responses[combiner.PipelineResponse], error) {
		return nil, nil
	})

	o, err := overrides.NewOverrides(overrides.Config{}, nil, prometheus.DefaultRegisterer)
	require.NoError(t, err)

	sharder := newAsyncSearchSharder(&mockReader{}, o, SearchSharderConfig{
		ConcurrentRequests:    defaultConcurrentRequests,
		TargetBytesPerRequest: defaultTargetBytesPerRequest,
		MostRecentShards:      defaultMostRecentShards,
		MaxDuration:           5 * time.Minute,
		MaxSpansPerSpanSet:    100,
	}, nil, newJobsPerQueryHistogram(), log.NewNopLogger())
	testRT := sharder.Wrap(next)

	// no org id
	req := httptest.NewRequest("GET", "/?start=1000&end=1100", nil)
	resp, err := testRT.RoundTrip(pipeline.NewHTTPRequest(req))
	testBadRequestFromResponses(t, resp, err, "no org id")

	// start/end outside of max duration
	req = httptest.NewRequest("GET", "/?start=1000&end=1500", nil)
	req = req.WithContext(user.InjectOrgID(req.Context(), "blerg"))
	resp, err = testRT.RoundTrip(pipeline.NewHTTPRequest(req))
	testBadRequestFromResponses(t, resp, err, "range specified by start and end exceeds 5m0s. received start=1000 end=1500")

	// spans per span set greater than maximum
	req = httptest.NewRequest("GET", "/?spss=200", nil)
	req = req.WithContext(user.InjectOrgID(req.Context(), "blerg"))
	resp, err = testRT.RoundTrip(pipeline.NewHTTPRequest(req))
	testBadRequestFromResponses(t, resp, err, "spans per span set exceeds 100. received 200")

	// bad request
	req = httptest.NewRequest("GET", "/?start=asdf&end=1500", nil)
	resp, err = testRT.RoundTrip(pipeline.NewHTTPRequest(req))
	testBadRequestFromResponses(t, resp, err, "invalid start: strconv.ParseUint: parsing \"asdf\": invalid syntax")

	// test max duration error with overrides
	o, err = overrides.NewOverrides(overrides.Config{
		Defaults: overrides.Overrides{
			Read: overrides.ReadOverrides{
				MaxSearchDuration: model.Duration(time.Minute),
			},
		},
	}, nil, prometheus.DefaultRegisterer)
	require.NoError(t, err)

	sharder = newAsyncSearchSharder(&mockReader{}, o, SearchSharderConfig{
		ConcurrentRequests:    defaultConcurrentRequests,
		TargetBytesPerRequest: defaultTargetBytesPerRequest,
		MostRecentShards:      defaultMostRecentShards,
		MaxDuration:           5 * time.Minute,
	}, nil, newJobsPerQueryHistogram(), log.NewNopLogger())
	testRT = sharder.Wrap(next)

	req = httptest.NewRequest("GET", "/?start=1000&end=1500", nil)
	req = req.WithContext(user.InjectOrgID(req.Context(), "blerg"))
	resp, err = testRT.RoundTrip(pipeline.NewHTTPRequest(req))
	testBadRequestFromResponses(t, resp, err, "range specified by start and end exceeds 1m0s. received start=1000 end=1500")
}

func testBadRequestFromResponses(t *testing.T, resp pipeline.Responses[combiner.PipelineResponse], err error, expectedBody string) {
	require.NoError(t, err)

	r, done, err := resp.Next(context.Background())
	require.NoError(t, err)
	require.True(t, done) // there should only be one response

	testBadRequest(t, r.HTTPResponse(), err, expectedBody)
}

func testBadRequest(t *testing.T, resp *http.Response, err error, expectedBody string) {
	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
	assert.Nil(t, err)
	buff, err := io.ReadAll(resp.Body)
	assert.NoError(t, err)
	assert.Equal(t, expectedBody, string(buff))
}

func TestAdjustLimit(t *testing.T) {
	l, err := adjustLimit(0, 10, 0)
	require.Equal(t, uint32(10), l)
	require.NoError(t, err)

	l, err = adjustLimit(3, 10, 0)
	require.Equal(t, uint32(3), l)
	require.NoError(t, err)

	l, err = adjustLimit(3, 10, 20)
	require.Equal(t, uint32(3), l)
	require.NoError(t, err)

	l, err = adjustLimit(25, 10, 20)
	require.Equal(t, uint32(0), l)
	require.EqualError(t, err, "limit 25 exceeds max limit 20")
}

func TestMaxDuration(t *testing.T) {
	//
	o, err := overrides.NewOverrides(overrides.Config{}, nil, prometheus.DefaultRegisterer)
	require.NoError(t, err)
	sharder := asyncSearchSharder{
		cfg: SearchSharderConfig{
			MaxDuration: 5 * time.Minute,
		},
		overrides: o,
	}
	actual := sharder.maxDuration("test")
	assert.Equal(t, 5*time.Minute, actual)

	o, err = overrides.NewOverrides(overrides.Config{
		Defaults: overrides.Overrides{
			Read: overrides.ReadOverrides{
				MaxSearchDuration: model.Duration(10 * time.Minute),
			},
		},
	}, nil, prometheus.DefaultRegisterer)
	require.NoError(t, err)
	sharder = asyncSearchSharder{
		cfg: SearchSharderConfig{
			MaxDuration: 5 * time.Minute,
		},
		overrides: o,
	}
	actual = sharder.maxDuration("test")
	assert.Equal(t, 10*time.Minute, actual)
}

func newJobsPerQueryHistogram() *prometheus.HistogramVec {
	return prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "test_query_frontend_jobs_per_query",
		Help:    "Test histogram for jobs per query.",
		Buckets: prometheus.DefBuckets,
	}, []string{"op"})
}

func TestHashTraceQLQuery(t *testing.T) {
	// exact same queries should have the same hash
	h1 := hashForSearchRequest(&tempopb.SearchRequest{Query: "{ span.foo = `bar` }"})
	h2 := hashForSearchRequest(&tempopb.SearchRequest{Query: "{ span.foo = `bar` }"})
	require.Equal(t, h1, h2)

	// equivalent queries should have the same hash
	h1 = hashForSearchRequest(&tempopb.SearchRequest{Query: "{ span.foo = `bar`     }"})
	h2 = hashForSearchRequest(&tempopb.SearchRequest{Query: "{ span.foo = `bar` }"})
	require.Equal(t, h1, h2)

	h1 = hashForSearchRequest(&tempopb.SearchRequest{Query: "{ (span.foo = `bar`) || (span.bar = `foo`) }"})
	h2 = hashForSearchRequest(&tempopb.SearchRequest{Query: "{ span.foo = `bar` || span.bar = `foo` }"})
	require.Equal(t, h1, h2)

	// different queries should have different hashes
	h1 = hashForSearchRequest(&tempopb.SearchRequest{Query: "{ span.foo = `bar` }"})
	h2 = hashForSearchRequest(&tempopb.SearchRequest{Query: "{ span.foo = `baz` }"})
	require.NotEqual(t, h1, h2)

	// invalid queries should return 0
	h1 = hashForSearchRequest(&tempopb.SearchRequest{Query: "{ span.foo = `bar` "})
	require.Equal(t, uint64(0), h1)

	h1 = hashForSearchRequest(&tempopb.SearchRequest{Query: ""})
	require.Equal(t, uint64(0), h1)

	// same queries with different spss and limit should have the different hash
	h1 = hashForSearchRequest(&tempopb.SearchRequest{Query: "{ span.foo = `bar` }", Limit: 1})
	h2 = hashForSearchRequest(&tempopb.SearchRequest{Query: "{ span.foo = `bar` }", Limit: 2})
	require.NotEqual(t, h1, h2)

	h1 = hashForSearchRequest(&tempopb.SearchRequest{Query: "{ span.foo = `bar` }", SpansPerSpanSet: 1})
	h2 = hashForSearchRequest(&tempopb.SearchRequest{Query: "{ span.foo = `bar` }", SpansPerSpanSet: 2})
	require.NotEqual(t, h1, h2)
}

func TestBackendShards(t *testing.T) {
	tcs := []struct {
		name      string
		maxShards int
		searchEnd uint32
		expected  []shardtracker.Shard
	}{
		{
			name:      "1 shard, puts all jobs in one shard",
			maxShards: 1,
			searchEnd: 50,
			expected: []shardtracker.Shard{
				{TotalJobs: 8, CompletedThroughSeconds: 1},
			},
		},
		{
			name:      "2 shards, split evenly between",
			maxShards: 2,
			searchEnd: 50,
			expected: []shardtracker.Shard{
				{TotalJobs: 4, CompletedThroughSeconds: 30},
				{TotalJobs: 4, CompletedThroughSeconds: 1},
			},
		},
		{
			name:      "3 shards, one for each block",
			maxShards: 3,
			searchEnd: 50,
			expected: []shardtracker.Shard{
				{TotalJobs: 2, CompletedThroughSeconds: 40},
				{TotalJobs: 2, CompletedThroughSeconds: 30},
				{TotalJobs: 4, CompletedThroughSeconds: 1},
			},
		},
		{
			name:      "4 shards, one for each block",
			maxShards: 4,
			searchEnd: 50,
			expected: []shardtracker.Shard{
				{TotalJobs: 2, CompletedThroughSeconds: 40},
				{TotalJobs: 2, CompletedThroughSeconds: 30},
				{TotalJobs: 2, CompletedThroughSeconds: 20},
				{TotalJobs: 2, CompletedThroughSeconds: 1},
			},
		},
		{
			name:      "5 shards, one for each block",
			maxShards: 5,
			searchEnd: 50,
			expected: []shardtracker.Shard{
				{TotalJobs: 2, CompletedThroughSeconds: 40},
				{TotalJobs: 2, CompletedThroughSeconds: 30},
				{TotalJobs: 2, CompletedThroughSeconds: 20},
				{TotalJobs: 2, CompletedThroughSeconds: 10},
			},
		},
		{
			name:      "4 shards, search end forces 2 blocks in the first shard",
			maxShards: 4,
			searchEnd: 35,
			expected: []shardtracker.Shard{
				{TotalJobs: 4, CompletedThroughSeconds: 30},
				{TotalJobs: 2, CompletedThroughSeconds: 20},
				{TotalJobs: 2, CompletedThroughSeconds: 10},
			},
		},
		{
			name:      "4 shards, search end forces 3 blocks in the first shard",
			maxShards: 4,
			searchEnd: 25,
			expected: []shardtracker.Shard{
				{TotalJobs: 6, CompletedThroughSeconds: 20},
				{TotalJobs: 2, CompletedThroughSeconds: 10},
			},
		},
		{
			name:      "2 shards, search end forces 2 blocks in the first shard",
			maxShards: 2,
			searchEnd: 35,
			expected: []shardtracker.Shard{
				{TotalJobs: 4, CompletedThroughSeconds: 30},
				{TotalJobs: 4, CompletedThroughSeconds: 1},
			},
		},
	}

	// create 4 metas with 2 records each for all the above test cases to use. 8 jobs total
	metas := make([]*backend.BlockMeta, 0, 4)
	for i := 0; i < 4; i++ {
		metas = append(metas, &backend.BlockMeta{
			StartTime:    time.Unix(int64(i*10), 0),        // block 0 starts at 0
			EndTime:      time.Unix(int64(i*10)+10, 0),     // block 0 ends a 10
			Size_:        defaultTargetBytesPerRequest * 2, // 2 jobs per block
			TotalRecords: 2,
			BlockID:      backend.MustParse("00000000-0000-0000-0000-000000000000"),
		})
	}

	// sort
	sort.Slice(metas, func(i, j int) bool {
		return metas[i].EndTime.After(metas[j].EndTime)
	})

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			fn := backendJobsFunc(metas, defaultTargetBytesPerRequest, tc.maxShards, tc.searchEnd)
			actualShards := []shardtracker.Shard{}

			fn(func(jobs int, _ uint64, completedThroughTime uint32) {
				actualShards = append(actualShards, shardtracker.Shard{
					TotalJobs:               uint32(jobs),
					CompletedThroughSeconds: completedThroughTime,
				})
			}, nil)

			assert.Equal(t, tc.expected, actualShards)
		})
	}
}

func TestSearchSharderReturnsConsistentShards(t *testing.T) {
	now := time.Now()

	// Create various block metas for testing different scenarios
	blockMetas := []*backend.BlockMeta{
		// Recent blocks (last 5 minutes)
		{StartTime: now.Add(-5 * time.Minute), EndTime: now.Add(-3 * time.Minute), Size_: defaultTargetBytesPerRequest, TotalRecords: 10, BlockID: backend.MustParse("00000000-0000-0000-0000-000000000001")},
		{StartTime: now.Add(-3 * time.Minute), EndTime: now.Add(-1 * time.Minute), Size_: defaultTargetBytesPerRequest * 2, TotalRecords: 20, BlockID: backend.MustParse("00000000-0000-0000-0000-000000000002")},

		// Medium age blocks (10-20 minutes)
		{StartTime: now.Add(-20 * time.Minute), EndTime: now.Add(-15 * time.Minute), Size_: defaultTargetBytesPerRequest * 3, TotalRecords: 30, BlockID: backend.MustParse("00000000-0000-0000-0000-000000000003")},
		{StartTime: now.Add(-15 * time.Minute), EndTime: now.Add(-10 * time.Minute), Size_: defaultTargetBytesPerRequest, TotalRecords: 15, BlockID: backend.MustParse("00000000-0000-0000-0000-000000000004")},

		// Old blocks (30+ minutes)
		{StartTime: now.Add(-40 * time.Minute), EndTime: now.Add(-35 * time.Minute), Size_: defaultTargetBytesPerRequest * 4, TotalRecords: 40, BlockID: backend.MustParse("00000000-0000-0000-0000-000000000005")},
		{StartTime: now.Add(-35 * time.Minute), EndTime: now.Add(-30 * time.Minute), Size_: defaultTargetBytesPerRequest * 2, TotalRecords: 25, BlockID: backend.MustParse("00000000-0000-0000-0000-000000000006")},
	}

	o, err := overrides.NewOverrides(overrides.Config{}, nil, prometheus.DefaultRegisterer)
	require.NoError(t, err)

	const ingesterShards = 3
	const mostRecentShards = 10

	const queryBackendAfter = 5 * time.Minute

	// Test scenarios with different configurations
	testCases := []struct {
		name      string
		startTime time.Time
		endTime   time.Time
	}{
		{
			name:      "ingester_only_recent",
			startTime: now.Add(-5 * time.Minute),
			endTime:   now,
		},
		{
			name:      "backend_only_old",
			startTime: now.Add(-40 * time.Minute),
			endTime:   now.Add(-30 * time.Minute),
		},
		{
			name:      "both_ingester_and_backend",
			startTime: now.Add(-25 * time.Minute),
			endTime:   now,
		},
		{
			name:      "multiple_shards_complex",
			startTime: now.Add(-45 * time.Minute),
			endTime:   now,
		},
		{
			name:      "edge_case_boundaries",
			startTime: now.Add(-15 * time.Minute),
			endTime:   now.Add(-14 * time.Minute),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			sharder := newAsyncSearchSharder(&mockReader{metas: blockMetas}, o, SearchSharderConfig{
				QueryBackendAfter:     queryBackendAfter,
				IngesterShards:        ingesterShards,
				MostRecentShards:      mostRecentShards,
				TargetBytesPerRequest: defaultTargetBytesPerRequest,
				ConcurrentRequests:    5,
			}, nil, newJobsPerQueryHistogram(), log.NewNopLogger())

			// Create request with the test scenario time range
			path := fmt.Sprintf("/?tags=service%%3Dapi&limit=100&start=%d&end=%d",
				tc.startTime.Unix(), tc.endTime.Unix())
			req := httptest.NewRequest("GET", path, nil)
			req = req.WithContext(user.InjectOrgID(req.Context(), "test-tenant"))

			_, err = api.ParseSearchRequest(req)
			require.NoError(t, err)

			// Execute the search using the sharder
			next := pipeline.AsyncRoundTripperFunc[combiner.PipelineResponse](func(r pipeline.Request) (pipeline.Responses[combiner.PipelineResponse], error) {
				// Mock response for each request
				resString, err := (&jsonpb.Marshaler{}).MarshalToString(&tempopb.SearchResponse{
					Metrics: &tempopb.SearchMetrics{},
				})
				require.NoError(t, err)

				shardIdx := r.ResponseData()
				return pipeline.NewHTTPToAsyncResponseWithRequestData(&http.Response{
					Body:       io.NopCloser(strings.NewReader(resString)),
					StatusCode: 200,
				}, shardIdx), nil
			})

			testRT := sharder.Wrap(next)
			resps, err := testRT.RoundTrip(pipeline.NewHTTPRequest(req))
			require.NoError(t, err)

			// collect all responses and tally the shards counts. make sure they match the returned shards in the job response
			var searchJobResponse *combiner.SearchJobResponse
			actualShards := make([]int, 50)

			for {
				res, done, err := resps.Next(context.Background())
				require.NoError(t, err)

				if done || res == nil {
					break
				}

				if res.IsMetadata() {
					if jobRes, ok := res.(*combiner.SearchJobResponse); ok {
						searchJobResponse = jobRes
						continue
					}
				}

				shardIdx := res.RequestData().(int)
				actualShards[shardIdx]++
			}

			require.NotNil(t, searchJobResponse, "Expected to receive SearchJobResponse metadata")

			// validate each shard bucket received the expected number of jobs
			for i, shard := range searchJobResponse.Shards {
				require.Equal(t, int(shard.TotalJobs), actualShards[i])
			}

			// Validate shard ordering (should be ordered by CompletedThroughSeconds descending, with MaxUint32 first)
			for i := 1; i < len(searchJobResponse.Shards); i++ {
				prev := searchJobResponse.Shards[i-1]
				curr := searchJobResponse.Shards[i]

				if prev.CompletedThroughSeconds != math.MaxUint32 && curr.CompletedThroughSeconds != math.MaxUint32 {
					require.GreaterOrEqual(t, prev.CompletedThroughSeconds, curr.CompletedThroughSeconds,
						"Backend shards should be ordered by CompletedThroughSeconds descending (most recent first)")
				}
			}

			// fmt.Println("Test case:", tc.name, "passed with shards:", searchJobResponse.Shards)
		})
	}
}

// TestDefaultSpansPerSpanSet verifies that the default_spans_per_span_set configuration
// is properly used when no spss parameter is provided in the request
func TestDefaultSpansPerSpanSet(t *testing.T) {
	tests := []struct {
		name               string
		configDefault      uint32
		requestSpss        string // empty means no spss param
		expectedSpss       uint32
		maxSpansPerSpanSet uint32
	}{
		{
			name:               "use configured default when no spss param",
			configDefault:      10,
			requestSpss:        "",
			expectedSpss:       10,
			maxSpansPerSpanSet: 100,
		},
		{
			name:               "use zero as configured default (unlimited)",
			configDefault:      0,
			requestSpss:        "",
			expectedSpss:       0, // 0 means unlimited when explicitly configured
			maxSpansPerSpanSet: 0,
		},
		{
			name:               "override configured default with request param",
			configDefault:      10,
			requestSpss:        "5",
			expectedSpss:       5,
			maxSpansPerSpanSet: 100,
		},
		{
			name:               "spss=0 in URL means unlimited when max=0",
			configDefault:      10,
			requestSpss:        "0",
			expectedSpss:       0, // 0 means unlimited, not "return 0 spans"
			maxSpansPerSpanSet: 0, // max=0 means unlimited allowed
		},
		{
			name:               "respect max_spans_per_span_set=0 (unlimited)",
			configDefault:      10,
			requestSpss:        "1000",
			expectedSpss:       1000,
			maxSpansPerSpanSet: 0, // 0 means unlimited
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			// Track the actual spss value that was used
			var capturedSpss uint32

			next := pipeline.AsyncRoundTripperFunc[combiner.PipelineResponse](func(r pipeline.Request) (pipeline.Responses[combiner.PipelineResponse], error) {
				// Parse the request to capture spss
				req, err := api.ParseSearchRequest(r.HTTPRequest())
				if err == nil {
					capturedSpss = req.SpansPerSpanSet
				}
				return pipeline.NewAsyncResponse(nil), nil
			})

			o, err := overrides.NewOverrides(overrides.Config{}, nil, prometheus.DefaultRegisterer)
			require.NoError(t, err)

			sharder := newAsyncSearchSharder(&mockReader{}, o, SearchSharderConfig{
				ConcurrentRequests:     defaultConcurrentRequests,
				TargetBytesPerRequest:  defaultTargetBytesPerRequest,
				DefaultSpansPerSpanSet: tc.configDefault,
				MaxSpansPerSpanSet:     tc.maxSpansPerSpanSet,
			}, nil, newJobsPerQueryHistogram(), log.NewNopLogger())
			testRT := sharder.Wrap(next)

			// Build request URL
			urlPath := "/"
			if tc.requestSpss != "" {
				urlPath = "/?spss=" + tc.requestSpss
			}

			req := httptest.NewRequest("GET", urlPath, nil)
			req = req.WithContext(user.InjectOrgID(req.Context(), "test-tenant"))

			resps, err := testRT.RoundTrip(pipeline.NewHTTPRequest(req))
			require.NoError(t, err)

			// Drain responses to ensure all the goroutines complete
			for {
				_, done, err := resps.Next(context.Background())
				require.NoError(t, err)
				if done {
					break
				}
			}

			assert.Equal(t, tc.expectedSpss, capturedSpss, "spss value mismatch")
		})
	}
}

// TestSkipASTTransformationsMerge verifies that the per-request skip_ast_transformations
// URL parameter is merged with the global config list rather than overwritten by it.
func TestSkipASTTransformationsMerge(t *testing.T) {
	tests := []struct {
		name         string
		globalSkip   []string
		requestSkip  string // URL param value; empty means param is absent
		expectedSkip []string
	}{
		{
			name:         "global config only",
			globalSkip:   []string{"global_skip"},
			requestSkip:  "",
			expectedSkip: []string{"global_skip"},
		},
		{
			name:         "per-request only",
			globalSkip:   nil,
			requestSkip:  "per_req_skip",
			expectedSkip: []string{"per_req_skip"},
		},
		{
			name:         "merge global and per-request",
			globalSkip:   []string{"global_skip"},
			requestSkip:  "per_req_skip",
			expectedSkip: []string{"global_skip", "per_req_skip"},
		},
		{
			name:         "duplicated entries",
			globalSkip:   []string{"or_to_in"},
			requestSkip:  "or_to_in",
			expectedSkip: []string{"or_to_in"},
		},
		{
			name:         "neither",
			globalSkip:   nil,
			requestSkip:  "",
			expectedSkip: nil,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var capturedSkip []string

			next := pipeline.AsyncRoundTripperFunc[combiner.PipelineResponse](func(r pipeline.Request) (pipeline.Responses[combiner.PipelineResponse], error) {
				req, err := api.ParseSearchRequest(r.HTTPRequest())
				if err == nil {
					capturedSkip = req.SkipASTTransformations
				}
				return pipeline.NewAsyncResponse(nil), nil
			})

			o, err := overrides.NewOverrides(overrides.Config{}, nil, prometheus.DefaultRegisterer)
			require.NoError(t, err)

			sharder := newAsyncSearchSharder(&mockReader{}, o, SearchSharderConfig{
				ConcurrentRequests:    defaultConcurrentRequests,
				TargetBytesPerRequest: defaultTargetBytesPerRequest,
			}, tc.globalSkip, newJobsPerQueryHistogram(), log.NewNopLogger())
			testRT := sharder.Wrap(next)

			urlPath := "/"
			if tc.requestSkip != "" {
				urlPath = "/?skip_ast_transformations=" + tc.requestSkip
			}

			req := httptest.NewRequest("GET", urlPath, nil)
			req = req.WithContext(user.InjectOrgID(req.Context(), "test-tenant"))

			resps, err := testRT.RoundTrip(pipeline.NewHTTPRequest(req))
			require.NoError(t, err)

			for {
				_, done, err := resps.Next(context.Background())
				require.NoError(t, err)
				if done {
					break
				}
			}

			assert.Equal(t, tc.expectedSkip, capturedSkip, "skip_ast_transformations mismatch")
		})
	}
}

// TestAsyncSearchSharder_VcntBytesReadSurfacesOnJobMetrics is the required sharder-level test
// for issue #218 Phase 3: drives the REAL asyncSearchSharder.RoundTrip with a real
// RawReaderProvider (local backend) and real VCNT objects, asserting the resulting
// SearchJobResponse metadata's VcntBytesRead equals the EXACT total bytes of the .vcnt objects
// written for the queried column (mutation-style: an exact value, not merely "> 0").
func TestAsyncSearchSharder_VcntBytesReadSurfacesOnJobMetrics(t *testing.T) {
	restore := vblockpack.ConfigureValueIndexQueryForTest(emptyVIStore{}, testIndexPrefix)
	defer restore()

	rawR, rawW := newLocalRawReadWriter(t)
	// writeVCNTObject always writes under the "tenant-a" keypath (see its own doc comment) --
	// the tenant used for this test must match it exactly.
	tenant := "tenant-a"

	// "POST" is the minority value (Selective) so the query never plan-time-declines on low
	// selectivity, letting a real, qualified plan (and its VCNT fetch) run to completion.
	obj1 := vcntObj(t, "span.http.method", 60, map[string]int64{"GET": 5, "POST": 3})
	obj2 := vcntObj(t, "span.http.method", 120, map[string]int64{"GET": 2})
	writeVCNTObject(t, rawW, "span.http.method", obj1)
	writeVCNTObject(t, rawW, "span.http.method", obj2)
	expectedBytesRead := int64(len(obj1) + len(obj2))

	bm := backend.NewBlockMeta(tenant, uuid.New(), "vParquet4")
	bm.StartTime = time.Unix(1, 0)
	bm.EndTime = time.Unix(200, 0)
	bm.Size_ = defaultTargetBytesPerRequest
	bm.TotalRecords = 8

	reader := &mockReaderWithRawReader{
		mockReader: &mockReader{metas: []*backend.BlockMeta{bm}},
		rawR:       rawR,
	}

	o, err := overrides.NewOverrides(overrides.Config{}, nil, prometheus.NewRegistry())
	require.NoError(t, err)

	sharder := newAsyncSearchSharder(reader, o, SearchSharderConfig{
		QueryBackendAfter:     0,
		IngesterShards:        1,
		MostRecentShards:      defaultMostRecentShards,
		TargetBytesPerRequest: defaultTargetBytesPerRequest,
		ConcurrentRequests:    5,
	}, nil, newJobsPerQueryHistogram(), log.NewNopLogger())

	// Start must be non-zero: RoundTrip only invokes buildQueryPlan (and therefore the VCNT
	// fetch) when both Start and End are set (see RoundTrip's own "Skipped entirely... when
	// searchReq.Start/End are zero" comment).
	searchReq := &tempopb.SearchRequest{
		Query: `{ span.http.method = "POST" }`,
		Start: 1,
		End:   200,
		Limit: 10,
	}
	httpReq, err := api.BuildSearchRequest(httptest.NewRequest("GET", "/", nil), searchReq)
	require.NoError(t, err)
	httpReq = httpReq.WithContext(user.InjectOrgID(httpReq.Context(), tenant))

	next := pipeline.AsyncRoundTripperFunc[combiner.PipelineResponse](func(r pipeline.Request) (pipeline.Responses[combiner.PipelineResponse], error) {
		resString, merr := (&jsonpb.Marshaler{}).MarshalToString(&tempopb.SearchResponse{Metrics: &tempopb.SearchMetrics{}})
		require.NoError(t, merr)
		return pipeline.NewHTTPToAsyncResponseWithRequestData(&http.Response{
			Body:       io.NopCloser(strings.NewReader(resString)),
			StatusCode: 200,
		}, r.ResponseData()), nil
	})

	testRT := sharder.Wrap(next)
	resps, err := testRT.RoundTrip(pipeline.NewHTTPRequest(httpReq))
	require.NoError(t, err)

	var searchJobResponse *combiner.SearchJobResponse
	for {
		res, done, rerr := resps.Next(context.Background())
		require.NoError(t, rerr)
		if done || res == nil {
			break
		}
		if jobRes, ok := res.(*combiner.SearchJobResponse); ok {
			searchJobResponse = jobRes
		}
	}

	require.NotNil(t, searchJobResponse, "expected to receive SearchJobResponse metadata")
	require.Equal(t, expectedBytesRead, searchJobResponse.VcntBytesRead)
	require.Positive(t, searchJobResponse.VcntBytesRead, "fixture sanity check: the written VCNT objects must be non-empty")
}

func urisEqual(t *testing.T, expectedURIs, actualURIs []string) {
	require.Equal(t, len(expectedURIs), len(actualURIs))

	for i, expected := range expectedURIs {
		actual := actualURIs[i]

		e, err := url.Parse(expected)
		require.NoError(t, err)
		a, err := url.Parse(actual)
		require.NoError(t, err)

		e.RawQuery = e.Query().Encode()
		a.RawQuery = a.Query().Encode()

		assert.Equal(t, e, a)
	}
}
