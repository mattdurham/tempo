package vblockpack

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/grafana/tempo/pkg/tempopb"
	v11 "github.com/grafana/tempo/pkg/tempopb/trace/v1"
	"github.com/grafana/tempo/tempodb/encoding/common"
)

func TestNewCompactor(t *testing.T) {
	opts := common.CompactionOptions{
		MaxBytesPerTrace: 1000000,
		OutputBlocks:     1,
		BlockConfig: common.BlockConfig{
			RowGroupSizeBytes: 100 * 1024 * 1024,
		},
	}

	compactor := NewCompactor(opts)
	require.NotNil(t, compactor)
	assert.Equal(t, opts, compactor.opts)
}

func TestCompactorCompact(t *testing.T) {
	// Skip this test for now as it requires full implementation
	t.Skip("Full compaction test requires complete blockpack implementation")
}

func TestTraceIDsEqual(t *testing.T) {
	tests := []struct {
		name     string
		t1       *tempopb.Trace
		t2       *tempopb.Trace
		expected bool
	}{
		{
			name:     "both nil",
			t1:       nil,
			t2:       nil,
			expected: true,
		},
		{
			name:     "one nil",
			t1:       &tempopb.Trace{ResourceSpans: []*v11.ResourceSpans{}},
			t2:       nil,
			expected: false,
		},
		{
			name:     "same empty resource spans",
			t1:       &tempopb.Trace{ResourceSpans: []*v11.ResourceSpans{}},
			t2:       &tempopb.Trace{ResourceSpans: []*v11.ResourceSpans{}},
			expected: true,
		},
		{
			name:     "different resource spans count",
			t1:       &tempopb.Trace{ResourceSpans: []*v11.ResourceSpans{{}}},
			t2:       &tempopb.Trace{ResourceSpans: []*v11.ResourceSpans{{}, {}}},
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := traceIDsEqual(tt.t1, tt.t2)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestCountSpansInTrace(t *testing.T) {
	tests := []struct {
		name     string
		trace    *tempopb.Trace
		expected int
	}{
		{
			name:     "nil trace",
			trace:    nil,
			expected: 0,
		},
		{
			name:     "empty trace",
			trace:    &tempopb.Trace{},
			expected: 0,
		},
		{
			name: "trace with spans",
			trace: &tempopb.Trace{
				ResourceSpans: []*v11.ResourceSpans{
					{
						ScopeSpans: []*v11.ScopeSpans{
							{
								Spans: []*v11.Span{
									{Name: "span1"},
									{Name: "span2"},
								},
							},
						},
					},
					{
						ScopeSpans: []*v11.ScopeSpans{
							{
								Spans: []*v11.Span{
									{Name: "span3"},
								},
							},
						},
					},
				},
			},
			expected: 3,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := countSpansInTrace(tt.trace)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestCompareIDs(t *testing.T) {
	tests := []struct {
		name     string
		id1      common.ID
		id2      common.ID
		expected int
	}{
		{
			name:     "equal IDs",
			id1:      []byte{1, 2, 3, 4},
			id2:      []byte{1, 2, 3, 4},
			expected: 0,
		},
		{
			name:     "id1 less than id2",
			id1:      []byte{1, 2, 3, 4},
			id2:      []byte{1, 2, 3, 5},
			expected: -1,
		},
		{
			name:     "id1 greater than id2",
			id1:      []byte{1, 2, 3, 5},
			id2:      []byte{1, 2, 3, 4},
			expected: 1,
		},
		{
			name:     "id1 shorter than id2",
			id1:      []byte{1, 2, 3},
			id2:      []byte{1, 2, 3, 4},
			expected: -1,
		},
		{
			name:     "id1 longer than id2",
			id1:      []byte{1, 2, 3, 4},
			id2:      []byte{1, 2, 3},
			expected: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := compareIDs(tt.id1, tt.id2)
			switch {
			case tt.expected == 0:
				assert.Equal(t, 0, result)
			case tt.expected < 0:
				assert.Less(t, result, 0)
			default:
				assert.Greater(t, result, 0)
			}
		})
	}
}

func TestIsTraceConnected(t *testing.T) {
	tests := []struct {
		name     string
		trace    *tempopb.Trace
		expected bool
	}{
		{
			name:     "nil trace",
			trace:    nil,
			expected: false,
		},
		{
			name:     "empty trace",
			trace:    &tempopb.Trace{},
			expected: false,
		},
		{
			name: "trace with resource spans",
			trace: &tempopb.Trace{
				ResourceSpans: []*v11.ResourceSpans{
					{},
				},
			},
			expected: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := isTraceConnected(tt.trace)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestMergeTraces(t *testing.T) {
	tests := []struct {
		name               string
		traces             []*tempopb.Trace
		expectedNil        bool
		expectedDedupSpans int
	}{
		{
			name:               "empty traces",
			traces:             []*tempopb.Trace{},
			expectedNil:        true,
			expectedDedupSpans: 0,
		},
		{
			name: "single trace",
			traces: []*tempopb.Trace{
				{ResourceSpans: []*v11.ResourceSpans{}},
			},
			expectedNil:        false,
			expectedDedupSpans: 0,
		},
		{
			name: "multiple traces",
			traces: []*tempopb.Trace{
				{
					ResourceSpans: []*v11.ResourceSpans{
						{
							ScopeSpans: []*v11.ScopeSpans{
								{
									Spans: []*v11.Span{
										{Name: "span1"},
									},
								},
							},
						},
					},
				},
				{
					ResourceSpans: []*v11.ResourceSpans{
						{
							ScopeSpans: []*v11.ScopeSpans{
								{
									Spans: []*v11.Span{
										{Name: "span2"},
									},
								},
							},
						},
					},
				},
			},
			expectedNil:        false,
			expectedDedupSpans: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			merged, dedupSpans := mergeTraces(tt.traces)
			if tt.expectedNil {
				assert.Nil(t, merged)
			} else {
				assert.NotNil(t, merged)
			}
			assert.Equal(t, tt.expectedDedupSpans, dedupSpans)
		})
	}
}
