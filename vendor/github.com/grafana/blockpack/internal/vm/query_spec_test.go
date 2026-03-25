package vm

import (
	"testing"
)

func TestQuerySpecNormalization(t *testing.T) {
	spec := &QuerySpec{
		Filter: FilterSpec{
			AttributeEquals: map[string][]any{
				"name":          {"test"},
				"span.duration": {100},
			},
			AttributeRanges: map[string]*RangeSpec{
				"duration": {MinValue: int64(100), MinInclusive: true},
			},
		},
		Aggregate: AggregateSpec{
			Function: "COUNT",
			GroupBy:  []string{"service.name", "http.method"},
		},
	}

	spec.Normalize()

	// GroupBy should be sorted
	if len(spec.Aggregate.GroupBy) != 2 {
		t.Errorf("expected 2 group by fields, got %d", len(spec.Aggregate.GroupBy))
	}
	if spec.Aggregate.GroupBy[0] != "http.method" || spec.Aggregate.GroupBy[1] != "service.name" {
		t.Errorf("GroupBy not sorted correctly: %v", spec.Aggregate.GroupBy)
	}

	// Field names should be normalized
	if _, ok := spec.Filter.AttributeRanges["span:duration"]; !ok {
		t.Errorf("duration not normalized to span:duration")
	}
}

func TestFilterSpecMatchAll(t *testing.T) {
	spec := FilterSpec{
		IsMatchAll: true,
	}

	if !spec.IsMatchAll {
		t.Error("expected IsMatchAll to be true")
	}
}
