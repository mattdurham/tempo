// Package logqlparser provides a minimal LogQL parser for blockpack log queries.
//
// Supported syntax:
//
//	{label="val", label2=~"regex", label3!="val", label4!~"regex"}
//	  |= "text" != "text" |~ "regex" !~ "regex"
//	  | json | logfmt | label_format dst=src | line_format "tmpl"
//	  | drop field1, field2 | keep field1
//	  | fieldname = "value" | fieldname > 42
//
// Metric queries:
//
//	count_over_time({sel} | pipeline [5m])
//	rate({sel} [5m])
//	sum by (label) (count_over_time(...))
package logqlparser

import "time"

// LogSelector is the root AST node for a LogQL filter query.
// It contains label matchers, an optional chain of line filters,
// and an optional pipeline of post-filter stages.
type LogSelector struct {
	Matchers    []LabelMatcher
	LineFilters []LineFilter
	Pipeline    []PipelineStage
}

// MatchType identifies the type of label match operation.
type MatchType int

// MatchType constants for label matcher operations.
const (
	MatchEqual    MatchType = iota // =
	MatchNotEqual                  // !=
	MatchRegex                     // =~
	MatchNotRegex                  // !~
)

// LabelMatcher matches a single label against a value or regex pattern.
type LabelMatcher struct {
	Name  string
	Value string
	Type  MatchType
}

// FilterType identifies the type of line filter operation.
type FilterType int

// FilterType constants for line filter operations.
const (
	FilterContains    FilterType = iota // |= "text"
	FilterNotContains                   // != "text"
	FilterRegex                         // |~ "regex"
	FilterNotRegex                      // !~ "regex"
)

// LineFilter filters log lines by substring match or regex.
type LineFilter struct {
	Pattern string
	Type    FilterType
}

// PipelineStageType identifies the kind of pipeline stage.
type PipelineStageType int

// PipelineStageType constants for pipeline stage operations.
const (
	StageJSON        PipelineStageType = iota // | json
	StageLogfmt                               // | logfmt
	StageLabelFormat                          // | label_format dst=src
	StageLineFormat                           // | line_format "tmpl"
	StageDrop                                 // | drop field1, field2
	StageKeep                                 // | keep field1, field2
	StageLabelFilter                          // | fieldname op value
	StageUnwrap                               // | unwrap fieldname
)

// PipelineStage represents a single stage in a LogQL pipeline.
// Params carries stage-specific string parameters (e.g., template string, field names,
// or "dst=src" pairs for label_format). LabelFilter is set only for StageLabelFilter.
// OrFilters holds additional label filters combined with OR logic (e.g.,
// | level="error" or level="warn"). The primary LabelFilter is the first
// condition; OrFilters are alternatives that also satisfy the stage.
type PipelineStage struct {
	LabelFilter *LabelFilter
	OrFilters   []*LabelFilter
	Params      []string
	Type        PipelineStageType
}

// FilterOp identifies the comparison operator used in label filter stages.
type FilterOp int

// FilterOp constants for label filter operations.
const (
	OpEqual    FilterOp = iota // =
	OpNotEqual                 // !=
	OpRegex                    // =~
	OpNotRegex                 // !~
	OpGT                       // >
	OpLT                       // <
	OpGTE                      // >=
	OpLTE                      // <=
)

// LabelFilter is the predicate for a StageLabelFilter pipeline stage.
type LabelFilter struct {
	Name  string   // label name to test
	Value string   // comparison value or regex pattern
	Op    FilterOp // comparison operation
}

// MetricExpr represents a LogQL range-vector metric expression such as
// count_over_time({selector} | pipeline [5m]).
type MetricExpr struct {
	Selector      *LogSelector    // the inner log selector
	Function      string          // count_over_time, rate, bytes_rate, bytes_over_time, sum_over_time, etc.
	Unwrap        string          // optional | unwrap field name (empty if not present)
	Pipeline      []PipelineStage // optional pipeline stages before the range
	RangeDuration time.Duration   // range interval (e.g., [5m])
}

// VectorAggExpr represents a LogQL vector aggregation over a metric expression,
// such as sum by (label) (count_over_time(...)).
type VectorAggExpr struct {
	Inner   *MetricExpr // the inner metric expression
	Op      string      // sum, avg, min, max, topk, bottomk
	GroupBy []string    // labels to group by (from "by (l1, l2)")
	Without []string    // labels to exclude (from "without (l1, l2)")
	Param   int         // k value for topk/bottomk; 0 otherwise
}

// LogQuery is the top-level union type representing any parsed LogQL query.
// Exactly one of Selector, Metric, or VectorAgg is non-nil.
//   - Selector: a plain log stream query (filter + pipeline)
//   - Metric: a range-vector metric expression
//   - VectorAgg: a vector aggregation over a metric expression
type LogQuery struct {
	Selector  *LogSelector
	Metric    *MetricExpr
	VectorAgg *VectorAggExpr
}
