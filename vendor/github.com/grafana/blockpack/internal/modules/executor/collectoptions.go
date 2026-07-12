package executor

import "github.com/grafana/blockpack/internal/modules/queryplanner"

// CollectOptions is a blockpack data type.
type CollectOptions struct {
	// TimestampColumn is the intrinsic column used to order results when WantSort is
	// true, and the column whose refs the match-all fast paths read. It may be set even
	// when WantSort is false (e.g. always "span:start" for trace search) to enable the
	// unsorted match-all-any fast path; in that case it is used only as the ref source,
	// not as a sort key.
	TimestampColumn string
	SelectColumns   []string
	TimeRange       queryplanner.TimeRange
	Limit           int
	StartBlock      int
	BlockCount      int
	Direction       queryplanner.Direction
	AllColumns      bool
	// WantSort indicates the caller wants results ordered by TimestampColumn. When false,
	// fast paths may return ANY Limit results without decoding or sorting timestamps
	// (NOTE-472, issue #393). When true, the timestamp-sorted top-K paths are used.
	WantSort bool
}
