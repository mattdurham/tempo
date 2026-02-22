// Copyright 2025 Buf Technologies, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package sql

// SQL operator constants
const (
	OpIsDistinctFrom    = "IS DISTINCT FROM"
	OpIsNotDistinctFrom = "IS NOT DISTINCT FROM"
)

// Type system constants
const (
	TypeUnknown = "unknown"
)

// Span intrinsic field names (colon notation for storage columns)
// These constants are duplicated from internal/executor to avoid import cycles
const (
	// Trace-level intrinsics
	IntrinsicTraceID = "trace:id"

	// Span-level intrinsics
	IntrinsicSpanID            = "span:id"
	IntrinsicSpanParentID      = "span:parent_id"
	IntrinsicSpanName          = "span:name"
	IntrinsicSpanKind          = "span:kind"
	IntrinsicSpanStatus        = "span:status"
	IntrinsicSpanStatusMessage = "span:status_message"
	IntrinsicSpanDuration      = "span:duration"
	IntrinsicSpanStart         = "span:start"
	IntrinsicSpanEnd           = "span:end"

	// Unscoped intrinsic names (TraceQL shorthand without span: prefix)
	UnscopedName          = "name"
	UnscopedKind          = "kind"
	UnscopedStatus        = "status"
	UnscopedStatusMessage = "status_message"
	UnscopedDuration      = "duration"
	UnscopedStart         = "start"
	UnscopedEnd           = "end"
	UnscopedParentID      = "parent_id"
	UnscopedEndTime       = "end_time"
)

// Span attribute field names
const (
	AttrStartTime              = "start_time"
	AttrTraceState             = "trace_state"
	AttrDroppedEventsCount     = "dropped_events_count"
	AttrDroppedAttributesCount = "dropped_attributes_count"
	AttrDroppedLinksCount      = "dropped_links_count"
)

// Resource attribute field names
const (
	AttrServiceName = "service.name"
)

// Aggregation function names (uppercase - for case matching)
const (
	FuncNameAVG       = "AVG"
	FuncNameCOUNT     = "COUNT"
	FuncNameHISTOGRAM = "HISTOGRAM"
	FuncNameMAX       = "MAX"
	FuncNameMIN       = "MIN"
	FuncNameQUANTILE  = "QUANTILE"
	FuncNameSTDDEV    = "STDDEV"
	FuncNameSUM       = "SUM"
	FuncNameRATE      = "RATE"
)

// Aggregation result names (lowercase - for return values)
const (
	AggregationRate = "rate"
)
