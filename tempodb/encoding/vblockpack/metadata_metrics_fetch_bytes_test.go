package vblockpack

// metadata_metrics_fetch_bytes_test.go — issue #218 Phase 8. FetchTagValues and FetchTagNames'
// MetricsCallback parameter was previously named "_" and never invoked (the executeQuery return
// value was discarded as matches, err := ...), so a caller building tempopb.MetadataMetrics from
// these two methods' callback (e.g. querier.go's internalTagValuesSearchBlockV2/
// internalTagsSearchBlockV2) always got DataFileBytesRead/InspectedBytes == 0. This file drives
// both methods directly against a real block and cross-checks the callback's value against an
// independent second call to executeQuery with the exact same query — not just "nonzero" — then
// mutation-guards the fix (see report; guard performed interactively during implementation).

import (
	"context"
	"testing"

	"github.com/google/uuid"
	blockpack "github.com/grafana/blockpack"
	"github.com/grafana/tempo/pkg/traceql"
	"github.com/grafana/tempo/tempodb/encoding/common"
	"github.com/stretchr/testify/require"
)

// TestFetchTagValues_ReportsDataFileBytesRead drives (*blockpackBlock).FetchTagValues with a real
// condition group against a real block and asserts the MetricsCallback receives exactly the sum
// of QueryStats.Steps[].BytesRead for the equivalent query — cross-checked via an independent
// executeQuery call with the same conditions, not merely "nonzero".
func TestFetchTagValues_ReportsDataFileBytesRead(t *testing.T) {
	block, _ := createFetchTestBlock(t)
	ctx := context.Background()

	conditionGroups := [][]traceql.Condition{{
		{
			Attribute: traceql.NewScopedAttribute(traceql.AttributeScopeResource, false, "service.name"),
			Op:        traceql.OpEqual,
			Operands:  traceql.Operands{traceql.NewStaticString("svc-a")},
		},
	}}
	req := traceql.FetchTagValuesRequest{
		ConditionGroups: conditionGroups,
		TagName:         traceql.NewScopedAttribute(traceql.AttributeScopeSpan, false, "http.method"),
	}

	// Independent cross-check: the exact same query FetchTagValues itself builds internally,
	// executed via the same private executeQuery helper, but as a wholly separate call.
	var flat []traceql.Condition
	for _, g := range conditionGroups {
		flat = append(flat, g...)
	}
	wantQuery := conditionsToTraceQL(flat, true)
	_, wantStats, err := block.executeQuery(ctx, wantQuery, blockpack.QueryOptions{})
	require.NoError(t, err)
	var wantBytes int64
	for _, step := range wantStats.Steps {
		wantBytes += step.BytesRead
	}
	require.NotZero(t, wantBytes, "the independent cross-check query must have performed a real, nonzero block scan")

	var gotBytes uint64
	mcb := func(bytesRead uint64) { gotBytes += bytesRead }
	seenAny := false
	err = block.FetchTagValues(ctx, req, func(traceql.Static) bool { seenAny = true; return false }, mcb, common.SearchOptions{})
	require.NoError(t, err)
	require.True(t, seenAny, "expected FetchTagValues to find at least one tag value for svc-a")

	require.EqualValues(t, wantBytes, gotBytes,
		"issue #218 Phase 8: FetchTagValues' MetricsCallback must report exactly the sum of QueryStats.Steps[].BytesRead")
}

// TestFetchTagNames_WithConditions_ReportsDataFileBytesRead drives the conditioned branch of
// (*blockpackBlock).FetchTagNames (len(flatConditions) > 0) and cross-checks the same way.
func TestFetchTagNames_WithConditions_ReportsDataFileBytesRead(t *testing.T) {
	block, _ := createFetchTestBlock(t)
	ctx := context.Background()

	conditionGroups := [][]traceql.Condition{{
		{
			Attribute: traceql.NewScopedAttribute(traceql.AttributeScopeResource, false, "service.name"),
			Op:        traceql.OpEqual,
			Operands:  traceql.Operands{traceql.NewStaticString("svc-b")},
		},
	}}
	req := traceql.FetchTagsRequest{
		ConditionGroups: conditionGroups,
		Scope:           traceql.AttributeScopeSpan,
	}

	var flat []traceql.Condition
	for _, g := range conditionGroups {
		flat = append(flat, g...)
	}
	wantQuery := conditionsToTraceQL(flat, true)
	_, wantStats, err := block.executeQuery(ctx, wantQuery, blockpack.QueryOptions{})
	require.NoError(t, err)
	var wantBytes int64
	for _, step := range wantStats.Steps {
		wantBytes += step.BytesRead
	}
	require.NotZero(t, wantBytes, "the independent cross-check query must have performed a real, nonzero block scan")

	var gotBytes uint64
	mcb := func(bytesRead uint64) { gotBytes += bytesRead }
	seenAny := false
	err = block.FetchTagNames(ctx, req, func(string, traceql.AttributeScope) bool { seenAny = true; return false }, mcb, common.SearchOptions{})
	require.NoError(t, err)
	require.True(t, seenAny, "expected FetchTagNames to find at least one tag for svc-b")

	require.EqualValues(t, wantBytes, gotBytes,
		"issue #218 Phase 8: FetchTagNames' conditioned branch MetricsCallback must report exactly the sum of QueryStats.Steps[].BytesRead")
}

// TestFetchTagNames_NoConditions_ReportsDataFileBytesRead drives the no-conditions branch of
// (*blockpackBlock).FetchTagNames (reads the whole blockpack file upfront) and cross-checks the
// callback's value against the exact file size reported by StreamReader.
func TestFetchTagNames_NoConditions_ReportsDataFileBytesRead(t *testing.T) {
	block, _ := createFetchTestBlock(t)
	ctx := context.Background()

	rc, size, err := block.reader.StreamReader(ctx, DataFileName, uuid.UUID(block.meta.BlockID), block.meta.TenantID)
	require.NoError(t, err)
	require.NoError(t, rc.Close())
	require.Positive(t, size, "the block's data file must have a positive size")

	req := traceql.FetchTagsRequest{Scope: traceql.AttributeScopeSpan}

	var gotBytes uint64
	mcb := func(bytesRead uint64) { gotBytes += bytesRead }
	seenAny := false
	err = block.FetchTagNames(ctx, req, func(string, traceql.AttributeScope) bool { seenAny = true; return false }, mcb, common.SearchOptions{})
	require.NoError(t, err)
	require.True(t, seenAny, "expected FetchTagNames to find at least one tag with no conditions")

	require.EqualValues(t, size, gotBytes,
		"issue #218 Phase 8: FetchTagNames' no-conditions branch MetricsCallback must report exactly the blockpack file's real size")
}
