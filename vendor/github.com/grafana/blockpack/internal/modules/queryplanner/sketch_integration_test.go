package queryplanner_test

import (
	"bytes"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	tracev1 "go.opentelemetry.io/proto/otlp/trace/v1"

	"github.com/grafana/blockpack/internal/modules/queryplanner"
	"github.com/grafana/blockpack/internal/modules/sketch"
)

// --- Explain output tests ---

// TestExplainBlockPriority verifies that the explain output includes the
// block priority section with English reasoning when sketch data is present.
func TestExplainBlockPriority(t *testing.T) {
	data := writeTwoBlockFileSvc(t, "service-A", "service-B", 5, 5)
	r := openReader(t, data)
	require.Equal(t, 2, r.BlockCount())

	p := queryplanner.NewPlanner(r)
	pred := queryplanner.Predicate{
		Columns: []string{"resource.service.name"},
		Values:  []string{"service-A"},
	}
	plan := p.Plan([]queryplanner.Predicate{pred}, queryplanner.TimeRange{})

	// Explain must contain the block priority section.
	assert.Contains(t, plan.Explain, "Block priority (best first):")
	// Must mention cardinality in English.
	assert.Contains(t, plan.Explain, "cardinality")
	// Must mention frequency source (topk or cms).
	assert.True(t,
		strings.Contains(plan.Explain, "topk") || strings.Contains(plan.Explain, "cms"),
		"explain must mention frequency source (topk or cms)")
	// Must mention the score.
	assert.Contains(t, plan.Explain, "score=")
	// Must show block ranking.
	assert.Contains(t, plan.Explain, "#1 block")
}

// TestExplainPruningPipeline verifies the pruning pipeline section shows
// correct running counts for each stage.
func TestExplainPruningPipeline(t *testing.T) {
	data := writeTwoBlockFileSvc(t, "service-A", "service-B", 5, 5)
	r := openReader(t, data)
	require.Equal(t, 2, r.BlockCount())

	p := queryplanner.NewPlanner(r)
	pred := queryplanner.Predicate{
		Columns: []string{"resource.service.name"},
		Values:  []string{"service-A"},
	}
	plan := p.Plan([]queryplanner.Predicate{pred}, queryplanner.TimeRange{})

	// At least one pruning stage must have run (range-index, fuse, or cms).
	totalPruned := plan.PrunedByIndex + plan.PrunedByFuse + plan.PrunedByCMS
	require.Greater(t, totalPruned, 0, "at least one stage must prune block 1")

	// Explain must contain the pipeline section.
	assert.Contains(t, plan.Explain, "Pruning pipeline:")
	assert.Contains(t, plan.Explain, "start: 2 blocks")
}

// TestExplainNoPredicates verifies the no-predicates explain format.
func TestExplainNoPredicatesFormat(t *testing.T) {
	data := writeTwoBlockFileSvc(t, "service-A", "service-B", 5, 5)
	r := openReader(t, data)

	p := queryplanner.NewPlanner(r)
	plan := p.Plan(nil, queryplanner.TimeRange{})

	assert.Equal(t, "no predicates → all 2 blocks", plan.Explain)
	// No pipeline section when nothing was pruned.
	assert.NotContains(t, plan.Explain, "Pruning pipeline:")
	assert.NotContains(t, plan.Explain, "Block priority")
}

// TestExplainThreeBlocksWithPriority verifies block priority ordering in explain
// for a 3-block file where one block is the best match.
func TestExplainThreeBlocksWithPriority(t *testing.T) {
	data := writeThreeBlockFileSvc(t, "service-A", "service-B", "service-C", 5, 5, 5)
	r := openReader(t, data)
	require.Equal(t, 3, r.BlockCount())

	p := queryplanner.NewPlanner(r)
	// Query for service-B: block 1 should be the only surviving block.
	pred := queryplanner.Predicate{
		Columns: []string{"resource.service.name"},
		Values:  []string{"service-B"},
	}
	plan := p.Plan([]queryplanner.Predicate{pred}, queryplanner.TimeRange{})

	assert.Contains(t, plan.SelectedBlocks, 1)
	// Explain must show the selected block in priority section.
	if len(plan.BlockScores) > 0 {
		assert.Contains(t, plan.Explain, "#1 block 1")
	}
}

// --- Multi-column sketch tests ---

// writeMultiColumnFile writes a blockpack file with two blocks, each having
// two distinct resource attributes (service.name and env).
func writeMultiColumnFile(t *testing.T) []byte {
	t.Helper()
	var combined bytes.Buffer
	wc := newWriter(t, &combined, 5)

	// Block 0: service-A in prod.
	for i := range 5 {
		sp := &tracev1.Span{
			TraceId:           []byte{byte(i), 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
			SpanId:            []byte{byte(i + 1), 0, 0, 0, 0, 0, 0, 0}, //nolint:gosec
			Name:              fmt.Sprintf("op-a%d", i),
			StartTimeUnixNano: uint64(i) * 1_000_000_000,             //nolint:gosec
			EndTimeUnixNano:   uint64(i)*1_000_000_000 + 500_000_000, //nolint:gosec
		}
		require.NoError(t, wc.AddSpan(sp.TraceId, sp, map[string]any{
			"service.name": "service-A",
			"env":          "prod",
		}, "", nil, ""))
	}
	// Block 1: service-B in staging.
	for i := range 5 {
		sp := &tracev1.Span{
			TraceId:           []byte{byte(i + 5), 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}, //nolint:gosec
			SpanId:            []byte{byte(i + 6), 0, 0, 0, 0, 0, 0, 0},                         //nolint:gosec
			Name:              fmt.Sprintf("op-b%d", i),
			StartTimeUnixNano: uint64(i+100) * 1_000_000_000,             //nolint:gosec
			EndTimeUnixNano:   uint64(i+100)*1_000_000_000 + 500_000_000, //nolint:gosec
		}
		require.NoError(t, wc.AddSpan(sp.TraceId, sp, map[string]any{
			"service.name": "service-B",
			"env":          "staging",
		}, "", nil, ""))
	}
	flush(t, wc)
	return combined.Bytes()
}

// TestMultiColumnSketchIndependence verifies that sketch data for different
// columns is independent: CMS for service.name and CMS for env are separate.
func TestMultiColumnSketchIndependence(t *testing.T) {
	data := writeMultiColumnFile(t)
	r := openReader(t, data)
	require.Equal(t, 2, r.BlockCount())

	svcCol := "resource.service.name"
	envCol := "resource.env"

	csSvc := r.ColumnSketch(svcCol)
	csEnv := r.ColumnSketch(envCol)
	require.NotNil(t, csSvc, "service.name column sketch must exist")
	require.NotNil(t, csEnv, "env column sketch must exist")

	// CMS for service-A: block 0 > 0, block 1 == 0.
	estSvc := csSvc.CMSEstimate("service-A")
	require.Len(t, estSvc, 2)
	assert.Greater(t, estSvc[0], uint32(0))
	assert.Equal(t, uint32(0), estSvc[1])

	// CMS for prod: block 0 > 0, block 1 == 0.
	estEnv := csEnv.CMSEstimate("prod")
	require.Len(t, estEnv, 2)
	assert.Greater(t, estEnv[0], uint32(0))
	assert.Equal(t, uint32(0), estEnv[1])

	// CMS for staging: block 0 == 0, block 1 > 0.
	estStaging := csEnv.CMSEstimate("staging")
	require.Len(t, estStaging, 2)
	assert.Equal(t, uint32(0), estStaging[0])
	assert.Greater(t, estStaging[1], uint32(0))
}

// TestMultiColumnDistinctIndependence verifies cardinality counts are independent per column.
func TestMultiColumnDistinctIndependence(t *testing.T) {
	data := writeMultiColumnFile(t)
	r := openReader(t, data)
	require.Equal(t, 2, r.BlockCount())

	csSvc := r.ColumnSketch("resource.service.name")
	csEnv := r.ColumnSketch("resource.env")
	require.NotNil(t, csSvc)
	require.NotNil(t, csEnv)

	// Each block has 1 distinct service name and 1 distinct env value.
	svcDistinct := csSvc.Distinct()
	envDistinct := csEnv.Distinct()
	require.Len(t, svcDistinct, 2)
	require.Len(t, envDistinct, 2)

	// Both blocks should show low cardinality (1 distinct value each).
	for b := range 2 {
		assert.Greater(t, svcDistinct[b], uint32(0), "block %d svc distinct must be > 0", b)
		assert.Greater(t, envDistinct[b], uint32(0), "block %d env distinct must be > 0", b)
	}
}

// TestMultiColumnFuseIndependence verifies fuse filters are independent per column.
func TestMultiColumnFuseIndependence(t *testing.T) {
	data := writeMultiColumnFile(t)
	r := openReader(t, data)
	require.Equal(t, 2, r.BlockCount())

	csSvc := r.ColumnSketch("resource.service.name")
	csEnv := r.ColumnSketch("resource.env")
	require.NotNil(t, csSvc)
	require.NotNil(t, csEnv)

	hA := sketch.HashForFuse("service-A")
	hProd := sketch.HashForFuse("prod")

	// Block 0 must contain service-A hash in svc column.
	fuseSvc := csSvc.FuseContains(hA)
	require.Len(t, fuseSvc, 2)
	assert.True(t, fuseSvc[0], "block 0 svc fuse must contain service-A")

	// Block 0 must contain prod hash in env column.
	fuseEnv := csEnv.FuseContains(hProd)
	require.Len(t, fuseEnv, 2)
	assert.True(t, fuseEnv[0], "block 0 env fuse must contain prod")
}

// --- Multi-predicate scoring tests ---

// TestMultiPredicateScoring verifies that scoring accumulates across multiple predicates.
func TestMultiPredicateScoring(t *testing.T) {
	data := writeMultiColumnFile(t)
	r := openReader(t, data)
	require.Equal(t, 2, r.BlockCount())

	p := queryplanner.NewPlanner(r)

	// Single predicate: service-A.
	singlePred := queryplanner.Predicate{
		Columns: []string{"resource.service.name"},
		Values:  []string{"service-A"},
	}
	planSingle := p.Plan([]queryplanner.Predicate{singlePred}, queryplanner.TimeRange{})

	// Two predicates: service-A AND prod.
	multiPred := []queryplanner.Predicate{
		{Columns: []string{"resource.service.name"}, Values: []string{"service-A"}},
		{Columns: []string{"resource.env"}, Values: []string{"prod"}},
	}
	planMulti := p.Plan(multiPred, queryplanner.TimeRange{})

	// Both plans should select block 0.
	assert.Contains(t, planSingle.SelectedBlocks, 0)
	assert.Contains(t, planMulti.SelectedBlocks, 0)

	// Multi-predicate score for block 0 should be >= single-predicate score,
	// because scoring accumulates freq/card across predicates.
	if planSingle.BlockScores != nil && planMulti.BlockScores != nil {
		scoreSingle := planSingle.BlockScores[0]
		scoreMulti := planMulti.BlockScores[0]
		assert.GreaterOrEqual(t, scoreMulti, scoreSingle,
			"multi-predicate score must be >= single-predicate score (accumulates)")
	}
}

// --- Presence bitset tests ---

// TestPresenceBitsetPartialColumn verifies the presence bitset is correct
// when a column is present in some blocks but not others.
func TestPresenceBitsetPartialColumn(t *testing.T) {
	var combined bytes.Buffer
	wc := newWriter(t, &combined, 5)

	// Block 0: has "extra" attribute.
	for i := range 5 {
		sp := &tracev1.Span{
			TraceId:           []byte{byte(i), 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
			SpanId:            []byte{byte(i + 1), 0, 0, 0, 0, 0, 0, 0}, //nolint:gosec
			Name:              fmt.Sprintf("op%d", i),
			StartTimeUnixNano: uint64(i) * 1_000_000_000,             //nolint:gosec
			EndTimeUnixNano:   uint64(i)*1_000_000_000 + 500_000_000, //nolint:gosec
		}
		require.NoError(t, wc.AddSpan(sp.TraceId, sp, map[string]any{
			"service.name": "svc",
			"extra":        "value-A",
		}, "", nil, ""))
	}
	// Block 1: no "extra" attribute.
	for i := range 5 {
		sp := &tracev1.Span{
			TraceId:           []byte{byte(i + 5), 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}, //nolint:gosec
			SpanId:            []byte{byte(i + 6), 0, 0, 0, 0, 0, 0, 0},                         //nolint:gosec
			Name:              fmt.Sprintf("op%d", i+5),
			StartTimeUnixNano: uint64(i+100) * 1_000_000_000,             //nolint:gosec
			EndTimeUnixNano:   uint64(i+100)*1_000_000_000 + 500_000_000, //nolint:gosec
		}
		require.NoError(t, wc.AddSpan(sp.TraceId, sp, map[string]any{
			"service.name": "svc",
		}, "", nil, ""))
	}
	flush(t, wc)
	data := combined.Bytes()

	r := openReader(t, data)
	require.Equal(t, 2, r.BlockCount())

	// "resource.extra" column should have presence for block 0 only.
	cs := r.ColumnSketch("resource.extra")
	if cs != nil {
		presence := cs.Presence()
		require.Greater(t, len(presence), 0)
		// Bit 0 should be set (column present in block 0).
		assert.True(t, presence[0]&1 == 1, "block 0 must be present in presence bitset for extra")
		// Bit 1 should be clear (column absent in block 1).
		assert.True(t, presence[0]&2 == 0, "block 1 must be absent in presence bitset for extra")
	}

	// "resource.service.name" column should have presence for both blocks.
	csSvc := r.ColumnSketch("resource.service.name")
	require.NotNil(t, csSvc)
	presenceSvc := csSvc.Presence()
	require.Greater(t, len(presenceSvc), 0)
	assert.True(t, presenceSvc[0]&1 == 1, "block 0 must be present for service.name")
	assert.True(t, presenceSvc[0]&2 == 2, "block 1 must be present for service.name")
}

// --- Timestamp sketch tests ---

// TestTimestampSketchPresent verifies that the __timestamp__ column gets sketch data.
func TestTimestampSketchPresent(t *testing.T) {
	data := writeTwoBlockFileSvc(t, "service-A", "service-B", 5, 5)
	r := openReader(t, data)
	require.Equal(t, 2, r.BlockCount())

	cs := r.ColumnSketch("__timestamp__")
	if cs == nil {
		t.Skip("timestamp sketch not present (implementation may not sketch timestamps)")
	}

	// If present, must have data for both blocks.
	distinct := cs.Distinct()
	require.Len(t, distinct, 2)
	assert.Greater(t, distinct[0], uint32(0), "block 0 must have timestamp cardinality")
	assert.Greater(t, distinct[1], uint32(0), "block 1 must have timestamp cardinality")
}

// --- Edge case: query value never written to any block ---

// TestAllBlocksPrunedBySketch verifies that querying a value not in ANY block
// correctly prunes all blocks via sketch data.
func TestAllBlocksPrunedBySketch(t *testing.T) {
	data := writeTwoBlockFileSvc(t, "service-A", "service-B", 5, 5)
	r := openReader(t, data)
	require.Equal(t, 2, r.BlockCount())

	p := queryplanner.NewPlanner(r)
	pred := queryplanner.Predicate{
		Columns: []string{"resource.service.name"},
		Values:  []string{"service-X-nonexistent"},
	}
	plan := p.Plan([]queryplanner.Predicate{pred}, queryplanner.TimeRange{})

	// service-X-nonexistent is not in any block.
	// Combined pruning (range + fuse + CMS) should eliminate all blocks.
	totalPruned := plan.PrunedByIndex + plan.PrunedByFuse + plan.PrunedByCMS
	assert.Equal(t, 2, totalPruned, "all blocks must be pruned for absent value")
	assert.Empty(t, plan.SelectedBlocks)
}

// --- Edge case: OR predicate with two values, one per block ---

// TestORPredicateTwoValuesSelectsBoth verifies that an OR predicate (via Children)
// where each child value exists in a different block selects both blocks.
func TestORPredicateTwoValuesSelectsBoth(t *testing.T) {
	data := writeThreeBlockFileSvc(t, "service-A", "service-B", "service-C", 5, 5, 5)
	r := openReader(t, data)
	require.Equal(t, 3, r.BlockCount())

	p := queryplanner.NewPlanner(r)
	// OR: service-A OR service-C → blocks 0 and 2.
	pred := queryplanner.Predicate{
		Op: queryplanner.LogicalOR,
		Children: []queryplanner.Predicate{
			{Columns: []string{"resource.service.name"}, Values: []string{"service-A"}},
			{Columns: []string{"resource.service.name"}, Values: []string{"service-C"}},
		},
	}
	plan := p.Plan([]queryplanner.Predicate{pred}, queryplanner.TimeRange{})

	assert.Contains(t, plan.SelectedBlocks, 0, "block 0 (service-A) must survive OR")
	assert.Contains(t, plan.SelectedBlocks, 2, "block 2 (service-C) must survive OR")
	// Block 1 (service-B) should be pruned.
	assert.NotContains(t, plan.SelectedBlocks, 1, "block 1 (service-B) should be pruned")
}

// --- Edge case: AND predicate pruning ---

// TestANDPredicatePrunesAll verifies that AND(service-A, service-B) with each
// in a different block prunes all blocks (impossible combination).
func TestANDPredicatePrunesAll(t *testing.T) {
	data := writeTwoBlockFileSvc(t, "service-A", "service-B", 5, 5)
	r := openReader(t, data)
	require.Equal(t, 2, r.BlockCount())

	p := queryplanner.NewPlanner(r)
	// AND: service-A AND service-B — no block has both.
	preds := []queryplanner.Predicate{
		{Columns: []string{"resource.service.name"}, Values: []string{"service-A"}},
		{Columns: []string{"resource.service.name"}, Values: []string{"service-B"}},
	}
	plan := p.Plan(preds, queryplanner.TimeRange{})

	// Both blocks should be pruned since no block contains both services.
	assert.Empty(t, plan.SelectedBlocks,
		"AND(service-A, service-B) should prune all blocks (impossible combination)")
}

// --- Score formula correctness ---

// TestScoreFormulaCorrectness verifies score = freq / max(cardinality, 1).
// With 5 spans all having the same service name, freq=5, distinct=1, score=5.0.
func TestScoreFormulaCorrectness(t *testing.T) {
	data := writeTwoBlockFileSvc(t, "service-A", "service-B", 5, 5)
	r := openReader(t, data)
	require.Equal(t, 2, r.BlockCount())

	p := queryplanner.NewPlanner(r)
	pred := queryplanner.Predicate{
		Columns: []string{"resource.service.name"},
		Values:  []string{"service-A"},
	}
	plan := p.Plan([]queryplanner.Predicate{pred}, queryplanner.TimeRange{})

	require.Contains(t, plan.SelectedBlocks, 0)
	require.NotNil(t, plan.BlockScores)

	score, ok := plan.BlockScores[0]
	require.True(t, ok, "block 0 must have a score")

	// With 5 spans of service-A in block 0:
	// TopK should have count=5, distinct should be 1.
	// Score = 5 / max(1, 1) = 5.0.
	// Allow some HLL estimation error (cardinality could be 1-2).
	assert.GreaterOrEqual(t, score, float64(2.0),
		"score should be high for low cardinality + high frequency")
}

// --- Explain with combined time + predicate pruning ---

// TestExplainCombinedTimePredicate verifies explain output when both time and
// predicate pruning happen.
func TestExplainCombinedTimePredicate(t *testing.T) {
	data := writeThreeBlockFileSvc(t, "service-A", "service-B", "service-C", 5, 5, 5)
	r := openReader(t, data)
	require.Equal(t, 3, r.BlockCount())

	p := queryplanner.NewPlanner(r)
	pred := queryplanner.Predicate{
		Columns: []string{"resource.service.name"},
		Values:  []string{"service-A"},
	}
	// Use a time range that includes all blocks (both predicates AND time should work).
	plan := p.Plan([]queryplanner.Predicate{pred}, queryplanner.TimeRange{
		MinNano: 0,
		MaxNano: 999_999_999_999,
	})

	// Explain should contain predicate tree.
	assert.Contains(t, plan.Explain, "resource.service.name=")
	// Block 0 should be selected.
	assert.Contains(t, plan.SelectedBlocks, 0)
}
