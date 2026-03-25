package executor_test

// NOTE: Any changes to this file must be reflected in the corresponding SPECS.md or NOTES.md.

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	commonv1 "go.opentelemetry.io/proto/otlp/common/v1"
	resourcev1 "go.opentelemetry.io/proto/otlp/resource/v1"
	tracev1 "go.opentelemetry.io/proto/otlp/trace/v1"

	modules_blockio "github.com/grafana/blockpack/internal/modules/blockio"
	modules_reader "github.com/grafana/blockpack/internal/modules/blockio/reader"
	"github.com/grafana/blockpack/internal/modules/executor"
	"github.com/grafana/blockpack/internal/modules/rw"
	"github.com/grafana/blockpack/internal/traceqlparser"
)

// writeStructuralTrace writes a known parent-child trace for structural query tests.
//
// Topology (all same trace ID):
//
//	root (service=svc-root, name=root-op)
//	├── child1 (service=svc-child, name=child-op)
//	│   └── grandchild (service=svc-leaf, name=leaf-op)
//	└── child2 (service=svc-child, name=sibling-op)
func writeStructuralTrace(t *testing.T, maxSpansPerBlock int) *modules_reader.Reader {
	t.Helper()

	traceID := [16]byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0A, 0x0B, 0x0C, 0x0D, 0x0E, 0x0F, 0x10}
	rootID := [8]byte{0xAA}
	child1ID := [8]byte{0xBB}
	grandchildID := [8]byte{0xCC}
	child2ID := [8]byte{0xDD}

	spans := []*tracev1.Span{
		{
			TraceId:           traceID[:],
			SpanId:            rootID[:],
			Name:              "root-op",
			StartTimeUnixNano: 1000,
			EndTimeUnixNano:   9000,
		},
		{
			TraceId:           traceID[:],
			SpanId:            child1ID[:],
			ParentSpanId:      rootID[:],
			Name:              "child-op",
			StartTimeUnixNano: 2000,
			EndTimeUnixNano:   8000,
		},
		{
			TraceId:           traceID[:],
			SpanId:            grandchildID[:],
			ParentSpanId:      child1ID[:],
			Name:              "leaf-op",
			StartTimeUnixNano: 3000,
			EndTimeUnixNano:   7000,
		},
		{
			TraceId:           traceID[:],
			SpanId:            child2ID[:],
			ParentSpanId:      rootID[:],
			Name:              "sibling-op",
			StartTimeUnixNano: 4000,
			EndTimeUnixNano:   6000,
		},
	}

	services := map[[8]byte]string{
		rootID:       "svc-root",
		child1ID:     "svc-child",
		grandchildID: "svc-leaf",
		child2ID:     "svc-child",
	}

	buf := &bytes.Buffer{}
	w, err := modules_blockio.NewWriter(buf, maxSpansPerBlock)
	require.NoError(t, err)

	for _, sp := range spans {
		var sid [8]byte
		copy(sid[:], sp.SpanId)
		svcName := services[sid]
		td := &tracev1.TracesData{
			ResourceSpans: []*tracev1.ResourceSpans{{
				Resource: &resourcev1.Resource{
					Attributes: []*commonv1.KeyValue{{
						Key:   "service.name",
						Value: &commonv1.AnyValue{Value: &commonv1.AnyValue_StringValue{StringValue: svcName}},
					}},
				},
				ScopeSpans: []*tracev1.ScopeSpans{{Spans: []*tracev1.Span{sp}}},
			}},
		}
		require.NoError(t, w.AddTracesData(td))
	}
	_, err = w.Flush()
	require.NoError(t, err)

	provider := &bufProvider{data: buf.Bytes()}
	r, err := modules_reader.NewReaderFromProvider(provider)
	require.NoError(t, err)
	return r
}

// spanIDHex returns the normalized hex string for a fixed span ID.
func spanIDHex(id [8]byte) string {
	return string([]byte{
		hexByte(id[0] >> 4), hexByte(id[0] & 0xF),
		hexByte(id[1] >> 4), hexByte(id[1] & 0xF),
		hexByte(id[2] >> 4), hexByte(id[2] & 0xF),
		hexByte(id[3] >> 4), hexByte(id[3] & 0xF),
		hexByte(id[4] >> 4), hexByte(id[4] & 0xF),
		hexByte(id[5] >> 4), hexByte(id[5] & 0xF),
		hexByte(id[6] >> 4), hexByte(id[6] & 0xF),
		hexByte(id[7] >> 4), hexByte(id[7] & 0xF),
	})
}

func hexByte(b byte) byte {
	if b < 10 {
		return '0' + b
	}
	return 'a' + b - 10
}

// matchedSpanIDs returns the set of hex span IDs from a StructuralResult.
func matchedSpanIDs(result *executor.StructuralResult) map[string]bool {
	out := make(map[string]bool, len(result.Matches))
	for _, m := range result.Matches {
		if len(m.SpanID) == 8 {
			var id [8]byte
			copy(id[:], m.SpanID)
			out[spanIDHex(id)] = true
		}
	}
	return out
}

// bufProvider is a simple in-memory rw.ReaderProvider for tests.
type bufProvider struct{ data []byte }

func (p *bufProvider) Size() (int64, error) { return int64(len(p.data)), nil }
func (p *bufProvider) ReadAt(buf []byte, off int64, _ rw.DataType) (int, error) {
	if off < 0 || int(off)+len(buf) > len(p.data) {
		return 0, bytes.ErrTooLarge
	}
	return copy(buf, p.data[off:]), nil
}

// EX-ST-01: nil reader returns empty result with no error.
func TestExecuteStructural_NilReader(t *testing.T) {
	q, err := parseStructural(`{ name = "x" } >> { name = "y" }`)
	require.NoError(t, err)

	result, err := executor.ExecuteStructural(nil, q, executor.Options{})
	require.NoError(t, err)
	assert.Empty(t, result.Matches)
}

// EX-ST-02: descendant (>>) returns grandchild as descendant of root.
func TestExecuteStructural_Descendant(t *testing.T) {
	r := writeStructuralTrace(t, 0)
	q, err := parseStructural(`{ resource.service.name = "svc-root" } >> { resource.service.name = "svc-leaf" }`)
	require.NoError(t, err)

	result, err := executor.ExecuteStructural(r, q, executor.Options{})
	require.NoError(t, err)

	got := matchedSpanIDs(result)
	grandchildID := [8]byte{0xCC}
	assert.True(t, got[spanIDHex(grandchildID)], "grandchild must be a descendant of root")
	assert.Len(t, got, 1)
}

// EX-ST-03: child (>) returns only direct children, not grandchildren.
func TestExecuteStructural_Child(t *testing.T) {
	r := writeStructuralTrace(t, 0)
	q, err := parseStructural(`{ resource.service.name = "svc-root" } > { resource.service.name = "svc-child" }`)
	require.NoError(t, err)

	result, err := executor.ExecuteStructural(r, q, executor.Options{})
	require.NoError(t, err)

	got := matchedSpanIDs(result)
	child1ID := [8]byte{0xBB}
	child2ID := [8]byte{0xDD}
	grandchildID := [8]byte{0xCC}
	assert.True(t, got[spanIDHex(child1ID)], "child1 must match")
	assert.True(t, got[spanIDHex(child2ID)], "child2 must match")
	assert.False(t, got[spanIDHex(grandchildID)], "grandchild must not match (not direct child of root)")
	assert.Len(t, got, 2)
}

// EX-ST-04: sibling (~) returns child2 as sibling of child1 (same parent).
func TestExecuteStructural_Sibling(t *testing.T) {
	r := writeStructuralTrace(t, 0)
	// child-op and sibling-op share parent root — they are siblings.
	q, err := parseStructural(`{ name = "child-op" } ~ { name = "sibling-op" }`)
	require.NoError(t, err)

	result, err := executor.ExecuteStructural(r, q, executor.Options{})
	require.NoError(t, err)

	got := matchedSpanIDs(result)
	child2ID := [8]byte{0xDD}
	child1ID := [8]byte{0xBB}
	assert.True(t, got[spanIDHex(child2ID)], "sibling-op must match")
	assert.False(t, got[spanIDHex(child1ID)], "child-op must not match itself as a sibling")
	assert.Len(t, got, 1)
}

// EX-ST-05: ancestor (<<) returns root and child1 as ancestors of grandchild.
func TestExecuteStructural_Ancestor(t *testing.T) {
	r := writeStructuralTrace(t, 0)
	q, err := parseStructural(`{ resource.service.name = "svc-leaf" } << { resource.service.name != "svc-leaf" }`)
	require.NoError(t, err)

	result, err := executor.ExecuteStructural(r, q, executor.Options{})
	require.NoError(t, err)

	got := matchedSpanIDs(result)
	rootID := [8]byte{0xAA}
	child1ID := [8]byte{0xBB}
	assert.True(t, got[spanIDHex(rootID)], "root must be an ancestor of grandchild")
	assert.True(t, got[spanIDHex(child1ID)], "child1 must be an ancestor of grandchild")
	assert.Len(t, got, 2)
}

// EX-ST-06: parent (<) returns only the direct parent of grandchild.
func TestExecuteStructural_Parent(t *testing.T) {
	r := writeStructuralTrace(t, 0)
	q, err := parseStructural(`{ resource.service.name = "svc-leaf" } < { resource.service.name != "svc-leaf" }`)
	require.NoError(t, err)

	result, err := executor.ExecuteStructural(r, q, executor.Options{})
	require.NoError(t, err)

	got := matchedSpanIDs(result)
	child1ID := [8]byte{0xBB}
	rootID := [8]byte{0xAA}
	assert.True(t, got[spanIDHex(child1ID)], "child1 must be the parent of grandchild")
	assert.False(t, got[spanIDHex(rootID)], "root is grandparent, not parent")
	assert.Len(t, got, 1)
}

// EX-ST-07: not-sibling (!~) returns right-side spans with no left sibling.
func TestExecuteStructural_NotSibling(t *testing.T) {
	r := writeStructuralTrace(t, 0)
	// grandchild has no sibling matching child-op (grandchild's parent is child1, and child1 has no other child matching child-op)
	q, err := parseStructural(`{ name = "child-op" } !~ { name = "leaf-op" }`)
	require.NoError(t, err)

	result, err := executor.ExecuteStructural(r, q, executor.Options{})
	require.NoError(t, err)

	// leaf-op's parent is child1 (child-op). child-op is a sibling of itself which is
	// excluded (no leftMatch sibling at the same parent level for leaf-op).
	got := matchedSpanIDs(result)
	grandchildID := [8]byte{0xCC}
	assert.True(t, got[spanIDHex(grandchildID)], "leaf-op has no left-sibling matching child-op at its parent level")
}

// EX-ST-08: Limit caps returned matches.
func TestExecuteStructural_Limit(t *testing.T) {
	r := writeStructuralTrace(t, 0)
	// All spans are descendants of root (3 descendants: child1, grandchild, child2)
	q, err := parseStructural(`{ name = "root-op" } >> {}`)
	require.NoError(t, err)

	result, err := executor.ExecuteStructural(r, q, executor.Options{Limit: 1})
	require.NoError(t, err)
	assert.Len(t, result.Matches, 1)
}

// EX-ST-09: multiblock — spans across multiple blocks are correctly resolved.
func TestExecuteStructural_MultiBlock(t *testing.T) {
	// maxSpansPerBlock=1 forces each span into its own block.
	r := writeStructuralTrace(t, 1)
	q, err := parseStructural(`{ resource.service.name = "svc-root" } >> { resource.service.name = "svc-leaf" }`)
	require.NoError(t, err)

	result, err := executor.ExecuteStructural(r, q, executor.Options{})
	require.NoError(t, err)

	got := matchedSpanIDs(result)
	grandchildID := [8]byte{0xCC}
	assert.True(t, got[spanIDHex(grandchildID)], "grandchild must still be found across block boundaries")
	assert.Len(t, got, 1)
}

// EX-ST-10: SPEC-STRUCT-6 — !>> returns only rightMatch spans with no leftMatch ancestor.
func TestExecuteStructural_NotDescendant(t *testing.T) {
	t.Parallel()
	r := writeStructuralTrace(t, 0)

	q, err := parseStructural(`{ name = "root-op" } !>> {}`)
	require.NoError(t, err)
	require.NotNil(t, q, "parser must produce a StructuralQuery for !>>")

	result, err := executor.ExecuteStructural(r, q, executor.Options{})
	require.NoError(t, err)

	got := matchedSpanIDs(result)
	rootID := [8]byte{0xAA}
	child1ID := [8]byte{0xBB}
	grandchildID := [8]byte{0xCC}
	child2ID := [8]byte{0xDD}

	assert.True(t, got[spanIDHex(rootID)], "root has no ancestors — must pass !>>")
	assert.False(t, got[spanIDHex(child1ID)], "child1 is a descendant of root — must be excluded by !>>")
	assert.False(t, got[spanIDHex(child2ID)], "child2 is a descendant of root — must be excluded by !>>")
	assert.False(t, got[spanIDHex(grandchildID)], "grandchild is a descendant of root — must be excluded by !>>")
	assert.Len(t, got, 1, "exactly 1 span must match: root only")
}

// EX-ST-11: SPEC-STRUCT-7 — !> returns rightMatch spans whose direct parent is NOT leftMatch.
func TestExecuteStructural_NotChild(t *testing.T) {
	t.Parallel()
	r := writeStructuralTrace(t, 0)

	q, err := parseStructural(`{ name = "root-op" } !> {}`)
	require.NoError(t, err)
	require.NotNil(t, q, "parser must produce a StructuralQuery for !>")

	result, err := executor.ExecuteStructural(r, q, executor.Options{})
	require.NoError(t, err)

	got := matchedSpanIDs(result)
	rootID := [8]byte{0xAA}
	child1ID := [8]byte{0xBB}
	grandchildID := [8]byte{0xCC}
	child2ID := [8]byte{0xDD}

	assert.True(t, got[spanIDHex(rootID)], "root has no parent — must pass !>")
	assert.True(t, got[spanIDHex(grandchildID)], "grandchild's parent is child1, not root — must pass !>")
	assert.False(t, got[spanIDHex(child1ID)], "child1's parent is root (leftMatch) — must be excluded by !>")
	assert.False(t, got[spanIDHex(child2ID)], "child2's parent is root (leftMatch) — must be excluded by !>")
	assert.Len(t, got, 2, "exactly 2 spans must match: root and grandchild")
}

func parseStructural(query string) (*traceqlparser.StructuralQuery, error) {
	parsed, err := traceqlparser.ParseTraceQL(query)
	if err != nil {
		return nil, err
	}
	sq, ok := parsed.(*traceqlparser.StructuralQuery)
	if !ok {
		return nil, nil
	}
	return sq, nil
}
