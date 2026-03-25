package reader_test

import (
	"bytes"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	tracev1 "go.opentelemetry.io/proto/otlp/trace/v1"

	modules_reader "github.com/grafana/blockpack/internal/modules/blockio/reader"
	"github.com/grafana/blockpack/internal/modules/blockio/writer"
	"github.com/grafana/blockpack/internal/modules/sketch"
)

// buildSketchReader creates a Reader from spans added via the callback.
// maxBlockSpans controls how many spans fit in one block (0 = default).
func buildSketchReader(t *testing.T, maxBlockSpans int, fn func(w *writer.Writer)) *modules_reader.Reader {
	t.Helper()
	var buf bytes.Buffer
	w := mustNewWriter(t, &buf, maxBlockSpans)
	fn(w)
	flushToBuffer(t, &buf, w)
	return openReader(t, buf.Bytes())
}

// addServiceSpan adds a span with the given service name and operation name.
func addServiceSpan(t *testing.T, w *writer.Writer, svc, op string, seed byte) {
	t.Helper()
	var traceID [16]byte
	traceID[0] = seed
	span := makeSpan(traceID, fixedSpanID(seed), op, 1000, 2000,
		tracev1.Span_SPAN_KIND_SERVER, nil)
	addSpanToWriter(t, w, traceID, span, map[string]any{"service.name": svc})
}

// TestFileSketchSummary_NilReader verifies nil is returned for files without a sketch section.
func TestFileSketchSummary_NilReader(t *testing.T) {
	r := buildSketchReader(t, 0, func(w *writer.Writer) {
		addServiceSpan(t, w, "svc-a", "op1", 1)
	})
	// Old-format files or files without sketches return nil.
	// Files written by the current writer DO have sketches, so we just verify no panic.
	_ = r.FileSketchSummary()
}

// TestFileSketchSummary_ColumnPresent verifies that a column with data has a non-nil entry.
func TestFileSketchSummary_ColumnPresent(t *testing.T) {
	r := buildSketchReader(t, 0, func(w *writer.Writer) {
		addServiceSpan(t, w, "svc-a", "op1", 1)
		addServiceSpan(t, w, "svc-b", "op2", 2)
		addServiceSpan(t, w, "svc-a", "op1", 3)
	})
	summary := r.FileSketchSummary()
	require.NotNil(t, summary)
	require.NotEmpty(t, summary.Columns)

	col := summary.Columns["resource.service.name"]
	require.NotNil(t, col, "resource.service.name must be present")
	assert.NotNil(t, col.CMS, "CMS must not be nil")
	assert.Greater(t, col.TotalDistinct, uint32(0), "TotalDistinct must be positive")
}

// TestFileSketchSummary_CMSAbsence verifies CMS estimate==0 for a value never added.
func TestFileSketchSummary_CMSAbsence(t *testing.T) {
	r := buildSketchReader(t, 0, func(w *writer.Writer) {
		addServiceSpan(t, w, "svc-a", "op1", 1)
		addServiceSpan(t, w, "svc-b", "op2", 2)
	})
	summary := r.FileSketchSummary()
	require.NotNil(t, summary)

	col := summary.Columns["resource.service.name"]
	require.NotNil(t, col)

	// "definitely-absent" was never added — CMS must return 0.
	assert.Equal(t, uint16(0), col.CMS.Estimate("definitely-absent"),
		"CMS must return 0 for value never added (no false negatives)")
}

// TestFileSketchSummary_CMSPresence verifies CMS estimate>0 for values that were added.
func TestFileSketchSummary_CMSPresence(t *testing.T) {
	r := buildSketchReader(t, 0, func(w *writer.Writer) {
		for i := range 10 {
			addServiceSpan(t, w, "svc-a", "op1", byte(i+1))
		}
	})
	summary := r.FileSketchSummary()
	require.NotNil(t, summary)

	col := summary.Columns["resource.service.name"]
	require.NotNil(t, col)

	assert.Greater(t, col.CMS.Estimate("svc-a"), uint16(0),
		"CMS must return >0 for value that was added")
}

// TestFileSketchSummary_MarshalRoundTrip verifies Marshal→Unmarshal preserves data.
func TestFileSketchSummary_MarshalRoundTrip(t *testing.T) {
	r := buildSketchReader(t, 0, func(w *writer.Writer) {
		addServiceSpan(t, w, "svc-a", "op1", 1)
		addServiceSpan(t, w, "svc-b", "op2", 2)
		addServiceSpan(t, w, "svc-a", "op1", 3)
	})
	original := r.FileSketchSummary()
	require.NotNil(t, original)

	b, err := modules_reader.MarshalFileSketchSummary(original)
	require.NoError(t, err)
	require.NotEmpty(t, b)

	restored, err := modules_reader.UnmarshalFileSketchSummary(b)
	require.NoError(t, err)
	require.NotNil(t, restored)

	// Column set must match.
	assert.Equal(t, len(original.Columns), len(restored.Columns))

	for name, origCol := range original.Columns {
		restCol, ok := restored.Columns[name]
		require.True(t, ok, "column %q missing after round-trip", name)

		assert.Equal(t, origCol.TotalDistinct, restCol.TotalDistinct, "TotalDistinct mismatch for %q", name)
		assert.Equal(t, len(origCol.TopK), len(restCol.TopK), "TopK length mismatch for %q", name)

		// CMS estimates for known values must match.
		assert.Equal(t, origCol.CMS.Estimate("svc-a"), restCol.CMS.Estimate("svc-a"),
			"CMS estimate mismatch for svc-a in %q", name)
		assert.Equal(t, origCol.CMS.Estimate("definitely-absent"), restCol.CMS.Estimate("definitely-absent"),
			"CMS absence mismatch for %q", name)
	}
}

// TestFileSketchSummary_MarshalNil verifies MarshalFileSketchSummary(nil) returns nil,nil.
func TestFileSketchSummary_MarshalNil(t *testing.T) {
	b, err := modules_reader.MarshalFileSketchSummary(nil)
	assert.NoError(t, err)
	assert.Nil(t, b)
}

// TestFileSketchSummary_UnmarshalEmpty verifies UnmarshalFileSketchSummary(nil) returns nil,nil.
func TestFileSketchSummary_UnmarshalEmpty(t *testing.T) {
	s, err := modules_reader.UnmarshalFileSketchSummary(nil)
	assert.NoError(t, err)
	assert.Nil(t, s)
}

// TestFileSummary_ConcurrentInit verifies that FileSketchSummary() is safe to call
// from many goroutines simultaneously. The sync.Once inside must prevent data races
// and all goroutines must receive the same non-nil pointer.
// GAP-33: concurrent FileSketchSummary initialization.
func TestFileSummary_ConcurrentInit(t *testing.T) {
	r := buildSketchReader(t, 0, func(w *writer.Writer) {
		for i := range 5 {
			addServiceSpan(t, w, "svc-a", "op1", byte(i+1))
		}
	})

	const N = 50
	results := make([]*modules_reader.FileSketchSummary, N)
	var wg sync.WaitGroup
	wg.Add(N)
	for i := range N {
		go func(idx int) {
			defer wg.Done()
			results[idx] = r.FileSketchSummary()
		}(i)
	}
	wg.Wait()

	// All results must be the same pointer (sync.Once guarantees single computation).
	first := results[0]
	require.NotNil(t, first, "FileSketchSummary must not return nil for a file with data")
	for i := 1; i < N; i++ {
		assert.Same(t, first, results[i],
			"goroutine %d returned different pointer than goroutine 0", i)
	}
}

// TestFileSketchSummary_Idempotent verifies FileSketchSummary() returns the same pointer.
func TestFileSketchSummary_Idempotent(t *testing.T) {
	r := buildSketchReader(t, 0, func(w *writer.Writer) {
		addServiceSpan(t, w, "svc-a", "op1", 1)
	})
	s1 := r.FileSketchSummary()
	s2 := r.FileSketchSummary()
	assert.Same(t, s1, s2, "FileSketchSummary must return the same pointer on repeated calls")
}

// TestFileSketchSummary_TopKMerged verifies TopK aggregates counts across blocks.
func TestFileSketchSummary_TopKMerged(t *testing.T) {
	// Use small block size (10) so 55 spans span multiple blocks.
	r := buildSketchReader(t, 10, func(w *writer.Writer) {
		for i := range 50 {
			addServiceSpan(t, w, "svc-hot", "op1", byte(i+1))
		}
		for i := range 5 {
			addServiceSpan(t, w, "svc-cold", "op2", byte(i+51))
		}
	})
	summary := r.FileSketchSummary()
	require.NotNil(t, summary)

	col := summary.Columns["resource.service.name"]
	require.NotNil(t, col)

	hotFP := sketch.HashForFuse("svc-hot")
	var hotCount uint32
	for _, entry := range col.TopK {
		if entry.FP == hotFP {
			hotCount = entry.Count
			break
		}
	}
	assert.Greater(t, hotCount, uint32(0), "svc-hot must appear in merged TopK")
}

// TestFileSketchSummaryRaw_NilForNoSketch verifies that FileSketchSummaryRaw returns nil
// when the file has no sketch section.
func TestFileSketchSummaryRaw_NilForNoSketch(t *testing.T) {
	r := buildSketchReader(t, 0, func(_ *writer.Writer) {
		// no spans — no sketch
	})
	b := r.FileSketchSummaryRaw()
	assert.Nil(t, b, "FileSketchSummaryRaw must return nil for files without sketch data")
}

// TestFileSketchSummaryRaw_RoundTrip verifies that FileSketchSummaryRaw bytes can be
// decoded back to a structurally equivalent FileSketchSummary via UnmarshalFileSketchSummary.
func TestFileSketchSummaryRaw_RoundTrip(t *testing.T) {
	r := buildSketchReader(t, 0, func(w *writer.Writer) {
		addServiceSpan(t, w, "svc-raw-test", "op", 1)
	})

	original := r.FileSketchSummary()
	require.NotNil(t, original, "FileSketchSummary must not be nil for a file with data")

	raw := r.FileSketchSummaryRaw()
	require.NotNil(t, raw, "FileSketchSummaryRaw must not be nil for a file with data")

	restored, err := modules_reader.UnmarshalFileSketchSummary(raw)
	require.NoError(t, err)
	require.NotNil(t, restored)

	// Column names must match.
	require.Equal(t, len(original.Columns), len(restored.Columns),
		"restored summary must have same number of columns")

	// CMS estimates must be identical for a written value.
	for name, origCol := range original.Columns {
		restCol := restored.Columns[name]
		require.NotNil(t, restCol, "column %q missing from restored summary", name)
		// Spot-check: CMS estimate for "svc-raw-test" in resource.service.name.
		if name == "resource.service.name" {
			assert.Equal(t,
				origCol.CMS.Estimate("svc-raw-test"),
				restCol.CMS.Estimate("svc-raw-test"),
				"CMS estimate must be preserved across marshal/unmarshal")
		}
	}
}

// TestFileSketchSummaryRaw_Idempotent verifies that two calls return equal bytes.
func TestFileSketchSummaryRaw_Idempotent(t *testing.T) {
	r := buildSketchReader(t, 0, func(w *writer.Writer) {
		addServiceSpan(t, w, "svc-idem", "op", 2)
	})
	b1 := r.FileSketchSummaryRaw()
	b2 := r.FileSketchSummaryRaw()
	require.NotNil(t, b1)
	assert.Equal(t, b1, b2, "FileSketchSummaryRaw must return equal bytes on repeated calls")
}
