package executor_test

// vector_e2e_test.go — end-to-end test of the vector search pipeline
// using a REAL embedding model via the embed-server.
//
// Requires: embed-server running at EMBED_SERVER_URL (default http://localhost:8765)
// Start with: docker compose up -d --build --wait
//
// If the server is not available, the test is skipped.

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
	"net/http"
	"os"
	"testing"

	commonv1 "go.opentelemetry.io/proto/otlp/common/v1"
	resourcev1 "go.opentelemetry.io/proto/otlp/resource/v1"
	tracev1 "go.opentelemetry.io/proto/otlp/trace/v1"

	modules_blockio "github.com/grafana/blockpack/internal/modules/blockio"
	modules_reader "github.com/grafana/blockpack/internal/modules/blockio/reader"
	modules_shared "github.com/grafana/blockpack/internal/modules/blockio/shared"
	"github.com/grafana/blockpack/internal/modules/embedder"
	"github.com/grafana/blockpack/internal/modules/executor"
	"github.com/grafana/blockpack/internal/traceqlparser"
	"github.com/grafana/blockpack/internal/vm"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func embedServerURL() string {
	if u := os.Getenv("EMBED_SERVER_URL"); u != "" {
		return u
	}
	return "http://localhost:8765"
}

func skipIfNoEmbedServer(t *testing.T) *embedder.Embedder {
	t.Helper()
	url := embedServerURL()
	// Quick probe — if the server isn't running, skip.
	resp, err := http.Post(url+"/embed", "application/json", //nolint:noctx
		bytes.NewReader([]byte(`{"texts":[]}`)))
	if err != nil {
		t.Skipf("embed-server not available at %s: %v (run: docker compose up -d)", url, err)
	}
	_ = resp.Body.Close()

	emb, err := embedder.NewHTTP(embedder.HTTPConfig{ServerURL: url})
	if err != nil {
		t.Skipf("embed-server probe failed: %v", err)
	}
	t.Logf("Connected to embed-server at %s (dim=%d)", url, emb.Dim())
	return emb
}

// TestVectorE2E_RealModel writes 200 spans with realistic text, embeds them via
// a REAL embedding model, then queries with progressively more specific VECTOR_AI()
// queries to verify semantic similarity ranking.
func TestVectorE2E_RealModel(t *testing.T) {
	emb := skipIfNoEmbedServer(t)
	defer emb.Close()
	dim := emb.Dim()

	// --- Build realistic spans ---
	type spanDef struct {
		name    string
		service string
		text    string
	}
	spans := make([]spanDef, 0, 200) //nolint:gomnd

	// 100 database error spans across servers and error types
	dbServers := []string{"prod-01", "prod-02", "prod-05", "prod-09", "prod-15", "staging-01", "dev-01"}
	dbErrors := []string{
		"connection refused",
		"connection timeout after 30s",
		"too many connections",
		"deadlock detected",
		"relation users does not exist",
		"disk full on data volume",
		"replication lag exceeded threshold",
	}
	for i := range 100 {
		server := dbServers[i%len(dbServers)]
		errMsg := dbErrors[i%len(dbErrors)]
		spans = append(spans, spanDef{
			name:    "SELECT users",
			service: "user-service",
			text:    fmt.Sprintf("database error postgres %s %s", server, errMsg),
		})
	}

	// 50 auth failure spans
	for i := range 50 {
		spans = append(spans, spanDef{
			name:    "POST /api/login",
			service: "auth-service",
			text:    fmt.Sprintf("authentication failure invalid credentials user-%d@example.com", i),
		})
	}

	// 50 k8s scheduling spans
	for i := range 50 {
		spans = append(spans, spanDef{
			name:    "pod-scheduling",
			service: "kube-scheduler",
			text:    fmt.Sprintf("kubernetes scheduling failed insufficient CPU grafana-%d-pod", i),
		})
	}

	// --- Write spans with REAL embeddings ---
	t.Logf("Embedding %d spans via real model (dim=%d)...", len(spans), dim)
	var buf bytes.Buffer
	w, err := modules_blockio.NewWriterWithConfig(modules_blockio.WriterConfig{
		OutputStream:    &buf,
		MaxBlockSpans:   25,
		VectorDimension: dim,
	})
	require.NoError(t, err)

	for i, s := range spans {
		vec, embErr := emb.Embed(s.text)
		require.NoError(t, embErr)

		span := &tracev1.Span{
			TraceId:           vecE2ETraceID(i),
			SpanId:            vecE2ESpanID(i),
			Name:              s.name,
			StartTimeUnixNano: 1_000_000_000 + uint64(i)*1000, //nolint:gosec,gomnd
			EndTimeUnixNano:   1_000_002_000 + uint64(i)*1000, //nolint:gosec,gomnd
			Kind:              tracev1.Span_SPAN_KIND_SERVER,
			Attributes: []*commonv1.KeyValue{
				{
					Key:   modules_shared.EmbeddingColumnName,
					Value: &commonv1.AnyValue{Value: &commonv1.AnyValue_BytesValue{BytesValue: vecToBytes(vec)}},
				},
				{
					Key:   modules_shared.EmbeddingTextColumnName,
					Value: &commonv1.AnyValue{Value: &commonv1.AnyValue_StringValue{StringValue: s.text}},
				},
			},
		}
		td := &tracev1.TracesData{
			ResourceSpans: []*tracev1.ResourceSpans{{
				Resource: &resourcev1.Resource{
					Attributes: []*commonv1.KeyValue{{
						Key:   "service.name",
						Value: &commonv1.AnyValue{Value: &commonv1.AnyValue_StringValue{StringValue: s.service}},
					}},
				},
				ScopeSpans: []*tracev1.ScopeSpans{{Spans: []*tracev1.Span{span}}},
			}},
		}
		require.NoError(t, w.AddTracesData(td))
	}

	_, err = w.Flush()
	require.NoError(t, err)
	t.Logf("Wrote %d bytes", buf.Len())

	r := openReader(t, buf.Bytes())
	vi, viErr := r.VectorIndex()
	require.NoError(t, viErr)
	require.NotNil(t, vi, "VectorIndex should exist")
	t.Logf("VectorIndex: dim=%d, blocks=%d", vi.Dim, len(vi.BlockCentroids))

	// --- Progressive query refinement ---
	// Each query gets more specific. With a REAL model, scores should increase
	// as the query matches stored spans more closely.
	t.Run("progressive_refinement", func(t *testing.T) {
		queries := []struct {
			label string
			query string
		}{
			{
				"vague: errors",
				`{ VECTOR_AI("errors") }`,
			},
			{
				"+ topic: database errors",
				`{ VECTOR_AI("database errors") }`,
			},
			{
				"+ db: database errors postgres",
				`{ VECTOR_AI("database errors postgres") }`,
			},
			{
				"+ server: database errors postgres prod-09",
				`{ VECTOR_AI("database errors postgres prod-09") }`,
			},
			{
				"+ error: database errors postgres prod-09 connection refused",
				`{ VECTOR_AI("database errors postgres prod-09 connection refused") }`,
			},
		}

		var prevBestScore float32
		for _, q := range queries {
			rows, _, qErr := vecE2EQuery(t, r, q.query, emb)
			require.NoError(t, qErr)
			require.NotEmpty(t, rows, "query %q should return results", q.label)

			bestScore := rows[0].Score
			worstScore := rows[len(rows)-1].Score
			t.Logf("  %-60s  best=%.4f  worst=%.4f  n=%d", q.label, bestScore, worstScore, len(rows))

			// With a real model, more specific queries should score at least as high.
			if prevBestScore > 0 {
				assert.GreaterOrEqual(t, bestScore, prevBestScore-0.05, //nolint:gomnd
					"more specific query %q should not score much worse (%.4f vs %.4f)",
					q.label, bestScore, prevBestScore)
			}
			prevBestScore = bestScore
		}
	})

	// --- Topic separation ---
	// Database query should return database spans, auth query should return auth spans.
	t.Run("topic_separation", func(t *testing.T) {
		dbRows, _, err := vecE2EQuery(t, r, `{ VECTOR_AI("database error postgres connection refused") }`, emb)
		require.NoError(t, err)
		require.NotEmpty(t, dbRows)

		authRows, _, err := vecE2EQuery(t, r, `{ VECTOR_AI("authentication failure invalid credentials login") }`, emb)
		require.NoError(t, err)
		require.NotEmpty(t, authRows)

		k8sRows, _, err := vecE2EQuery(t, r, `{ VECTOR_AI("kubernetes scheduling failed insufficient CPU") }`, emb)
		require.NoError(t, err)
		require.NotEmpty(t, k8sRows)

		t.Logf("  db query best score:   %.4f", dbRows[0].Score)
		t.Logf("  auth query best score: %.4f", authRows[0].Score)
		t.Logf("  k8s query best score:  %.4f", k8sRows[0].Score)

		// All three topics should produce high confidence scores
		assert.Greater(t, dbRows[0].Score, float32(0.5), "db query should have strong match")
		assert.Greater(t, authRows[0].Score, float32(0.5), "auth query should have strong match")
		assert.Greater(t, k8sRows[0].Score, float32(0.5), "k8s query should have strong match")
	})

	// --- Natural language query ---
	// A natural question should still find relevant spans.
	t.Run("natural_language", func(t *testing.T) {
		rows, _, err := vecE2EQuery(t, r,
			`{ VECTOR_AI("why are we seeing connection failures to the database?") }`, emb)
		require.NoError(t, err)
		require.NotEmpty(t, rows, "natural language query should return results")
		t.Logf("  natural language query: best=%.4f  n=%d", rows[0].Score, len(rows))
		assert.Greater(t, rows[0].Score, float32(0.3), "natural language should match db error spans")
	})

	// --- LIVESTORE compat ---
	t.Run("nil_embedder_no_error", func(t *testing.T) {
		parsed, pErr := traceqlparser.ParseTraceQL(`{ VECTOR_AI("some query") }`)
		require.NoError(t, pErr)
		fe := parsed.(*traceqlparser.FilterExpression)
		prog, cErr := vm.CompileTraceQLFilterWithOptions(fe, vm.CompileOptions{Embedder: nil, Limit: 10})
		require.NoError(t, cErr)
		rows, _, eErr := executor.Collect(r, prog, executor.CollectOptions{})
		require.NoError(t, eErr)
		assert.Empty(t, rows)
	})

	t.Logf("\n=== E2E COMPLETE: %d spans, real model, semantic similarity verified ===", len(spans))
}

// --- helpers ---

func vecE2EQuery(
	t *testing.T,
	r *modules_reader.Reader,
	query string,
	emb *embedder.Embedder,
) ([]executor.MatchedRow, executor.QueryStats, error) {
	t.Helper()
	parsed, err := traceqlparser.ParseTraceQL(query)
	require.NoError(t, err)
	fe, ok := parsed.(*traceqlparser.FilterExpression)
	require.True(t, ok)
	prog, err := vm.CompileTraceQLFilterWithOptions(fe, vm.CompileOptions{
		Embedder: emb,
		Limit:    10,
	})
	require.NoError(t, err)
	return executor.Collect(r, prog, executor.CollectOptions{})
}

func vecToBytes(vec []float32) []byte {
	b := make([]byte, len(vec)*4) //nolint:gomnd
	for i, f := range vec {
		binary.LittleEndian.PutUint32(b[i*4:], math.Float32bits(f)) //nolint:gomnd
	}
	return b
}

func vecE2ETraceID(idx int) []byte {
	id := make([]byte, 16)
	binary.BigEndian.PutUint32(id, uint32(idx)) //nolint:gosec
	id[15] = 1
	return id
}

func vecE2ESpanID(idx int) []byte {
	id := make([]byte, 8)
	binary.BigEndian.PutUint16(id, uint16(idx)) //nolint:gosec
	id[7] = 1
	return id
}
