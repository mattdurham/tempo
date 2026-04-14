package embedder

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// startTEIServer starts a test HTTP server that speaks the TEI protocol.
// POST /embed: {"inputs": [...], "normalize": true} → [[float, ...], ...]
func startTEIServer(t *testing.T, dim int, handler http.HandlerFunc) *httptest.Server {
	t.Helper()
	if dim <= 0 {
		dim = 8
	}
	if handler == nil {
		handler = func(w http.ResponseWriter, r *http.Request) {
			if r.Method != http.MethodPost || r.URL.Path != "/embed" {
				http.NotFound(w, r)
				return
			}
			var req teiRequest
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			// Return fixed unit vectors for protocol testing.
			vecs := make([][]float32, len(req.Inputs))
			for i := range vecs {
				v := make([]float32, dim)
				v[i%dim] = 1.0
				vecs[i] = v
			}
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(vecs) // TEI returns flat [[...], ...]
		}
	}
	return httptest.NewServer(handler)
}

// Compile-time check: *httpBackend implements Backend.
var _ Backend = (*httpBackend)(nil)

func TestHTTPBackend_ImplementsBackend(t *testing.T) {
	srv := startTEIServer(t, 8, nil)
	defer srv.Close()

	b, err := NewHTTP(HTTPConfig{ServerURL: srv.URL})
	require.NoError(t, err)
	defer b.Close()

	assert.Greater(t, b.Dim(), 0)
}

func TestHTTPBackend_Embed(t *testing.T) {
	srv := startTEIServer(t, 8, nil)
	defer srv.Close()

	b, err := NewHTTP(HTTPConfig{ServerURL: srv.URL})
	require.NoError(t, err)
	defer b.Close()

	vec, err := b.Embed("hello world")
	require.NoError(t, err)
	assert.Len(t, vec, 8)
}

func TestHTTPBackend_EmbedBatch(t *testing.T) {
	srv := startTEIServer(t, 8, nil)
	defer srv.Close()

	b, err := NewHTTP(HTTPConfig{ServerURL: srv.URL})
	require.NoError(t, err)
	defer b.Close()

	vecs, err := b.EmbedBatch([]string{"one", "two", "three"})
	require.NoError(t, err)
	assert.Len(t, vecs, 3)
	for _, v := range vecs {
		assert.Len(t, v, 8)
	}
}

func TestHTTPBackend_EmptyBatch(t *testing.T) {
	srv := startTEIServer(t, 8, nil)
	defer srv.Close()

	b, err := NewHTTP(HTTPConfig{ServerURL: srv.URL})
	require.NoError(t, err)
	defer b.Close()

	vecs, err := b.EmbedBatch([]string{})
	require.NoError(t, err)
	assert.Empty(t, vecs)
}

func TestHTTPBackend_ServerError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "internal server error", http.StatusInternalServerError)
	}))
	defer srv.Close()

	_, err := NewHTTP(HTTPConfig{ServerURL: srv.URL})
	assert.Error(t, err, "should fail on 500 during probe")
}

func TestHTTPBackend_WrongVectorCount(t *testing.T) {
	// Server returns wrong number of vectors.
	dim := 8
	srv := startTEIServer(t, dim, func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.NotFound(w, r)
			return
		}
		var req teiRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		// Always return just 1 vector regardless of input count.
		v := make([]float32, dim)
		v[0] = 1.0
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode([][]float32{v})
	})
	defer srv.Close()

	e, err := NewHTTP(HTTPConfig{ServerURL: srv.URL})
	require.NoError(t, err)
	defer e.Close()

	_, err = e.EmbedBatch([]string{"a", "b", "c"})
	assert.Error(t, err, "should error when vector count != text count")
}
