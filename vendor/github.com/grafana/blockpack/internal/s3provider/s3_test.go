package s3provider_test

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"sync"
	"testing"
	"time"

	minio "github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/grafana/blockpack/internal/modules/rw"
	"github.com/grafana/blockpack/internal/s3provider"
)

// s3MockServer is an httptest.Server that simulates an S3/MinIO endpoint.
// It serves HEAD (for StatObject) and ranged GET (for GetObject) requests.
type s3MockServer struct {
	server    *httptest.Server
	data      []byte
	mu        sync.Mutex
	statCalls int
}

// StatCalls returns the number of HEAD requests received, safe for concurrent use.
func (m *s3MockServer) StatCalls() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.statCalls
}

func newS3MockServer(t *testing.T, data []byte) *s3MockServer {
	t.Helper()
	m := &s3MockServer{data: data}
	m.server = httptest.NewServer(http.HandlerFunc(m.handler))
	t.Cleanup(m.server.Close)
	return m
}

func (m *s3MockServer) handler(w http.ResponseWriter, req *http.Request) {
	w.Header().Set("x-amz-request-id", "TESTREQ000000001")
	w.Header().Set("x-amz-id-2", "TESTSECONDARY0001")
	w.Header().Set("Server", "AmazonS3")
	w.Header().Set("ETag", `"abc123abc123abc1"`)
	w.Header().Set("Accept-Ranges", "bytes")
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Last-Modified", time.Now().UTC().Format(http.TimeFormat))

	switch req.Method {
	case http.MethodHead:
		m.mu.Lock()
		m.statCalls++
		m.mu.Unlock()
		w.Header().Set("Content-Length", strconv.FormatInt(int64(len(m.data)), 10))
		w.WriteHeader(http.StatusOK)

	case http.MethodGet:
		rangeHdr := req.Header.Get("Range")
		if rangeHdr == "" {
			w.Header().Set("Content-Length", strconv.FormatInt(int64(len(m.data)), 10))
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write(m.data)
			return
		}
		// Parse "bytes=start-end" (inclusive).
		var start, end int64
		if _, err := fmt.Sscanf(rangeHdr, "bytes=%d-%d", &start, &end); err != nil {
			http.Error(w, "bad range", http.StatusBadRequest)
			return
		}
		total := int64(len(m.data))
		if end >= total {
			end = total - 1
		}
		if start > end || start >= total {
			w.Header().Set("Content-Range", fmt.Sprintf("bytes */%d", total))
			w.WriteHeader(http.StatusRequestedRangeNotSatisfiable)
			return
		}
		chunk := m.data[start : end+1]
		w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, total))
		w.Header().Set("Content-Length", strconv.FormatInt(int64(len(chunk)), 10))
		w.WriteHeader(http.StatusPartialContent)
		_, _ = io.Copy(w, bytes.NewReader(chunk))

	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

// newTestProvider creates a MinIOProvider pointing at the mock server.
func newTestProvider(t *testing.T, data []byte) (*s3provider.MinIOProvider, *s3MockServer) {
	t.Helper()
	mock := newS3MockServer(t, data)
	client, err := minio.New(mock.server.Listener.Addr().String(), &minio.Options{
		Creds:  credentials.NewStaticV4("testaccess", "testsecretkey1234567890", ""),
		Secure: false,
		Region: "us-east-1", // Skip bucket region detection request
	})
	require.NoError(t, err)
	return s3provider.NewMinIOProvider(client, "testbucket", "testobject"), mock
}

// --- S3P-T-01: Size caching ---

func TestMinIOProvider_SizeReturnsObjectSize(t *testing.T) {
	data := make([]byte, 1024)
	p, mock := newTestProvider(t, data)

	sz, err := p.Size()
	require.NoError(t, err)
	assert.Equal(t, int64(1024), sz)
	assert.Equal(t, 1, mock.StatCalls())
}

func TestMinIOProvider_SizeIsCached(t *testing.T) {
	data := make([]byte, 512)
	p, mock := newTestProvider(t, data)

	// Call Size() three times — StatObject must be called only once.
	for range 3 {
		sz, err := p.Size()
		require.NoError(t, err)
		assert.Equal(t, int64(512), sz)
	}
	assert.Equal(t, 1, mock.StatCalls(), "StatObject should be called at most once")
}

// --- S3P-T-02: ReadAt basic correctness ---

func TestMinIOProvider_ReadAt_FullRead(t *testing.T) {
	data := []byte("hello, s3 world!")
	p, _ := newTestProvider(t, data)

	buf := make([]byte, len(data))
	n, err := p.ReadAt(buf, 0, rw.DataTypeBlock)
	require.NoError(t, err)
	assert.Equal(t, len(data), n)
	assert.Equal(t, data, buf)
}

func TestMinIOProvider_ReadAt_PartialRange(t *testing.T) {
	data := []byte("abcdefghij")
	p, _ := newTestProvider(t, data)

	buf := make([]byte, 3)
	n, err := p.ReadAt(buf, 3, rw.DataTypeBlock)
	require.NoError(t, err)
	assert.Equal(t, 3, n)
	assert.Equal(t, []byte("def"), buf)
}

func TestMinIOProvider_ReadAt_EmptyBuffer(t *testing.T) {
	data := []byte("hello")
	p, _ := newTestProvider(t, data)

	n, err := p.ReadAt(make([]byte, 0), 0, rw.DataTypeBlock)
	require.NoError(t, err)
	assert.Equal(t, 0, n)
}

// --- S3P-T-03: ReadAt error cases ---

func TestMinIOProvider_ReadAt_NegativeOffset(t *testing.T) {
	p, _ := newTestProvider(t, []byte("hello"))

	_, err := p.ReadAt(make([]byte, 3), -1, rw.DataTypeBlock)
	assert.ErrorIs(t, err, os.ErrInvalid)
}

func TestMinIOProvider_ReadAt_OffsetAtEOF(t *testing.T) {
	data := []byte("hello")
	p, _ := newTestProvider(t, data)

	_, err := p.ReadAt(make([]byte, 1), int64(len(data)), rw.DataTypeBlock)
	assert.ErrorIs(t, err, io.EOF)
}

func TestMinIOProvider_ReadAt_OffsetPastEOF(t *testing.T) {
	data := []byte("hello")
	p, _ := newTestProvider(t, data)

	_, err := p.ReadAt(make([]byte, 1), int64(len(data))+10, rw.DataTypeBlock)
	assert.ErrorIs(t, err, io.EOF)
}

func TestMinIOProvider_ReadAt_SpanningEOF(t *testing.T) {
	data := []byte("hello") // 5 bytes: indices 0-4
	p, _ := newTestProvider(t, data)

	// Request 10 bytes starting at offset 3 — only 2 bytes remain.
	buf := make([]byte, 10)
	n, err := p.ReadAt(buf, 3, rw.DataTypeBlock)
	assert.ErrorIs(t, err, io.EOF)
	assert.Equal(t, 2, n)
	assert.Equal(t, []byte("lo"), buf[:n])
}

// --- S3P-T-04: DataType hint is ignored ---

func TestMinIOProvider_ReadAt_DataTypeHintIgnored(t *testing.T) {
	data := []byte("hello world")
	p, _ := newTestProvider(t, data)

	for _, dt := range []rw.DataType{
		rw.DataTypeBlock, rw.DataTypeFooter, rw.DataTypeHeader,
		rw.DataTypeMetadata, rw.DataTypeTraceBloomFilter, rw.DataTypeTimestampIndex,
	} {
		buf := make([]byte, len(data))
		n, err := p.ReadAt(buf, 0, dt)
		require.NoError(t, err, "DataType=%q should not affect ReadAt", dt)
		assert.Equal(t, len(data), n)
		assert.Equal(t, data, buf)
	}
}

// --- S3P-T-05: Concurrent safety ---

func TestMinIOProvider_ReadAt_Concurrent(t *testing.T) {
	const size = 256
	data := make([]byte, size)
	for i := range data {
		data[i] = byte(i)
	}
	p, _ := newTestProvider(t, data)

	const goroutines = 20
	errs := make(chan error, goroutines)
	var wg sync.WaitGroup
	wg.Add(goroutines)

	for g := range goroutines {
		g := g
		go func() {
			defer wg.Done()
			off := int64((g % 8) * 10)
			buf := make([]byte, 10)
			n, err := p.ReadAt(buf, off, rw.DataTypeBlock)
			if err != nil {
				errs <- fmt.Errorf("goroutine %d: %w", g, err)
				return
			}
			if n != 10 {
				errs <- fmt.Errorf("goroutine %d: got %d bytes, want 10", g, n)
				return
			}
			for i, b := range buf {
				want := data[off+int64(i)]
				if b != want {
					errs <- fmt.Errorf("goroutine %d: buf[%d]=%d, want %d", g, i, b, want)
					return
				}
			}
		}()
	}

	wg.Wait()
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}
}
