package backendworker

// fake_s3_e2e_test.go — #181 Phase 6: a minimal, hand-rolled, in-memory S3-compatible
// HTTP server for the end-to-end tests in backend_jobs_e2e_test.go. No S3-compatible
// testcontainer module is vendored in this repo (only postgres's), and RunCubeBackfill/
// LoadCubeEntry (cube_backfill.go) construct their own *minio.Client directly from
// *s3backend.Config with NO dependency-injection seam (unlike RunViBackfill's
// RunViBackfillDeps) -- so a real e2e test of the worker's Postgres-claim dispatch path
// needs a real S3-shaped HTTP endpoint, not a hand-built fake ObjectStore. This server
// implements just enough of the S3 REST surface (PUT/GET/HEAD object, ranged GET,
// conditional PUT via If-Match, and both ListObjects V1 and ListObjectsV2 bucket
// listing) for a real *minio.Client/s3backend.Config round trip: registry Get/
// ConditionalPut (viUsageObjectStore/minioObjectStore), VI/VCNT file List/Get
// (minioVIStore), L0 file Put (s3ObjectPutter), and tenant block listing
// (backend.Reader.Blocks, used by viBlockFetcher). No request-signature verification is
// performed (this server trusts every request) -- sufficient since the point is to
// prove the real HTTP round trip through minio-go/tempo's own S3 backend code, not to
// validate AWS SigV4.

import (
	"bytes"
	"crypto/md5" //nolint:gosec // test-only content ETag, not a security use
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	s3backend "github.com/grafana/tempo/tempodb/backend/s3"
)

type fakeS3Object struct {
	data []byte
	etag string
}

// fakeS3Server is a bucket-scoped, in-memory S3-compatible object store served over a
// real httptest.Server. blockPuts, when set (SetBlockPuts), rejects every subsequent PUT
// with a real error while leaving GET/LIST unaffected.
//
// SetBlockPuts is currently unused (issue #504, 2026-07-15): it used to force a genuine,
// immediate watermark-persist failure (blobEntryStore.updateWatermarksEntry's
// ConditionalPut) against an otherwise-healthy, already-populated registry -- but cube's
// registry is Postgres-only now (no blob/index.json fallback), so watermark persistence no
// longer goes through S3 at all, and blocking S3 PUTs no longer has any effect on it.
// TestE2E_CubeBackfill_FailureThenReclaimSucceeds (backend_jobs_e2e_test.go) now injects its
// "LoadCubeEntry succeeds, the run itself later genuinely fails" scenario via a
// structurally-invalid (missing AggAttrs) seeded entry instead, tripping
// runCubeBackfillCore's consecutive-failure circuit breaker -- see also
// TestE2E_CubeBackfill_NoRegistryEntry_FailsFastNotBurningCtx (the "never registered at
// all" fast-fail case) and TestRunCubeBackfillCore_ConsecutiveStructuralFailuresAbortEarly
// (the unit-level equivalent). Left in place (not deleted) as real, working
// infrastructure -- it may still be useful for a future S3-side (not registry-side)
// failure-injection test.
type fakeS3Server struct {
	mu        sync.Mutex
	bucket    string
	objects   map[string]fakeS3Object
	blockPuts bool
}

func newFakeS3Server(t *testing.T, bucket string) (*fakeS3Server, *httptest.Server) {
	t.Helper()
	srv := &fakeS3Server{bucket: bucket, objects: map[string]fakeS3Object{}}
	ts := httptest.NewServer(http.HandlerFunc(srv.serveHTTP))
	t.Cleanup(ts.Close)
	return srv, ts
}

// SetBlockPuts toggles whether subsequent PUT requests are rejected (see fakeS3Server's
// own doc comment for why this exists).
func (s *fakeS3Server) SetBlockPuts(block bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.blockPuts = block
}

// newFakeS3Config starts a fakeS3Server and returns a *s3backend.Config pointed at it,
// path-style, insecure, with dummy static credentials. Also sets AWS_ACCESS_KEY_ID/
// AWS_SECRET_ACCESS_KEY in the test environment: cube_backfill.go's RunCubeBackfill/
// LoadCubeEntry build their own minio.Client via credentials.NewEnvAWS() directly,
// bypassing s3backend.Config's own AccessKey/SecretKey fields entirely (a real, narrow
// gap independent of this test) -- without these env vars, credential retrieval itself
// errors before any HTTP request is even issued.
func newFakeS3Config(t *testing.T, bucket string) *s3backend.Config {
	t.Helper()
	cfg, _ := newFakeS3ConfigWithServer(t, bucket)
	return cfg
}

// newFakeS3ConfigWithServer is newFakeS3Config plus the underlying *fakeS3Server, for
// tests that need to mutate its behavior after seeding (e.g. SetBlockPuts).
func newFakeS3ConfigWithServer(t *testing.T, bucket string) (*s3backend.Config, *fakeS3Server) {
	t.Helper()
	srv, ts := newFakeS3Server(t, bucket)
	t.Setenv("AWS_ACCESS_KEY_ID", "test-access-key")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "test-secret-key")
	return &s3backend.Config{
		Bucket:         bucket,
		Region:         "us-east-1",
		Endpoint:       strings.TrimPrefix(ts.URL, "http://"),
		AccessKey:      "test-access-key",
		Insecure:       true,
		ForcePathStyle: true,
	}, srv
}

func (s *fakeS3Server) serveHTTP(w http.ResponseWriter, r *http.Request) {
	bucket, key := splitBucketKey(r.URL.Path)
	if bucket != s.bucket {
		http.Error(w, "no such bucket", http.StatusNotFound)
		return
	}

	switch {
	case key == "":
		s.handleBucketList(w, r)
	case r.Method == http.MethodPut:
		s.handlePut(w, r, key)
	case r.Method == http.MethodGet:
		s.handleGet(w, r, key, true)
	case r.Method == http.MethodHead:
		s.handleGet(w, r, key, false)
	case r.Method == http.MethodDelete:
		s.handleDelete(w, key)
	default:
		http.Error(w, "unsupported method", http.StatusMethodNotAllowed)
	}
}

func splitBucketKey(urlPath string) (bucket, key string) {
	trimmed := strings.TrimPrefix(urlPath, "/")
	idx := strings.Index(trimmed, "/")
	if idx < 0 {
		return trimmed, ""
	}
	return trimmed[:idx], trimmed[idx+1:]
}

func (s *fakeS3Server) handlePut(w http.ResponseWriter, r *http.Request, key string) {
	s.mu.Lock()
	blocked := s.blockPuts
	s.mu.Unlock()
	if blocked {
		writeS3Error(w, http.StatusServiceUnavailable, "SlowDown", "Please reduce your request rate")
		return
	}

	raw, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	// minio-go signs PutObject payloads with STREAMING-AWS4-HMAC-SHA256-PAYLOAD by
	// default (aws-chunked content encoding: each chunk is
	// "<hex-size>;chunk-signature=<sig>\r\n<data>\r\n", terminated by a zero-size
	// chunk) regardless of PutObjectOptions -- this server doesn't verify signatures,
	// but must still decode the chunk framing to recover the real payload bytes.
	body := raw
	if strings.HasPrefix(r.Header.Get("X-Amz-Content-Sha256"), "STREAMING-") {
		body = decodeAWSChunked(raw)
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if ifMatch := r.Header.Get("If-Match"); ifMatch != "" && ifMatch != "*" {
		want := strings.Trim(ifMatch, `"`)
		cur := s.objects[key]
		if cur.etag != want {
			writeS3Error(w, http.StatusPreconditionFailed, "PreconditionFailed", "At least one of the pre-conditions you specified did not hold")
			return
		}
	}

	etag := fmt.Sprintf("%x", md5.Sum(body)) //nolint:gosec // test-only content hash
	s.objects[key] = fakeS3Object{data: body, etag: etag}
	w.Header().Set("ETag", `"`+etag+`"`)
	w.WriteHeader(http.StatusOK)
}

// decodeAWSChunked strips STREAMING-AWS4-HMAC-SHA256-PAYLOAD chunk framing
// ("<hex-size>;chunk-signature=<sig>\r\n<data>\r\n", repeated, terminated by a
// zero-size chunk) to recover the raw payload. No signature verification --
// this server trusts every request.
func decodeAWSChunked(raw []byte) []byte {
	var out []byte
	for len(raw) > 0 {
		idx := bytes.IndexByte(raw, '\n')
		if idx < 0 {
			break
		}
		header := string(raw[:idx])
		header = strings.TrimSuffix(header, "\r")
		sizeHex := header
		if semi := strings.IndexByte(header, ';'); semi >= 0 {
			sizeHex = header[:semi]
		}
		size, err := strconv.ParseInt(sizeHex, 16, 64)
		if err != nil {
			break
		}
		raw = raw[idx+1:]
		if size == 0 {
			break
		}
		if int64(len(raw)) < size {
			break
		}
		out = append(out, raw[:size]...)
		raw = raw[size:]
		// Skip the trailing "\r\n" after this chunk's data.
		raw = bytes.TrimPrefix(raw, []byte("\r\n"))
	}
	return out
}

func (s *fakeS3Server) handleGet(w http.ResponseWriter, r *http.Request, key string, withBody bool) {
	s.mu.Lock()
	obj, ok := s.objects[key]
	s.mu.Unlock()
	if !ok {
		writeS3Error(w, http.StatusNotFound, "NoSuchKey", "The specified key does not exist.")
		return
	}

	data := obj.data
	status := http.StatusOK
	if rng := r.Header.Get("Range"); rng != "" {
		if start, end, ok := parseRange(rng, len(data)); ok {
			w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, len(data)))
			data = data[start : end+1]
			status = http.StatusPartialContent
		}
	}

	w.Header().Set("ETag", `"`+obj.etag+`"`)
	w.Header().Set("Content-Length", strconv.Itoa(len(data)))
	w.Header().Set("Last-Modified", time.Now().UTC().Format(http.TimeFormat))
	w.WriteHeader(status)
	if withBody {
		_, _ = w.Write(data)
	}
}

// parseRange parses a "bytes=start-end" Range header (the only form minio-go's
// GetObjectOptions.SetRange emits).
func parseRange(header string, size int) (start, end int, ok bool) {
	spec := strings.TrimPrefix(header, "bytes=")
	parts := strings.SplitN(spec, "-", 2)
	if len(parts) != 2 {
		return 0, 0, false
	}
	s, err1 := strconv.Atoi(parts[0])
	e, err2 := strconv.Atoi(parts[1])
	if err1 != nil || err2 != nil || s < 0 || s > e {
		return 0, 0, false
	}
	if e >= size {
		e = size - 1
	}
	return s, e, true
}

func (s *fakeS3Server) handleDelete(w http.ResponseWriter, key string) {
	s.mu.Lock()
	delete(s.objects, key)
	s.mu.Unlock()
	w.WriteHeader(http.StatusNoContent)
}

// s3ListResult is a superset of both ListObjects V1 (Marker/NextMarker) and
// ListObjectsV2 (StartAfter/ContinuationToken/NextContinuationToken/KeyCount) response
// shapes under the shared <ListBucketResult> root element -- minio-go's XML decoder
// only reads the fields it knows about for whichever call it made, so one response
// shape safely serves both.
type s3ListResult struct {
	XMLName               xml.Name         `xml:"ListBucketResult"`
	Name                  string           `xml:"Name"`
	Prefix                string           `xml:"Prefix"`
	Marker                string           `xml:"Marker"`
	NextMarker            string           `xml:"NextMarker"`
	StartAfter            string           `xml:"StartAfter"`
	ContinuationToken     string           `xml:"ContinuationToken"`
	NextContinuationToken string           `xml:"NextContinuationToken"`
	KeyCount              int              `xml:"KeyCount"`
	Delimiter             string           `xml:"Delimiter"`
	MaxKeys               int64            `xml:"MaxKeys"`
	IsTruncated           bool             `xml:"IsTruncated"`
	Contents              []s3ListContent  `xml:"Contents"`
	CommonPrefixes        []s3CommonPrefix `xml:"CommonPrefixes"`
}

type s3ListContent struct {
	Key          string    `xml:"Key"`
	ETag         string    `xml:"ETag"`
	Size         int64     `xml:"Size"`
	StorageClass string    `xml:"StorageClass"`
	LastModified time.Time `xml:"LastModified"`
}

type s3CommonPrefix struct {
	Prefix string `xml:"Prefix"`
}

func (s *fakeS3Server) handleBucketList(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	prefix := q.Get("prefix")
	delimiter := q.Get("delimiter")
	isV2 := q.Get("list-type") == "2"
	marker := q.Get("marker")
	if isV2 {
		marker = q.Get("start-after")
	}

	s.mu.Lock()
	keys := make([]string, 0, len(s.objects))
	for k := range s.objects {
		keys = append(keys, k)
	}
	s.mu.Unlock()
	sort.Strings(keys)

	result := s3ListResult{Name: s.bucket, Prefix: prefix, Delimiter: delimiter, MaxKeys: 1000}
	if isV2 {
		result.StartAfter = marker
	} else {
		result.Marker = marker
	}

	seenPrefixes := map[string]bool{}
	for _, k := range keys {
		if !strings.HasPrefix(k, prefix) || (marker != "" && k <= marker) {
			continue
		}
		remainder := strings.TrimPrefix(k, prefix)
		if delimiter != "" {
			if idx := strings.Index(remainder, delimiter); idx >= 0 {
				cp := prefix + remainder[:idx+len(delimiter)]
				if !seenPrefixes[cp] {
					seenPrefixes[cp] = true
					result.CommonPrefixes = append(result.CommonPrefixes, s3CommonPrefix{Prefix: cp})
				}
				continue
			}
		}
		s.mu.Lock()
		obj := s.objects[k]
		s.mu.Unlock()
		result.Contents = append(result.Contents, s3ListContent{
			Key: k, ETag: `"` + obj.etag + `"`, Size: int64(len(obj.data)), StorageClass: "STANDARD",
			LastModified: time.Now().UTC(),
		})
	}
	result.KeyCount = len(result.Contents)

	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(xml.Header))
	_ = xml.NewEncoder(w).Encode(result)
}

func writeS3Error(w http.ResponseWriter, status int, code, message string) {
	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(status)
	_, _ = w.Write([]byte(`<?xml version="1.0" encoding="UTF-8"?>
<Error><Code>` + code + `</Code><Message>` + message + `</Message><Key>x</Key><RequestId>x</RequestId><HostId>x</HostId></Error>`))
}
