package tempodb

// newminioforvalueindex_test.go — task #202 regression coverage for newMinioForValueIndex's new
// instrumented-transport wiring (tempodb.go). Confirms the client still constructs successfully
// offline (client construction never dials the network -- minio.New only builds local state) and
// that the constructed options actually carry the instrumented transport instead of silently
// falling back to a bare one, so a future refactor cannot accidentally drop this wiring again.

import (
	"reflect"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/grafana/tempo/tempodb/backend/s3"
)

func TestMinioOptionsForValueIndex_UsesInstrumentedTransport(t *testing.T) {
	cfg := &s3.Config{
		Bucket:   "test-bucket",
		Region:   "us-east-1",
		Endpoint: "127.0.0.1:0",
		Insecure: true,
	}

	opts, err := minioOptionsForValueIndex(cfg)
	require.NoError(t, err)
	require.NotNil(t, opts.Transport)

	// instrumentedTransport (tempodb/backend/instrumentation) is unexported, so this asserts on
	// the reflected type name rather than a type assertion -- the point is simply that this is
	// NOT the bare minio.DefaultTransport, but the SAME wrapper the main S3 backend's own
	// createCore uses (tempodb/backend/s3/s3.go), so every GetObject/StatObject call this client
	// issues -- including every minio-go internal retry attempt -- populates
	// tempodb_backend_request_duration_seconds exactly like the main data-block path already
	// does. Before task #202, this Transport field was nil (unset), so the value-index query
	// path had literally zero HTTP-level observability.
	typeName := reflect.TypeOf(opts.Transport).String()
	require.True(t, strings.Contains(typeName, "instrumentation"),
		"want the value-index minio client wired to the instrumentation package's transport, got %s", typeName)
}

func TestNewMinioForValueIndex_ConstructsOfflineWithoutError(t *testing.T) {
	cfg := &s3.Config{
		Bucket:   "test-bucket",
		Region:   "us-east-1",
		Endpoint: "127.0.0.1:0", // never dialed at construction time -- minio.New builds local state only
		Insecure: true,
	}

	client, err := newMinioForValueIndex(cfg)
	require.NoError(t, err)
	require.NotNil(t, client)
}
