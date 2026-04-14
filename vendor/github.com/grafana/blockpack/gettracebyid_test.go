package blockpack_test

// gettracebyid_test.go — regression tests and benchmarks for GetTraceByID block-scan scope.
//
// These tests verify that GetTraceByID touches only the blocks that the trace index
// identifies as containing the target trace, not all blocks in the file.
// See: fix(reader): scope GetTraceByID intrinsic scan to matching blocks only

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"sync/atomic"
	"testing"

	blockpack "github.com/grafana/blockpack"
	commonv1 "go.opentelemetry.io/proto/otlp/common/v1"
	resourcev1 "go.opentelemetry.io/proto/otlp/resource/v1"
	tracev1 "go.opentelemetry.io/proto/otlp/trace/v1"

	"github.com/stretchr/testify/require"
)

// countingProvider wraps an in-memory byte slice and counts ReadAt calls.
type countingProvider struct {
	data    []byte
	readOps atomic.Int64
}

func (c *countingProvider) Size() (int64, error) { return int64(len(c.data)), nil }

func (c *countingProvider) ReadAt(p []byte, off int64, _ blockpack.DataType) (int, error) {
	c.readOps.Add(1)
	if off < 0 || off > int64(len(c.data)) {
		return 0, fmt.Errorf("ReadAt: offset %d out of range", off)
	}
	n := copy(p, c.data[off:])
	return n, nil
}

// writeMultiBlockFile writes a blockpack file with nBlocks blocks (1 span per block).
// The target trace is placed in block 0 only. All other blocks get distinct trace IDs.
// Returns (fileBytes, targetTraceIDHex).
func writeMultiBlockFile(t testing.TB, nBlocks int) ([]byte, string) {
	t.Helper()

	// Target trace: always in block 0 (first span written).
	targetTraceID := [16]byte{
		0xAA,
		0xBB,
		0xCC,
		0xDD,
		0x00,
		0x00,
		0x00,
		0x00,
		0x00,
		0x00,
		0x00,
		0x00,
		0x00,
		0x00,
		0x00,
		0x01,
	}
	targetSpanID := [8]byte{0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01}

	output := &bytes.Buffer{}
	// maxSpansPerBlock=1 guarantees each span goes into its own block.
	w, err := blockpack.NewWriter(output, 1)
	require.NoError(t, err)

	for i := range nBlocks {
		var traceID [16]byte
		var spanID [8]byte
		if i == 0 {
			traceID = targetTraceID
			spanID = targetSpanID
		} else {
			// Distinct trace and span IDs for all other blocks.
			traceID = [16]byte{0xFF, 0x00, byte(i >> 8), byte(i)} //nolint:gosec // test data
			spanID = [8]byte{0x02, 0x00, byte(i >> 8), byte(i)}   //nolint:gosec // test data
		}

		td := &tracev1.TracesData{
			ResourceSpans: []*tracev1.ResourceSpans{{
				Resource: &resourcev1.Resource{
					Attributes: []*commonv1.KeyValue{{
						Key:   "service.name",
						Value: &commonv1.AnyValue{Value: &commonv1.AnyValue_StringValue{StringValue: "svc"}},
					}},
				},
				ScopeSpans: []*tracev1.ScopeSpans{{
					Spans: []*tracev1.Span{{
						TraceId:           traceID[:],
						SpanId:            spanID[:],
						Name:              fmt.Sprintf("span-%d", i),
						StartTimeUnixNano: uint64(1_000_000_000 + i*1_000_000), //nolint:gosec // test data
						EndTimeUnixNano:   uint64(1_001_000_000 + i*1_000_000), //nolint:gosec // test data
					}},
				}},
			}},
		}
		require.NoError(t, w.AddTracesData(td))
	}
	_, err = w.Flush()
	require.NoError(t, err)

	return output.Bytes(), hex.EncodeToString(targetTraceID[:])
}

// TestGetTraceByID_BlockScanDoesNotScaleWithBlockCount is the regression test.
//
// It verifies that the number of ReadAt calls issued by GetTraceByID does NOT grow
// linearly with block count when the target trace lives in only one block.
//
// With 5 blocks and 20 blocks, the difference in ReadAt calls should not be
// proportional to (20-5) = 15 extra blocks. We allow a small constant slack for
// footer/header/compact-index reads.
func TestGetTraceByID_BlockScanDoesNotScaleWithBlockCount(t *testing.T) {
	blockpack.ClearReaderCaches()

	const smallBlockCount = 5
	const largeBlockCount = 20

	fileSmall, traceIDHex := writeMultiBlockFile(t, smallBlockCount)
	fileLarge, _ := writeMultiBlockFile(t, largeBlockCount)

	// Use the same targetTraceID (block 0 only) in both files.
	provSmall := &countingProvider{data: fileSmall}
	provLarge := &countingProvider{data: fileLarge}

	rSmall, err := blockpack.NewLeanReaderFromProvider(provSmall)
	require.NoError(t, err)
	opsAfterOpen5 := provSmall.readOps.Load()

	rLarge, err := blockpack.NewLeanReaderFromProvider(provLarge)
	require.NoError(t, err)
	opsAfterOpen20 := provLarge.readOps.Load()

	// Reset counters so we measure only GetTraceByID I/O.
	provSmall.readOps.Store(0)
	provLarge.readOps.Store(0)
	_ = opsAfterOpen5
	_ = opsAfterOpen20

	matchesSmall, err := blockpack.GetTraceByID(rSmall, traceIDHex)
	require.NoError(t, err)
	require.NotEmpty(t, matchesSmall, "target trace must be found in small file")
	opsSmall := provSmall.readOps.Load()

	matchesLarge, err := blockpack.GetTraceByID(rLarge, traceIDHex)
	require.NoError(t, err)
	require.NotEmpty(t, matchesLarge, "target trace must be found in large file")
	opsLarge := provLarge.readOps.Load()

	// The intrinsic column is decoded once per GetTraceByID call (1 I/O for the blob).
	// Block data read (1 I/O for the matching block) is constant regardless of total block count.
	// Allow up to 3× the small-file ops as slack for any fixed overhead differences, but
	// the large file must not be proportionally more expensive than the small file.
	//
	// Concretely: if opsSmall == 3 then opsLarge must be <= 3*3 = 9 (not ~12 for 20 blocks).
	maxAllowed := opsSmall*3 + 2 // generous constant slack
	require.LessOrEqual(t, opsLarge, maxAllowed,
		"GetTraceByID ReadAt ops must not scale linearly with block count: opsSmall=%d opsLarge=%d maxAllowed=%d",
		opsSmall, opsLarge, maxAllowed,
	)
}

// TestNewReaderForProgram_NilProgram verifies that NewReaderForProgram with a nil program
// returns a lean reader (no column data needed). This exercises the public API path so
// the deadcode tool does not report the function as unreachable.
func TestNewReaderForProgram_NilProgram(t *testing.T) {
	data, _ := writeMultiBlockFile(t, 1)
	prov := &memReaderProvider{data: data}
	r, err := blockpack.NewReaderForProgram(nil, prov, "", nil)
	require.NoError(t, err)
	require.NotNil(t, r, "NewReaderForProgram with nil program must return a non-nil reader")
}

// BenchmarkGetTraceByIDBlockCount measures GetTraceByID latency as block count scales.
// The target trace exists only in block 0. Latency should be roughly constant, not linear.
//
// Run with: go test -run=^$ -bench=BenchmarkGetTraceByIDBlockCount -benchmem ./...
func BenchmarkGetTraceByIDBlockCount(b *testing.B) {
	for _, nBlocks := range []int{1, 5, 10, 20} {
		b.Run(fmt.Sprintf("blocks=%d", nBlocks), func(b *testing.B) {
			blockpack.ClearReaderCaches()

			data, traceIDHex := writeMultiBlockFile(b, nBlocks)

			b.ResetTimer()
			for range b.N {
				// Re-create reader each iteration to avoid per-reader caching skewing results.
				// ClearReaderCaches ensures process-level caches don't help either.
				blockpack.ClearReaderCaches()
				r, err := blockpack.NewLeanReaderFromProvider(&memReaderProvider{data: data})
				if err != nil {
					b.Fatal(err)
				}
				matches, err := blockpack.GetTraceByID(r, traceIDHex)
				if err != nil {
					b.Fatal(err)
				}
				if len(matches) == 0 {
					b.Fatal("expected matches, got none")
				}
			}
		})
	}
}
