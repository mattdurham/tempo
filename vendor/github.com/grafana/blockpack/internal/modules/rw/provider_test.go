package rw_test

// NOTE: Any changes to this file must be reflected in the corresponding SPECS.md or NOTES.md.

import (
	"bytes"
	"fmt"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/grafana/blockpack/internal/modules/rw"
)

// --- memProvider: minimal in-memory ReaderProvider for tests ---

type memProvider struct {
	data    []byte
	readCnt int
	mu      sync.RWMutex
}

func (m *memProvider) Size() (int64, error) { return int64(len(m.data)), nil }

func (m *memProvider) ReadAt(p []byte, off int64, _ rw.DataType) (int, error) {
	m.mu.Lock()
	m.readCnt++
	m.mu.Unlock()
	if off < 0 || off > int64(len(m.data)) {
		return 0, bytes.ErrTooLarge
	}
	n := copy(p, m.data[off:])
	return n, nil
}

func (m *memProvider) ReadCount() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.readCnt
}

// --- RW-T-01: TrackingReaderProvider ---

func TestTrackingReaderProviderInitialCountersAreZero(t *testing.T) {
	mem := &memProvider{data: []byte("hello world")}
	tr := rw.NewTrackingReaderProvider(mem)
	assert.Equal(t, int64(0), tr.IOOps())
	assert.Equal(t, int64(0), tr.BytesRead())
}

func TestTrackingReaderProviderIncrementsOnRead(t *testing.T) {
	mem := &memProvider{data: []byte("hello world")}
	tr := rw.NewTrackingReaderProvider(mem)

	buf := make([]byte, 5)
	n, err := tr.ReadAt(buf, 0, rw.DataTypeBlock)
	require.NoError(t, err)
	assert.Equal(t, 5, n)
	assert.Equal(t, int64(1), tr.IOOps())
	assert.Equal(t, int64(5), tr.BytesRead())
}

func TestTrackingReaderProviderMultipleReads(t *testing.T) {
	mem := &memProvider{data: make([]byte, 100)}
	tr := rw.NewTrackingReaderProvider(mem)

	buf := make([]byte, 10)
	for i := range 5 {
		_, err := tr.ReadAt(buf, int64(i*10), rw.DataTypeBlock)
		require.NoError(t, err)
	}
	assert.Equal(t, int64(5), tr.IOOps())
	assert.Equal(t, int64(50), tr.BytesRead())
}

func TestTrackingReaderProviderReset(t *testing.T) {
	mem := &memProvider{data: make([]byte, 100)}
	tr := rw.NewTrackingReaderProvider(mem)

	buf := make([]byte, 10)
	_, err := tr.ReadAt(buf, 0, rw.DataTypeBlock)
	require.NoError(t, err)

	tr.Reset()
	assert.Equal(t, int64(0), tr.IOOps())
	assert.Equal(t, int64(0), tr.BytesRead())
}

func TestTrackingReaderProviderSizeDelegates(t *testing.T) {
	mem := &memProvider{data: make([]byte, 42)}
	tr := rw.NewTrackingReaderProvider(mem)
	sz, err := tr.Size()
	require.NoError(t, err)
	assert.Equal(t, int64(42), sz)
}

// --- RW-T-02: RangeCachingProvider ---

func TestRangeCachingProviderCacheMiss(t *testing.T) {
	mem := &memProvider{data: []byte("abcdefghij")}
	rc := rw.NewRangeCachingProvider(mem)

	buf := make([]byte, 5)
	n, err := rc.ReadAt(buf, 0, rw.DataTypeBlock)
	require.NoError(t, err)
	assert.Equal(t, 5, n)
	assert.Equal(t, []byte("abcde"), buf)
	assert.Equal(t, 1, mem.ReadCount())
}

func TestRangeCachingProviderCacheHit(t *testing.T) {
	mem := &memProvider{data: []byte("abcdefghij")}
	rc := rw.NewRangeCachingProvider(mem)

	// First read — cache miss.
	buf := make([]byte, 10)
	_, err := rc.ReadAt(buf, 0, rw.DataTypeBlock)
	require.NoError(t, err)

	// Second read — sub-range of cached region.
	buf2 := make([]byte, 3)
	n, err := rc.ReadAt(buf2, 2, rw.DataTypeBlock)
	require.NoError(t, err)
	assert.Equal(t, 3, n)
	assert.Equal(t, []byte("cde"), buf2)
	// Underlying should still have been called only once.
	assert.Equal(t, 1, mem.ReadCount())
}

func TestRangeCachingProviderSubRangeHit(t *testing.T) {
	mem := &memProvider{data: make([]byte, 100)}
	for i := range mem.data {
		mem.data[i] = byte(i)
	}
	rc := rw.NewRangeCachingProvider(mem)

	// Prime cache with [0, 50).
	buf := make([]byte, 50)
	_, err := rc.ReadAt(buf, 0, rw.DataTypeBlock)
	require.NoError(t, err)

	// Sub-range [10, 20) should be served from cache.
	buf2 := make([]byte, 10)
	n, err := rc.ReadAt(buf2, 10, rw.DataTypeBlock)
	require.NoError(t, err)
	assert.Equal(t, 10, n)
	for i, b := range buf2 {
		assert.Equal(t, byte(10+i), b)
	}
	assert.Equal(t, 1, mem.ReadCount())
}

func TestRangeCachingProviderSizeDelegates(t *testing.T) {
	mem := &memProvider{data: make([]byte, 77)}
	rc := rw.NewRangeCachingProvider(mem)
	sz, err := rc.Size()
	require.NoError(t, err)
	assert.Equal(t, int64(77), sz)
}

// --- RW-T-03: DefaultProvider ---

func TestDefaultProviderRoundTrip(t *testing.T) {
	data := []byte("hello default provider")
	mem := &memProvider{data: data}
	dp := rw.NewDefaultProvider(mem)

	buf := make([]byte, len(data))
	n, err := dp.ReadAt(buf, 0, rw.DataTypeBlock)
	require.NoError(t, err)
	assert.Equal(t, len(data), n)
	assert.Equal(t, data, buf)
}

func TestDefaultProviderTracksIOOps(t *testing.T) {
	mem := &memProvider{data: make([]byte, 100)}
	dp := rw.NewDefaultProvider(mem)

	buf := make([]byte, 10)
	for range 3 {
		_, err := dp.ReadAt(buf, 0, rw.DataTypeBlock)
		require.NoError(t, err)
	}
	// Cache hit on second and third read — only 1 real I/O.
	assert.Equal(t, int64(1), dp.IOOps())
}

func TestDefaultProviderBytesRead(t *testing.T) {
	mem := &memProvider{data: make([]byte, 50)}
	dp := rw.NewDefaultProvider(mem)

	buf := make([]byte, 50)
	_, err := dp.ReadAt(buf, 0, rw.DataTypeBlock)
	require.NoError(t, err)
	assert.Equal(t, int64(50), dp.BytesRead())
}

func TestDefaultProviderSizeDelegates(t *testing.T) {
	mem := &memProvider{data: make([]byte, 99)}
	dp := rw.NewDefaultProvider(mem)
	sz, err := dp.Size()
	require.NoError(t, err)
	assert.Equal(t, int64(99), sz)
}

func TestDefaultProviderWithLatencyInjectsDelay(t *testing.T) {
	mem := &memProvider{data: make([]byte, 10)}
	latency := 5 * time.Millisecond
	dp := rw.NewDefaultProviderWithLatency(mem, latency)

	buf := make([]byte, 10)
	start := time.Now()
	_, err := dp.ReadAt(buf, 0, rw.DataTypeBlock)
	elapsed := time.Since(start)
	require.NoError(t, err)
	assert.GreaterOrEqual(t, elapsed, latency)
}

func TestDefaultProviderReset(t *testing.T) {
	mem := &memProvider{data: make([]byte, 50)}
	dp := rw.NewDefaultProvider(mem)

	buf := make([]byte, 50)
	_, err := dp.ReadAt(buf, 0, rw.DataTypeBlock)
	require.NoError(t, err)
	assert.Equal(t, int64(1), dp.IOOps())
	assert.Equal(t, int64(50), dp.BytesRead())

	dp.Reset()
	assert.Equal(t, int64(0), dp.IOOps())
	assert.Equal(t, int64(0), dp.BytesRead())
}

// --- RW-T-04: Concurrent safety ---

// TestRangeCachingProviderConcurrentReads verifies that RangeCachingProvider is safe
// for concurrent use. Multiple goroutines read from the same provider simultaneously;
// the -race detector will catch any data races. All goroutines must receive correct data.
func TestRangeCachingProviderConcurrentReads(t *testing.T) {
	const dataSize = 64
	data := make([]byte, dataSize)
	for i := range data {
		data[i] = byte(i)
	}
	mem := &memProvider{data: data}
	rc := rw.NewRangeCachingProvider(mem)

	const goroutines = 20
	errs := make(chan error, goroutines)
	var wg sync.WaitGroup
	wg.Add(goroutines)

	for g := range goroutines {
		g := g
		go func() {
			defer wg.Done()
			off := int64((g % 4) * 10) // spread across four overlapping sub-ranges
			length := 10
			if off+int64(length) > dataSize {
				length = dataSize - int(off)
			}
			buf := make([]byte, length)
			n, err := rc.ReadAt(buf, off, rw.DataTypeBlock)
			if err != nil {
				errs <- err
				return
			}
			if n != length {
				errs <- fmt.Errorf("goroutine %d: got %d bytes, want %d", g, n, length)
				return
			}
			// Compare against the original data slice to avoid G115 int->byte cast.
			want := data[off : off+int64(n)]
			for i, b := range buf {
				if b != want[i] {
					errs <- fmt.Errorf("goroutine %d: buf[%d]=%d, want %d", g, i, b, want[i])
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

// --- RW-T-04b: TrackingProvider concurrent counters ---

// TestTrackingProvider_AtomicCounters verifies that TrackingReaderProvider counters
// are race-safe when accessed concurrently from many goroutines.
// GAP-32: concurrent read tracking.
func TestTrackingProvider_AtomicCounters(t *testing.T) {
	const goroutines = 20
	const readsPerGoroutine = 10
	const bytesPerRead = 8

	data := make([]byte, 1024)
	for i := range data {
		data[i] = byte(i) //nolint:gosec // safe: i%256 fits byte
	}
	mem := &memProvider{data: data}
	tp := rw.NewTrackingReaderProvider(mem)

	var wg sync.WaitGroup
	errs := make(chan error, goroutines)
	wg.Add(goroutines)

	for range goroutines {
		go func() {
			defer wg.Done()
			for range readsPerGoroutine {
				buf := make([]byte, bytesPerRead)
				if _, err := tp.ReadAt(buf, 0, rw.DataTypeBlock); err != nil {
					errs <- err
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

	expectedIOOps := int64(goroutines * readsPerGoroutine)
	expectedBytes := int64(goroutines * readsPerGoroutine * bytesPerRead)
	assert.Equal(t, expectedIOOps, tp.IOOps(), "IOOps must equal goroutines*readsPerGoroutine")
	assert.Equal(t, expectedBytes, tp.BytesRead(), "BytesRead must equal goroutines*readsPerGoroutine*bytesPerRead")
}

// TestTrackingProvider_IOFailureOnFail verifies that TrackingProvider propagates errors
// from the underlying provider. IOOps counts all ReadAt attempts, including failed ones.
// GAP-9 supplement: I/O failure at rw layer.
func TestTrackingProvider_IOFailureOnFail(t *testing.T) {
	data := make([]byte, 32)

	// Build a custom provider that returns ErrUnexpectedEOF on the 2nd ReadAt call.
	var callCount int
	var mu sync.Mutex
	inner := &memProvider{data: data}
	adapter := &failOnNthProvider{
		memProvider: inner,
		failOnN:     2,
		mu:          &mu,
		callCount:   &callCount,
	}
	tp := rw.NewTrackingReaderProvider(adapter)

	buf := make([]byte, 8)

	// First read: must succeed.
	_, err1 := tp.ReadAt(buf, 0, rw.DataTypeBlock)
	require.NoError(t, err1, "first read must succeed")
	assert.Equal(t, int64(1), tp.IOOps(), "one successful read so far")

	// Second read: must fail.
	_, err2 := tp.ReadAt(buf, 0, rw.DataTypeBlock)
	require.ErrorIs(t, err2, io.ErrUnexpectedEOF, "second read must propagate the injected error")
	assert.Equal(t, int64(2), tp.IOOps(), "IOOps counts all attempts including failures")
}

// failOnNthProvider wraps a memProvider and returns ErrUnexpectedEOF on the Nth call.
type failOnNthProvider struct {
	memProvider *memProvider
	callCount   *int
	mu          *sync.Mutex
	failOnN     int
}

func (f *failOnNthProvider) Size() (int64, error) { return f.memProvider.Size() }

func (f *failOnNthProvider) ReadAt(p []byte, off int64, dt rw.DataType) (int, error) {
	f.mu.Lock()
	*f.callCount++
	n := *f.callCount
	f.mu.Unlock()
	if n >= f.failOnN {
		return 0, io.ErrUnexpectedEOF
	}
	return f.memProvider.ReadAt(p, off, dt)
}

// --- RW-T-05: Partial-read guard ---

// shortReadProvider is an in-memory provider that deliberately returns fewer bytes
// than requested with no error, simulating a provider that violates the full-read
// contract (io.ReaderAt semantics).
type shortReadProvider struct {
	data []byte
}

func (s *shortReadProvider) Size() (int64, error) { return int64(len(s.data)), nil }

func (s *shortReadProvider) ReadAt(p []byte, off int64, _ rw.DataType) (int, error) {
	if off < 0 || off >= int64(len(s.data)) {
		return 0, io.EOF
	}
	available := s.data[off:]
	// Return only half the requested bytes (rounded up to at least 1), no error.
	half := (len(p) + 1) / 2
	if half > len(available) {
		half = len(available)
	}
	n := copy(p[:half], available)
	return n, nil // short read, no error — the bug condition being tested
}

// TestRangeCachingProviderPartialReadReturnsError verifies that RangeCachingProvider
// escalates a short-read-with-no-error from the underlying provider to
// io.ErrUnexpectedEOF, preventing silent data truncation.
func TestRangeCachingProviderPartialReadReturnsError(t *testing.T) {
	sp := &shortReadProvider{data: []byte("abcdefghij")} // 10 bytes
	rc := rw.NewRangeCachingProvider(sp)

	buf := make([]byte, 10)
	n, err := rc.ReadAt(buf, 0, rw.DataTypeBlock)
	assert.ErrorIs(t, err, io.ErrUnexpectedEOF,
		"partial read with no error from underlying must become ErrUnexpectedEOF")
	assert.Less(t, n, 10, "n must be less than requested length on short read")
}
