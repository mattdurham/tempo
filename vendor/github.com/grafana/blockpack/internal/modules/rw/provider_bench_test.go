package rw_test

// NOTE: Any changes to this file must be reflected in the corresponding SPECS.md or NOTES.md.

import (
	"testing"
	"time"

	"github.com/grafana/blockpack/internal/modules/rw"
)

// BenchmarkDefaultProviderSingleRead measures the cost of a single cache-miss read
// through the full DefaultProvider stack with simulated object-storage latency.
// BENCH-RW-01: target 1 I/O, ~20 ms elapsed, 1 MB/io.
func BenchmarkDefaultProviderSingleRead(b *testing.B) {
	const payloadSize = 1 << 20 // 1 MiB
	data := make([]byte, payloadSize)
	for i := range data {
		data[i] = byte(i)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		// Create a fresh provider each iteration so every read is a cache miss.
		mem := &memProvider{data: data}
		dp := rw.NewDefaultProviderWithLatency(mem, 20*time.Millisecond)

		buf := make([]byte, payloadSize)
		_, err := dp.ReadAt(buf, 0, rw.DataTypeBlock)
		if err != nil {
			b.Fatalf("ReadAt: %v", err)
		}

		b.ReportMetric(float64(dp.IOOps()), "io_ops")
		b.ReportMetric(float64(dp.BytesRead()), "bytes_read")
	}
}

// BenchmarkDefaultProviderCacheHit measures the cost of a cache-hit read on
// DefaultProvider — no underlying I/O should occur.
// BENCH-RW-02: target 0 I/O, < 1 µs.
func BenchmarkDefaultProviderCacheHit(b *testing.B) {
	const payloadSize = 1 << 20 // 1 MiB
	data := make([]byte, payloadSize)
	for i := range data {
		data[i] = byte(i)
	}

	// Prime the cache once before the timed loop.
	mem := &memProvider{data: data}
	dp := rw.NewDefaultProvider(mem)

	primeBuf := make([]byte, payloadSize)
	if _, err := dp.ReadAt(primeBuf, 0, rw.DataTypeBlock); err != nil {
		b.Fatalf("prime read: %v", err)
	}
	dp.Reset() // zero counters so only cache-hit ops are measured

	buf := make([]byte, payloadSize)
	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, err := dp.ReadAt(buf, 0, rw.DataTypeBlock)
		if err != nil {
			b.Fatalf("ReadAt: %v", err)
		}
	}

	// After all iterations the tracker should still show 0 ops (all cache hits).
	b.ReportMetric(float64(dp.IOOps()), "io_ops")
}
