package main

import (
	"encoding/binary"
	"fmt"
	"math/bits"
	"math/rand/v2"
	"testing"

	"github.com/golang/snappy"
)

// Simulates realistic span:start data: sorted nanosecond timestamps with small deltas.
func generateSortedTimestamps(n int) []uint64 {
	vals := make([]uint64, n)
	// Base: ~2026-01-30 in nanoseconds
	base := uint64(1769752749_000_000_000)
	for i := range n {
		// Each span ~1-500 microseconds after the previous, with some jitter.
		base += uint64(rand.IntN(500_000)) + 1000 // 1-500µs gaps
		vals[i] = base
	}
	return vals
}

// --- Current encoding: fixed 8-byte delta ---

func encodeDelta8(vals []uint64) []byte {
	buf := make([]byte, len(vals)*8)
	var prev uint64
	for i, v := range vals {
		binary.LittleEndian.PutUint64(buf[i*8:], v-prev)
		prev = v
	}
	return buf
}

func decodeDelta8(buf []byte, n int) []uint64 {
	vals := make([]uint64, n)
	var acc uint64
	for i := range n {
		acc += binary.LittleEndian.Uint64(buf[i*8:])
		vals[i] = acc
	}
	return vals
}

// --- Varint delta encoding ---

func encodeVarintDelta(vals []uint64) []byte {
	buf := make([]byte, 0, len(vals)*4) // optimistic estimate
	var tmp [10]byte
	var prev uint64
	for _, v := range vals {
		delta := v - prev
		n := binary.PutUvarint(tmp[:], delta)
		buf = append(buf, tmp[:n]...)
		prev = v
	}
	return buf
}

func decodeVarintDelta(buf []byte, n int) []uint64 {
	vals := make([]uint64, n)
	var acc uint64
	pos := 0
	for i := range n {
		delta, bytesRead := binary.Uvarint(buf[pos:])
		acc += delta
		vals[i] = acc
		pos += bytesRead
	}
	return vals
}

// --- Gorilla delta-of-delta + bit packing ---

type bitWriter struct {
	buf     []byte
	current byte
	count   uint8
}

func (w *bitWriter) writeBits(val uint64, nbits int) {
	for nbits > 0 {
		space := 8 - int(w.count)
		if nbits <= space {
			w.current |= byte(val<<uint(space-nbits)) & (0xFF << uint(space-nbits))
			w.count += uint8(nbits)
			if w.count == 8 {
				w.buf = append(w.buf, w.current)
				w.current = 0
				w.count = 0
			}
			return
		}
		// Fill remaining bits in current byte.
		w.current |= byte(val >> uint(nbits-space))
		nbits -= space
		val &= (1 << uint(nbits)) - 1
		w.buf = append(w.buf, w.current)
		w.current = 0
		w.count = 0
	}
}

func (w *bitWriter) flush() []byte {
	if w.count > 0 {
		w.buf = append(w.buf, w.current)
	}
	return w.buf
}

func encodeGorillaDelta(vals []uint64) []byte {
	if len(vals) == 0 {
		return nil
	}
	w := &bitWriter{buf: make([]byte, 0, len(vals))}

	// Write first value as 64 bits.
	w.writeBits(vals[0], 64)

	if len(vals) == 1 {
		return w.flush()
	}

	// Write second value as delta (64 bits).
	prevDelta := int64(vals[1] - vals[0])
	w.writeBits(uint64(prevDelta), 64) //nolint:gosec

	// Subsequent values: delta-of-delta encoding.
	for i := 2; i < len(vals); i++ {
		delta := int64(vals[i] - vals[i-1])
		dod := delta - prevDelta

		if dod == 0 {
			w.writeBits(0, 1) // single 0 bit
		} else {
			absDod := dod
			if absDod < 0 {
				absDod = -absDod
			}
			bitsNeeded := bits.Len64(uint64(absDod)) + 1 // +1 for sign

			switch {
			case bitsNeeded <= 7:
				w.writeBits(0b10, 2)             // prefix 10
				w.writeBits(uint64(dod)&0x7F, 7) //nolint:gosec
			case bitsNeeded <= 9:
				w.writeBits(0b110, 3)             // prefix 110
				w.writeBits(uint64(dod)&0x1FF, 9) //nolint:gosec
			case bitsNeeded <= 12:
				w.writeBits(0b1110, 4)             // prefix 1110
				w.writeBits(uint64(dod)&0xFFF, 12) //nolint:gosec
			default:
				w.writeBits(0b1111, 4)       // prefix 1111
				w.writeBits(uint64(dod), 64) //nolint:gosec
			}
		}
		prevDelta = delta
	}
	return w.flush()
}

// --- Benchmark ---

func TestIntrinsicCompressionComparison(t *testing.T) {
	for _, n := range []int{10_000, 100_000, 1_000_000, 2_000_000} {
		vals := generateSortedTimestamps(n)

		// Current: fixed 8-byte delta.
		delta8Raw := encodeDelta8(vals)
		delta8Snappy := snappy.Encode(nil, delta8Raw)

		// Varint delta.
		varintRaw := encodeVarintDelta(vals)
		varintSnappy := snappy.Encode(nil, varintRaw)

		// Gorilla delta-of-delta.
		gorillaRaw := encodeGorillaDelta(vals)
		gorillaSnappy := snappy.Encode(nil, gorillaRaw)

		// Verify varint roundtrip.
		decoded := decodeVarintDelta(varintRaw, n)
		for i := range n {
			if decoded[i] != vals[i] {
				t.Fatalf("varint roundtrip mismatch at %d: got %d want %d", i, decoded[i], vals[i])
			}
		}

		// Verify delta8 roundtrip.
		decoded8 := decodeDelta8(delta8Raw, n)
		for i := range n {
			if decoded8[i] != vals[i] {
				t.Fatalf("delta8 roundtrip mismatch at %d", i)
			}
		}

		rawSize := n * 8 // raw uint64
		fmt.Printf("\n=== %d values (raw: %.2f MB) ===\n", n, float64(rawSize)/(1024*1024))
		fmt.Printf("  %-25s %10s %10s %10s\n", "Encoding", "Raw", "+Snappy", "Ratio")
		fmt.Printf("  %-25s %10s %10s %10s\n", "--------", "---", "-------", "-----")
		for _, enc := range []struct {
			name   string
			raw    []byte
			snappy []byte
		}{
			{"Delta8 (current)", delta8Raw, delta8Snappy},
			{"Varint delta", varintRaw, varintSnappy},
			{"Gorilla DoD", gorillaRaw, gorillaSnappy},
		} {
			ratio := float64(len(enc.snappy)) / float64(rawSize) * 100
			fmt.Printf("  %-25s %8s %8s %9.1f%%\n",
				enc.name,
				humanB(len(enc.raw)),
				humanB(len(enc.snappy)),
				ratio)
		}
	}
}

func BenchmarkDecodeDelta8(b *testing.B) {
	vals := generateSortedTimestamps(1_000_000)
	buf := encodeDelta8(vals)
	compressed := snappy.Encode(nil, buf)
	b.ResetTimer()
	for range b.N {
		raw, _ := snappy.Decode(nil, compressed)
		decodeDelta8(raw, 1_000_000)
	}
}

func BenchmarkDecodeVarintDelta(b *testing.B) {
	vals := generateSortedTimestamps(1_000_000)
	buf := encodeVarintDelta(vals)
	compressed := snappy.Encode(nil, buf)
	b.ResetTimer()
	for range b.N {
		raw, _ := snappy.Decode(nil, compressed)
		decodeVarintDelta(raw, 1_000_000)
	}
}

func humanB(b int) string {
	if b < 1024 {
		return fmt.Sprintf("%d B", b)
	}
	if b < 1024*1024 {
		return fmt.Sprintf("%.1f KB", float64(b)/1024)
	}
	return fmt.Sprintf("%.2f MB", float64(b)/(1024*1024))
}
