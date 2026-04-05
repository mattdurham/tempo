// Package sketch provides HyperLogLog, Count-Min Sketch, and BinaryFuse8
// implementations for per-block cardinality estimation, frequency estimation,
// and value membership testing.
//
// NOTE: HyperLogLog with p=4 (16 registers). Cardinality estimation only; ~26% std error.
// SPEC-SK-01 through SPEC-SK-05 govern this file.
// Marshal/Unmarshal are 16 bytes fixed-size (SPEC-SK-03, SPEC-SK-04).
package sketch

import (
	"fmt"
	"math"
	"math/bits"
)

const (
	hllP         = 4
	hllM         = 1 << hllP // 16 registers
	hllAlpha     = 0.673     // bias correction for p=4
	hllMarshalSz = hllM      // 16 bytes
)

// HyperLogLog is a p=4 HLL with 16 uint8 registers.
// NOT safe for concurrent use. Callers must serialize Add/Cardinality/Marshal calls.
// SPEC-SK-01: Add never panics.
// SPEC-SK-02: Cardinality() == 0 when empty.
// SPEC-SK-03: Marshal() == 16 bytes.
type HyperLogLog struct {
	regs [hllM]uint8
}

// NewHyperLogLog creates an empty HLL.
func NewHyperLogLog() *HyperLogLog {
	return &HyperLogLog{}
}

// Reset clears all registers, returning the HLL to its zero state for reuse.
func (h *HyperLogLog) Reset() { h.regs = [hllM]uint8{} }

// Add hashes v and updates registers. Never panics. (SPEC-SK-01)
func (h *HyperLogLog) Add(v string) {
	h.AddHash(hllHash(v))
}

// AddHash updates registers from a pre-computed hash, skipping the hash step.
// Use when the caller already holds the HashForFuse fingerprint for the same value.
func (h *HyperLogLog) AddHash(hash uint64) {
	reg := hash >> (64 - hllP)               // top p bits select register index (0..15)
	w := hash << hllP                        // shift out top p bits; remaining bits for rho
	rho := uint8(bits.LeadingZeros64(w)) + 1 //nolint:gosec // safe: LeadingZeros64 returns 0..64, fits uint8
	if rho > h.regs[reg] {
		h.regs[reg] = rho
	}
}

// Cardinality estimates the number of distinct elements added. (SPEC-SK-02)
func (h *HyperLogLog) Cardinality() uint64 {
	var sum float64
	var zeros int
	for _, v := range h.regs {
		sum += 1.0 / math.Pow(2, float64(v))
		if v == 0 {
			zeros++
		}
	}
	m := float64(hllM)
	est := hllAlpha * m * m / sum
	// Small range correction: linear counting when estimate is small
	if est <= 2.5*m && zeros > 0 {
		est = m * math.Log(m/float64(zeros))
	}
	return uint64(math.Round(est))
}

// Marshal serializes the HLL to 16 bytes. (SPEC-SK-03)
func (h *HyperLogLog) Marshal() []byte {
	b := make([]byte, hllMarshalSz)
	copy(b, h.regs[:])
	return b
}

// Unmarshal deserializes from b. Returns error if len(b) != 16. (SPEC-SK-04)
func (h *HyperLogLog) Unmarshal(b []byte) error {
	if len(b) != hllMarshalSz {
		return fmt.Errorf("hll: Unmarshal: need %d bytes, got %d", hllMarshalSz, len(b))
	}
	copy(h.regs[:], b)
	return nil
}

// hllHash computes a 64-bit hash of s using FNV-1a with a finalizing mix step.
// The mix step (bit mixing via multiply+XOR) ensures the top bits are well-distributed,
// which is required for HLL register selection (top p bits).
// SPEC-SK-16: HashForFuse uses this same function.
func hllHash(s string) uint64 {
	const (
		offset64 uint64 = 14695981039346656037
		prime64  uint64 = 1099511628211
	)
	h := offset64
	for i := range len(s) {
		h ^= uint64(s[i])
		h *= prime64
	}
	// Finalization mix: ensures uniform distribution in high bits.
	h ^= h >> 33
	h *= 0xff51afd7ed558ccd
	h ^= h >> 33
	h *= 0xc4ceb9fe1a85ec53
	h ^= h >> 33
	return h
}

// HashForFuse hashes a string value to uint64 for BinaryFuse8 keys.
// SPEC-SK-16: must be called identically at write time and query time.
func HashForFuse(v string) uint64 {
	return hllHash(v)
}
