// Package shared provides common types and interfaces for the blockio packages.
package shared

// NOTE: Any changes to this file must be reflected in the corresponding specs.md or NOTES.md.

import (
	"encoding/binary"
	"hash/fnv"
)

// BloomHash1 computes FNV-1a 32-bit hash of name, result % 256.
func BloomHash1(name string) uint8 {
	h := fnv.New32a()
	_, _ = h.Write([]byte(name))
	return uint8(h.Sum32() % 256) //nolint:gosec // safe: result of % 256 always fits in uint8
}

// BloomHash2 computes MurmurHash3 32-bit hash of name, result % 256.
// Pure-Go implementation to avoid unsafe pointer arithmetic in external packages.
func BloomHash2(name string) uint8 {
	return uint8(murmur32([]byte(name)) % 256) //nolint:gosec // safe: result of % 256 always fits in uint8
}

// murmur32 is a pure-Go MurmurHash3 32-bit implementation (seed=0).
func murmur32(data []byte) uint32 {
	const (
		c1 = uint32(0xcc9e2d51)
		c2 = uint32(0x1b873593)
		r1 = 15
		r2 = 13
		m  = uint32(5)
		n  = uint32(0xe6546b64)
	)

	h := uint32(0)
	nblocks := len(data) / 4

	for i := range nblocks {
		k := binary.LittleEndian.Uint32(data[i*4:])
		k *= c1
		k = (k << r1) | (k >> (32 - r1))
		k *= c2
		h ^= k
		h = (h << r2) | (h >> (32 - r2))
		h = h*m + n
	}

	tail := data[nblocks*4:]
	var k1 uint32

	switch len(tail) {
	case 3:
		k1 ^= uint32(tail[2]) << 16

		fallthrough
	case 2:
		k1 ^= uint32(tail[1]) << 8

		fallthrough
	case 1:
		k1 ^= uint32(tail[0])
		k1 *= c1
		k1 = (k1 << r1) | (k1 >> (32 - r1))
		k1 *= c2
		h ^= k1
	}

	h ^= uint32(len(data)) //nolint:gosec // safe: bloom filter hashing
	h ^= h >> 16
	h *= 0x85ebca6b
	h ^= h >> 13
	h *= 0xc2b2ae35
	h ^= h >> 16

	return h
}

// SetBit sets bit pos in the bloom slice.
// The slice must be at least ceil((pos+1)/8) bytes long.
func SetBit(bloom []byte, pos uint8) {
	bloom[pos/8] |= 1 << (pos % 8)
}

// IsBitSet tests whether bit pos is set in the bloom slice.
// The slice must be at least ceil((pos+1)/8) bytes long.
func IsBitSet(bloom []byte, pos uint8) bool {
	return bloom[pos/8]&(1<<(pos%8)) != 0
}

// AddToBloom adds name to the bloom filter by setting both hash bits.
func AddToBloom(bloom []byte, name string) {
	SetBit(bloom, BloomHash1(name))
	SetBit(bloom, BloomHash2(name))
}

// TestBloom returns false only if name is definitely absent from the filter.
func TestBloom(bloom []byte, name string) bool {
	return IsBitSet(bloom, BloomHash1(name)) && IsBitSet(bloom, BloomHash2(name))
}
