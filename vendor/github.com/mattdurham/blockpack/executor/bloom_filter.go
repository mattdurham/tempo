package executor

import (
	"hash/fnv"
)

// BloomFilterChecker provides methods to check bloom filters
type BloomFilterChecker struct{}

// CheckAttributeKey checks if an attribute key might be present in the block's bloom filter.
// Returns true if the key might be present (including false positives).
// Returns false if the key is definitely not present.
//
// The bloom filter uses FNV-1a hash with 3 hash functions derived from a single hash.
// Filter size: 80 bytes = 640 bits
// Expected false positive rate: ~5% for 100 keys
func (c *BloomFilterChecker) CheckAttributeKey(bloomFilter [80]byte, attributeKey string) bool {
	const numHashFunctions = 3
	const filterSizeBits = 640 // 80 bytes * 8 bits

	// Compute FNV-1a hash
	h := fnv.New64a()
	h.Write([]byte(attributeKey))
	hash := h.Sum64()

	// Derive 3 hash values from the single hash
	// h1 = hash & 0xFFFFFFFF (lower 32 bits)
	// h2 = hash >> 32 (upper 32 bits)
	// h_i = (h1 + i * h2) % filterSizeBits
	h1 := uint32(hash & 0xFFFFFFFF)
	h2 := uint32(hash >> 32)

	for i := uint32(0); i < numHashFunctions; i++ {
		// Compute hash function i
		bitIndex := (h1 + i*h2) % filterSizeBits

		// Check if bit is set
		byteIndex := bitIndex / 8
		bitOffset := bitIndex % 8

		if bloomFilter[byteIndex]&(1<<bitOffset) == 0 {
			// Bit is not set - key is definitely not present
			return false
		}
	}

	// All bits are set - key might be present (or false positive)
	return true
}

// CheckMultipleAttributeKeys checks if ALL attribute keys in the list might be present.
// Returns true only if all keys pass the bloom filter check.
// This is useful for AND queries where all attributes must be present.
func (c *BloomFilterChecker) CheckMultipleAttributeKeys(bloomFilter [80]byte, attributeKeys []string) bool {
	for _, key := range attributeKeys {
		if !c.CheckAttributeKey(bloomFilter, key) {
			// At least one key is definitely not present
			return false
		}
	}
	// All keys might be present
	return true
}

// CheckAnyAttributeKey checks if ANY attribute key in the list might be present.
// Returns true if at least one key passes the bloom filter check.
// This is useful for OR queries where any attribute matching is sufficient.
func (c *BloomFilterChecker) CheckAnyAttributeKey(bloomFilter [80]byte, attributeKeys []string) bool {
	for _, key := range attributeKeys {
		if c.CheckAttributeKey(bloomFilter, key) {
			// At least one key might be present
			return true
		}
	}
	// None of the keys might be present
	return false
}
