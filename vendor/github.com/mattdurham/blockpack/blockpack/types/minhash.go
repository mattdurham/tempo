package ondisk

import (
	"encoding/binary"
	"unsafe"

	"github.com/cespare/xxhash/v2"
	"github.com/mattdurham/blockpack/internal/arena"
)

// OTELSemanticFields lists string fields used for span similarity clustering.
// These fields are based on OpenTelemetry semantic conventions and are used
// to compute MinHash signatures for grouping similar spans together.
// BOT: Are these instrinsics or do they come from otel like this?
var OTELSemanticFields = []string{
	"span.service.name",
	"span.http.method",
	"span.http.status_code",
	"span.http.route",
	"span.http.url",
	"span.http.target",
	"span.rpc.system",
	"span.rpc.service",
	"span.rpc.method",
	"span.db.system",
	"span.db.operation",
	"span.db.name",
	"span.k8s.namespace.name",
	"span.k8s.pod.name",
	"span.k8s.deployment.name",
	"span.name",
	"span.kind",
}

// MinHashSignature is a 128-element array of hash values representing a span's
// similarity fingerprint. It's used to cluster similar spans together during
// block writing to improve query performance.
type MinHashSignature [128]uint64

// ComputeMinHash computes a MinHash signature for a set of key:value tokens.
//
// Flow:
// 1) Initialize a 128-slot signature with max uint64 values.
// 2) For each token, compute 128 hashes using different seeds (0..127).
// 3) For each position i, keep the minimum hash seen so far.
// 4) The resulting 128 values form a compact fingerprint.
//
// Why this works:
//   - For any two token sets A and B, the fraction of positions where the
//     signatures match approximates Jaccard(A,B) = |A intersect B| / |A union B|.
//   - This lets us compare span similarity without storing full attribute sets.
//
// Example:
// tokens A = {"span.http.method:GET", "span.name:GET /cart"}
// tokens B = {"span.http.method:GET", "span.name:GET /cart", "span.http.status_code:200"}
// -> Jaccard(A,B) = 2 / 3 ~= 0.67, so we expect ~67% of signature slots to match.
//
// Scaling note:
//   - Comparing all spans is O(N^2). For N=10,000 spans, that is 49,995,000
//     signature comparisons, each comparing 128 uint64 slots. This is feasible
//     for small batches but too costly for large datasets without pruning
//     (bucketing, LSH, or blocking by known attributes).
func ComputeMinHash(tokens []string) MinHashSignature {
	var sig MinHashSignature

	// Initialize with max values
	for i := range sig {
		sig[i] = ^uint64(0)
	}

	// Reuse hash object and buffers for all hashing operations
	// This avoids creating 128 * len(tokens) hash objects
	h := xxhash.New()

	// Pre-allocate buffer for token bytes to avoid repeated string-to-byte conversions
	// Start with 256 bytes, will grow if needed
	tokenBuf := make([]byte, 0, 256)

	// For each token, compute 128 hash values and take minimum
	for _, token := range tokens {
		// Reuse tokenBuf for each token (avoids allocation per token)
		tokenBuf = tokenBuf[:0]
		tokenBuf = append(tokenBuf, token...)

		for i := 0; i < 128; i++ {
			hashVal := hashTokenWithSeedReuseXXHash(h, tokenBuf, uint64(i))
			if hashVal < sig[i] {
				sig[i] = hashVal
			}
		}
	}

	return sig
}

// hashTokenWithSeedReuseXXHash computes a hash of token with a seed using xxhash.
// Uses xxhash which is 5-10x faster than SHA256 for non-cryptographic hashing.
// Reuses the hash object to avoid allocations.
func hashTokenWithSeedReuseXXHash(h *xxhash.Digest, tokenBytes []byte, seed uint64) uint64 {
	h.Reset() // Reset hash to initial state for reuse

	// Write seed first to mix it into the hash
	var seedBytes [8]byte
	binary.LittleEndian.PutUint64(seedBytes[:], seed)
	_, _ = h.Write(seedBytes[:])

	// Write token bytes (already converted, avoiding allocation)
	_, _ = h.Write(tokenBytes)

	// Return the hash value
	return h.Sum64()
}

// CompareMinHashSigs compares two MinHash signatures lexicographically.
// Returns -1 if a < b, 0 if a == b, 1 if a > b.
// This comparison is used as the secondary sort key for span clustering.
func CompareMinHashSigs(a, b MinHashSignature) int {
	for i := 0; i < 128; i++ {
		if a[i] < b[i] {
			return -1
		}
		if a[i] > b[i] {
			return 1
		}
	}
	return 0
}

// ExtractOTELTokens extracts key:value tokens from span attributes based on
// OpenTelemetry semantic conventions. Only fields listed in OTELSemanticFields
// are extracted. Returns "field:value" tokens in the fixed order of
// OTELSemanticFields for deterministic MinHash computation.
func ExtractOTELTokens(attributes map[string]string) []string {
	return ExtractOTELTokensWithArena(attributes, nil)
}

// ExtractOTELTokensWithArena extracts key:value tokens from span attributes based on
// OpenTelemetry semantic conventions, optionally allocating token strings on the arena.
// The returned tokens should not outlive the provided arena.
func ExtractOTELTokensWithArena(attributes map[string]string, a *arena.Arena) []string {
	tokens := make([]string, 0, len(OTELSemanticFields))
	for _, field := range OTELSemanticFields {
		if val, ok := attributes[field]; ok && val != "" {
			if a == nil {
				tokens = append(tokens, field+":"+val)
				continue
			}
			tokens = append(tokens, arenaTokenString(a, field, val))
		}
	}
	return tokens
}

func arenaTokenString(a *arena.Arena, field, value string) string {
	size := len(field) + 1 + len(value)
	if size == 0 {
		return ""
	}
	buf := unsafe.Slice((*byte)(unsafe.Pointer(a.Alloc(size))), size)
	copy(buf, field)
	buf[len(field)] = ':'
	copy(buf[len(field)+1:], value)
	return unsafe.String(&buf[0], size)
}
