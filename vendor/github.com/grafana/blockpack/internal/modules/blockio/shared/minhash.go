package shared

// FNV-1a constants used by the MinHash signature helpers.
const (
	fnv1aOffset = uint64(14695981039346656037)
	fnv1aPrime  = uint64(1099511628211)
)

// AddHashToMinHeap hashes name via FNV-1a and inserts the result into the
// 4-element sorted min-heap, keeping the smallest 4 hashes.
// Inlines FNV-1a to avoid heap allocation and string→[]byte conversion.
func AddHashToMinHeap(name string, minHashSig *[4]uint64) {
	h := fnv1aOffset
	for i := range len(name) {
		h ^= uint64(name[i])
		h *= fnv1aPrime
	}
	insertMinHeap(h, minHashSig)
}

// AddKVHashToMinHeap hashes "key=value" via FNV-1a and inserts the result into
// the 4-element sorted min-heap. The combined hash is computed without allocating
// a new string: FNV-1a is applied over key bytes, then '=' byte, then value bytes.
func AddKVHashToMinHeap(key, value string, minHashSig *[4]uint64) {
	h := fnv1aOffset
	for i := range len(key) {
		h ^= uint64(key[i])
		h *= fnv1aPrime
	}
	h ^= '='
	h *= fnv1aPrime
	for i := range len(value) {
		h ^= uint64(value[i])
		h *= fnv1aPrime
	}
	insertMinHeap(h, minHashSig)
}

// insertMinHeap inserts v into the 4-element sorted min-heap (ascending order),
// keeping the smallest 4 values.
func insertMinHeap(v uint64, minHashSig *[4]uint64) {
	for i := range 4 {
		if v < minHashSig[i] {
			// Shift larger values right and insert v at position i.
			for j := 3; j > i; j-- {
				minHashSig[j] = minHashSig[j-1]
			}
			minHashSig[i] = v
			break
		}
	}
}
