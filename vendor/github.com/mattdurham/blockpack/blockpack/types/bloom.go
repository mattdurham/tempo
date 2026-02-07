package ondisk

// ColumnNameBloom tracks column name presence per block using a 256-bit bloom filter.
//
// Collision/False Positive Rate Chart:
// This bloom filter uses 256 bits (m=256) with 2 hash functions (k=2).
// False positive probability: (1 - e^(-kn/m))^k where n = number of items inserted
//
// Expected false positive rates:
//
//	n=10 items:  ~0.7%   (1 in 143)
//	n=20 items:  ~2.8%   (1 in 36)
//	n=30 items:  ~6.2%   (1 in 16)
//	n=50 items:  ~16%    (1 in 6)
//	n=100 items: ~53%    (1 in 2)
//	n=200 items: ~87%    (9 in 10)
//
// This filter is optimized for small item counts (<50 columns per block).
// For typical traces with 20-30 unique column names per block, the false positive
// rate is acceptable at ~3-7%. False positives only cause us to read a block's
// column metadata, not the actual column data, so the cost is minimal.
type ColumnNameBloom [columnNameBloomBytes]byte
