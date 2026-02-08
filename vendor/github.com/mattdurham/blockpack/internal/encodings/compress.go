package encodings

import (
	"github.com/klauspost/compress/zstd"
)

// Compression level for encoders. Level 3 provides good balance of speed and compression.
const compressionLevel = 3

// CompressZstd compresses data using zstd with a provided encoder.
// Uses compression level 3 for good balance of speed and compression.
//
// IMPORTANT: This function requires a per-Writer encoder to be passed in.
// DO NOT use sync.Pool for encoders - pools retain all objects until GC runs,
// causing memory accumulation (128 MB per encoder × hundreds of blocks = OOM).
// Each Writer should own ONE encoder and reuse it for all compressions.
//
// The encoder is reset before use to clear any previous state.
func CompressZstd(data []byte, encoder *zstd.Encoder) []byte {
	// Reset encoder to clear internal state from previous compressions
	encoder.Reset(nil)
	compressed := encoder.EncodeAll(data, make([]byte, 0, len(data)))
	return compressed
}

// NewEncoder creates a new zstd encoder with the standard compression level.
// Each Writer should create ONE encoder and reuse it for all compressions.
func NewEncoder() (*zstd.Encoder, error) {
	return zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.EncoderLevelFromZstd(compressionLevel)))
}
