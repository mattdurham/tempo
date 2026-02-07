package encodings

import (
	"sync"

	"github.com/klauspost/compress/zstd"
)

// Compression level for pooled encoders. Level 3 provides good balance of speed and compression.
const compressionLevel = 3

// encoderPool reuses zstd encoders to avoid massive memory allocations.
// Encoders can consume 100+ MB each, so pooling provides dramatic memory savings.
var encoderPool = sync.Pool{
	New: func() any {
		// NewWriter with nil destination and valid options should never fail
		encoder, err := zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.EncoderLevelFromZstd(compressionLevel)))
		if err != nil {
			// This should never happen with valid options, but handle it explicitly
			panic("failed to create zstd encoder: " + err.Error())
		}
		return encoder
	},
}

// CompressZstd compresses data using zstd with encoder pooling.
// Uses compression level 3 for good balance of speed and compression.
// Reuses encoders from a pool to avoid massive memory allocations (encoders can be 100+ MB each).
func CompressZstd(data []byte) []byte {
	encoder := encoderPool.Get().(*zstd.Encoder)
	defer func() {
		// Reset encoder to clear internal state before returning to pool
		encoder.Reset(nil)
		encoderPool.Put(encoder)
	}()

	compressed := encoder.EncodeAll(data, make([]byte, 0, len(data)))
	return compressed
}
