package embedder

import "time"

// HTTPConfig is a blockpack data type.
type HTTPConfig struct {
	ServerURL            string
	Fields               []EmbeddingField
	MaxTextLength        int
	Timeout              time.Duration
	MaxBatchSize         int
	MaxConcurrentBatches int
}
