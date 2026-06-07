package reader

import "github.com/grafana/blockpack/internal/modules/sketch"

// FileBloom is a blockpack data type.
type FileBloom struct {
	columns map[string]*sketch.BinaryFuse8
	raw     []byte
}
