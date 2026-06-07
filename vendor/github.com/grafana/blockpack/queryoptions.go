package blockpack

import "github.com/grafana/blockpack/internal/vm"

// QueryOptions is a blockpack data type.
type QueryOptions struct {
	Embedder      vm.TextEmbedder
	SelectColumns []string
	StartNano     uint64
	EndNano       uint64
	Limit         int
	StartBlock    int
	BlockCount    int
	MostRecent    bool
}
