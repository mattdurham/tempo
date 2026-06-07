package writer

import "github.com/grafana/blockpack/internal/modules/blockio/shared"

type dictEntry struct {
	strVal   string
	refs     []shared.BlockRef
	int64Val int64
}
