package writer

import "github.com/grafana/blockpack/internal/modules/blockio/reader"

type addRowCacheKey struct {
	r        *reader.Reader
	blockIdx int
}
