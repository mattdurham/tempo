package parquetconv

import (
	"github.com/grafana/tempo/pkg/tempopb"
	"github.com/grafana/tempo/tempodb/encoding/common"
)

type traceIterator struct {
	traces []*tempopb.Trace
	ids    []common.ID
	index  int
}
