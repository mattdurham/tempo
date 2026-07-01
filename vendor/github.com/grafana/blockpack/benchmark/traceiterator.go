package benchmark

import (
	"github.com/grafana/tempo/pkg/tempopb"
	"github.com/grafana/tempo/tempodb/encoding/common"
)

// traceIterator implements common.Iterator for a slice of traces.
type traceIterator struct {
	traces []*tempopb.Trace
	ids    []common.ID
	index  int
}
