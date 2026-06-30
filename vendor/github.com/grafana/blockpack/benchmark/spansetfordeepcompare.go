package benchmark

import (
	"github.com/grafana/tempo/pkg/tempopb"
	tempocommon "github.com/grafana/tempo/pkg/tempopb/common/v1"
)

type spanSetForDeepCompare struct {
	key     string
	attrs   []*tempocommon.KeyValue
	spans   []*tempopb.Span
	matched uint32
}
