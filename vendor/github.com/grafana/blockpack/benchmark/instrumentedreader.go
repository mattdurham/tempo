package benchmark

import (
	"time"

	"github.com/grafana/tempo/tempodb/backend"
)

type instrumentedReader struct {
	backend.Reader
	bytesRead    *int64
	ioOperations *int64
	latency      time.Duration
}
