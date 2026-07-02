package benchmark

import (
	"os"
	"time"
)

type trackedFileProvider struct {
	file      *os.File
	bytesRead int64
	ioOps     int64
	latency   time.Duration
}
