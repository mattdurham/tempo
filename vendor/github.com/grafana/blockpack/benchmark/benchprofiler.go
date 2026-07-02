package benchmark

import "os"

type benchProfiler struct {
	cpuFile           *os.File
	memFile           *os.File
	memProfileRateOld int
}
