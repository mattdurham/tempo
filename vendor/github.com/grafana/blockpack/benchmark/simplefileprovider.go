package benchmark

import "os"

// simpleFileProvider wraps an os.File to implement blockpack.ReaderProvider without tracking.
type simpleFileProvider struct {
	file *os.File
}
