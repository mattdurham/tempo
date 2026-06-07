package writer

import "io"

type countingWriter struct {
	w     io.Writer
	total int64
}
