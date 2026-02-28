// Package blockio provides the writer and reader for the blockpack columnar storage format.
package blockio

// NOTE: Any changes to this file must be reflected in the corresponding specs.md or NOTES.md.

import (
	"github.com/grafana/blockpack/internal/modules/blockio/writer"
)

// WriterConfig configures the blockpack Writer.
type WriterConfig = writer.Config

// Writer encodes OTLP spans into the blockpack columnar format.
type Writer = writer.Writer

// NewWriterWithConfig creates a new Writer.
// Returns error if OutputStream is nil or MaxBlockSpans > 65535.
func NewWriterWithConfig(cfg WriterConfig) (*Writer, error) {
	return writer.NewWriterWithConfig(cfg)
}
