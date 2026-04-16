// Package blockio provides the writer and reader for the blockpack columnar storage format.
package blockio

// NOTE: Any changes to this file must be reflected in the corresponding specs.md or NOTES.md.

import (
	"io"

	"github.com/grafana/blockpack/internal/modules/blockio/writer"
)

// DedicatedColumn describes an attribute column to be stored in the intrinsic section
// as a dedicated column. See writer.DedicatedColumn for full documentation.
type DedicatedColumn = writer.DedicatedColumn

// WriterConfig configures the blockpack Writer.
type WriterConfig = writer.Config

// Writer encodes OTLP spans into the blockpack columnar format.
type Writer = writer.Writer

// NewWriterWithConfig creates a new Writer.
// Returns error if OutputStream is nil or MaxBlockSpans > 65535.
func NewWriterWithConfig(cfg WriterConfig) (*Writer, error) {
	return writer.NewWriterWithConfig(cfg)
}

// NewWriter creates a new Writer writing to w with the given max spans per block.
// maxSpansPerBlock=0 uses the writer's default block size limit.
func NewWriter(w io.Writer, maxSpansPerBlock int) (*Writer, error) {
	return writer.NewWriterWithConfig(WriterConfig{
		OutputStream:  w,
		MaxBlockSpans: maxSpansPerBlock,
	})
}
