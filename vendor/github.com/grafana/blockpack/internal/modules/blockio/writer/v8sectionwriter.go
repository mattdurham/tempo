package writer

import "github.com/grafana/blockpack/internal/modules/blockio/shared"

type v8SectionWriter struct {
	out     *countingWriter
	entries []shared.ToCEntry
}
