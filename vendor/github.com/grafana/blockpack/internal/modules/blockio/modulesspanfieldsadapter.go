package blockio

import modules_reader "github.com/grafana/blockpack/internal/modules/blockio/reader"

type modulesSpanFieldsAdapter struct {
	block *modules_reader.Block

	// reader + blockIdx enable the SpanTree identity fallback (NOTE-476, issue #394) for the
	// three identity fields (trace:id/span:id/span:parent_id) when they are absent from the
	// block payload AND the IntrinsicTOC (blocks written with OmitIntrinsicIdentityColumns).
	// Nil reader disables the fallback (the block payload remains authoritative for all other
	// fields). The notable consumer is root-span detection (SpanMatch.IsRoot reads
	// span:parent_id absence), which would otherwise treat every span as a root.
	reader   *modules_reader.Reader
	blockIdx int
	rowIdx   int
}
