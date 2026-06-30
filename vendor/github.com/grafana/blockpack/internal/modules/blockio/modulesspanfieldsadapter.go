package blockio

import modules_reader "github.com/grafana/blockpack/internal/modules/blockio/reader"

type modulesSpanFieldsAdapter struct {
	block *modules_reader.Block

	// reader + blockIdx enable the identity fallback (NOTE-476, issue #394) for the three
	// identity fields (trace:id/span:id/span:parent_id) when they are absent from the block
	// payload. The SpanTree (#434) and IntrinsicTOC (#433) fallback stores have been removed;
	// in v2 these fields live in block columns. Nil reader disables the fallback (the block
	// payload remains authoritative for all other fields). The notable consumer is root-span
	// detection (SpanMatch.IsRoot reads span:parent_id absence), which would otherwise treat
	// every span as a root.
	reader   *modules_reader.Reader
	blockIdx int
	rowIdx   int
}
