package blockio

import modules_reader "github.com/grafana/blockpack/internal/modules/blockio/reader"

// modulesSpanFieldsAdapter reads a single row's fields directly from a decoded block.
// NOTE-436: the block payload is the sole authoritative source for all fields,
// including the identity columns (trace:id/span:id/span:parent_id) — there is no
// reader-backed intrinsic fallback.
type modulesSpanFieldsAdapter struct {
	block  *modules_reader.Block
	rowIdx int
}
