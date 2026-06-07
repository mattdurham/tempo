package blockio

import modules_reader "github.com/grafana/blockpack/internal/modules/blockio/reader"

type modulesSpanFieldsAdapter struct {
	block  *modules_reader.Block
	rowIdx int
}
