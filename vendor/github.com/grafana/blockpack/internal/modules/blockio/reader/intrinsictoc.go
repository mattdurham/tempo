package reader

import "github.com/grafana/blockpack/internal/modules/blockio/shared"

type intrinsicTOC struct {
	entries map[string]shared.IntrinsicColMeta
}
