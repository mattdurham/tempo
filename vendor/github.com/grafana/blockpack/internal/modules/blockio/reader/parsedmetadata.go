package reader

import "github.com/grafana/blockpack/internal/modules/blockio/shared"

type parsedMetadata struct {
	sketchIdx     *sketchIndex
	metadataBytes []byte
	blockMetas    []shared.BlockMeta
	rangeOffsets  map[string]rangeIndexMeta
	traceIndexRaw []byte
	tsRaw         []byte
	fileBloomRaw  []byte
	tsCount       int
}
