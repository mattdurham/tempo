package writer

import "github.com/grafana/blockpack/internal/modules/blockio/shared"

type stringColumnBuilder struct {
	colName        string
	values         []string
	present        []bool
	typ            shared.ColumnType
	detectedAsUUID bool
}
