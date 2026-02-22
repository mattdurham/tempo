package reader

import "github.com/mattdurham/blockpack/internal/blockio/shared"

// DataType is an alias for shared.DataType for backward compatibility
type DataType = shared.DataType

const (
	// DataTypeFooter is the data type for footer sections.
	DataTypeFooter = shared.DataTypeFooter
	// DataTypeMetadata is the data type for metadata sections.
	DataTypeMetadata = shared.DataTypeMetadata
	// DataTypeColumn is the data type for column sections.
	DataTypeColumn = shared.DataTypeColumn
	// DataTypeRow is the data type for row sections.
	DataTypeRow = shared.DataTypeRow
	// DataTypeIndex is the data type for index sections.
	DataTypeIndex = shared.DataTypeIndex
	// DataTypeUnknown is the data type for unknown sections.
	DataTypeUnknown = shared.DataTypeUnknown
)

// ReaderProvider is an alias to shared.ReaderProvider for backward compatibility.
type ReaderProvider = shared.ReaderProvider //nolint:revive

// CloseableReaderProvider is an alias to shared.CloseableReaderProvider for backward compatibility.
type CloseableReaderProvider = shared.CloseableReaderProvider
