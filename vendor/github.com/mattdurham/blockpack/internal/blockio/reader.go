package blockio

import (
	"github.com/mattdurham/blockpack/internal/blockio/reader"
	"github.com/mattdurham/blockpack/internal/blockio/shared"
)

// Reader is an alias to reader.Reader for backward compatibility.
// All reader functionality is implemented in the reader/ subdirectory.
type Reader = reader.Reader

// Block is an alias to reader.Block for backward compatibility.
type Block = reader.Block

// BlockWithBytes is an alias to reader.BlockWithBytes for backward compatibility.
type BlockWithBytes = reader.BlockWithBytes

// TraceBlockEntry is an alias to reader.TraceBlockEntry for backward compatibility.
type TraceBlockEntry = reader.TraceBlockEntry

// DataType is an alias for shared.DataType for backward compatibility
type DataType = shared.DataType

const (
	// DataTypeFooter indicates footer data type.
	DataTypeFooter = shared.DataTypeFooter
	// DataTypeMetadata indicates metadata data type.
	DataTypeMetadata = shared.DataTypeMetadata
	// DataTypeColumn indicates column data type.
	DataTypeColumn = shared.DataTypeColumn
	// DataTypeRow indicates row data type.
	DataTypeRow = shared.DataTypeRow
	// DataTypeIndex indicates index data type.
	DataTypeIndex = shared.DataTypeIndex
	// DataTypeUnknown indicates unknown data type.
	DataTypeUnknown = shared.DataTypeUnknown
)

// ReaderProvider is an alias to shared.ReaderProvider.
type ReaderProvider = shared.ReaderProvider

// CloseableReaderProvider is an alias to shared.CloseableReaderProvider.
type CloseableReaderProvider = shared.CloseableReaderProvider

// CoalescedRead is an alias to shared.CoalescedRead for backward compatibility
type CoalescedRead = shared.CoalescedRead

// CoalesceConfig is an alias to shared.CoalesceConfig for backward compatibility
type CoalesceConfig = shared.CoalesceConfig

// ParseMetricStreamLocation is a re-export of reader.ParseMetricStreamLocation.
// It extracts the metric stream offset and length from a raw blockpack header.
var ParseMetricStreamLocation = reader.ParseMetricStreamLocation
