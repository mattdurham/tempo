package executor

// Intrinsic column name constants formerly defined in intrinsic_row.go (#436 cleanup).
const (
	colNameTraceID       = "trace:id"
	colNameSpanID        = "span:id"
	colNameParentID      = "span:parent_id"
	colNameSpanName      = "span:name"
	colNameServiceName   = "resource.service.name"
	colNameStatusMessage = "span:status_message"
	colNameSpanStart     = "span:start"
	colNameSpanEnd       = "span:end"
	colNameSpanDuration  = "span:duration"
	colNameSpanKind      = "span:kind"
	colNameSpanStatus    = "span:status"
)

const (
	spanIDByteLen  = 8
	traceIDByteLen = 16
)

// intrinsicRowFields holds per-row typed field values for predicate evaluation.
// After #433 (IntrinsicTOC removal) and #436 (cleanup), these are populated from
// block columns rather than the IntrinsicTOC.
type intrinsicRowFields struct {
	spanName      string
	serviceName   string
	statusMessage string
	spanStart     uint64
	spanEnd       uint64
	spanDuration  uint64
	spanKind      int64
	spanStatus    int64
	spanID        [8]byte
	parentID      [8]byte
	present       uint16
	traceID       [16]byte
}

func getIntrinsicRowFields(n int) []intrinsicRowFields {
	if n <= 0 {
		return nil
	}
	s := make([]intrinsicRowFields, n)
	return s
}

func putIntrinsicRowFields(_ []intrinsicRowFields) {} // no pool after #436

// Presence bitmask constants for intrinsicRowFields.present.
const (
	intrinsicPresentTraceID       uint16 = 1 << iota // bit 0
	intrinsicPresentSpanID                           // bit 1
	intrinsicPresentParentID                         // bit 2
	intrinsicPresentSpanName                         // bit 3
	intrinsicPresentServiceName                      // bit 4
	intrinsicPresentStatusMessage                    // bit 5
	intrinsicPresentSpanStart                        // bit 6
	intrinsicPresentSpanEnd                          // bit 7
	intrinsicPresentSpanDuration                     // bit 8
	intrinsicPresentSpanKind                         // bit 9
	intrinsicPresentSpanStatus                       // bit 10
)
