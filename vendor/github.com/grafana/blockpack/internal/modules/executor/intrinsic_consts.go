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

// intrinsicRowFields holds a row's identity fields, read from block columns.
// NOTE-436: only the identity columns (trace:id/span:id/span:parent_id) are carried
// here for trace reconstruction in the structural scan; all predicate evaluation runs
// directly against block columns via the column provider.
type intrinsicRowFields struct {
	spanID   [8]byte
	parentID [8]byte
	present  uint16
	traceID  [16]byte
}

func getIntrinsicRowFields(n int) []intrinsicRowFields {
	if n <= 0 {
		return nil
	}
	return make([]intrinsicRowFields, n)
}

func putIntrinsicRowFields(_ []intrinsicRowFields) {} // no pool after #436

// Presence bitmask constants for intrinsicRowFields.present.
const (
	intrinsicPresentTraceID  uint16 = 1 << iota // bit 0
	intrinsicPresentSpanID                      // bit 1
	intrinsicPresentParentID                    // bit 2
)
