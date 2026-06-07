package executor

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
