package executor

// LogEntry is a blockpack data type.
type LogEntry struct {
	LokiLabels     string
	Line           string
	LogAttrs       LogAttrs
	TimestampNanos uint64
}
