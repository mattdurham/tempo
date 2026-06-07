package writer

import logsv1 "go.opentelemetry.io/proto/otlp/logs/v1"

type pendingLogRecord struct {
	rl         *logsv1.ResourceLogs
	sl         *logsv1.ScopeLogs
	record     *logsv1.LogRecord
	svcName    string
	minHashSig [4]uint64
	timestamp  uint64
}
