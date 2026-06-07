package compaction

import logsv1 "go.opentelemetry.io/proto/otlp/logs/v1"

type pendingLogRow struct {
	ld         *logsv1.LogsData
	minHashSig [4]uint64
	timestamp  uint64
}
