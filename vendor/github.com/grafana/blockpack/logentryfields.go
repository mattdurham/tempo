package blockpack

import modules_executor "github.com/grafana/blockpack/internal/modules/executor"

type logEntryFields struct {
	lokiLabels string
	line       string
	logAttrs   modules_executor.LogAttrs
	timestamp  uint64
}
