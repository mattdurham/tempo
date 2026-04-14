package shared_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/grafana/blockpack/internal/modules/blockio/shared"
)

// TestIsIntrinsicColumn verifies the canonical intrinsic column set.
func TestIsIntrinsicColumn(t *testing.T) {
	traceIntrinsics := []string{
		"trace:id",
		"span:id",
		"span:parent_id",
		"span:name",
		"span:kind",
		"span:start",
		"span:end",
		"span:duration",
		"span:status",
		"span:status_message",
		"resource.service.name",
	}

	logIntrinsics := []string{
		"log:timestamp",
		"log:observed_timestamp",
		"log:body",
		"log:severity_number",
		"log:severity_text",
		"log:trace_id",
		"log:span_id",
		"log:flags",
	}

	for _, name := range traceIntrinsics {
		assert.True(t, shared.IsIntrinsicColumn(name), "expected %q to be intrinsic", name)
	}

	for _, name := range logIntrinsics {
		assert.True(t, shared.IsIntrinsicColumn(name), "expected %q to be intrinsic", name)
	}

	// User-defined attribute columns must not be classified as intrinsic.
	attributeColumns := []string{
		"span.http.method",
		"span.http.status_code",
		"resource.cloud.region",
		"resource.deployment.environment",
		"scope.library.name",
		"log.level",
		"log.http.url",
		"",
		"span:",
		"trace:",
	}

	for _, name := range attributeColumns {
		assert.False(t, shared.IsIntrinsicColumn(name), "expected %q NOT to be intrinsic", name)
	}
}
