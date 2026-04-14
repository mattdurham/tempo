package embedder

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestAssembleAllFields_priorityOrder(t *testing.T) {
	fields := map[string]string{
		"span.name":             "POST /api/users",
		"resource.service.name": "auth-service",
		"span.http.status_code": "503",
		"span:duration":         "245ms",
		"resource.k8s.pod.name": "auth-abc123",
		"custom.tag":            "foobar",
	}
	text := AssembleAllFields(fields, DefaultMaxTextLen)

	nameIdx := strings.Index(text, "span.name=POST /api/users")
	svcIdx := strings.Index(text, "resource.service.name=auth-service")
	durIdx := strings.Index(text, "span:duration=245ms")
	httpIdx := strings.Index(text, "span.http.status_code=503")
	k8sIdx := strings.Index(text, "resource.k8s.pod.name=auth-abc123")
	customIdx := strings.Index(text, "custom.tag=foobar")

	assert.Greater(t, durIdx, nameIdx, "duration should come after span.name")
	assert.Greater(t, svcIdx, durIdx, "service.name should come after timing")
	assert.Greater(t, httpIdx, svcIdx, "http fields should come after service identity")
	assert.Greater(t, k8sIdx, httpIdx, "k8s fields should come after http")
	assert.Greater(t, customIdx, k8sIdx, "custom fields should come after k8s")
}

func TestAssembleAllFields_shipmateFieldsHighPriority(t *testing.T) {
	fields := map[string]string{
		"span.name":             "PostToolUse",
		"prompt.text":           "fix the authentication bug",
		"tool.command":          "go test ./...",
		"tool.error":            "FAIL: TestLogin expected 200 got 401",
		"tool.file":             "internal/auth/handler.go",
		"agent_type":            "team-coder",
		"task.subject":          "Fix login authentication",
		"memory.text":           "auth handler uses bcrypt for passwords",
		"score.comments":        "Good fix, covers edge cases",
		"resource.service.name": "shipmate",
		"resource.k8s.pod.name": "coder-abc123",
		"custom.unrelated":      "noise",
	}
	text := AssembleAllFields(fields, DefaultMaxTextLen)

	// Shipmate fields should come right after span.name, before timing/service/k8s
	nameIdx := strings.Index(text, "span.name=PostToolUse")
	promptIdx := strings.Index(text, "prompt.text=fix the authentication bug")
	cmdIdx := strings.Index(text, "tool.command=go test")
	errIdx := strings.Index(text, "tool.error=FAIL")
	agentIdx := strings.Index(text, "agent_type=team-coder")
	taskIdx := strings.Index(text, "task.subject=Fix login")
	_ = strings.Index(text, "memory.text=auth handler")
	svcIdx := strings.Index(text, "resource.service.name=shipmate")
	k8sIdx := strings.Index(text, "resource.k8s.pod.name=coder")
	customIdx := strings.Index(text, "custom.unrelated=noise")

	// Priority order: span.name → prompt → success/error/score → memory → task/agent → tool details → service → k8s → custom
	assert.Greater(t, promptIdx, nameIdx, "prompt should come after span.name")
	assert.Greater(t, errIdx, promptIdx, "tool.error should come after prompt (quality signal)")
	assert.Greater(t, taskIdx, errIdx, "task.subject should come after quality signals")
	assert.Greater(t, agentIdx, taskIdx, "agent_type should come after task.subject")
	assert.Greater(t, cmdIdx, agentIdx, "tool.command should come after agent/task context")
	assert.Greater(t, svcIdx, cmdIdx, "service.name should come after tool details")
	assert.Greater(t, k8sIdx, svcIdx, "k8s should come after service")
	assert.Greater(t, customIdx, k8sIdx, "custom should come last")
}

func TestAssembleAllFields_skipsExcludedFields(t *testing.T) {
	fields := map[string]string{
		"trace:id":           "abc123",
		"span:id":            "def456",
		"span:parent_id":     "ghi789",
		"__embedding__":      "should not appear",
		"__embedding_text__": "should not appear",
		"span.name":          "GET /health",
	}
	text := AssembleAllFields(fields, DefaultMaxTextLen)

	assert.NotContains(t, text, "trace:id")
	assert.NotContains(t, text, "span:id")
	assert.NotContains(t, text, "span:parent_id")
	assert.NotContains(t, text, "__embedding__")
	assert.NotContains(t, text, "__embedding_text__")
	assert.Contains(t, text, "span.name=GET /health")
}

func TestAssembleAllFields_emptyFields(t *testing.T) {
	text := AssembleAllFields(map[string]string{}, DefaultMaxTextLen)
	assert.Equal(t, "", text)
}

func TestAssembleAllFields_emptyValuesSkipped(t *testing.T) {
	fields := map[string]string{
		"span.name":     "op",
		"span.http.url": "",
	}
	text := AssembleAllFields(fields, DefaultMaxTextLen)
	assert.Contains(t, text, "span.name=op")
	assert.NotContains(t, text, "span.http.url")
}

func TestAssembleAllFields_truncation(t *testing.T) {
	fields := map[string]string{
		"span.name":             "short",
		"resource.service.name": "svc",
		"big.field":             strings.Repeat("x", 30000),
	}
	text := AssembleAllFields(fields, 100)

	assert.LessOrEqual(t, len(text), 100)
	// Priority fields should survive truncation
	assert.Contains(t, text, "span.name=short")
}

func TestAssembleAllFields_nonPriorityAlphabetical(t *testing.T) {
	fields := map[string]string{
		"zzz.custom": "last",
		"aaa.custom": "first",
		"mmm.custom": "middle",
	}
	text := AssembleAllFields(fields, DefaultMaxTextLen)

	aaaIdx := strings.Index(text, "aaa.custom=first")
	mmmIdx := strings.Index(text, "mmm.custom=middle")
	zzzIdx := strings.Index(text, "zzz.custom=last")

	assert.Greater(t, mmmIdx, aaaIdx)
	assert.Greater(t, zzzIdx, mmmIdx)
}

func TestAssembleAllFields_realWorldSpan(t *testing.T) {
	// Simulate a realistic span with many OTel attributes
	fields := map[string]string{
		"trace:id":                        "abc123", // should be skipped
		"span:id":                         "def456", // should be skipped
		"span.name":                       "POST /api/v2/orders",
		"span:start":                      "1712000000000000000",
		"span:end":                        "1712000000245000000",
		"span:duration":                   "245ms",
		"span.status":                     "error",
		"span.status.message":             "connection refused",
		"resource.service.name":           "order-service",
		"resource.service.version":        "2.3.1",
		"resource.deployment.environment": "production",
		"span.http.method":                "POST",
		"span.http.route":                 "/api/v2/orders",
		"span.http.status_code":           "503",
		"span.db.system":                  "postgresql",
		"span.db.statement":               "INSERT INTO orders (user_id, total) VALUES ($1, $2)",
		"resource.k8s.namespace.name":     "orders-prod",
		"resource.k8s.pod.name":           "order-service-7b4c9d-xk2f9",
		"resource.k8s.deployment.name":    "order-service",
		"resource.cloud.provider":         "aws",
		"resource.cloud.region":           "us-east-1",
		"span.exception.type":             "ConnectionRefusedError",
		"span.exception.message":          "could not connect to postgres:5432",
		"custom.order.id":                 "ORD-12345",
		"custom.user.tier":                "premium",
	}
	text := AssembleAllFields(fields, DefaultMaxTextLen)

	// Verify key fields are present and in correct order
	assert.NotContains(t, text, "trace:id")
	assert.NotContains(t, text, "span:id")
	assert.Contains(t, text, "span.name=POST /api/v2/orders")
	assert.Contains(t, text, "span.status=error")
	assert.Contains(t, text, "span.db.statement=INSERT INTO orders")
	assert.Contains(t, text, "resource.k8s.namespace.name=orders-prod")
	assert.Contains(t, text, "custom.order.id=ORD-12345")

	// span.name should come first
	assert.True(t, strings.HasPrefix(text, "span.name="))
}

func TestAssembleAllFields_maxLenDefault(t *testing.T) {
	fields := map[string]string{"span.name": "op"}
	text := AssembleAllFields(fields, 0) // 0 → default
	assert.Contains(t, text, "span.name=op")
}
