package embedder

// NOTE: Any changes to this file must be reflected in the corresponding specs.md or NOTES.md.

import (
	"sort"
	"strings"
	"unicode/utf8"
)

// DefaultMaxTextLen is the maximum character length for embedding text.
// ~24KB fits within nomic-embed-text-v1.5's 8192 token window for JSON/structured content.
const DefaultMaxTextLen = 24000

// skipFields are intrinsic fields that are excluded from the ALL embedding.
// These are identifiers with no semantic value for similarity search.
var skipFields = map[string]bool{
	"trace:id":           true,
	"span:id":            true,
	"span:parent_id":     true,
	"__embedding__":      true,
	"__embedding_text__": true,
}

// fieldMaxLen caps individual field values to prevent large tool output from
// crowding out higher-priority fields. Fields not in this map use their full value.
var fieldMaxLen = map[string]int{
	// Tool output fields can be very large (command output, file contents)
	"span.tool.command":    512,
	"tool.command":         512,
	"span.tool.old_string": 256,
	"tool.old_string":      256,
	"span.tool.new_string": 256,
	"tool.new_string":      256,
	"span.tool.content":    256,
	"tool.content":         256,
	// DB statements can be large
	"span.db.statement": 512,
	// Stack traces — keep enough for the top frames
	"span.exception.stacktrace": 1024,
	// Agent messages can be verbose
	"span.agent.last_message": 512,
	"agent.last_message":      512,
	// Task descriptions
	"span.task.description": 512,
	"task.description":      512,
}

// priorityFields defines the ordered list of fields for the ALL embedding.
// Fields are appended in this order until the max text length is reached.
// Fields not in this list are appended alphabetically after all priority fields.
//
// The ordering is designed so the most semantically valuable fields survive truncation:
//  1. Span identity (what happened)
//  2. Shipmate — prompt, tool execution, agent context, memory, quality
//  3. Timing (when and how long)
//  4. Error/status (what went wrong)
//  5. Service identity (who)
//  6. OTel semantic conventions — HTTP, DB, RPC, messaging
//  7. Kubernetes / infrastructure
//  8. Everything else alphabetically
var priorityFields = []string{
	// --- Span identity ---
	"span.name",
	"span:name",
	"log.body",
	"log:body",

	// --- Shipmate: User prompt (what the user asked — highest semantic value) ---
	"span.prompt.text",
	"resource.prompt.text",
	"prompt.text",

	// --- Shipmate: Quality and success (key signals for search) ---
	"span.tool.success",
	"tool.success",
	"span.tool.error",
	"tool.error",
	"span.score",
	"score",
	"span.score.comments",
	"score.comments",

	// --- Shipmate: Memory annotations (user knowledge) ---
	"span.memory.text",
	"memory.text",

	// --- Shipmate: Task context (what was being worked on) ---
	"span.task.subject",
	"task.subject",
	"span.agent_type",
	"resource.agent_type",
	"agent_type",
	"span.task.description",
	"task.description",

	// --- Shipmate: Tool details (what tool ran, where) ---
	"span.tool.description",
	"tool.description",
	"span.tool.file",
	"tool.file",
	"span.tool.command",
	"tool.command",
	"span.tool.pattern",
	"tool.pattern",
	"span.tool.query",
	"tool.query",
	"span.tool.url",
	"tool.url",
	"span.tool.old_string",
	"tool.old_string",
	"span.tool.new_string",
	"tool.new_string",
	"span.tool.content",
	"tool.content",

	// --- Shipmate: Agent lifecycle ---
	"span.agent.last_message",
	"agent.last_message",

	// --- Shipmate: Session context ---
	"span.hook.event",
	"hook.event",
	"resource.session.model",
	"session.model",
	"span.session.id",
	"resource.session.id",
	"session.id",

	// --- Timing ---
	"span:start",
	"span:end",
	"span:duration",

	// --- Status / errors ---
	"span.status",
	"span:status",
	"span.status.message",
	"span:status_message",
	"log.severity",
	"log:severity",
	"detected_level",

	// --- Service identity ---
	"resource.service.name",
	"resource.service.namespace",
	"resource.service.version",
	"resource.service.instance.id",

	// --- Deployment ---
	"resource.deployment.environment",
	"resource.deployment.environment.name",

	// --- HTTP (OTel semconv) ---
	"span.http.method",
	"span.http.request.method",
	"span.http.route",
	"span.http.url",
	"span.url.full",
	"span.url.path",
	"span.http.status_code",
	"span.http.response.status_code",
	"span.http.target",
	"span.user_agent.original",

	// --- Database (OTel semconv) ---
	"span.db.system",
	"span.db.name",
	"span.db.operation",
	"span.db.statement",
	"span.db.collection.name",

	// --- RPC (OTel semconv) ---
	"span.rpc.system",
	"span.rpc.service",
	"span.rpc.method",
	"span.rpc.grpc.status_code",

	// --- Messaging (OTel semconv) ---
	"span.messaging.system",
	"span.messaging.operation",
	"span.messaging.destination.name",

	// --- Kubernetes ---
	"resource.k8s.namespace.name",
	"resource.k8s.pod.name",
	"resource.k8s.deployment.name",
	"resource.k8s.container.name",
	"resource.k8s.node.name",
	"resource.k8s.cluster.name",

	// --- Cloud ---
	"resource.cloud.provider",
	"resource.cloud.region",
	"resource.cloud.availability_zone",

	// --- Network ---
	"span.net.peer.name",
	"span.server.address",
	"span.net.peer.port",
	"span.server.port",

	// --- Exception ---
	"span.exception.type",
	"span.exception.message",
	"span.exception.stacktrace",

	// --- Telemetry ---
	"resource.telemetry.sdk.name",
	"resource.telemetry.sdk.language",
}

// priorityIndex maps field name → position in priorityFields for O(1) lookup.
var priorityIndex = func() map[string]int { //nolint:gochecknoglobals
	m := make(map[string]int, len(priorityFields))
	for i, f := range priorityFields {
		m[f] = i
	}
	return m
}()

// AssembleAllFields builds embedding text from ALL fields on a span, ordered by priority.
// Fields in skipFields are excluded. Priority fields come first (in the order defined by
// priorityFields), then remaining fields sorted alphabetically. Each field is formatted as
// "key=value". The result is truncated to maxLen characters at a valid UTF-8 boundary.
//
// This is the default embedding strategy when no explicit field config is provided.
func AssembleAllFields(fields map[string]string, maxLen int) string {
	if maxLen <= 0 {
		maxLen = DefaultMaxTextLen
	}

	// Separate priority fields from the rest.
	type kv struct {
		key string
		val string
		pri int // priority index (-1 = not priority)
	}

	items := make([]kv, 0, len(fields))
	for k, v := range fields {
		if v == "" || skipFields[k] {
			continue
		}
		pri, ok := priorityIndex[k]
		if !ok {
			pri = -1
		}
		items = append(items, kv{key: k, val: v, pri: pri})
	}

	// Sort: priority fields first (by index), then non-priority alphabetically.
	sort.Slice(items, func(i, j int) bool {
		a, b := items[i], items[j]
		if a.pri >= 0 && b.pri >= 0 {
			return a.pri < b.pri
		}
		if a.pri >= 0 {
			return true // priority before non-priority
		}
		if b.pri >= 0 {
			return false
		}
		return a.key < b.key // both non-priority: alphabetical
	})

	// Build text, stopping before exceeding maxLen.
	var b strings.Builder
	b.Grow(min(maxLen, len(items)*50)) //nolint:gomnd
	for _, item := range items {
		// Cap individual field values to prevent large tool output from dominating.
		val := item.val
		if maxFieldLen, ok := fieldMaxLen[item.key]; ok && len(val) > maxFieldLen {
			val = val[:maxFieldLen]
		}
		entry := item.key + "=" + val
		needed := len(entry)
		if b.Len() > 0 {
			needed++ // space separator
		}
		if b.Len()+needed > maxLen {
			// Add as much of this entry as fits.
			remaining := maxLen - b.Len()
			if remaining > 1 && b.Len() > 0 {
				b.WriteByte(' ')
				remaining--
			}
			if remaining > 0 {
				partial := entry
				if len(partial) > remaining {
					partial = partial[:remaining]
				}
				b.WriteString(partial)
			}
			break
		}
		if b.Len() > 0 {
			b.WriteByte(' ')
		}
		b.WriteString(entry)
	}

	text := b.String()
	// Ensure valid UTF-8 at the boundary.
	for len(text) > 0 && !utf8.Valid([]byte(text)) {
		text = text[:len(text)-1]
	}
	return text
}
