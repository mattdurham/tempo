package valueindex

// tracer.go — package-level OTel tracer for the trace-by-id partial-read path (task #202,
// live production incident: trace-by-id lookups taking 10-20+ seconds and timing out, with
// each candidate index file resolving via only 3-4 sequential round trips that, even at a
// pessimistic 100-200ms each, cannot explain the observed latency on their own). Mirrors
// executor's own package-level tracer exactly (NOTE-449, internal/modules/executor/tracer.go):
// var tracer = otel.Tracer(...) at package scope, initialized lazily by the OTel global -- a
// no-op when no TracerProvider is configured, so this instrumentation costs nothing beyond a
// couple of time.Now() calls when tracing is disabled.

import "go.opentelemetry.io/otel"

// tracer is the package-level OTel tracer for the valueindex module's partial-read
// trace-by-id lookup path (LookupTraceGroupPartial and its per-step helpers).
//
//nolint:gochecknoglobals
var tracer = otel.Tracer("github.com/grafana/blockpack/valueindex")
