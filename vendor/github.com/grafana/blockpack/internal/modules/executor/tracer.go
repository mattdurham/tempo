package executor

// NOTE: Any changes to this file must be reflected in the corresponding SPECS.md or NOTES.md.
// NOTE-449: package-level OTel tracer for the executor query path (issue #368).
// Follows Tempo's pattern: var tracer = otel.Tracer("...") at package level.
// Initialized lazily by the OTel global — no-op when no TracerProvider is configured.

import "go.opentelemetry.io/otel"

// tracer is the package-level OTel tracer for the executor module.
// SPEC-OBS-002: every Collect call uses this tracer to start blockpack.query/planner/block spans.
//
//nolint:gochecknoglobals
var tracer = otel.Tracer("github.com/grafana/blockpack/executor")
