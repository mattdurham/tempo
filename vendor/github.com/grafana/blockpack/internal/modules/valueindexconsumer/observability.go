package valueindexconsumer

// NOTE: Any changes to this file must be reflected in the corresponding NOTES.md.
//
// NOTE-VI-028 (issue #410): structured logging + OTel tracing for the consumer
// pipeline. Before this the pod was a black box — silent after startup even
// while burning a CPU on a large block. This file holds the package-level OTel
// tracer and the logger plumbing so service.go stays focused on orchestration.
//
// Logging: a *slog.Logger is carried on the Service (s.logger), defaulting to
// slog.Default() so a nil Config.Logger is safe and never panics. Job/flush
// boundaries log at info; per-column flush detail logs at debug, matching the
// verbosity split the issue asks for.
//
// Tracing: follows the executor's pattern (NOTE-449) — a package-level
// otel.Tracer initialized lazily by the OTel global, a no-op when no
// TracerProvider is configured. Spans are started from the context threaded
// from Poll → ingest → Extract and Run → flushAll → flushColumn. All
// span.SetAttributes calls are guarded by span.IsRecording() so unsampled jobs
// pay no attribute-allocation cost (SPEC-OBS-003 style).

import "go.opentelemetry.io/otel"

// tracer is the package-level OTel tracer for the value-index consumer module.
//
//nolint:gochecknoglobals
var tracer = otel.Tracer("github.com/grafana/blockpack/valueindexconsumer")
