package viusage

// NOTE: Any changes to this file must be reflected in the corresponding SPECS.md or NOTES.md.

// DefaultDedicatedColumns is the provisional #496 bootstrap dedicated-column
// list (R2): sourced from Tempo's Parquet-14 defaultDedicatedColumns
// (tempo/tempodb/backend/block_meta.go:151-169), translated into blockpack's
// scope-prefixed column-name convention (internal/modules/blockio/writer's
// "resource."/"span." prefixing, config.go:15).
//
// Includes the 4 "legacy" HTTP semconv aliases (http.method, http.url,
// http.route, http.status_code). #496's A0 verification found these remain
// live, queryable dedicated columns in Tempo today -- block_meta.go still
// returns them from DefaultDedicatedColumns() alongside the new-semconv
// names, vparquet4's WellKnownColumnLookups still statically maps 3 of the 4
// to production query-execution code paths, and no TraceQL
// attribute-name canonicalization exists anywhere in tempo's pkg/traceql,
// pkg/tempopb, or blockpack's internal/traceqlparser -- a query naming
// http.method literally is not rewritten to http.request.method before
// reaching VI. See task #103's finding for full file:line evidence.
//
// This list is explicitly provisional (R2), not a final data-driven answer --
// it will be tuned once real production usage-registry telemetry exists.
var DefaultDedicatedColumns = []string{
	// resource-scoped
	"resource.k8s.cluster.name",
	"resource.k8s.namespace.name",
	"resource.k8s.pod.name",
	"resource.k8s.container.name",
	// span-scoped, current semconv
	"span.http.request.method",
	"span.http.response.status_code",
	"span.url.path",
	"span.url.route",
	"span.server.address",
	"span.server.port",
	// span-scoped, legacy semconv aliases (A0 finding: keep all 4)
	"span.http.method",
	"span.http.url",
	"span.http.route",
	"span.http.status_code",
}
