package lokibench

import (
	"fmt"
	"math/rand/v2"
	"time"

	commonv1 "go.opentelemetry.io/proto/otlp/common/v1"
	logsv1 "go.opentelemetry.io/proto/otlp/logs/v1"
	resourcev1 "go.opentelemetry.io/proto/otlp/resource/v1"
)

// OTLPGenConfig controls the OTLP log data generator.
type OTLPGenConfig struct {
	// DataMB is the approximate target dataset size in megabytes.
	DataMB int64
	// Window is the time range over which log timestamps are distributed.
	Window time.Duration
	// Start is the beginning of the time window.
	// If zero, defaults to 2024-01-01T00:00:00Z.
	Start time.Time
}

// Label value distributions matching logFilterQueries.
var (
	otlpClusters    = []string{"cluster-0", "cluster-1", "cluster-2", "cluster-3", "cluster-4"}
	otlpEnvs        = []string{"prod", "staging", "dev"}
	otlpRegions     = []string{"us-east-1", "us-west-2", "eu-west-1", "ap-southeast-1"}
	otlpDatacenters = []string{"dc1", "dc2", "dc3"}
	otlpNamespaces  = []string{
		"namespace-0", "namespace-1", "namespace-2", "namespace-3", "namespace-4",
		"namespace-5", "namespace-6", "namespace-7", "namespace-8", "namespace-9",
	}

	otlpSeverities = []struct {
		text   string
		number logsv1.SeverityNumber
	}{
		{"error", logsv1.SeverityNumber_SEVERITY_NUMBER_ERROR},
		{"warn", logsv1.SeverityNumber_SEVERITY_NUMBER_WARN},
		{"info", logsv1.SeverityNumber_SEVERITY_NUMBER_INFO},
		{"debug", logsv1.SeverityNumber_SEVERITY_NUMBER_DEBUG},
	}

	otlpComponents = []string{"api", "db", "worker", "cache", "scheduler", "proxy", "auth", "ingester"}
	otlpMessages   = []string{
		"request failed", "request completed", "slow query", "connection timeout",
		"cache miss", "cache hit", "job started", "job completed",
		"health check failed", "authentication failed", "rate limit exceeded", "retry attempt",
	}

	// otlpInstances has 200 unique values simulating high-cardinality instance IDs.
	// Instances are assigned per resource group index (resourceKeys[i % 200]) so that
	// logs from the same resource group tend to share an instance_id, giving the
	// range index some clustering and enabling block-level pruning.
	otlpInstances = func() []string {
		ids := make([]string, 200)
		for i := range ids {
			ids[i] = fmt.Sprintf("instance-%03d", i)
		}
		return ids
	}()
)

// avgRecordBytes is a conservative per-record byte estimate for size tracking.
const avgRecordBytes = 180

// GenerateOTLP generates OTLP LogsData with the given config.
//
// Resource attributes match the label distribution in logFilterQueries:
// cluster-0…4, env prod/staging/dev, region us-east-1/us-west-2/eu-west-1/ap-southeast-1,
// datacenter dc1/dc2/dc3, namespace namespace-0…9.
//
// Each LogRecord has:
//   - SeverityText/SeverityNumber set to error/warn/info/debug uniformly
//   - Body: logfmt-formatted line for body-scan query compatibility
//   - Attributes: detected_level, level (both equal to SeverityText), and
//     instance_id (200 unique values, assigned round-robin for block clustering)
//     so all three pipeline filters work without a parse stage
//
// Records are distributed round-robin across all 1800 resource combinations
// until the target size is reached.
func GenerateOTLP(cfg OTLPGenConfig) *logsv1.LogsData {
	if cfg.Start.IsZero() {
		cfg.Start = time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	}

	targetBytes := cfg.DataMB * 1024 * 1024
	rng := rand.New(rand.NewPCG(42, 0)) // deterministic seed for reproducibility
	startNano := cfg.Start.UnixNano()
	windowNanos := cfg.Window.Nanoseconds()

	// Compute the per-record base timestamp step so that records advance
	// monotonically and deterministically across the window with no jitter.
	// This guarantees every record has a unique nanosecond timestamp, which
	// makes both stores return identical "oldest N" subsets for any query.
	estimatedTotal := targetBytes / avgRecordBytes
	if estimatedTotal == 0 {
		estimatedTotal = 1
	}
	stepNano := windowNanos / estimatedTotal

	// Build the full cartesian product of resource label combinations.
	type resourceKey struct {
		cluster, env, region, datacenter, namespace string
	}
	var resourceKeys []resourceKey
	for _, cl := range otlpClusters {
		for _, ev := range otlpEnvs {
			for _, rg := range otlpRegions {
				for _, dc := range otlpDatacenters {
					for _, ns := range otlpNamespaces {
						resourceKeys = append(resourceKeys, resourceKey{cl, ev, rg, dc, ns})
					}
				}
			}
		}
	}

	// Pre-build one ResourceLogs per resource combination.
	resourceLogs := make([]*logsv1.ResourceLogs, len(resourceKeys))
	for i, rk := range resourceKeys {
		resourceLogs[i] = &logsv1.ResourceLogs{
			Resource: &resourcev1.Resource{
				Attributes: []*commonv1.KeyValue{
					stringKV("cluster", rk.cluster),
					stringKV("datacenter", rk.datacenter),
					stringKV("env", rk.env),
					stringKV("namespace", rk.namespace),
					stringKV("region", rk.region),
					// service.name uses the namespace as a logical service identifier (10 unique values).
					// MinHash clustering hashes all resource attribute key=value pairs, so the 1800
					// resource combinations cluster naturally without a composite key.
					stringKV("service.name", rk.namespace),
				},
			},
			ScopeLogs: []*logsv1.ScopeLogs{{LogRecords: nil}},
		}
	}

	// Generate records round-robin until the target byte estimate is reached.
	var totalBytes int64
	ri := 0
	for totalBytes < targetBytes {
		sev := otlpSeverities[rng.IntN(len(otlpSeverities))]
		comp := otlpComponents[rng.IntN(len(otlpComponents))]
		msg := otlpMessages[rng.IntN(len(otlpMessages))]
		latency := rng.IntN(5000)

		body := fmt.Sprintf(`level=%s component=%s msg=%q latency_ms=%d`,
			sev.text, comp, msg, latency)

		// Deterministic timestamp: each record advances by stepNano from Start.
		// No jitter — ensures every record has a unique nanosecond timestamp so
		// both stores always return identical "oldest N" subsets for any query.
		tsNano := startNano + int64(ri)*stepNano

		// Assign instance_id based on resource index for block-level clustering:
		// logs from the same resource group share an instance_id, so the range
		// index can prune blocks that don't contain the queried instance.
		instanceID := otlpInstances[ri%len(otlpInstances)]

		record := &logsv1.LogRecord{
			TimeUnixNano:   uint64(tsNano), //nolint:gosec // timestamps are positive
			SeverityText:   sev.text,
			SeverityNumber: sev.number,
			Body: &commonv1.AnyValue{
				Value: &commonv1.AnyValue_StringValue{StringValue: body},
			},
			// Both detected_level and level are written as LogRecord attributes so that:
			//   | detected_level="error"  — scans log.detected_level column natively
			//   | level="error"           — scans log.level column natively
			// instance_id is a high-cardinality field (200 values) stored as log.instance_id
			// for T11 benchmark queries testing range-index pruning on high-cardinality columns.
			// No logfmt parse stage needed for any of these filters in blockpack.
			Attributes: []*commonv1.KeyValue{
				stringKV("detected_level", sev.text),
				stringKV("instance_id", instanceID),
				stringKV("latency_ms", fmt.Sprintf("%d", latency)),
				stringKV("level", sev.text),
			},
		}

		sl := resourceLogs[ri%len(resourceKeys)].ScopeLogs[0]
		sl.LogRecords = append(sl.LogRecords, record)
		totalBytes += avgRecordBytes
		ri++
	}

	return &logsv1.LogsData{ResourceLogs: resourceLogs}
}
