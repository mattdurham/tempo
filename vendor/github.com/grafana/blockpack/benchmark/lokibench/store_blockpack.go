package lokibench

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"

	"github.com/grafana/blockpack"
	"github.com/grafana/loki/v3/pkg/logproto"
	"github.com/grafana/loki/v3/pkg/logql/syntax"
	labels_model "github.com/prometheus/prometheus/model/labels"
	commonv1 "go.opentelemetry.io/proto/otlp/common/v1"
	logsv1 "go.opentelemetry.io/proto/otlp/logs/v1"
	resourcev1 "go.opentelemetry.io/proto/otlp/resource/v1"
)

// BlockpackStore implements the bench.Store interface by writing Loki streams into
// a blockpack log file. Streams are converted to OTLP LogsData messages and passed
// to a blockpack Writer. After Close(), the file is written to disk.
type BlockpackStore struct {
	writer *blockpack.Writer
	buf    bytes.Buffer
	dir    string
	path   string // set by Close(); accessible via Path()
}

// svcTypeAttrs defines two resource attribute key-value pairs per service type.
// Each type has distinct keys so computeMinHashSigFromLog hashes different "key=value"
// strings, producing distinct MinHash signatures. Streams of the same type cluster
// into the same blocks; streams of different types separate into different blocks.
// Values are fixed strings — resource attributes represent stream identity, not
// individual log entry properties.
var svcTypeAttrs = [5][2][2]string{
	// type 0: HTTP service
	{{"http.flavor", "1.1"}, {"telemetry.sdk.name", "otlp-http"}},
	// type 1: Database service
	{{"db.system", "postgresql"}, {"telemetry.sdk.name", "otlp-db"}},
	// type 2: Kubernetes workload
	{{"k8s.cluster.name", "k8s-1"}, {"telemetry.sdk.name", "otlp-k8s"}},
	// type 3: Messaging
	{{"messaging.system", "kafka"}, {"telemetry.sdk.name", "otlp-messaging"}},
	// type 4: Cache service
	{{"cache.backend", "redis"}, {"telemetry.sdk.name", "otlp-cache"}},
}

// NewBlockpackStore creates a BlockpackStore that writes a blockpack log file
// under dir when Close() is called.
func NewBlockpackStore(dir string) (*BlockpackStore, error) {
	s := &BlockpackStore{dir: dir}
	w, err := blockpack.NewWriter(&s.buf, 0 /* 0 = default 2000 rows/block */)
	if err != nil {
		return nil, fmt.Errorf("NewBlockpackStore: %w", err)
	}
	s.writer = w
	return s, nil
}

// Write converts Loki streams to OTLP LogsData and buffers them in the blockpack writer.
// Each stream becomes one ResourceLogs; each entry becomes one LogRecord.
func (s *BlockpackStore) Write(_ context.Context, streams []logproto.Stream) error {
	ld := convertStreamsToLogsData(streams)
	if err := s.writer.AddLogsData(ld); err != nil {
		return fmt.Errorf("BlockpackStore.Write: %w", err)
	}
	return nil
}

// Name implements bench.Store.
func (s *BlockpackStore) Name() string { return "blockpack" }

// Close flushes the blockpack writer and writes the file to disk.
// After Close returns nil, Path() returns the valid file path.
func (s *BlockpackStore) Close() error {
	if _, err := s.writer.Flush(); err != nil {
		return fmt.Errorf("BlockpackStore.Close: Flush: %w", err)
	}
	s.path = filepath.Join(s.dir, "blockpack.logs")
	if err := os.WriteFile(s.path, s.buf.Bytes(), 0o644); err != nil {
		return fmt.Errorf("BlockpackStore.Close: WriteFile: %w", err)
	}
	return nil
}

// Path returns the path of the written blockpack file.
// Valid only after Close() has returned nil.
func (s *BlockpackStore) Path() string { return s.path }

// convertStreamsToLogsData converts a slice of Loki logproto.Stream values into an
// OTLP logsv1.LogsData message. The mapping is:
//
//   - stream.Labels parsed into one resource attribute per label key-value pair
//   - stream.Labels stored verbatim as resource attribute "__loki_labels__" (querier
//     uses this to reconstruct the exact Loki label string without re-serialization)
//   - entry.StructuredMetadata key-values stored as LogRecord attributes
//   - entry.Timestamp stored as LogRecord.TimeUnixNano (nanoseconds)
//   - entry.Line stored as LogRecord.Body (string value)
func convertStreamsToLogsData(streams []logproto.Stream) *logsv1.LogsData {
	ld := &logsv1.LogsData{
		ResourceLogs: make([]*logsv1.ResourceLogs, 0, len(streams)),
	}
	for _, stream := range streams {
		ld.ResourceLogs = append(ld.ResourceLogs, convertStreamToResourceLogs(stream))
	}
	return ld
}

func convertStreamToResourceLogs(stream logproto.Stream) *logsv1.ResourceLogs {
	// Parse the Loki labels string into individual key-value pairs.
	// syntax.ParseLabels returns a sorted labels.Labels slice (Prometheus label format).
	parsed, err := syntax.ParseLabels(stream.Labels)

	// Capacity: individual label attrs + 1 for __loki_labels__.
	resourceAttrs := make([]*commonv1.KeyValue, 0, 8)
	if err == nil {
		parsed.Range(func(lbl labels_model.Label) {
			resourceAttrs = append(resourceAttrs, stringKV(lbl.Name, lbl.Value))
		})
	}
	// Store the raw labels string verbatim so the querier can return it
	// as the stream's Labels() value without re-serialization.
	resourceAttrs = append(resourceAttrs, stringKV("__loki_labels__", stream.Labels))

	// Set service.name to the full labels string so the blockpack writer sorts by
	// (labelsString, minHashSig, timestamp). This clusters all entries for the same
	// stream contiguously and in timestamp order — enabling stream-level block skipping
	// and correct limit-based early termination.
	// The real Loki label "service_name" (underscore) is stored as resource.service_name
	// and is unaffected by this — the writer only reads "service.name" (dot notation).
	resourceAttrs = append(resourceAttrs, stringKV("service.name", stream.Labels))

	// Add service-type-specific resource attributes to enrich the MinHash schema.
	// Different stream types (HTTP, DB, K8s, messaging, cache) get distinct resource
	// attribute key sets so that computeMinHashSigFromLog produces distinct signatures
	// for streams of different types. This clusters same-type streams into the same
	// blocks and separates different-type streams into different blocks.
	//
	// Resource attributes are used here (not LogRecord attributes) because:
	// 1. They contribute to MinHash clustering at write time.
	// 2. They are NOT returned as StructuredMetadata by the querier — the querier uses
	//    resource.__loki_labels__ for stream identity, not individual resource attributes.
	// 3. This keeps per-entry StructuredMetadata identical to the Loki chunk store,
	//    so TestSyntheticDeepCompare continues to pass.
	svcType := streamServiceType(stream.Labels)
	resourceAttrs = addStreamServiceTypeAttrs(resourceAttrs, svcType)

	records := make([]*logsv1.LogRecord, 0, len(stream.Entries))
	for _, entry := range stream.Entries {
		records = append(records, convertEntryToLogRecord(entry))
	}

	return &logsv1.ResourceLogs{
		Resource: &resourcev1.Resource{Attributes: resourceAttrs},
		ScopeLogs: []*logsv1.ScopeLogs{
			{LogRecords: records},
		},
	}
}

// streamServiceType derives a deterministic service type index (0–4) from the
// labels string using FNV-1a hashing. Streams with the same labels always get
// the same type; different streams get different types with uniform distribution.
func streamServiceType(labels string) int {
	const (
		offset = uint64(14695981039346656037)
		prime  = uint64(1099511628211)
		nTypes = 5
	)
	h := offset
	for i := range len(labels) {
		h ^= uint64(labels[i])
		h *= prime
	}
	return int(h % nTypes)
}

// addStreamServiceTypeAttrs appends two service-type-specific resource attributes to attrs.
// The attribute key schema differs by svcType so that streams of different types produce
// distinct MinHash signatures (via computeMinHashSigFromLog key=value hashing). Storing
// these as resource attributes (not LogRecord attributes) ensures they influence MinHash
// clustering without appearing as StructuredMetadata in query results, keeping
// TestSyntheticDeepCompare passing.
func addStreamServiceTypeAttrs(attrs []*commonv1.KeyValue, svcType int) []*commonv1.KeyValue {
	a := svcTypeAttrs[svcType]
	return append(attrs, stringKV(a[0][0], a[0][1]), stringKV(a[1][0], a[1][1]))
}

func convertEntryToLogRecord(entry logproto.Entry) *logsv1.LogRecord {
	tsNano := uint64(entry.Timestamp.UnixNano()) //nolint:gosec // timestamps are positive
	logAttrs := make([]*commonv1.KeyValue, 0, len(entry.StructuredMetadata))
	for _, sm := range entry.StructuredMetadata {
		logAttrs = append(logAttrs, stringKV(sm.Name, sm.Value))
	}
	return &logsv1.LogRecord{
		TimeUnixNano: tsNano,
		Body: &commonv1.AnyValue{
			Value: &commonv1.AnyValue_StringValue{StringValue: entry.Line},
		},
		Attributes: logAttrs,
	}
}

// stringKV constructs an OTLP string-valued KeyValue pair.
func stringKV(key, value string) *commonv1.KeyValue {
	return &commonv1.KeyValue{
		Key:   key,
		Value: &commonv1.AnyValue{Value: &commonv1.AnyValue_StringValue{StringValue: value}},
	}
}
