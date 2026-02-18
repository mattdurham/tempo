package kafkaotlpforwarder

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

func TestMetrics_BytesRead(t *testing.T) {
	m := NewMetrics(prometheus.NewRegistry())
	require.NotNil(t, m)

	// Test that BytesRead counter can be incremented
	m.BytesRead.Add(100)

	count := testutil.ToFloat64(m.BytesRead)
	require.Equal(t, float64(100), count)
}

func TestMetrics_BytesSent(t *testing.T) {
	m := NewMetrics(prometheus.NewRegistry())
	require.NotNil(t, m)

	// Test that BytesSent counter tracks per endpoint
	m.BytesSent.WithLabelValues("localhost:4317").Add(200)
	m.BytesSent.WithLabelValues("localhost:4318").Add(300)

	// Should be able to retrieve metrics for specific endpoint
	count1 := testutil.ToFloat64(m.BytesSent.WithLabelValues("localhost:4317"))
	count2 := testutil.ToFloat64(m.BytesSent.WithLabelValues("localhost:4318"))

	require.Equal(t, float64(200), count1)
	require.Equal(t, float64(300), count2)
}

func TestMetrics_KafkaOffset(t *testing.T) {
	m := NewMetrics(prometheus.NewRegistry())
	require.NotNil(t, m)

	// Test that KafkaOffset gauge tracks per partition
	m.KafkaOffset.WithLabelValues("0").Set(1234)
	m.KafkaOffset.WithLabelValues("1").Set(5678)

	offset1 := testutil.ToFloat64(m.KafkaOffset.WithLabelValues("0"))
	offset2 := testutil.ToFloat64(m.KafkaOffset.WithLabelValues("1"))

	require.Equal(t, float64(1234), offset1)
	require.Equal(t, float64(5678), offset2)
}

func TestMetrics_Registration(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := NewMetrics(reg)
	require.NotNil(t, m)

	// Access Vec metrics to ensure they're registered
	// (Prometheus doesn't register Vec families until first access)
	m.BytesSent.WithLabelValues("test").Add(0)
	m.KafkaOffset.WithLabelValues("0").Set(0)

	// Verify metrics are registered
	metricFamilies, err := reg.Gather()
	require.NoError(t, err)
	require.Len(t, metricFamilies, 3, "should have 3 metric families")

	names := make(map[string]bool)
	for _, mf := range metricFamilies {
		names[mf.GetName()] = true
	}

	require.True(t, names["kafka_otlp_forwarder_bytes_read_total"])
	require.True(t, names["kafka_otlp_forwarder_bytes_sent_total"])
	require.True(t, names["kafka_otlp_forwarder_kafka_offset"])
}
