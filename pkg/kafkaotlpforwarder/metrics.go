package kafkaotlpforwarder

import (
	"github.com/prometheus/client_golang/prometheus"
)

// Metrics holds all Prometheus metrics for the forwarder.
type Metrics struct {
	BytesRead   prometheus.Counter
	BytesSent   *prometheus.CounterVec
	KafkaOffset *prometheus.GaugeVec
}

// NewMetrics creates and registers all metrics with the provided registry.
func NewMetrics(reg prometheus.Registerer) *Metrics {
	bytesRead := prometheus.NewCounter(prometheus.CounterOpts{
		Name: "kafka_otlp_forwarder_bytes_read_total",
		Help: "Total bytes read from Kafka",
	})

	bytesSent := prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "kafka_otlp_forwarder_bytes_sent_total",
		Help: "Total bytes sent to OTLP endpoints",
	}, []string{"endpoint"})

	kafkaOffset := prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "kafka_otlp_forwarder_kafka_offset",
		Help: "Current Kafka offset per partition",
	}, []string{"partition"})

	reg.MustRegister(bytesRead, bytesSent, kafkaOffset)

	return &Metrics{
		BytesRead:   bytesRead,
		BytesSent:   bytesSent,
		KafkaOffset: kafkaOffset,
	}
}
