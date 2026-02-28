package kafkaotlpforwarder

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNewConsumerConfig_Defaults(t *testing.T) {
	cfg := &ConsumerConfig{
		Brokers:       []string{"localhost:9092"},
		Topic:         "tempo-ingest",
		ConsumerGroup: "test-group",
		FromBeginning: false,
	}

	require.Equal(t, []string{"localhost:9092"}, cfg.Brokers)
	require.Equal(t, "tempo-ingest", cfg.Topic)
	require.Equal(t, "test-group", cfg.ConsumerGroup)
	require.False(t, cfg.FromBeginning)
}

func TestNewConsumerConfig_Validation(t *testing.T) {
	tests := []struct {
		name        string
		cfg         *ConsumerConfig
		expectError bool
		errorMsg    string
	}{
		{
			name: "valid config",
			cfg: &ConsumerConfig{
				Brokers:       []string{"localhost:9092"},
				Topic:         "tempo-ingest",
				ConsumerGroup: "test-group",
			},
			expectError: false,
		},
		{
			name: "missing brokers",
			cfg: &ConsumerConfig{
				Brokers:       []string{},
				Topic:         "tempo-ingest",
				ConsumerGroup: "test-group",
			},
			expectError: true,
			errorMsg:    "brokers",
		},
		{
			name: "missing topic",
			cfg: &ConsumerConfig{
				Brokers:       []string{"localhost:9092"},
				Topic:         "",
				ConsumerGroup: "test-group",
			},
			expectError: true,
			errorMsg:    "topic",
		},
		{
			name: "missing consumer group",
			cfg: &ConsumerConfig{
				Brokers:       []string{"localhost:9092"},
				Topic:         "tempo-ingest",
				ConsumerGroup: "",
			},
			expectError: true,
			errorMsg:    "consumer group",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.cfg.Validate()
			if tt.expectError {
				require.Error(t, err)
				require.Contains(t, err.Error(), tt.errorMsg)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestConsumerConfig_BrokersFromString(t *testing.T) {
	cfg := &ConsumerConfig{}
	cfg.SetBrokersFromString("broker1:9092,broker2:9092,broker3:9092")

	require.Len(t, cfg.Brokers, 3)
	require.Equal(t, "broker1:9092", cfg.Brokers[0])
	require.Equal(t, "broker2:9092", cfg.Brokers[1])
	require.Equal(t, "broker3:9092", cfg.Brokers[2])
}

func TestConsumerConfig_BrokersFromString_SingleBroker(t *testing.T) {
	cfg := &ConsumerConfig{}
	cfg.SetBrokersFromString("localhost:9092")

	require.Len(t, cfg.Brokers, 1)
	require.Equal(t, "localhost:9092", cfg.Brokers[0])
}

func TestConsumerConfig_BrokersFromString_WithSpaces(t *testing.T) {
	cfg := &ConsumerConfig{}
	cfg.SetBrokersFromString(" broker1:9092 , broker2:9092 , broker3:9092 ")

	require.Len(t, cfg.Brokers, 3)
	require.Equal(t, "broker1:9092", cfg.Brokers[0])
	require.Equal(t, "broker2:9092", cfg.Brokers[1])
	require.Equal(t, "broker3:9092", cfg.Brokers[2])
}
