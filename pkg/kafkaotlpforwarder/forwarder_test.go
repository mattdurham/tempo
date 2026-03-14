package kafkaotlpforwarder

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestForwarder_Validation(t *testing.T) {
	tests := []struct {
		name        string
		cfg         *ForwarderConfig
		expectError bool
		errorMsg    string
	}{
		{
			name: "valid config",
			cfg: &ForwarderConfig{
				ConsumerConfig: &ConsumerConfig{
					Brokers:       []string{"localhost:9092"},
					Topic:         "tempo-ingest",
					ConsumerGroup: "test-group",
				},
				Endpoints:        []string{"localhost:4317"},
				ErrorMode:        ErrorModeFailFast,
				BatchWaitTimeout: 30 * time.Second,
			},
			expectError: false,
		},
		{
			name: "missing consumer config",
			cfg: &ForwarderConfig{
				Endpoints:        []string{"localhost:4317"},
				ErrorMode:        ErrorModeFailFast,
				BatchWaitTimeout: 30 * time.Second,
			},
			expectError: true,
			errorMsg:    "consumer config",
		},
		{
			name: "missing endpoints",
			cfg: &ForwarderConfig{
				ConsumerConfig: &ConsumerConfig{
					Brokers:       []string{"localhost:9092"},
					Topic:         "tempo-ingest",
					ConsumerGroup: "test-group",
				},
				Endpoints:        []string{},
				ErrorMode:        ErrorModeFailFast,
				BatchWaitTimeout: 30 * time.Second,
			},
			expectError: true,
			errorMsg:    "endpoints",
		},
		{
			name: "invalid error mode",
			cfg: &ForwarderConfig{
				ConsumerConfig: &ConsumerConfig{
					Brokers:       []string{"localhost:9092"},
					Topic:         "tempo-ingest",
					ConsumerGroup: "test-group",
				},
				Endpoints:        []string{"localhost:4317"},
				ErrorMode:        "invalid-mode",
				BatchWaitTimeout: 30 * time.Second,
			},
			expectError: true,
			errorMsg:    "error mode",
		},
		{
			name: "zero timeout",
			cfg: &ForwarderConfig{
				ConsumerConfig: &ConsumerConfig{
					Brokers:       []string{"localhost:9092"},
					Topic:         "tempo-ingest",
					ConsumerGroup: "test-group",
				},
				Endpoints:        []string{"localhost:4317"},
				ErrorMode:        ErrorModeFailFast,
				BatchWaitTimeout: 0,
			},
			expectError: true,
			errorMsg:    "timeout",
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

func TestForwarderConfig_ParseErrorMode(t *testing.T) {
	tests := []struct {
		input    string
		expected ErrorMode
		isValid  bool
	}{
		{"fail-fast", ErrorModeFailFast, true},
		{"best-effort", ErrorModeBestEffort, true},
		{"all-or-nothing", ErrorModeAllOrNothing, true},
		{"invalid", "", false},
		{"", "", false},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			mode, valid := ParseErrorMode(tt.input)
			require.Equal(t, tt.isValid, valid)
			if valid {
				require.Equal(t, tt.expected, mode)
			}
		})
	}
}
