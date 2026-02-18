package kafkaotlpforwarder

import (
	"context"
	"fmt"
	"sync"
	"time"

	v1_trace "github.com/grafana/tempo/pkg/tempopb/trace/v1"
)

// ErrorMode defines how to handle endpoint failures.
type ErrorMode string

const (
	ErrorModeFailFast     ErrorMode = "fail-fast"
	ErrorModeBestEffort   ErrorMode = "best-effort"
	ErrorModeAllOrNothing ErrorMode = "all-or-nothing"
)

// OTLPClient defines the interface for sending OTLP data.
type OTLPClient interface {
	Send(ctx context.Context, data []byte) error
}

// Sender handles sending OTLP data to multiple endpoints.
type Sender struct {
	clients   []OTLPClient
	errorMode ErrorMode
	metrics   *Metrics
	timeout   time.Duration
}

// NewSender creates a new Sender instance.
func NewSender(clients []OTLPClient, errorMode ErrorMode, metrics *Metrics, timeout time.Duration) (*Sender, error) {
	if len(clients) == 0 {
		return nil, fmt.Errorf("no endpoints configured")
	}

	return &Sender{
		clients:   clients,
		errorMode: errorMode,
		metrics:   metrics,
		timeout:   timeout,
	}, nil
}

// SendBatch sends a batch of traces to all configured endpoints.
func (s *Sender) SendBatch(ctx context.Context, tracesData *v1_trace.TracesData) error {
	if len(s.clients) == 0 {
		return fmt.Errorf("no endpoints configured")
	}

	// Create context with timeout
	ctx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()

	// Marshal once for all endpoints
	data, err := tracesData.Marshal()
	if err != nil {
		return fmt.Errorf("marshal traces: %w", err)
	}

	var wg sync.WaitGroup
	errChan := make(chan error, len(s.clients))

	for i, client := range s.clients {
		wg.Add(1)
		go func(idx int, c OTLPClient) {
			defer wg.Done()

			if err := c.Send(ctx, data); err != nil {
				errChan <- fmt.Errorf("endpoint %d: %w", idx, err)
			}
		}(i, client)
	}

	wg.Wait()
	close(errChan)

	return s.handleErrors(errChan, len(data))
}

func (s *Sender) handleErrors(errChan <-chan error, dataSize int) error {
	var errors []error
	for err := range errChan {
		errors = append(errors, err)
	}

	switch s.errorMode {
	case ErrorModeFailFast:
		if len(errors) > 0 {
			return errors[0]
		}
	case ErrorModeBestEffort:
		// Log errors but continue - no error returned
		// Metrics will track failures
		return nil
	case ErrorModeAllOrNothing:
		if len(errors) > 0 {
			return fmt.Errorf("all-or-nothing failed: %d endpoints errored", len(errors))
		}
	}

	return nil
}
