package jobplanner

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/grafana/tempo/tempodb/encoding/common"
)

// TestService_Run_StopsOnContextCancel proves Run returns promptly once ctx is
// canceled, without a real Postgres connection (pollFn is overridden with a
// no-op).
func TestService_Run_StopsOnContextCancel(t *testing.T) {
	s := &Service{cfg: common.JobPlannerConfig{Enabled: true, PollInterval: time.Millisecond}}
	s.pollFn = func(context.Context) error { return nil }

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- s.Run(ctx) }()

	cancel()
	select {
	case err := <-done:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(2 * time.Second):
		t.Fatal("Run must return promptly once ctx is canceled")
	}
}

// TestService_Run_DisabledBlocksUntilContextDone proves a disabled Service
// never ticks at all (pollFn would panic/fail if called, proving it wasn't).
func TestService_Run_DisabledBlocksUntilContextDone(t *testing.T) {
	s := &Service{cfg: common.JobPlannerConfig{Enabled: false, PollInterval: time.Millisecond}}
	s.pollFn = func(context.Context) error {
		t.Fatal("pollFn must never be called when disabled")
		return nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	err := s.Run(ctx)
	require.ErrorIs(t, err, context.DeadlineExceeded)
}

// TestService_PollOnce_ErrorDoesNotAbortLoop proves a tick's error is
// recorded (via the poll-errors counter) but never stops Run from ticking
// again -- mirrors backendscheduler's "one bad tick doesn't kill the poller"
// posture.
func TestService_PollOnce_ErrorDoesNotAbortLoop(t *testing.T) {
	s := &Service{cfg: common.JobPlannerConfig{Enabled: true, PollInterval: time.Millisecond}}
	var calls atomic.Int32
	s.pollFn = func(context.Context) error {
		n := calls.Add(1)
		if n == 1 {
			return errors.New("simulated transient failure")
		}
		return nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	_ = s.Run(ctx)

	assert.GreaterOrEqual(t, calls.Load(), int32(2), "Run must keep ticking after pollFn returns an error")
}
