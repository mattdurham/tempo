package vblockpack

// metrics.go — Prometheus instrumentation for VI usage-recording/backfill-triggering and
// cube backfill (2026-07-11). Neither had any metrics before this: the only observability
// into "did a use get recorded / did a trigger fire / did a backfill run" was plain log
// lines, which made a live "are we creating indexes?" question require manual log
// archaeology across every component. These counters give a directly queryable answer.

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

var (
	// metricViUsageRecorded counts every RecordUse call that actually reaches the
	// registry (i.e. a query against a non-dedicated, currently-uncovered column) —
	// incremented once, centrally, in realUsageRecorder.RecordUse, regardless of which
	// of the 5 call sites (4 querier-side, 1 frontend plan-time) triggered it.
	metricViUsageRecorded = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: "tempodb",
		Subsystem: "vblockpack",
		Name:      "vi_usage_recorded_total",
		Help:      "Total number of times a query against a non-dedicated, currently-uncovered column was recorded as a usage signal for VI backfill triggering.",
	})
	// metricViBackfillTriggered counts every time a column's usage crossed the trigger
	// threshold and won the backfill lease (RecordUse's ShouldBackfill == true) —
	// incremented regardless of whether onShouldBackfill is nil, since winning the lease
	// is itself the meaningful signal even if this specific process doesn't launch the
	// goroutine.
	metricViBackfillTriggered = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: "tempodb",
		Subsystem: "vblockpack",
		Name:      "vi_backfill_triggered_total",
		Help:      "Total number of times a column's usage crossed the trigger threshold and won the backfill lease.",
	})
	// metricViBackfillStarted counts every launchViBackfill/RunViBackfill invocation,
	// covering both the async querier/frontend-triggered path and the backend-worker
	// job-queue-dispatched path.
	metricViBackfillStarted = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: "tempodb",
		Subsystem: "vblockpack",
		Name:      "vi_backfill_started_total",
		Help:      "Total number of VI column backfill runs started.",
	})
	// metricViBackfillCompleted counts every backfill run that reached prog.Done == true
	// (the full historical window was successfully covered).
	metricViBackfillCompleted = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: "tempodb",
		Subsystem: "vblockpack",
		Name:      "vi_backfill_completed_total",
		Help:      "Total number of VI column backfill runs that completed successfully.",
	})
	// metricViBackfillFailed counts every backfill run that ended in a real error
	// (registry persist failure or a BackfillEngine.Run error), excluding routine
	// context cancellation.
	metricViBackfillFailed = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: "tempodb",
		Subsystem: "vblockpack",
		Name:      "vi_backfill_failed_total",
		Help:      "Total number of VI column backfill runs that ended in error (excluding routine context cancellation).",
	})

	// metricCubeBackfillStarted mirrors metricViBackfillStarted for cube backfill runs
	// (launchBackfill's async path and RunCubeBackfill's job-queue path).
	metricCubeBackfillStarted = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: "tempodb",
		Subsystem: "vblockpack",
		Name:      "cube_backfill_started_total",
		Help:      "Total number of cube backfill runs started.",
	})
	// metricCubeBackfillCompleted mirrors metricViBackfillCompleted for cube backfill.
	metricCubeBackfillCompleted = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: "tempodb",
		Subsystem: "vblockpack",
		Name:      "cube_backfill_completed_total",
		Help:      "Total number of cube backfill runs that completed successfully.",
	})
	// metricCubeBackfillFailed mirrors metricViBackfillFailed for cube backfill.
	metricCubeBackfillFailed = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: "tempodb",
		Subsystem: "vblockpack",
		Name:      "cube_backfill_failed_total",
		Help:      "Total number of cube backfill runs that ended in error (excluding routine context cancellation).",
	})
)

// MetricViBackfillTriggeredForTest and MetricViBackfillStartedForTest are TEST-ONLY
// accessors for metricViBackfillTriggered/metricViBackfillStarted, exported so tests in
// OTHER packages (e.g. modules/frontend's local-backend integration test) can observe
// these package-level counters directly, mirroring how in-package tests
// (vi_usage_hook_test.go/vi_backfill_test.go) already read them via
// testutil.ToFloat64(metricViBackfillTriggered) — not meant for production use.
func MetricViBackfillTriggeredForTest() float64 { return testutil.ToFloat64(metricViBackfillTriggered) }

func MetricViBackfillStartedForTest() float64 { return testutil.ToFloat64(metricViBackfillStarted) }

// MetricViBackfillCompletedForTest and MetricViBackfillFailedForTest are the terminal-state
// counterparts to MetricViBackfillStartedForTest -- tests driving the real, async
// launchViBackfill goroutine (e.g. modules/frontend's local-backend integration test) should
// wait for one of these to increment before returning, so the background goroutine has
// finished all its file I/O before t.TempDir()'s cleanup removes the directory out from under
// it (a genuine async race, not a flake to paper over with a longer sleep).
func MetricViBackfillCompletedForTest() float64 { return testutil.ToFloat64(metricViBackfillCompleted) }

func MetricViBackfillFailedForTest() float64 { return testutil.ToFloat64(metricViBackfillFailed) }
