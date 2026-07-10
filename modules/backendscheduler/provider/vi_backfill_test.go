package provider

// vi_backfill_test.go — #496 B2 tests for ViBackfillProvider's emit
// decision. Uses the real work.New(...) Scheduler (the same pattern
// redaction_test.go/compaction_test.go use), so this exercises the real
// HasJobsForTenant/RegisterJob contract, not a hand-rolled fake -- only
// emitIfNeeded's pure decision logic is under test here (no minio/S3
// dependency), since that is where R4/R8's decline-reason-adjacent
// correctness lives.

import (
	"context"
	"flag"
	"os"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	blockpack "github.com/grafana/blockpack"
	"github.com/grafana/tempo/modules/backendscheduler/work"
	"github.com/grafana/tempo/pkg/tempopb"
)

func newTestViBackfillProvider(t *testing.T) (*ViBackfillProvider, work.Interface) {
	t.Helper()
	workCfg := work.Config{}
	workCfg.RegisterFlagsAndApplyDefaults("", &flag.FlagSet{})
	w := work.New(workCfg)
	return &ViBackfillProvider{
		sched:  w,
		logger: log.NewLogfmtLogger(os.Stderr),
	}, w
}

func triggeredEntry(colName string) blockpack.Entry {
	return blockpack.Entry{
		Tenant:     "tenant-a",
		ColumnHash: "hash-" + colName,
		ColumnName: colName,
		ColumnType: "string",
		Backfill:   blockpack.BackfillState{Triggered: true, Done: false},
	}
}

func TestViBackfillProvider_EmitsJobForTriggeredEntry(t *testing.T) {
	p, _ := newTestViBackfillProvider(t)
	ch := make(chan *work.Job, 1)
	entry := triggeredEntry("span.custom.attr")

	p.emitIfNeeded(context.Background(), ch, "tenant-a", entry, uint64(time.Now().Unix())) //nolint:gosec

	select {
	case job := <-ch:
		require.NotNil(t, job)
		assert.Equal(t, tempopb.JobType_JOB_TYPE_VI_BACKFILL, job.Type)
		require.NotNil(t, job.JobDetail.ViBackfill)
		assert.Equal(t, entry.ColumnName, job.JobDetail.ViBackfill.ColumnName)
	default:
		t.Fatal("expected a job to be emitted for a triggered, incomplete entry")
	}
}

func TestViBackfillProvider_NeverTriggeredEmitsNoJob(t *testing.T) {
	p, _ := newTestViBackfillProvider(t)
	ch := make(chan *work.Job, 1)
	entry := blockpack.Entry{Tenant: "tenant-a", ColumnName: "span.x", Backfill: blockpack.BackfillState{Triggered: false}}

	p.emitIfNeeded(context.Background(), ch, "tenant-a", entry, uint64(time.Now().Unix())) //nolint:gosec

	select {
	case job := <-ch:
		t.Fatalf("expected no job for a never-triggered entry, got %+v", job)
	default:
	}
}

func TestViBackfillProvider_AlreadyDoneEmitsNoJob(t *testing.T) {
	p, _ := newTestViBackfillProvider(t)
	ch := make(chan *work.Job, 1)
	entry := triggeredEntry("span.x")
	entry.Backfill.Done = true

	p.emitIfNeeded(context.Background(), ch, "tenant-a", entry, uint64(time.Now().Unix())) //nolint:gosec

	select {
	case job := <-ch:
		t.Fatalf("expected no job for an already-Done entry, got %+v", job)
	default:
	}
}

// TestViBackfillProvider_SkipsEntryWithUnexpiredLease is R8's dedup
// requirement: a second poll cycle must not re-emit a job for an entry whose
// lease is still held and unexpired.
func TestViBackfillProvider_SkipsEntryWithUnexpiredLease(t *testing.T) {
	p, _ := newTestViBackfillProvider(t)
	ch := make(chan *work.Job, 1)
	entry := triggeredEntry("span.x")
	nowSec := uint64(time.Now().Unix()) //nolint:gosec
	entry.Backfill.BackfillInProgress = true
	entry.Backfill.LeaseExpiresAt = nowSec + 1800 // 30 min in the future

	p.emitIfNeeded(context.Background(), ch, "tenant-a", entry, nowSec)

	select {
	case job := <-ch:
		t.Fatalf("expected no job while the lease is unexpired, got %+v", job)
	default:
	}
}

// TestViBackfillProvider_ReEmitsAfterLeaseExpiry is R8's self-heal
// requirement: once a lease has expired (a crashed worker never released
// it), the next poll cycle must emit a fresh job.
func TestViBackfillProvider_ReEmitsAfterLeaseExpiry(t *testing.T) {
	p, _ := newTestViBackfillProvider(t)
	ch := make(chan *work.Job, 1)
	entry := triggeredEntry("span.x")
	nowSec := uint64(time.Now().Unix()) //nolint:gosec
	entry.Backfill.BackfillInProgress = true
	entry.Backfill.LeaseExpiresAt = nowSec - 10 // expired 10s ago

	p.emitIfNeeded(context.Background(), ch, "tenant-a", entry, nowSec)

	select {
	case job := <-ch:
		require.NotNil(t, job)
		assert.Equal(t, tempopb.JobType_JOB_TYPE_VI_BACKFILL, job.Type)
	default:
		t.Fatal("expected a fresh job after the lease expired (self-heal, no manual intervention)")
	}
}

// TestViBackfillProvider_SkipsWhenTenantAlreadyHasAJob mirrors
// CubeBackfillProvider's own HasJobsForTenant de-dup check.
func TestViBackfillProvider_SkipsWhenTenantAlreadyHasAJob(t *testing.T) {
	p, w := newTestViBackfillProvider(t)
	existing := &work.Job{
		ID:        "existing",
		Type:      tempopb.JobType_JOB_TYPE_VI_BACKFILL,
		JobDetail: tempopb.JobDetail{Tenant: "tenant-a"},
	}
	w.RegisterJob(existing)

	ch := make(chan *work.Job, 1)
	entry := triggeredEntry("span.x")

	p.emitIfNeeded(context.Background(), ch, "tenant-a", entry, uint64(time.Now().Unix())) //nolint:gosec

	select {
	case job := <-ch:
		t.Fatalf("expected no job while the tenant already has one in flight, got %+v", job)
	default:
	}
}
