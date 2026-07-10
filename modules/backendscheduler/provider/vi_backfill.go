package provider

// vi_backfill.go — #496 B2: VI usage-triggered column backfill job provider
// for the backend scheduler, mirroring cubebackfill.go's CubeBackfillProvider
// (poll loop, HasJobsForTenant de-dup check, job emission) PLUS the R8 lease
// check CubeBackfillProvider has no equivalent of: a registry entry whose
// BackfillInProgress lease is still unexpired is skipped (another
// replica/job instance already owns it); once the lease expires (a crashed
// worker never released it), the NEXT poll cycle re-emits a fresh job --
// self-heals without manual intervention.

import (
	"bytes"
	"context"
	"io"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/google/uuid"
	blockpack "github.com/grafana/blockpack"
	"github.com/grafana/tempo/modules/backendscheduler/work"
	"github.com/grafana/tempo/pkg/tempopb"
	s3backend "github.com/grafana/tempo/tempodb/backend/s3"
	minio "github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

// ViBackfillConfig configures the VI usage-triggered backfill provider.
type ViBackfillConfig struct {
	PollInterval time.Duration `yaml:"poll_interval"`
	Tenants      []string      `yaml:"tenants"`
}

func (c *ViBackfillConfig) setDefaults() {
	if c.PollInterval <= 0 {
		c.PollInterval = 10 * time.Minute
	}
}

// ViBackfillProvider emits backfill jobs for registry entries whose R4
// repeated-use threshold has been crossed (Triggered=true) and whose backfill
// is not yet complete (Done=false).
type ViBackfillProvider struct {
	cfg    ViBackfillConfig
	client *minio.Client
	bucket string
	sched  Scheduler
	logger log.Logger
}

// NewViBackfillProvider creates a ViBackfillProvider.
func NewViBackfillProvider(
	cfg ViBackfillConfig,
	s3cfg *s3backend.Config,
	sched Scheduler,
	logger log.Logger,
) (*ViBackfillProvider, error) {
	cfg.setDefaults()
	endpoint := s3cfg.Endpoint
	if endpoint == "" {
		endpoint = "s3." + s3cfg.Region + ".amazonaws.com"
	}
	client, err := minio.New(endpoint, &minio.Options{
		Creds:  credentials.NewEnvAWS(),
		Secure: !s3cfg.Insecure,
		Region: s3cfg.Region,
	})
	if err != nil {
		return nil, err
	}
	return &ViBackfillProvider{
		cfg:    cfg,
		client: client,
		bucket: s3cfg.Bucket,
		sched:  sched,
		logger: logger,
	}, nil
}

// Start begins emitting backfill jobs. Implements provider.Provider.
func (p *ViBackfillProvider) Start(ctx context.Context) <-chan *work.Job {
	ch := make(chan *work.Job, 32)
	go func() {
		defer close(ch)
		ticker := time.NewTicker(p.cfg.PollInterval)
		defer ticker.Stop()
		p.poll(ctx, ch)
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				p.poll(ctx, ch)
			}
		}
	}()
	return ch
}

func (p *ViBackfillProvider) poll(ctx context.Context, ch chan<- *work.Job) {
	os := &viBackfillRegistryStore{client: p.client, bucket: p.bucket}
	nowSec := uint64(time.Now().Unix()) //nolint:gosec // unix seconds fits uint64 for any realistic timestamp

	for _, tenant := range p.cfg.Tenants {
		registry := blockpack.NewRegistry(os, tenant)
		entries, _, err := registry.Load(ctx)
		if err != nil {
			level.Warn(p.logger).Log("msg", "vi backfill provider: load registry", "tenant", tenant, "err", err)
			continue
		}
		for _, entry := range entries {
			p.emitIfNeeded(ctx, ch, tenant, entry, nowSec)
		}
	}
}

// emitIfNeeded emits one job for entry if it needs backfilling and is not
// already being worked on. Split out from poll for per-entry testability.
func (p *ViBackfillProvider) emitIfNeeded(
	ctx context.Context,
	ch chan<- *work.Job,
	tenant string,
	entry blockpack.Entry,
	nowSec uint64,
) {
	if !entry.Backfill.Triggered || entry.Backfill.Done {
		return // never triggered, or already fully backfilled
	}
	if p.sched.HasJobsForTenant(tenant, tempopb.JobType_JOB_TYPE_VI_BACKFILL) {
		return // a VI backfill job is already in flight for this tenant
	}
	// R8: a still-unexpired lease means another replica/job instance already
	// owns this entry's backfill -- skip. Once the lease expires (a crashed
	// worker never released it), this condition is false and the next poll
	// cycle re-emits a fresh job (self-heal, no manual intervention).
	if entry.Backfill.BackfillInProgress && entry.Backfill.LeaseExpiresAt > nowSec {
		return
	}

	job := &work.Job{
		ID:   uuid.New().String(),
		Type: tempopb.JobType_JOB_TYPE_VI_BACKFILL,
		JobDetail: tempopb.JobDetail{
			Tenant: tenant,
			ViBackfill: &tempopb.ViBackfillDetail{
				ColumnHash: entry.ColumnHash,
				ColumnName: entry.ColumnName,
				ColumnType: entry.ColumnType,
			},
		},
	}
	p.sched.RegisterJob(job)
	select {
	case ch <- job:
	case <-ctx.Done():
		return
	}
	level.Info(p.logger).Log(
		"msg", "vi backfill job scheduled",
		"tenant", tenant, "column", entry.ColumnName, "job_id", job.ID,
	)
}

// viBackfillRegistryStore implements blockpack.ObjectStore over minio for
// registry reads, mirroring cubeRegistryStore's identical shape but
// returning blockpack.ErrConflict (not blockpack.CubeErrConflict) on a 412 --
// see vi_backfill.go's viUsageObjectStore doc comment for why this
// distinction matters to the conditional-PUT retry loop.
type viBackfillRegistryStore struct {
	client *minio.Client
	bucket string
}

func (s *viBackfillRegistryStore) Get(ctx context.Context, key string) ([]byte, string, error) {
	obj, err := s.client.GetObject(ctx, s.bucket, key, minio.GetObjectOptions{})
	if err != nil {
		resp := minio.ToErrorResponse(err)
		if resp.Code == "NoSuchKey" || resp.StatusCode == 404 {
			return nil, "", nil
		}
		return nil, "", err
	}
	defer func() { _ = obj.Close() }()
	data, err := io.ReadAll(obj)
	if err != nil {
		return nil, "", err
	}
	info, statErr := s.client.StatObject(ctx, s.bucket, key, minio.StatObjectOptions{})
	if statErr != nil {
		return data, "", nil
	}
	return data, info.ETag, nil
}

func (s *viBackfillRegistryStore) ConditionalPut(ctx context.Context, key string, data []byte, etag string) error {
	opts := minio.PutObjectOptions{ContentType: "application/json"}
	if etag != "" {
		opts.SetMatchETag(etag)
	}
	_, err := s.client.PutObject(ctx, s.bucket, key, bytes.NewReader(data), int64(len(data)), opts)
	if err != nil {
		resp := minio.ToErrorResponse(err)
		if resp.StatusCode == 412 {
			return blockpack.ErrConflict
		}
		return err
	}
	return nil
}
