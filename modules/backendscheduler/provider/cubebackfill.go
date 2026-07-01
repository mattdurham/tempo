package provider

// cubebackfill.go — cube backfill job provider for the backend scheduler.
//
// CubeBackfillProvider loads the cube registry on each poll cycle and emits
// one JOB_TYPE_CUBE_BACKFILL job per registered cube that lacks backfill coverage.
// The backend-worker executes these jobs by running the Backfiller.

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

// CubeBackfillConfig configures the cube backfill provider.
type CubeBackfillConfig struct {
	PollInterval  time.Duration `yaml:"poll_interval"`
	WindowMinutes uint32        `yaml:"window_minutes"`
	Tenants       []string      `yaml:"tenants"`
}

func (c *CubeBackfillConfig) setDefaults() {
	if c.PollInterval <= 0 {
		c.PollInterval = 10 * time.Minute
	}
	if c.WindowMinutes == 0 {
		c.WindowMinutes = 60 * 24 * 7
	}
}

// CubeBackfillProvider emits backfill jobs for registered cubes.
type CubeBackfillProvider struct {
	cfg    CubeBackfillConfig
	client *minio.Client
	bucket string
	sched  Scheduler
	logger log.Logger
}

// NewCubeBackfillProvider creates a CubeBackfillProvider.
func NewCubeBackfillProvider(cfg CubeBackfillConfig, s3cfg *s3backend.Config, sched Scheduler, logger log.Logger) (*CubeBackfillProvider, error) {
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
	return &CubeBackfillProvider{
		cfg:    cfg,
		client: client,
		bucket: s3cfg.Bucket,
		sched:  sched,
		logger: logger,
	}, nil
}

// Start begins emitting backfill jobs. Implements provider.Provider.
func (p *CubeBackfillProvider) Start(ctx context.Context) <-chan *work.Job {
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

func (p *CubeBackfillProvider) poll(ctx context.Context, ch chan<- *work.Job) {
	os := &cubeRegistryStore{client: p.client, bucket: p.bucket}

	for _, tenant := range p.cfg.Tenants {
		reg := blockpack.NewCubeRegistry(os, tenant)
		entries, _, err := reg.Load(ctx)
		if err != nil {
			level.Warn(p.logger).Log("msg", "cube backfill provider: load registry", "tenant", tenant, "err", err)
			continue
		}
		for _, entry := range entries {
			if p.sched.HasJobsForTenant(tenant, tempopb.JobType_JOB_TYPE_CUBE_BACKFILL) {
				continue // already a backfill job running for this tenant
			}
			job := &work.Job{
				ID:   uuid.New().String(),
				Type: tempopb.JobType_JOB_TYPE_CUBE_BACKFILL,
				JobDetail: tempopb.JobDetail{
					Tenant: tenant,
					CubeBackfill: &tempopb.CubeBackfillDetail{
						CubeID:        entry.CubeID,
						WindowMinutes: p.cfg.WindowMinutes,
					},
				},
			}
			p.sched.RegisterJob(job)
			select {
			case ch <- job:
			case <-ctx.Done():
				return
			}
			level.Info(p.logger).Log("msg", "cube backfill job scheduled",
				"tenant", tenant, "cube_id", entry.CubeID, "job_id", job.ID)
		}
	}
}

// cubeRegistryStore implements blockpack.CubeObjectStore over minio for registry reads.
type cubeRegistryStore struct {
	client *minio.Client
	bucket string
}

func (s *cubeRegistryStore) Get(ctx context.Context, key string) ([]byte, string, error) {
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
	info, err := s.client.StatObject(ctx, s.bucket, key, minio.StatObjectOptions{})
	if err != nil {
		return data, "", nil
	}
	return data, info.ETag, nil
}

func (s *cubeRegistryStore) ConditionalPut(ctx context.Context, key string, data []byte, etag string) error {
	opts := minio.PutObjectOptions{ContentType: "application/json"}
	if etag != "" {
		opts.SetMatchETag(etag)
	}
	_, err := s.client.PutObject(ctx, s.bucket, key, bytes.NewReader(data), int64(len(data)), opts)
	if err != nil {
		resp := minio.ToErrorResponse(err)
		if resp.StatusCode == 412 {
			return blockpack.CubeErrConflict
		}
		return err
	}
	return nil
}
