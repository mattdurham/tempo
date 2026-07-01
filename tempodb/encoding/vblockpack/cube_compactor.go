package vblockpack

// cube_compactor.go — periodic cube compaction service.
//
// Runs inside the value-index-compactor target alongside the VI index compactor.
// Every CubeCompactorInterval it merges many 1-minute L0 cube files into:
//   - merged L0 files spanning a full hour (PlanCubeL0Merge)
//   - L1 rollup files at 60-minute granularity (PlanCubeL1Rollup)

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"path"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/go-kit/log/level"
	blockpack "github.com/grafana/blockpack"
	util_log "github.com/grafana/tempo/pkg/util/log"
	minio "github.com/minio/minio-go/v7"
)

// CubeCompactorService drives periodic cube compaction for a set of tenants.
type CubeCompactorService struct {
	client          *minio.Client
	bucket          string
	tenants         []string
	compactInterval time.Duration
}

// NewCubeCompactorService creates a CubeCompactorService.
func NewCubeCompactorService(client *minio.Client, bucket string, tenants []string, interval time.Duration) *CubeCompactorService {
	if interval <= 0 {
		interval = 5 * time.Minute
	}
	level.Info(util_log.Logger).Log("msg", "vblockpack: cube compactor configured",
		"tenants", strings.Join(tenants, ","), "interval", interval)
	return &CubeCompactorService{
		client:          client,
		bucket:          bucket,
		tenants:         tenants,
		compactInterval: interval,
	}
}

// Run drives the compaction loop until ctx is done.
func (svc *CubeCompactorService) Run(ctx context.Context) {
	ticker := time.NewTicker(svc.compactInterval)
	defer ticker.Stop()
	svc.runOnce(ctx)
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			svc.runOnce(ctx)
		}
	}
}

func (svc *CubeCompactorService) runOnce(ctx context.Context) {
	level.Debug(util_log.Logger).Log("msg", "vblockpack: cube compaction pass starting")
	for _, tenant := range svc.tenants {
		if err := svc.compactTenant(ctx, tenant); err != nil {
			level.Warn(util_log.Logger).Log("msg", "vblockpack: cube compaction error",
				"tenant", tenant, "err", err)
		}
	}
}

func (svc *CubeCompactorService) compactTenant(ctx context.Context, tenant string) error {
	level.Info(util_log.Logger).Log("msg", "vblockpack: cube compaction tenant pass", "tenant", tenant)
	os := &minioObjectStore{client: svc.client, bucket: svc.bucket}
	reg := blockpack.NewCubeRegistry(os, tenant)
	entries, _, err := reg.Load(ctx)
	if err != nil {
		return fmt.Errorf("load registry: %w", err)
	}
	if len(entries) == 0 {
		return nil
	}

	store := &cubeFileStore{client: svc.client, bucket: svc.bucket}
	compactor := blockpack.NewCubeCompactor(store, reg, blockpack.CubeCompactorConfig{
		L0MergeThreshold: 10,
		L1MergeThreshold: 24,
	})

	for _, entry := range entries {
		if err := svc.compactCube(ctx, tenant, entry, store, compactor); err != nil {
			level.Warn(util_log.Logger).Log("msg", "vblockpack: cube compaction error",
				"tenant", tenant, "cube_id", entry.CubeID, "err", err)
		}
	}
	return nil
}

func (svc *CubeCompactorService) compactCube(
	ctx context.Context,
	tenant string,
	entry blockpack.CubeRegistryEntry,
	store blockpack.CubeFileStore,
	compactor *blockpack.CubeCompactor,
) error {
	files, err := store.List(ctx, tenant, entry.CubeID)
	if err != nil {
		return fmt.Errorf("list: %w", err)
	}
	if len(files) == 0 {
		return nil
	}

	id, idErr := blockpack.CubeIDFromHex(entry.CubeID)
	if idErr != nil {
		return fmt.Errorf("parse id: %w", idErr)
	}

	// L0 merge: group files by hour, merge when ≥10 cover the same hour.
	l0Plans := blockpack.PlanCubeL0Merge(files, 10, tenant, entry.CubeID)
	merged := 0
	for _, plan := range l0Plans {
		if exErr := compactor.Execute(ctx, id, plan); exErr != nil {
			level.Warn(util_log.Logger).Log("msg", "vblockpack: L0 merge failed",
				"cube_id", entry.CubeID, "err", exErr)
			continue
		}
		merged += len(plan.InputKeys)
	}

	// Re-list after merges for L1 rollup.
	if len(l0Plans) > 0 {
		if files, err = store.List(ctx, tenant, entry.CubeID); err != nil {
			return nil
		}
	}

	// L1 rollup: for each hour that has L0 files, roll up to one L1 file.
	hours := make(map[uint32]bool)
	for _, f := range files {
		if f.Level == 1 {
			hours[f.MinMinute/60] = true
		}
	}
	rolledUp := 0
	for h := range hours {
		keys, ok := blockpack.PlanCubeL1Rollup(files, h*60, tenant, entry.CubeID)
		if !ok || len(keys) < 2 {
			continue
		}
		outKey := fmt.Sprintf("%s/cubes/%s/L1-%d-%d-%s.cube",
			tenant, entry.CubeID, h*60, h*60+59, blockpack.VCNTNewID())
		plan := blockpack.CubeCompactionPlan{
			InputKeys: keys, OutputKey: outKey,
			Level: 60, MinMinute: h * 60, MaxMinute: h*60 + 59,
		}
		if exErr := compactor.Execute(ctx, id, plan); exErr != nil {
			level.Warn(util_log.Logger).Log("msg", "vblockpack: L1 rollup failed",
				"cube_id", entry.CubeID, "err", exErr)
			continue
		}
		rolledUp++
	}

	if merged > 0 || rolledUp > 0 {
		level.Info(util_log.Logger).Log("msg", "vblockpack: cube compaction done",
			"tenant", tenant, "cube_id", entry.CubeID,
			"l0_merged", merged, "l1_rollups", rolledUp)
	}
	return nil
}

// cubeFileStore implements blockpack.CubeFileStore over minio.
type cubeFileStore struct {
	client *minio.Client
	bucket string
}

// cubeFileRe matches any .cube file (with or without embedded time range).
var cubeFileRe = regexp.MustCompile(`\.cube$`)

// cubeTimedFileRe parses merged files: L<level>-<minM>-<maxM>-<xid>.cube
var cubeTimedFileRe = regexp.MustCompile(`^L(\d+)-(\d+)-(\d+)-[^/]+\.cube$`)

func (s *cubeFileStore) List(ctx context.Context, tenant, cubeID string) ([]blockpack.CubeFileInfo, error) {
	prefix := path.Join(tenant, "cubes", cubeID) + "/"
	var files []blockpack.CubeFileInfo
	for obj := range s.client.ListObjects(ctx, s.bucket,
		minio.ListObjectsOptions{Prefix: prefix, Recursive: true}) {
		if obj.Err != nil {
			return nil, obj.Err
		}
		if !cubeFileRe.MatchString(obj.Key) {
			continue
		}
		base := path.Base(obj.Key)
		// Try to parse time range from filename (merged files).
		if m := cubeTimedFileRe.FindStringSubmatch(base); m != nil {
			lv, _ := strconv.ParseUint(m[1], 10, 32)
			minM, _ := strconv.ParseUint(m[2], 10, 32)
			maxM, _ := strconv.ParseUint(m[3], 10, 32)
			files = append(files, blockpack.CubeFileInfo{
				Key:       obj.Key,
				Level:     uint32(lv),   //nolint:gosec
				MinMinute: uint32(minM), //nolint:gosec
				MaxMinute: uint32(maxM), //nolint:gosec
			})
			continue
		}
		// Accumulator-written files (L0-<xid>.cube): read header via ranged GET.
		fi, err := s.readFileInfo(ctx, obj.Key)
		if err != nil {
			continue // skip unreadable files
		}
		files = append(files, fi)
	}
	return files, nil
}

// readFileInfo fetches the first CubeHeaderSize bytes of a cube file and
// returns its FileInfo (Level=Resolution, MinMinute, MaxMinute).
func (s *cubeFileStore) readFileInfo(ctx context.Context, key string) (blockpack.CubeFileInfo, error) {
	opts := minio.GetObjectOptions{}
	if err := opts.SetRange(0, int64(blockpack.CubeHeaderSize)-1); err != nil {
		return blockpack.CubeFileInfo{}, err
	}
	obj, err := s.client.GetObject(ctx, s.bucket, key, opts)
	if err != nil {
		return blockpack.CubeFileInfo{}, err
	}
	defer func() { _ = obj.Close() }()
	buf := make([]byte, blockpack.CubeHeaderSize)
	if _, err := io.ReadFull(obj, buf); err != nil {
		return blockpack.CubeFileInfo{}, err
	}
	minM, maxM, res, err := blockpack.CubeReadHeader(buf)
	if err != nil {
		return blockpack.CubeFileInfo{}, err
	}
	return blockpack.CubeFileInfo{
		Key:       key,
		Level:     res,
		MinMinute: minM,
		MaxMinute: maxM,
	}, nil
}

func (s *cubeFileStore) Get(ctx context.Context, key string) (*blockpack.CubeReader, error) {
	obj, err := s.client.GetObject(ctx, s.bucket, key, minio.GetObjectOptions{})
	if err != nil {
		return nil, err
	}
	defer func() { _ = obj.Close() }()
	var buf bytes.Buffer
	if _, err := io.Copy(&buf, obj); err != nil {
		return nil, err
	}
	return blockpack.OpenCubeReaderFromBytes(buf.Bytes())
}

func (s *cubeFileStore) Put(key string, data []byte) error {
	_, err := s.client.PutObject(
		context.Background(), s.bucket, key,
		bytes.NewReader(data), int64(len(data)),
		minio.PutObjectOptions{ContentType: "application/octet-stream"},
	)
	return err
}

func (s *cubeFileStore) Delete(ctx context.Context, key string) error {
	return s.client.RemoveObject(ctx, s.bucket, key, minio.RemoveObjectOptions{})
}
