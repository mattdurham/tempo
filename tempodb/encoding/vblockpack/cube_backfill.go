package vblockpack

// cube_backfill.go — value-index backed CubeValueIndexSource for cube backfill,
// and backfill launch wired into the first-query cube creation trigger.
//
// When a cube is created (CreationTrigger.TryCreate returns Created=true), we
// immediately launch a background goroutine that backfills historical data from
// the value index, newest-first, writing L0 cube files per minute.

import (
	"bytes"
	"context"
	"io"
	"path"

	"github.com/go-kit/log/level"
	blockpack "github.com/grafana/blockpack"
	util_log "github.com/grafana/tempo/pkg/util/log"
	minio "github.com/minio/minio-go/v7"
)

// viBackfillSource implements blockpack.CubeValueIndexSource over S3 VI files.
// LookupColumn lists + downloads VI files for (tenant, column) in [minSec, maxSec],
// returning one VIQueryResult per span with SourceRef set to the decoded string
// column value (the convention expected by the cube Backfiller).
type viBackfillSource struct {
	client      *minio.Client
	bucket      string
	indexPrefix string
}

func (s *viBackfillSource) LookupColumn(
	ctx context.Context,
	tenant, column string,
	minSec, maxSec uint64,
) ([]blockpack.VIQueryResult, error) {
	colHash := blockpack.VCNTColHash(column)

	var results []blockpack.VIQueryResult
	for _, typeName := range []string{"string", "int64", "uint64", "bool", "float64"} {
		prefix := path.Join(tenant, s.indexPrefix, colHash, typeName) + "/"
		keys, err := s.listObjects(ctx, prefix)
		if err != nil || len(keys) == 0 {
			continue
		}

		for _, k := range keys {
			meta, perr := blockpack.VIParseFilenameV2(path.Base(k))
			if perr != nil {
				continue
			}
			if !meta.IsInTimeRange(minSec, maxSec) {
				continue
			}
			data, getErr := s.getObject(ctx, k)
			if getErr != nil {
				continue
			}
			r, openErr := blockpack.VIOpenReader(data)
			if openErr != nil {
				continue
			}
			tr := [2]uint64{minSec, maxSec}
			hits, qErr := r.Lookup(nil, &tr)
			if qErr != nil {
				continue
			}
			for _, h := range hits {
				// The backfill reads the column value from SourceRef, not Value.
				// Decode the canonical value bytes to a string.
				valStr := decodeCanonicalVI(h.Value)
				results = append(results, blockpack.VIQueryResult{
					SourceRef: valStr,
					Value:     h.Value,
					TimeSec:   h.TimeSec,
					TraceID:   h.TraceID,
					SpanID:    h.SpanID,
					RowIdx:    h.RowIdx,
				})
			}
		}
	}
	return results, nil
}

func (s *viBackfillSource) listObjects(ctx context.Context, prefix string) ([]string, error) {
	var keys []string
	for obj := range s.client.ListObjects(ctx, s.bucket,
		minio.ListObjectsOptions{Prefix: prefix, Recursive: true}) {
		if obj.Err != nil {
			return nil, obj.Err
		}
		keys = append(keys, obj.Key)
	}
	return keys, nil
}

func (s *viBackfillSource) getObject(ctx context.Context, key string) ([]byte, error) {
	obj, err := s.client.GetObject(ctx, s.bucket, key, minio.GetObjectOptions{})
	if err != nil {
		return nil, err
	}
	defer func() { _ = obj.Close() }()
	var buf bytes.Buffer
	_, err = io.Copy(&buf, obj)
	return buf.Bytes(), err
}

// decodeCanonicalVI converts canonical VI value bytes to a string.
// String columns store raw UTF-8; numeric columns store 8-byte little-endian.
func decodeCanonicalVI(b []byte) string {
	if len(b) == 0 {
		return ""
	}
	// If all bytes are valid UTF-8 printable, treat as string.
	for _, c := range b {
		if c < 0x20 {
			goto numeric
		}
	}
	return string(b)
numeric:
	if len(b) == 8 {
		v := int64(b[0]) | int64(b[1])<<8 | int64(b[2])<<16 | int64(b[3])<<24 |
			int64(b[4])<<32 | int64(b[5])<<40 | int64(b[6])<<48 | int64(b[7])<<56 //nolint:gosec
		return path.Join("", string(rune('0'+v%10))) // simple numeric → string
	}
	return string(b)
}

// launchBackfill starts a background goroutine that backfills a newly-created cube
// from the value index, reading VI files newest→oldest for the configured window.
func launchBackfill(entry blockpack.CubeRegistryEntry) {
	cqp := getCubeQueryPath()
	if cqp == nil {
		return
	}
	src := &viBackfillSource{
		client:      cqp.client,
		bucket:      cqp.bucket,
		indexPrefix: defaultValueIndexPref,
	}
	store := &s3ObjectPutter{client: cqp.client, bucket: cqp.bucket}
	cfg := blockpack.CubeBackfillConfig{
		Store:         store,
		Workers:       4,
		WindowMinutes: 60 * 24 * 7, // 7 days
	}
	bf := blockpack.NewCubeBackfiller(entry, src, cfg)

	go func() {
		level.Info(util_log.Logger).Log(
			"msg", "vblockpack: cube backfill started",
			"tenant", entry.Tenant,
			"cube_id", entry.CubeID,
		)
		err := bf.Run(context.Background(), 0, func(prog blockpack.CubeBackfillProgress) error {
			if prog.Watermark.Done {
				level.Info(util_log.Logger).Log(
					"msg", "vblockpack: cube backfill complete",
					"tenant", entry.Tenant,
					"cube_id", entry.CubeID,
				)
			}
			return nil
		})
		if err != nil {
			level.Warn(util_log.Logger).Log(
				"msg", "vblockpack: cube backfill error",
				"tenant", entry.Tenant,
				"cube_id", entry.CubeID,
				"err", err,
			)
		}
	}()
}
