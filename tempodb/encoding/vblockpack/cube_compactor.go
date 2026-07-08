package vblockpack

// cube_compactor.go — cubeFileStore: the blockpack.CubeFileStore implementation over minio,
// shared by cube_scheduler.go (the only cube compaction driver — see that file's doc comment for
// history: this file used to also own a periodic CubeCompactorService, which had a real,
// currently-live correctness bug — its L1-rollup loop had no boundary-completeness gate, so it
// rolled up any hour with >=2 L0 files regardless of whether the hour had finished, permanently
// undercounting that hour's L1 data. It was deleted in favor of cube_scheduler.go's single,
// boundary-gated ladder implementation rather than patched in place, to avoid two independent
// cube-compaction drivers racing over the same files).

import (
	"bytes"
	"context"
	"io"
	"path"
	"regexp"
	"strconv"
	"strings"

	blockpack "github.com/grafana/blockpack"
	minio "github.com/minio/minio-go/v7"
)

// cubeFileStore implements blockpack.CubeFileStore over minio.
type cubeFileStore struct {
	client *minio.Client
	bucket string
}

// cubeFileRe matches any .cube file (with or without embedded time range).
var cubeFileRe = regexp.MustCompile(`\.cube$`)

// cubeTimedFileRe parses merged files: L<tier>-<minM>-<maxM>-<xid>.cube, where <tier> is the
// human-readable filename prefix 0/1/2 (L0/L1/L2) — NOT the same numbering as
// blockpack.CubeFileInfo.Level, whose documented contract is "output resolution (1, 60, or
// 1440)" (the same values RollupL0/RollupL1/RollupL2, PlanCubeL0Merge, PlanCubeL1Rollup, and
// Compactor.EvictAgedL0 all compare against). cubeTierToLevel translates between the two; do not
// use the regex's captured tier digit as Level directly (see cubeTierToLevel's doc for the bug
// this fixed: a re-listed L1 rollup file's tier digit "1" collided with RollupL0's value 1,
// causing it to be misclassified as an evictable L0 file).
var cubeTimedFileRe = regexp.MustCompile(`^L(\d+)-(\d+)-(\d+)-[^/]+\.cube$`)

// cubeTierToLevel maps a merged filename's tier prefix (0/1/2, from "L0"/"L1"/"L2") to the actual
// blockpack.CubeFileInfo.Level value (1/60/1440) every other cube compaction function compares
// against. Before this mapping existed, List() assigned the raw tier digit straight to Level,
// so a re-listed L1 rollup file ("L1-...cube" -> tier 1) collided with RollupL0's value (also 1)
// and was misclassified as an L0 file — passing EvictAgedL0's L0 guard and, once past retention
// and covered by the very L1 watermark it had just established, getting deleted outright (real
// data loss, not cosmetic). Returns (0, false) for an unrecognized tier so the caller can skip
// the file rather than tag it with a garbage Level.
func cubeTierToLevel(tier uint64) (uint32, bool) {
	switch tier {
	case 0:
		return cubeLevelL0, true
	case 1:
		return cubeLevelL1, true
	case 2:
		return cubeLevelL2, true
	default:
		return 0, false
	}
}

func (s *cubeFileStore) List(ctx context.Context, tenant, cubeID string) ([]blockpack.CubeFileInfo, error) {
	// Registry stores 16-hex-char IDs (8 bytes); S3 dirs use 32-hex-char (16 bytes, zero-padded).
	paddedID := cubeID
	if len(cubeID) == 16 {
		paddedID = cubeID + strings.Repeat("0", 16)
	}
	prefix := path.Join(tenant, "cubes", paddedID) + "/"
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
			tier, _ := strconv.ParseUint(m[1], 10, 32)
			level, ok := cubeTierToLevel(tier)
			if !ok {
				continue // unrecognized tier prefix — skip rather than tag with a garbage Level
			}
			minM, _ := strconv.ParseUint(m[2], 10, 32)
			maxM, _ := strconv.ParseUint(m[3], 10, 32)
			files = append(files, blockpack.CubeFileInfo{
				Key:       obj.Key,
				Level:     level,
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
