// Package s3provider provides a ReaderProvider implementation backed by
// MinIO / Amazon S3-compatible object storage.
//
// NOTE: Size() is cached after the first call — StatObject is issued at most
// once per provider instance.  ReadAt issues an independent HTTP range request
// per call, so all concurrent callers are safe.
package s3provider

import (
	"context"
	"fmt"
	"io"
	"os"
	"sync"

	minio "github.com/minio/minio-go/v7"

	"github.com/grafana/blockpack/internal/modules/rw"
)

// MinIOProvider implements rw.ReaderProvider backed by a single MinIO/S3 object.
// All methods are safe for concurrent use.
type MinIOProvider struct {
	sizeErr error
	client  *minio.Client
	bucket  string
	object  string

	size int64

	once sync.Once
}

// NewMinIOProvider returns a MinIOProvider for the given MinIO client, bucket,
// and object path.  The client is not contacted until the first call to Size or
// ReadAt.
func NewMinIOProvider(client *minio.Client, bucket, object string) *MinIOProvider {
	return &MinIOProvider{
		client: client,
		bucket: bucket,
		object: object,
	}
}

// Size returns the total size of the object in bytes.
// The result is cached: StatObject is called at most once per provider instance.
func (p *MinIOProvider) Size() (int64, error) {
	p.once.Do(func() {
		info, err := p.client.StatObject(
			context.Background(), p.bucket, p.object, minio.StatObjectOptions{},
		)
		if err != nil {
			p.sizeErr = fmt.Errorf("s3provider: stat %s/%s: %w", p.bucket, p.object, err)
			return
		}
		p.size = info.Size
	})
	return p.size, p.sizeErr
}

// ReadAt reads len(buf) bytes from the object at byte offset off.
//
// It follows io.ReaderAt semantics:
//   - off < 0               → (0, os.ErrInvalid)
//   - off >= Size()         → (0, io.EOF)
//   - short read at EOF     → (n, io.EOF)
//   - full read             → (len(buf), nil)
//
// Each call issues an independent HTTP range request; safe for concurrent use.
// The dataType hint is accepted but ignored (as permitted by the rw.ReaderProvider spec).
func (p *MinIOProvider) ReadAt(buf []byte, off int64, _ rw.DataType) (int, error) {
	if off < 0 {
		return 0, os.ErrInvalid
	}
	if len(buf) == 0 {
		return 0, nil
	}

	size, err := p.Size()
	if err != nil {
		return 0, err
	}
	if off >= size {
		return 0, io.EOF
	}

	// Range header is inclusive: bytes=off-(off+len-1).
	end := off + int64(len(buf)) - 1
	opts := minio.GetObjectOptions{}
	if setErr := opts.SetRange(off, end); setErr != nil {
		return 0, fmt.Errorf("s3provider: set range [%d, %d]: %w", off, end, setErr)
	}

	obj, err := p.client.GetObject(context.Background(), p.bucket, p.object, opts)
	if err != nil {
		return 0, fmt.Errorf("s3provider: get %s/%s: %w", p.bucket, p.object, err)
	}
	defer func() { _ = obj.Close() }()

	n, err := io.ReadFull(obj, buf)
	if err == io.ErrUnexpectedEOF {
		// Only translate to io.EOF when the requested range reaches or passes the
		// last byte of the object.  If the range is fully within bounds, an
		// unexpected short read signals truncation, not EOF, and must be surfaced
		// as io.ErrUnexpectedEOF so callers can distinguish the two cases.
		if end >= size-1 {
			return n, io.EOF
		}
		return n, err
	}
	return n, err
}
