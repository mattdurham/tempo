package vblockpack

// catalog_object_store.go — issue #522 Phase 0.4: the object-storage
// capability backend-worker's catalog_reconcile/catalog_reap handlers need
// for the trace/span subsystem. Deliberately NOT the full
// valueindexcompactor.IndexStore surface (Peek/ListDirs) -- catalog
// reconcile/reap only ever list a tenant-scoped prefix and delete a single
// key by its already-known full object_key, none of which needs Peek/ListDirs.
// Get/Put were removed by #155: VI/VCNT/cube compaction execution (the only
// callers that needed to read/write catalog objects directly) moved entirely
// into blockpack's own compaction-worker.
//
// S3-only, mirroring processViBackfillJobPostgres/processCubeBackfillJobPostgres's
// existing "if w.s3Cfg == nil, fail" convention -- backend-worker's own VI/cube
// execution paths are already S3-only today, so this introduces no new
// backend-agnostic requirement.

import (
	"context"

	minio "github.com/minio/minio-go/v7"

	s3backend "github.com/grafana/tempo/tempodb/backend/s3"
)

// CatalogObjectStore lists and deletes objects for the
// catalog_reconcile/catalog_reap handlers.
type CatalogObjectStore interface {
	// List returns the full object keys under prefix.
	List(ctx context.Context, prefix string) ([]string, error)
	// Delete removes the object at key.
	Delete(ctx context.Context, key string) error
}

// NewCatalogObjectStoreS3 builds a CatalogObjectStore over an S3 client,
// mirroring minioVIStore's/viUsageObjectStore's construction pattern.
func NewCatalogObjectStoreS3(s3cfg *s3backend.Config) (CatalogObjectStore, error) {
	client, err := newMinioClientFromS3Config(s3cfg)
	if err != nil {
		return nil, err
	}
	return &catalogObjectStoreS3{client: client, bucket: s3cfg.Bucket}, nil
}

type catalogObjectStoreS3 struct {
	client *minio.Client
	bucket string
}

// List mirrors minioVIStore.List exactly.
func (s *catalogObjectStoreS3) List(ctx context.Context, prefix string) ([]string, error) {
	var keys []string
	for obj := range s.client.ListObjects(ctx, s.bucket, minio.ListObjectsOptions{Prefix: prefix, Recursive: true}) {
		if obj.Err != nil {
			return nil, obj.Err
		}
		keys = append(keys, obj.Key)
	}
	return keys, nil
}

// Delete removes the object at key. Deleting an already-gone key is not an
// error on S3 (DELETE is idempotent by design), matching
// pgcatalog.Store.DeleteRow's own idempotent, safe-to-retry contract.
func (s *catalogObjectStoreS3) Delete(ctx context.Context, key string) error {
	return s.client.RemoveObject(ctx, s.bucket, key, minio.RemoveObjectOptions{})
}
