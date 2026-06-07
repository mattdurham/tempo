package s3provider

import (
	"sync"

	minio "github.com/minio/minio-go/v7"
)

// MinIOProvider is a blockpack data type.
type MinIOProvider struct {
	sizeErr error
	client  *minio.Client
	bucket  string
	object  string
	size    int64
	once    sync.Once
}
