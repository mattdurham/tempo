package vblockpack

import (
	"errors"
	"io"
	"testing"

	blockpack "github.com/grafana/blockpack"
	minio "github.com/minio/minio-go/v7"
	"github.com/stretchr/testify/assert"
)

// mapNotFound translates a minio 404 / NoSuchKey into
// blockpack.ErrValueIndexFileNotFound so the index builder skips a file the
// compactor deleted out from under a stale listing (blockpack issue #399 point 5),
// while passing every other error through unchanged.
func TestMapNotFound(t *testing.T) {
	t.Run("nil stays nil", func(t *testing.T) {
		assert.NoError(t, mapNotFound(nil))
	})

	t.Run("NoSuchKey maps to sentinel", func(t *testing.T) {
		err := minio.ErrorResponse{Code: "NoSuchKey", StatusCode: 404}
		got := mapNotFound(err)
		assert.True(t, errors.Is(got, blockpack.ErrValueIndexFileNotFound),
			"a NoSuchKey/404 must map to the not-found sentinel")
	})

	t.Run("404 status alone maps to sentinel", func(t *testing.T) {
		err := minio.ErrorResponse{StatusCode: 404}
		got := mapNotFound(err)
		assert.True(t, errors.Is(got, blockpack.ErrValueIndexFileNotFound))
	})

	t.Run("transient error passes through", func(t *testing.T) {
		orig := errors.New("connection reset by peer")
		got := mapNotFound(orig)
		assert.Equal(t, orig, got)
		assert.False(t, errors.Is(got, blockpack.ErrValueIndexFileNotFound),
			"a non-404 error must NOT be treated as not-found — it must still abort")
	})

	t.Run("io.EOF passes through unchanged", func(t *testing.T) {
		// readWhole tolerates io.EOF from a short read; mapNotFound must not
		// reclassify it as not-found.
		got := mapNotFound(io.EOF)
		assert.Equal(t, io.EOF, got)
		assert.False(t, errors.Is(got, blockpack.ErrValueIndexFileNotFound))
	})

	t.Run("server error 500 passes through", func(t *testing.T) {
		err := minio.ErrorResponse{Code: "InternalError", StatusCode: 500}
		got := mapNotFound(err)
		assert.False(t, errors.Is(got, blockpack.ErrValueIndexFileNotFound))
	})
}
