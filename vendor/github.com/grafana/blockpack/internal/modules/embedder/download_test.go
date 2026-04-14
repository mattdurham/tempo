package embedder

// NOTE: Any changes to this file must be reflected in the corresponding specs.md or NOTES.md.

import (
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestDefaultModelPath verifies the path ends with the expected model filename.
func TestDefaultModelPath(t *testing.T) {
	p := DefaultModelPath()
	assert.True(t, strings.HasSuffix(p, "nomic-embed-text-v1.5.Q8_0.gguf"),
		"DefaultModelPath should end with model filename, got: %q", p)
	// Must be an absolute path (home dir expanded).
	assert.True(t, filepath.IsAbs(p), "DefaultModelPath must return an absolute path, got: %q", p)
}

// TestEnsureModel_existingFile verifies that when the file already exists, EnsureModel returns nil
// without attempting any network call.
func TestEnsureModel_existingFile(t *testing.T) {
	dir := t.TempDir()
	modelPath := filepath.Join(dir, "model.gguf")

	// Create the file so it already exists.
	require.NoError(t, os.WriteFile(modelPath, []byte("fake model content"), 0o600))

	cfg := Config{
		ModelPath:    modelPath,
		AutoDownload: true,
		// ModelURL is intentionally left empty — a download attempt would fail or panic
		// if EnsureModel tried to fetch.
		ModelURL: "http://127.0.0.1:0/should-not-be-called",
	}

	err := EnsureModel(cfg)
	assert.NoError(t, err, "EnsureModel should return nil when file already exists")

	// File must still be present and unchanged.
	content, readErr := os.ReadFile(modelPath) //nolint:gosec // test path from t.TempDir
	require.NoError(t, readErr)
	assert.Equal(t, "fake model content", string(content))
}

// TestEnsureModel_createsDirectories verifies that parent directories are created when they
// don't exist, and the downloaded content is written correctly.
func TestEnsureModel_createsDirectories(t *testing.T) {
	dir := t.TempDir()
	// Use a deeply nested path that does not exist yet.
	modelPath := filepath.Join(dir, "a", "b", "c", "model.gguf")

	expectedContent := "gguf model bytes"

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(expectedContent))
	}))
	defer srv.Close()

	cfg := Config{
		ModelPath:    modelPath,
		AutoDownload: true,
		ModelURL:     srv.URL + "/model.gguf",
	}

	err := EnsureModel(cfg)
	require.NoError(t, err, "EnsureModel should create parent directories and download the file")

	// Verify file exists with the correct content.
	got, readErr := os.ReadFile(modelPath) //nolint:gosec // test path from t.TempDir
	require.NoError(t, readErr)
	assert.Equal(t, expectedContent, string(got))
}

// TestEnsureModel_atomicWrite verifies the write-to-temp-then-rename pattern:
// if no crash, the final file exists at ModelPath and no temp file is left behind.
func TestEnsureModel_atomicWrite(t *testing.T) {
	dir := t.TempDir()
	modelPath := filepath.Join(dir, "model.gguf")

	payload := strings.Repeat("x", 1024)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(payload))
	}))
	defer srv.Close()

	cfg := Config{
		ModelPath:    modelPath,
		AutoDownload: true,
		ModelURL:     srv.URL + "/model.gguf",
	}

	err := EnsureModel(cfg)
	require.NoError(t, err)

	// Final file must exist at the correct path.
	info, statErr := os.Stat(modelPath)
	require.NoError(t, statErr, "model file must exist at ModelPath after download")
	assert.Equal(t, int64(len(payload)), info.Size())

	// No leftover temp files in the directory.
	entries, readDirErr := os.ReadDir(dir)
	require.NoError(t, readDirErr)
	for _, e := range entries {
		assert.False(t, strings.HasPrefix(e.Name(), ".tmp-"),
			"unexpected temp file left behind: %s", e.Name())
	}
}

// TestEnsureModel_downloadError verifies that a failed download returns a non-nil error
// and does not leave a partial file at ModelPath.
func TestEnsureModel_downloadError(t *testing.T) {
	dir := t.TempDir()
	modelPath := filepath.Join(dir, "model.gguf")

	// Server returns a 500.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "internal server error", http.StatusInternalServerError)
	}))
	defer srv.Close()

	cfg := Config{
		ModelPath:    modelPath,
		AutoDownload: true,
		ModelURL:     srv.URL + "/model.gguf",
	}

	err := EnsureModel(cfg)
	assert.Error(t, err, "EnsureModel should return an error on non-200 response")

	// ModelPath must not exist (no partial file).
	_, statErr := os.Stat(modelPath)
	assert.True(t, os.IsNotExist(statErr), "no partial file should exist at ModelPath after failed download")
}
