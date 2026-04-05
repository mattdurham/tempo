package embedder

// NOTE: Any changes to this file must be reflected in the corresponding specs.md or NOTES.md.

import (
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log"
	"net/http"
	"os"
	"path/filepath"
)

// DefaultModelURL is the HuggingFace URL for the nomic-embed-text-v1.5 Q8_0 GGUF model.
const DefaultModelURL = "https://huggingface.co/nomic-ai/nomic-embed-text-v1.5-GGUF/resolve/main/nomic-embed-text-v1.5.Q8_0.gguf"

// defaultModelFilename is the basename of the default GGUF model.
const defaultModelFilename = "nomic-embed-text-v1.5.Q8_0.gguf"

// DefaultModelPath returns the default local path for the GGUF model:
// ~/.blockpack/models/nomic-embed-text-v1.5.Q8_0.gguf
//
// The home directory is expanded at call time. If the home directory cannot be
// determined, the path is returned relative to the current working directory.
func DefaultModelPath() string {
	home, err := os.UserHomeDir()
	if err != nil {
		// Fall back to a relative path — unlikely in practice.
		return filepath.Join(".blockpack", "models", defaultModelFilename)
	}
	return filepath.Join(home, ".blockpack", "models", defaultModelFilename)
}

// EnsureModel checks whether cfg.ModelPath exists on disk and, if it does not,
// downloads the model from cfg.ModelURL (falling back to DefaultModelURL).
//
// Download is performed to a temporary file in the same directory as the target,
// then atomically renamed to cfg.ModelPath — preventing partial files from being
// visible to other readers on crash or error.
//
// Parent directories are created with os.MkdirAll if they do not exist.
//
// Returns nil immediately if the file already exists.
// Returns an error if the HTTP request fails, the server returns a non-200 status,
// or any filesystem operation fails.
func EnsureModel(cfg Config) error {
	// Fast path: file already exists.
	if _, err := os.Stat(cfg.ModelPath); err == nil {
		return nil
	} else if !errors.Is(err, fs.ErrNotExist) {
		return fmt.Errorf("embedder: stat model path %q: %w", cfg.ModelPath, err)
	}

	modelURL := cfg.ModelURL
	if modelURL == "" {
		modelURL = DefaultModelURL
	}

	// Ensure parent directories exist.
	dir := filepath.Dir(cfg.ModelPath)
	if err := os.MkdirAll(dir, 0o750); err != nil { //nolint:gomnd // standard restrictive dir perms
		return fmt.Errorf("embedder: create model directory %q: %w", dir, err)
	}

	// Download to a temp file in the same directory for atomic rename.
	tmp, err := os.CreateTemp(dir, ".tmp-embedder-model-*")
	if err != nil {
		return fmt.Errorf("embedder: create temp file: %w", err)
	}
	tmpName := tmp.Name()

	// Clean up the temp file on any error path.
	success := false
	defer func() {
		if !success {
			_ = tmp.Close()
			_ = os.Remove(tmpName)
		}
	}()

	log.Printf("embedder: downloading model from %s to %s", modelURL, cfg.ModelPath)

	resp, err := http.Get(modelURL) //nolint:noctx,gosec // model download is a one-shot CLI operation; URL from config
	if err != nil {
		return fmt.Errorf("embedder: download model from %q: %w", modelURL, err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("embedder: download model: server returned %s", resp.Status)
	}

	if resp.ContentLength > 0 {
		log.Printf("embedder: model size: %.1f MB", float64(resp.ContentLength)/1024/1024)
	}

	n, err := io.Copy(tmp, resp.Body)
	if err != nil {
		return fmt.Errorf("embedder: write model to temp file: %w", err)
	}
	log.Printf("embedder: downloaded %.1f MB", float64(n)/1024/1024) //nolint:gosec // n is bytes read, not user input

	if err := tmp.Close(); err != nil {
		return fmt.Errorf("embedder: close temp file: %w", err)
	}

	// Atomic rename — temp file is now the final model file.
	if err := os.Rename(tmpName, cfg.ModelPath); err != nil { //nolint:gosec // tmpName is from CreateTemp, ModelPath from config
		return fmt.Errorf("embedder: rename temp file to %q: %w", cfg.ModelPath, err)
	}

	success = true
	log.Printf("embedder: model ready at %s", cfg.ModelPath)
	return nil
}
