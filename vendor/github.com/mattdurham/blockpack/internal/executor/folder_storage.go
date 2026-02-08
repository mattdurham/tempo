package executor

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	blockpackio "github.com/mattdurham/blockpack/internal/blockio"
)

// FolderStorage provides FileStorage interface for reading files from a directory.
type FolderStorage struct {
	baseDir string
}

// NewFolderStorage creates a FileStorage that reads from the specified directory.
func NewFolderStorage(baseDir string) *FolderStorage {
	return &FolderStorage{baseDir: baseDir}
}

// safePath validates that the given path is safe and within the base directory.
// It prevents path traversal attacks by ensuring the resolved path doesn't escape baseDir.
func (s *FolderStorage) safePath(path string) (string, error) {
	// Clean and join the paths
	fullPath := filepath.Join(s.baseDir, filepath.Clean(path))

	// Ensure the full path is still within the base directory
	// Both paths must be absolute for proper comparison
	absBase, err := filepath.Abs(s.baseDir)
	if err != nil {
		return "", fmt.Errorf("failed to resolve base directory: %w", err)
	}

	absPath, err := filepath.Abs(fullPath)
	if err != nil {
		return "", fmt.Errorf("failed to resolve path: %w", err)
	}

	// Check if the resolved path is within the base directory
	relPath, err := filepath.Rel(absBase, absPath)
	if err != nil {
		return "", fmt.Errorf("path outside base directory: %w", err)
	}

	// filepath.Rel returns a path starting with ".." if target is outside base
	if strings.HasPrefix(relPath, "..") {
		return "", fmt.Errorf("path traversal attempt detected: %s", path)
	}

	return fullPath, nil
}

// Get reads the file at the given relative path from the base directory.
func (s *FolderStorage) Get(path string) ([]byte, error) {
	fullPath, err := s.safePath(path)
	if err != nil {
		return nil, err
	}

	data, err := os.ReadFile(fullPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read file %s: %w", fullPath, err)
	}
	return data, nil
}

// GetProvider returns a reader provider for the requested path.
func (s *FolderStorage) GetProvider(path string) (blockpackio.ReaderProvider, error) {
	fullPath, err := s.safePath(path)
	if err != nil {
		return nil, err
	}

	file, err := os.Open(fullPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open file %s: %w", fullPath, err)
	}
	stat, err := file.Stat()
	if err != nil {
		if closeErr := file.Close(); closeErr != nil {
			return nil, fmt.Errorf("failed to stat file %s: %w (close error: %v)", fullPath, err, closeErr)
		}
		return nil, fmt.Errorf("failed to stat file %s: %w", fullPath, err)
	}
	return &fileReaderProvider{file: file, size: stat.Size()}, nil
}
