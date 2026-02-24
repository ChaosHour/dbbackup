package fs

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

// SecureMkdirAll creates directories with secure permissions, handling race conditions
// Uses 0700 permissions (owner-only access) for sensitive data directories
func SecureMkdirAll(path string, perm os.FileMode) error {
	err := os.MkdirAll(path, perm)
	if err != nil && !errors.Is(err, os.ErrExist) {
		return fmt.Errorf("failed to create directory: %w", err)
	}
	return nil
}

// SecureCreate creates a file with secure permissions (0600 - owner read/write only)
// Used for backup files containing sensitive database data
func SecureCreate(path string) (*os.File, error) {
	return os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0600)
}

// SecureOpenFile opens a file with specified flags and secure permissions
func SecureOpenFile(path string, flag int, perm os.FileMode) (*os.File, error) {
	// Ensure permission is restrictive for new files
	if flag&os.O_CREATE != 0 && perm > 0600 {
		perm = 0600
	}
	return os.OpenFile(path, flag, perm)
}

// SecureMkdirTemp creates a temporary directory with 0700 permissions
// Returns absolute path to created directory
func SecureMkdirTemp(dir, pattern string) (string, error) {
	if dir == "" {
		dir = os.TempDir()
	}

	tempDir, err := os.MkdirTemp(dir, pattern)
	if err != nil {
		return "", fmt.Errorf("failed to create temp directory: %w", err)
	}

	// Ensure temp directory has secure permissions
	if err := os.Chmod(tempDir, 0700); err != nil {
		_ = os.RemoveAll(tempDir)
		return "", fmt.Errorf("failed to secure temp directory: %w", err)
	}

	return tempDir, nil
}

// CheckWriteAccess tests if directory is writable by creating and removing a test file
// Returns error if directory is not writable (e.g., read-only filesystem)
func CheckWriteAccess(dir string) error {
	testFile := filepath.Join(dir, ".dbbackup-write-test")

	f, err := os.Create(testFile)
	if err != nil {
		if os.IsPermission(err) {
			return fmt.Errorf("directory is not writable (permission denied): %s", dir)
		}
		if errors.Is(err, os.ErrPermission) {
			return fmt.Errorf("directory is read-only: %s", dir)
		}
		return fmt.Errorf("cannot write to directory: %w", err)
	}
	_ = f.Close()

	if err := os.Remove(testFile); err != nil {
		return fmt.Errorf("cannot remove test file (directory may be read-only): %w", err)
	}

	return nil
}
