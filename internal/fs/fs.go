// Package fs provides filesystem abstraction using spf13/afero for testability.
// It allows swapping the real filesystem with an in-memory mock for unit tests.
package fs

import (
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/spf13/afero"
)

// FS is the global filesystem interface used throughout the application.
// By default, it uses the real OS filesystem.
// For testing, use SetFS(afero.NewMemMapFs()) to use an in-memory filesystem.
var FS afero.Fs = afero.NewOsFs()

// SetFS sets the global filesystem (useful for testing)
func SetFS(fs afero.Fs) {
	FS = fs
}

// ResetFS resets to the real OS filesystem
func ResetFS() {
	FS = afero.NewOsFs()
}

// NewMemMapFs creates a new in-memory filesystem for testing
func NewMemMapFs() afero.Fs {
	return afero.NewMemMapFs()
}

// NewReadOnlyFs wraps a filesystem to make it read-only
func NewReadOnlyFs(base afero.Fs) afero.Fs {
	return afero.NewReadOnlyFs(base)
}

// NewBasePathFs creates a filesystem rooted at a specific path
func NewBasePathFs(base afero.Fs, path string) afero.Fs {
	return afero.NewBasePathFs(base, path)
}

// --- File Operations (use global FS) ---

// Create creates a file
func Create(name string) (afero.File, error) {
	return FS.Create(name)
}

// Open opens a file for reading
func Open(name string) (afero.File, error) {
	return FS.Open(name)
}

// OpenFile opens a file with specified flags and permissions
func OpenFile(name string, flag int, perm os.FileMode) (afero.File, error) {
	return FS.OpenFile(name, flag, perm)
}

// Remove removes a file or empty directory
func Remove(name string) error {
	return FS.Remove(name)
}

// RemoveAll removes a path and any children it contains
func RemoveAll(path string) error {
	return FS.RemoveAll(path)
}

// Rename renames (moves) a file
func Rename(oldname, newname string) error {
	return FS.Rename(oldname, newname)
}

// Stat returns file info
func Stat(name string) (os.FileInfo, error) {
	return FS.Stat(name)
}

// Chmod changes file mode
func Chmod(name string, mode os.FileMode) error {
	return FS.Chmod(name, mode)
}

// Chown changes file ownership (may not work on all filesystems)
func Chown(name string, uid, gid int) error {
	return FS.Chown(name, uid, gid)
}

// Chtimes changes file access and modification times
func Chtimes(name string, atime, mtime time.Time) error {
	return FS.Chtimes(name, atime, mtime)
}

// --- Directory Operations ---

// Mkdir creates a directory
func Mkdir(name string, perm os.FileMode) error {
	return FS.Mkdir(name, perm)
}

// MkdirAll creates a directory and all parents
func MkdirAll(path string, perm os.FileMode) error {
	return FS.MkdirAll(path, perm)
}

// ReadDir reads a directory
func ReadDir(dirname string) ([]os.FileInfo, error) {
	return afero.ReadDir(FS, dirname)
}

// --- File Content Operations ---

// ReadFile reads an entire file
func ReadFile(filename string) ([]byte, error) {
	return afero.ReadFile(FS, filename)
}

// WriteFile writes data to a file
func WriteFile(filename string, data []byte, perm os.FileMode) error {
	return afero.WriteFile(FS, filename, data, perm)
}

// --- Existence Checks ---

// Exists checks if a file or directory exists
func Exists(path string) (bool, error) {
	return afero.Exists(FS, path)
}

// DirExists checks if a directory exists
func DirExists(path string) (bool, error) {
	return afero.DirExists(FS, path)
}

// IsDir checks if path is a directory
func IsDir(path string) (bool, error) {
	return afero.IsDir(FS, path)
}

// IsEmpty checks if a directory is empty
func IsEmpty(path string) (bool, error) {
	return afero.IsEmpty(FS, path)
}

// --- Utility Functions ---

// Walk walks a directory tree
func Walk(root string, walkFn filepath.WalkFunc) error {
	return afero.Walk(FS, root, walkFn)
}

// Glob returns the names of all files matching pattern
func Glob(pattern string) ([]string, error) {
	return afero.Glob(FS, pattern)
}

// TempDir creates a temporary directory
func TempDir(dir, prefix string) (string, error) {
	return afero.TempDir(FS, dir, prefix)
}

// TempFile creates a temporary file
func TempFile(dir, pattern string) (afero.File, error) {
	return afero.TempFile(FS, dir, pattern)
}

// CopyFile copies a file from src to dst
func CopyFile(src, dst string) error {
	srcFile, err := FS.Open(src)
	if err != nil {
		return err
	}
	defer func() { _ = srcFile.Close() }()

	srcInfo, err := srcFile.Stat()
	if err != nil {
		return err
	}

	dstFile, err := FS.OpenFile(dst, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, srcInfo.Mode())
	if err != nil {
		return err
	}
	defer func() { _ = dstFile.Close() }()

	_, err = io.Copy(dstFile, srcFile)
	return err
}

// FileSize returns the size of a file
func FileSize(path string) (int64, error) {
	info, err := FS.Stat(path)
	if err != nil {
		return 0, err
	}
	return info.Size(), nil
}

// --- Testing Helpers ---

// WithMemFs executes a function with an in-memory filesystem, then restores the original
func WithMemFs(fn func(fs afero.Fs)) {
	original := FS
	memFs := afero.NewMemMapFs()
	FS = memFs
	defer func() { FS = original }()
	fn(memFs)
}

// SetupTestDir creates a test directory structure in-memory
func SetupTestDir(files map[string]string) afero.Fs {
	memFs := afero.NewMemMapFs()
	for path, content := range files {
		dir := filepath.Dir(path)
		if dir != "." && dir != "/" {
			_ = memFs.MkdirAll(dir, 0755)
		}
		_ = afero.WriteFile(memFs, path, []byte(content), 0644)
	}
	return memFs
}
