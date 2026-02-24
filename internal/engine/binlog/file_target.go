package binlog

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"dbbackup/internal/fs"

	"github.com/klauspost/pgzip"
)

// FileTarget writes binlog events to local files
type FileTarget struct {
	basePath   string
	rotateSize int64

	mu      sync.Mutex
	current *os.File
	written int64
	fileNum int
	healthy bool
	lastErr error
}

// NewFileTarget creates a new file target
func NewFileTarget(basePath string, rotateSize int64) (*FileTarget, error) {
	if rotateSize == 0 {
		rotateSize = 100 * 1024 * 1024 // 100MB default
	}

	// Ensure directory exists
	if err := os.MkdirAll(basePath, 0755); err != nil {
		return nil, fmt.Errorf("failed to create directory: %w", err)
	}

	return &FileTarget{
		basePath:   basePath,
		rotateSize: rotateSize,
		healthy:    true,
	}, nil
}

// Name returns the target name
func (f *FileTarget) Name() string {
	return fmt.Sprintf("file:%s", f.basePath)
}

// Type returns the target type
func (f *FileTarget) Type() string {
	return "file"
}

// Write writes events to the current file
func (f *FileTarget) Write(ctx context.Context, events []*Event) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	// Open file if needed
	if f.current == nil {
		if err := f.openNewFile(); err != nil {
			f.healthy = false
			f.lastErr = err
			return err
		}
	}

	// Write events
	for _, ev := range events {
		data, err := json.Marshal(ev)
		if err != nil {
			continue
		}

		// Add newline for line-delimited JSON
		data = append(data, '\n')

		n, err := f.current.Write(data)
		if err != nil {
			f.healthy = false
			f.lastErr = err
			return fmt.Errorf("failed to write: %w", err)
		}

		f.written += int64(n)
	}

	// Rotate if needed
	if f.written >= f.rotateSize {
		if err := f.rotate(); err != nil {
			f.healthy = false
			f.lastErr = err
			return err
		}
	}

	f.healthy = true
	return nil
}

// openNewFile opens a new output file
func (f *FileTarget) openNewFile() error {
	f.fileNum++
	filename := filepath.Join(f.basePath,
		fmt.Sprintf("binlog_%s_%04d.jsonl",
			time.Now().Format("20060102_150405"),
			f.fileNum))

	file, err := os.Create(filename)
	if err != nil {
		return err
	}

	f.current = file
	f.written = 0
	return nil
}

// rotate closes current file and opens a new one
func (f *FileTarget) rotate() error {
	if f.current != nil {
		if err := f.current.Close(); err != nil {
			return err
		}
		f.current = nil
	}

	return f.openNewFile()
}

// Flush syncs the current file
func (f *FileTarget) Flush(ctx context.Context) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.current != nil {
		return f.current.Sync()
	}
	return nil
}

// Close closes the target
func (f *FileTarget) Close() error {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.current != nil {
		err := f.current.Close()
		f.current = nil
		return err
	}
	return nil
}

// Healthy returns target health status
func (f *FileTarget) Healthy() bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.healthy
}

// CompressedFileTarget writes compressed binlog events
type CompressedFileTarget struct {
	basePath   string
	rotateSize int64

	mu       sync.Mutex
	file     *os.File
	sw       *fs.SafeWriter
	gzWriter *pgzip.Writer
	written  int64
	fileNum  int
	healthy  bool
	lastErr  error
}

// NewCompressedFileTarget creates a gzip-compressed file target
func NewCompressedFileTarget(basePath string, rotateSize int64) (*CompressedFileTarget, error) {
	if rotateSize == 0 {
		rotateSize = 100 * 1024 * 1024 // 100MB uncompressed
	}

	if err := os.MkdirAll(basePath, 0755); err != nil {
		return nil, fmt.Errorf("failed to create directory: %w", err)
	}

	return &CompressedFileTarget{
		basePath:   basePath,
		rotateSize: rotateSize,
		healthy:    true,
	}, nil
}

// Name returns the target name
func (c *CompressedFileTarget) Name() string {
	return fmt.Sprintf("file-gzip:%s", c.basePath)
}

// Type returns the target type
func (c *CompressedFileTarget) Type() string {
	return "file-gzip"
}

// Write writes events to compressed file
func (c *CompressedFileTarget) Write(ctx context.Context, events []*Event) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Open file if needed
	if c.file == nil {
		if err := c.openNewFile(); err != nil {
			c.healthy = false
			c.lastErr = err
			return err
		}
	}

	// Write events
	for _, ev := range events {
		data, err := json.Marshal(ev)
		if err != nil {
			continue
		}

		data = append(data, '\n')

		n, err := c.gzWriter.Write(data)
		if err != nil {
			c.healthy = false
			c.lastErr = err
			return fmt.Errorf("failed to write: %w", err)
		}

		c.written += int64(n)
	}

	// Rotate if needed
	if c.written >= c.rotateSize {
		if err := c.rotate(); err != nil {
			c.healthy = false
			c.lastErr = err
			return err
		}
	}

	c.healthy = true
	return nil
}

// openNewFile opens a new compressed file
func (c *CompressedFileTarget) openNewFile() error {
	c.fileNum++
	filename := filepath.Join(c.basePath,
		fmt.Sprintf("binlog_%s_%04d.jsonl.gz",
			time.Now().Format("20060102_150405"),
			c.fileNum))

	file, err := os.Create(filename)
	if err != nil {
		return err
	}

	c.file = file
	// Wrap file in SafeWriter to prevent pgzip goroutine panics
	c.sw = fs.NewSafeWriter(file)
	c.gzWriter = pgzip.NewWriter(c.sw)
	c.written = 0
	return nil
}

// rotate closes current file and opens a new one
func (c *CompressedFileTarget) rotate() error {
	if c.gzWriter != nil {
		_ = c.gzWriter.Close()
	}
	if c.sw != nil {
		c.sw.Shutdown() // Block lingering pgzip goroutines before closing file
		c.sw = nil
	}
	if c.file != nil {
		_ = c.file.Close()
		c.file = nil
	}

	return c.openNewFile()
}

// Flush flushes the gzip writer
func (c *CompressedFileTarget) Flush(ctx context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.gzWriter != nil {
		if err := c.gzWriter.Flush(); err != nil {
			return err
		}
	}
	if c.file != nil {
		return c.file.Sync()
	}
	return nil
}

// Close closes the target
func (c *CompressedFileTarget) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	var errs []error
	if c.gzWriter != nil {
		if err := c.gzWriter.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	if c.file != nil {
		if err := c.file.Close(); err != nil {
			errs = append(errs, err)
		}
		c.file = nil
	}

	if len(errs) > 0 {
		return errs[0]
	}
	return nil
}

// Healthy returns target health status
func (c *CompressedFileTarget) Healthy() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.healthy
}
