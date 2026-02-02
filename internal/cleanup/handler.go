// Package cleanup provides graceful shutdown and resource cleanup functionality
package cleanup

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"dbbackup/internal/logger"
)

// CleanupFunc is a function that performs cleanup with a timeout context
type CleanupFunc func(ctx context.Context) error

// Handler manages graceful shutdown and resource cleanup
type Handler struct {
	ctx    context.Context
	cancel context.CancelFunc

	cleanupFns []cleanupEntry
	mu         sync.Mutex

	shutdownTimeout time.Duration
	log             logger.Logger

	// Track if shutdown has been initiated
	shutdownOnce sync.Once
	shutdownDone chan struct{}
}

type cleanupEntry struct {
	name string
	fn   CleanupFunc
}

// NewHandler creates a shutdown handler
func NewHandler(log logger.Logger) *Handler {
	ctx, cancel := context.WithCancel(context.Background())

	h := &Handler{
		ctx:             ctx,
		cancel:          cancel,
		cleanupFns:      make([]cleanupEntry, 0),
		shutdownTimeout: 30 * time.Second,
		log:             log,
		shutdownDone:    make(chan struct{}),
	}

	return h
}

// Context returns the shutdown context
func (h *Handler) Context() context.Context {
	return h.ctx
}

// RegisterCleanup adds a named cleanup function
func (h *Handler) RegisterCleanup(name string, fn CleanupFunc) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.cleanupFns = append(h.cleanupFns, cleanupEntry{name: name, fn: fn})
}

// SetShutdownTimeout sets the maximum time to wait for cleanup
func (h *Handler) SetShutdownTimeout(d time.Duration) {
	h.shutdownTimeout = d
}

// Shutdown triggers graceful shutdown
func (h *Handler) Shutdown() {
	h.shutdownOnce.Do(func() {
		h.log.Info("Initiating graceful shutdown...")

		// Cancel context first (stops all ongoing operations)
		h.cancel()

		// Run cleanup functions
		h.runCleanup()

		close(h.shutdownDone)
	})
}

// ShutdownWithSignal triggers shutdown due to an OS signal
func (h *Handler) ShutdownWithSignal(sig os.Signal) {
	h.log.Info("Received signal, initiating graceful shutdown", "signal", sig.String())
	h.Shutdown()
}

// Wait blocks until shutdown is complete
func (h *Handler) Wait() {
	<-h.shutdownDone
}

// runCleanup executes all cleanup functions in LIFO order
func (h *Handler) runCleanup() {
	h.mu.Lock()
	fns := make([]cleanupEntry, len(h.cleanupFns))
	copy(fns, h.cleanupFns)
	h.mu.Unlock()

	if len(fns) == 0 {
		h.log.Info("No cleanup functions registered")
		return
	}

	h.log.Info("Running cleanup functions", "count", len(fns))

	// Create timeout context for cleanup
	ctx, cancel := context.WithTimeout(context.Background(), h.shutdownTimeout)
	defer cancel()

	// Run all cleanups in LIFO order (most recently registered first)
	var failed int
	for i := len(fns) - 1; i >= 0; i-- {
		entry := fns[i]

		h.log.Debug("Running cleanup", "name", entry.name)

		if err := entry.fn(ctx); err != nil {
			h.log.Warn("Cleanup function failed", "name", entry.name, "error", err)
			failed++
		} else {
			h.log.Debug("Cleanup completed", "name", entry.name)
		}
	}

	if failed > 0 {
		h.log.Warn("Some cleanup functions failed", "failed", failed, "total", len(fns))
	} else {
		h.log.Info("All cleanup functions completed successfully")
	}
}

// RegisterSignalHandler sets up signal handling for graceful shutdown
func (h *Handler) RegisterSignalHandler() {
	sigChan := make(chan os.Signal, 2)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM, syscall.SIGINT)

	go func() {
		// First signal: graceful shutdown
		sig := <-sigChan
		h.ShutdownWithSignal(sig)

		// Second signal: force exit
		sig = <-sigChan
		h.log.Warn("Received second signal, forcing exit", "signal", sig.String())
		os.Exit(1)
	}()
}

// ChildProcessCleanup creates a cleanup function for killing child processes
func (h *Handler) ChildProcessCleanup() CleanupFunc {
	return func(ctx context.Context) error {
		h.log.Info("Cleaning up orphaned child processes...")

		if err := KillOrphanedProcesses(h.log); err != nil {
			h.log.Warn("Failed to kill some orphaned processes", "error", err)
			return err
		}

		h.log.Info("Child process cleanup complete")
		return nil
	}
}

// DatabasePoolCleanup creates a cleanup function for database connection pools
// poolCloser should be a function that closes the pool
func DatabasePoolCleanup(log logger.Logger, name string, poolCloser func()) CleanupFunc {
	return func(ctx context.Context) error {
		log.Debug("Closing database connection pool", "name", name)
		poolCloser()
		log.Debug("Database connection pool closed", "name", name)
		return nil
	}
}

// FileCleanup creates a cleanup function for file handles
func FileCleanup(log logger.Logger, path string, file *os.File) CleanupFunc {
	return func(ctx context.Context) error {
		if file == nil {
			return nil
		}

		log.Debug("Closing file", "path", path)
		if err := file.Close(); err != nil {
			return fmt.Errorf("failed to close file %s: %w", path, err)
		}
		return nil
	}
}

// TempFileCleanup creates a cleanup function that closes and removes a temp file
func TempFileCleanup(log logger.Logger, file *os.File) CleanupFunc {
	return func(ctx context.Context) error {
		if file == nil {
			return nil
		}

		path := file.Name()
		log.Debug("Removing temporary file", "path", path)

		// Close file first
		if err := file.Close(); err != nil {
			log.Warn("Failed to close temp file", "path", path, "error", err)
		}

		// Remove file
		if err := os.Remove(path); err != nil {
			if !os.IsNotExist(err) {
				return fmt.Errorf("failed to remove temp file %s: %w", path, err)
			}
		}

		log.Debug("Temporary file removed", "path", path)
		return nil
	}
}

// TempDirCleanup creates a cleanup function that removes a temp directory
func TempDirCleanup(log logger.Logger, path string) CleanupFunc {
	return func(ctx context.Context) error {
		if path == "" {
			return nil
		}

		log.Debug("Removing temporary directory", "path", path)

		if err := os.RemoveAll(path); err != nil {
			if !os.IsNotExist(err) {
				return fmt.Errorf("failed to remove temp dir %s: %w", path, err)
			}
		}

		log.Debug("Temporary directory removed", "path", path)
		return nil
	}
}
