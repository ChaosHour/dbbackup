// Package performance provides goroutine pool and worker management
package performance

import (
	"context"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

// WorkerPoolConfig configures the worker pool
type WorkerPoolConfig struct {
	// MinWorkers is the minimum number of workers to keep alive
	MinWorkers int

	// MaxWorkers is the maximum number of workers
	MaxWorkers int

	// IdleTimeout is how long a worker can be idle before being terminated
	IdleTimeout time.Duration

	// QueueSize is the size of the work queue
	QueueSize int

	// TaskTimeout is the maximum time for a single task
	TaskTimeout time.Duration
}

// DefaultWorkerPoolConfig returns sensible defaults
func DefaultWorkerPoolConfig() WorkerPoolConfig {
	numCPU := runtime.NumCPU()
	return WorkerPoolConfig{
		MinWorkers:  1,
		MaxWorkers:  numCPU,
		IdleTimeout: 30 * time.Second,
		QueueSize:   numCPU * 4,
		TaskTimeout: 0, // No timeout by default
	}
}

// Task represents a unit of work
type Task func(ctx context.Context) error

// WorkerPool manages a pool of worker goroutines
type WorkerPool struct {
	config WorkerPoolConfig
	taskCh chan taskWrapper
	stopCh chan struct{}
	doneCh chan struct{}
	wg     sync.WaitGroup

	// Metrics
	activeWorkers  atomic.Int64
	pendingTasks   atomic.Int64
	completedTasks atomic.Int64
	failedTasks    atomic.Int64

	// State
	running atomic.Bool
}

type taskWrapper struct {
	task   Task
	ctx    context.Context
	result chan error
}

// NewWorkerPool creates a new worker pool
func NewWorkerPool(config WorkerPoolConfig) *WorkerPool {
	if config.MaxWorkers <= 0 {
		config.MaxWorkers = runtime.NumCPU()
	}
	if config.MinWorkers <= 0 {
		config.MinWorkers = 1
	}
	if config.MinWorkers > config.MaxWorkers {
		config.MinWorkers = config.MaxWorkers
	}
	if config.QueueSize <= 0 {
		config.QueueSize = config.MaxWorkers * 2
	}
	if config.IdleTimeout <= 0 {
		config.IdleTimeout = 30 * time.Second
	}

	return &WorkerPool{
		config: config,
		taskCh: make(chan taskWrapper, config.QueueSize),
		stopCh: make(chan struct{}),
		doneCh: make(chan struct{}),
	}
}

// Start starts the worker pool with minimum workers
func (wp *WorkerPool) Start() {
	if wp.running.Swap(true) {
		return // Already running
	}

	// Start minimum workers
	for i := 0; i < wp.config.MinWorkers; i++ {
		wp.startWorker(true)
	}
}

func (wp *WorkerPool) startWorker(permanent bool) {
	wp.wg.Add(1)
	wp.activeWorkers.Add(1)

	go func() {
		defer wp.wg.Done()
		defer wp.activeWorkers.Add(-1)

		idleTimer := time.NewTimer(wp.config.IdleTimeout)
		defer idleTimer.Stop()

		for {
			select {
			case <-wp.stopCh:
				return

			case task, ok := <-wp.taskCh:
				if !ok {
					return
				}

				wp.pendingTasks.Add(-1)

				// Reset idle timer
				if !idleTimer.Stop() {
					select {
					case <-idleTimer.C:
					default:
					}
				}
				idleTimer.Reset(wp.config.IdleTimeout)

				// Execute task
				var err error
				if wp.config.TaskTimeout > 0 {
					ctx, cancel := context.WithTimeout(task.ctx, wp.config.TaskTimeout)
					err = task.task(ctx)
					cancel()
				} else {
					err = task.task(task.ctx)
				}

				if err != nil {
					wp.failedTasks.Add(1)
				} else {
					wp.completedTasks.Add(1)
				}

				if task.result != nil {
					task.result <- err
				}

			case <-idleTimer.C:
				// Only exit if we're not a permanent worker and above minimum
				if !permanent && wp.activeWorkers.Load() > int64(wp.config.MinWorkers) {
					return
				}
				idleTimer.Reset(wp.config.IdleTimeout)
			}
		}
	}()
}

// Submit submits a task to the pool and blocks until it completes
func (wp *WorkerPool) Submit(ctx context.Context, task Task) error {
	if !wp.running.Load() {
		return context.Canceled
	}

	result := make(chan error, 1)
	tw := taskWrapper{
		task:   task,
		ctx:    ctx,
		result: result,
	}

	wp.pendingTasks.Add(1)

	// Try to scale up if queue is getting full
	if wp.pendingTasks.Load() > int64(wp.config.QueueSize/2) {
		if wp.activeWorkers.Load() < int64(wp.config.MaxWorkers) {
			wp.startWorker(false)
		}
	}

	select {
	case wp.taskCh <- tw:
	case <-ctx.Done():
		wp.pendingTasks.Add(-1)
		return ctx.Err()
	case <-wp.stopCh:
		wp.pendingTasks.Add(-1)
		return context.Canceled
	}

	select {
	case err := <-result:
		return err
	case <-ctx.Done():
		return ctx.Err()
	case <-wp.stopCh:
		return context.Canceled
	}
}

// SubmitAsync submits a task without waiting for completion
func (wp *WorkerPool) SubmitAsync(ctx context.Context, task Task) bool {
	if !wp.running.Load() {
		return false
	}

	tw := taskWrapper{
		task:   task,
		ctx:    ctx,
		result: nil, // No result channel for async
	}

	select {
	case wp.taskCh <- tw:
		wp.pendingTasks.Add(1)
		return true
	default:
		return false
	}
}

// Stop gracefully stops the worker pool
func (wp *WorkerPool) Stop() {
	if !wp.running.Swap(false) {
		return // Already stopped
	}

	close(wp.stopCh)
	close(wp.taskCh)
	wp.wg.Wait()
	close(wp.doneCh)
}

// Wait waits for all tasks to complete
func (wp *WorkerPool) Wait() {
	<-wp.doneCh
}

// Stats returns current pool statistics
func (wp *WorkerPool) Stats() WorkerPoolStats {
	return WorkerPoolStats{
		ActiveWorkers:  int(wp.activeWorkers.Load()),
		PendingTasks:   int(wp.pendingTasks.Load()),
		CompletedTasks: int(wp.completedTasks.Load()),
		FailedTasks:    int(wp.failedTasks.Load()),
		MaxWorkers:     wp.config.MaxWorkers,
		QueueSize:      wp.config.QueueSize,
	}
}

// WorkerPoolStats contains pool statistics
type WorkerPoolStats struct {
	ActiveWorkers  int
	PendingTasks   int
	CompletedTasks int
	FailedTasks    int
	MaxWorkers     int
	QueueSize      int
}

// Semaphore provides a bounded concurrency primitive
type Semaphore struct {
	ch chan struct{}
}

// NewSemaphore creates a new semaphore with the given limit
func NewSemaphore(limit int) *Semaphore {
	if limit <= 0 {
		limit = 1
	}
	return &Semaphore{
		ch: make(chan struct{}, limit),
	}
}

// Acquire acquires a semaphore slot
func (s *Semaphore) Acquire(ctx context.Context) error {
	select {
	case s.ch <- struct{}{}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// TryAcquire tries to acquire a slot without blocking
func (s *Semaphore) TryAcquire() bool {
	select {
	case s.ch <- struct{}{}:
		return true
	default:
		return false
	}
}

// Release releases a semaphore slot
func (s *Semaphore) Release() {
	select {
	case <-s.ch:
	default:
		// No slot to release - this is a programming error
		panic("semaphore: release without acquire")
	}
}

// Available returns the number of available slots
func (s *Semaphore) Available() int {
	return cap(s.ch) - len(s.ch)
}

// ParallelExecutor executes functions in parallel with bounded concurrency
type ParallelExecutor struct {
	sem    *Semaphore
	wg     sync.WaitGroup
	mu     sync.Mutex
	errors []error
}

// NewParallelExecutor creates a new parallel executor with the given concurrency limit
func NewParallelExecutor(concurrency int) *ParallelExecutor {
	if concurrency <= 0 {
		concurrency = runtime.NumCPU()
	}
	return &ParallelExecutor{
		sem: NewSemaphore(concurrency),
	}
}

// Execute runs the function in a goroutine, respecting concurrency limits
func (pe *ParallelExecutor) Execute(ctx context.Context, fn func() error) {
	pe.wg.Add(1)

	go func() {
		defer pe.wg.Done()

		if err := pe.sem.Acquire(ctx); err != nil {
			pe.mu.Lock()
			pe.errors = append(pe.errors, err)
			pe.mu.Unlock()
			return
		}
		defer pe.sem.Release()

		if err := fn(); err != nil {
			pe.mu.Lock()
			pe.errors = append(pe.errors, err)
			pe.mu.Unlock()
		}
	}()
}

// Wait waits for all executions to complete and returns any errors
func (pe *ParallelExecutor) Wait() []error {
	pe.wg.Wait()
	pe.mu.Lock()
	defer pe.mu.Unlock()
	return pe.errors
}

// FirstError returns the first error encountered, if any
func (pe *ParallelExecutor) FirstError() error {
	pe.mu.Lock()
	defer pe.mu.Unlock()
	if len(pe.errors) > 0 {
		return pe.errors[0]
	}
	return nil
}
