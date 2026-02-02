package performance

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

func TestWorkerPool(t *testing.T) {
	t.Run("BasicOperation", func(t *testing.T) {
		pool := NewWorkerPool(DefaultWorkerPoolConfig())
		pool.Start()
		defer pool.Stop()

		var counter atomic.Int64

		err := pool.Submit(context.Background(), func(ctx context.Context) error {
			counter.Add(1)
			return nil
		})

		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if counter.Load() != 1 {
			t.Errorf("expected counter 1, got %d", counter.Load())
		}
	})

	t.Run("ConcurrentTasks", func(t *testing.T) {
		config := DefaultWorkerPoolConfig()
		config.MaxWorkers = 4
		pool := NewWorkerPool(config)
		pool.Start()
		defer pool.Stop()

		var counter atomic.Int64
		numTasks := 100
		done := make(chan struct{}, numTasks)

		for i := 0; i < numTasks; i++ {
			go func() {
				err := pool.Submit(context.Background(), func(ctx context.Context) error {
					counter.Add(1)
					time.Sleep(time.Millisecond)
					return nil
				})
				if err != nil {
					t.Errorf("unexpected error: %v", err)
				}
				done <- struct{}{}
			}()
		}

		// Wait for all tasks
		for i := 0; i < numTasks; i++ {
			<-done
		}

		if counter.Load() != int64(numTasks) {
			t.Errorf("expected counter %d, got %d", numTasks, counter.Load())
		}
	})

	t.Run("ContextCancellation", func(t *testing.T) {
		config := DefaultWorkerPoolConfig()
		config.MaxWorkers = 1
		config.QueueSize = 1
		pool := NewWorkerPool(config)
		pool.Start()
		defer pool.Stop()

		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		err := pool.Submit(ctx, func(ctx context.Context) error {
			time.Sleep(time.Second)
			return nil
		})

		if err != context.Canceled {
			t.Errorf("expected context.Canceled, got %v", err)
		}
	})

	t.Run("ErrorPropagation", func(t *testing.T) {
		pool := NewWorkerPool(DefaultWorkerPoolConfig())
		pool.Start()
		defer pool.Stop()

		expectedErr := errors.New("test error")

		err := pool.Submit(context.Background(), func(ctx context.Context) error {
			return expectedErr
		})

		if err != expectedErr {
			t.Errorf("expected %v, got %v", expectedErr, err)
		}
	})

	t.Run("Stats", func(t *testing.T) {
		pool := NewWorkerPool(DefaultWorkerPoolConfig())
		pool.Start()

		// Submit some successful tasks
		for i := 0; i < 5; i++ {
			pool.Submit(context.Background(), func(ctx context.Context) error {
				return nil
			})
		}

		// Submit some failing tasks
		for i := 0; i < 3; i++ {
			pool.Submit(context.Background(), func(ctx context.Context) error {
				return errors.New("fail")
			})
		}

		pool.Stop()

		stats := pool.Stats()
		if stats.CompletedTasks != 5 {
			t.Errorf("expected 5 completed, got %d", stats.CompletedTasks)
		}
		if stats.FailedTasks != 3 {
			t.Errorf("expected 3 failed, got %d", stats.FailedTasks)
		}
	})
}

func TestSemaphore(t *testing.T) {
	t.Run("BasicAcquireRelease", func(t *testing.T) {
		sem := NewSemaphore(2)

		if sem.Available() != 2 {
			t.Errorf("expected 2 available, got %d", sem.Available())
		}

		if err := sem.Acquire(context.Background()); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if sem.Available() != 1 {
			t.Errorf("expected 1 available, got %d", sem.Available())
		}

		sem.Release()

		if sem.Available() != 2 {
			t.Errorf("expected 2 available, got %d", sem.Available())
		}
	})

	t.Run("TryAcquire", func(t *testing.T) {
		sem := NewSemaphore(1)

		if !sem.TryAcquire() {
			t.Error("expected TryAcquire to succeed")
		}

		if sem.TryAcquire() {
			t.Error("expected TryAcquire to fail")
		}

		sem.Release()

		if !sem.TryAcquire() {
			t.Error("expected TryAcquire to succeed after release")
		}

		sem.Release()
	})

	t.Run("ContextCancellation", func(t *testing.T) {
		sem := NewSemaphore(1)
		sem.Acquire(context.Background()) // Exhaust the semaphore

		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		err := sem.Acquire(ctx)
		if err != context.Canceled {
			t.Errorf("expected context.Canceled, got %v", err)
		}

		sem.Release()
	})
}

func TestParallelExecutor(t *testing.T) {
	t.Run("BasicParallel", func(t *testing.T) {
		pe := NewParallelExecutor(4)

		var counter atomic.Int64

		for i := 0; i < 10; i++ {
			pe.Execute(context.Background(), func() error {
				counter.Add(1)
				return nil
			})
		}

		errs := pe.Wait()

		if len(errs) != 0 {
			t.Errorf("expected no errors, got %d", len(errs))
		}

		if counter.Load() != 10 {
			t.Errorf("expected counter 10, got %d", counter.Load())
		}
	})

	t.Run("ErrorCollection", func(t *testing.T) {
		pe := NewParallelExecutor(4)

		for i := 0; i < 5; i++ {
			idx := i
			pe.Execute(context.Background(), func() error {
				if idx%2 == 0 {
					return errors.New("error")
				}
				return nil
			})
		}

		errs := pe.Wait()

		if len(errs) != 3 { // 0, 2, 4 should fail
			t.Errorf("expected 3 errors, got %d", len(errs))
		}
	})

	t.Run("FirstError", func(t *testing.T) {
		pe := NewParallelExecutor(1) // Sequential to ensure order

		pe.Execute(context.Background(), func() error {
			return errors.New("some error")
		})
		pe.Execute(context.Background(), func() error {
			return errors.New("another error")
		})

		pe.Wait()

		// FirstError should return one of the errors (order may vary due to goroutines)
		if pe.FirstError() == nil {
			t.Error("expected an error, got nil")
		}
	})
}

// Benchmarks

func BenchmarkWorkerPoolSubmit(b *testing.B) {
	pool := NewWorkerPool(DefaultWorkerPoolConfig())
	pool.Start()
	defer pool.Stop()

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		pool.Submit(context.Background(), func(ctx context.Context) error {
			return nil
		})
	}
}

func BenchmarkWorkerPoolParallel(b *testing.B) {
	pool := NewWorkerPool(DefaultWorkerPoolConfig())
	pool.Start()
	defer pool.Stop()

	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			pool.Submit(context.Background(), func(ctx context.Context) error {
				return nil
			})
		}
	})
}

func BenchmarkSemaphoreAcquireRelease(b *testing.B) {
	sem := NewSemaphore(100)
	ctx := context.Background()

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		sem.Acquire(ctx)
		sem.Release()
	}
}

func BenchmarkSemaphoreParallel(b *testing.B) {
	sem := NewSemaphore(100)
	ctx := context.Background()

	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			sem.Acquire(ctx)
			sem.Release()
		}
	})
}

func BenchmarkParallelExecutor(b *testing.B) {
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		pe := NewParallelExecutor(4)
		for j := 0; j < 10; j++ {
			pe.Execute(context.Background(), func() error {
				return nil
			})
		}
		pe.Wait()
	}
}
