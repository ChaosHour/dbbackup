// Package native provides panic recovery utilities for native database engines
package native

import (
	"fmt"
	"log"
	"runtime/debug"
	"sync"
)

// PanicRecovery wraps any function with panic recovery
func PanicRecovery(name string, fn func() error) error {
	var err error

	func() {
		defer func() {
			if r := recover(); r != nil {
				log.Printf("PANIC in %s: %v", name, r)
				log.Printf("Stack trace:\n%s", debug.Stack())
				err = fmt.Errorf("panic in %s: %v", name, r)
			}
		}()

		err = fn()
	}()

	return err
}

// SafeGoroutine starts a goroutine with panic recovery
func SafeGoroutine(name string, fn func()) {
	go func() {
		defer func() {
			if r := recover(); r != nil {
				log.Printf("PANIC in goroutine %s: %v", name, r)
				log.Printf("Stack trace:\n%s", debug.Stack())
			}
		}()

		fn()
	}()
}

// SafeChannel sends to channel with panic recovery (non-blocking)
func SafeChannel[T any](ch chan<- T, val T, name string) bool {
	defer func() {
		if r := recover(); r != nil {
			log.Printf("PANIC sending to channel %s: %v", name, r)
		}
	}()

	select {
	case ch <- val:
		return true
	default:
		// Channel full or closed, drop message
		return false
	}
}

// SafeCallback wraps a callback function with panic recovery
func SafeCallback[T any](name string, cb func(T), val T) {
	if cb == nil {
		return
	}

	defer func() {
		if r := recover(); r != nil {
			log.Printf("PANIC in callback %s: %v", name, r)
			log.Printf("Stack trace:\n%s", debug.Stack())
		}
	}()

	cb(val)
}

// SafeCallbackWithMutex wraps a callback with mutex protection and panic recovery
type SafeCallbackWrapper[T any] struct {
	mu       sync.RWMutex
	callback func(T)
	stopped  bool
}

// NewSafeCallbackWrapper creates a new safe callback wrapper
func NewSafeCallbackWrapper[T any]() *SafeCallbackWrapper[T] {
	return &SafeCallbackWrapper[T]{}
}

// Set sets the callback function
func (w *SafeCallbackWrapper[T]) Set(cb func(T)) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.callback = cb
	w.stopped = false
}

// Stop stops the callback from being called
func (w *SafeCallbackWrapper[T]) Stop() {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.stopped = true
	w.callback = nil
}

// Call safely calls the callback if it's set and not stopped
func (w *SafeCallbackWrapper[T]) Call(val T) {
	w.mu.RLock()
	if w.stopped || w.callback == nil {
		w.mu.RUnlock()
		return
	}
	cb := w.callback
	w.mu.RUnlock()

	// Call with panic recovery
	defer func() {
		if r := recover(); r != nil {
			log.Printf("PANIC in safe callback: %v", r)
		}
	}()

	cb(val)
}

// IsStopped returns whether the callback is stopped
func (w *SafeCallbackWrapper[T]) IsStopped() bool {
	w.mu.RLock()
	defer w.mu.RUnlock()
	return w.stopped
}
