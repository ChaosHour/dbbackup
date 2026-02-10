package restore

import (
	"io"
	"testing"
	"time"
)

func TestProgressReaderRead(t *testing.T) {
	data := []byte("hello world this is test data for progress reader")
	reader := &progressReader{
		reader:      &mockReader{data: data},
		totalSize:   int64(len(data)),
		callback:    nil,
		desc:        "test",
		reportEvery: 10 * time.Millisecond,
	}

	buf := make([]byte, 10)
	n, err := reader.Read(buf)

	if err != nil && err != io.EOF {
		t.Errorf("unexpected error: %v", err)
	}
	if n != 10 {
		t.Errorf("expected n=10, got %d", n)
	}
	if reader.bytesRead != 10 {
		t.Errorf("expected bytesRead=10, got %d", reader.bytesRead)
	}
}

func TestProgressReaderWithCallback(t *testing.T) {
	data := []byte("test data for callback testing")
	callbackCalled := false
	var reportedCurrent, reportedTotal int64

	reader := &progressReader{
		reader:    &mockReader{data: data},
		totalSize: int64(len(data)),
		callback: func(current, total int64, desc string) {
			callbackCalled = true
			reportedCurrent = current
			reportedTotal = total
		},
		desc:        "testing",
		reportEvery: 0, // Report immediately
		lastReport:  time.Time{},
	}

	buf := make([]byte, len(data))
	_, _ = reader.Read(buf)

	if !callbackCalled {
		t.Error("callback was not called")
	}
	if reportedTotal != int64(len(data)) {
		t.Errorf("expected total=%d, got %d", len(data), reportedTotal)
	}
	if reportedCurrent <= 0 {
		t.Error("expected current > 0")
	}
}

func TestProgressReaderThrottling(t *testing.T) {
	data := make([]byte, 1000)
	callCount := 0

	reader := &progressReader{
		reader:    &mockReader{data: data},
		totalSize: int64(len(data)),
		callback: func(current, total int64, desc string) {
			callCount++
		},
		desc:        "throttle test",
		reportEvery: 100 * time.Millisecond, // Long throttle
		lastReport:  time.Now(),             // Just reported
	}

	// Read multiple times quickly
	buf := make([]byte, 100)
	for i := 0; i < 5; i++ {
		reader.Read(buf)
	}

	// Should not have called callback due to throttling
	if callCount > 1 {
		t.Errorf("expected throttled calls, got %d", callCount)
	}
}

// mockReader is a simple io.Reader for testing
type mockReader struct {
	data   []byte
	offset int
}

func (m *mockReader) Read(p []byte) (n int, err error) {
	if m.offset >= len(m.data) {
		return 0, io.EOF
	}
	n = copy(p, m.data[m.offset:])
	m.offset += n
	return n, nil
}
