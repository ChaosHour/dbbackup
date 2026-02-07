package restore

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"dbbackup/internal/config"
	"dbbackup/internal/logger"
)

// mockProgressLogger implements logger.Logger for testing
type mockProgressLogger struct {
	logs []string
}

func (m *mockProgressLogger) Info(msg string, args ...any)                          { m.logs = append(m.logs, msg) }
func (m *mockProgressLogger) Warn(msg string, args ...any)                          { m.logs = append(m.logs, msg) }
func (m *mockProgressLogger) Error(msg string, args ...any)                         { m.logs = append(m.logs, msg) }
func (m *mockProgressLogger) Debug(msg string, args ...any)                         { m.logs = append(m.logs, msg) }
func (m *mockProgressLogger) Fatal(msg string, args ...any)                         {}
func (m *mockProgressLogger) StartOperation(name string) logger.OperationLogger    { return &mockOperation{} }
func (m *mockProgressLogger) WithFields(fields map[string]any) logger.Logger       { return m }
func (m *mockProgressLogger) WithField(key string, value any) logger.Logger        { return m }
func (m *mockProgressLogger) Time(msg string, args ...any)                          {}

type mockOperation struct{}

func (o *mockOperation) Update(msg string, args ...any)   {}
func (o *mockOperation) Complete(msg string, args ...any) {}
func (o *mockOperation) Fail(msg string, args ...any)     {}

func TestSetDebugLogPath(t *testing.T) {
	cfg := &config.Config{}
	log := &mockProgressLogger{}
	e := New(cfg, log, nil)

	e.SetDebugLogPath("/tmp/debug.log")

	if e.debugLogPath != "/tmp/debug.log" {
		t.Errorf("expected debugLogPath=/tmp/debug.log, got %s", e.debugLogPath)
	}
}

func TestSetProgressCallback(t *testing.T) {
	cfg := &config.Config{}
	log := &mockProgressLogger{}
	e := New(cfg, log, nil)

	called := false
	e.SetProgressCallback(func(current, total int64, description string) {
		called = true
	})

	// Trigger callback
	e.reportProgress(50, 100, "test")

	if !called {
		t.Error("progress callback was not called")
	}
}

func TestSetDatabaseProgressCallback(t *testing.T) {
	cfg := &config.Config{}
	log := &mockProgressLogger{}
	e := New(cfg, log, nil)

	var gotDone, gotTotal int
	var gotName string

	e.SetDatabaseProgressCallback(func(done, total int, dbName string) {
		gotDone = done
		gotTotal = total
		gotName = dbName
	})

	e.reportDatabaseProgress(5, 10, "testdb")

	if gotDone != 5 || gotTotal != 10 || gotName != "testdb" {
		t.Errorf("unexpected values: done=%d, total=%d, name=%s", gotDone, gotTotal, gotName)
	}
}

func TestSetDatabaseProgressWithTimingCallback(t *testing.T) {
	cfg := &config.Config{}
	log := &mockProgressLogger{}
	e := New(cfg, log, nil)

	called := false
	e.SetDatabaseProgressWithTimingCallback(func(done, total int, dbName string, phaseElapsed, avgPerDB time.Duration) {
		called = true
		if done != 3 || total != 6 {
			t.Errorf("expected done=3, total=6, got done=%d, total=%d", done, total)
		}
	})

	e.reportDatabaseProgressWithTiming(3, 6, "db", time.Second, time.Millisecond*500)

	if !called {
		t.Error("timing callback was not called")
	}
}

func TestSetDatabaseProgressByBytesCallback(t *testing.T) {
	cfg := &config.Config{}
	log := &mockProgressLogger{}
	e := New(cfg, log, nil)

	var gotBytesDone, gotBytesTotal int64
	e.SetDatabaseProgressByBytesCallback(func(bytesDone, bytesTotal int64, dbName string, dbDone, dbTotal int) {
		gotBytesDone = bytesDone
		gotBytesTotal = bytesTotal
	})

	e.reportDatabaseProgressByBytes(1000, 5000, "bigdb", 1, 3)

	if gotBytesDone != 1000 || gotBytesTotal != 5000 {
		t.Errorf("expected 1000/5000, got %d/%d", gotBytesDone, gotBytesTotal)
	}
}

func TestReportProgressWithoutCallback(t *testing.T) {
	cfg := &config.Config{}
	log := &mockProgressLogger{}
	e := New(cfg, log, nil)

	// Should not panic when no callback is set
	e.reportProgress(100, 200, "test")
}

func TestReportDatabaseProgressWithoutCallback(t *testing.T) {
	cfg := &config.Config{}
	log := &mockProgressLogger{}
	e := New(cfg, log, nil)

	// Should not panic when no callback is set
	e.reportDatabaseProgress(1, 2, "db")
}

func TestReportDatabaseProgressPanicRecovery(t *testing.T) {
	cfg := &config.Config{}
	log := &mockProgressLogger{}
	e := New(cfg, log, nil)

	// Set a callback that panics
	e.SetDatabaseProgressCallback(func(done, total int, dbName string) {
		panic("simulated panic")
	})

	// Should not propagate panic
	e.reportDatabaseProgress(1, 2, "db")

	// If we get here, panic was recovered
}

func TestGetLiveBytes(t *testing.T) {
	cfg := &config.Config{}
	log := &mockProgressLogger{}
	e := New(cfg, log, nil)

	// Set values using atomic
	atomic.StoreInt64(&e.liveBytesDone, 12345)
	atomic.StoreInt64(&e.liveBytesTotal, 99999)

	done, total := e.GetLiveBytes()

	if done != 12345 {
		t.Errorf("expected done=12345, got %d", done)
	}
	if total != 99999 {
		t.Errorf("expected total=99999, got %d", total)
	}
}

func TestSetLiveBytesTotal(t *testing.T) {
	cfg := &config.Config{}
	log := &mockProgressLogger{}
	e := New(cfg, log, nil)

	e.SetLiveBytesTotal(50000)

	if atomic.LoadInt64(&e.liveBytesTotal) != 50000 {
		t.Errorf("expected liveBytesTotal=50000, got %d", e.liveBytesTotal)
	}
}

func TestMonitorRestoreProgress(t *testing.T) {
	cfg := &config.Config{}
	log := &mockProgressLogger{}
	e := New(cfg, log, nil)

	// Set up callback to count calls (atomic to avoid data race with monitor goroutine)
	var callCount int64
	e.SetDatabaseProgressByBytesCallback(func(bytesDone, bytesTotal int64, dbName string, dbDone, dbTotal int) {
		atomic.AddInt64(&callCount, 1)
	})

	// Set total bytes
	e.SetLiveBytesTotal(1000)
	atomic.StoreInt64(&e.liveBytesDone, 500)

	// Run monitor briefly
	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()

	go e.monitorRestoreProgress(ctx, 0, 50*time.Millisecond)

	<-ctx.Done()
	time.Sleep(10 * time.Millisecond) // Let goroutine finish

	// Should have been called at least once
	if atomic.LoadInt64(&callCount) < 1 {
		t.Errorf("expected at least 1 callback, got %d", atomic.LoadInt64(&callCount))
	}
}

func TestLoggerAdapter(t *testing.T) {
	log := &mockProgressLogger{}
	adapter := &loggerAdapter{logger: log}

	adapter.Info("info msg")
	adapter.Warn("warn msg")
	adapter.Error("error msg")
	adapter.Debug("debug msg")

	if len(log.logs) != 4 {
		t.Errorf("expected 4 log entries, got %d", len(log.logs))
	}
}
