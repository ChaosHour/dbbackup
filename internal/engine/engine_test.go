package engine

import (
	"context"
	"io"
	"testing"
	"time"
)

// MockBackupEngine implements BackupEngine for testing
type MockBackupEngine struct {
	name              string
	description       string
	available         bool
	availReason       string
	supportsRestore   bool
	supportsIncr      bool
	supportsStreaming bool
	backupResult      *BackupResult
	backupError       error
	restoreError      error
}

func (m *MockBackupEngine) Name() string        { return m.name }
func (m *MockBackupEngine) Description() string { return m.description }

func (m *MockBackupEngine) CheckAvailability(ctx context.Context) (*AvailabilityResult, error) {
	return &AvailabilityResult{
		Available: m.available,
		Reason:    m.availReason,
	}, nil
}

func (m *MockBackupEngine) Backup(ctx context.Context, opts *BackupOptions) (*BackupResult, error) {
	if m.backupError != nil {
		return nil, m.backupError
	}
	if m.backupResult != nil {
		return m.backupResult, nil
	}
	return &BackupResult{
		Engine:    m.name,
		StartTime: time.Now().Add(-time.Minute),
		EndTime:   time.Now(),
		TotalSize: 1024 * 1024,
	}, nil
}

func (m *MockBackupEngine) Restore(ctx context.Context, opts *RestoreOptions) error {
	return m.restoreError
}

func (m *MockBackupEngine) SupportsRestore() bool     { return m.supportsRestore }
func (m *MockBackupEngine) SupportsIncremental() bool { return m.supportsIncr }
func (m *MockBackupEngine) SupportsStreaming() bool   { return m.supportsStreaming }

// MockStreamingEngine implements StreamingEngine
type MockStreamingEngine struct {
	MockBackupEngine
	backupToWriterResult *BackupResult
	backupToWriterError  error
}

func (m *MockStreamingEngine) BackupToWriter(ctx context.Context, w io.Writer, opts *BackupOptions) (*BackupResult, error) {
	if m.backupToWriterError != nil {
		return nil, m.backupToWriterError
	}
	if m.backupToWriterResult != nil {
		return m.backupToWriterResult, nil
	}
	// Write some test data
	_, _ = w.Write([]byte("test backup data"))
	return &BackupResult{
		Engine:    m.name,
		StartTime: time.Now().Add(-time.Minute),
		EndTime:   time.Now(),
		TotalSize: 16,
	}, nil
}

func TestRegistryRegisterAndGet(t *testing.T) {
	registry := NewRegistry()

	engine := &MockBackupEngine{
		name:        "test-engine",
		description: "Test backup engine",
		available:   true,
	}

	registry.Register(engine)

	got, err := registry.Get("test-engine")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got == nil {
		t.Fatal("expected to get registered engine")
	}
	if got.Name() != "test-engine" {
		t.Errorf("expected name 'test-engine', got %s", got.Name())
	}
}

func TestRegistryGetNonExistent(t *testing.T) {
	registry := NewRegistry()

	_, err := registry.Get("nonexistent")
	if err == nil {
		t.Error("expected error for non-existent engine")
	}
}

func TestRegistryList(t *testing.T) {
	registry := NewRegistry()

	engine1 := &MockBackupEngine{name: "engine1"}
	engine2 := &MockBackupEngine{name: "engine2"}

	registry.Register(engine1)
	registry.Register(engine2)

	list := registry.List()
	if len(list) != 2 {
		t.Errorf("expected 2 engines, got %d", len(list))
	}
}

func TestRegistryRegisterDuplicate(t *testing.T) {
	registry := NewRegistry()

	engine1 := &MockBackupEngine{name: "test", description: "first"}
	engine2 := &MockBackupEngine{name: "test", description: "second"}

	registry.Register(engine1)
	registry.Register(engine2) // Should replace

	got, _ := registry.Get("test")
	if got.Description() != "second" {
		t.Error("duplicate registration should replace existing engine")
	}
}

func TestBackupResult(t *testing.T) {
	result := &BackupResult{
		Engine:       "test",
		StartTime:    time.Now().Add(-time.Minute),
		EndTime:      time.Now(),
		TotalSize:    1024 * 1024 * 100, // 100 MB
		BinlogFile:   "mysql-bin.000001",
		BinlogPos:    12345,
		GTIDExecuted: "uuid:1-100",
		Files: []BackupFile{
			{
				Path:     "/backup/backup.tar.gz",
				Size:     1024 * 1024 * 100,
				Checksum: "sha256:abc123",
			},
		},
	}

	if result.Engine != "test" {
		t.Errorf("expected engine 'test', got %s", result.Engine)
	}

	if len(result.Files) != 1 {
		t.Errorf("expected 1 file, got %d", len(result.Files))
	}
}

func TestProgress(t *testing.T) {
	progress := Progress{
		Stage:      "copying",
		Percent:    50.0,
		BytesDone:  512 * 1024 * 1024,
		BytesTotal: 1024 * 1024 * 1024,
	}

	if progress.Stage != "copying" {
		t.Errorf("expected stage 'copying', got %s", progress.Stage)
	}

	if progress.Percent != 50.0 {
		t.Errorf("expected percent 50.0, got %f", progress.Percent)
	}
}

func TestAvailabilityResult(t *testing.T) {
	tests := []struct {
		name   string
		result AvailabilityResult
	}{
		{
			name: "available",
			result: AvailabilityResult{
				Available: true,
				Info:      map[string]string{"version": "8.0.30"},
			},
		},
		{
			name: "not available",
			result: AvailabilityResult{
				Available: false,
				Reason:    "MySQL 8.0.17+ required for clone plugin",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if !tt.result.Available && tt.result.Reason == "" {
				t.Error("unavailable result should have a reason")
			}
		})
	}
}

func TestRecoveryTarget(t *testing.T) {
	now := time.Now()

	tests := []struct {
		name   string
		target RecoveryTarget
	}{
		{
			name: "time target",
			target: RecoveryTarget{
				Type: "time",
				Time: now,
			},
		},
		{
			name: "gtid target",
			target: RecoveryTarget{
				Type: "gtid",
				GTID: "uuid:1-100",
			},
		},
		{
			name: "position target",
			target: RecoveryTarget{
				Type: "position",
				File: "mysql-bin.000001",
				Pos:  12345,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.target.Type == "" {
				t.Error("target type should be set")
			}
		})
	}
}

func TestMockEngineBackup(t *testing.T) {
	engine := &MockBackupEngine{
		name:      "mock",
		available: true,
		backupResult: &BackupResult{
			Engine:     "mock",
			TotalSize:  1024,
			BinlogFile: "test",
			BinlogPos:  123,
		},
	}

	ctx := context.Background()
	opts := &BackupOptions{
		OutputDir: "/test",
	}

	result, err := engine.Backup(ctx, opts)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if result.Engine != "mock" {
		t.Errorf("expected engine 'mock', got %s", result.Engine)
	}

	if result.BinlogFile != "test" {
		t.Errorf("expected binlog file 'test', got %s", result.BinlogFile)
	}
}

func TestMockStreamingEngine(t *testing.T) {
	engine := &MockStreamingEngine{
		MockBackupEngine: MockBackupEngine{
			name:              "mock-streaming",
			supportsStreaming: true,
		},
	}

	if !engine.SupportsStreaming() {
		t.Error("expected streaming support")
	}

	ctx := context.Background()
	var buf mockWriter
	opts := &BackupOptions{}

	result, err := engine.BackupToWriter(ctx, &buf, opts)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if result.Engine != "mock-streaming" {
		t.Errorf("expected engine 'mock-streaming', got %s", result.Engine)
	}

	if len(buf.data) == 0 {
		t.Error("expected data to be written")
	}
}

type mockWriter struct {
	data []byte
}

func (m *mockWriter) Write(p []byte) (int, error) {
	m.data = append(m.data, p...)
	return len(p), nil
}

func TestDefaultRegistry(t *testing.T) {
	// DefaultRegistry should be initialized
	if DefaultRegistry == nil {
		t.Error("DefaultRegistry should not be nil")
	}
}

// Benchmark tests
func BenchmarkRegistryGet(b *testing.B) {
	registry := NewRegistry()
	for i := 0; i < 10; i++ {
		registry.Register(&MockBackupEngine{
			name: string(rune('a' + i)),
		})
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = registry.Get("e")
	}
}

func BenchmarkRegistryList(b *testing.B) {
	registry := NewRegistry()
	for i := 0; i < 10; i++ {
		registry.Register(&MockBackupEngine{
			name: string(rune('a' + i)),
		})
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		registry.List()
	}
}
