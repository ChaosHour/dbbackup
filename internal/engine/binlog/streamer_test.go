package binlog

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestEventTypes(t *testing.T) {
	types := []string{"write", "update", "delete", "query", "gtid", "rotate", "format"}

	for _, eventType := range types {
		t.Run(eventType, func(t *testing.T) {
			event := &Event{Type: eventType}
			if event.Type != eventType {
				t.Errorf("expected %s, got %s", eventType, event.Type)
			}
		})
	}
}

func TestPosition(t *testing.T) {
	pos := Position{
		File:     "mysql-bin.000001",
		Position: 12345,
	}

	if pos.File != "mysql-bin.000001" {
		t.Errorf("expected file mysql-bin.000001, got %s", pos.File)
	}

	if pos.Position != 12345 {
		t.Errorf("expected position 12345, got %d", pos.Position)
	}
}

func TestGTIDPosition(t *testing.T) {
	pos := Position{
		File:     "mysql-bin.000001",
		Position: 12345,
		GTID:     "3E11FA47-71CA-11E1-9E33-C80AA9429562:1-5",
	}

	if pos.GTID == "" {
		t.Error("expected GTID to be set")
	}
}

func TestEvent(t *testing.T) {
	event := &Event{
		Type:      "write",
		Timestamp: time.Now(),
		Database:  "testdb",
		Table:     "users",
		Rows: []map[string]any{
			{"id": 1, "name": "test"},
		},
		RawData: []byte("INSERT INTO users (id, name) VALUES (1, 'test')"),
	}

	if event.Type != "write" {
		t.Errorf("expected write, got %s", event.Type)
	}

	if event.Database != "testdb" {
		t.Errorf("expected database testdb, got %s", event.Database)
	}

	if len(event.Rows) != 1 {
		t.Errorf("expected 1 row, got %d", len(event.Rows))
	}
}

func TestConfig(t *testing.T) {
	cfg := Config{
		Host:           "localhost",
		Port:           3306,
		User:           "repl",
		Password:       "secret",
		ServerID:       99999,
		Flavor:         "mysql",
		BatchMaxEvents: 1000,
		BatchMaxBytes:  10 * 1024 * 1024,
		BatchMaxWait:   time.Second,
		CheckpointEnabled: true,
		CheckpointFile:    "/var/lib/dbbackup/checkpoint",
		UseGTID:        true,
	}

	if cfg.Host != "localhost" {
		t.Errorf("expected host localhost, got %s", cfg.Host)
	}

	if cfg.ServerID != 99999 {
		t.Errorf("expected server ID 99999, got %d", cfg.ServerID)
	}

	if !cfg.UseGTID {
		t.Error("expected GTID to be enabled")
	}
}

// MockTarget implements Target for testing
type MockTarget struct {
	events  []*Event
	healthy bool
	closed  bool
}

func NewMockTarget() *MockTarget {
	return &MockTarget{
		events:  make([]*Event, 0),
		healthy: true,
	}
}

func (m *MockTarget) Name() string {
	return "mock"
}

func (m *MockTarget) Type() string {
	return "mock"
}

func (m *MockTarget) Write(ctx context.Context, events []*Event) error {
	m.events = append(m.events, events...)
	return nil
}

func (m *MockTarget) Flush(ctx context.Context) error {
	return nil
}

func (m *MockTarget) Close() error {
	m.closed = true
	return nil
}

func (m *MockTarget) Healthy() bool {
	return m.healthy
}

func TestMockTarget(t *testing.T) {
	target := NewMockTarget()
	ctx := context.Background()
	events := []*Event{
		{Type: "write", Database: "test", Table: "users"},
		{Type: "update", Database: "test", Table: "users"},
	}

	err := target.Write(ctx, events)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(target.events) != 2 {
		t.Errorf("expected 2 events, got %d", len(target.events))
	}

	if !target.Healthy() {
		t.Error("expected target to be healthy")
	}

	target.Close()
	if !target.closed {
		t.Error("expected target to be closed")
	}
}

func TestFileTargetWrite(t *testing.T) {
	tmpDir := t.TempDir()
	// FileTarget takes a directory path and creates files inside it
	outputDir := filepath.Join(tmpDir, "binlog_output")

	target, err := NewFileTarget(outputDir, 0)
	if err != nil {
		t.Fatalf("failed to create file target: %v", err)
	}
	defer target.Close()

	ctx := context.Background()
	events := []*Event{
		{
			Type:      "write",
			Timestamp: time.Now(),
			Database:  "test",
			Table:     "users",
			Rows:      []map[string]any{{"id": 1}},
		},
	}

	err = target.Write(ctx, events)
	if err != nil {
		t.Fatalf("write error: %v", err)
	}

	err = target.Flush(ctx)
	if err != nil {
		t.Fatalf("flush error: %v", err)
	}

	target.Close()

	// Find the generated file in the output directory
	files, err := os.ReadDir(outputDir)
	if err != nil {
		t.Fatalf("failed to read output dir: %v", err)
	}

	if len(files) == 0 {
		t.Fatal("expected at least one output file")
	}

	// Read the first file
	outputPath := filepath.Join(outputDir, files[0].Name())
	data, err := os.ReadFile(outputPath)
	if err != nil {
		t.Fatalf("failed to read output: %v", err)
	}

	if len(data) == 0 {
		t.Error("expected data in output file")
	}

	// Parse JSON
	var event Event
	err = json.Unmarshal(bytes.TrimSpace(data), &event)
	if err != nil {
		t.Fatalf("failed to parse JSON: %v", err)
	}

	if event.Database != "test" {
		t.Errorf("expected database test, got %s", event.Database)
	}
}

func TestCompressedFileTarget(t *testing.T) {
	tmpDir := t.TempDir()
	outputPath := filepath.Join(tmpDir, "binlog.jsonl.gz")

	target, err := NewCompressedFileTarget(outputPath, 0)
	if err != nil {
		t.Fatalf("failed to create target: %v", err)
	}
	defer target.Close()

	ctx := context.Background()
	events := []*Event{
		{
			Type:      "write",
			Timestamp: time.Now(),
			Database:  "test",
			Table:     "users",
		},
	}

	err = target.Write(ctx, events)
	if err != nil {
		t.Fatalf("write error: %v", err)
	}

	err = target.Flush(ctx)
	if err != nil {
		t.Fatalf("flush error: %v", err)
	}

	target.Close()

	// Verify file exists
	info, err := os.Stat(outputPath)
	if err != nil {
		t.Fatalf("failed to stat output: %v", err)
	}

	if info.Size() == 0 {
		t.Error("expected non-empty compressed file")
	}
}

// Note: StreamerState doesn't have Running field in actual struct
func TestStreamerStatePosition(t *testing.T) {
	state := StreamerState{
		Position: Position{File: "mysql-bin.000001", Position: 12345},
	}

	if state.Position.File != "mysql-bin.000001" {
		t.Errorf("expected file mysql-bin.000001, got %s", state.Position.File)
	}
}

func BenchmarkEventMarshal(b *testing.B) {
	event := &Event{
		Type:      "write",
		Timestamp: time.Now(),
		Database:  "benchmark",
		Table:     "test",
		Rows: []map[string]any{
			{"id": 1, "name": "test", "value": 123.45},
		},
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		json.Marshal(event)
	}
}
