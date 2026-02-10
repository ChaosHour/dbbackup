package backup

import (
	"regexp"
	"testing"

	"dbbackup/internal/logger"
)

// mockLogger implements logger.Logger for testing
type mockLogger struct{}

func (m *mockLogger) Debug(msg string, args ...interface{})                  {}
func (m *mockLogger) Info(msg string, args ...interface{})                   {}
func (m *mockLogger) Warn(msg string, args ...interface{})                   {}
func (m *mockLogger) Error(msg string, args ...interface{})                  {}
func (m *mockLogger) Time(msg string, args ...any)                           {}
func (m *mockLogger) WithFields(fields map[string]interface{}) logger.Logger { return m }
func (m *mockLogger) WithField(key string, value interface{}) logger.Logger  { return m }
func (m *mockLogger) StartOperation(name string) logger.OperationLogger      { return &mockOpLogger{} }

type mockOpLogger struct{}

func (m *mockOpLogger) Update(msg string, args ...any)   {}
func (m *mockOpLogger) Complete(msg string, args ...any) {}
func (m *mockOpLogger) Fail(msg string, args ...any)     {}

func TestNewTableBackup(t *testing.T) {
	cfg := &TableBackupConfig{}
	log := &mockLogger{}

	tb, err := NewTableBackup(cfg, log)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if tb.config.Port != 5432 {
		t.Errorf("expected default port 5432, got %d", tb.config.Port)
	}
	if tb.config.BatchSize != 10000 {
		t.Errorf("expected default batch size 10000, got %d", tb.config.BatchSize)
	}
	if tb.config.Parallel != 1 {
		t.Errorf("expected default parallel 1, got %d", tb.config.Parallel)
	}
}

func TestNewTableBackupWithConfig(t *testing.T) {
	cfg := &TableBackupConfig{
		Host:      "localhost",
		Port:      5433,
		User:      "backup",
		Database:  "mydb",
		BatchSize: 5000,
		Parallel:  4,
	}
	log := &mockLogger{}

	tb, err := NewTableBackup(cfg, log)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if tb.config.Port != 5433 {
		t.Errorf("expected port 5433, got %d", tb.config.Port)
	}
	if tb.config.BatchSize != 5000 {
		t.Errorf("expected batch size 5000, got %d", tb.config.BatchSize)
	}
}

func TestMatchesFiltersNoFilters(t *testing.T) {
	cfg := &TableBackupConfig{}
	log := &mockLogger{}
	tb, _ := NewTableBackup(cfg, log)

	info := &TableInfo{
		Schema:   "public",
		Name:     "users",
		FullName: "public.users",
		RowCount: 1000,
	}

	if !tb.matchesFilters(info, nil) {
		t.Error("expected table to match with no filters")
	}
}

func TestMatchesFiltersIncludeSchemas(t *testing.T) {
	cfg := &TableBackupConfig{
		IncludeSchemas: []string{"public", "app"},
	}
	log := &mockLogger{}
	tb, _ := NewTableBackup(cfg, log)

	tests := []struct {
		schema   string
		expected bool
	}{
		{"public", true},
		{"app", true},
		{"private", false},
		{"pg_catalog", false},
	}

	for _, tc := range tests {
		info := &TableInfo{Schema: tc.schema, Name: "test", FullName: tc.schema + ".test"}
		result := tb.matchesFilters(info, nil)
		if result != tc.expected {
			t.Errorf("schema %q: expected %v, got %v", tc.schema, tc.expected, result)
		}
	}
}

func TestMatchesFiltersExcludeSchemas(t *testing.T) {
	cfg := &TableBackupConfig{
		ExcludeSchemas: []string{"temp", "cache"},
	}
	log := &mockLogger{}
	tb, _ := NewTableBackup(cfg, log)

	tests := []struct {
		schema   string
		expected bool
	}{
		{"public", true},
		{"app", true},
		{"temp", false},
		{"cache", false},
	}

	for _, tc := range tests {
		info := &TableInfo{Schema: tc.schema, Name: "test", FullName: tc.schema + ".test"}
		result := tb.matchesFilters(info, nil)
		if result != tc.expected {
			t.Errorf("schema %q: expected %v, got %v", tc.schema, tc.expected, result)
		}
	}
}

func TestMatchesFiltersIncludeTables(t *testing.T) {
	cfg := &TableBackupConfig{
		IncludeTables: []string{"public.users", "orders"},
	}
	log := &mockLogger{}
	tb, _ := NewTableBackup(cfg, log)

	tests := []struct {
		fullName string
		name     string
		expected bool
	}{
		{"public.users", "users", true},
		{"public.orders", "orders", true},
		{"app.orders", "orders", true}, // matches by name alone
		{"public.products", "products", false},
	}

	for _, tc := range tests {
		info := &TableInfo{Schema: "public", Name: tc.name, FullName: tc.fullName}
		result := tb.matchesFilters(info, nil)
		if result != tc.expected {
			t.Errorf("table %q: expected %v, got %v", tc.fullName, tc.expected, result)
		}
	}
}

func TestMatchesFiltersExcludeTables(t *testing.T) {
	cfg := &TableBackupConfig{
		ExcludeTables: []string{"public.logs", "sessions"},
	}
	log := &mockLogger{}
	tb, _ := NewTableBackup(cfg, log)

	tests := []struct {
		fullName string
		name     string
		expected bool
	}{
		{"public.users", "users", true},
		{"public.logs", "logs", false},
		{"app.sessions", "sessions", false},
		{"public.orders", "orders", true},
	}

	for _, tc := range tests {
		info := &TableInfo{Schema: "public", Name: tc.name, FullName: tc.fullName}
		result := tb.matchesFilters(info, nil)
		if result != tc.expected {
			t.Errorf("table %q: expected %v, got %v", tc.fullName, tc.expected, result)
		}
	}
}

func TestMatchesFiltersPattern(t *testing.T) {
	cfg := &TableBackupConfig{}
	log := &mockLogger{}
	tb, _ := NewTableBackup(cfg, log)

	pattern := regexp.MustCompile(`^public\.audit_.*`)

	tests := []struct {
		fullName string
		expected bool
	}{
		{"public.audit_log", true},
		{"public.audit_events", true},
		{"public.audit_access", true},
		{"public.users", false},
		{"app.audit_log", false},
	}

	for _, tc := range tests {
		info := &TableInfo{FullName: tc.fullName}
		result := tb.matchesFilters(info, pattern)
		if result != tc.expected {
			t.Errorf("table %q with pattern: expected %v, got %v", tc.fullName, tc.expected, result)
		}
	}
}

func TestMatchesFiltersRowCount(t *testing.T) {
	tests := []struct {
		minRows  int64
		maxRows  int64
		rowCount int64
		expected bool
	}{
		{0, 0, 1000, true},        // No filters
		{100, 0, 1000, true},      // Min only, passes
		{100, 0, 50, false},       // Min only, fails
		{0, 5000, 1000, true},     // Max only, passes
		{0, 5000, 10000, false},   // Max only, fails
		{100, 5000, 1000, true},   // Both, passes
		{100, 5000, 50, false},    // Both, fails min
		{100, 5000, 10000, false}, // Both, fails max
	}

	for i, tc := range tests {
		cfg := &TableBackupConfig{
			MinRows: tc.minRows,
			MaxRows: tc.maxRows,
		}
		log := &mockLogger{}
		tb, _ := NewTableBackup(cfg, log)

		info := &TableInfo{
			Schema:   "public",
			Name:     "test",
			FullName: "public.test",
			RowCount: tc.rowCount,
		}

		result := tb.matchesFilters(info, nil)
		if result != tc.expected {
			t.Errorf("test %d: minRows=%d, maxRows=%d, rowCount=%d: expected %v, got %v",
				i, tc.minRows, tc.maxRows, tc.rowCount, tc.expected, result)
		}
	}
}

func TestMatchesFiltersCombined(t *testing.T) {
	cfg := &TableBackupConfig{
		IncludeSchemas: []string{"public"},
		ExcludeTables:  []string{"public.logs"},
		MinRows:        100,
	}
	log := &mockLogger{}
	tb, _ := NewTableBackup(cfg, log)

	tests := []struct {
		schema   string
		name     string
		rowCount int64
		expected bool
	}{
		{"public", "users", 1000, true},
		{"public", "logs", 1000, false},   // Excluded table
		{"private", "users", 1000, false}, // Wrong schema
		{"public", "users", 50, false},    // Too few rows
	}

	for _, tc := range tests {
		info := &TableInfo{
			Schema:   tc.schema,
			Name:     tc.name,
			FullName: tc.schema + "." + tc.name,
			RowCount: tc.rowCount,
		}

		result := tb.matchesFilters(info, nil)
		if result != tc.expected {
			t.Errorf("table %s.%s (rows=%d): expected %v, got %v",
				tc.schema, tc.name, tc.rowCount, tc.expected, result)
		}
	}
}

func TestTableBackupClose(t *testing.T) {
	cfg := &TableBackupConfig{}
	log := &mockLogger{}
	tb, _ := NewTableBackup(cfg, log)

	// Should not panic when pool is nil
	tb.Close()
}

func TestTableInfoFullName(t *testing.T) {
	info := TableInfo{
		Schema: "public",
		Name:   "users",
	}
	info.FullName = info.Schema + "." + info.Name

	if info.FullName != "public.users" {
		t.Errorf("expected 'public.users', got %q", info.FullName)
	}
}

func TestColumnInfoPosition(t *testing.T) {
	cols := []ColumnInfo{
		{Name: "id", DataType: "integer", Position: 1, IsPrimaryKey: true},
		{Name: "name", DataType: "text", Position: 2},
		{Name: "email", DataType: "text", Position: 3},
	}

	if cols[0].Position != 1 {
		t.Error("expected first column position to be 1")
	}
	if !cols[0].IsPrimaryKey {
		t.Error("expected first column to be primary key")
	}
}

func TestTableBackupConfigDefaults(t *testing.T) {
	cfg := &TableBackupConfig{
		Host:     "localhost",
		Database: "testdb",
	}

	// Before NewTableBackup
	if cfg.Port != 0 {
		t.Error("port should be 0 before NewTableBackup")
	}

	log := &mockLogger{}
	NewTableBackup(cfg, log)

	// After NewTableBackup - defaults should be set
	if cfg.Port != 5432 {
		t.Errorf("expected default port 5432, got %d", cfg.Port)
	}
}
