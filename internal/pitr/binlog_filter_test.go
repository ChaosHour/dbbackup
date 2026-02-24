package pitr

import (
	"testing"
	"time"
)

func TestBinlogFilterConfig_Validate(t *testing.T) {
	tests := []struct {
		name    string
		config  *BinlogFilterConfig
		wantErr bool
		errMsg  string
	}{
		{
			name:    "nil config is valid",
			config:  nil,
			wantErr: false,
		},
		{
			name:    "empty config is valid",
			config:  &BinlogFilterConfig{},
			wantErr: false,
		},
		{
			name: "include databases only",
			config: &BinlogFilterConfig{
				IncludeDatabases: []string{"mydb"},
			},
			wantErr: false,
		},
		{
			name: "exclude databases only",
			config: &BinlogFilterConfig{
				ExcludeDatabases: []string{"testdb"},
			},
			wantErr: false,
		},
		{
			name: "include and exclude databases conflict",
			config: &BinlogFilterConfig{
				IncludeDatabases: []string{"mydb"},
				ExcludeDatabases: []string{"testdb"},
			},
			wantErr: true,
			errMsg:  "cannot specify both include-databases and exclude-databases",
		},
		{
			name: "include and exclude GTIDs conflict",
			config: &BinlogFilterConfig{
				IncludeGTIDs: "uuid:1-5",
				ExcludeGTIDs: "uuid:6-10",
			},
			wantErr: true,
			errMsg:  "cannot specify both include-gtids and exclude-gtids",
		},
		{
			name: "valid include GTIDs",
			config: &BinlogFilterConfig{
				IncludeGTIDs: "3E11FA47-71CA-11E1-9E33-C80AA9429562:1-5",
			},
			wantErr: false,
		},
		{
			name: "valid table references",
			config: &BinlogFilterConfig{
				IncludeTables: []string{"mydb.users", "mydb.orders"},
			},
			wantErr: false,
		},
		{
			name: "invalid table reference - no dot",
			config: &BinlogFilterConfig{
				IncludeTables: []string{"users"},
			},
			wantErr: true,
			errMsg:  "invalid table reference",
		},
		{
			name: "invalid table reference - empty db",
			config: &BinlogFilterConfig{
				ExcludeTables: []string{".users"},
			},
			wantErr: true,
			errMsg:  "invalid table reference",
		},
		{
			name: "invalid table reference - empty table",
			config: &BinlogFilterConfig{
				IncludeTables: []string{"mydb."},
			},
			wantErr: true,
			errMsg:  "invalid table reference",
		},
		{
			name: "include and exclude tables conflict",
			config: &BinlogFilterConfig{
				IncludeTables: []string{"mydb.users"},
				ExcludeTables: []string{"mydb.orders"},
			},
			wantErr: true,
			errMsg:  "cannot specify both include-tables and exclude-tables",
		},
		{
			name: "valid rewrite-db",
			config: &BinlogFilterConfig{
				RewriteDB: map[string]string{"prod": "staging"},
			},
			wantErr: false,
		},
		{
			name: "invalid rewrite-db empty source",
			config: &BinlogFilterConfig{
				RewriteDB: map[string]string{"": "staging"},
			},
			wantErr: true,
			errMsg:  "rewrite-db mapping cannot have empty",
		},
		{
			name: "invalid rewrite-db empty destination",
			config: &BinlogFilterConfig{
				RewriteDB: map[string]string{"prod": ""},
			},
			wantErr: true,
			errMsg:  "rewrite-db mapping cannot have empty",
		},
		{
			name: "combined valid config",
			config: &BinlogFilterConfig{
				IncludeDatabases: []string{"mydb", "analytics"},
				IncludeGTIDs:     "uuid:1-100",
				SkipDDL:          true,
				RewriteDB:        map[string]string{"prod": "staging"},
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.config.Validate()
			if tt.wantErr {
				if err == nil {
					t.Fatalf("expected error containing %q, got nil", tt.errMsg)
				}
				if tt.errMsg != "" && !contains(err.Error(), tt.errMsg) {
					t.Fatalf("expected error containing %q, got %q", tt.errMsg, err.Error())
				}
			} else {
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
			}
		})
	}
}

func TestBinlogFilterConfig_Helpers(t *testing.T) {
	t.Run("HasDatabaseFilter", func(t *testing.T) {
		var nilFilter *BinlogFilterConfig
		if nilFilter.HasDatabaseFilter() {
			t.Error("nil filter should return false")
		}

		empty := &BinlogFilterConfig{}
		if empty.HasDatabaseFilter() {
			t.Error("empty filter should return false")
		}

		withInclude := &BinlogFilterConfig{IncludeDatabases: []string{"mydb"}}
		if !withInclude.HasDatabaseFilter() {
			t.Error("filter with include databases should return true")
		}

		withExclude := &BinlogFilterConfig{ExcludeDatabases: []string{"testdb"}}
		if !withExclude.HasDatabaseFilter() {
			t.Error("filter with exclude databases should return true")
		}
	})

	t.Run("HasTableFilter", func(t *testing.T) {
		var nilFilter *BinlogFilterConfig
		if nilFilter.HasTableFilter() {
			t.Error("nil filter should return false")
		}

		withInclude := &BinlogFilterConfig{IncludeTables: []string{"db.tbl"}}
		if !withInclude.HasTableFilter() {
			t.Error("filter with include tables should return true")
		}
	})

	t.Run("HasGTIDFilter", func(t *testing.T) {
		var nilFilter *BinlogFilterConfig
		if nilFilter.HasGTIDFilter() {
			t.Error("nil filter should return false")
		}

		withInclude := &BinlogFilterConfig{IncludeGTIDs: "uuid:1-5"}
		if !withInclude.HasGTIDFilter() {
			t.Error("filter with include GTIDs should return true")
		}
	})
}

func TestBuildMysqlbinlogArgs_Basic(t *testing.T) {
	files := []string{"/var/lib/mysql/mysql-bin.000001"}

	args := buildMysqlbinlogArgs(nil, nil, nil, nil, false, files, 0)

	if args[0] != "--no-defaults" {
		t.Errorf("first arg should be --no-defaults, got %s", args[0])
	}
	if args[len(args)-1] != files[0] {
		t.Errorf("last arg should be the file, got %s", args[len(args)-1])
	}
}

func TestBuildMysqlbinlogArgs_StartPosition(t *testing.T) {
	files := []string{
		"/data/mysql-bin.000001",
		"/data/mysql-bin.000002",
		"/data/mysql-bin.000003",
	}
	startPos := &BinlogPosition{File: "mysql-bin.000001", Position: 1234}

	// First file should have --start-position
	args := buildMysqlbinlogArgs(nil, startPos, nil, nil, false, files, 0)
	found := false
	for _, arg := range args {
		if arg == "--start-position=1234" {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("expected --start-position=1234 for first file, args: %v", args)
	}

	// Second file should NOT have --start-position
	args = buildMysqlbinlogArgs(nil, startPos, nil, nil, false, files, 1)
	for _, arg := range args {
		if contains(arg, "--start-position") {
			t.Errorf("second file should not have --start-position, args: %v", args)
		}
	}
}

func TestBuildMysqlbinlogArgs_StopPosition(t *testing.T) {
	files := []string{
		"/data/mysql-bin.000001",
		"/data/mysql-bin.000002",
	}
	stopPos := &BinlogPosition{File: "mysql-bin.000002", Position: 5678}

	// First file (not last) should NOT have --stop-position
	args := buildMysqlbinlogArgs(nil, nil, stopPos, nil, false, files, 0)
	for _, arg := range args {
		if contains(arg, "--stop-position") {
			t.Errorf("first file should not have --stop-position, args: %v", args)
		}
	}

	// Last file should have --stop-position
	args = buildMysqlbinlogArgs(nil, nil, stopPos, nil, false, files, 1)
	found := false
	for _, arg := range args {
		if arg == "--stop-position=5678" {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("expected --stop-position=5678 for last file, args: %v", args)
	}
}

func TestBuildMysqlbinlogArgs_StopTime(t *testing.T) {
	files := []string{"/data/mysql-bin.000001"}
	stopTime := time.Date(2024, 6, 15, 14, 30, 0, 0, time.UTC)

	args := buildMysqlbinlogArgs(nil, nil, nil, &stopTime, false, files, 0)

	found := false
	for _, arg := range args {
		if arg == "--stop-datetime=2024-06-15 14:30:00" {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("expected --stop-datetime flag, args: %v", args)
	}
}

func TestBuildMysqlbinlogArgs_DatabaseFilter(t *testing.T) {
	files := []string{"/data/mysql-bin.000001"}
	filter := &BinlogFilterConfig{
		IncludeDatabases: []string{"mydb", "analytics"},
	}

	args := buildMysqlbinlogArgs(filter, nil, nil, nil, false, files, 0)

	dbCount := 0
	for _, arg := range args {
		if contains(arg, "--database=") {
			dbCount++
		}
	}
	if dbCount != 2 {
		t.Errorf("expected 2 --database flags, got %d, args: %v", dbCount, args)
	}
}

func TestBuildMysqlbinlogArgs_GTIDFilter(t *testing.T) {
	files := []string{"/data/mysql-bin.000001"}

	t.Run("include GTIDs", func(t *testing.T) {
		filter := &BinlogFilterConfig{
			IncludeGTIDs: "3E11FA47-71CA-11E1-9E33-C80AA9429562:1-5",
		}
		args := buildMysqlbinlogArgs(filter, nil, nil, nil, true, files, 0)
		found := false
		for _, arg := range args {
			if contains(arg, "--include-gtids=") {
				found = true
			}
		}
		if !found {
			t.Errorf("expected --include-gtids flag, args: %v", args)
		}
	})

	t.Run("exclude GTIDs", func(t *testing.T) {
		filter := &BinlogFilterConfig{
			ExcludeGTIDs: "3E11FA47-71CA-11E1-9E33-C80AA9429562:6-10",
		}
		args := buildMysqlbinlogArgs(filter, nil, nil, nil, true, files, 0)
		found := false
		for _, arg := range args {
			if contains(arg, "--exclude-gtids=") {
				found = true
			}
		}
		if !found {
			t.Errorf("expected --exclude-gtids flag, args: %v", args)
		}
	})
}

func TestBuildMysqlbinlogArgs_RewriteDB(t *testing.T) {
	files := []string{"/data/mysql-bin.000001"}
	filter := &BinlogFilterConfig{
		RewriteDB: map[string]string{"prod": "staging"},
	}

	args := buildMysqlbinlogArgs(filter, nil, nil, nil, false, files, 0)
	found := false
	for _, arg := range args {
		if arg == "--rewrite-db=prod->staging" {
			found = true
		}
	}
	if !found {
		t.Errorf("expected --rewrite-db=prod->staging, args: %v", args)
	}
}

func TestMultiFilePositionSelection(t *testing.T) {
	files := []string{
		"/data/mysql-bin.000001",
		"/data/mysql-bin.000002",
		"/data/mysql-bin.000003",
		"/data/mysql-bin.000004",
		"/data/mysql-bin.000005",
	}

	t.Run("no position filter", func(t *testing.T) {
		selected := selectFilesForReplay(files, nil, nil)
		if len(selected) != 5 {
			t.Errorf("expected 5 files, got %d", len(selected))
		}
	})

	t.Run("start position only", func(t *testing.T) {
		startPos := &BinlogPosition{File: "mysql-bin.000003", Position: 100}
		selected := selectFilesForReplay(files, startPos, nil)
		if len(selected) != 3 {
			t.Errorf("expected 3 files (000003-000005), got %d: %v", len(selected), selected)
		}
	})

	t.Run("stop position only", func(t *testing.T) {
		stopPos := &BinlogPosition{File: "mysql-bin.000003", Position: 500}
		selected := selectFilesForReplay(files, nil, stopPos)
		if len(selected) != 3 {
			t.Errorf("expected 3 files (000001-000003), got %d: %v", len(selected), selected)
		}
	})

	t.Run("start and stop position", func(t *testing.T) {
		startPos := &BinlogPosition{File: "mysql-bin.000002", Position: 100}
		stopPos := &BinlogPosition{File: "mysql-bin.000004", Position: 500}
		selected := selectFilesForReplay(files, startPos, stopPos)
		if len(selected) != 3 {
			t.Errorf("expected 3 files (000002-000004), got %d: %v", len(selected), selected)
		}
	})

	t.Run("single file range", func(t *testing.T) {
		startPos := &BinlogPosition{File: "mysql-bin.000003", Position: 100}
		stopPos := &BinlogPosition{File: "mysql-bin.000003", Position: 500}
		selected := selectFilesForReplay(files, startPos, stopPos)
		if len(selected) != 1 {
			t.Errorf("expected 1 file (000003), got %d: %v", len(selected), selected)
		}
	})

	t.Run("empty files", func(t *testing.T) {
		selected := selectFilesForReplay(nil, nil, nil)
		if selected != nil {
			t.Errorf("expected nil, got %v", selected)
		}
	})

	t.Run("no matching files", func(t *testing.T) {
		startPos := &BinlogPosition{File: "mysql-bin.000010", Position: 100}
		selected := selectFilesForReplay(files, startPos, nil)
		if len(selected) != 0 {
			t.Errorf("expected 0 files, got %d: %v", len(selected), selected)
		}
	})
}

func TestSQLFilter_NoFilter(t *testing.T) {
	f := newSQLTableFilter(nil)
	if f != nil {
		t.Error("nil config should return nil filter")
	}

	f = newSQLTableFilter(&BinlogFilterConfig{})
	if f != nil {
		t.Error("empty config should return nil filter")
	}
}

func TestSQLFilter_DDLSkip(t *testing.T) {
	f := newSQLTableFilter(&BinlogFilterConfig{SkipDDL: true})
	if f == nil {
		t.Fatal("expected non-nil filter")
	}

	tests := []struct {
		line    string
		include bool
	}{
		{"CREATE TABLE foo (id INT)", false},
		{"ALTER TABLE foo ADD COLUMN bar VARCHAR(50)", false},
		{"DROP TABLE foo", false},
		{"TRUNCATE TABLE foo", false},
		{"INSERT INTO foo VALUES (1)", true},
		{"UPDATE foo SET bar=1", true},
		{"DELETE FROM foo WHERE id=1", true},
		{"-- comment", true},
		{"", true},
		{"# binlog comment", true},
	}

	db := ""
	for _, tt := range tests {
		include, newDB := f.shouldIncludeLine(tt.line, db)
		db = newDB
		if include != tt.include {
			t.Errorf("line %q: expected include=%v, got %v", tt.line, tt.include, include)
		}
	}
}

func TestSQLFilter_IncludeTables(t *testing.T) {
	f := newSQLTableFilter(&BinlogFilterConfig{
		IncludeTables: []string{"mydb.users", "mydb.orders"},
	})
	if f == nil {
		t.Fatal("expected non-nil filter")
	}

	tests := []struct {
		line      string
		currentDB string
		include   bool
	}{
		{"INSERT INTO mydb.users VALUES (1, 'test')", "", true},
		{"INSERT INTO mydb.orders VALUES (1, 100)", "", true},
		{"INSERT INTO mydb.products VALUES (1, 'widget')", "", false},
		{"UPDATE mydb.users SET name='foo'", "", true},
		{"DELETE FROM mydb.orders WHERE id=1", "", true},
		{"DELETE FROM mydb.logs WHERE id=1", "", false},
		// With USE context
		{"USE mydb", "", true},
		{"INSERT INTO users VALUES (1, 'test')", "mydb", true},
		{"INSERT INTO products VALUES (1, 'widget')", "mydb", false},
		// Comments always pass through
		{"-- some comment", "", true},
	}

	for _, tt := range tests {
		include, _ := f.shouldIncludeLine(tt.line, tt.currentDB)
		if include != tt.include {
			t.Errorf("line %q (db=%q): expected include=%v, got %v",
				tt.line, tt.currentDB, tt.include, include)
		}
	}
}

func TestSQLFilter_ExcludeTables(t *testing.T) {
	f := newSQLTableFilter(&BinlogFilterConfig{
		ExcludeTables: []string{"mydb.audit_log", "mydb.sessions"},
	})
	if f == nil {
		t.Fatal("expected non-nil filter")
	}

	tests := []struct {
		line      string
		currentDB string
		include   bool
	}{
		{"INSERT INTO mydb.users VALUES (1)", "", true},
		{"INSERT INTO mydb.audit_log VALUES (1)", "", false},
		{"INSERT INTO mydb.sessions VALUES (1)", "", false},
		{"UPDATE mydb.audit_log SET level='info'", "", false},
		// With USE context
		{"INSERT INTO audit_log VALUES (1)", "mydb", false},
		{"INSERT INTO users VALUES (1)", "mydb", true},
	}

	for _, tt := range tests {
		include, _ := f.shouldIncludeLine(tt.line, tt.currentDB)
		if include != tt.include {
			t.Errorf("line %q (db=%q): expected include=%v, got %v",
				tt.line, tt.currentDB, tt.include, include)
		}
	}
}

func TestSQLFilter_USETracking(t *testing.T) {
	f := newSQLTableFilter(&BinlogFilterConfig{
		IncludeTables: []string{"db1.users"},
	})
	if f == nil {
		t.Fatal("expected non-nil filter")
	}

	// Test USE statement tracking
	_, db := f.shouldIncludeLine("USE db1", "")
	if db != "db1" {
		t.Errorf("expected db=db1 after USE, got %q", db)
	}

	_, db = f.shouldIncludeLine("USE `db2`", db)
	if db != "db2" {
		t.Errorf("expected db=db2 after USE `db2`, got %q", db)
	}
}

func TestBinlogReplayResult_Summary(t *testing.T) {
	result := &BinlogReplayResult{
		FilesProcessed: 3,
		EventsApplied:  1000,
		EventsFiltered: 50,
		BytesProcessed: 10 * 1024 * 1024,
		Duration:       5 * time.Second,
	}

	summary := result.Summary()
	if !contains(summary, "[OK]") {
		t.Errorf("expected [OK] in summary, got %s", summary)
	}
	if !contains(summary, "3 files") {
		t.Errorf("expected '3 files' in summary, got %s", summary)
	}
	if !contains(summary, "1000 events applied") {
		t.Errorf("expected '1000 events applied' in summary, got %s", summary)
	}

	// With errors
	result.Errors = []string{"some error"}
	summary = result.Summary()
	if !contains(summary, "[FAILED]") {
		t.Errorf("expected [FAILED] in summary, got %s", summary)
	}
}

func TestExtractFilename(t *testing.T) {
	tests := []struct {
		path     string
		expected string
	}{
		{"/var/lib/mysql/mysql-bin.000001", "mysql-bin.000001"},
		{"mysql-bin.000001", "mysql-bin.000001"},
		{"/data/mysql-bin.000042", "mysql-bin.000042"},
		{"./mysql-bin.000001", "mysql-bin.000001"},
	}

	for _, tt := range tests {
		result := extractFilename(tt.path)
		if result != tt.expected {
			t.Errorf("extractFilename(%q) = %q, want %q", tt.path, result, tt.expected)
		}
	}
}

func TestFormatBytes(t *testing.T) {
	tests := []struct {
		bytes    int64
		expected string
	}{
		{0, "0 B"},
		{500, "500 B"},
		{1024, "1.00 KB"},
		{1536, "1.50 KB"},
		{1048576, "1.00 MB"},
		{1073741824, "1.00 GB"},
	}

	for _, tt := range tests {
		result := formatBytes(tt.bytes)
		if result != tt.expected {
			t.Errorf("formatBytes(%d) = %q, want %q", tt.bytes, result, tt.expected)
		}
	}
}

func TestIsValidTableRef(t *testing.T) {
	tests := []struct {
		ref   string
		valid bool
	}{
		{"db.table", true},
		{"my_db.my_table", true},
		{"db.", false},
		{".table", false},
		{"notable", false},
		{"", false},
	}

	for _, tt := range tests {
		result := isValidTableRef(tt.ref)
		if result != tt.valid {
			t.Errorf("isValidTableRef(%q) = %v, want %v", tt.ref, result, tt.valid)
		}
	}
}

// contains checks if a string contains a substring (helper for tests)
func contains(s, substr string) bool {
	return len(s) >= len(substr) && searchString(s, substr)
}

func searchString(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
