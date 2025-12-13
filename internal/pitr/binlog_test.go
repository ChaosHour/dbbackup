package pitr

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestBinlogPosition_String(t *testing.T) {
	tests := []struct {
		name     string
		position BinlogPosition
		expected string
	}{
		{
			name: "basic position",
			position: BinlogPosition{
				File:     "mysql-bin.000042",
				Position: 1234,
			},
			expected: "mysql-bin.000042:1234",
		},
		{
			name: "with GTID",
			position: BinlogPosition{
				File:     "mysql-bin.000042",
				Position: 1234,
				GTID:     "3E11FA47-71CA-11E1-9E33-C80AA9429562:1-5",
			},
			expected: "mysql-bin.000042:1234 (GTID: 3E11FA47-71CA-11E1-9E33-C80AA9429562:1-5)",
		},
		{
			name: "MariaDB GTID",
			position: BinlogPosition{
				File:     "mariadb-bin.000010",
				Position: 500,
				GTID:     "0-1-100",
			},
			expected: "mariadb-bin.000010:500 (GTID: 0-1-100)",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.position.String()
			if result != tt.expected {
				t.Errorf("got %q, want %q", result, tt.expected)
			}
		})
	}
}

func TestBinlogPosition_IsZero(t *testing.T) {
	tests := []struct {
		name     string
		position BinlogPosition
		expected bool
	}{
		{
			name:     "empty position",
			position: BinlogPosition{},
			expected: true,
		},
		{
			name: "has file",
			position: BinlogPosition{
				File: "mysql-bin.000001",
			},
			expected: false,
		},
		{
			name: "has position only",
			position: BinlogPosition{
				Position: 100,
			},
			expected: false,
		},
		{
			name: "has GTID only",
			position: BinlogPosition{
				GTID: "3E11FA47-71CA-11E1-9E33-C80AA9429562:1",
			},
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.position.IsZero()
			if result != tt.expected {
				t.Errorf("got %v, want %v", result, tt.expected)
			}
		})
	}
}

func TestBinlogPosition_Compare(t *testing.T) {
	tests := []struct {
		name     string
		a        *BinlogPosition
		b        *BinlogPosition
		expected int
	}{
		{
			name: "equal positions",
			a: &BinlogPosition{
				File:     "mysql-bin.000010",
				Position: 1000,
			},
			b: &BinlogPosition{
				File:     "mysql-bin.000010",
				Position: 1000,
			},
			expected: 0,
		},
		{
			name: "a before b - same file",
			a: &BinlogPosition{
				File:     "mysql-bin.000010",
				Position: 100,
			},
			b: &BinlogPosition{
				File:     "mysql-bin.000010",
				Position: 200,
			},
			expected: -1,
		},
		{
			name: "a after b - same file",
			a: &BinlogPosition{
				File:     "mysql-bin.000010",
				Position: 300,
			},
			b: &BinlogPosition{
				File:     "mysql-bin.000010",
				Position: 200,
			},
			expected: 1,
		},
		{
			name: "a before b - different files",
			a: &BinlogPosition{
				File:     "mysql-bin.000009",
				Position: 9999,
			},
			b: &BinlogPosition{
				File:     "mysql-bin.000010",
				Position: 100,
			},
			expected: -1,
		},
		{
			name: "a after b - different files",
			a: &BinlogPosition{
				File:     "mysql-bin.000011",
				Position: 100,
			},
			b: &BinlogPosition{
				File:     "mysql-bin.000010",
				Position: 9999,
			},
			expected: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.a.Compare(tt.b)
			if result != tt.expected {
				t.Errorf("got %d, want %d", result, tt.expected)
			}
		})
	}
}

func TestParseBinlogPosition(t *testing.T) {
	tests := []struct {
		name        string
		input       string
		expected    *BinlogPosition
		expectError bool
	}{
		{
			name:  "basic position",
			input: "mysql-bin.000042:1234",
			expected: &BinlogPosition{
				File:     "mysql-bin.000042",
				Position: 1234,
			},
			expectError: false,
		},
		{
			name:  "with GTID",
			input: "mysql-bin.000042:1234:3E11FA47-71CA-11E1-9E33-C80AA9429562:1-5",
			expected: &BinlogPosition{
				File:     "mysql-bin.000042",
				Position: 1234,
				GTID:     "3E11FA47-71CA-11E1-9E33-C80AA9429562:1-5",
			},
			expectError: false,
		},
		{
			name:        "invalid format",
			input:       "invalid",
			expected:    nil,
			expectError: true,
		},
		{
			name:        "invalid position",
			input:       "mysql-bin.000042:notanumber",
			expected:    nil,
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := ParseBinlogPosition(tt.input)

			if tt.expectError {
				if err == nil {
					t.Error("expected error, got nil")
				}
				return
			}

			if err != nil {
				t.Errorf("unexpected error: %v", err)
				return
			}

			if result.File != tt.expected.File {
				t.Errorf("File: got %q, want %q", result.File, tt.expected.File)
			}
			if result.Position != tt.expected.Position {
				t.Errorf("Position: got %d, want %d", result.Position, tt.expected.Position)
			}
			if result.GTID != tt.expected.GTID {
				t.Errorf("GTID: got %q, want %q", result.GTID, tt.expected.GTID)
			}
		})
	}
}

func TestExtractBinlogNumber(t *testing.T) {
	tests := []struct {
		name     string
		filename string
		expected int
	}{
		{"mysql binlog", "mysql-bin.000042", 42},
		{"mariadb binlog", "mariadb-bin.000100", 100},
		{"first binlog", "mysql-bin.000001", 1},
		{"large number", "mysql-bin.999999", 999999},
		{"no number", "mysql-bin", 0},
		{"invalid format", "binlog", 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := extractBinlogNumber(tt.filename)
			if result != tt.expected {
				t.Errorf("got %d, want %d", result, tt.expected)
			}
		})
	}
}

func TestCompareBinlogFiles(t *testing.T) {
	tests := []struct {
		name     string
		a        string
		b        string
		expected int
	}{
		{"equal", "mysql-bin.000010", "mysql-bin.000010", 0},
		{"a < b", "mysql-bin.000009", "mysql-bin.000010", -1},
		{"a > b", "mysql-bin.000011", "mysql-bin.000010", 1},
		{"large difference", "mysql-bin.000001", "mysql-bin.000100", -1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := compareBinlogFiles(tt.a, tt.b)
			if result != tt.expected {
				t.Errorf("got %d, want %d", result, tt.expected)
			}
		})
	}
}

func TestValidateBinlogChain(t *testing.T) {
	ctx := context.Background()
	bm := &BinlogManager{}

	tests := []struct {
		name           string
		binlogs        []BinlogFile
		expectValid    bool
		expectGaps     int
		expectWarnings bool
	}{
		{
			name:        "empty chain",
			binlogs:     []BinlogFile{},
			expectValid: true,
			expectGaps:  0,
		},
		{
			name: "continuous chain",
			binlogs: []BinlogFile{
				{Name: "mysql-bin.000001", ServerID: 1},
				{Name: "mysql-bin.000002", ServerID: 1},
				{Name: "mysql-bin.000003", ServerID: 1},
			},
			expectValid: true,
			expectGaps:  0,
		},
		{
			name: "chain with gap",
			binlogs: []BinlogFile{
				{Name: "mysql-bin.000001", ServerID: 1},
				{Name: "mysql-bin.000003", ServerID: 1}, // 000002 missing
				{Name: "mysql-bin.000004", ServerID: 1},
			},
			expectValid: false,
			expectGaps:  1,
		},
		{
			name: "chain with multiple gaps",
			binlogs: []BinlogFile{
				{Name: "mysql-bin.000001", ServerID: 1},
				{Name: "mysql-bin.000005", ServerID: 1}, // 000002-000004 missing
				{Name: "mysql-bin.000010", ServerID: 1}, // 000006-000009 missing
			},
			expectValid: false,
			expectGaps:  2,
		},
		{
			name: "server_id change warning",
			binlogs: []BinlogFile{
				{Name: "mysql-bin.000001", ServerID: 1},
				{Name: "mysql-bin.000002", ServerID: 2}, // Server ID changed
				{Name: "mysql-bin.000003", ServerID: 2},
			},
			expectValid:    true,
			expectGaps:     0,
			expectWarnings: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := bm.ValidateBinlogChain(ctx, tt.binlogs)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			if result.Valid != tt.expectValid {
				t.Errorf("Valid: got %v, want %v", result.Valid, tt.expectValid)
			}

			if len(result.Gaps) != tt.expectGaps {
				t.Errorf("Gaps: got %d, want %d", len(result.Gaps), tt.expectGaps)
			}

			if tt.expectWarnings && len(result.Warnings) == 0 {
				t.Error("expected warnings, got none")
			}
		})
	}
}

func TestFindBinlogsInRange(t *testing.T) {
	ctx := context.Background()
	bm := &BinlogManager{}

	now := time.Now()
	hour := time.Hour

	binlogs := []BinlogFile{
		{
			Name:      "mysql-bin.000001",
			StartTime: now.Add(-5 * hour),
			EndTime:   now.Add(-4 * hour),
		},
		{
			Name:      "mysql-bin.000002",
			StartTime: now.Add(-4 * hour),
			EndTime:   now.Add(-3 * hour),
		},
		{
			Name:      "mysql-bin.000003",
			StartTime: now.Add(-3 * hour),
			EndTime:   now.Add(-2 * hour),
		},
		{
			Name:      "mysql-bin.000004",
			StartTime: now.Add(-2 * hour),
			EndTime:   now.Add(-1 * hour),
		},
		{
			Name:      "mysql-bin.000005",
			StartTime: now.Add(-1 * hour),
			EndTime:   now,
		},
	}

	tests := []struct {
		name     string
		start    time.Time
		end      time.Time
		expected int
	}{
		{
			name:     "all binlogs",
			start:    now.Add(-6 * hour),
			end:      now.Add(1 * hour),
			expected: 5,
		},
		{
			name:     "middle range",
			start:    now.Add(-4 * hour),
			end:      now.Add(-2 * hour),
			expected: 4, // binlogs 1-4 overlap (1 ends at -4h, 4 starts at -2h)
		},
		{
			name:     "last two",
			start:    now.Add(-2 * hour),
			end:      now,
			expected: 3, // binlogs 3-5 overlap (3 ends at -2h, 5 ends at now)
		},
		{
			name:     "exact match one binlog",
			start:    now.Add(-3 * hour),
			end:      now.Add(-2 * hour),
			expected: 3, // binlogs 2,3,4 overlap with this range
		},
		{
			name:     "no overlap - before",
			start:    now.Add(-10 * hour),
			end:      now.Add(-6 * hour),
			expected: 0,
		},
		{
			name:     "no overlap - after",
			start:    now.Add(1 * hour),
			end:      now.Add(2 * hour),
			expected: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := bm.FindBinlogsInRange(ctx, binlogs, tt.start, tt.end)
			if len(result) != tt.expected {
				t.Errorf("got %d binlogs, want %d", len(result), tt.expected)
			}
		})
	}
}

func TestBinlogArchiveInfo_Metadata(t *testing.T) {
	// Test that archive metadata is properly saved and loaded
	tempDir, err := os.MkdirTemp("", "binlog_test")
	if err != nil {
		t.Fatalf("creating temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	bm := &BinlogManager{
		archiveDir: tempDir,
	}

	archives := []BinlogArchiveInfo{
		{
			OriginalFile: "mysql-bin.000001",
			ArchivePath:  filepath.Join(tempDir, "mysql-bin.000001.gz"),
			Size:         1024,
			Compressed:   true,
			ArchivedAt:   time.Now().Add(-2 * time.Hour),
			StartPos:     4,
			EndPos:       1024,
			StartTime:    time.Now().Add(-3 * time.Hour),
			EndTime:      time.Now().Add(-2 * time.Hour),
		},
		{
			OriginalFile: "mysql-bin.000002",
			ArchivePath:  filepath.Join(tempDir, "mysql-bin.000002.gz"),
			Size:         2048,
			Compressed:   true,
			ArchivedAt:   time.Now().Add(-1 * time.Hour),
			StartPos:     4,
			EndPos:       2048,
			StartTime:    time.Now().Add(-2 * time.Hour),
			EndTime:      time.Now().Add(-1 * time.Hour),
		},
	}

	// Save metadata
	err = bm.SaveArchiveMetadata(archives)
	if err != nil {
		t.Fatalf("saving metadata: %v", err)
	}

	// Verify metadata file exists
	metadataPath := filepath.Join(tempDir, "metadata.json")
	if _, err := os.Stat(metadataPath); os.IsNotExist(err) {
		t.Fatal("metadata file was not created")
	}

	// Load and verify
	loaded := bm.loadArchiveMetadata(metadataPath)
	if len(loaded) != 2 {
		t.Errorf("got %d archives, want 2", len(loaded))
	}

	if loaded["mysql-bin.000001"].Size != 1024 {
		t.Errorf("wrong size for first archive")
	}

	if loaded["mysql-bin.000002"].Size != 2048 {
		t.Errorf("wrong size for second archive")
	}
}

func TestLimitedScanner(t *testing.T) {
	// Test the limited scanner used for reading dump headers
	input := "line1\nline2\nline3\nline4\nline5\nline6\nline7\nline8\nline9\nline10\n"
	reader := NewLimitedScanner(strings.NewReader(input), 5)

	var lines []string
	for reader.Scan() {
		lines = append(lines, reader.Text())
	}

	if len(lines) != 5 {
		t.Errorf("got %d lines, want 5", len(lines))
	}
}

// TestDatabaseType tests database type constants
func TestDatabaseType(t *testing.T) {
	tests := []struct {
		name     string
		dbType   DatabaseType
		expected string
	}{
		{"PostgreSQL", DatabasePostgreSQL, "postgres"},
		{"MySQL", DatabaseMySQL, "mysql"},
		{"MariaDB", DatabaseMariaDB, "mariadb"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if string(tt.dbType) != tt.expected {
				t.Errorf("got %q, want %q", tt.dbType, tt.expected)
			}
		})
	}
}

// TestRestoreTargetType tests restore target type constants
func TestRestoreTargetType(t *testing.T) {
	tests := []struct {
		name     string
		target   RestoreTargetType
		expected string
	}{
		{"Time", RestoreTargetTime, "time"},
		{"Position", RestoreTargetPosition, "position"},
		{"Immediate", RestoreTargetImmediate, "immediate"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if string(tt.target) != tt.expected {
				t.Errorf("got %q, want %q", tt.target, tt.expected)
			}
		})
	}
}
