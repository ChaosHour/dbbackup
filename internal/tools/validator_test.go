package tools

import (
	"fmt"
	"testing"
)

// nullLogger satisfies logger.Logger for tests without importing the full package.
type nullLogger struct{}

func (nullLogger) Debug(_ string, _ ...any)              {}
func (nullLogger) Info(_ string, _ ...any)               {}
func (nullLogger) Warn(_ string, _ ...any)               {}
func (nullLogger) Error(_ string, _ ...any)              {}
func (n nullLogger) WithFields(_ map[string]any) nullLogger { return n }
func (n nullLogger) WithField(_ string, _ any) nullLogger   { return n }
func (nullLogger) Time(_ string, _ ...any)               {}

func TestValidateTools_AllAvailable(t *testing.T) {
	v := &Validator{
		LookPathFunc: func(file string) (string, error) {
			return "/usr/bin/" + file, nil
		},
	}

	reqs := []ToolRequirement{
		{Name: "pg_dump", Purpose: "test", Required: true},
		{Name: "psql", Purpose: "test", Required: false},
	}

	statuses, err := v.ValidateTools(reqs)
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}
	if len(statuses) != 2 {
		t.Fatalf("expected 2 statuses, got %d", len(statuses))
	}
	for _, s := range statuses {
		if !s.Available {
			t.Errorf("expected %s to be available", s.Name)
		}
		if s.Path == "" {
			t.Errorf("expected path for %s", s.Name)
		}
	}
}

func TestValidateTools_RequiredMissing(t *testing.T) {
	v := &Validator{
		LookPathFunc: func(file string) (string, error) {
			if file == "pg_dump" {
				return "", fmt.Errorf("not found")
			}
			return "/usr/bin/" + file, nil
		},
	}

	reqs := []ToolRequirement{
		{Name: "pg_dump", Purpose: "test", Required: true},
		{Name: "psql", Purpose: "test", Required: false},
	}

	statuses, err := v.ValidateTools(reqs)
	if err == nil {
		t.Fatal("expected error for missing required tool")
	}
	if len(statuses) != 2 {
		t.Fatalf("expected 2 statuses, got %d", len(statuses))
	}
	if statuses[0].Available {
		t.Error("pg_dump should not be available")
	}
	if !statuses[1].Available {
		t.Error("psql should be available")
	}
}

func TestValidateTools_OptionalMissing(t *testing.T) {
	v := &Validator{
		LookPathFunc: func(file string) (string, error) {
			if file == "psql" {
				return "", fmt.Errorf("not found")
			}
			return "/usr/bin/" + file, nil
		},
	}

	reqs := []ToolRequirement{
		{Name: "pg_dump", Purpose: "test", Required: true},
		{Name: "psql", Purpose: "test", Required: false},
	}

	statuses, err := v.ValidateTools(reqs)
	if err != nil {
		t.Fatalf("optional missing should not cause error, got: %v", err)
	}
	if statuses[1].Available {
		t.Error("psql should not be available")
	}
}

func TestPredefinedSets(t *testing.T) {
	tests := []struct {
		name string
		reqs []ToolRequirement
	}{
		{"PostgresBackup", PostgresBackupTools()},
		{"PostgresRestore", PostgresRestoreTools()},
		{"MySQLBackup", MySQLBackupTools()},
		{"MySQLRestore", MySQLRestoreTools()},
		{"DiagnoseAll", DiagnoseTools("")},
		{"DiagnosePostgres", DiagnoseTools("postgres")},
		{"DiagnoseMySQL", DiagnoseTools("mysql")},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if len(tt.reqs) == 0 {
				t.Error("expected non-empty requirement set")
			}
			for _, r := range tt.reqs {
				if r.Name == "" {
					t.Error("requirement has empty name")
				}
				if r.Purpose == "" {
					t.Error("requirement has empty purpose")
				}
			}
		})
	}
}
