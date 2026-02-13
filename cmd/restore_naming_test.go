package cmd

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

func TestExtractDBNameFromArchive_WithMetadata(t *testing.T) {
	// Create temp dir with a fake backup + meta.json
	dir := t.TempDir()
	backupFile := filepath.Join(dir, "9.10.3_prod_dump-resydb_20260211.sql")
	os.WriteFile(backupFile, []byte("-- fake"), 0644)

	meta := map[string]string{"database": "resydb", "version": "1.0"}
	data, _ := json.Marshal(meta)
	os.WriteFile(backupFile+".meta.json", data, 0644)

	got := extractDBNameFromArchive(backupFile)
	if got != "resydb" {
		t.Errorf("with meta.json: got %q, want %q", got, "resydb")
	}
}

func TestExtractDBNameFromArchive_FallbackFilename(t *testing.T) {
	tests := []struct {
		filename string
		want     string
	}{
		{"9.10.3_prod_dump-resydb_20260211.sql", "9.10.3_prod_dump-resydb"},
		{"pg_mydb_20260212_143000.dump", "pg_mydb"},
		{"resydb_20260211.sql", "resydb"},
		{"mydb.sql", "mydb"},
		{"db_test_20260101_120000.dump.gz", "db_test"},
	}
	for _, tt := range tests {
		t.Run(tt.filename, func(t *testing.T) {
			// Use a path that won't have a .meta.json
			got := extractDBNameFromArchive("/nonexistent/" + tt.filename)
			if got != tt.want {
				t.Errorf("filename %q: got %q, want %q", tt.filename, got, tt.want)
			}
		})
	}
}
