package restore

import (
	"testing"
)

func TestValidateTarPath_Exploits(t *testing.T) {
	tests := []struct {
		name    string
		path    string
		baseDir string
		wantErr bool
	}{
		{"normal file", "dumps/db.sql", "/tmp/restore", false},
		{"nested normal", "dumps/subdir/db.sql", "/tmp/restore", false},
		{"parent escape", "../etc/passwd", "/tmp/restore", true},
		{"absolute path", "/etc/passwd", "/tmp/restore", true},
		{"nested escape", "dumps/../../etc/passwd", "/tmp/restore", true},
		{"deep escape", "dumps/../../../root/.ssh/id_rsa", "/tmp/restore", true},
		{"double dot only", "..", "/tmp/restore", true},
		{"dot-dot-slash", "../", "/tmp/restore", true},
		{"hidden escape", "dumps/foo/../../../../etc/shadow", "/tmp/restore", true},
		{"current dir", ".", "/tmp/restore", false},
		{"just filename", "db.sql", "/tmp/restore", false},
		{"windows absolute", "C:\\Windows\\System32\\evil.exe", "/tmp/restore", false}, // not abs on Linux
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateTarPath(tt.path, tt.baseDir)
			if (err != nil) != tt.wantErr {
				t.Errorf("validateTarPath(%q, %q) error = %v, wantErr %v", tt.path, tt.baseDir, err, tt.wantErr)
			}
		})
	}
}
