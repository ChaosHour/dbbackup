package restore

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"dbbackup/internal/config"
	"dbbackup/internal/logger"
)

// =============================================================================
// buildGuardConnString
// =============================================================================

func TestBuildGuardConnString(t *testing.T) {
	tests := []struct {
		name     string
		host     string
		port     int
		user     string
		password string
		wantAll  []string
		wantNone []string
	}{
		{
			name: "unix socket path",
			host: "/var/run/postgresql", port: 5432, user: "pg", password: "",
			wantAll:  []string{"host=/var/run/postgresql", "user=pg", "dbname=postgres"},
			wantNone: []string{"port="},
		},
		{
			name: "unix socket with password",
			host: "/var/run/postgresql", port: 5432, user: "pg", password: "secret",
			wantAll: []string{"host=/var/run/postgresql", "password=secret"},
		},
		{
			name: "remote host",
			host: "db.example.com", port: 5433, user: "admin", password: "pw",
			wantAll: []string{"host=db.example.com", "port=5433", "user=admin", "password=pw"},
		},
		{
			name: "empty host defaults to localhost TCP",
			host: "", port: 5432, user: "pg", password: "pw",
			wantAll: []string{"host=localhost", "port=5432"},
		},
		{
			name: "localhost without password probes sockets",
			host: "localhost", port: 5432, user: "pg", password: "",
			// Will fall through to TCP since sockets likely don't exist in test env
			wantAll: []string{"user=pg", "dbname=postgres", "sslmode=disable"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := buildGuardConnString(tt.host, tt.port, tt.user, tt.password)

			for _, w := range tt.wantAll {
				if !strings.Contains(got, w) {
					t.Errorf("DSN %q missing %q", got, w)
				}
			}
			for _, w := range tt.wantNone {
				if strings.Contains(got, w) {
					t.Errorf("DSN %q should not contain %q", got, w)
				}
			}
		})
	}
}

// =============================================================================
// estimateTotalSize
// =============================================================================

func TestEstimateTotalSize(t *testing.T) {
	guard := NewLargeDBGuard(&config.Config{}, logger.NewNullLogger())

	t.Run("two files", func(t *testing.T) {
		dir := t.TempDir()
		f1 := filepath.Join(dir, "a.dump")
		f2 := filepath.Join(dir, "b.dump")
		os.WriteFile(f1, make([]byte, 1000), 0644)
		os.WriteFile(f2, make([]byte, 2000), 0644)

		got := guard.estimateTotalSize([]string{f1, f2})
		if got != 3000 {
			t.Errorf("got %d, want 3000", got)
		}
	})

	t.Run("empty list", func(t *testing.T) {
		got := guard.estimateTotalSize(nil)
		if got != 0 {
			t.Errorf("got %d, want 0", got)
		}
	})

	t.Run("nonexistent file", func(t *testing.T) {
		got := guard.estimateTotalSize([]string{"/nonexistent/file.dump"})
		if got != 0 {
			t.Errorf("got %d, want 0", got)
		}
	})

	t.Run("mixed existing and nonexistent", func(t *testing.T) {
		dir := t.TempDir()
		f1 := filepath.Join(dir, "real.dump")
		os.WriteFile(f1, make([]byte, 500), 0644)

		got := guard.estimateTotalSize([]string{f1, "/no/such/file.dump"})
		if got != 500 {
			t.Errorf("got %d, want 500", got)
		}
	})
}

// =============================================================================
// findLargestDump
// =============================================================================

func TestFindLargestDump(t *testing.T) {
	guard := NewLargeDBGuard(&config.Config{}, logger.NewNullLogger())

	t.Run("multiple sizes", func(t *testing.T) {
		dir := t.TempDir()
		os.WriteFile(filepath.Join(dir, "small.dump"), make([]byte, 100), 0644)
		os.WriteFile(filepath.Join(dir, "big.dump"), make([]byte, 9999), 0644)
		os.WriteFile(filepath.Join(dir, "medium.dump"), make([]byte, 500), 0644)

		result := guard.findLargestDump([]string{
			filepath.Join(dir, "small.dump"),
			filepath.Join(dir, "big.dump"),
			filepath.Join(dir, "medium.dump"),
		})
		if result.name != "big.dump" {
			t.Errorf("got name %q, want big.dump", result.name)
		}
		if result.size != 9999 {
			t.Errorf("got size %d, want 9999", result.size)
		}
	})

	t.Run("empty list", func(t *testing.T) {
		result := guard.findLargestDump(nil)
		if result.name != "" || result.size != 0 {
			t.Errorf("got (%q, %d), want empty", result.name, result.size)
		}
	})

	t.Run("single file", func(t *testing.T) {
		dir := t.TempDir()
		f := filepath.Join(dir, "only.dump")
		os.WriteFile(f, make([]byte, 42), 0644)

		result := guard.findLargestDump([]string{f})
		if result.name != "only.dump" {
			t.Errorf("got name %q, want only.dump", result.name)
		}
	})
}

// =============================================================================
// TunePostgresForRestore
// =============================================================================

func TestTunePostgresForRestore(t *testing.T) {
	guard := NewLargeDBGuard(&config.Config{}, logger.NewNullLogger())

	tests := []struct {
		name      string
		lockBoost int
		wantLock  string
	}{
		{"zero clamped to 2048", 0, "max_locks_per_transaction = 2048"},
		{"below minimum clamped", 1000, "max_locks_per_transaction = 2048"},
		{"exact 4096", 4096, "max_locks_per_transaction = 4096"},
		{"above max capped", 100000, "max_locks_per_transaction = 65536"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cmds := guard.TunePostgresForRestore(tt.lockBoost)
			joined := strings.Join(cmds, "\n")
			if !strings.Contains(joined, tt.wantLock) {
				t.Errorf("commands missing %q:\n%s", tt.wantLock, joined)
			}
			if !strings.Contains(joined, "work_mem") {
				t.Error("missing work_mem setting")
			}
			if !strings.Contains(joined, "pg_reload_conf") {
				t.Error("missing pg_reload_conf")
			}
		})
	}
}

// =============================================================================
// RevertPostgresSettings
// =============================================================================

func TestRevertPostgresSettings(t *testing.T) {
	guard := NewLargeDBGuard(&config.Config{}, logger.NewNullLogger())
	cmds := guard.RevertPostgresSettings()

	joined := strings.Join(cmds, "\n")
	for _, kw := range []string{"RESET work_mem", "RESET maintenance_work_mem", "pg_reload_conf"} {
		if !strings.Contains(joined, kw) {
			t.Errorf("missing %q in revert commands", kw)
		}
	}
}

// =============================================================================
// TuneMySQLForRestore / RevertMySQLSettings
// =============================================================================

func TestTuneMySQLForRestore(t *testing.T) {
	guard := NewLargeDBGuard(&config.Config{}, logger.NewNullLogger())
	cmds := guard.TuneMySQLForRestore()

	joined := strings.Join(cmds, "\n")
	for _, kw := range []string{"innodb_flush_log_at_trx_commit = 2", "foreign_key_checks = 0"} {
		if !strings.Contains(joined, kw) {
			t.Errorf("missing %q in tune commands", kw)
		}
	}
}

func TestRevertMySQLSettings(t *testing.T) {
	guard := NewLargeDBGuard(&config.Config{}, logger.NewNullLogger())
	cmds := guard.RevertMySQLSettings()

	joined := strings.Join(cmds, "\n")
	for _, kw := range []string{"innodb_flush_log_at_trx_commit = 1", "foreign_key_checks = 1"} {
		if !strings.Contains(joined, kw) {
			t.Errorf("missing %q in revert commands", kw)
		}
	}
}

// =============================================================================
// ApplyStrategy
// =============================================================================

func TestApplyStrategy(t *testing.T) {
	guard := NewLargeDBGuard(&config.Config{}, logger.NewNullLogger())

	t.Run("conservative warns but does not override", func(t *testing.T) {
		cfg := &config.Config{Jobs: 8}
		strategy := &RestoreStrategy{UseConservative: true, Reason: "test", Jobs: 1}
		guard.ApplyStrategy(strategy, cfg)
		// Jobs must NOT be overridden
		if cfg.Jobs != 8 {
			t.Errorf("Jobs was overridden to %d, should remain 8", cfg.Jobs)
		}
	})

	t.Run("non-conservative is noop", func(t *testing.T) {
		cfg := &config.Config{Jobs: 4}
		strategy := &RestoreStrategy{UseConservative: false}
		guard.ApplyStrategy(strategy, cfg)
		if cfg.Jobs != 4 {
			t.Errorf("Jobs changed to %d, should remain 4", cfg.Jobs)
		}
	})
}

// =============================================================================
// CheckSystemMemory — basic sanity (does not panic)
// =============================================================================

func TestCheckSystemMemory(t *testing.T) {
	guard := NewLargeDBGuard(&config.Config{}, logger.NewNullLogger())

	t.Run("small backup", func(t *testing.T) {
		check := guard.CheckSystemMemory(100 * 1024 * 1024) // 100MB
		if check == nil {
			t.Fatal("expected non-nil MemoryCheck")
		}
		// Should not be critical for 100MB
		if check.BackupSizeGB > 1 {
			t.Errorf("unexpected BackupSizeGB: %f", check.BackupSizeGB)
		}
	})

	t.Run("cluster archive", func(t *testing.T) {
		check := guard.CheckSystemMemoryWithType(1024*1024*1024, true) // 1GB cluster
		if check == nil {
			t.Fatal("expected non-nil MemoryCheck")
		}
	})
}
