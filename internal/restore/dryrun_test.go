package restore

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"dbbackup/internal/config"
	"dbbackup/internal/logger"
)

// =============================================================================
// DryRunStatus.String() — table-driven
// =============================================================================

func TestDryRunStatusString(t *testing.T) {
	tests := []struct {
		status DryRunStatus
		want   string
	}{
		{DryRunPassed, "PASS"},
		{DryRunWarning, "WARN"},
		{DryRunFailed, "FAIL"},
		{DryRunSkipped, "SKIP"},
		{DryRunStatus(99), "UNKNOWN"},
	}

	for _, tt := range tests {
		t.Run(tt.want, func(t *testing.T) {
			if got := tt.status.String(); got != tt.want {
				t.Errorf("got %q, want %q", got, tt.want)
			}
		})
	}
}

// =============================================================================
// DryRunStatus.Icon() — table-driven
// =============================================================================

func TestDryRunStatusIcon(t *testing.T) {
	tests := []struct {
		status DryRunStatus
		want   string
	}{
		{DryRunPassed, "[+]"},
		{DryRunWarning, "[!]"},
		{DryRunFailed, "[-]"},
		{DryRunSkipped, "[ ]"},
		{DryRunStatus(99), "[?]"},
	}

	for _, tt := range tests {
		t.Run(tt.want, func(t *testing.T) {
			if got := tt.status.Icon(); got != tt.want {
				t.Errorf("got %q, want %q", got, tt.want)
			}
		})
	}
}

// =============================================================================
// formatBytesSize — comprehensive
// =============================================================================

func TestFormatBytesSizeComprehensive(t *testing.T) {
	tests := []struct {
		name  string
		bytes int64
		want  string
	}{
		{"bytes", 512, "512 B"},
		{"kilobytes", 2048, "2.0 KB"},
		{"megabytes", 5 * 1024 * 1024, "5.0 MB"},
		{"gigabytes", 3 * 1024 * 1024 * 1024, "3.0 GB"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := formatBytesSize(tt.bytes)
			if got != tt.want {
				t.Errorf("got %q, want %q", got, tt.want)
			}
		})
	}
}

// =============================================================================
// checkArchiveAccess — comprehensive
// =============================================================================

func TestCheckArchiveAccessComprehensive(t *testing.T) {
	log := logger.NewNullLogger()
	cfg := &config.Config{}

	t.Run("file not found", func(t *testing.T) {
		dr := NewRestoreDryRun(cfg, log, "/nonexistent/archive.dump", "testdb")
		check := dr.checkArchiveAccess()
		if check.Status != DryRunFailed {
			t.Errorf("got status %v, want FAIL", check.Status)
		}
		if check.Message != "Archive file not found" {
			t.Errorf("unexpected message: %s", check.Message)
		}
	})

	t.Run("empty file", func(t *testing.T) {
		dir := t.TempDir()
		f := filepath.Join(dir, "empty.dump")
		os.WriteFile(f, []byte{}, 0644)

		dr := NewRestoreDryRun(cfg, log, f, "testdb")
		check := dr.checkArchiveAccess()
		if check.Status != DryRunFailed {
			t.Errorf("got status %v, want FAIL", check.Status)
		}
	})

	t.Run("valid file", func(t *testing.T) {
		dir := t.TempDir()
		f := filepath.Join(dir, "valid.dump")
		os.WriteFile(f, make([]byte, 1024), 0644)

		dr := NewRestoreDryRun(cfg, log, f, "testdb")
		check := dr.checkArchiveAccess()
		if check.Status != DryRunPassed {
			t.Errorf("got status %v, want PASS", check.Status)
		}
	})
}

// =============================================================================
// checkWorkDirectory — comprehensive
// =============================================================================

func TestCheckWorkDirectoryComprehensive(t *testing.T) {
	log := logger.NewNullLogger()
	dir := t.TempDir()
	archiveFile := filepath.Join(dir, "test.dump")
	os.WriteFile(archiveFile, make([]byte, 100), 0644)

	t.Run("writable directory", func(t *testing.T) {
		workDir := t.TempDir()
		cfg := &config.Config{WorkDir: workDir}
		dr := NewRestoreDryRun(cfg, log, archiveFile, "testdb")
		check := dr.checkWorkDirectory()
		if check.Status != DryRunPassed {
			t.Errorf("got status %v, want PASS: %s", check.Status, check.Message)
		}
	})

	t.Run("nonexistent directory", func(t *testing.T) {
		cfg := &config.Config{WorkDir: "/nonexistent/dir/foo"}
		dr := NewRestoreDryRun(cfg, log, archiveFile, "testdb")
		check := dr.checkWorkDirectory()
		if check.Status != DryRunFailed {
			t.Errorf("got status %v, want FAIL", check.Status)
		}
	})

	t.Run("file not dir", func(t *testing.T) {
		tmpFile := filepath.Join(dir, "notadir")
		os.WriteFile(tmpFile, []byte("x"), 0644)
		cfg := &config.Config{WorkDir: tmpFile}
		dr := NewRestoreDryRun(cfg, log, archiveFile, "testdb")
		check := dr.checkWorkDirectory()
		if check.Status != DryRunFailed {
			t.Errorf("got status %v, want FAIL", check.Status)
		}
	})
}

// =============================================================================
// checkTargetConflicts — empty target
// =============================================================================

func TestCheckTargetConflicts_EmptyTarget(t *testing.T) {
	cfg := &config.Config{}
	log := logger.NewNullLogger()
	dir := t.TempDir()
	f := filepath.Join(dir, "test.dump")
	os.WriteFile(f, make([]byte, 100), 0644)

	dr := NewRestoreDryRun(cfg, log, f, "")
	check := dr.checkTargetConflicts(context.TODO())
	if check.Status != DryRunSkipped {
		t.Errorf("got status %v, want SKIP for empty target", check.Status)
	}
}

// =============================================================================
// estimateRestoreTime — comprehensive
// =============================================================================

func TestEstimateRestoreTimeComprehensive(t *testing.T) {
	log := logger.NewNullLogger()
	cfg := &config.Config{}

	t.Run("small file min 1 minute", func(t *testing.T) {
		dir := t.TempDir()
		f := filepath.Join(dir, "small.dump")
		os.WriteFile(f, make([]byte, 1024), 0644)

		dr := NewRestoreDryRun(cfg, log, f, "testdb")
		dur := dr.estimateRestoreTime()
		if dur < time.Minute {
			t.Errorf("expected at least 1m, got %v", dur)
		}
	})

	t.Run("large file scales", func(t *testing.T) {
		dir := t.TempDir()
		f := filepath.Join(dir, "large.dump")
		// 500MB
		os.WriteFile(f, make([]byte, 500*1024*1024), 0644)

		dr := NewRestoreDryRun(cfg, log, f, "testdb")
		dur := dr.estimateRestoreTime()
		if dur < 2*time.Minute {
			t.Errorf("expected > 2 minutes for 500MB, got %v", dur)
		}
	})

	t.Run("nonexistent returns 0", func(t *testing.T) {
		dr := NewRestoreDryRun(cfg, log, "/no/file", "testdb")
		dur := dr.estimateRestoreTime()
		if dur != 0 {
			t.Errorf("expected 0, got %v", dur)
		}
	})
}

// =============================================================================
// DryRunResult summary fields
// =============================================================================

func TestDryRunResult_Summary(t *testing.T) {
	t.Run("critical count", func(t *testing.T) {
		result := &DryRunResult{
			Checks: []DryRunCheck{
				{Status: DryRunFailed, Critical: true},
				{Status: DryRunFailed, Critical: true},
				{Status: DryRunPassed},
			},
		}

		critCount := 0
		for _, c := range result.Checks {
			if c.Status == DryRunFailed && c.Critical {
				critCount++
			}
		}
		if critCount != 2 {
			t.Errorf("got %d criticals, want 2", critCount)
		}
	})

	t.Run("warning count", func(t *testing.T) {
		result := &DryRunResult{
			Checks: []DryRunCheck{
				{Status: DryRunWarning},
				{Status: DryRunWarning},
				{Status: DryRunPassed},
			},
		}

		warnCount := 0
		for _, c := range result.Checks {
			if c.Status == DryRunWarning {
				warnCount++
			}
		}
		if warnCount != 2 {
			t.Errorf("got %d warnings, want 2", warnCount)
		}
	})

	t.Run("can proceed when no critical", func(t *testing.T) {
		result := &DryRunResult{
			CanProceed: true,
			Checks: []DryRunCheck{
				{Status: DryRunWarning},
				{Status: DryRunPassed},
			},
		}

		canProceed := true
		for _, c := range result.Checks {
			if c.Status == DryRunFailed && c.Critical {
				canProceed = false
			}
		}
		if !canProceed {
			t.Error("should be able to proceed without critical failures")
		}
		if !result.CanProceed {
			t.Error("CanProceed should be true")
		}
	})
}
