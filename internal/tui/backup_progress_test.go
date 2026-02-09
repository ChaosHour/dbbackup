package tui

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"dbbackup/internal/progress"
)

// TestExtrapolateBackupTotal tests extrapolation math for tiered progress
func TestExtrapolateBackupTotal(t *testing.T) {
	tests := []struct {
		name          string
		completedDBs  []completedDBBackup
		dbTotal       int
		currentEst    int64
		expectGreater int64 // Result should be >= this
		expectLess    int64 // Result should be <= this
	}{
		{
			name:          "no completed DBs returns current estimate",
			completedDBs:  nil,
			dbTotal:       4,
			currentEst:    100 * 1024 * 1024 * 1024, // 100 GB
			expectGreater: 100 * 1024 * 1024 * 1024,
			expectLess:    100*1024*1024*1024 + 1,
		},
		{
			name: "single DB extrapolates to total with buffer",
			completedDBs: []completedDBBackup{
				{Name: "db1", FinalSize: 50 * 1024 * 1024 * 1024}, // 50 GB
			},
			dbTotal:       2,
			currentEst:    100 * 1024 * 1024 * 1024,
			expectGreater: 100 * 1024 * 1024 * 1024, // 50GB avg × 2 × 1.1 = 110 GB
			expectLess:    120 * 1024 * 1024 * 1024,
		},
		{
			name: "multiple DBs average correctly",
			completedDBs: []completedDBBackup{
				{Name: "db1", FinalSize: 10 * 1024 * 1024 * 1024}, // 10 GB
				{Name: "db2", FinalSize: 30 * 1024 * 1024 * 1024}, // 30 GB
			},
			dbTotal:       4,
			currentEst:    80 * 1024 * 1024 * 1024,
			expectGreater: 80 * 1024 * 1024 * 1024,  // 20GB avg × 4 × 1.1 = 88 GB
			expectLess:    100 * 1024 * 1024 * 1024,
		},
		{
			name:          "zero dbTotal returns current estimate",
			completedDBs:  []completedDBBackup{{Name: "db1", FinalSize: 50 * 1024 * 1024 * 1024}},
			dbTotal:       0,
			currentEst:    100 * 1024 * 1024 * 1024,
			expectGreater: 100 * 1024 * 1024 * 1024,
			expectLess:    100*1024*1024*1024 + 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := extrapolateBackupTotal(tt.completedDBs, tt.dbTotal, tt.currentEst)
			if result < tt.expectGreater {
				t.Errorf("extrapolateBackupTotal() = %d, want >= %d", result, tt.expectGreater)
			}
			if result > tt.expectLess {
				t.Errorf("extrapolateBackupTotal() = %d, want <= %d", result, tt.expectLess)
			}
		})
	}
}

// TestDetectBackupMetadataSource tests metadata detection for tiered progress
func TestDetectBackupMetadataSource(t *testing.T) {
	t.Run("empty dir returns unknown", func(t *testing.T) {
		result := detectBackupMetadataSource("")
		if result != "unknown" {
			t.Errorf("detectBackupMetadataSource('') = %q, want 'unknown'", result)
		}
	})

	t.Run("nonexistent dir returns unknown", func(t *testing.T) {
		result := detectBackupMetadataSource("/tmp/nonexistent_dir_test_12345")
		if result != "unknown" {
			t.Errorf("detectBackupMetadataSource(nonexistent) = %q, want 'unknown'", result)
		}
	})

	t.Run("dir with meta.json returns accurate", func(t *testing.T) {
		tmpDir := t.TempDir()
		metaFile := filepath.Join(tmpDir, "cluster_20260209_120000.tar.gz.meta.json")
		if err := os.WriteFile(metaFile, []byte(`{"version":"6.0","databases":[]}`), 0644); err != nil {
			t.Fatal(err)
		}

		result := detectBackupMetadataSource(tmpDir)
		if result != "accurate" {
			t.Errorf("detectBackupMetadataSource(with meta) = %q, want 'accurate'", result)
		}
	})

	t.Run("dir without meta.json returns unknown", func(t *testing.T) {
		tmpDir := t.TempDir()
		// Create a non-meta file
		if err := os.WriteFile(filepath.Join(tmpDir, "cluster_20260209.tar.gz"), []byte("data"), 0644); err != nil {
			t.Fatal(err)
		}

		result := detectBackupMetadataSource(tmpDir)
		if result != "unknown" {
			t.Errorf("detectBackupMetadataSource(without meta) = %q, want 'unknown'", result)
		}
	})
}

// TestRenderBackupProgressTier1Accurate tests Tier 1 display (accurate metadata)
func TestRenderBackupProgressTier1Accurate(t *testing.T) {
	estimator := progress.NewEMAEstimator(0.5, 2)
	now := time.Now()
	// Warm up estimator
	estimator.Update(0, now)
	estimator.Update(10*1024*1024*1024, now.Add(10*time.Second))
	estimator.Update(20*1024*1024*1024, now.Add(20*time.Second))

	result := renderBackupDatabaseProgressBarWithTiming(
		1, 2, 20*time.Second,
		50*1024*1024*1024, 100*1024*1024*1024,
		estimator, "accurate",
	)

	// Tier 1 should show full progress bar, percentage, speed, and ETA (not ~ETA)
	if !strings.Contains(result, "█") {
		t.Error("Tier 1 should show progress bar")
	}
	if !strings.Contains(result, "50%") {
		t.Error("Tier 1 should show 50%")
	}
	if !strings.Contains(result, "ETA:") {
		t.Error("Tier 1 should show ETA")
	}
	if strings.Contains(result, "~") {
		t.Error("Tier 1 should NOT show ~ prefix on ETA")
	}
	if strings.Contains(result, "est.") {
		t.Error("Tier 1 should NOT show 'est.' suffix")
	}
}

// TestRenderBackupProgressTier2Extrapolated tests Tier 2 display (extrapolated)
func TestRenderBackupProgressTier2Extrapolated(t *testing.T) {
	estimator := progress.NewEMAEstimator(0.5, 2)
	now := time.Now()
	estimator.Update(0, now)
	estimator.Update(10*1024*1024*1024, now.Add(10*time.Second))
	estimator.Update(20*1024*1024*1024, now.Add(20*time.Second))

	result := renderBackupDatabaseProgressBarWithTiming(
		1, 2, 20*time.Second,
		50*1024*1024*1024, 100*1024*1024*1024,
		estimator, "extrapolated",
	)

	// Tier 2 should show ~ETA and est.
	if !strings.Contains(result, "█") {
		t.Error("Tier 2 should show progress bar")
	}
	if !strings.Contains(result, "est.") {
		t.Error("Tier 2 should show 'est.' suffix")
	}
	if !strings.Contains(result, "ETA: ~") {
		t.Error("Tier 2 should show ~ETA prefix")
	}
}

// TestRenderBackupProgressTier3Unknown tests Tier 3 display (unknown)
func TestRenderBackupProgressTier3Unknown(t *testing.T) {
	estimator := progress.NewEMAEstimator(0.5, 2)
	now := time.Now()
	estimator.Update(0, now)
	estimator.Update(5*1024*1024*1024, now.Add(10*time.Second))
	estimator.Update(10*1024*1024*1024, now.Add(20*time.Second))

	result := renderBackupDatabaseProgressBarWithTiming(
		0, 2, 20*time.Second,
		5*1024*1024*1024, 100*1024*1024*1024,
		estimator, "unknown",
	)

	// Tier 3 should show throughput only, no ETA or percentage
	if strings.Contains(result, "ETA") {
		t.Error("Tier 3 should NOT show ETA")
	}
	if strings.Contains(result, "█") {
		t.Error("Tier 3 should NOT show progress bar")
	}
	if !strings.Contains(result, "transferred") {
		t.Error("Tier 3 should show 'transferred'")
	}
	if !strings.Contains(result, "Elapsed:") {
		t.Error("Tier 3 should show Elapsed")
	}
}

// TestRenderBackupProgressTier4OverBudget tests Tier 4 display (over budget)
func TestRenderBackupProgressTier4OverBudget(t *testing.T) {
	estimator := progress.NewEMAEstimator(0.5, 2)
	now := time.Now()
	estimator.Update(0, now)
	estimator.Update(50*1024*1024*1024, now.Add(10*time.Second))
	estimator.Update(110*1024*1024*1024, now.Add(20*time.Second))

	// bytesDone > bytesTotal * 1.05 → over budget
	result := renderBackupDatabaseProgressBarWithTiming(
		1, 2, 20*time.Second,
		110*1024*1024*1024, 100*1024*1024*1024,
		estimator, "extrapolated", // Non-accurate source triggers over-budget
	)

	// Tier 4 should drop ETA and show "Calculating..."
	if !strings.Contains(result, "Calculating...") {
		t.Errorf("Tier 4 should show 'Calculating...' but got: %s", result)
	}
	if strings.Contains(result, "█") {
		t.Error("Tier 4 should NOT show progress bar")
	}
}

// TestRenderBackupProgressTier1AccurateOverBudget tests that accurate metadata doesn't trigger over-budget
func TestRenderBackupProgressTier1AccurateOverBudget(t *testing.T) {
	estimator := progress.NewEMAEstimator(0.5, 2)
	now := time.Now()
	estimator.Update(0, now)
	estimator.Update(50*1024*1024*1024, now.Add(10*time.Second))
	estimator.Update(110*1024*1024*1024, now.Add(20*time.Second))

	// Even over budget, Tier 1 (accurate) should still show progress bar capped at 100%
	result := renderBackupDatabaseProgressBarWithTiming(
		1, 2, 20*time.Second,
		110*1024*1024*1024, 100*1024*1024*1024,
		estimator, "accurate",
	)

	// Accurate source should NOT switch to over-budget display
	if strings.Contains(result, "Calculating...") {
		t.Error("Tier 1 (accurate) should NOT show 'Calculating...' even when over budget")
	}
	if !strings.Contains(result, "100%") {
		t.Error("Tier 1 (accurate) should cap at 100%")
	}
}

// TestRenderBackupProgressEmptyTotal tests edge case with zero total
func TestRenderBackupProgressEmptyTotal(t *testing.T) {
	result := renderBackupDatabaseProgressBarWithTiming(0, 0, 0, 0, 0, nil, "unknown")
	if result != "" {
		t.Errorf("Should return empty string for zero total, got: %q", result)
	}
}

// TestCompletedDBBackupTracking tests the completed DB tracking logic
func TestCompletedDBBackupTracking(t *testing.T) {
	completed := []completedDBBackup{
		{Name: "small_db", FinalSize: 1 * 1024 * 1024 * 1024},  // 1 GB
		{Name: "large_db", FinalSize: 50 * 1024 * 1024 * 1024}, // 50 GB
	}

	// Average should be ~25.5 GB, extrapolated for 4 DBs with 10% buffer
	result := extrapolateBackupTotal(completed, 4, 80*1024*1024*1024)

	// Expected: ((1+50)/2) * 4 * 1.10 = 25.5 * 4 * 1.10 = 112.2 GB
	expectedMin := int64(100 * 1024 * 1024 * 1024) // At least 100 GB
	expectedMax := int64(120 * 1024 * 1024 * 1024) // At most 120 GB

	if result < expectedMin || result > expectedMax {
		t.Errorf("extrapolateBackupTotal() = %d bytes, want between %d and %d",
			result, expectedMin, expectedMax)
	}
}
