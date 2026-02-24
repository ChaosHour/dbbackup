package backup

import (
	"strings"
	"testing"
	"time"
)

// =============================================================================
// FormatSizeEstimate
// =============================================================================

func TestFormatSizeEstimate(t *testing.T) {
	t.Run("small database", func(t *testing.T) {
		est := &SizeEstimate{
			DatabaseName:        "testdb",
			EstimatedRawSize:    100 * 1024 * 1024,
			EstimatedCompressed: 30 * 1024 * 1024,
			CompressionRatio:    0.3,
			TableCount:          10,
			LargestTable:        "users",
			LargestTableSize:    50 * 1024 * 1024,
			EstimatedDuration:   2 * time.Minute,
			RecommendedProfile:  "default",
			RequiredDiskSpace:   200 * 1024 * 1024,
			AvailableDiskSpace:  10 * 1024 * 1024 * 1024,
			HasSufficientSpace:  true,
		}

		result := FormatSizeEstimate(est)
		for _, want := range []string{"testdb", "users", "default", "Sufficient"} {
			if !strings.Contains(result, want) {
				t.Errorf("result missing %q:\n%s", want, result)
			}
		}
	})

	t.Run("large database", func(t *testing.T) {
		est := &SizeEstimate{
			DatabaseName:        "bigdb",
			EstimatedRawSize:    50 * 1024 * 1024 * 1024,
			EstimatedCompressed: 15 * 1024 * 1024 * 1024,
			CompressionRatio:    0.3,
			TableCount:          500,
			LargestTable:        "events",
			LargestTableSize:    10 * 1024 * 1024 * 1024,
			EstimatedDuration:   45 * time.Minute,
			RecommendedProfile:  "performance",
			RequiredDiskSpace:   100 * 1024 * 1024 * 1024,
			AvailableDiskSpace:  50 * 1024 * 1024 * 1024,
			HasSufficientSpace:  false,
		}

		result := FormatSizeEstimate(est)
		if !strings.Contains(result, "INSUFFICIENT") {
			t.Errorf("expected INSUFFICIENT warning:\n%s", result)
		}
	})

	t.Run("no largest table", func(t *testing.T) {
		est := &SizeEstimate{
			DatabaseName:       "emptydb",
			TableCount:         0,
			HasSufficientSpace: true,
		}

		result := FormatSizeEstimate(est)
		if !strings.Contains(result, "emptydb") {
			t.Errorf("missing db name:\n%s", result)
		}
	})
}

// =============================================================================
// FormatClusterSizeEstimate
// =============================================================================

func TestFormatClusterSizeEstimate(t *testing.T) {
	t.Run("happy path", func(t *testing.T) {
		est := &ClusterSizeEstimate{
			TotalDatabases:      5,
			TotalRawSize:        200 * 1024 * 1024 * 1024,
			TotalCompressed:     60 * 1024 * 1024 * 1024,
			LargestDatabase:     "analytics",
			LargestDatabaseSize: 100 * 1024 * 1024 * 1024,
			EstimatedDuration:   2 * time.Hour,
			RequiredDiskSpace:   400 * 1024 * 1024 * 1024,
			AvailableDiskSpace:  500 * 1024 * 1024 * 1024,
			HasSufficientSpace:  true,
		}

		result := FormatClusterSizeEstimate(est)
		for _, want := range []string{"analytics", "Sufficient", "Cluster"} {
			if !strings.Contains(result, want) {
				t.Errorf("result missing %q:\n%s", want, result)
			}
		}
	})

	t.Run("empty cluster", func(t *testing.T) {
		est := &ClusterSizeEstimate{
			TotalDatabases:     0,
			HasSufficientSpace: true,
		}

		result := FormatClusterSizeEstimate(est)
		if !strings.Contains(result, "0") {
			t.Errorf("expected 0 databases:\n%s", result)
		}
	})
}

// =============================================================================
// getSpaceStatus
// =============================================================================

func TestGetSpaceStatus(t *testing.T) {
	tests := []struct {
		name      string
		hasSuffi  bool
		wantPart  string
	}{
		{"sufficient", true, "Sufficient"},
		{"insufficient", false, "INSUFFICIENT"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := getSpaceStatus(tt.hasSuffi)
			if !strings.Contains(got, tt.wantPart) {
				t.Errorf("got %q, want containing %q", got, tt.wantPart)
			}
		})
	}
}
