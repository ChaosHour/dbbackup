package retention

import (
	"testing"
	"time"

	"dbbackup/internal/metadata"
)

func TestParseWeekday(t *testing.T) {
	tests := []struct {
		input    string
		expected int
	}{
		{"Sunday", 0},
		{"sunday", 0},
		{"Sun", 0},
		{"Monday", 1},
		{"mon", 1},
		{"Tuesday", 2},
		{"Wed", 3},
		{"Thursday", 4},
		{"Friday", 5},
		{"Saturday", 6},
		{"sat", 6},
		{"invalid", 0},
		{"", 0},
	}

	for _, tc := range tests {
		result := ParseWeekday(tc.input)
		if result != tc.expected {
			t.Errorf("ParseWeekday(%q) = %d, expected %d", tc.input, result, tc.expected)
		}
	}
}

func TestClassifyBackup(t *testing.T) {
	policy := GFSPolicy{
		WeeklyDay:  0,
		MonthlyDay: 1,
	}

	tests := []struct {
		name     string
		time     time.Time
		expected []Tier
	}{
		{
			name:     "Regular weekday",
			time:     time.Date(2024, 1, 15, 10, 0, 0, 0, time.UTC),
			expected: []Tier{TierDaily},
		},
		{
			name:     "Sunday weekly",
			time:     time.Date(2024, 1, 14, 10, 0, 0, 0, time.UTC),
			expected: []Tier{TierDaily, TierWeekly},
		},
		{
			name:     "First of month",
			time:     time.Date(2024, 2, 1, 10, 0, 0, 0, time.UTC),
			expected: []Tier{TierDaily, TierMonthly},
		},
		{
			name:     "First of January yearly",
			time:     time.Date(2024, 1, 1, 10, 0, 0, 0, time.UTC),
			expected: []Tier{TierDaily, TierMonthly, TierYearly},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			result := ClassifyBackup(tc.time, policy)
			if len(result) != len(tc.expected) {
				t.Errorf("ClassifyBackup() returned %d tiers, expected %d", len(result), len(tc.expected))
				return
			}
			for i, tier := range result {
				if tier != tc.expected[i] {
					t.Errorf("tier[%d] = %v, expected %v", i, tier, tc.expected[i])
				}
			}
		})
	}
}

func TestApplyGFSPolicyToBackups(t *testing.T) {
	now := time.Now()
	backups := []*metadata.BackupMetadata{
		{BackupFile: "backup_day1.dump", Timestamp: now.AddDate(0, 0, -1), SizeBytes: 1000},
		{BackupFile: "backup_day2.dump", Timestamp: now.AddDate(0, 0, -2), SizeBytes: 1000},
		{BackupFile: "backup_day3.dump", Timestamp: now.AddDate(0, 0, -3), SizeBytes: 1000},
		{BackupFile: "backup_day4.dump", Timestamp: now.AddDate(0, 0, -4), SizeBytes: 1000},
		{BackupFile: "backup_day5.dump", Timestamp: now.AddDate(0, 0, -5), SizeBytes: 1000},
		{BackupFile: "backup_day6.dump", Timestamp: now.AddDate(0, 0, -6), SizeBytes: 1000},
		{BackupFile: "backup_day7.dump", Timestamp: now.AddDate(0, 0, -7), SizeBytes: 1000},
		{BackupFile: "backup_day8.dump", Timestamp: now.AddDate(0, 0, -8), SizeBytes: 1000},
		{BackupFile: "backup_day9.dump", Timestamp: now.AddDate(0, 0, -9), SizeBytes: 1000},
		{BackupFile: "backup_day10.dump", Timestamp: now.AddDate(0, 0, -10), SizeBytes: 1000},
	}

	policy := GFSPolicy{
		Enabled:    true,
		Daily:      5,
		Weekly:     2,
		Monthly:    1,
		Yearly:     1,
		WeeklyDay:  0,
		MonthlyDay: 1,
		DryRun:     true,
	}

	result, err := ApplyGFSPolicyToBackups(backups, policy)
	if err != nil {
		t.Fatalf("ApplyGFSPolicyToBackups() error = %v", err)
	}

	if result.TotalKept < policy.Daily {
		t.Errorf("TotalKept = %d, expected at least %d", result.TotalKept, policy.Daily)
	}

	if result.TotalBackups != len(backups) {
		t.Errorf("TotalBackups = %d, expected %d", result.TotalBackups, len(backups))
	}

	if len(result.ToKeep)+len(result.ToDelete) != result.TotalBackups {
		t.Errorf("ToKeep(%d) + ToDelete(%d) != TotalBackups(%d)",
			len(result.ToKeep), len(result.ToDelete), result.TotalBackups)
	}
}

func TestGFSPolicyWithEmptyBackups(t *testing.T) {
	policy := DefaultGFSPolicy()
	policy.DryRun = true

	result, err := ApplyGFSPolicyToBackups([]*metadata.BackupMetadata{}, policy)
	if err != nil {
		t.Fatalf("ApplyGFSPolicyToBackups() error = %v", err)
	}

	if result.TotalBackups != 0 {
		t.Errorf("TotalBackups = %d, expected 0", result.TotalBackups)
	}

	if result.TotalKept != 0 {
		t.Errorf("TotalKept = %d, expected 0", result.TotalKept)
	}
}

func TestDefaultGFSPolicy(t *testing.T) {
	policy := DefaultGFSPolicy()

	if !policy.Enabled {
		t.Error("DefaultGFSPolicy should be enabled")
	}

	if policy.Daily != 7 {
		t.Errorf("Daily = %d, expected 7", policy.Daily)
	}

	if policy.Weekly != 4 {
		t.Errorf("Weekly = %d, expected 4", policy.Weekly)
	}

	if policy.Monthly != 12 {
		t.Errorf("Monthly = %d, expected 12", policy.Monthly)
	}

	if policy.Yearly != 3 {
		t.Errorf("Yearly = %d, expected 3", policy.Yearly)
	}
}

func TestTierString(t *testing.T) {
	tests := []struct {
		tier     Tier
		expected string
	}{
		{TierDaily, "daily"},
		{TierWeekly, "weekly"},
		{TierMonthly, "monthly"},
		{TierYearly, "yearly"},
		{Tier(99), "unknown"},
	}

	for _, tc := range tests {
		result := tc.tier.String()
		if result != tc.expected {
			t.Errorf("Tier(%d).String() = %q, expected %q", tc.tier, result, tc.expected)
		}
	}
}
