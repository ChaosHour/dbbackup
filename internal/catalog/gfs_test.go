package catalog

import (
	"testing"
	"time"
)

func TestClassifyGFS_KeepDaily(t *testing.T) {
	now := time.Now()
	backups := make([]*Entry, 10)
	for i := 0; i < 10; i++ {
		backups[i] = &Entry{
			ID:        int64(i + 1),
			Database:  "mydb",
			CreatedAt: now.Add(-time.Duration(i) * 24 * time.Hour),
			SizeBytes: 1000,
		}
	}

	policy := &GFSPolicy{KeepDaily: 3, KeepWeekly: 0, KeepMonthly: 0, KeepYearly: 0}
	keep := classifyGFS(backups, policy)

	// Should keep the 3 newest
	for _, b := range backups[:3] {
		if keep[b.ID] != "daily" {
			t.Errorf("expected backup %d to be kept as daily, got %q", b.ID, keep[b.ID])
		}
	}
	// The rest should be unmarked (to delete)
	for _, b := range backups[3:] {
		if keep[b.ID] != "" {
			t.Errorf("expected backup %d to be pruned, got %q", b.ID, keep[b.ID])
		}
	}
}

func TestClassifyGFS_KeepWeekly(t *testing.T) {
	now := time.Now()
	var backups []*Entry
	// Create 2 backups per week for 6 weeks
	for w := 0; w < 6; w++ {
		for d := 0; d < 2; d++ {
			backups = append(backups, &Entry{
				ID:        int64(w*2 + d + 1),
				Database:  "mydb",
				CreatedAt: now.Add(-time.Duration(w*7+d) * 24 * time.Hour),
				SizeBytes: 1000,
			})
		}
	}

	policy := &GFSPolicy{KeepDaily: 0, KeepWeekly: 4, KeepMonthly: 0, KeepYearly: 0}
	keep := classifyGFS(backups, policy)

	weeklyCount := 0
	for _, tier := range keep {
		if tier == "weekly" {
			weeklyCount++
		}
	}
	if weeklyCount > 4 {
		t.Errorf("expected at most 4 weekly backups, got %d", weeklyCount)
	}
	if weeklyCount == 0 {
		t.Error("expected some weekly backups to be kept")
	}
}

func TestClassifyGFS_KeepMonthly(t *testing.T) {
	now := time.Now()
	var backups []*Entry
	// Create 2 backups per month for 6 months
	for m := 0; m < 6; m++ {
		for d := 0; d < 2; d++ {
			backups = append(backups, &Entry{
				ID:        int64(m*2 + d + 1),
				Database:  "mydb",
				CreatedAt: now.AddDate(0, -m, -d),
				SizeBytes: 1000,
			})
		}
	}

	policy := &GFSPolicy{KeepDaily: 0, KeepWeekly: 0, KeepMonthly: 3, KeepYearly: 0}
	keep := classifyGFS(backups, policy)

	monthlyCount := 0
	for _, tier := range keep {
		if tier == "monthly" {
			monthlyCount++
		}
	}
	if monthlyCount > 3 {
		t.Errorf("expected at most 3 monthly backups, got %d", monthlyCount)
	}
	if monthlyCount == 0 {
		t.Error("expected some monthly backups to be kept")
	}
}

func TestClassifyGFS_KeepYearly(t *testing.T) {
	now := time.Now()
	var backups []*Entry
	for y := 0; y < 5; y++ {
		backups = append(backups, &Entry{
			ID:        int64(y + 1),
			Database:  "mydb",
			CreatedAt: now.AddDate(-y, 0, 0),
			SizeBytes: 1000,
		})
	}

	policy := &GFSPolicy{KeepDaily: 0, KeepWeekly: 0, KeepMonthly: 0, KeepYearly: 2}
	keep := classifyGFS(backups, policy)

	yearlyCount := 0
	for _, tier := range keep {
		if tier == "yearly" {
			yearlyCount++
		}
	}
	if yearlyCount != 2 {
		t.Errorf("expected 2 yearly backups, got %d", yearlyCount)
	}
}

func TestClassifyGFS_Combined(t *testing.T) {
	now := time.Now()
	var backups []*Entry

	// 30 daily backups
	for i := 0; i < 30; i++ {
		backups = append(backups, &Entry{
			ID:        int64(i + 1),
			Database:  "mydb",
			CreatedAt: now.Add(-time.Duration(i) * 24 * time.Hour),
			SizeBytes: 1000,
		})
	}

	policy := &GFSPolicy{
		KeepDaily:   7,
		KeepWeekly:  4,
		KeepMonthly: 2,
		KeepYearly:  1,
	}
	keep := classifyGFS(backups, policy)

	// Count kept
	kept := 0
	for _, tier := range keep {
		if tier != "" {
			kept++
		}
	}

	// Should keep at least 7 (daily) but some daily may overlap with weekly/monthly
	if kept < 7 {
		t.Errorf("expected at least 7 kept backups, got %d", kept)
	}

	// Should not keep all 30
	if kept >= 30 {
		t.Errorf("expected some backups to be pruned, but kept all %d", kept)
	}
}

func TestClassifyGFS_NoBackups(t *testing.T) {
	policy := &GFSPolicy{KeepDaily: 7, KeepWeekly: 4, KeepMonthly: 12, KeepYearly: 3}
	keep := classifyGFS(nil, policy)
	if len(keep) != 0 {
		t.Errorf("expected empty result for no backups, got %d entries", len(keep))
	}
}

func TestClassifyGFS_FewerBackupsThanPolicy(t *testing.T) {
	now := time.Now()
	backups := []*Entry{
		{ID: 1, Database: "mydb", CreatedAt: now, SizeBytes: 1000},
		{ID: 2, Database: "mydb", CreatedAt: now.Add(-24 * time.Hour), SizeBytes: 1000},
	}

	policy := &GFSPolicy{KeepDaily: 7, KeepWeekly: 4, KeepMonthly: 12, KeepYearly: 3}
	keep := classifyGFS(backups, policy)

	// All should be kept — there are fewer backups than the daily quota alone
	for _, b := range backups {
		if keep[b.ID] == "" {
			t.Errorf("expected backup %d to be kept, but it was marked for deletion", b.ID)
		}
	}
}

func TestClassifyGFS_TierPriority(t *testing.T) {
	// A backup that qualifies for daily AND weekly should be assigned to daily
	// (first tier wins since we process daily first)
	now := time.Now()
	backups := []*Entry{
		{ID: 1, Database: "mydb", CreatedAt: now, SizeBytes: 1000},
	}

	policy := &GFSPolicy{KeepDaily: 1, KeepWeekly: 1, KeepMonthly: 1, KeepYearly: 1}
	keep := classifyGFS(backups, policy)

	if keep[1] != "daily" {
		t.Errorf("expected single backup to be classified as daily, got %q", keep[1])
	}
}

func TestFormatAge(t *testing.T) {
	tests := []struct {
		dur      time.Duration
		expected string
	}{
		{30 * time.Minute, "30m"},
		{3 * time.Hour, "3h"},
		{5 * 24 * time.Hour, "5d"},
		{60 * 24 * time.Hour, "2mo"},
		{400 * 24 * time.Hour, "1y"},
	}

	for _, tt := range tests {
		got := formatAge(tt.dur)
		if got != tt.expected {
			t.Errorf("formatAge(%v) = %q, want %q", tt.dur, got, tt.expected)
		}
	}
}
