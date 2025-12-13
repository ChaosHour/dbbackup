package retention

import (
	"sort"
	"strings"
	"time"

	"dbbackup/internal/metadata"
)

// Tier represents a retention tier in GFS scheme
type Tier int

const (
	TierDaily Tier = iota
	TierWeekly
	TierMonthly
	TierYearly
)

func (t Tier) String() string {
	switch t {
	case TierDaily:
		return "daily"
	case TierWeekly:
		return "weekly"
	case TierMonthly:
		return "monthly"
	case TierYearly:
		return "yearly"
	default:
		return "unknown"
	}
}

// ParseWeekday converts a weekday name to its integer value (0=Sunday, etc.)
func ParseWeekday(name string) int {
	name = strings.ToLower(strings.TrimSpace(name))
	weekdays := map[string]int{
		"sunday":    0,
		"sun":       0,
		"monday":    1,
		"mon":       1,
		"tuesday":   2,
		"tue":       2,
		"wednesday": 3,
		"wed":       3,
		"thursday":  4,
		"thu":       4,
		"friday":    5,
		"fri":       5,
		"saturday":  6,
		"sat":       6,
	}
	if val, ok := weekdays[name]; ok {
		return val
	}
	return 0 // Default to Sunday
}

// GFSPolicy defines a Grandfather-Father-Son retention policy
type GFSPolicy struct {
	Enabled    bool
	Daily      int  // Number of daily backups to keep
	Weekly     int  // Number of weekly backups to keep (e.g., Sunday)
	Monthly    int  // Number of monthly backups to keep (e.g., 1st of month)
	Yearly     int  // Number of yearly backups to keep (e.g., Jan 1st)
	WeeklyDay  int  // Day of week for weekly (0=Sunday, 1=Monday, etc.)
	MonthlyDay int  // Day of month for monthly (1-31, 0 means last day)
	DryRun     bool // Preview mode - don't actually delete
}

// DefaultGFSPolicy returns a sensible default GFS policy
func DefaultGFSPolicy() GFSPolicy {
	return GFSPolicy{
		Enabled:    true,
		Daily:      7,
		Weekly:     4,
		Monthly:    12,
		Yearly:     3,
		WeeklyDay:  0, // Sunday
		MonthlyDay: 1, // 1st of month
		DryRun:     false,
	}
}

// BackupClassification holds the tier classification for a backup
type BackupClassification struct {
	Backup       *metadata.BackupMetadata
	Tiers        []Tier
	IsBestDaily  bool
	IsBestWeekly bool
	IsBestMonth  bool
	IsBestYear   bool
	DayKey       string // YYYY-MM-DD
	WeekKey      string // YYYY-WNN
	MonthKey     string // YYYY-MM
	YearKey      string // YYYY
}

// GFSResult contains the results of GFS policy application
type GFSResult struct {
	TotalBackups int
	ToDelete     []*metadata.BackupMetadata
	ToKeep       []*metadata.BackupMetadata
	Deleted      []string // File paths that were deleted (or would be in dry-run)
	Kept         []string // File paths that are kept
	TotalKept    int      // Total count of kept backups
	DailyKept    int
	WeeklyKept   int
	MonthlyKept  int
	YearlyKept   int
	SpaceFreed   int64
	Errors       []error
}

// ApplyGFSPolicy applies Grandfather-Father-Son retention policy
func ApplyGFSPolicy(backupDir string, policy GFSPolicy) (*GFSResult, error) {
	// Load all backups
	backups, err := metadata.ListBackups(backupDir)
	if err != nil {
		return nil, err
	}

	return ApplyGFSPolicyToBackups(backups, policy)
}

// ApplyGFSPolicyToBackups applies GFS policy to a list of backups
func ApplyGFSPolicyToBackups(backups []*metadata.BackupMetadata, policy GFSPolicy) (*GFSResult, error) {
	result := &GFSResult{
		TotalBackups: len(backups),
		ToDelete:     make([]*metadata.BackupMetadata, 0),
		ToKeep:       make([]*metadata.BackupMetadata, 0),
		Errors:       make([]error, 0),
	}

	if len(backups) == 0 {
		return result, nil
	}

	// Sort backups by timestamp (newest first)
	sort.Slice(backups, func(i, j int) bool {
		return backups[i].Timestamp.After(backups[j].Timestamp)
	})

	// Classify all backups
	classifications := classifyBackups(backups, policy)

	// Select best backup for each tier
	dailySelected := selectBestForTier(classifications, TierDaily, policy.Daily)
	weeklySelected := selectBestForTier(classifications, TierWeekly, policy.Weekly)
	monthlySelected := selectBestForTier(classifications, TierMonthly, policy.Monthly)
	yearlySelected := selectBestForTier(classifications, TierYearly, policy.Yearly)

	// Merge all selected backups
	keepSet := make(map[string]bool)
	for _, b := range dailySelected {
		keepSet[b.BackupFile] = true
		result.DailyKept++
	}
	for _, b := range weeklySelected {
		if !keepSet[b.BackupFile] {
			keepSet[b.BackupFile] = true
			result.WeeklyKept++
		}
	}
	for _, b := range monthlySelected {
		if !keepSet[b.BackupFile] {
			keepSet[b.BackupFile] = true
			result.MonthlyKept++
		}
	}
	for _, b := range yearlySelected {
		if !keepSet[b.BackupFile] {
			keepSet[b.BackupFile] = true
			result.YearlyKept++
		}
	}

	// Categorize backups into keep/delete
	for _, backup := range backups {
		if keepSet[backup.BackupFile] {
			result.ToKeep = append(result.ToKeep, backup)
			result.Kept = append(result.Kept, backup.BackupFile)
		} else {
			result.ToDelete = append(result.ToDelete, backup)
			result.Deleted = append(result.Deleted, backup.BackupFile)
			result.SpaceFreed += backup.SizeBytes
		}
	}

	// Set total kept count
	result.TotalKept = len(result.ToKeep)

	// Execute deletions if not dry run
	if !policy.DryRun {
		for _, backup := range result.ToDelete {
			if err := deleteBackup(backup.BackupFile); err != nil {
				result.Errors = append(result.Errors, err)
			}
		}
	}

	return result, nil
}

// classifyBackups classifies each backup into tiers
func classifyBackups(backups []*metadata.BackupMetadata, policy GFSPolicy) []BackupClassification {
	classifications := make([]BackupClassification, len(backups))

	for i, backup := range backups {
		ts := backup.Timestamp.UTC()

		classifications[i] = BackupClassification{
			Backup:   backup,
			Tiers:    make([]Tier, 0),
			DayKey:   ts.Format("2006-01-02"),
			WeekKey:  getWeekKey(ts),
			MonthKey: ts.Format("2006-01"),
			YearKey:  ts.Format("2006"),
		}

		// Every backup qualifies for daily
		classifications[i].Tiers = append(classifications[i].Tiers, TierDaily)

		// Check if qualifies for weekly (correct day of week)
		if int(ts.Weekday()) == policy.WeeklyDay {
			classifications[i].Tiers = append(classifications[i].Tiers, TierWeekly)
		}

		// Check if qualifies for monthly (correct day of month)
		if isMonthlyQualified(ts, policy.MonthlyDay) {
			classifications[i].Tiers = append(classifications[i].Tiers, TierMonthly)
		}

		// Check if qualifies for yearly (January + monthly day)
		if ts.Month() == time.January && isMonthlyQualified(ts, policy.MonthlyDay) {
			classifications[i].Tiers = append(classifications[i].Tiers, TierYearly)
		}
	}

	return classifications
}

// selectBestForTier selects the best N backups for a tier
func selectBestForTier(classifications []BackupClassification, tier Tier, count int) []*metadata.BackupMetadata {
	if count <= 0 {
		return nil
	}

	// Group by tier key
	groups := make(map[string][]*metadata.BackupMetadata)

	for _, c := range classifications {
		if !hasTier(c.Tiers, tier) {
			continue
		}

		var key string
		switch tier {
		case TierDaily:
			key = c.DayKey
		case TierWeekly:
			key = c.WeekKey
		case TierMonthly:
			key = c.MonthKey
		case TierYearly:
			key = c.YearKey
		}

		groups[key] = append(groups[key], c.Backup)
	}

	// Get unique keys sorted by recency (newest first)
	keys := make([]string, 0, len(groups))
	for key := range groups {
		keys = append(keys, key)
	}
	sort.Slice(keys, func(i, j int) bool {
		return keys[i] > keys[j] // Reverse sort (newest first)
	})

	// Limit to requested count
	if len(keys) > count {
		keys = keys[:count]
	}

	// Select newest backup from each period
	result := make([]*metadata.BackupMetadata, 0, len(keys))
	for _, key := range keys {
		backups := groups[key]
		// Sort by timestamp, newest first
		sort.Slice(backups, func(i, j int) bool {
			return backups[i].Timestamp.After(backups[j].Timestamp)
		})
		result = append(result, backups[0])
	}

	return result
}

// getWeekKey returns ISO week key (YYYY-WNN)
func getWeekKey(t time.Time) string {
	year, week := t.ISOWeek()
	return t.Format("2006") + "-W" + padInt(week, 2) + "-" + padInt(year, 4)
}

// isMonthlyQualified checks if timestamp qualifies for monthly tier
func isMonthlyQualified(ts time.Time, monthlyDay int) bool {
	if monthlyDay == 0 {
		// Last day of month
		nextMonth := time.Date(ts.Year(), ts.Month()+1, 1, 0, 0, 0, 0, ts.Location())
		lastDay := nextMonth.AddDate(0, 0, -1).Day()
		return ts.Day() == lastDay
	}
	return ts.Day() == monthlyDay
}

// hasTier checks if tier list contains a specific tier
func hasTier(tiers []Tier, tier Tier) bool {
	for _, t := range tiers {
		if t == tier {
			return true
		}
	}
	return false
}

// padInt pads an integer with leading zeros
func padInt(n, width int) string {
	s := ""
	for i := 0; i < width; i++ {
		digit := byte('0' + n%10)
		s = string(digit) + s
		n /= 10
	}
	return s
}

// ClassifyBackup classifies a single backup into its tiers
func ClassifyBackup(ts time.Time, policy GFSPolicy) []Tier {
	tiers := make([]Tier, 0, 4)

	// Every backup qualifies for daily
	tiers = append(tiers, TierDaily)

	// Weekly: correct day of week
	if int(ts.Weekday()) == policy.WeeklyDay {
		tiers = append(tiers, TierWeekly)
	}

	// Monthly: correct day of month
	if isMonthlyQualified(ts, policy.MonthlyDay) {
		tiers = append(tiers, TierMonthly)
	}

	// Yearly: January + monthly day
	if ts.Month() == time.January && isMonthlyQualified(ts, policy.MonthlyDay) {
		tiers = append(tiers, TierYearly)
	}

	return tiers
}
