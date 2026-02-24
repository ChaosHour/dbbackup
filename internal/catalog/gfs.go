// Package catalog provides backup catalog management with SQLite storage
package catalog

import (
	"context"
	"fmt"
	"os"
	"sort"
	"time"
)

// GFSPolicy defines a Grandfather-Father-Son retention policy
type GFSPolicy struct {
	KeepDaily   int    // Number of daily backups to keep (newest N)
	KeepWeekly  int    // Number of weekly backups to keep (first per ISO week)
	KeepMonthly int    // Number of monthly backups to keep (first per month)
	KeepYearly  int    // Number of yearly backups to keep (first per year)
	DryRun      bool   // Preview without deleting
	DeleteFiles bool   // Also delete backup files from disk
	Database    string // Filter by specific database (empty = all)
}

// GFSResult contains the results of a GFS prune operation
type GFSResult struct {
	TotalBackups int            `json:"total_backups"`
	Kept         int            `json:"kept"`
	Removed      int            `json:"removed"`
	SpaceFreed   int64          `json:"space_freed_bytes"`
	Duration     float64        `json:"duration_seconds"`
	KeptByTier   map[string]int `json:"kept_by_tier"` // daily/weekly/monthly/yearly
	ToDelete     []GFSCandidate `json:"to_delete"`
	ToKeep       []GFSCandidate `json:"to_keep"`
	FilesDeleted int            `json:"files_deleted"`
	FileErrors   []string       `json:"file_errors,omitempty"`
}

// GFSCandidate represents a backup candidate for GFS evaluation
type GFSCandidate struct {
	ID       int64     `json:"id"`
	Database string    `json:"database"`
	Path     string    `json:"path"`
	Size     int64     `json:"size_bytes"`
	Created  time.Time `json:"created_at"`
	Tier     string    `json:"tier"` // daily, weekly, monthly, yearly, or "" (to delete)
	Age      string    `json:"age"`
}

// PruneByGFS evaluates and optionally removes backups according to a GFS retention policy.
// It keeps:
//   - The newest N daily backups
//   - The oldest backup per ISO week (for the most recent N weeks with backups)
//   - The oldest backup per calendar month (for the most recent N months with backups)
//   - The oldest backup per calendar year (for the most recent N years with backups)
//
// All remaining completed backups are marked for deletion.
func (c *SQLiteCatalog) PruneByGFS(ctx context.Context, policy *GFSPolicy) (*GFSResult, error) {
	startTime := time.Now()

	result := &GFSResult{
		KeptByTier: map[string]int{
			"daily":   0,
			"weekly":  0,
			"monthly": 0,
			"yearly":  0,
		},
	}

	// Fetch all completed backups, sorted newest first
	allBackups, err := c.listCompletedBackups(ctx, policy.Database)
	if err != nil {
		return nil, fmt.Errorf("failed to list backups: %w", err)
	}

	if len(allBackups) == 0 {
		result.Duration = time.Since(startTime).Seconds()
		return result, nil
	}

	result.TotalBackups = len(allBackups)

	// Determine which backups to keep
	keep := classifyGFS(allBackups, policy)

	// Build result lists
	for _, b := range allBackups {
		tier := keep[b.ID]
		age := formatAge(time.Since(b.CreatedAt))
		candidate := GFSCandidate{
			ID:       b.ID,
			Database: b.Database,
			Path:     b.BackupPath,
			Size:     b.SizeBytes,
			Created:  b.CreatedAt,
			Tier:     tier,
			Age:      age,
		}

		if tier != "" {
			result.ToKeep = append(result.ToKeep, candidate)
			result.KeptByTier[tier]++
		} else {
			result.ToDelete = append(result.ToDelete, candidate)
			result.SpaceFreed += b.SizeBytes
		}
	}

	result.Kept = len(result.ToKeep)
	result.Removed = len(result.ToDelete)

	// Actually delete if not dry-run
	if !policy.DryRun && len(result.ToDelete) > 0 {
		if err := c.deleteEntries(ctx, result.ToDelete); err != nil {
			return result, fmt.Errorf("failed to delete catalog entries: %w", err)
		}

		// Delete files from disk if requested
		if policy.DeleteFiles {
			for _, d := range result.ToDelete {
				if d.Path == "" {
					continue
				}
				if err := os.Remove(d.Path); err != nil {
					if !os.IsNotExist(err) {
						result.FileErrors = append(result.FileErrors, fmt.Sprintf("%s: %v", d.Path, err))
					}
				} else {
					result.FilesDeleted++
				}
				// Also remove .meta.json and .sha256 sidecar files
				_ = os.Remove(d.Path + ".meta.json")
				_ = os.Remove(d.Path + ".sha256")
			}
		}
	}

	result.Duration = time.Since(startTime).Seconds()
	return result, nil
}

// classifyGFS assigns each backup to a GFS tier (daily/weekly/monthly/yearly)
// or marks it for deletion (empty string). A backup is assigned to the highest
// tier it qualifies for, and each slot can only be used once.
func classifyGFS(backups []*Entry, policy *GFSPolicy) map[int64]string {
	keep := make(map[int64]string)

	// Sort newest first (should already be, but ensure)
	sorted := make([]*Entry, len(backups))
	copy(sorted, backups)
	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i].CreatedAt.After(sorted[j].CreatedAt)
	})

	// === Daily: keep the most recent N backups ===
	dailyKept := 0
	for _, b := range sorted {
		if dailyKept >= policy.KeepDaily {
			break
		}
		if keep[b.ID] == "" { // not already assigned
			keep[b.ID] = "daily"
			dailyKept++
		}
	}

	// === Weekly: keep the oldest backup per ISO week (most recent N weeks) ===
	// Traverse newest→oldest, group by ISO week, then pick the OLDEST per week
	weekMap := make(map[string]*Entry)
	var weekOrder []string
	for _, b := range sorted {
		year, week := b.CreatedAt.ISOWeek()
		wk := fmt.Sprintf("%04d-W%02d", year, week)
		// Keep the oldest entry in each week (we're iterating newest→oldest,
		// so always overwrite to get the oldest)
		if _, seen := weekMap[wk]; !seen {
			weekOrder = append(weekOrder, wk)
		}
		weekMap[wk] = b
	}
	// weekOrder is newest-week first; take the first N
	weeklyKept := 0
	for _, wk := range weekOrder {
		if weeklyKept >= policy.KeepWeekly {
			break
		}
		b := weekMap[wk]
		if keep[b.ID] == "" {
			keep[b.ID] = "weekly"
		}
		weeklyKept++
	}

	// === Monthly: oldest backup per calendar month (most recent N months) ===
	monthMap := make(map[string]*Entry)
	var monthOrder []string
	for _, b := range sorted {
		mk := b.CreatedAt.Format("2006-01")
		if _, seen := monthMap[mk]; !seen {
			monthOrder = append(monthOrder, mk)
		}
		monthMap[mk] = b // overwrite → oldest
	}
	monthlyKept := 0
	for _, mk := range monthOrder {
		if monthlyKept >= policy.KeepMonthly {
			break
		}
		b := monthMap[mk]
		if keep[b.ID] == "" {
			keep[b.ID] = "monthly"
		}
		monthlyKept++
	}

	// === Yearly: oldest backup per calendar year (most recent N years) ===
	yearMap := make(map[string]*Entry)
	var yearOrder []string
	for _, b := range sorted {
		yk := b.CreatedAt.Format("2006")
		if _, seen := yearMap[yk]; !seen {
			yearOrder = append(yearOrder, yk)
		}
		yearMap[yk] = b
	}
	yearlyKept := 0
	for _, yk := range yearOrder {
		if yearlyKept >= policy.KeepYearly {
			break
		}
		b := yearMap[yk]
		if keep[b.ID] == "" {
			keep[b.ID] = "yearly"
		}
		yearlyKept++
	}

	return keep
}

// listCompletedBackups fetches all completed (non-deleted, non-failed) backups
// sorted by creation time descending
func (c *SQLiteCatalog) listCompletedBackups(ctx context.Context, database string) ([]*Entry, error) {
	query := `SELECT id, database, backup_path, backup_type, size_bytes, created_at, status
		FROM backups
		WHERE status IN ('completed', 'verified')`
	args := []interface{}{}

	if database != "" {
		query += " AND database = ?"
		args = append(args, database)
	}

	query += " ORDER BY created_at DESC"

	rows, err := c.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	var entries []*Entry
	for rows.Next() {
		var e Entry
		var createdAtStr string
		var status string
		if err := rows.Scan(&e.ID, &e.Database, &e.BackupPath, &e.BackupType, &e.SizeBytes, &createdAtStr, &status); err != nil {
			return nil, err
		}
		e.CreatedAt = parseTimeString(createdAtStr)
		e.Status = BackupStatus(status)
		entries = append(entries, &e)
	}
	return entries, rows.Err()
}

// deleteEntries removes catalog entries by ID in a transaction
func (c *SQLiteCatalog) deleteEntries(ctx context.Context, candidates []GFSCandidate) error {
	tx, err := c.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin transaction: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	stmt, err := tx.PrepareContext(ctx, "DELETE FROM backups WHERE id = ?")
	if err != nil {
		return fmt.Errorf("prepare statement: %w", err)
	}
	defer func() { _ = stmt.Close() }()

	for _, d := range candidates {
		if _, err := stmt.ExecContext(ctx, d.ID); err != nil {
			return fmt.Errorf("delete id %d: %w", d.ID, err)
		}
	}

	return tx.Commit()
}

// formatAge returns a human-readable age string
func formatAge(d time.Duration) string {
	switch {
	case d < time.Hour:
		return fmt.Sprintf("%dm", int(d.Minutes()))
	case d < 24*time.Hour:
		return fmt.Sprintf("%dh", int(d.Hours()))
	case d < 30*24*time.Hour:
		return fmt.Sprintf("%dd", int(d.Hours()/24))
	case d < 365*24*time.Hour:
		return fmt.Sprintf("%dmo", int(d.Hours()/(24*30)))
	default:
		return fmt.Sprintf("%dy", int(d.Hours()/(24*365)))
	}
}
