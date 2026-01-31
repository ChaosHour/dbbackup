package catalog

import (
	"context"
	"fmt"
	"os"
	"time"
)

// PruneConfig defines criteria for pruning catalog entries
type PruneConfig struct {
	CheckMissing bool       // Remove entries for missing backup files
	OlderThan    *time.Time // Remove entries older than this time
	Status       string     // Remove entries with specific status
	Database     string     // Only prune entries for this database
	DryRun       bool       // Preview without actually deleting
}

// PruneResult contains the results of a prune operation
type PruneResult struct {
	TotalChecked int      // Total entries checked
	Removed      int      // Number of entries removed
	SpaceFreed   int64    // Estimated disk space freed (bytes)
	Duration     float64  // Operation duration in seconds
	Details      []string // Details of removed entries
}

// PruneAdvanced removes catalog entries matching the specified criteria
func (c *SQLiteCatalog) PruneAdvanced(ctx context.Context, config *PruneConfig) (*PruneResult, error) {
	startTime := time.Now()

	result := &PruneResult{
		Details: []string{},
	}

	// Build query to find matching entries
	query := "SELECT id, database, backup_path, size_bytes, created_at, status FROM backups WHERE 1=1"
	args := []interface{}{}

	if config.Database != "" {
		query += " AND database = ?"
		args = append(args, config.Database)
	}

	if config.Status != "" {
		query += " AND status = ?"
		args = append(args, config.Status)
	}

	if config.OlderThan != nil {
		query += " AND created_at < ?"
		args = append(args, config.OlderThan.Unix())
	}

	query += " ORDER BY created_at ASC"

	rows, err := c.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("query failed: %w", err)
	}
	defer rows.Close()

	idsToRemove := []int64{}
	spaceToFree := int64(0)

	for rows.Next() {
		var id int64
		var database, backupPath, status string
		var sizeBytes int64
		var createdAt int64

		if err := rows.Scan(&id, &database, &backupPath, &sizeBytes, &createdAt, &status); err != nil {
			return nil, fmt.Errorf("scan failed: %w", err)
		}

		result.TotalChecked++

		shouldRemove := false
		reason := ""

		// Check if file is missing (if requested)
		if config.CheckMissing {
			if _, err := os.Stat(backupPath); os.IsNotExist(err) {
				shouldRemove = true
				reason = "missing file"
			}
		}

		// Check if older than cutoff (already filtered in query, but double-check)
		if config.OlderThan != nil && time.Unix(createdAt, 0).Before(*config.OlderThan) {
			if !shouldRemove {
				shouldRemove = true
				reason = fmt.Sprintf("older than %s", config.OlderThan.Format("2006-01-02"))
			}
		}

		// Check status (already filtered in query)
		if config.Status != "" && status == config.Status {
			if !shouldRemove {
				shouldRemove = true
				reason = fmt.Sprintf("status: %s", status)
			}
		}

		if shouldRemove {
			idsToRemove = append(idsToRemove, id)
			spaceToFree += sizeBytes
			createdTime := time.Unix(createdAt, 0)
			detail := fmt.Sprintf("%s - %s (created %s) - %s",
				database,
				backupPath,
				createdTime.Format("2006-01-02"),
				reason)
			result.Details = append(result.Details, detail)
		}
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("row iteration failed: %w", err)
	}

	// Actually delete entries if not dry run
	if !config.DryRun && len(idsToRemove) > 0 {
		// Use transaction for safety
		tx, err := c.db.BeginTx(ctx, nil)
		if err != nil {
			return nil, fmt.Errorf("begin transaction failed: %w", err)
		}
		defer tx.Rollback()

		stmt, err := tx.PrepareContext(ctx, "DELETE FROM backups WHERE id = ?")
		if err != nil {
			return nil, fmt.Errorf("prepare delete statement failed: %w", err)
		}
		defer stmt.Close()

		for _, id := range idsToRemove {
			if _, err := stmt.ExecContext(ctx, id); err != nil {
				return nil, fmt.Errorf("delete failed for id %d: %w", id, err)
			}
		}

		if err := tx.Commit(); err != nil {
			return nil, fmt.Errorf("commit transaction failed: %w", err)
		}
	}

	result.Removed = len(idsToRemove)
	result.SpaceFreed = spaceToFree
	result.Duration = time.Since(startTime).Seconds()

	return result, nil
}
