// Package catalog - SQLite storage implementation
package catalog

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	_ "modernc.org/sqlite" // Pure Go SQLite driver (no CGO required)
)

// SQLiteCatalog implements Catalog interface with SQLite storage
type SQLiteCatalog struct {
	db   *sql.DB
	path string
}

// NewSQLiteCatalog creates a new SQLite-backed catalog
func NewSQLiteCatalog(dbPath string) (*SQLiteCatalog, error) {
	// Ensure directory exists
	dir := filepath.Dir(dbPath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create catalog directory: %w", err)
	}

	// SQLite connection with performance optimizations:
	// - WAL mode: better concurrency (multiple readers + one writer)
	// - foreign_keys: enforce referential integrity
	// - busy_timeout: wait up to 5s for locks instead of failing immediately
	// - cache_size: 64MB cache for faster queries with large catalogs
	// - synchronous=NORMAL: good durability with better performance than FULL
	db, err := sql.Open("sqlite", dbPath+"?_journal_mode=WAL&_foreign_keys=ON&_busy_timeout=5000&_cache_size=-65536&_synchronous=NORMAL")
	if err != nil {
		return nil, fmt.Errorf("failed to open catalog database: %w", err)
	}

	// Configure connection pool for concurrent access
	db.SetMaxOpenConns(1) // SQLite only supports one writer
	db.SetMaxIdleConns(1)

	catalog := &SQLiteCatalog{
		db:   db,
		path: dbPath,
	}

	if err := catalog.initialize(); err != nil {
		db.Close()
		return nil, err
	}

	return catalog, nil
}

// DB returns the underlying *sql.DB for advanced operations (e.g., integrity checks)
func (c *SQLiteCatalog) DB() *sql.DB {
	return c.db
}

// initialize creates the database schema
func (c *SQLiteCatalog) initialize() error {
	schema := `
	CREATE TABLE IF NOT EXISTS backups (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		database TEXT NOT NULL,
		database_type TEXT NOT NULL,
		host TEXT,
		port INTEGER,
		backup_path TEXT NOT NULL UNIQUE,
		backup_type TEXT DEFAULT 'full',
		size_bytes INTEGER,
		sha256 TEXT,
		compression TEXT,
		encrypted INTEGER DEFAULT 0,
		created_at DATETIME NOT NULL,
		duration REAL,
		status TEXT DEFAULT 'completed',
		verified_at DATETIME,
		verify_valid INTEGER,
		drill_tested_at DATETIME,
		drill_success INTEGER,
		cloud_location TEXT,
		retention_policy TEXT,
		tags TEXT,
		metadata TEXT,
		updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
	);

	CREATE INDEX IF NOT EXISTS idx_backups_database ON backups(database);
	CREATE INDEX IF NOT EXISTS idx_backups_created_at ON backups(created_at);
	CREATE INDEX IF NOT EXISTS idx_backups_created_at_desc ON backups(created_at DESC);
	CREATE INDEX IF NOT EXISTS idx_backups_status ON backups(status);
	CREATE INDEX IF NOT EXISTS idx_backups_host ON backups(host);
	CREATE INDEX IF NOT EXISTS idx_backups_database_type ON backups(database_type);
	CREATE INDEX IF NOT EXISTS idx_backups_database_status ON backups(database, status);
	CREATE INDEX IF NOT EXISTS idx_backups_database_created ON backups(database, created_at DESC);

	CREATE TABLE IF NOT EXISTS catalog_meta (
		key TEXT PRIMARY KEY,
		value TEXT,
		updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
	);

	-- Store schema version for migrations
	INSERT OR IGNORE INTO catalog_meta (key, value) VALUES ('schema_version', '1');
	`

	_, err := c.db.Exec(schema)
	if err != nil {
		return fmt.Errorf("failed to initialize schema: %w", err)
	}

	return nil
}

// Add inserts a new backup entry
func (c *SQLiteCatalog) Add(ctx context.Context, entry *Entry) error {
	tagsJSON, _ := json.Marshal(entry.Tags)
	metaJSON, _ := json.Marshal(entry.Metadata)

	result, err := c.db.ExecContext(ctx, `
		INSERT INTO backups (
			database, database_type, host, port, backup_path, backup_type,
			size_bytes, sha256, compression, encrypted, created_at, duration,
			status, cloud_location, retention_policy, tags, metadata
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`,
		entry.Database, entry.DatabaseType, entry.Host, entry.Port,
		entry.BackupPath, entry.BackupType, entry.SizeBytes, entry.SHA256,
		entry.Compression, entry.Encrypted, entry.CreatedAt, entry.Duration,
		entry.Status, entry.CloudLocation, entry.RetentionPolicy,
		string(tagsJSON), string(metaJSON),
	)
	if err != nil {
		return fmt.Errorf("failed to add catalog entry: %w", err)
	}

	id, _ := result.LastInsertId()
	entry.ID = id
	return nil
}

// Update updates an existing backup entry
func (c *SQLiteCatalog) Update(ctx context.Context, entry *Entry) error {
	tagsJSON, _ := json.Marshal(entry.Tags)
	metaJSON, _ := json.Marshal(entry.Metadata)

	_, err := c.db.ExecContext(ctx, `
		UPDATE backups SET
			database = ?, database_type = ?, host = ?, port = ?,
			backup_type = ?, size_bytes = ?, sha256 = ?, compression = ?,
			encrypted = ?, duration = ?, status = ?, verified_at = ?,
			verify_valid = ?, drill_tested_at = ?, drill_success = ?,
			cloud_location = ?, retention_policy = ?, tags = ?, metadata = ?,
			updated_at = CURRENT_TIMESTAMP
		WHERE id = ?
	`,
		entry.Database, entry.DatabaseType, entry.Host, entry.Port,
		entry.BackupType, entry.SizeBytes, entry.SHA256, entry.Compression,
		entry.Encrypted, entry.Duration, entry.Status, entry.VerifiedAt,
		entry.VerifyValid, entry.DrillTestedAt, entry.DrillSuccess,
		entry.CloudLocation, entry.RetentionPolicy,
		string(tagsJSON), string(metaJSON), entry.ID,
	)
	if err != nil {
		return fmt.Errorf("failed to update catalog entry: %w", err)
	}
	return nil
}

// Delete removes a backup entry
func (c *SQLiteCatalog) Delete(ctx context.Context, id int64) error {
	_, err := c.db.ExecContext(ctx, "DELETE FROM backups WHERE id = ?", id)
	if err != nil {
		return fmt.Errorf("failed to delete catalog entry: %w", err)
	}
	return nil
}

// Get retrieves a backup entry by ID
func (c *SQLiteCatalog) Get(ctx context.Context, id int64) (*Entry, error) {
	row := c.db.QueryRowContext(ctx, `
		SELECT id, database, database_type, host, port, backup_path, backup_type,
			size_bytes, sha256, compression, encrypted, created_at, duration,
			status, verified_at, verify_valid, drill_tested_at, drill_success,
			cloud_location, retention_policy, tags, metadata
		FROM backups WHERE id = ?
	`, id)

	return c.scanEntry(row)
}

// GetByPath retrieves a backup entry by file path
func (c *SQLiteCatalog) GetByPath(ctx context.Context, path string) (*Entry, error) {
	row := c.db.QueryRowContext(ctx, `
		SELECT id, database, database_type, host, port, backup_path, backup_type,
			size_bytes, sha256, compression, encrypted, created_at, duration,
			status, verified_at, verify_valid, drill_tested_at, drill_success,
			cloud_location, retention_policy, tags, metadata
		FROM backups WHERE backup_path = ?
	`, path)

	return c.scanEntry(row)
}

// scanEntry scans a row into an Entry struct
func (c *SQLiteCatalog) scanEntry(row *sql.Row) (*Entry, error) {
	var entry Entry
	var tagsJSON, metaJSON sql.NullString
	var verifiedAt, drillTestedAt sql.NullTime
	var verifyValid, drillSuccess sql.NullBool

	err := row.Scan(
		&entry.ID, &entry.Database, &entry.DatabaseType, &entry.Host, &entry.Port,
		&entry.BackupPath, &entry.BackupType, &entry.SizeBytes, &entry.SHA256,
		&entry.Compression, &entry.Encrypted, &entry.CreatedAt, &entry.Duration,
		&entry.Status, &verifiedAt, &verifyValid, &drillTestedAt, &drillSuccess,
		&entry.CloudLocation, &entry.RetentionPolicy, &tagsJSON, &metaJSON,
	)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("failed to scan entry: %w", err)
	}

	if verifiedAt.Valid {
		entry.VerifiedAt = &verifiedAt.Time
	}
	if verifyValid.Valid {
		entry.VerifyValid = &verifyValid.Bool
	}
	if drillTestedAt.Valid {
		entry.DrillTestedAt = &drillTestedAt.Time
	}
	if drillSuccess.Valid {
		entry.DrillSuccess = &drillSuccess.Bool
	}

	if tagsJSON.Valid && tagsJSON.String != "" {
		json.Unmarshal([]byte(tagsJSON.String), &entry.Tags)
	}
	if metaJSON.Valid && metaJSON.String != "" {
		json.Unmarshal([]byte(metaJSON.String), &entry.Metadata)
	}

	return &entry, nil
}

// Search finds backup entries matching the query
func (c *SQLiteCatalog) Search(ctx context.Context, query *SearchQuery) ([]*Entry, error) {
	where, args := c.buildSearchQuery(query)

	orderBy := "created_at DESC"
	if query.OrderBy != "" {
		orderBy = query.OrderBy
		if query.OrderDesc {
			orderBy += " DESC"
		}
	}

	sql := fmt.Sprintf(`
		SELECT id, database, database_type, host, port, backup_path, backup_type,
			size_bytes, sha256, compression, encrypted, created_at, duration,
			status, verified_at, verify_valid, drill_tested_at, drill_success,
			cloud_location, retention_policy, tags, metadata
		FROM backups
		%s
		ORDER BY %s
	`, where, orderBy)

	if query.Limit > 0 {
		sql += fmt.Sprintf(" LIMIT %d", query.Limit)
		if query.Offset > 0 {
			sql += fmt.Sprintf(" OFFSET %d", query.Offset)
		}
	}

	rows, err := c.db.QueryContext(ctx, sql, args...)
	if err != nil {
		return nil, fmt.Errorf("search query failed: %w", err)
	}
	defer rows.Close()

	return c.scanEntries(rows)
}

// scanEntries scans multiple rows into Entry slices
func (c *SQLiteCatalog) scanEntries(rows *sql.Rows) ([]*Entry, error) {
	var entries []*Entry

	for rows.Next() {
		var entry Entry
		var tagsJSON, metaJSON sql.NullString
		var verifiedAt, drillTestedAt sql.NullTime
		var verifyValid, drillSuccess sql.NullBool

		err := rows.Scan(
			&entry.ID, &entry.Database, &entry.DatabaseType, &entry.Host, &entry.Port,
			&entry.BackupPath, &entry.BackupType, &entry.SizeBytes, &entry.SHA256,
			&entry.Compression, &entry.Encrypted, &entry.CreatedAt, &entry.Duration,
			&entry.Status, &verifiedAt, &verifyValid, &drillTestedAt, &drillSuccess,
			&entry.CloudLocation, &entry.RetentionPolicy, &tagsJSON, &metaJSON,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to scan row: %w", err)
		}

		if verifiedAt.Valid {
			entry.VerifiedAt = &verifiedAt.Time
		}
		if verifyValid.Valid {
			entry.VerifyValid = &verifyValid.Bool
		}
		if drillTestedAt.Valid {
			entry.DrillTestedAt = &drillTestedAt.Time
		}
		if drillSuccess.Valid {
			entry.DrillSuccess = &drillSuccess.Bool
		}

		if tagsJSON.Valid && tagsJSON.String != "" {
			json.Unmarshal([]byte(tagsJSON.String), &entry.Tags)
		}
		if metaJSON.Valid && metaJSON.String != "" {
			json.Unmarshal([]byte(metaJSON.String), &entry.Metadata)
		}

		entries = append(entries, &entry)
	}

	return entries, rows.Err()
}

// buildSearchQuery builds the WHERE clause from a SearchQuery
func (c *SQLiteCatalog) buildSearchQuery(query *SearchQuery) (string, []interface{}) {
	var conditions []string
	var args []interface{}

	if query.Database != "" {
		if strings.Contains(query.Database, "*") {
			conditions = append(conditions, "database LIKE ?")
			args = append(args, strings.ReplaceAll(query.Database, "*", "%"))
		} else {
			conditions = append(conditions, "database = ?")
			args = append(args, query.Database)
		}
	}

	if query.DatabaseType != "" {
		conditions = append(conditions, "database_type = ?")
		args = append(args, query.DatabaseType)
	}

	if query.Host != "" {
		conditions = append(conditions, "host = ?")
		args = append(args, query.Host)
	}

	if query.Status != "" {
		conditions = append(conditions, "status = ?")
		args = append(args, query.Status)
	}

	if query.StartDate != nil {
		conditions = append(conditions, "created_at >= ?")
		args = append(args, *query.StartDate)
	}

	if query.EndDate != nil {
		conditions = append(conditions, "created_at <= ?")
		args = append(args, *query.EndDate)
	}

	if query.MinSize > 0 {
		conditions = append(conditions, "size_bytes >= ?")
		args = append(args, query.MinSize)
	}

	if query.MaxSize > 0 {
		conditions = append(conditions, "size_bytes <= ?")
		args = append(args, query.MaxSize)
	}

	if query.BackupType != "" {
		conditions = append(conditions, "backup_type = ?")
		args = append(args, query.BackupType)
	}

	if query.Encrypted != nil {
		conditions = append(conditions, "encrypted = ?")
		args = append(args, *query.Encrypted)
	}

	if query.Verified != nil {
		if *query.Verified {
			conditions = append(conditions, "verified_at IS NOT NULL AND verify_valid = 1")
		} else {
			conditions = append(conditions, "verified_at IS NULL")
		}
	}

	if query.DrillTested != nil {
		if *query.DrillTested {
			conditions = append(conditions, "drill_tested_at IS NOT NULL AND drill_success = 1")
		} else {
			conditions = append(conditions, "drill_tested_at IS NULL")
		}
	}

	if len(conditions) == 0 {
		return "", nil
	}

	return "WHERE " + strings.Join(conditions, " AND "), args
}

// List returns recent backups for a database
func (c *SQLiteCatalog) List(ctx context.Context, database string, limit int) ([]*Entry, error) {
	query := &SearchQuery{
		Database:  database,
		Limit:     limit,
		OrderBy:   "created_at",
		OrderDesc: true,
	}
	return c.Search(ctx, query)
}

// ListDatabases returns all unique database names
func (c *SQLiteCatalog) ListDatabases(ctx context.Context) ([]string, error) {
	rows, err := c.db.QueryContext(ctx, "SELECT DISTINCT database FROM backups ORDER BY database")
	if err != nil {
		return nil, fmt.Errorf("failed to list databases: %w", err)
	}
	defer rows.Close()

	var databases []string
	for rows.Next() {
		var db string
		if err := rows.Scan(&db); err != nil {
			return nil, err
		}
		databases = append(databases, db)
	}

	return databases, rows.Err()
}

// Count returns the number of entries matching the query
func (c *SQLiteCatalog) Count(ctx context.Context, query *SearchQuery) (int64, error) {
	where, args := c.buildSearchQuery(query)

	sql := "SELECT COUNT(*) FROM backups " + where

	var count int64
	err := c.db.QueryRowContext(ctx, sql, args...).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("count query failed: %w", err)
	}

	return count, nil
}

// Stats returns overall catalog statistics
func (c *SQLiteCatalog) Stats(ctx context.Context) (*Stats, error) {
	stats := &Stats{
		ByDatabase: make(map[string]int64),
		ByType:     make(map[string]int64),
		ByStatus:   make(map[string]int64),
	}

	// Basic stats
	row := c.db.QueryRowContext(ctx, `
		SELECT 
			COUNT(*),
			COALESCE(SUM(size_bytes), 0),
			MIN(created_at),
			MAX(created_at),
			COALESCE(AVG(duration), 0),
			CAST(COALESCE(AVG(size_bytes), 0) AS INTEGER),
			COALESCE(SUM(CASE WHEN verified_at IS NOT NULL THEN 1 ELSE 0 END), 0),
			COALESCE(SUM(CASE WHEN drill_tested_at IS NOT NULL THEN 1 ELSE 0 END), 0)
		FROM backups WHERE status != 'deleted'
	`)

	var oldest, newest sql.NullString
	err := row.Scan(
		&stats.TotalBackups, &stats.TotalSize, &oldest, &newest,
		&stats.AvgDuration, &stats.AvgSize,
		&stats.VerifiedCount, &stats.DrillTestedCount,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to get stats: %w", err)
	}

	if oldest.Valid {
		if t, err := time.Parse(time.RFC3339Nano, oldest.String); err == nil {
			stats.OldestBackup = &t
		} else if t, err := time.Parse("2006-01-02 15:04:05.999999999-07:00", oldest.String); err == nil {
			stats.OldestBackup = &t
		} else if t, err := time.Parse("2006-01-02T15:04:05Z", oldest.String); err == nil {
			stats.OldestBackup = &t
		}
	}
	if newest.Valid {
		if t, err := time.Parse(time.RFC3339Nano, newest.String); err == nil {
			stats.NewestBackup = &t
		} else if t, err := time.Parse("2006-01-02 15:04:05.999999999-07:00", newest.String); err == nil {
			stats.NewestBackup = &t
		} else if t, err := time.Parse("2006-01-02T15:04:05Z", newest.String); err == nil {
			stats.NewestBackup = &t
		}
	}
	stats.TotalSizeHuman = FormatSize(stats.TotalSize)

	// By database
	rows, _ := c.db.QueryContext(ctx, "SELECT database, COUNT(*) FROM backups GROUP BY database")
	defer rows.Close()
	for rows.Next() {
		var db string
		var count int64
		rows.Scan(&db, &count)
		stats.ByDatabase[db] = count
	}

	// By type
	rows, _ = c.db.QueryContext(ctx, "SELECT backup_type, COUNT(*) FROM backups GROUP BY backup_type")
	defer rows.Close()
	for rows.Next() {
		var t string
		var count int64
		rows.Scan(&t, &count)
		stats.ByType[t] = count
	}

	// By status
	rows, _ = c.db.QueryContext(ctx, "SELECT status, COUNT(*) FROM backups GROUP BY status")
	defer rows.Close()
	for rows.Next() {
		var s string
		var count int64
		rows.Scan(&s, &count)
		stats.ByStatus[s] = count
	}

	return stats, nil
}

// StatsByDatabase returns statistics for a specific database
func (c *SQLiteCatalog) StatsByDatabase(ctx context.Context, database string) (*Stats, error) {
	stats := &Stats{
		ByDatabase: make(map[string]int64),
		ByType:     make(map[string]int64),
		ByStatus:   make(map[string]int64),
	}

	row := c.db.QueryRowContext(ctx, `
		SELECT 
			COUNT(*),
			COALESCE(SUM(size_bytes), 0),
			MIN(created_at),
			MAX(created_at),
			COALESCE(AVG(duration), 0),
			COALESCE(AVG(size_bytes), 0),
			COALESCE(SUM(CASE WHEN verified_at IS NOT NULL THEN 1 ELSE 0 END), 0),
			COALESCE(SUM(CASE WHEN drill_tested_at IS NOT NULL THEN 1 ELSE 0 END), 0)
		FROM backups WHERE database = ? AND status != 'deleted'
	`, database)

	var oldest, newest sql.NullTime
	err := row.Scan(
		&stats.TotalBackups, &stats.TotalSize, &oldest, &newest,
		&stats.AvgDuration, &stats.AvgSize,
		&stats.VerifiedCount, &stats.DrillTestedCount,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to get database stats: %w", err)
	}

	if oldest.Valid {
		stats.OldestBackup = &oldest.Time
	}
	if newest.Valid {
		stats.NewestBackup = &newest.Time
	}
	stats.TotalSizeHuman = FormatSize(stats.TotalSize)

	return stats, nil
}

// MarkVerified updates the verification status of a backup
func (c *SQLiteCatalog) MarkVerified(ctx context.Context, id int64, valid bool) error {
	status := StatusVerified
	if !valid {
		status = StatusCorrupted
	}

	_, err := c.db.ExecContext(ctx, `
		UPDATE backups SET 
			verified_at = CURRENT_TIMESTAMP,
			verify_valid = ?,
			status = ?,
			updated_at = CURRENT_TIMESTAMP
		WHERE id = ?
	`, valid, status, id)
	if err != nil {
		return fmt.Errorf("mark verified failed for backup %d: %w", id, err)
	}
	return nil
}

// MarkDrillTested updates the drill test status of a backup
func (c *SQLiteCatalog) MarkDrillTested(ctx context.Context, id int64, success bool) error {
	_, err := c.db.ExecContext(ctx, `
		UPDATE backups SET 
			drill_tested_at = CURRENT_TIMESTAMP,
			drill_success = ?,
			updated_at = CURRENT_TIMESTAMP
		WHERE id = ?
	`, success, id)
	if err != nil {
		return fmt.Errorf("mark drill tested failed for backup %d: %w", id, err)
	}
	return nil
}

// Prune removes entries older than the given time
func (c *SQLiteCatalog) Prune(ctx context.Context, before time.Time) (int, error) {
	result, err := c.db.ExecContext(ctx,
		"DELETE FROM backups WHERE created_at < ? AND status = 'deleted'",
		before,
	)
	if err != nil {
		return 0, fmt.Errorf("prune failed: %w", err)
	}

	affected, _ := result.RowsAffected()
	return int(affected), nil
}

// Vacuum optimizes the database
func (c *SQLiteCatalog) Vacuum(ctx context.Context) error {
	_, err := c.db.ExecContext(ctx, "VACUUM")
	if err != nil {
		return fmt.Errorf("vacuum catalog database failed: %w", err)
	}
	return nil
}

// Close closes the database connection
func (c *SQLiteCatalog) Close() error {
	if err := c.db.Close(); err != nil {
		return fmt.Errorf("close catalog database failed: %w", err)
	}
	return nil
}
