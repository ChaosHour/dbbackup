// Package drill - Validation logic for DR drills
package drill

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql"
	_ "github.com/jackc/pgx/v5/stdlib"
)

// Validator handles database validation during DR drills
type Validator struct {
	db      *sql.DB
	dbType  string
	verbose bool
}

// NewValidator creates a new database validator
func NewValidator(dbType string, host string, port int, user, password, dbname string, verbose bool) (*Validator, error) {
	var dsn string
	var driver string

	switch dbType {
	case "postgresql", "postgres":
		driver = "pgx"
		dsn = fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable",
			host, port, user, password, dbname)
	case "mysql":
		driver = "mysql"
		dsn = fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?parseTime=true",
			user, password, host, port, dbname)
	case "mariadb":
		driver = "mysql"
		dsn = fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?parseTime=true",
			user, password, host, port, dbname)
	default:
		return nil, fmt.Errorf("unsupported database type: %s", dbType)
	}

	db, err := sql.Open(driver, dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to database: %w", err)
	}

	// Test connection
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("failed to ping database: %w", err)
	}

	return &Validator{
		db:      db,
		dbType:  dbType,
		verbose: verbose,
	}, nil
}

// Close closes the database connection
func (v *Validator) Close() error {
	return v.db.Close()
}

// RunValidationQueries executes validation queries and returns results
func (v *Validator) RunValidationQueries(ctx context.Context, queries []ValidationQuery) []ValidationResult {
	var results []ValidationResult

	for _, q := range queries {
		result := v.runQuery(ctx, q)
		results = append(results, result)
	}

	return results
}

// runQuery executes a single validation query
func (v *Validator) runQuery(ctx context.Context, query ValidationQuery) ValidationResult {
	result := ValidationResult{
		Name:     query.Name,
		Query:    query.Query,
		Expected: query.ExpectedValue,
	}

	start := time.Now()
	rows, err := v.db.QueryContext(ctx, query.Query)
	result.Duration = float64(time.Since(start).Milliseconds())

	if err != nil {
		result.Success = false
		result.Error = err.Error()
		return result
	}
	defer func() { _ = rows.Close() }()

	// Get result
	if rows.Next() {
		var value interface{}
		if err := rows.Scan(&value); err != nil {
			result.Success = false
			result.Error = fmt.Sprintf("scan error: %v", err)
			return result
		}
		result.Result = fmt.Sprintf("%v", value)
	}

	// Validate result
	result.Success = true
	if query.ExpectedValue != "" && result.Result != query.ExpectedValue {
		result.Success = false
		result.Error = fmt.Sprintf("expected %s, got %s", query.ExpectedValue, result.Result)
	}

	// Check min/max if specified
	if query.MinValue > 0 || query.MaxValue > 0 {
		var numValue int64
		_, _ = fmt.Sscanf(result.Result, "%d", &numValue)

		if query.MinValue > 0 && numValue < query.MinValue {
			result.Success = false
			result.Error = fmt.Sprintf("value %d below minimum %d", numValue, query.MinValue)
		}
		if query.MaxValue > 0 && numValue > query.MaxValue {
			result.Success = false
			result.Error = fmt.Sprintf("value %d above maximum %d", numValue, query.MaxValue)
		}
	}

	return result
}

// RunCustomChecks executes custom validation checks
func (v *Validator) RunCustomChecks(ctx context.Context, checks []CustomCheck) []CheckResult {
	var results []CheckResult

	for _, check := range checks {
		result := v.runCheck(ctx, check)
		results = append(results, result)
	}

	return results
}

// runCheck executes a single custom check
func (v *Validator) runCheck(ctx context.Context, check CustomCheck) CheckResult {
	result := CheckResult{
		Name:     check.Name,
		Type:     check.Type,
		Expected: check.MinValue,
	}

	switch check.Type {
	case "row_count":
		count, err := v.getRowCount(ctx, check.Table, check.Condition)
		if err != nil {
			result.Success = false
			result.Message = fmt.Sprintf("failed to get row count: %v", err)
			return result
		}
		result.Actual = count
		result.Success = count >= check.MinValue
		if result.Success {
			result.Message = fmt.Sprintf("Table %s has %d rows (min: %d)", check.Table, count, check.MinValue)
		} else {
			result.Message = fmt.Sprintf("Table %s has %d rows, expected at least %d", check.Table, count, check.MinValue)
		}

	case "table_exists":
		exists, err := v.tableExists(ctx, check.Table)
		if err != nil {
			result.Success = false
			result.Message = fmt.Sprintf("failed to check table: %v", err)
			return result
		}
		result.Success = exists
		if exists {
			result.Actual = 1
			result.Message = fmt.Sprintf("Table %s exists", check.Table)
		} else {
			result.Actual = 0
			result.Message = fmt.Sprintf("Table %s does not exist", check.Table)
		}

	case "column_check":
		exists, err := v.columnExists(ctx, check.Table, check.Column)
		if err != nil {
			result.Success = false
			result.Message = fmt.Sprintf("failed to check column: %v", err)
			return result
		}
		result.Success = exists
		if exists {
			result.Actual = 1
			result.Message = fmt.Sprintf("Column %s.%s exists", check.Table, check.Column)
		} else {
			result.Actual = 0
			result.Message = fmt.Sprintf("Column %s.%s does not exist", check.Table, check.Column)
		}

	default:
		result.Success = false
		result.Message = fmt.Sprintf("unknown check type: %s", check.Type)
	}

	return result
}

// getRowCount returns the row count for a table
func (v *Validator) getRowCount(ctx context.Context, table, condition string) (int64, error) {
	query := fmt.Sprintf("SELECT COUNT(*) FROM %s", v.quoteIdentifier(table))
	if condition != "" {
		query += " WHERE " + condition
	}

	var count int64
	err := v.db.QueryRowContext(ctx, query).Scan(&count)
	return count, err
}

// tableExists checks if a table exists
func (v *Validator) tableExists(ctx context.Context, table string) (bool, error) {
	var query string
	switch v.dbType {
	case "postgresql", "postgres":
		query = `SELECT EXISTS (
			SELECT FROM information_schema.tables 
			WHERE table_name = $1
		)`
	case "mysql", "mariadb":
		query = `SELECT COUNT(*) > 0 FROM information_schema.tables 
			WHERE table_name = ?`
	}

	var exists bool
	err := v.db.QueryRowContext(ctx, query, table).Scan(&exists)
	return exists, err
}

// columnExists checks if a column exists
func (v *Validator) columnExists(ctx context.Context, table, column string) (bool, error) {
	var query string
	switch v.dbType {
	case "postgresql", "postgres":
		query = `SELECT EXISTS (
			SELECT FROM information_schema.columns 
			WHERE table_name = $1 AND column_name = $2
		)`
	case "mysql", "mariadb":
		query = `SELECT COUNT(*) > 0 FROM information_schema.columns 
			WHERE table_name = ? AND column_name = ?`
	}

	var exists bool
	err := v.db.QueryRowContext(ctx, query, table, column).Scan(&exists)
	return exists, err
}

// GetTableList returns all tables in the database
func (v *Validator) GetTableList(ctx context.Context) ([]string, error) {
	var query string
	switch v.dbType {
	case "postgresql", "postgres":
		query = `SELECT table_name FROM information_schema.tables 
			WHERE table_schema = 'public' AND table_type = 'BASE TABLE'`
	case "mysql", "mariadb":
		query = `SELECT table_name FROM information_schema.tables 
			WHERE table_schema = DATABASE() AND table_type = 'BASE TABLE'`
	}

	rows, err := v.db.QueryContext(ctx, query)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	var tables []string
	for rows.Next() {
		var table string
		if err := rows.Scan(&table); err != nil {
			return nil, err
		}
		tables = append(tables, table)
	}

	return tables, rows.Err()
}

// GetTotalRowCount returns total row count across all tables
func (v *Validator) GetTotalRowCount(ctx context.Context) (int64, error) {
	tables, err := v.GetTableList(ctx)
	if err != nil {
		return 0, err
	}

	var total int64
	for _, table := range tables {
		count, err := v.getRowCount(ctx, table, "")
		if err != nil {
			continue // Skip tables that can't be counted
		}
		total += count
	}

	return total, nil
}

// GetDatabaseSize returns the database size in bytes
func (v *Validator) GetDatabaseSize(ctx context.Context, dbname string) (int64, error) {
	var query string
	switch v.dbType {
	case "postgresql", "postgres":
		query = fmt.Sprintf("SELECT pg_database_size('%s')", dbname)
	case "mysql", "mariadb":
		query = fmt.Sprintf(`SELECT SUM(data_length + index_length) 
			FROM information_schema.tables WHERE table_schema = '%s'`, dbname)
	}

	var size sql.NullInt64
	err := v.db.QueryRowContext(ctx, query).Scan(&size)
	if err != nil {
		return 0, err
	}

	return size.Int64, nil
}

// ValidateExpectedTables checks that all expected tables exist
func (v *Validator) ValidateExpectedTables(ctx context.Context, expectedTables []string) []CheckResult {
	var results []CheckResult

	for _, table := range expectedTables {
		check := CustomCheck{
			Name:  fmt.Sprintf("Table '%s' exists", table),
			Type:  "table_exists",
			Table: table,
		}
		results = append(results, v.runCheck(ctx, check))
	}

	return results
}

// quoteIdentifier quotes a database identifier
func (v *Validator) quoteIdentifier(id string) string {
	switch v.dbType {
	case "postgresql", "postgres":
		return fmt.Sprintf(`"%s"`, strings.ReplaceAll(id, `"`, `""`))
	case "mysql", "mariadb":
		return fmt.Sprintf("`%s`", strings.ReplaceAll(id, "`", "``"))
	default:
		return id
	}
}
