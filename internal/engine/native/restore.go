package native

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"strings"
	"time"

	"dbbackup/internal/logger"
)

// RestoreEngine defines the interface for native restore operations
type RestoreEngine interface {
	// Restore from a backup source
	Restore(ctx context.Context, source io.Reader, options *RestoreOptions) (*RestoreResult, error)

	// Check if the target database is reachable
	Ping() error

	// Close any open connections
	Close() error
}

// RestoreOptions contains restore-specific configuration
type RestoreOptions struct {
	// Target database name (for single database restore)
	Database string

	// Only restore schema, skip data
	SchemaOnly bool

	// Only restore data, skip schema
	DataOnly bool

	// Drop existing objects before restore
	DropIfExists bool

	// Continue on error instead of stopping
	ContinueOnError bool

	// Disable foreign key checks during restore
	DisableForeignKeys bool

	// Use transactions for restore (when possible)
	UseTransactions bool

	// Parallel restore (number of workers)
	Parallel int

	// Progress callback
	ProgressCallback func(progress *RestoreProgress)
}

// RestoreProgress provides real-time restore progress information
type RestoreProgress struct {
	// Current operation description
	Operation string

	// Current object being processed
	CurrentObject string

	// Objects completed
	ObjectsCompleted int64

	// Total objects (if known)
	TotalObjects int64

	// Rows processed
	RowsProcessed int64

	// Bytes processed
	BytesProcessed int64

	// Estimated completion percentage (0-100)
	PercentComplete float64
}

// PostgreSQLRestoreEngine implements PostgreSQL restore functionality
type PostgreSQLRestoreEngine struct {
	engine *PostgreSQLNativeEngine
}

// NewPostgreSQLRestoreEngine creates a new PostgreSQL restore engine
func NewPostgreSQLRestoreEngine(config *PostgreSQLNativeConfig, log logger.Logger) (*PostgreSQLRestoreEngine, error) {
	engine, err := NewPostgreSQLNativeEngine(config, log)
	if err != nil {
		return nil, fmt.Errorf("failed to create backup engine: %w", err)
	}

	return &PostgreSQLRestoreEngine{
		engine: engine,
	}, nil
}

// Restore restores from a PostgreSQL backup
func (r *PostgreSQLRestoreEngine) Restore(ctx context.Context, source io.Reader, options *RestoreOptions) (*RestoreResult, error) {
	startTime := time.Now()
	result := &RestoreResult{
		EngineUsed: "postgresql_native",
	}

	if options == nil {
		options = &RestoreOptions{}
	}

	// Acquire connection for restore operations
	conn, err := r.engine.pool.Acquire(ctx)
	if err != nil {
		return result, fmt.Errorf("failed to acquire connection: %w", err)
	}
	defer conn.Release()

	// Parse and execute SQL statements from the backup
	scanner := bufio.NewScanner(source)
	scanner.Buffer(make([]byte, 1024*1024), 10*1024*1024) // 10MB max line

	var (
		stmtBuffer    bytes.Buffer
		inCopyMode    bool
		copyTableName string
		copyData      bytes.Buffer
		stmtCount     int64
		rowsRestored  int64
	)

	for scanner.Scan() {
		line := scanner.Text()

		// Handle COPY data mode
		if inCopyMode {
			if line == "\\." {
				// End of COPY data - execute the COPY FROM
				if copyData.Len() > 0 {
					copySQL := fmt.Sprintf("COPY %s FROM STDIN", copyTableName)
					tag, err := conn.Conn().PgConn().CopyFrom(ctx, strings.NewReader(copyData.String()), copySQL)
					if err != nil {
						if options.ContinueOnError {
							r.engine.log.Warn("COPY failed, continuing", "table", copyTableName, "error", err)
						} else {
							return result, fmt.Errorf("COPY to %s failed: %w", copyTableName, err)
						}
					} else {
						rowsRestored += tag.RowsAffected()
					}
				}
				copyData.Reset()
				inCopyMode = false
				copyTableName = ""
				continue
			}
			copyData.WriteString(line)
			copyData.WriteByte('\n')
			continue
		}

		// Check for COPY statement start
		if strings.HasPrefix(strings.ToUpper(strings.TrimSpace(line)), "COPY ") && strings.HasSuffix(strings.TrimSpace(line), "FROM stdin;") {
			// Extract table name from COPY statement
			parts := strings.Fields(line)
			if len(parts) >= 2 {
				copyTableName = parts[1]
				inCopyMode = true
				stmtCount++
				if options.ProgressCallback != nil {
					options.ProgressCallback(&RestoreProgress{
						Operation:        "COPY",
						CurrentObject:    copyTableName,
						ObjectsCompleted: stmtCount,
						RowsProcessed:    rowsRestored,
					})
				}
				continue
			}
		}

		// Skip comments and empty lines for regular statements
		trimmed := strings.TrimSpace(line)
		if trimmed == "" || strings.HasPrefix(trimmed, "--") {
			continue
		}

		// Accumulate statement
		stmtBuffer.WriteString(line)
		stmtBuffer.WriteByte('\n')

		// Check if statement is complete (ends with ;)
		if strings.HasSuffix(trimmed, ";") {
			stmt := stmtBuffer.String()
			stmtBuffer.Reset()

			// Skip data statements if schema-only mode
			if options.SchemaOnly && (strings.HasPrefix(strings.ToUpper(trimmed), "INSERT") ||
				strings.HasPrefix(strings.ToUpper(trimmed), "COPY")) {
				continue
			}

			// Skip schema statements if data-only mode
			if options.DataOnly && !strings.HasPrefix(strings.ToUpper(trimmed), "INSERT") &&
				!strings.HasPrefix(strings.ToUpper(trimmed), "COPY") {
				continue
			}

			// Execute the statement
			_, err := conn.Exec(ctx, stmt)
			if err != nil {
				if options.ContinueOnError {
					r.engine.log.Warn("Statement failed, continuing", "error", err)
				} else {
					return result, fmt.Errorf("statement execution failed: %w", err)
				}
			}
			stmtCount++

			if options.ProgressCallback != nil && stmtCount%100 == 0 {
				options.ProgressCallback(&RestoreProgress{
					Operation:        "SQL",
					ObjectsCompleted: stmtCount,
					RowsProcessed:    rowsRestored,
				})
			}
		}
	}

	if err := scanner.Err(); err != nil {
		return result, fmt.Errorf("error reading backup: %w", err)
	}

	result.Duration = time.Since(startTime)
	result.ObjectsProcessed = int(stmtCount)
	result.BytesProcessed = rowsRestored
	r.engine.log.Info("Restore completed", "statements", stmtCount, "rows", rowsRestored, "duration", result.Duration)

	return result, nil
}

// Ping checks database connectivity
func (r *PostgreSQLRestoreEngine) Ping() error {
	// Use the connection from the backup engine
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return r.engine.conn.Ping(ctx)
}

// Close closes database connections
func (r *PostgreSQLRestoreEngine) Close() error {
	return r.engine.Close()
}

// MySQLRestoreEngine implements MySQL restore functionality
type MySQLRestoreEngine struct {
	engine *MySQLNativeEngine
}

// NewMySQLRestoreEngine creates a new MySQL restore engine
func NewMySQLRestoreEngine(config *MySQLNativeConfig, log logger.Logger) (*MySQLRestoreEngine, error) {
	engine, err := NewMySQLNativeEngine(config, log)
	if err != nil {
		return nil, fmt.Errorf("failed to create backup engine: %w", err)
	}

	return &MySQLRestoreEngine{
		engine: engine,
	}, nil
}

// Restore restores from a MySQL backup
func (r *MySQLRestoreEngine) Restore(ctx context.Context, source io.Reader, options *RestoreOptions) (*RestoreResult, error) {
	startTime := time.Now()
	result := &RestoreResult{
		EngineUsed: "mysql_native",
	}

	if options == nil {
		options = &RestoreOptions{}
	}

	// Parse and execute SQL statements from the backup
	scanner := bufio.NewScanner(source)
	scanner.Buffer(make([]byte, 1024*1024), 10*1024*1024) // 10MB max line

	var (
		stmtBuffer   bytes.Buffer
		stmtCount    int64
		rowsRestored int64
		inMultiLine  bool
		delimiter    = ";"
	)

	// Disable foreign key checks if requested
	if options.DisableForeignKeys {
		if _, err := r.engine.db.ExecContext(ctx, "SET FOREIGN_KEY_CHECKS = 0"); err != nil {
			r.engine.log.Warn("Failed to disable foreign key checks", "error", err)
		}
		defer func() {
			_, _ = r.engine.db.ExecContext(ctx, "SET FOREIGN_KEY_CHECKS = 1")
		}()
	}

	for scanner.Scan() {
		line := scanner.Text()
		trimmed := strings.TrimSpace(line)

		// Skip comments and empty lines
		if trimmed == "" || strings.HasPrefix(trimmed, "--") || strings.HasPrefix(trimmed, "/*") {
			continue
		}

		// Handle DELIMITER changes (common in MySQL dumps)
		if strings.HasPrefix(strings.ToUpper(trimmed), "DELIMITER ") {
			delimiter = strings.TrimSpace(strings.TrimPrefix(trimmed, "DELIMITER "))
			if delimiter == "" {
				delimiter = ";"
			}
			continue
		}

		// Accumulate statement
		stmtBuffer.WriteString(line)
		stmtBuffer.WriteByte('\n')

		// Check if statement is complete
		if strings.HasSuffix(trimmed, delimiter) {
			stmt := strings.TrimSuffix(stmtBuffer.String(), delimiter+"\n")
			stmt = strings.TrimSuffix(stmt, delimiter)
			stmtBuffer.Reset()
			inMultiLine = false

			upperStmt := strings.ToUpper(strings.TrimSpace(stmt))

			// Skip data statements if schema-only mode
			if options.SchemaOnly && strings.HasPrefix(upperStmt, "INSERT") {
				continue
			}

			// Skip schema statements if data-only mode
			if options.DataOnly && !strings.HasPrefix(upperStmt, "INSERT") {
				continue
			}

			// Execute the statement
			res, err := r.engine.db.ExecContext(ctx, stmt)
			if err != nil {
				if options.ContinueOnError {
					r.engine.log.Warn("Statement failed, continuing", "error", err)
				} else {
					return result, fmt.Errorf("statement execution failed: %w", err)
				}
			} else {
				if rows, _ := res.RowsAffected(); rows > 0 {
					rowsRestored += rows
				}
			}
			stmtCount++

			if options.ProgressCallback != nil && stmtCount%100 == 0 {
				options.ProgressCallback(&RestoreProgress{
					Operation:        "SQL",
					ObjectsCompleted: stmtCount,
					RowsProcessed:    rowsRestored,
				})
			}
		} else {
			inMultiLine = true
		}
	}

	// Handle any remaining statement
	if stmtBuffer.Len() > 0 && !inMultiLine {
		stmt := stmtBuffer.String()
		if _, err := r.engine.db.ExecContext(ctx, stmt); err != nil {
			if !options.ContinueOnError {
				return result, fmt.Errorf("final statement failed: %w", err)
			}
		}
		stmtCount++
	}

	if err := scanner.Err(); err != nil {
		return result, fmt.Errorf("error reading backup: %w", err)
	}

	result.Duration = time.Since(startTime)
	result.ObjectsProcessed = int(stmtCount)
	result.BytesProcessed = rowsRestored
	r.engine.log.Info("Restore completed", "statements", stmtCount, "rows", rowsRestored, "duration", result.Duration)

	return result, nil
}

// Ping checks database connectivity
func (r *MySQLRestoreEngine) Ping() error {
	return r.engine.db.Ping()
}

// Close closes database connections
func (r *MySQLRestoreEngine) Close() error {
	return r.engine.Close()
}
