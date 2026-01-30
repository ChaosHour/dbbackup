package native

import (
	"bufio"
	"context"
	"database/sql"
	"fmt"
	"io"
	"math"
	"strings"
	"time"

	"dbbackup/internal/logger"

	"github.com/go-sql-driver/mysql"
)

// MySQLNativeEngine implements pure Go MySQL backup/restore
type MySQLNativeEngine struct {
	db  *sql.DB
	cfg *MySQLNativeConfig
	log logger.Logger
}

type MySQLNativeConfig struct {
	// Connection
	Host     string
	Port     int
	User     string
	Password string
	Database string
	Socket   string
	SSLMode  string

	// Backup options
	Format            string // sql
	Compression       int    // 0-9
	SingleTransaction bool
	LockTables        bool
	Routines          bool
	Triggers          bool
	Events            bool

	// Schema options
	SchemaOnly      bool
	DataOnly        bool
	IncludeDatabase []string
	ExcludeDatabase []string
	IncludeTable    []string
	ExcludeTable    []string

	// Advanced options
	AddDropTable   bool
	CreateOptions  bool
	DisableKeys    bool
	ExtendedInsert bool
	HexBlob        bool
	QuickDump      bool

	// PITR options
	MasterData       int // 0=disabled, 1=CHANGE MASTER, 2=commented
	FlushLogs        bool
	DeleteMasterLogs bool
}

// MySQLDatabaseObject represents a MySQL database object
type MySQLDatabaseObject struct {
	Database     string
	Name         string
	Type         string // table, view, procedure, function, trigger, event
	Engine       string // InnoDB, MyISAM, etc.
	CreateSQL    string
	Dependencies []string
}

// MySQLTableInfo contains table metadata
type MySQLTableInfo struct {
	Name          string
	Engine        string
	Collation     string
	RowCount      int64
	DataLength    int64
	IndexLength   int64
	AutoIncrement *int64
	CreateTime    *time.Time
	UpdateTime    *time.Time
}

// BinlogPosition represents MySQL binary log position
type BinlogPosition struct {
	File     string
	Position int64
	GTIDSet  string
}

// NewMySQLNativeEngine creates a new native MySQL engine
func NewMySQLNativeEngine(cfg *MySQLNativeConfig, log logger.Logger) (*MySQLNativeEngine, error) {
	engine := &MySQLNativeEngine{
		cfg: cfg,
		log: log,
	}

	return engine, nil
}

// Connect establishes database connection
func (e *MySQLNativeEngine) Connect(ctx context.Context) error {
	dsn := e.buildDSN()

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return fmt.Errorf("failed to open MySQL connection: %w", err)
	}

	// Configure connection pool
	db.SetMaxOpenConns(10)
	db.SetMaxIdleConns(5)
	db.SetConnMaxLifetime(30 * time.Minute)

	if err := db.PingContext(ctx); err != nil {
		db.Close()
		return fmt.Errorf("failed to ping MySQL server: %w", err)
	}

	e.db = db
	return nil
}

// Backup performs native MySQL backup
func (e *MySQLNativeEngine) Backup(ctx context.Context, outputWriter io.Writer) (*BackupResult, error) {
	startTime := time.Now()
	result := &BackupResult{
		Format: "sql",
	}

	e.log.Info("Starting native MySQL backup", "database", e.cfg.Database)

	// Get binlog position for PITR
	binlogPos, err := e.getBinlogPosition(ctx)
	if err != nil {
		e.log.Warn("Failed to get binlog position", "error", err)
	}

	// Start transaction for consistent backup
	var tx *sql.Tx
	if e.cfg.SingleTransaction {
		tx, err = e.db.BeginTx(ctx, &sql.TxOptions{
			Isolation: sql.LevelRepeatableRead,
			ReadOnly:  true,
		})
		if err != nil {
			return nil, fmt.Errorf("failed to start transaction: %w", err)
		}
		defer tx.Rollback()

		// Set transaction isolation
		if _, err := tx.ExecContext(ctx, "SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ"); err != nil {
			return nil, fmt.Errorf("failed to set isolation level: %w", err)
		}

		if _, err := tx.ExecContext(ctx, "START TRANSACTION WITH CONSISTENT SNAPSHOT"); err != nil {
			return nil, fmt.Errorf("failed to start consistent snapshot: %w", err)
		}
	}

	// Write SQL header
	if err := e.writeSQLHeader(outputWriter, binlogPos); err != nil {
		return nil, err
	}

	// Get databases to backup
	databases, err := e.getDatabases(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get databases: %w", err)
	}

	// Backup each database
	for _, database := range databases {
		if !e.shouldIncludeDatabase(database) {
			continue
		}

		e.log.Debug("Backing up database", "database", database)

		if err := e.backupDatabase(ctx, outputWriter, database, tx, result); err != nil {
			return nil, fmt.Errorf("failed to backup database %s: %w", database, err)
		}
	}

	// Write SQL footer
	if err := e.writeSQLFooter(outputWriter); err != nil {
		return nil, err
	}

	result.Duration = time.Since(startTime)
	return result, nil
}

// backupDatabase backs up a single database
func (e *MySQLNativeEngine) backupDatabase(ctx context.Context, w io.Writer, database string, tx *sql.Tx, result *BackupResult) error {
	// Write database header
	if err := e.writeDatabaseHeader(w, database); err != nil {
		return err
	}

	// Get database objects
	objects, err := e.getDatabaseObjects(ctx, database)
	if err != nil {
		return fmt.Errorf("failed to get database objects: %w", err)
	}

	// Create database
	if !e.cfg.DataOnly {
		createSQL, err := e.getDatabaseCreateSQL(ctx, database)
		if err != nil {
			return fmt.Errorf("failed to get database create SQL: %w", err)
		}

		if _, err := w.Write([]byte(createSQL + "\n")); err != nil {
			return err
		}

		// Use database
		useSQL := fmt.Sprintf("USE `%s`;\n\n", database)
		if _, err := w.Write([]byte(useSQL)); err != nil {
			return err
		}
	}

	// Backup tables (schema and data)
	tables := e.filterObjectsByType(objects, "table")

	// Schema first
	if !e.cfg.DataOnly {
		for _, table := range tables {
			if err := e.backupTableSchema(ctx, w, database, table.Name); err != nil {
				return fmt.Errorf("failed to backup table schema %s: %w", table.Name, err)
			}
			result.ObjectsProcessed++
		}
	}

	// Then data
	if !e.cfg.SchemaOnly {
		for _, table := range tables {
			bytesWritten, err := e.backupTableData(ctx, w, database, table.Name, tx)
			if err != nil {
				return fmt.Errorf("failed to backup table data %s: %w", table.Name, err)
			}
			result.BytesProcessed += bytesWritten
		}
	}

	// Backup other objects
	if !e.cfg.DataOnly {
		if e.cfg.Routines {
			if err := e.backupRoutines(ctx, w, database); err != nil {
				return fmt.Errorf("failed to backup routines: %w", err)
			}
		}

		if e.cfg.Triggers {
			if err := e.backupTriggers(ctx, w, database); err != nil {
				return fmt.Errorf("failed to backup triggers: %w", err)
			}
		}

		if e.cfg.Events {
			if err := e.backupEvents(ctx, w, database); err != nil {
				return fmt.Errorf("failed to backup events: %w", err)
			}
		}
	}

	return nil
}

// backupTableData exports table data using SELECT INTO OUTFILE equivalent
func (e *MySQLNativeEngine) backupTableData(ctx context.Context, w io.Writer, database, table string, tx *sql.Tx) (int64, error) {
	// Get table info
	tableInfo, err := e.getTableInfo(ctx, database, table)
	if err != nil {
		return 0, err
	}

	// Skip empty tables
	if tableInfo.RowCount == 0 {
		return 0, nil
	}

	// Write table data header
	header := fmt.Sprintf("--\n-- Dumping data for table `%s`\n--\n\n", table)
	if e.cfg.DisableKeys {
		header += fmt.Sprintf("/*!40000 ALTER TABLE `%s` DISABLE KEYS */;\n", table)
	}

	if _, err := w.Write([]byte(header)); err != nil {
		return 0, err
	}

	// Get column information
	columns, err := e.getTableColumns(ctx, database, table)
	if err != nil {
		return 0, err
	}

	// Build SELECT query
	selectSQL := fmt.Sprintf("SELECT %s FROM `%s`.`%s`",
		strings.Join(columns, ", "), database, table)

	// Execute query using transaction if available
	var rows *sql.Rows
	if tx != nil {
		rows, err = tx.QueryContext(ctx, selectSQL)
	} else {
		rows, err = e.db.QueryContext(ctx, selectSQL)
	}

	if err != nil {
		return 0, fmt.Errorf("failed to query table data: %w", err)
	}
	defer rows.Close()

	// Process rows in batches and generate INSERT statements
	var bytesWritten int64
	var insertValues []string
	const batchSize = 1000
	rowCount := 0

	for rows.Next() {
		// Scan row values
		values, err := e.scanRowValues(rows, len(columns))
		if err != nil {
			return bytesWritten, err
		}

		// Format values for INSERT
		valueStr := e.formatInsertValues(values)
		insertValues = append(insertValues, valueStr)
		rowCount++

		// Write batch when full
		if rowCount >= batchSize {
			if err := e.writeInsertBatch(w, database, table, columns, insertValues, &bytesWritten); err != nil {
				return bytesWritten, err
			}
			insertValues = insertValues[:0]
			rowCount = 0
		}
	}

	// Write remaining batch
	if rowCount > 0 {
		if err := e.writeInsertBatch(w, database, table, columns, insertValues, &bytesWritten); err != nil {
			return bytesWritten, err
		}
	}

	// Write table data footer
	footer := ""
	if e.cfg.DisableKeys {
		footer = fmt.Sprintf("/*!40000 ALTER TABLE `%s` ENABLE KEYS */;\n", table)
	}
	footer += "\n"

	written, err := w.Write([]byte(footer))
	if err != nil {
		return bytesWritten, err
	}
	bytesWritten += int64(written)

	return bytesWritten, rows.Err()
}

// Helper methods
func (e *MySQLNativeEngine) buildDSN() string {
	cfg := mysql.Config{
		User:   e.cfg.User,
		Passwd: e.cfg.Password,
		Net:    "tcp",
		Addr:   fmt.Sprintf("%s:%d", e.cfg.Host, e.cfg.Port),
		DBName: e.cfg.Database,

		// Performance settings
		Timeout:      30 * time.Second,
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 30 * time.Second,

		// Character set
		Params: map[string]string{
			"charset":   "utf8mb4",
			"parseTime": "true",
			"loc":       "Local",
		},
	}

	// Use socket if specified
	if e.cfg.Socket != "" {
		cfg.Net = "unix"
		cfg.Addr = e.cfg.Socket
	}

	// SSL configuration
	if e.cfg.SSLMode != "" {
		switch strings.ToLower(e.cfg.SSLMode) {
		case "disable", "disabled":
			cfg.TLSConfig = "false"
		case "require", "required":
			cfg.TLSConfig = "true"
		default:
			cfg.TLSConfig = "preferred"
		}
	}

	return cfg.FormatDSN()
}

func (e *MySQLNativeEngine) getBinlogPosition(ctx context.Context) (*BinlogPosition, error) {
	var file string
	var position int64

	row := e.db.QueryRowContext(ctx, "SHOW MASTER STATUS")
	if err := row.Scan(&file, &position, nil, nil, nil); err != nil {
		return nil, fmt.Errorf("failed to get master status: %w", err)
	}

	// Try to get GTID set (MySQL 5.6+)
	var gtidSet string
	if row := e.db.QueryRowContext(ctx, "SELECT @@global.gtid_executed"); row != nil {
		row.Scan(&gtidSet)
	}

	return &BinlogPosition{
		File:     file,
		Position: position,
		GTIDSet:  gtidSet,
	}, nil
}

// Additional helper methods (stubs for brevity)
func (e *MySQLNativeEngine) writeSQLHeader(w io.Writer, binlogPos *BinlogPosition) error {
	header := fmt.Sprintf(`/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;
/*!40101 SET @OLD_CHARACTER_SET_RESULTS=@@CHARACTER_SET_RESULTS */;
/*!40101 SET @OLD_COLLATION_CONNECTION=@@COLLATION_CONNECTION */;
/*!40101 SET NAMES utf8mb4 */;
/*!40103 SET @OLD_TIME_ZONE=@@TIME_ZONE */;
/*!40103 SET TIME_ZONE='+00:00' */;
/*!40014 SET @OLD_UNIQUE_CHECKS=@@UNIQUE_CHECKS, UNIQUE_CHECKS=0 */;
/*!40014 SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0 */;
/*!40101 SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='NO_AUTO_VALUE_ON_ZERO' */;
/*!40111 SET @OLD_SQL_NOTES=@@SQL_NOTES, SQL_NOTES=0 */;

-- MySQL dump generated by dbbackup native engine
-- Host: %s    Database: %s
-- ------------------------------------------------------
-- Server version: TBD

`, e.cfg.Host, e.cfg.Database)

	if binlogPos != nil && e.cfg.MasterData > 0 {
		comment := ""
		if e.cfg.MasterData == 2 {
			comment = "-- "
		}
		header += fmt.Sprintf("\n%sCHANGE MASTER TO MASTER_LOG_FILE='%s', MASTER_LOG_POS=%d;\n\n",
			comment, binlogPos.File, binlogPos.Position)
	}

	_, err := w.Write([]byte(header))
	return err
}

func (e *MySQLNativeEngine) getDatabases(ctx context.Context) ([]string, error) {
	if e.cfg.Database != "" {
		return []string{e.cfg.Database}, nil
	}

	rows, err := e.db.QueryContext(ctx, "SHOW DATABASES")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var databases []string
	for rows.Next() {
		var db string
		if err := rows.Scan(&db); err != nil {
			return nil, err
		}

		// Skip system databases
		if db != "information_schema" && db != "mysql" && db != "performance_schema" && db != "sys" {
			databases = append(databases, db)
		}
	}

	return databases, rows.Err()
}

func (e *MySQLNativeEngine) shouldIncludeDatabase(database string) bool {
	// Skip system databases
	if database == "information_schema" || database == "mysql" ||
		database == "performance_schema" || database == "sys" {
		return false
	}

	// Apply include/exclude filters if configured
	if len(e.cfg.IncludeDatabase) > 0 {
		for _, included := range e.cfg.IncludeDatabase {
			if database == included {
				return true
			}
		}
		return false
	}

	for _, excluded := range e.cfg.ExcludeDatabase {
		if database == excluded {
			return false
		}
	}

	return true
}

func (e *MySQLNativeEngine) getDatabaseObjects(ctx context.Context, database string) ([]MySQLDatabaseObject, error) {
	var objects []MySQLDatabaseObject

	// Get tables
	tables, err := e.getTables(ctx, database)
	if err != nil {
		return nil, fmt.Errorf("failed to get tables: %w", err)
	}
	objects = append(objects, tables...)

	// Get views
	views, err := e.getViews(ctx, database)
	if err != nil {
		return nil, fmt.Errorf("failed to get views: %w", err)
	}
	objects = append(objects, views...)

	return objects, nil
}

// getTables retrieves all tables in database
func (e *MySQLNativeEngine) getTables(ctx context.Context, database string) ([]MySQLDatabaseObject, error) {
	query := `
		SELECT table_name, engine, table_collation
		FROM information_schema.tables
		WHERE table_schema = ? AND table_type = 'BASE TABLE'
		ORDER BY table_name`

	rows, err := e.db.QueryContext(ctx, query, database)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var objects []MySQLDatabaseObject
	for rows.Next() {
		var tableName, engine, collation sql.NullString
		if err := rows.Scan(&tableName, &engine, &collation); err != nil {
			return nil, err
		}

		obj := MySQLDatabaseObject{
			Database: database,
			Name:     tableName.String,
			Type:     "table",
			Engine:   engine.String,
		}

		objects = append(objects, obj)
	}

	return objects, rows.Err()
}

// getViews retrieves all views in database
func (e *MySQLNativeEngine) getViews(ctx context.Context, database string) ([]MySQLDatabaseObject, error) {
	query := `
		SELECT table_name
		FROM information_schema.views
		WHERE table_schema = ?
		ORDER BY table_name`

	rows, err := e.db.QueryContext(ctx, query, database)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var objects []MySQLDatabaseObject
	for rows.Next() {
		var viewName string
		if err := rows.Scan(&viewName); err != nil {
			return nil, err
		}

		obj := MySQLDatabaseObject{
			Database: database,
			Name:     viewName,
			Type:     "view",
		}

		objects = append(objects, obj)
	}

	return objects, rows.Err()
}

func (e *MySQLNativeEngine) filterObjectsByType(objects []MySQLDatabaseObject, objType string) []MySQLDatabaseObject {
	var filtered []MySQLDatabaseObject
	for _, obj := range objects {
		if obj.Type == objType {
			filtered = append(filtered, obj)
		}
	}
	return filtered
}

func (e *MySQLNativeEngine) getDatabaseCreateSQL(ctx context.Context, database string) (string, error) {
	query := "SHOW CREATE DATABASE " + fmt.Sprintf("`%s`", database)

	row := e.db.QueryRowContext(ctx, query)

	var dbName, createSQL string
	if err := row.Scan(&dbName, &createSQL); err != nil {
		return "", err
	}

	return createSQL + ";", nil
}

func (e *MySQLNativeEngine) writeDatabaseHeader(w io.Writer, database string) error {
	header := fmt.Sprintf("\n--\n-- Database: `%s`\n--\n\n", database)
	_, err := w.Write([]byte(header))
	return err
}

func (e *MySQLNativeEngine) backupTableSchema(ctx context.Context, w io.Writer, database, table string) error {
	query := "SHOW CREATE TABLE " + fmt.Sprintf("`%s`.`%s`", database, table)

	row := e.db.QueryRowContext(ctx, query)

	var tableName, createSQL string
	if err := row.Scan(&tableName, &createSQL); err != nil {
		return err
	}

	// Write table header
	header := fmt.Sprintf("\n--\n-- Table structure for table `%s`\n--\n\n", table)
	if _, err := w.Write([]byte(header)); err != nil {
		return err
	}

	// Add DROP TABLE if configured
	if e.cfg.AddDropTable {
		dropSQL := fmt.Sprintf("DROP TABLE IF EXISTS `%s`;\n", table)
		if _, err := w.Write([]byte(dropSQL)); err != nil {
			return err
		}
	}

	// Write CREATE TABLE
	createSQL += ";\n\n"
	if _, err := w.Write([]byte(createSQL)); err != nil {
		return err
	}

	return nil
}

func (e *MySQLNativeEngine) getTableInfo(ctx context.Context, database, table string) (*MySQLTableInfo, error) {
	query := `
		SELECT table_name, engine, table_collation, table_rows,
			   data_length, index_length, auto_increment,
			   create_time, update_time
		FROM information_schema.tables
		WHERE table_schema = ? AND table_name = ?`

	row := e.db.QueryRowContext(ctx, query, database, table)

	var info MySQLTableInfo
	var autoInc, createTime, updateTime sql.NullInt64
	var collation sql.NullString

	err := row.Scan(&info.Name, &info.Engine, &collation, &info.RowCount,
		&info.DataLength, &info.IndexLength, &autoInc, &createTime, &updateTime)

	if err != nil {
		return nil, err
	}

	info.Collation = collation.String
	if autoInc.Valid {
		info.AutoIncrement = &autoInc.Int64
	}

	if createTime.Valid {
		createTimeVal := time.Unix(createTime.Int64, 0)
		info.CreateTime = &createTimeVal
	}

	if updateTime.Valid {
		updateTimeVal := time.Unix(updateTime.Int64, 0)
		info.UpdateTime = &updateTimeVal
	}

	return &info, nil
}

func (e *MySQLNativeEngine) getTableColumns(ctx context.Context, database, table string) ([]string, error) {
	query := `
		SELECT column_name
		FROM information_schema.columns
		WHERE table_schema = ? AND table_name = ?
		ORDER BY ordinal_position`

	rows, err := e.db.QueryContext(ctx, query, database, table)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var columns []string
	for rows.Next() {
		var columnName string
		if err := rows.Scan(&columnName); err != nil {
			return nil, err
		}
		columns = append(columns, fmt.Sprintf("`%s`", columnName))
	}

	return columns, rows.Err()
}

func (e *MySQLNativeEngine) scanRowValues(rows *sql.Rows, columnCount int) ([]interface{}, error) {
	// Create slice to hold column values
	values := make([]interface{}, columnCount)
	valuePtrs := make([]interface{}, columnCount)

	// Initialize value pointers
	for i := range values {
		valuePtrs[i] = &values[i]
	}

	// Scan row into value pointers
	if err := rows.Scan(valuePtrs...); err != nil {
		return nil, err
	}

	return values, nil
}

func (e *MySQLNativeEngine) formatInsertValues(values []interface{}) string {
	var formattedValues []string

	for _, value := range values {
		if value == nil {
			formattedValues = append(formattedValues, "NULL")
		} else {
			switch v := value.(type) {
			case string:
				// Properly escape string values using MySQL escaping rules
				formattedValues = append(formattedValues, e.escapeString(v))
			case []byte:
				// Handle binary data based on configuration
				if len(v) == 0 {
					formattedValues = append(formattedValues, "''")
				} else if e.cfg.HexBlob {
					formattedValues = append(formattedValues, fmt.Sprintf("0x%X", v))
				} else {
					// Check if it's printable text or binary
					if e.isPrintableBinary(v) {
						escaped := e.escapeBinaryString(string(v))
						formattedValues = append(formattedValues, escaped)
					} else {
						// Force hex encoding for true binary data
						formattedValues = append(formattedValues, fmt.Sprintf("0x%X", v))
					}
				}
			case time.Time:
				// Format timestamps properly with microseconds if needed
				if v.Nanosecond() != 0 {
					formattedValues = append(formattedValues, fmt.Sprintf("'%s'", v.Format("2006-01-02 15:04:05.999999")))
				} else {
					formattedValues = append(formattedValues, fmt.Sprintf("'%s'", v.Format("2006-01-02 15:04:05")))
				}
			case bool:
				if v {
					formattedValues = append(formattedValues, "1")
				} else {
					formattedValues = append(formattedValues, "0")
				}
			case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
				// Integer types - no quotes
				formattedValues = append(formattedValues, fmt.Sprintf("%v", v))
			case float32, float64:
				// Float types - no quotes, handle NaN and Inf
				var floatVal float64
				if f32, ok := v.(float32); ok {
					floatVal = float64(f32)
				} else {
					floatVal = v.(float64)
				}
				
				if math.IsNaN(floatVal) {
					formattedValues = append(formattedValues, "NULL")
				} else if math.IsInf(floatVal, 0) {
					formattedValues = append(formattedValues, "NULL")
				} else {
					formattedValues = append(formattedValues, fmt.Sprintf("%v", v))
				}
			default:
				// Other types - convert to string and escape
				str := fmt.Sprintf("%v", v)
				formattedValues = append(formattedValues, e.escapeString(str))
			}
		}
	}

	return "(" + strings.Join(formattedValues, ",") + ")"
}

// isPrintableBinary checks if binary data contains mostly printable characters
func (e *MySQLNativeEngine) isPrintableBinary(data []byte) bool {
	if len(data) == 0 {
		return true
	}

	printableCount := 0
	for _, b := range data {
		if b >= 32 && b <= 126 || b == '\n' || b == '\r' || b == '\t' {
			printableCount++
		}
	}

	// Consider it printable if more than 80% are printable chars
	return float64(printableCount)/float64(len(data)) > 0.8
}

// escapeBinaryString escapes binary data when treating as string
func (e *MySQLNativeEngine) escapeBinaryString(s string) string {
	// Use MySQL-style escaping for binary strings
	s = strings.ReplaceAll(s, "\\", "\\\\")
	s = strings.ReplaceAll(s, "'", "\\'")
	s = strings.ReplaceAll(s, "\"", "\\\"")
	s = strings.ReplaceAll(s, "\n", "\\n")
	s = strings.ReplaceAll(s, "\r", "\\r")
	s = strings.ReplaceAll(s, "\t", "\\t")
	s = strings.ReplaceAll(s, "\x00", "\\0")
	s = strings.ReplaceAll(s, "\x1a", "\\Z")

	return fmt.Sprintf("'%s'", s)
}

func (e *MySQLNativeEngine) writeInsertBatch(w io.Writer, database, table string, columns []string, values []string, bytesWritten *int64) error {
	if len(values) == 0 {
		return nil
	}

	var insertSQL string

	if e.cfg.ExtendedInsert {
		// Use extended INSERT syntax for better performance
		insertSQL = fmt.Sprintf("INSERT INTO `%s`.`%s` (%s) VALUES\n%s;\n",
			database, table, strings.Join(columns, ","), strings.Join(values, ",\n"))
	} else {
		// Use individual INSERT statements
		var statements []string
		for _, value := range values {
			stmt := fmt.Sprintf("INSERT INTO `%s`.`%s` (%s) VALUES %s;",
				database, table, strings.Join(columns, ","), value)
			statements = append(statements, stmt)
		}
		insertSQL = strings.Join(statements, "\n") + "\n"
	}

	written, err := w.Write([]byte(insertSQL))
	if err != nil {
		return err
	}

	*bytesWritten += int64(written)
	return nil
}

func (e *MySQLNativeEngine) backupRoutines(ctx context.Context, w io.Writer, database string) error {
	query := `
		SELECT routine_name, routine_type
		FROM information_schema.routines
		WHERE routine_schema = ? AND routine_type IN ('FUNCTION', 'PROCEDURE')
		ORDER BY routine_name`

	rows, err := e.db.QueryContext(ctx, query, database)
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var routineName, routineType string
		if err := rows.Scan(&routineName, &routineType); err != nil {
			return err
		}

		// Get routine definition
		var showCmd string
		if routineType == "FUNCTION" {
			showCmd = "SHOW CREATE FUNCTION"
		} else {
			showCmd = "SHOW CREATE PROCEDURE"
		}

		defRow := e.db.QueryRowContext(ctx, fmt.Sprintf("%s `%s`.`%s`", showCmd, database, routineName))

		var name, createSQL, charset, collation sql.NullString
		if err := defRow.Scan(&name, &createSQL, &charset, &collation); err != nil {
			continue // Skip routines we can't read
		}

		// Write routine header
		header := fmt.Sprintf("\n--\n-- %s `%s`\n--\n\n", strings.Title(strings.ToLower(routineType)), routineName)
		if _, err := w.Write([]byte(header)); err != nil {
			return err
		}

		// Write DROP statement
		dropSQL := fmt.Sprintf("DROP %s IF EXISTS `%s`;\n", routineType, routineName)
		if _, err := w.Write([]byte(dropSQL)); err != nil {
			return err
		}

		// Write CREATE statement
		if _, err := w.Write([]byte(createSQL.String + ";\n\n")); err != nil {
			return err
		}
	}

	return rows.Err()
}

func (e *MySQLNativeEngine) backupTriggers(ctx context.Context, w io.Writer, database string) error {
	query := `
		SELECT trigger_name
		FROM information_schema.triggers
		WHERE trigger_schema = ?
		ORDER BY trigger_name`

	rows, err := e.db.QueryContext(ctx, query, database)
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var triggerName string
		if err := rows.Scan(&triggerName); err != nil {
			return err
		}

		// Get trigger definition
		defRow := e.db.QueryRowContext(ctx, fmt.Sprintf("SHOW CREATE TRIGGER `%s`.`%s`", database, triggerName))

		var name, createSQL, charset, collation sql.NullString
		if err := defRow.Scan(&name, &createSQL, &charset, &collation); err != nil {
			continue // Skip triggers we can't read
		}

		// Write trigger
		header := fmt.Sprintf("\n--\n-- Trigger `%s`\n--\n\n", triggerName)
		if _, err := w.Write([]byte(header + createSQL.String + ";\n\n")); err != nil {
			return err
		}
	}

	return rows.Err()
}

func (e *MySQLNativeEngine) backupEvents(ctx context.Context, w io.Writer, database string) error {
	query := `
		SELECT event_name
		FROM information_schema.events
		WHERE event_schema = ?
		ORDER BY event_name`

	rows, err := e.db.QueryContext(ctx, query, database)
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var eventName string
		if err := rows.Scan(&eventName); err != nil {
			return err
		}

		// Get event definition
		defRow := e.db.QueryRowContext(ctx, fmt.Sprintf("SHOW CREATE EVENT `%s`.`%s`", database, eventName))

		var name, createSQL, charset, collation sql.NullString
		if err := defRow.Scan(&name, &createSQL, &charset, &collation); err != nil {
			continue // Skip events we can't read
		}

		// Write event
		header := fmt.Sprintf("\n--\n-- Event `%s`\n--\n\n", eventName)
		if _, err := w.Write([]byte(header + createSQL.String + ";\n\n")); err != nil {
			return err
		}
	}

	return rows.Err()
}
func (e *MySQLNativeEngine) writeSQLFooter(w io.Writer) error {
	footer := `/*!40103 SET TIME_ZONE=@OLD_TIME_ZONE */;
/*!40101 SET SQL_MODE=@OLD_SQL_MODE */;
/*!40014 SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS */;
/*!40014 SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS */;
/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;
/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;
/*!40111 SET SQL_NOTES=@OLD_SQL_NOTES */;

-- Dump completed
`
	_, err := w.Write([]byte(footer))
	return err
}

// escapeString properly escapes a string value for MySQL SQL
func (e *MySQLNativeEngine) escapeString(s string) string {
	// Use MySQL-style escaping
	s = strings.ReplaceAll(s, "\\", "\\\\")
	s = strings.ReplaceAll(s, "'", "\\'")
	s = strings.ReplaceAll(s, "\"", "\\\"")
	s = strings.ReplaceAll(s, "\n", "\\n")
	s = strings.ReplaceAll(s, "\r", "\\r")
	s = strings.ReplaceAll(s, "\t", "\\t")
	s = strings.ReplaceAll(s, "\x00", "\\0")
	s = strings.ReplaceAll(s, "\x1a", "\\Z")
	
	return fmt.Sprintf("'%s'", s)
}

// Name returns the engine name
func (e *MySQLNativeEngine) Name() string {
	return "MySQL Native Engine"
}

// Version returns the engine version
func (e *MySQLNativeEngine) Version() string {
	return "1.0.0-native"
}

// SupportedFormats returns list of supported backup formats
func (e *MySQLNativeEngine) SupportedFormats() []string {
	return []string{"sql"}
}

// SupportsParallel returns true if parallel processing is supported
func (e *MySQLNativeEngine) SupportsParallel() bool {
	return false // TODO: Implement multi-threaded dumping
}

// SupportsIncremental returns true if incremental backups are supported
func (e *MySQLNativeEngine) SupportsIncremental() bool {
	return false // TODO: Implement binary log-based incremental backups
}

// SupportsPointInTime returns true if point-in-time recovery is supported
func (e *MySQLNativeEngine) SupportsPointInTime() bool {
	return true // Binary log position tracking implemented
}

// SupportsStreaming returns true if streaming backups are supported
func (e *MySQLNativeEngine) SupportsStreaming() bool {
	return true
}

// CheckConnection verifies database connectivity
func (e *MySQLNativeEngine) CheckConnection(ctx context.Context) error {
	if e.db == nil {
		return fmt.Errorf("not connected")
	}

	return e.db.PingContext(ctx)
}

// ValidateConfiguration checks if configuration is valid
func (e *MySQLNativeEngine) ValidateConfiguration() error {
	if e.cfg.Host == "" && e.cfg.Socket == "" {
		return fmt.Errorf("either host or socket is required")
	}
	if e.cfg.User == "" {
		return fmt.Errorf("user is required")
	}
	if e.cfg.Host != "" && e.cfg.Port <= 0 {
		return fmt.Errorf("invalid port: %d", e.cfg.Port)
	}

	return nil
}

// Restore performs native MySQL restore
func (e *MySQLNativeEngine) Restore(ctx context.Context, inputReader io.Reader, targetDB string) error {
	e.log.Info("Starting native MySQL restore", "target", targetDB)

	// Use database if specified
	if targetDB != "" {
		if _, err := e.db.ExecContext(ctx, "USE `"+targetDB+"`"); err != nil {
			return fmt.Errorf("failed to use database %s: %w", targetDB, err)
		}
	}

	// Read and execute SQL script
	scanner := bufio.NewScanner(inputReader)
	var sqlBuffer strings.Builder

	for scanner.Scan() {
		line := scanner.Text()

		// Skip comments and empty lines
		trimmed := strings.TrimSpace(line)
		if trimmed == "" || strings.HasPrefix(trimmed, "--") || strings.HasPrefix(trimmed, "/*") {
			continue
		}

		sqlBuffer.WriteString(line)
		sqlBuffer.WriteString("\n")

		// Execute statement if it ends with semicolon
		if strings.HasSuffix(trimmed, ";") {
			stmt := sqlBuffer.String()
			sqlBuffer.Reset()

			if _, err := e.db.ExecContext(ctx, stmt); err != nil {
				e.log.Warn("Failed to execute statement", "error", err, "statement", stmt[:100])
				// Continue with next statement (non-fatal errors)
			}
		}
	}

	if err := scanner.Err(); err != nil {
		return fmt.Errorf("error reading input: %w", err)
	}

	e.log.Info("Native MySQL restore completed")
	return nil
}

func (e *MySQLNativeEngine) Close() error {
	if e.db != nil {
		return e.db.Close()
	}
	return nil
}
