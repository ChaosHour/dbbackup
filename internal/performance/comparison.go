// Package performance provides native vs tool-based performance comparison
package performance

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
)

// ComparisonResult contains the results of a native vs tool-based comparison
type ComparisonResult struct {
	Database     DatabaseBackend  `json:"database"`
	DataSize     DataSize         `json:"data_size"`
	Operation    string           `json:"operation"` // "backup" or "restore"
	NativeResult *BenchmarkResult `json:"native_result"`
	ToolResult   *BenchmarkResult `json:"tool_result"`
	SpeedupRatio float64          `json:"speedup_ratio"`  // native/tool throughput
	MemoryRatio  float64          `json:"memory_ratio"`   // tool/native memory
}

// ComparisonSuite manages performance comparison benchmarks
type ComparisonSuite struct {
	mysqlDSN    string
	pgDSN       string
	outputDir   string
	mysqlDB     *sql.DB
	pgDB        *sql.DB
	results     []ComparisonResult
}

// NewComparisonSuite creates a new comparison suite
func NewComparisonSuite(mysqlDSN, pgDSN, outputDir string) *ComparisonSuite {
	return &ComparisonSuite{
		mysqlDSN:  mysqlDSN,
		pgDSN:     pgDSN,
		outputDir: outputDir,
	}
}

// Setup connects to databases and prepares output directory
func (cs *ComparisonSuite) Setup(ctx context.Context) error {
	if err := os.MkdirAll(cs.outputDir, 0750); err != nil {
		return fmt.Errorf("creating output directory: %w", err)
	}

	if cs.mysqlDSN != "" {
		db, err := sql.Open("mysql", cs.mysqlDSN)
		if err != nil {
			return fmt.Errorf("connecting to MySQL: %w", err)
		}
		if err := db.PingContext(ctx); err != nil {
			return fmt.Errorf("pinging MySQL: %w", err)
		}
		cs.mysqlDB = db
	}

	if cs.pgDSN != "" {
		db, err := sql.Open("postgres", cs.pgDSN)
		if err != nil {
			return fmt.Errorf("connecting to PostgreSQL: %w", err)
		}
		if err := db.PingContext(ctx); err != nil {
			return fmt.Errorf("pinging PostgreSQL: %w", err)
		}
		cs.pgDB = db
	}

	return nil
}

// Cleanup closes database connections
func (cs *ComparisonSuite) Cleanup() {
	if cs.mysqlDB != nil {
		_ = cs.mysqlDB.Close()
	}
	if cs.pgDB != nil {
		_ = cs.pgDB.Close()
	}
}

// RunMySQLBackupComparison runs backup comparison for MySQL
func (cs *ComparisonSuite) RunMySQLBackupComparison(ctx context.Context, size DataSize) (*ComparisonResult, error) {
	if cs.mysqlDB == nil {
		return nil, fmt.Errorf("MySQL not connected")
	}

	spec := GetDatasetSpec(size)
	dbName := fmt.Sprintf("bench_%s", size)

	// Create test dataset
	if err := CreateMySQLDataset(ctx, cs.mysqlDB, dbName, spec); err != nil {
		return nil, fmt.Errorf("creating dataset: %w", err)
	}
	defer func() { _ = CleanupMySQLDataset(ctx, cs.mysqlDB, dbName) }()

	dataSize := spec.ApproxSizeBytes()

	// Benchmark native backup (using Go database/sql)
	nativeResult, err := cs.benchmarkMySQLNativeBackup(ctx, dbName, dataSize)
	if err != nil {
		return nil, fmt.Errorf("native backup benchmark: %w", err)
	}

	// Benchmark tool-based backup (using mysqldump)
	toolResult, err := cs.benchmarkMySQLToolBackup(ctx, dbName, dataSize)
	if err != nil {
		return nil, fmt.Errorf("tool backup benchmark: %w", err)
	}

	result := &ComparisonResult{
		Database:     DatabaseBackendMySQL,
		DataSize:     size,
		Operation:    "backup",
		NativeResult: nativeResult,
		ToolResult:   toolResult,
	}

	if toolResult.Throughput > 0 {
		result.SpeedupRatio = nativeResult.Throughput / toolResult.Throughput
	}
	if nativeResult.AllocBytes > 0 {
		result.MemoryRatio = float64(toolResult.AllocBytes) / float64(nativeResult.AllocBytes)
	}

	cs.results = append(cs.results, *result)
	return result, nil
}

// RunMySQLRestoreComparison runs restore comparison for MySQL
func (cs *ComparisonSuite) RunMySQLRestoreComparison(ctx context.Context, size DataSize) (*ComparisonResult, error) {
	if cs.mysqlDB == nil {
		return nil, fmt.Errorf("MySQL not connected")
	}

	spec := GetDatasetSpec(size)
	dbName := fmt.Sprintf("bench_%s", size)

	// Create test dataset and dump it
	if err := CreateMySQLDataset(ctx, cs.mysqlDB, dbName, spec); err != nil {
		return nil, fmt.Errorf("creating dataset: %w", err)
	}

	// Create dump for restore benchmark
	dumpFile := filepath.Join(cs.outputDir, fmt.Sprintf("mysql_bench_%s.sql", size))
	if err := cs.createMySQLDump(ctx, dbName, dumpFile); err != nil {
		_ = CleanupMySQLDataset(ctx, cs.mysqlDB, dbName)
		return nil, fmt.Errorf("creating dump: %w", err)
	}
	defer func() { _ = os.Remove(dumpFile) }()

	dumpInfo, _ := os.Stat(dumpFile)
	dataSize := dumpInfo.Size()

	_ = CleanupMySQLDataset(ctx, cs.mysqlDB, dbName)

	// Benchmark native restore
	nativeResult, err := cs.benchmarkMySQLNativeRestore(ctx, dbName, dumpFile, dataSize)
	if err != nil {
		return nil, fmt.Errorf("native restore benchmark: %w", err)
	}
	_ = CleanupMySQLDataset(ctx, cs.mysqlDB, dbName)

	// Benchmark tool-based restore (using mysql client)
	toolResult, err := cs.benchmarkMySQLToolRestore(ctx, dbName, dumpFile, dataSize)
	if err != nil {
		return nil, fmt.Errorf("tool restore benchmark: %w", err)
	}
	_ = CleanupMySQLDataset(ctx, cs.mysqlDB, dbName)

	result := &ComparisonResult{
		Database:     DatabaseBackendMySQL,
		DataSize:     size,
		Operation:    "restore",
		NativeResult: nativeResult,
		ToolResult:   toolResult,
	}

	if toolResult.Throughput > 0 {
		result.SpeedupRatio = nativeResult.Throughput / toolResult.Throughput
	}
	if nativeResult.AllocBytes > 0 {
		result.MemoryRatio = float64(toolResult.AllocBytes) / float64(nativeResult.AllocBytes)
	}

	cs.results = append(cs.results, *result)
	return result, nil
}

// RunPostgreSQLBackupComparison runs backup comparison for PostgreSQL
func (cs *ComparisonSuite) RunPostgreSQLBackupComparison(ctx context.Context, size DataSize) (*ComparisonResult, error) {
	if cs.pgDB == nil {
		return nil, fmt.Errorf("PostgreSQL not connected")
	}

	spec := GetDatasetSpec(size)

	// Create test dataset
	if err := CreatePostgreSQLDataset(ctx, cs.pgDB, spec); err != nil {
		return nil, fmt.Errorf("creating dataset: %w", err)
	}
	defer func() { _ = CleanupPostgreSQLDataset(ctx, cs.pgDB, spec.NumTables) }()

	dataSize := spec.ApproxSizeBytes()

	// Benchmark native backup
	nativeResult, err := cs.benchmarkPGNativeBackup(ctx, dataSize)
	if err != nil {
		return nil, fmt.Errorf("native backup benchmark: %w", err)
	}

	// Benchmark tool-based backup (using pg_dump)
	toolResult, err := cs.benchmarkPGToolBackup(ctx, dataSize)
	if err != nil {
		return nil, fmt.Errorf("tool backup benchmark: %w", err)
	}

	result := &ComparisonResult{
		Database:     DatabaseBackendPostgreSQL,
		DataSize:     size,
		Operation:    "backup",
		NativeResult: nativeResult,
		ToolResult:   toolResult,
	}

	if toolResult.Throughput > 0 {
		result.SpeedupRatio = nativeResult.Throughput / toolResult.Throughput
	}
	if nativeResult.AllocBytes > 0 {
		result.MemoryRatio = float64(toolResult.AllocBytes) / float64(nativeResult.AllocBytes)
	}

	cs.results = append(cs.results, *result)
	return result, nil
}

// RunPostgreSQLRestoreComparison runs restore comparison for PostgreSQL
func (cs *ComparisonSuite) RunPostgreSQLRestoreComparison(ctx context.Context, size DataSize) (*ComparisonResult, error) {
	if cs.pgDB == nil {
		return nil, fmt.Errorf("PostgreSQL not connected")
	}

	spec := GetDatasetSpec(size)

	// Create test dataset and dump
	if err := CreatePostgreSQLDataset(ctx, cs.pgDB, spec); err != nil {
		return nil, fmt.Errorf("creating dataset: %w", err)
	}

	dumpFile := filepath.Join(cs.outputDir, fmt.Sprintf("pg_bench_%s.sql", size))
	if err := cs.createPGDump(ctx, dumpFile); err != nil {
		_ = CleanupPostgreSQLDataset(ctx, cs.pgDB, spec.NumTables)
		return nil, fmt.Errorf("creating dump: %w", err)
	}
	defer func() { _ = os.Remove(dumpFile) }()

	dumpInfo, _ := os.Stat(dumpFile)
	dataSize := dumpInfo.Size()

	_ = CleanupPostgreSQLDataset(ctx, cs.pgDB, spec.NumTables)

	// Benchmark native restore
	nativeResult, err := cs.benchmarkPGNativeRestore(ctx, dumpFile, dataSize)
	if err != nil {
		return nil, fmt.Errorf("native restore benchmark: %w", err)
	}
	_ = CleanupPostgreSQLDataset(ctx, cs.pgDB, spec.NumTables)

	// Benchmark tool-based restore
	toolResult, err := cs.benchmarkPGToolRestore(ctx, dumpFile, dataSize)
	if err != nil {
		return nil, fmt.Errorf("tool restore benchmark: %w", err)
	}
	_ = CleanupPostgreSQLDataset(ctx, cs.pgDB, spec.NumTables)

	result := &ComparisonResult{
		Database:     DatabaseBackendPostgreSQL,
		DataSize:     size,
		Operation:    "restore",
		NativeResult: nativeResult,
		ToolResult:   toolResult,
	}

	if toolResult.Throughput > 0 {
		result.SpeedupRatio = nativeResult.Throughput / toolResult.Throughput
	}
	if nativeResult.AllocBytes > 0 {
		result.MemoryRatio = float64(toolResult.AllocBytes) / float64(nativeResult.AllocBytes)
	}

	cs.results = append(cs.results, *result)
	return result, nil
}

// Results returns all comparison results
func (cs *ComparisonSuite) Results() []ComparisonResult {
	return cs.results
}

// ── MySQL benchmark implementations ──────────────────────────────────────────

func (cs *ComparisonSuite) benchmarkMySQLNativeBackup(ctx context.Context, dbName string, _ int64) (*BenchmarkResult, error) {
	mc := NewMetricsCollector()
	mc.Start()

	// Native approach: SELECT * FROM each table, write rows
	outputFile := filepath.Join(cs.outputDir, "native_mysql_backup.sql")
	f, err := os.Create(outputFile)
	if err != nil {
		mc.Stop("mysql_native_backup", "backup", 0)
		return nil, err
	}
	defer func() {
		_ = f.Close()
		_ = os.Remove(outputFile)
	}()

	tables, err := cs.getMySQLTables(ctx, dbName)
	if err != nil {
		mc.Stop("mysql_native_backup", "backup", 0)
		return nil, err
	}

	var totalBytes int64
	for _, table := range tables {
		n, err := cs.dumpMySQLTableNative(ctx, cs.mysqlDB, dbName, table, f)
		if err != nil {
			mc.Stop("mysql_native_backup", "backup", 0)
			return nil, fmt.Errorf("dumping table %s: %w", table, err)
		}
		totalBytes += n
		mc.RecordWrite(n)
	}

	return mc.Stop("mysql_native_backup", "backup", totalBytes), nil
}

func (cs *ComparisonSuite) benchmarkMySQLToolBackup(ctx context.Context, dbName string, _ int64) (*BenchmarkResult, error) {
	mc := NewMetricsCollector()
	mc.Start()

	outputFile := filepath.Join(cs.outputDir, "tool_mysql_backup.sql")
	f, err := os.Create(outputFile)
	if err != nil {
		mc.Stop("mysql_tool_backup", "backup", 0)
		return nil, err
	}
	defer func() {
		_ = f.Close()
		_ = os.Remove(outputFile)
	}()

	cmd := exec.CommandContext(ctx, "mysqldump",
		"--single-transaction",
		"--routines",
		"--triggers",
		"--no-tablespaces",
		dbName,
	)
	cmd.Stdout = f

	if err := cmd.Run(); err != nil {
		mc.Stop("mysql_tool_backup", "backup", 0)
		return nil, fmt.Errorf("mysqldump failed: %w", err)
	}

	info, _ := os.Stat(outputFile)
	totalBytes := info.Size()
	mc.RecordWrite(totalBytes)

	return mc.Stop("mysql_tool_backup", "backup", totalBytes), nil
}

func (cs *ComparisonSuite) benchmarkMySQLNativeRestore(ctx context.Context, dbName, dumpFile string, _ int64) (*BenchmarkResult, error) {
	mc := NewMetricsCollector()
	mc.Start()

	// Ensure database exists
	if _, err := cs.mysqlDB.ExecContext(ctx, fmt.Sprintf("CREATE DATABASE IF NOT EXISTS `%s`", dbName)); err != nil {
		mc.Stop("mysql_native_restore", "restore", 0)
		return nil, err
	}

	// Read and execute SQL statements
	f, err := os.Open(dumpFile)
	if err != nil {
		mc.Stop("mysql_native_restore", "restore", 0)
		return nil, err
	}
	defer func() { _ = f.Close() }()

	data, err := io.ReadAll(f)
	if err != nil {
		mc.Stop("mysql_native_restore", "restore", 0)
		return nil, err
	}
	mc.RecordRead(int64(len(data)))

	if _, err := cs.mysqlDB.ExecContext(ctx, fmt.Sprintf("USE `%s`", dbName)); err != nil {
		mc.Stop("mysql_native_restore", "restore", 0)
		return nil, err
	}

	// Execute the dump (simplified: send entire dump as one execution)
	if _, err := cs.mysqlDB.ExecContext(ctx, string(data)); err != nil {
		// Try statement-by-statement for multi-statement dumps
		stmts := splitSQLStatements(string(data))
		for _, stmt := range stmts {
			if stmt == "" {
				continue
			}
			if _, execErr := cs.mysqlDB.ExecContext(ctx, stmt); execErr != nil {
				// Log but continue - some statements may fail in isolation
				continue
			}
		}
	}

	return mc.Stop("mysql_native_restore", "restore", int64(len(data))), nil
}

func (cs *ComparisonSuite) benchmarkMySQLToolRestore(ctx context.Context, dbName, dumpFile string, _ int64) (*BenchmarkResult, error) {
	mc := NewMetricsCollector()
	mc.Start()

	// Ensure database exists
	if _, err := cs.mysqlDB.ExecContext(ctx, fmt.Sprintf("CREATE DATABASE IF NOT EXISTS `%s`", dbName)); err != nil {
		mc.Stop("mysql_tool_restore", "restore", 0)
		return nil, err
	}

	f, err := os.Open(dumpFile)
	if err != nil {
		mc.Stop("mysql_tool_restore", "restore", 0)
		return nil, err
	}
	defer func() { _ = f.Close() }()

	fi, _ := f.Stat()
	fileSize := fi.Size()

	cmd := exec.CommandContext(ctx, "mysql", dbName)
	cmd.Stdin = f

	if err := cmd.Run(); err != nil {
		mc.Stop("mysql_tool_restore", "restore", 0)
		return nil, fmt.Errorf("mysql client restore failed: %w", err)
	}

	mc.RecordRead(fileSize)
	return mc.Stop("mysql_tool_restore", "restore", fileSize), nil
}

// ── PostgreSQL benchmark implementations ─────────────────────────────────────

func (cs *ComparisonSuite) benchmarkPGNativeBackup(ctx context.Context, _ int64) (*BenchmarkResult, error) {
	mc := NewMetricsCollector()
	mc.Start()

	outputFile := filepath.Join(cs.outputDir, "native_pg_backup.sql")
	f, err := os.Create(outputFile)
	if err != nil {
		mc.Stop("pg_native_backup", "backup", 0)
		return nil, err
	}
	defer func() {
		_ = f.Close()
		_ = os.Remove(outputFile)
	}()

	// Native: COPY each table TO STDOUT
	tables, err := cs.getPGTables(ctx)
	if err != nil {
		mc.Stop("pg_native_backup", "backup", 0)
		return nil, err
	}

	var totalBytes int64
	for _, table := range tables {
		n, err := cs.dumpPGTableNative(ctx, table, f)
		if err != nil {
			mc.Stop("pg_native_backup", "backup", 0)
			return nil, fmt.Errorf("dumping table %s: %w", table, err)
		}
		totalBytes += n
		mc.RecordWrite(n)
	}

	return mc.Stop("pg_native_backup", "backup", totalBytes), nil
}

func (cs *ComparisonSuite) benchmarkPGToolBackup(ctx context.Context, _ int64) (*BenchmarkResult, error) {
	mc := NewMetricsCollector()
	mc.Start()

	outputFile := filepath.Join(cs.outputDir, "tool_pg_backup.sql")
	f, err := os.Create(outputFile)
	if err != nil {
		mc.Stop("pg_tool_backup", "backup", 0)
		return nil, err
	}
	defer func() {
		_ = f.Close()
		_ = os.Remove(outputFile)
	}()

	cmd := exec.CommandContext(ctx, "pg_dump",
		"--no-owner",
		"--no-privileges",
	)
	cmd.Stdout = f

	if err := cmd.Run(); err != nil {
		mc.Stop("pg_tool_backup", "backup", 0)
		return nil, fmt.Errorf("pg_dump failed: %w", err)
	}

	info, _ := os.Stat(outputFile)
	totalBytes := info.Size()
	mc.RecordWrite(totalBytes)

	return mc.Stop("pg_tool_backup", "backup", totalBytes), nil
}

func (cs *ComparisonSuite) benchmarkPGNativeRestore(ctx context.Context, dumpFile string, _ int64) (*BenchmarkResult, error) {
	mc := NewMetricsCollector()
	mc.Start()

	f, err := os.Open(dumpFile)
	if err != nil {
		mc.Stop("pg_native_restore", "restore", 0)
		return nil, err
	}
	defer func() { _ = f.Close() }()

	data, err := io.ReadAll(f)
	if err != nil {
		mc.Stop("pg_native_restore", "restore", 0)
		return nil, err
	}
	mc.RecordRead(int64(len(data)))

	stmts := splitSQLStatements(string(data))
	for _, stmt := range stmts {
		if stmt == "" {
			continue
		}
		if _, err := cs.pgDB.ExecContext(ctx, stmt); err != nil {
			continue // Skip failures in isolation
		}
	}

	return mc.Stop("pg_native_restore", "restore", int64(len(data))), nil
}

func (cs *ComparisonSuite) benchmarkPGToolRestore(ctx context.Context, dumpFile string, _ int64) (*BenchmarkResult, error) {
	mc := NewMetricsCollector()
	mc.Start()

	f, err := os.Open(dumpFile)
	if err != nil {
		mc.Stop("pg_tool_restore", "restore", 0)
		return nil, err
	}
	defer func() { _ = f.Close() }()

	fi, _ := f.Stat()
	fileSize := fi.Size()

	cmd := exec.CommandContext(ctx, "psql")
	cmd.Stdin = f

	if err := cmd.Run(); err != nil {
		mc.Stop("pg_tool_restore", "restore", 0)
		return nil, fmt.Errorf("psql restore failed: %w", err)
	}

	mc.RecordRead(fileSize)
	return mc.Stop("pg_tool_restore", "restore", fileSize), nil
}

// ── Helper methods ───────────────────────────────────────────────────────────

func (cs *ComparisonSuite) getMySQLTables(ctx context.Context, dbName string) ([]string, error) {
	rows, err := cs.mysqlDB.QueryContext(ctx,
		"SELECT TABLE_NAME FROM information_schema.TABLES WHERE TABLE_SCHEMA = ?", dbName)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	var tables []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, err
		}
		tables = append(tables, name)
	}
	return tables, rows.Err()
}

func (cs *ComparisonSuite) getPGTables(ctx context.Context) ([]string, error) {
	rows, err := cs.pgDB.QueryContext(ctx,
		"SELECT tablename FROM pg_tables WHERE schemaname = 'public' AND tablename LIKE 'bench_%'")
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	var tables []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, err
		}
		tables = append(tables, name)
	}
	return tables, rows.Err()
}

func (cs *ComparisonSuite) dumpMySQLTableNative(ctx context.Context, db *sql.DB, dbName, table string, w io.Writer) (int64, error) {
	rows, err := db.QueryContext(ctx, fmt.Sprintf("SELECT * FROM `%s`.`%s`", dbName, table))
	if err != nil {
		return 0, err
	}
	defer func() { _ = rows.Close() }()

	cols, err := rows.Columns()
	if err != nil {
		return 0, err
	}

	var totalBytes int64
	values := make([]any, len(cols))
	valuePtrs := make([]any, len(cols))
	for i := range values {
		valuePtrs[i] = &values[i]
	}

	for rows.Next() {
		if err := rows.Scan(valuePtrs...); err != nil {
			return totalBytes, err
		}
		// Write as tab-separated values (simplified dump)
		for i, val := range values {
			if i > 0 {
				n, _ := fmt.Fprint(w, "\t")
				totalBytes += int64(n)
			}
			n, _ := fmt.Fprintf(w, "%v", val)
			totalBytes += int64(n)
		}
		n, _ := fmt.Fprintln(w)
		totalBytes += int64(n)
	}

	return totalBytes, rows.Err()
}

func (cs *ComparisonSuite) dumpPGTableNative(ctx context.Context, table string, w io.Writer) (int64, error) {
	rows, err := cs.pgDB.QueryContext(ctx, fmt.Sprintf("SELECT * FROM %s", table))
	if err != nil {
		return 0, err
	}
	defer func() { _ = rows.Close() }()

	cols, err := rows.Columns()
	if err != nil {
		return 0, err
	}

	var totalBytes int64
	values := make([]any, len(cols))
	valuePtrs := make([]any, len(cols))
	for i := range values {
		valuePtrs[i] = &values[i]
	}

	for rows.Next() {
		if err := rows.Scan(valuePtrs...); err != nil {
			return totalBytes, err
		}
		for i, val := range values {
			if i > 0 {
				n, _ := fmt.Fprint(w, "\t")
				totalBytes += int64(n)
			}
			n, _ := fmt.Fprintf(w, "%v", val)
			totalBytes += int64(n)
		}
		n, _ := fmt.Fprintln(w)
		totalBytes += int64(n)
	}

	return totalBytes, rows.Err()
}

func (cs *ComparisonSuite) createMySQLDump(ctx context.Context, dbName, outputFile string) error {
	f, err := os.Create(outputFile)
	if err != nil {
		return err
	}
	defer func() { _ = f.Close() }()

	cmd := exec.CommandContext(ctx, "mysqldump",
		"--single-transaction",
		"--no-tablespaces",
		dbName,
	)
	cmd.Stdout = f
	return cmd.Run()
}

func (cs *ComparisonSuite) createPGDump(ctx context.Context, outputFile string) error {
	f, err := os.Create(outputFile)
	if err != nil {
		return err
	}
	defer func() { _ = f.Close() }()

	cmd := exec.CommandContext(ctx, "pg_dump",
		"--no-owner",
		"--no-privileges",
	)
	cmd.Stdout = f
	return cmd.Run()
}

// splitSQLStatements splits a SQL dump into individual statements
func splitSQLStatements(dump string) []string {
	var stmts []string
	var current []byte
	inString := false
	escape := false

	for i := 0; i < len(dump); i++ {
		ch := dump[i]

		if escape {
			current = append(current, ch)
			escape = false
			continue
		}

		if ch == '\\' && inString {
			current = append(current, ch)
			escape = true
			continue
		}

		if ch == '\'' {
			inString = !inString
		}

		if ch == ';' && !inString {
			stmt := string(current)
			if len(stmt) > 0 {
				stmts = append(stmts, stmt)
			}
			current = current[:0]
			continue
		}

		current = append(current, ch)
	}

	if len(current) > 0 {
		stmt := string(current)
		if len(stmt) > 0 {
			stmts = append(stmts, stmt)
		}
	}

	return stmts
}

