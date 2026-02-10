package native

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"dbbackup/internal/logger"

	"github.com/jackc/pgx/v5/pgxpool"
)

// ═══════════════════════════════════════════════════════════════════════════════
// SMART TRANSACTION BATCHER
// ═══════════════════════════════════════════════════════════════════════════════
//
// Collects DDL statements and executes them in optimal transaction batches.
// Goals:
//   - Reduce round-trips: batch 50-200 small DDL statements per transaction
//   - Respect dependencies: CREATE TABLE before ALTER TABLE on same table
//   - Fail gracefully: if one statement fails, report it and continue
//
// pg_restore executes one statement per transaction in --single-transaction
// mode, or wraps everything in one giant transaction. Both are suboptimal.
// ═══════════════════════════════════════════════════════════════════════════════

// BatchConfig controls batching behavior
type BatchConfig struct {
	// MaxBatchSize is the maximum number of statements per transaction
	MaxBatchSize int
	// MaxBatchBytes is the maximum total SQL bytes per transaction
	MaxBatchBytes int64
	// ContinueOnError controls whether to continue after a statement fails
	ContinueOnError bool
	// ParallelBatches controls independent batch parallel execution
	ParallelBatches int
}

// DefaultBatchConfig returns sensible defaults
func DefaultBatchConfig() BatchConfig {
	return BatchConfig{
		MaxBatchSize:    100,
		MaxBatchBytes:   5 * 1024 * 1024, // 5MB
		ContinueOnError: true,
		ParallelBatches: 1,
	}
}

// TransactionBatcher groups and executes SQL statements in optimal batches
type TransactionBatcher struct {
	pool   *pgxpool.Pool
	log    logger.Logger
	config BatchConfig

	// Stats
	batchCount    atomic.Int64
	stmtCount     atomic.Int64
	errorCount    atomic.Int64
	totalDuration atomic.Int64 // nanoseconds
}

// BatchResult contains execution results for a batch
type BatchResult struct {
	BatchID    int
	Statements int
	Duration   time.Duration
	Errors     []BatchError
	Success    bool
}

// BatchError records a failed statement within a batch
type BatchError struct {
	SQL      string
	Error    string
	Position int // Position within batch
}

// TransactionBatcherResult contains aggregate results
type TransactionBatcherResult struct {
	TotalBatches   int
	TotalStatements int
	Successful     int
	Failed         int
	TotalDuration  time.Duration
	BatchResults   []BatchResult
}

// NewTransactionBatcher creates a new batcher
func NewTransactionBatcher(pool *pgxpool.Pool, log logger.Logger, config BatchConfig) *TransactionBatcher {
	if config.MaxBatchSize == 0 {
		config = DefaultBatchConfig()
	}
	return &TransactionBatcher{
		pool:   pool,
		log:    log,
		config: config,
	}
}

// ExecuteStatements executes a slice of SQL statements in optimal batches.
// Statements are grouped to respect dependencies.
func (b *TransactionBatcher) ExecuteStatements(ctx context.Context, statements []string) *TransactionBatcherResult {
	start := time.Now()
	result := &TransactionBatcherResult{}

	if len(statements) == 0 {
		return result
	}

	// Group statements into dependency-respecting batches
	batches := b.buildBatches(statements)
	result.TotalBatches = len(batches)
	result.TotalStatements = len(statements)

	b.log.Info("Transaction batcher starting",
		"total_statements", len(statements),
		"batches", len(batches),
		"max_batch_size", b.config.MaxBatchSize)

	for i, batch := range batches {
		select {
		case <-ctx.Done():
			return result
		default:
		}

		br := b.executeBatch(ctx, i, batch)
		result.BatchResults = append(result.BatchResults, br)

		if br.Success {
			result.Successful += br.Statements
		} else {
			result.Failed += len(br.Errors)
			result.Successful += br.Statements - len(br.Errors)
		}

		b.batchCount.Add(1)
	}

	result.TotalDuration = time.Since(start)

	b.log.Info("Transaction batcher complete",
		"batches", result.TotalBatches,
		"statements", result.TotalStatements,
		"successful", result.Successful,
		"failed", result.Failed,
		"duration", result.TotalDuration)

	return result
}

// ExecuteStatementsParallel executes independent batches in parallel
func (b *TransactionBatcher) ExecuteStatementsParallel(ctx context.Context, statements []string) *TransactionBatcherResult {
	start := time.Now()
	result := &TransactionBatcherResult{}

	if len(statements) == 0 {
		return result
	}

	batches := b.buildBatches(statements)
	result.TotalBatches = len(batches)
	result.TotalStatements = len(statements)

	workers := b.config.ParallelBatches
	if workers < 1 {
		workers = 1
	}
	if workers > len(batches) {
		workers = len(batches)
	}

	b.log.Info("Transaction batcher starting (parallel)",
		"total_statements", len(statements),
		"batches", len(batches),
		"workers", workers)

	batchCh := make(chan indexedBatch, len(batches))
	resultCh := make(chan BatchResult, len(batches))

	for i, batch := range batches {
		batchCh <- indexedBatch{index: i, stmts: batch}
	}
	close(batchCh)

	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for ib := range batchCh {
				select {
				case <-ctx.Done():
					return
				default:
				}
				resultCh <- b.executeBatch(ctx, ib.index, ib.stmts)
			}
		}()
	}

	go func() {
		wg.Wait()
		close(resultCh)
	}()

	for br := range resultCh {
		result.BatchResults = append(result.BatchResults, br)
		if br.Success {
			result.Successful += br.Statements
		} else {
			result.Failed += len(br.Errors)
			result.Successful += br.Statements - len(br.Errors)
		}
	}

	// Sort results by batch ID for deterministic output
	sort.Slice(result.BatchResults, func(i, j int) bool {
		return result.BatchResults[i].BatchID < result.BatchResults[j].BatchID
	})

	result.TotalDuration = time.Since(start)

	b.log.Info("Transaction batcher complete (parallel)",
		"batches", result.TotalBatches,
		"statements", result.TotalStatements,
		"successful", result.Successful,
		"failed", result.Failed,
		"duration", result.TotalDuration)

	return result
}

type indexedBatch struct {
	index int
	stmts []string
}

// buildBatches groups statements into dependency-respecting batches
func (b *TransactionBatcher) buildBatches(statements []string) [][]string {
	var batches [][]string
	var currentBatch []string
	var currentBytes int64

	for _, sql := range statements {
		sqlBytes := int64(len(sql))

		// Start new batch if current is full
		if len(currentBatch) >= b.config.MaxBatchSize ||
			(b.config.MaxBatchBytes > 0 && currentBytes+sqlBytes > b.config.MaxBatchBytes) {
			if len(currentBatch) > 0 {
				batches = append(batches, currentBatch)
				currentBatch = nil
				currentBytes = 0
			}
		}

		currentBatch = append(currentBatch, sql)
		currentBytes += sqlBytes
	}

	if len(currentBatch) > 0 {
		batches = append(batches, currentBatch)
	}

	return batches
}

// executeBatch executes a single batch of statements in a transaction
func (b *TransactionBatcher) executeBatch(ctx context.Context, batchID int, statements []string) BatchResult {
	start := time.Now()
	result := BatchResult{
		BatchID:    batchID,
		Statements: len(statements),
	}

	conn, err := b.pool.Acquire(ctx)
	if err != nil {
		result.Errors = append(result.Errors, BatchError{
			Error: fmt.Sprintf("failed to acquire connection: %v", err),
		})
		result.Duration = time.Since(start)
		return result
	}
	defer conn.Release()

	if b.config.ContinueOnError {
		// Execute each statement individually, collecting errors
		for i, sql := range statements {
			if _, execErr := conn.Exec(ctx, sql); execErr != nil {
				b.errorCount.Add(1)
				result.Errors = append(result.Errors, BatchError{
					SQL:      truncateSQL(sql, 200),
					Error:    execErr.Error(),
					Position: i,
				})
				b.log.Debug("Statement failed in batch",
					"batch", batchID,
					"position", i,
					"error", execErr)
			} else {
				b.stmtCount.Add(1)
			}
		}
	} else {
		// Execute all in a single transaction
		tx, txErr := conn.Begin(ctx)
		if txErr != nil {
			result.Errors = append(result.Errors, BatchError{
				Error: fmt.Sprintf("failed to begin transaction: %v", txErr),
			})
			result.Duration = time.Since(start)
			return result
		}

		for i, sql := range statements {
			if _, execErr := tx.Exec(ctx, sql); execErr != nil {
				_ = tx.Rollback(ctx)
				result.Errors = append(result.Errors, BatchError{
					SQL:      truncateSQL(sql, 200),
					Error:    execErr.Error(),
					Position: i,
				})
				result.Duration = time.Since(start)
				return result
			}
		}

		if commitErr := tx.Commit(ctx); commitErr != nil {
			result.Errors = append(result.Errors, BatchError{
				Error: fmt.Sprintf("failed to commit: %v", commitErr),
			})
			result.Duration = time.Since(start)
			return result
		}
		b.stmtCount.Add(int64(len(statements)))
	}

	result.Duration = time.Since(start)
	result.Success = len(result.Errors) == 0

	elapsed := time.Since(start)
	b.totalDuration.Add(int64(elapsed))

	b.log.Debug("Batch executed",
		"batch", batchID,
		"statements", len(statements),
		"errors", len(result.Errors),
		"duration", elapsed)

	return result
}

func truncateSQL(sql string, maxLen int) string {
	if len(sql) <= maxLen {
		return sql
	}
	return sql[:maxLen] + "..."
}

// ═══════════════════════════════════════════════════════════════════════════════
// CONSTRAINT OPTIMIZER
// ═══════════════════════════════════════════════════════════════════════════════
//
// Optimizes constraint application during restore:
// 1. Drop all constraints before data load
// 2. Load data (parallel COPY)
// 3. Re-create constraints in optimal order:
//    - PRIMARY KEY first (required by FOREIGN KEY references)
//    - UNIQUE constraints (may be referenced by FK)
//    - FOREIGN KEY constraints last
//    - CHECK constraints can be parallel with anything
//
// Additionally, validates constraints in parallel after creation using
// ALTER TABLE ... VALIDATE CONSTRAINT for deferred FK constraints.
// ═══════════════════════════════════════════════════════════════════════════════

// ConstraintType classifies constraint types for ordering
type ConstraintType int

const (
	ConstraintPrimaryKey ConstraintType = iota
	ConstraintUnique
	ConstraintForeignKey
	ConstraintCheck
	ConstraintExclusion
	ConstraintOther
)

func (ct ConstraintType) String() string {
	switch ct {
	case ConstraintPrimaryKey:
		return "PRIMARY KEY"
	case ConstraintUnique:
		return "UNIQUE"
	case ConstraintForeignKey:
		return "FOREIGN KEY"
	case ConstraintCheck:
		return "CHECK"
	case ConstraintExclusion:
		return "EXCLUSION"
	default:
		return "OTHER"
	}
}

// ConstraintJob represents a constraint to create/validate
type ConstraintJob struct {
	Table          string
	Schema         string
	ConstraintName string
	Definition     string // Full ALTER TABLE ... ADD CONSTRAINT statement
	Type           ConstraintType
	Priority       int    // Lower = create first
	DependsOn      []string // Constraint names this depends on
}

// ConstraintOptimizer manages constraint ordering and parallel validation
type ConstraintOptimizer struct {
	pool    *pgxpool.Pool
	log     logger.Logger
	workers int
}

// ConstraintOptimizerResult contains the results of constraint optimization
type ConstraintOptimizerResult struct {
	TotalConstraints int
	Created          int
	Failed           int
	Validated        int
	Duration         time.Duration
	Phases           []ConstraintPhaseResult
}

// ConstraintPhaseResult contains results for one phase of constraint creation
type ConstraintPhaseResult struct {
	Phase       string
	Constraints int
	Duration    time.Duration
	Errors      []string
}

// NewConstraintOptimizer creates a new constraint optimizer
func NewConstraintOptimizer(pool *pgxpool.Pool, log logger.Logger, workers int) *ConstraintOptimizer {
	if workers < 1 {
		workers = 4
	}
	return &ConstraintOptimizer{
		pool:    pool,
		log:     log,
		workers: workers,
	}
}

// ClassifyConstraints sorts constraint statements into typed jobs with dependency ordering
func (o *ConstraintOptimizer) ClassifyConstraints(statements []string) []ConstraintJob {
	var jobs []ConstraintJob

	for _, sql := range statements {
		job := ConstraintJob{
			Definition: sql,
			Type:       classifyConstraintType(sql),
			Table:      extractConstraintTable(sql),
			ConstraintName: extractConstraintName(sql),
		}
		job.Priority = constraintPriority(job.Type)
		jobs = append(jobs, job)
	}

	// Sort by priority (PK first, FK last)
	sort.SliceStable(jobs, func(i, j int) bool {
		return jobs[i].Priority < jobs[j].Priority
	})

	return jobs
}

// ApplyConstraints creates constraints in optimal order using parallel workers
func (o *ConstraintOptimizer) ApplyConstraints(ctx context.Context, jobs []ConstraintJob) *ConstraintOptimizerResult {
	start := time.Now()
	result := &ConstraintOptimizerResult{
		TotalConstraints: len(jobs),
	}

	if len(jobs) == 0 {
		result.Duration = time.Since(start)
		return result
	}

	// Phase 1: Primary keys (serial — required before FK)
	pkJobs := filterConstraintType(jobs, ConstraintPrimaryKey)
	pkResult := o.executePhase(ctx, "PRIMARY KEY", pkJobs, 1) // Serial for PK
	result.Phases = append(result.Phases, pkResult)
	result.Created += pkResult.Constraints - len(pkResult.Errors)
	result.Failed += len(pkResult.Errors)

	// Phase 2: Unique constraints (can be parallel)
	uqJobs := filterConstraintType(jobs, ConstraintUnique)
	uqResult := o.executePhase(ctx, "UNIQUE", uqJobs, o.workers)
	result.Phases = append(result.Phases, uqResult)
	result.Created += uqResult.Constraints - len(uqResult.Errors)
	result.Failed += len(uqResult.Errors)

	// Phase 3: Check constraints (fully parallel, no dependencies)
	ckJobs := filterConstraintType(jobs, ConstraintCheck)
	ckResult := o.executePhase(ctx, "CHECK", ckJobs, o.workers)
	result.Phases = append(result.Phases, ckResult)
	result.Created += ckResult.Constraints - len(ckResult.Errors)
	result.Failed += len(ckResult.Errors)

	// Phase 4: Foreign keys (parallel, but after PK/UNIQUE)
	fkJobs := filterConstraintType(jobs, ConstraintForeignKey)
	fkResult := o.executePhase(ctx, "FOREIGN KEY", fkJobs, o.workers)
	result.Phases = append(result.Phases, fkResult)
	result.Created += fkResult.Constraints - len(fkResult.Errors)
	result.Failed += len(fkResult.Errors)

	// Phase 5: Exclusion and other constraints
	otherJobs := filterConstraintTypes(jobs, ConstraintExclusion, ConstraintOther)
	if len(otherJobs) > 0 {
		otherResult := o.executePhase(ctx, "OTHER", otherJobs, o.workers)
		result.Phases = append(result.Phases, otherResult)
		result.Created += otherResult.Constraints - len(otherResult.Errors)
		result.Failed += len(otherResult.Errors)
	}

	result.Duration = time.Since(start)

	o.log.Info("Constraint optimization complete",
		"total", result.TotalConstraints,
		"created", result.Created,
		"failed", result.Failed,
		"duration", result.Duration)

	return result
}

// executePhase executes one phase of constraint creation
func (o *ConstraintOptimizer) executePhase(ctx context.Context, phaseName string, jobs []ConstraintJob, workers int) ConstraintPhaseResult {
	start := time.Now()
	result := ConstraintPhaseResult{
		Phase:       phaseName,
		Constraints: len(jobs),
	}

	if len(jobs) == 0 {
		result.Duration = time.Since(start)
		return result
	}

	o.log.Info("Constraint phase starting",
		"phase", phaseName,
		"constraints", len(jobs),
		"workers", workers)

	if workers <= 1 {
		// Serial execution
		for _, job := range jobs {
			if err := o.executeConstraint(ctx, job); err != nil {
				result.Errors = append(result.Errors,
					fmt.Sprintf("%s.%s: %v", job.Table, job.ConstraintName, err))
			}
		}
	} else {
		// Parallel execution
		jobCh := make(chan ConstraintJob, len(jobs))
		errCh := make(chan string, len(jobs))

		for _, job := range jobs {
			jobCh <- job
		}
		close(jobCh)

		var wg sync.WaitGroup
		for w := 0; w < workers; w++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for job := range jobCh {
					if err := o.executeConstraint(ctx, job); err != nil {
						errCh <- fmt.Sprintf("%s.%s: %v", job.Table, job.ConstraintName, err)
					}
				}
			}()
		}

		wg.Wait()
		close(errCh)

		for e := range errCh {
			result.Errors = append(result.Errors, e)
		}
	}

	result.Duration = time.Since(start)

	o.log.Info("Constraint phase complete",
		"phase", phaseName,
		"constraints", len(jobs),
		"errors", len(result.Errors),
		"duration", result.Duration)

	return result
}

// executeConstraint executes a single constraint DDL statement
func (o *ConstraintOptimizer) executeConstraint(ctx context.Context, job ConstraintJob) error {
	conn, err := o.pool.Acquire(ctx)
	if err != nil {
		return fmt.Errorf("acquire connection: %w", err)
	}
	defer conn.Release()

	// Set a generous timeout for constraint creation
	_, _ = conn.Exec(ctx, "SET statement_timeout = '1800s'") // 30 min

	_, err = conn.Exec(ctx, job.Definition)
	if err != nil {
		return fmt.Errorf("execute constraint %s on %s: %w", job.ConstraintName, job.Table, err)
	}

	o.log.Debug("Constraint created",
		"table", job.Table,
		"constraint", job.ConstraintName,
		"type", job.Type)

	return nil
}

// ValidateConstraints runs ALTER TABLE ... VALIDATE CONSTRAINT on NOT VALID constraints
func (o *ConstraintOptimizer) ValidateConstraints(ctx context.Context) (int, error) {
	conn, err := o.pool.Acquire(ctx)
	if err != nil {
		return 0, fmt.Errorf("acquire connection: %w", err)
	}
	defer conn.Release()

	// Find constraints marked as NOT VALID
	rows, err := conn.Query(ctx, `
		SELECT 
			n.nspname AS schema,
			c.relname AS table_name,
			con.conname AS constraint_name
		FROM pg_constraint con
		JOIN pg_class c ON c.oid = con.conrelid
		JOIN pg_namespace n ON n.oid = c.relnamespace
		WHERE NOT con.convalidated
		AND n.nspname NOT IN ('pg_catalog', 'information_schema')
	`)
	if err != nil {
		return 0, fmt.Errorf("query not-valid constraints: %w", err)
	}
	defer rows.Close()

	type validateJob struct {
		schema, table, constraint string
	}
	var validateJobs []validateJob

	for rows.Next() {
		var j validateJob
		if err := rows.Scan(&j.schema, &j.table, &j.constraint); err != nil {
			continue
		}
		validateJobs = append(validateJobs, j)
	}

	if len(validateJobs) == 0 {
		return 0, nil
	}

	o.log.Info("Validating NOT VALID constraints", "count", len(validateJobs))

	var validated atomic.Int64
	var wg sync.WaitGroup
	sem := make(chan struct{}, o.workers)

	for _, job := range validateJobs {
		wg.Add(1)
		sem <- struct{}{}
		go func(j validateJob) {
			defer wg.Done()
			defer func() { <-sem }()

			vConn, vErr := o.pool.Acquire(ctx)
			if vErr != nil {
				o.log.Warn("Failed to acquire connection for validation",
					"constraint", j.constraint, "error", vErr)
				return
			}
			defer vConn.Release()

			sql := fmt.Sprintf("ALTER TABLE %s.%s VALIDATE CONSTRAINT %s",
				quoteIdentifier(j.schema), quoteIdentifier(j.table), quoteIdentifier(j.constraint))
			if _, vErr = vConn.Exec(ctx, sql); vErr != nil {
				o.log.Warn("Constraint validation failed",
					"constraint", j.constraint,
					"table", j.table,
					"error", vErr)
			} else {
				validated.Add(1)
			}
		}(job)
	}

	wg.Wait()

	return int(validated.Load()), nil
}

// Helper functions

func classifyConstraintType(sql string) ConstraintType {
	upper := strings.ToUpper(sql)
	switch {
	case strings.Contains(upper, "PRIMARY KEY"):
		return ConstraintPrimaryKey
	case strings.Contains(upper, "UNIQUE"):
		return ConstraintUnique
	case strings.Contains(upper, "FOREIGN KEY") || strings.Contains(upper, "REFERENCES"):
		return ConstraintForeignKey
	case strings.Contains(upper, "CHECK"):
		return ConstraintCheck
	case strings.Contains(upper, "EXCLUDE") || strings.Contains(upper, "EXCLUSION"):
		return ConstraintExclusion
	default:
		return ConstraintOther
	}
}

func constraintPriority(ct ConstraintType) int {
	switch ct {
	case ConstraintPrimaryKey:
		return 0
	case ConstraintUnique:
		return 1
	case ConstraintCheck:
		return 2
	case ConstraintForeignKey:
		return 3
	case ConstraintExclusion:
		return 4
	default:
		return 5
	}
}

func extractConstraintTable(sql string) string {
	upper := strings.ToUpper(sql)
	idx := strings.Index(upper, "ALTER TABLE")
	if idx < 0 {
		return ""
	}
	rest := strings.TrimSpace(sql[idx+11:])
	if strings.HasPrefix(strings.ToUpper(rest), "ONLY ") {
		rest = strings.TrimSpace(rest[5:])
	}
	var name strings.Builder
	for _, c := range rest {
		if c == ' ' {
			break
		}
		name.WriteRune(c)
	}
	return name.String()
}

func extractConstraintName(sql string) string {
	upper := strings.ToUpper(sql)
	idx := strings.Index(upper, "ADD CONSTRAINT")
	if idx < 0 {
		return ""
	}
	rest := strings.TrimSpace(sql[idx+14:])
	var name strings.Builder
	for _, c := range rest {
		if c == ' ' || c == '(' {
			break
		}
		name.WriteRune(c)
	}
	return name.String()
}

func filterConstraintType(jobs []ConstraintJob, ct ConstraintType) []ConstraintJob {
	var result []ConstraintJob
	for _, j := range jobs {
		if j.Type == ct {
			result = append(result, j)
		}
	}
	return result
}

func filterConstraintTypes(jobs []ConstraintJob, types ...ConstraintType) []ConstraintJob {
	typeSet := make(map[ConstraintType]bool, len(types))
	for _, t := range types {
		typeSet[t] = true
	}
	var result []ConstraintJob
	for _, j := range jobs {
		if typeSet[j.Type] {
			result = append(result, j)
		}
	}
	return result
}

func quoteIdentifier(name string) string {
	return `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
}
