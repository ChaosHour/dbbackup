// Package drill - Main drill execution engine
package drill

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"dbbackup/internal/fs"
	"dbbackup/internal/logger"

	"github.com/klauspost/pgzip"
)

// Engine executes DR drills
type Engine struct {
	docker  *DockerManager
	log     logger.Logger
	verbose bool
}

// NewEngine creates a new drill engine
func NewEngine(log logger.Logger, verbose bool) *Engine {
	return &Engine{
		docker:  NewDockerManager(verbose),
		log:     log,
		verbose: verbose,
	}
}

// Run executes a complete DR drill
func (e *Engine) Run(ctx context.Context, config *DrillConfig) (*DrillResult, error) {
	result := &DrillResult{
		DrillID:      NewDrillID(),
		StartTime:    time.Now(),
		BackupPath:   config.BackupPath,
		DatabaseName: config.DatabaseName,
		DatabaseType: config.DatabaseType,
		Status:       StatusRunning,
		Phases:       make([]DrillPhase, 0),
		TargetRTO:    float64(config.MaxRestoreSeconds),
	}

	e.log.Info("=====================================================")
	e.log.Info("  [TEST] DR Drill: " + result.DrillID)
	e.log.Info("=====================================================")
	e.log.Info("")

	// Cleanup function for error cases
	var containerID string
	cleanup := func() {
		if containerID != "" && config.CleanupOnExit && (result.Success || !config.KeepOnFailure) {
			e.log.Info("[DEL]  Cleaning up container...")
			e.docker.RemoveContainer(context.Background(), containerID)
		} else if containerID != "" {
			result.ContainerKept = true
			e.log.Info("[PKG] Container kept for debugging: " + containerID)
		}
	}
	defer cleanup()

	// Phase 1: Preflight checks
	phase := e.startPhase("Preflight Checks")
	if err := e.preflightChecks(ctx, config); err != nil {
		e.failPhase(&phase, err.Error())
		result.Phases = append(result.Phases, phase)
		result.Status = StatusFailed
		result.Message = "Preflight checks failed: " + err.Error()
		result.Errors = append(result.Errors, err.Error())
		e.finalize(result)
		return result, nil
	}
	e.completePhase(&phase, "All checks passed")
	result.Phases = append(result.Phases, phase)

	// Phase 2: Start container
	phase = e.startPhase("Start Container")
	containerConfig := e.buildContainerConfig(config)
	container, err := e.docker.CreateContainer(ctx, containerConfig)
	if err != nil {
		e.failPhase(&phase, err.Error())
		result.Phases = append(result.Phases, phase)
		result.Status = StatusFailed
		result.Message = "Failed to start container: " + err.Error()
		result.Errors = append(result.Errors, err.Error())
		e.finalize(result)
		return result, nil
	}
	containerID = container.ID
	result.ContainerID = containerID
	e.log.Info("[PKG] Container started: " + containerID[:12])

	// Wait for container to be healthy
	if err := e.docker.WaitForHealth(ctx, containerID, config.DatabaseType, config.ContainerTimeout); err != nil {
		e.failPhase(&phase, "Container health check failed: "+err.Error())
		result.Phases = append(result.Phases, phase)
		result.Status = StatusFailed
		result.Message = "Container failed to start"
		result.Errors = append(result.Errors, err.Error())
		e.finalize(result)
		return result, nil
	}
	e.completePhase(&phase, "Container healthy")
	result.Phases = append(result.Phases, phase)

	// Phase 3: Restore backup
	phase = e.startPhase("Restore Backup")
	restoreStart := time.Now()
	if err := e.restoreBackup(ctx, config, containerID, containerConfig); err != nil {
		e.failPhase(&phase, err.Error())
		result.Phases = append(result.Phases, phase)
		result.Status = StatusFailed
		result.Message = "Restore failed: " + err.Error()
		result.Errors = append(result.Errors, err.Error())
		e.finalize(result)
		return result, nil
	}
	result.RestoreTime = time.Since(restoreStart).Seconds()
	e.completePhase(&phase, fmt.Sprintf("Restored in %.2fs", result.RestoreTime))
	result.Phases = append(result.Phases, phase)
	e.log.Info(fmt.Sprintf("[OK] Backup restored in %.2fs", result.RestoreTime))

	// Phase 4: Validate
	phase = e.startPhase("Validate Database")
	validateStart := time.Now()
	validationErrors := e.validateDatabase(ctx, config, result, containerConfig)
	result.ValidationTime = time.Since(validateStart).Seconds()
	if validationErrors > 0 {
		e.completePhase(&phase, fmt.Sprintf("Completed with %d errors", validationErrors))
	} else {
		e.completePhase(&phase, "All validations passed")
	}
	result.Phases = append(result.Phases, phase)

	// Determine overall status
	result.ActualRTO = result.RestoreTime + result.ValidationTime
	result.RTOMet = result.ActualRTO <= result.TargetRTO

	criticalFailures := 0
	for _, vr := range result.ValidationResults {
		if !vr.Success {
			criticalFailures++
		}
	}
	for _, cr := range result.CheckResults {
		if !cr.Success {
			criticalFailures++
		}
	}

	if criticalFailures == 0 {
		result.Success = true
		result.Status = StatusCompleted
		result.Message = "DR drill completed successfully"
	} else if criticalFailures < len(result.ValidationResults)+len(result.CheckResults) {
		result.Success = false
		result.Status = StatusPartial
		result.Message = fmt.Sprintf("DR drill completed with %d validation failures", criticalFailures)
	} else {
		result.Success = false
		result.Status = StatusFailed
		result.Message = "All validations failed"
	}

	e.finalize(result)

	// Save result if output dir specified
	if config.OutputDir != "" {
		if err := result.SaveResult(config.OutputDir); err != nil {
			e.log.Warn("Failed to save drill result", "error", err)
		} else {
			e.log.Info("📄 Report saved to: " + filepath.Join(config.OutputDir, result.DrillID+"_report.json"))
		}
	}

	return result, nil
}

// preflightChecks runs preflight checks before the drill
func (e *Engine) preflightChecks(ctx context.Context, config *DrillConfig) error {
	// Check Docker is available
	if err := e.docker.CheckDockerAvailable(ctx); err != nil {
		return fmt.Errorf("docker not available: %w", err)
	}
	e.log.Info("[OK] Docker is available")

	// Check backup file exists
	if _, err := os.Stat(config.BackupPath); err != nil {
		return fmt.Errorf("backup file not found: %s", config.BackupPath)
	}
	e.log.Info("[OK] Backup file exists: " + filepath.Base(config.BackupPath))

	// Pull Docker image
	image := config.ContainerImage
	if image == "" {
		image = GetDefaultImage(config.DatabaseType, "")
	}
	e.log.Info("[DOWN]  Pulling image: " + image)
	if err := e.docker.PullImage(ctx, image); err != nil {
		return fmt.Errorf("failed to pull image: %w", err)
	}
	e.log.Info("[OK] Image ready: " + image)

	return nil
}

// buildContainerConfig creates container configuration
func (e *Engine) buildContainerConfig(config *DrillConfig) *ContainerConfig {
	containerName := config.ContainerName
	if containerName == "" {
		containerName = fmt.Sprintf("drill_%s_%s", config.DatabaseName, time.Now().Format("20060102_150405"))
	}

	image := config.ContainerImage
	if image == "" {
		image = GetDefaultImage(config.DatabaseType, "")
	}

	port := config.ContainerPort
	if port == 0 {
		port = 15432 // Default drill port (different from production)
		if config.DatabaseType == "mysql" || config.DatabaseType == "mariadb" {
			port = 13306
		}
	}

	containerPort := GetDefaultPort(config.DatabaseType)
	env := GetDefaultEnvironment(config.DatabaseType)

	return &ContainerConfig{
		Image:         image,
		Name:          containerName,
		Port:          port,
		ContainerPort: containerPort,
		Environment:   env,
		Timeout:       config.ContainerTimeout,
	}
}

// decompressWithPgzip decompresses a .gz file using in-process pgzip
func (e *Engine) decompressWithPgzip(srcPath string) (string, error) {
	if !strings.HasSuffix(srcPath, ".gz") {
		return srcPath, nil // Not compressed
	}

	dstPath := strings.TrimSuffix(srcPath, ".gz")
	e.log.Info("Decompressing with pgzip", "src", srcPath, "dst", dstPath)

	srcFile, err := os.Open(srcPath)
	if err != nil {
		return "", fmt.Errorf("failed to open source: %w", err)
	}
	defer srcFile.Close()

	gz, err := pgzip.NewReader(srcFile)
	if err != nil {
		return "", fmt.Errorf("failed to create pgzip reader: %w", err)
	}
	defer gz.Close()

	dstFile, err := os.Create(dstPath)
	if err != nil {
		return "", fmt.Errorf("failed to create destination: %w", err)
	}
	defer dstFile.Close()

	// Use context.Background() since decompressWithPgzip doesn't take context
	// The parent restoreBackup function handles context cancellation
	if _, err := fs.CopyWithContext(context.Background(), dstFile, gz); err != nil {
		os.Remove(dstPath)
		return "", fmt.Errorf("decompression failed: %w", err)
	}

	return dstPath, nil
}

// restoreBackup restores the backup into the container
func (e *Engine) restoreBackup(ctx context.Context, config *DrillConfig, containerID string, containerConfig *ContainerConfig) error {
	backupPath := config.BackupPath

	// Decompress on host with pgzip before copying to container
	if strings.HasSuffix(backupPath, ".gz") {
		e.log.Info("[DECOMPRESS] Decompressing backup with pgzip on host...")
		decompressedPath, err := e.decompressWithPgzip(backupPath)
		if err != nil {
			return fmt.Errorf("failed to decompress backup: %w", err)
		}
		backupPath = decompressedPath
		defer os.Remove(decompressedPath) // Clean up temp file
	}

	// Copy backup to container
	backupName := filepath.Base(backupPath)
	containerBackupPath := "/tmp/" + backupName

	e.log.Info("[DIR] Copying backup to container...")
	if err := e.docker.CopyToContainer(ctx, containerID, backupPath, containerBackupPath); err != nil {
		return fmt.Errorf("failed to copy backup: %w", err)
	}

	// Handle encrypted backups
	if config.EncryptionKeyFile != "" {
		// For encrypted backups, we'd need to decrypt first
		// This is a simplified implementation
		e.log.Warn("Encrypted backup handling not fully implemented in drill mode")
	}

	// Restore based on database type and format
	e.log.Info("[EXEC] Restoring backup...")
	return e.executeRestore(ctx, config, containerID, containerBackupPath, containerConfig)
}

// executeRestore runs the actual restore command
func (e *Engine) executeRestore(ctx context.Context, config *DrillConfig, containerID, backupPath string, containerConfig *ContainerConfig) error {
	var cmd []string

	// Note: Decompression is now done on host with pgzip before copying to container
	// So backupPath should never end with .gz at this point

	switch config.DatabaseType {
	case "postgresql", "postgres":
		// Create database
		_, err := e.docker.ExecCommand(ctx, containerID, []string{
			"psql", "-U", "postgres", "-c", fmt.Sprintf("CREATE DATABASE %s", config.DatabaseName),
		})
		if err != nil {
			// Database might already exist
			e.log.Debug("Create database returned (may already exist)")
		}

		// Detect restore method based on file content
		isCustomFormat := strings.Contains(backupPath, ".dump") || strings.Contains(backupPath, ".custom")
		if isCustomFormat {
			cmd = []string{"pg_restore", "-U", "postgres", "-d", config.DatabaseName, "-v", backupPath}
		} else {
			cmd = []string{"sh", "-c", fmt.Sprintf("psql -U postgres -d %s < %s", config.DatabaseName, backupPath)}
		}

	case "mysql":
		// Drop database if exists (backup contains CREATE DATABASE)
		_, _ = e.docker.ExecCommand(ctx, containerID, []string{
			"mysql", "-h", "127.0.0.1", "-u", "root", "--password=root", "-e",
			fmt.Sprintf("DROP DATABASE IF EXISTS %s", config.DatabaseName),
		})
		cmd = []string{"sh", "-c", fmt.Sprintf("mysql -h 127.0.0.1 -u root --password=root < %s", backupPath)}

	case "mariadb":
		// Drop database if exists (backup contains CREATE DATABASE)
		_, _ = e.docker.ExecCommand(ctx, containerID, []string{
			"mariadb", "-h", "127.0.0.1", "-u", "root", "--password=root", "-e",
			fmt.Sprintf("DROP DATABASE IF EXISTS %s", config.DatabaseName),
		})
		// Use mariadb client (mysql symlink may not exist in newer images)
		cmd = []string{"sh", "-c", fmt.Sprintf("mariadb -h 127.0.0.1 -u root --password=root < %s", backupPath)}

	default:
		return fmt.Errorf("unsupported database type: %s", config.DatabaseType)
	}

	output, err := e.docker.ExecCommand(ctx, containerID, cmd)
	if err != nil {
		return fmt.Errorf("restore failed: %w (output: %s)", err, output)
	}

	return nil
}

// validateDatabase runs validation against the restored database
func (e *Engine) validateDatabase(ctx context.Context, config *DrillConfig, result *DrillResult, containerConfig *ContainerConfig) int {
	errorCount := 0

	// Connect to database
	var user, password string
	switch config.DatabaseType {
	case "postgresql", "postgres":
		user = "postgres"
		password = containerConfig.Environment["POSTGRES_PASSWORD"]
	case "mysql":
		user = "root"
		password = "root"
	case "mariadb":
		user = "root"
		password = "root"
	}

	validator, err := NewValidator(config.DatabaseType, "localhost", containerConfig.Port, user, password, config.DatabaseName, e.verbose)
	if err != nil {
		e.log.Error("Failed to connect for validation", "error", err)
		result.Errors = append(result.Errors, "Validation connection failed: "+err.Error())
		return 1
	}
	defer validator.Close()

	// Get database metrics
	tables, err := validator.GetTableList(ctx)
	if err == nil {
		result.TableCount = len(tables)
		e.log.Info(fmt.Sprintf("[STATS] Tables found: %d", result.TableCount))
	}

	totalRows, err := validator.GetTotalRowCount(ctx)
	if err == nil {
		result.TotalRows = totalRows
		e.log.Info(fmt.Sprintf("[STATS] Total rows: %d", result.TotalRows))
	}

	dbSize, err := validator.GetDatabaseSize(ctx, config.DatabaseName)
	if err == nil {
		result.DatabaseSize = dbSize
	}

	// Run expected tables check
	if len(config.ExpectedTables) > 0 {
		tableResults := validator.ValidateExpectedTables(ctx, config.ExpectedTables)
		for _, tr := range tableResults {
			result.CheckResults = append(result.CheckResults, tr)
			if !tr.Success {
				errorCount++
				e.log.Warn("[FAIL] " + tr.Message)
			} else {
				e.log.Info("[OK] " + tr.Message)
			}
		}
	}

	// Run validation queries
	if len(config.ValidationQueries) > 0 {
		queryResults := validator.RunValidationQueries(ctx, config.ValidationQueries)
		result.ValidationResults = append(result.ValidationResults, queryResults...)

		var totalQueryTime float64
		for _, qr := range queryResults {
			totalQueryTime += qr.Duration
			if !qr.Success {
				errorCount++
				e.log.Warn(fmt.Sprintf("[FAIL] %s: %s", qr.Name, qr.Error))
			} else {
				e.log.Info(fmt.Sprintf("[OK] %s: %s (%.0fms)", qr.Name, qr.Result, qr.Duration))
			}
		}
		if len(queryResults) > 0 {
			result.QueryTimeAvg = totalQueryTime / float64(len(queryResults))
		}
	}

	// Run custom checks
	if len(config.CustomChecks) > 0 {
		checkResults := validator.RunCustomChecks(ctx, config.CustomChecks)
		for _, cr := range checkResults {
			result.CheckResults = append(result.CheckResults, cr)
			if !cr.Success {
				errorCount++
				e.log.Warn("[FAIL] " + cr.Message)
			} else {
				e.log.Info("[OK] " + cr.Message)
			}
		}
	}

	// Check minimum row count if specified
	if config.MinRowCount > 0 && result.TotalRows < config.MinRowCount {
		errorCount++
		msg := fmt.Sprintf("Total rows (%d) below minimum (%d)", result.TotalRows, config.MinRowCount)
		result.Warnings = append(result.Warnings, msg)
		e.log.Warn("[WARN] " + msg)
	}

	return errorCount
}

// startPhase starts a new drill phase
func (e *Engine) startPhase(name string) DrillPhase {
	e.log.Info("[RUN]  " + name)
	return DrillPhase{
		Name:      name,
		Status:    "running",
		StartTime: time.Now(),
	}
}

// completePhase marks a phase as completed
func (e *Engine) completePhase(phase *DrillPhase, message string) {
	phase.EndTime = time.Now()
	phase.Duration = phase.EndTime.Sub(phase.StartTime).Seconds()
	phase.Status = "completed"
	phase.Message = message
}

// failPhase marks a phase as failed
func (e *Engine) failPhase(phase *DrillPhase, message string) {
	phase.EndTime = time.Now()
	phase.Duration = phase.EndTime.Sub(phase.StartTime).Seconds()
	phase.Status = "failed"
	phase.Message = message
	e.log.Error("[FAIL] Phase failed: " + message)
}

// finalize completes the drill result
func (e *Engine) finalize(result *DrillResult) {
	result.EndTime = time.Now()
	result.Duration = result.EndTime.Sub(result.StartTime).Seconds()

	e.log.Info("")
	e.log.Info("=====================================================")
	e.log.Info("  " + result.Summary())
	e.log.Info("=====================================================")

	if result.Success {
		e.log.Info(fmt.Sprintf("  RTO: %.2fs (target: %.0fs) %s",
			result.ActualRTO, result.TargetRTO, boolIcon(result.RTOMet)))
	}
}

func boolIcon(b bool) string {
	if b {
		return "[OK]"
	}
	return "[FAIL]"
}

// Cleanup removes drill resources
func (e *Engine) Cleanup(ctx context.Context, drillID string) error {
	containers, err := e.docker.ListDrillContainers(ctx)
	if err != nil {
		return err
	}

	for _, c := range containers {
		if strings.Contains(c.Name, drillID) || (drillID == "" && strings.HasPrefix(c.Name, "drill_")) {
			e.log.Info("[DEL]  Removing container: " + c.Name)
			if err := e.docker.RemoveContainer(ctx, c.ID); err != nil {
				e.log.Warn("Failed to remove container", "id", c.ID, "error", err)
			}
		}
	}

	return nil
}

// QuickTest runs a quick restore test without full validation
func (e *Engine) QuickTest(ctx context.Context, backupPath, dbType, dbName string) (*DrillResult, error) {
	config := DefaultConfig()
	config.BackupPath = backupPath
	config.DatabaseType = dbType
	config.DatabaseName = dbName
	config.CleanupOnExit = true
	config.MaxRestoreSeconds = 600

	return e.Run(ctx, config)
}

// Validate runs validation queries against an existing database (non-Docker)
func (e *Engine) Validate(ctx context.Context, config *DrillConfig, host string, port int, user, password string) ([]ValidationResult, error) {
	validator, err := NewValidator(config.DatabaseType, host, port, user, password, config.DatabaseName, e.verbose)
	if err != nil {
		return nil, err
	}
	defer validator.Close()

	return validator.RunValidationQueries(ctx, config.ValidationQueries), nil
}
