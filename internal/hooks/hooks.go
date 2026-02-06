// Package hooks provides pre/post backup hook execution.
// Hooks allow running custom scripts before and after backup operations,
// useful for:
//   - Running VACUUM ANALYZE before backup
//   - Notifying monitoring systems
//   - Stopping/starting replication
//   - Custom validation scripts
//   - Cleanup operations
package hooks

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"dbbackup/internal/logger"
)

// Manager handles hook execution
type Manager struct {
	config *Config
	log    logger.Logger
}

// Config contains hook configuration
type Config struct {
	// Pre-backup hooks
	PreBackup     []Hook // Run before backup starts
	PreDatabase   []Hook // Run before each database backup
	PreTable      []Hook // Run before each table (for selective backup)

	// Post-backup hooks
	PostBackup    []Hook // Run after backup completes
	PostDatabase  []Hook // Run after each database backup
	PostTable     []Hook // Run after each table
	PostUpload    []Hook // Run after cloud upload

	// Error hooks
	OnError       []Hook // Run when backup fails
	OnSuccess     []Hook // Run when backup succeeds

	// Settings
	ContinueOnError bool          // Continue backup if pre-hook fails
	Timeout         time.Duration // Default timeout for hooks
	WorkDir         string        // Working directory for hook execution
	Environment     map[string]string // Additional environment variables
}

// Hook defines a single hook to execute
type Hook struct {
	Name        string            // Hook name for logging
	Command     string            // Command to execute (can be path to script or inline command)
	Args        []string          // Command arguments
	Shell       bool              // Execute via shell (allows pipes, redirects)
	Timeout     time.Duration     // Override default timeout
	Environment map[string]string // Additional environment variables
	ContinueOnError bool          // Override global setting
	Condition   string            // Shell condition that must be true to run
}

// HookContext provides context to hooks via environment variables
type HookContext struct {
	Operation    string    // "backup", "restore", "verify"
	Phase        string    // "pre", "post", "error"
	Database     string    // Current database name
	Table        string    // Current table (for selective backup)
	BackupPath   string    // Path to backup file
	BackupSize   int64     // Backup size in bytes
	StartTime    time.Time // When operation started
	Duration     time.Duration // Operation duration (for post hooks)
	Error        string    // Error message (for error hooks)
	ExitCode     int       // Exit code (for post/error hooks)
	CloudTarget  string    // Cloud storage URI
	Success      bool      // Whether operation succeeded
}

// HookResult contains the result of hook execution
type HookResult struct {
	Hook      string
	Success   bool
	Output    string
	Error     string
	Duration  time.Duration
	ExitCode  int
}

// NewManager creates a new hook manager
func NewManager(cfg *Config, log logger.Logger) *Manager {
	if cfg.Timeout == 0 {
		cfg.Timeout = 5 * time.Minute
	}
	if cfg.WorkDir == "" {
		cfg.WorkDir, _ = os.Getwd()
	}

	return &Manager{
		config: cfg,
		log:    log,
	}
}

// RunPreBackup executes pre-backup hooks
func (m *Manager) RunPreBackup(ctx context.Context, hctx *HookContext) error {
	hctx.Phase = "pre"
	hctx.Operation = "backup"
	return m.runHooks(ctx, m.config.PreBackup, hctx)
}

// RunPostBackup executes post-backup hooks
func (m *Manager) RunPostBackup(ctx context.Context, hctx *HookContext) error {
	hctx.Phase = "post"
	return m.runHooks(ctx, m.config.PostBackup, hctx)
}

// RunPreDatabase executes pre-database hooks
func (m *Manager) RunPreDatabase(ctx context.Context, hctx *HookContext) error {
	hctx.Phase = "pre"
	return m.runHooks(ctx, m.config.PreDatabase, hctx)
}

// RunPostDatabase executes post-database hooks
func (m *Manager) RunPostDatabase(ctx context.Context, hctx *HookContext) error {
	hctx.Phase = "post"
	return m.runHooks(ctx, m.config.PostDatabase, hctx)
}

// RunOnError executes error hooks
func (m *Manager) RunOnError(ctx context.Context, hctx *HookContext) error {
	hctx.Phase = "error"
	return m.runHooks(ctx, m.config.OnError, hctx)
}

// RunOnSuccess executes success hooks
func (m *Manager) RunOnSuccess(ctx context.Context, hctx *HookContext) error {
	hctx.Phase = "success"
	return m.runHooks(ctx, m.config.OnSuccess, hctx)
}

// runHooks executes a list of hooks
func (m *Manager) runHooks(ctx context.Context, hooks []Hook, hctx *HookContext) error {
	if len(hooks) == 0 {
		return nil
	}

	m.log.Debug("Running hooks", "phase", hctx.Phase, "count", len(hooks))

	for _, hook := range hooks {
		result := m.runSingleHook(ctx, &hook, hctx)

		if !result.Success {
			m.log.Warn("Hook failed",
				"name", hook.Name,
				"error", result.Error,
				"output", result.Output)

			continueOnError := hook.ContinueOnError || m.config.ContinueOnError
			if !continueOnError {
				return fmt.Errorf("hook '%s' failed: %s", hook.Name, result.Error)
			}
		} else {
			m.log.Debug("Hook completed",
				"name", hook.Name,
				"duration", result.Duration)
		}
	}

	return nil
}

// runSingleHook executes a single hook
func (m *Manager) runSingleHook(ctx context.Context, hook *Hook, hctx *HookContext) *HookResult {
	result := &HookResult{
		Hook: hook.Name,
	}
	startTime := time.Now()

	// Check condition
	if hook.Condition != "" {
		if !m.evaluateCondition(ctx, hook.Condition, hctx) {
			result.Success = true
			result.Output = "skipped: condition not met"
			return result
		}
	}

	// Prepare timeout
	timeout := hook.Timeout
	if timeout == 0 {
		timeout = m.config.Timeout
	}

	hookCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	// Build command
	var cmd *exec.Cmd
	if hook.Shell {
		shellCmd := m.expandVariables(hook.Command, hctx)
		if len(hook.Args) > 0 {
			shellCmd += " " + strings.Join(hook.Args, " ")
		}
		cmd = exec.CommandContext(hookCtx, "sh", "-c", shellCmd)
	} else {
		expandedCmd := m.expandVariables(hook.Command, hctx)
		expandedArgs := make([]string, len(hook.Args))
		for i, arg := range hook.Args {
			expandedArgs[i] = m.expandVariables(arg, hctx)
		}
		cmd = exec.CommandContext(hookCtx, expandedCmd, expandedArgs...)
	}

	// Set environment
	cmd.Env = m.buildEnvironment(hctx, hook.Environment)
	cmd.Dir = m.config.WorkDir

	// Capture output
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	// Run command
	err := cmd.Run()
	result.Duration = time.Since(startTime)
	result.Output = strings.TrimSpace(stdout.String())

	if err != nil {
		result.Success = false
		result.Error = err.Error()
		if stderr.Len() > 0 {
			result.Error += ": " + strings.TrimSpace(stderr.String())
		}
		if exitErr, ok := err.(*exec.ExitError); ok {
			result.ExitCode = exitErr.ExitCode()
		}
	} else {
		result.Success = true
		result.ExitCode = 0
	}

	return result
}

// evaluateCondition checks if a shell condition is true
func (m *Manager) evaluateCondition(ctx context.Context, condition string, hctx *HookContext) bool {
	expandedCondition := m.expandVariables(condition, hctx)
	cmd := exec.CommandContext(ctx, "sh", "-c", fmt.Sprintf("[ %s ]", expandedCondition))
	cmd.Env = m.buildEnvironment(hctx, nil)
	return cmd.Run() == nil
}

// buildEnvironment creates the environment for hook execution
func (m *Manager) buildEnvironment(hctx *HookContext, extra map[string]string) []string {
	env := os.Environ()

	// Add hook context
	contextEnv := map[string]string{
		"DBBACKUP_OPERATION":    hctx.Operation,
		"DBBACKUP_PHASE":        hctx.Phase,
		"DBBACKUP_DATABASE":     hctx.Database,
		"DBBACKUP_TABLE":        hctx.Table,
		"DBBACKUP_BACKUP_PATH":  hctx.BackupPath,
		"DBBACKUP_BACKUP_SIZE":  fmt.Sprintf("%d", hctx.BackupSize),
		"DBBACKUP_START_TIME":   hctx.StartTime.Format(time.RFC3339),
		"DBBACKUP_DURATION_SEC": fmt.Sprintf("%.0f", hctx.Duration.Seconds()),
		"DBBACKUP_ERROR":        hctx.Error,
		"DBBACKUP_EXIT_CODE":    fmt.Sprintf("%d", hctx.ExitCode),
		"DBBACKUP_CLOUD_TARGET": hctx.CloudTarget,
		"DBBACKUP_SUCCESS":      fmt.Sprintf("%t", hctx.Success),
	}

	for k, v := range contextEnv {
		env = append(env, fmt.Sprintf("%s=%s", k, v))
	}

	// Add global config environment
	for k, v := range m.config.Environment {
		env = append(env, fmt.Sprintf("%s=%s", k, v))
	}

	// Add hook-specific environment
	for k, v := range extra {
		env = append(env, fmt.Sprintf("%s=%s", k, v))
	}

	return env
}

// expandVariables expands ${VAR} style variables in strings
func (m *Manager) expandVariables(s string, hctx *HookContext) string {
	replacements := map[string]string{
		"${DATABASE}":     hctx.Database,
		"${TABLE}":        hctx.Table,
		"${BACKUP_PATH}":  hctx.BackupPath,
		"${BACKUP_SIZE}":  fmt.Sprintf("%d", hctx.BackupSize),
		"${OPERATION}":    hctx.Operation,
		"${PHASE}":        hctx.Phase,
		"${ERROR}":        hctx.Error,
		"${CLOUD_TARGET}": hctx.CloudTarget,
	}

	result := s
	for k, v := range replacements {
		result = strings.ReplaceAll(result, k, v)
	}

	// Expand environment variables
	result = os.ExpandEnv(result)

	return result
}

// LoadHooksFromDir loads hooks from a directory structure
// Expected structure:
//   hooks/
//     pre-backup/
//       00-vacuum.sh
//       10-notify.sh
//     post-backup/
//       00-verify.sh
//       10-cleanup.sh
func (m *Manager) LoadHooksFromDir(hooksDir string) error {
	if _, err := os.Stat(hooksDir); os.IsNotExist(err) {
		return nil // No hooks directory
	}

	phases := map[string]*[]Hook{
		"pre-backup":    &m.config.PreBackup,
		"post-backup":   &m.config.PostBackup,
		"pre-database":  &m.config.PreDatabase,
		"post-database": &m.config.PostDatabase,
		"on-error":      &m.config.OnError,
		"on-success":    &m.config.OnSuccess,
	}

	for phase, hooks := range phases {
		phaseDir := filepath.Join(hooksDir, phase)
		if _, err := os.Stat(phaseDir); os.IsNotExist(err) {
			continue
		}

		entries, err := os.ReadDir(phaseDir)
		if err != nil {
			return fmt.Errorf("failed to read %s: %w", phaseDir, err)
		}

		for _, entry := range entries {
			if entry.IsDir() {
				continue
			}

			name := entry.Name()
			path := filepath.Join(phaseDir, name)

			// Check if executable
			info, err := entry.Info()
			if err != nil {
				continue
			}
			if info.Mode()&0111 == 0 {
				continue // Not executable
			}

			*hooks = append(*hooks, Hook{
				Name:    name,
				Command: path,
				Shell:   true,
			})

			m.log.Debug("Loaded hook", "phase", phase, "name", name)
		}
	}

	return nil
}

// PredefinedHooks provides common hooks
var PredefinedHooks = map[string]Hook{
	"vacuum-analyze": {
		Name:    "vacuum-analyze",
		Command: "psql",
		Args:    []string{"-h", "${PGHOST}", "-U", "${PGUSER}", "-d", "${DATABASE}", "-c", "VACUUM ANALYZE"},
		Shell:   false,
	},
	"checkpoint": {
		Name:    "checkpoint",
		Command: "psql",
		Args:    []string{"-h", "${PGHOST}", "-U", "${PGUSER}", "-d", "${DATABASE}", "-c", "CHECKPOINT"},
		Shell:   false,
	},
	"slack-notify": {
		Name:    "slack-notify",
		Command: `curl -X POST -H 'Content-type: application/json' --data '{"text":"Backup ${PHASE} for ${DATABASE}"}' ${SLACK_WEBHOOK_URL}`,
		Shell:   true,
	},
	"email-notify": {
		Name:    "email-notify",
		Command: `echo "Backup ${PHASE} for ${DATABASE}: ${SUCCESS}" | mail -s "dbbackup notification" ${NOTIFY_EMAIL}`,
		Shell:   true,
	},
}

// GetPredefinedHook returns a predefined hook by name
func GetPredefinedHook(name string) (Hook, bool) {
	hook, ok := PredefinedHooks[name]
	return hook, ok
}
