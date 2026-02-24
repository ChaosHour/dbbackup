package tui

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	tea "github.com/charmbracelet/bubbletea"

	"dbbackup/internal/config"
	"dbbackup/internal/logger"
	"dbbackup/internal/restore"
)

// PreflightCheck defines a single pre-restore check
type PreflightCheck struct {
	ID       string
	Label    string
	Required bool // true = must pass, false = warning only
}

// PreflightCheckStatus tracks the result of a single check
type PreflightCheckStatus struct {
	State   string // "pending" | "running" | "pass" | "fail" | "warn"
	Message string
	Error   error
}

// preflightCompleteMsg is sent when all checks finish
type preflightCompleteMsg struct {
	allPassed bool
}

// checkStartedMsg signals a check is beginning
type checkStartedMsg struct {
	checkID string
}

// checkCompletedMsg signals a check has finished
type checkCompletedMsg struct {
	checkID string
	status  PreflightCheckStatus
}

// PreRestoreChecklistModel shows a checklist of preflight checks before restore
type PreRestoreChecklistModel struct {
	parent      tea.Model
	config      *config.Config
	logger      logger.Logger
	archivePath string
	targetDB    string
	mode        string // "restore-single" | "restore-cluster"

	checks    []PreflightCheck
	statuses  map[string]PreflightCheckStatus
	running   bool
	allPassed bool
	failed    bool // At least one required check failed
	done      bool
	message   string

	// Sequential check execution
	currentCheck int // index of check currently running

	// Carry forward from restore preview
	archive           ArchiveInfo
	cleanFirst        bool
	createIfMissing   bool
	cleanClusterFirst bool
	existingDBs       []string
	saveDebugLog      bool
	workDir           string
	debugLocks        bool

	ctx context.Context
}

// restorePreflightChecks defines the checks to run
var restorePreflightChecks = []PreflightCheck{
	{ID: "archive_exists", Label: "Archive file exists", Required: true},
	{ID: "archive_integrity", Label: "Archive integrity", Required: true},
	{ID: "disk_space", Label: "Sufficient disk space", Required: true},
	{ID: "required_tools", Label: "Required tools available", Required: true},
	{ID: "target_db_check", Label: "Target database status", Required: false},
	{ID: "privileges_check", Label: "Database user privileges", Required: false},
	{ID: "locks_sufficient", Label: "Lock capacity sufficient", Required: false},
}

// NewPreRestoreChecklist creates a new pre-restore checklist
func NewPreRestoreChecklist(
	cfg *config.Config, log logger.Logger, parent tea.Model, ctx context.Context,
	archive ArchiveInfo, targetDB, mode string,
	cleanFirst, createIfMissing, cleanClusterFirst bool,
	existingDBs []string, saveDebugLog bool, workDir string, debugLocks bool,
) PreRestoreChecklistModel {
	statuses := make(map[string]PreflightCheckStatus)
	for _, check := range restorePreflightChecks {
		statuses[check.ID] = PreflightCheckStatus{State: "pending", Message: ""}
	}

	return PreRestoreChecklistModel{
		parent:            parent,
		config:            cfg,
		logger:            log,
		archivePath:       archive.Path,
		targetDB:          targetDB,
		mode:              mode,
		checks:            restorePreflightChecks,
		statuses:          statuses,
		running:           true,
		currentCheck:      0,
		archive:           archive,
		cleanFirst:        cleanFirst,
		createIfMissing:   createIfMissing,
		cleanClusterFirst: cleanClusterFirst,
		existingDBs:       existingDBs,
		saveDebugLog:      saveDebugLog,
		workDir:           workDir,
		debugLocks:        debugLocks,
		ctx:               ctx,
	}
}

func (m PreRestoreChecklistModel) Init() tea.Cmd {
	// Skip preflight in auto-confirm mode
	if m.config.TUIAutoConfirm || (m.config != nil && m.config.SkipPreflightChecks) {
		return func() tea.Msg {
			statuses := make(map[string]PreflightCheckStatus)
			for _, check := range restorePreflightChecks {
				statuses[check.ID] = PreflightCheckStatus{State: "pass", Message: "Skipped"}
			}
			return preflightCompleteMsg{allPassed: true}
		}
	}
	// Start first check
	return m.runNextCheck()
}

// runNextCheck starts the next pending check sequentially
func (m PreRestoreChecklistModel) runNextCheck() tea.Cmd {
	if m.currentCheck >= len(m.checks) {
		// All checks complete
		return func() tea.Msg {
			return preflightCompleteMsg{allPassed: true}
		}
	}

	check := m.checks[m.currentCheck]
	checkID := check.ID

	// Send "started" message first, then run the check
	return tea.Batch(
		func() tea.Msg { return checkStartedMsg{checkID: checkID} },
		m.executeCheck(check),
	)
}

// executeCheck runs a single check and returns a checkCompletedMsg
func (m PreRestoreChecklistModel) executeCheck(check PreflightCheck) tea.Cmd {
	cfg := m.config
	log := m.logger
	archivePath := m.archivePath
	targetDB := m.targetDB
	mode := m.mode

	return func() tea.Msg {
		var status PreflightCheckStatus

		switch check.ID {
		case "archive_exists":
			status = runArchiveExistsCheck(archivePath)
		case "archive_integrity":
			status = runArchiveIntegrityCheck(cfg, log, archivePath)
		case "disk_space":
			status = runDiskSpaceCheck(cfg, log, archivePath)
		case "required_tools":
			status = runRequiredToolsCheck(cfg, log)
		case "target_db_check":
			status = runTargetDBCheck(cfg, log, targetDB, mode)
		case "privileges_check":
			status = runPrivilegesCheck(cfg, log)
		case "locks_sufficient":
			status = runLocksCheck(cfg, log)
		default:
			status = PreflightCheckStatus{State: "pass", Message: "Unknown check — skipped"}
		}

		return checkCompletedMsg{checkID: check.ID, status: status}
	}
}

// Individual check functions

func runArchiveExistsCheck(archivePath string) PreflightCheckStatus {
	fi, err := os.Stat(archivePath)
	if err != nil {
		return PreflightCheckStatus{
			State: "fail", Message: fmt.Sprintf("File not found: %s", archivePath), Error: err,
		}
	}
	return PreflightCheckStatus{
		State: "pass", Message: fmt.Sprintf("%s (%s)", archivePath, FormatBytes(fi.Size())),
	}
}

func runArchiveIntegrityCheck(cfg *config.Config, log logger.Logger, archivePath string) PreflightCheckStatus {
	safety := restore.NewSafety(cfg, log)
	if err := safety.ValidateArchive(archivePath); err != nil {
		return PreflightCheckStatus{
			State: "fail", Message: err.Error(), Error: err,
		}
	}
	return PreflightCheckStatus{State: "pass", Message: "Valid backup archive"}
}

func runDiskSpaceCheck(cfg *config.Config, log logger.Logger, archivePath string) PreflightCheckStatus {
	log.Debug("[DISKSPACE-PREFLIGHT] Running disk space preflight check",
		"archive_path", archivePath,
		"config_multiplier", cfg.DiskSpaceMultiplier,
		"skip_disk_check", cfg.SkipDiskCheck)
	safety := restore.NewSafety(cfg, log)
	if err := safety.CheckDiskSpace(archivePath, 0); err != nil {
		log.Warn("[DISKSPACE-PREFLIGHT] Disk space check failed",
			"error", err.Error())
		return PreflightCheckStatus{State: "warn", Message: err.Error()}
	}
	log.Debug("[DISKSPACE-PREFLIGHT] Disk space check passed")
	return PreflightCheckStatus{State: "pass", Message: "Sufficient space available"}
}

func runRequiredToolsCheck(cfg *config.Config, log logger.Logger) PreflightCheckStatus {
	if cfg.UseNativeEngine {
		return PreflightCheckStatus{State: "pass", Message: "Native engine — no external tools required"}
	}
	dbType := "postgres"
	if cfg.IsMySQL() {
		dbType = "mysql"
	}
	safety := restore.NewSafety(cfg, log)
	if err := safety.VerifyTools(dbType); err != nil {
		return PreflightCheckStatus{State: "fail", Message: err.Error(), Error: err}
	}
	return PreflightCheckStatus{State: "pass", Message: "All required tools available"}
}

func runTargetDBCheck(cfg *config.Config, log logger.Logger, targetDB, mode string) PreflightCheckStatus {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if mode == "restore-cluster" {
		return PreflightCheckStatus{
			State: "pass", Message: "Cluster restore — individual DB checks at execution time",
		}
	}

	safety := restore.NewSafety(cfg, log)
	exists, err := safety.CheckDatabaseExists(ctx, targetDB)
	if err != nil {
		return PreflightCheckStatus{
			State: "warn", Message: fmt.Sprintf("Cannot check: %v", err),
		}
	}
	if exists {
		return PreflightCheckStatus{
			State: "warn", Message: fmt.Sprintf("Database '%s' exists — will be overwritten", targetDB),
		}
	}
	return PreflightCheckStatus{
		State: "pass", Message: fmt.Sprintf("Database '%s' does not exist — will be created", targetDB),
	}
}

func runPrivilegesCheck(cfg *config.Config, log logger.Logger) PreflightCheckStatus {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	db, err := openTUIDatabase(cfg, "postgres")
	if err != nil {
		return PreflightCheckStatus{
			State: "warn", Message: fmt.Sprintf("Cannot connect: %v", err),
		}
	}
	defer func() { _ = db.Close() }()

	if err := db.PingContext(ctx); err != nil {
		return PreflightCheckStatus{
			State: "warn", Message: fmt.Sprintf("Connection failed: %v", err),
		}
	}

	// For PostgreSQL, check if the user has CREATEDB privilege
	if cfg.IsPostgreSQL() {
		var rolCreateDB sql.NullBool
		err := db.QueryRowContext(ctx,
			"SELECT rolcreatedb FROM pg_roles WHERE rolname = current_user").Scan(&rolCreateDB)
		if err != nil {
			return PreflightCheckStatus{
				State: "warn", Message: fmt.Sprintf("Connected OK, cannot check privileges: %v", err),
			}
		}
		if !rolCreateDB.Valid || !rolCreateDB.Bool {
			// Also check if user is superuser (superusers can always create DBs)
			var rolSuper sql.NullBool
			err := db.QueryRowContext(ctx,
				"SELECT rolsuper FROM pg_roles WHERE rolname = current_user").Scan(&rolSuper)
			if err == nil && rolSuper.Valid && rolSuper.Bool {
				return PreflightCheckStatus{
					State: "pass", Message: "Superuser — full privileges",
				}
			}
			return PreflightCheckStatus{
				State: "warn", Message: "User lacks CREATEDB privilege — restore may fail for new databases",
			}
		}
		return PreflightCheckStatus{State: "pass", Message: "CREATEDB privilege confirmed"}
	}

	// MySQL — just verify connection is sufficient
	return PreflightCheckStatus{State: "pass", Message: "Connected successfully"}
}

func runLocksCheck(cfg *config.Config, log logger.Logger) PreflightCheckStatus {
	if !cfg.IsPostgreSQL() {
		return PreflightCheckStatus{State: "pass", Message: "N/A for MySQL — skipped"}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	db, err := openTUIDatabase(cfg, "postgres")
	if err != nil {
		return PreflightCheckStatus{
			State: "warn", Message: fmt.Sprintf("Cannot connect: %v", err),
		}
	}
	defer func() { _ = db.Close() }()

	var maxLocksStr string
	err = db.QueryRowContext(ctx, "SHOW max_locks_per_transaction").Scan(&maxLocksStr)
	if err != nil {
		return PreflightCheckStatus{
			State: "warn", Message: fmt.Sprintf("Cannot query max_locks_per_transaction: %v", err),
		}
	}

	maxLocks, err := strconv.Atoi(strings.TrimSpace(maxLocksStr))
	if err != nil {
		return PreflightCheckStatus{
			State: "warn", Message: fmt.Sprintf("Cannot parse max_locks_per_transaction: %s", maxLocksStr),
		}
	}

	// Recommend >= 256 for high-parallelism restores, >= 128 for single
	recommended := 128
	label := "single restore"
	if cfg.Jobs > 4 || cfg.ResourceProfile == "turbo" || cfg.ResourceProfile == "max-performance" {
		recommended = 256
		label = "parallel/high-performance"
	}

	if maxLocks < recommended {
		return PreflightCheckStatus{
			State:   "warn",
			Message: fmt.Sprintf("max_locks_per_transaction=%d (recommend >=%d for %s)", maxLocks, recommended, label),
		}
	}

	return PreflightCheckStatus{
		State:   "pass",
		Message: fmt.Sprintf("max_locks_per_transaction=%d (sufficient)", maxLocks),
	}
}

func (m PreRestoreChecklistModel) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	tuiDebugLog(m.config, m.logger, "preflight", msg)
	switch msg := msg.(type) {
	case checkStartedMsg:
		m.statuses[msg.checkID] = PreflightCheckStatus{State: "running", Message: "Checking..."}
		return m, nil

	case checkCompletedMsg:
		m.statuses[msg.checkID] = msg.status
		m.currentCheck++

		// Check if this required check failed
		if msg.status.State == "fail" {
			for _, check := range m.checks {
				if check.ID == msg.checkID && check.Required {
					m.failed = true
					break
				}
			}
		}

		// Move to next check
		if m.currentCheck < len(m.checks) {
			return m, m.runNextCheck()
		}

		// All checks done
		m.running = false
		m.done = true
		m.allPassed = !m.failed

		// Auto-proceed if all passed and auto-confirm
		if m.config.TUIAutoConfirm && m.allPassed {
			return m.proceedToRestore()
		}
		return m, nil

	case preflightCompleteMsg:
		// Used by auto-skip path
		m.running = false
		m.done = true
		m.allPassed = msg.allPassed

		if m.config.TUIAutoConfirm && m.allPassed {
			return m.proceedToRestore()
		}
		return m, nil

	case tea.InterruptMsg:
		return m.parent, nil

	case tea.KeyMsg:
		switch msg.String() {
		case "ctrl+c", "q", "esc":
			return m.parent, nil
		case "enter", " ":
			if m.done && !m.failed {
				return m.proceedToRestore()
			} else if m.done && m.failed {
				m.message = errorStyle.Render("[FAIL] Cannot proceed — fix critical errors above")
			}
		case "r":
			// Rerun checks
			m.running = true
			m.done = false
			m.failed = false
			m.currentCheck = 0
			for _, check := range m.checks {
				m.statuses[check.ID] = PreflightCheckStatus{State: "pending", Message: ""}
			}
			return m, m.runNextCheck()
		}
	}

	return m, nil
}

func (m PreRestoreChecklistModel) proceedToRestore() (tea.Model, tea.Cmd) {
	if m.debugLocks {
		m.config.DebugLocks = true
	}
	exec := NewRestoreExecution(
		m.config, m.logger, m.parent, m.ctx,
		m.archive, m.targetDB,
		m.cleanFirst, m.createIfMissing,
		m.mode, m.cleanClusterFirst,
		m.existingDBs, m.saveDebugLog, m.workDir,
	)
	return exec, exec.Init()
}

func (m PreRestoreChecklistModel) View() string {
	var s strings.Builder

	s.WriteString("\n")
	s.WriteString(titleStyle.Render("[CHECK] Pre-Restore Validation"))
	s.WriteString("\n\n")

	s.WriteString(fmt.Sprintf("  Archive: %s\n", m.archive.Name))
	if m.mode != "restore-cluster" {
		s.WriteString(fmt.Sprintf("  Target:  %s\n", m.targetDB))
	}
	s.WriteString("\n")

	for _, check := range m.checks {
		status := m.statuses[check.ID]
		var icon string
		var style func(strs ...string) string

		switch status.State {
		case "pass":
			icon = "[+]"
			style = CheckPassedStyle.Render
		case "fail":
			icon = "[-]"
			style = CheckFailedStyle.Render
		case "warn":
			icon = "[!]"
			style = CheckWarningStyle.Render
		case "running":
			icon = "[~]"
			style = CheckPendingStyle.Render
		default:
			icon = "[ ]"
			style = CheckPendingStyle.Render
		}

		line := fmt.Sprintf("  %s %s", icon, check.Label)
		if check.Required {
			line += " *"
		}
		s.WriteString(style(line))
		s.WriteString("\n")
		if status.Message != "" {
			s.WriteString(CheckPendingStyle.Render(fmt.Sprintf("      %s", status.Message)))
			s.WriteString("\n")
		}
	}

	s.WriteString("\n")

	if m.running {
		s.WriteString(infoStyle.Render("  Running checks... please wait"))
		s.WriteString("\n")
	} else if m.failed {
		s.WriteString(errorStyle.Render("  [-] Critical checks failed. Cannot proceed."))
		s.WriteString("\n\n")
		s.WriteString(infoStyle.Render("  [KEYS] r: Re-run checks | Esc: Cancel"))
	} else if m.done {
		s.WriteString(successStyle.Render("  [+] All checks passed."))
		s.WriteString("\n\n")
		s.WriteString(infoStyle.Render("  [KEYS] Enter: Start restore | r: Re-run | Esc: Cancel"))
	}

	if m.message != "" {
		s.WriteString("\n\n")
		s.WriteString(m.message)
	}

	s.WriteString("\n")

	return s.String()
}
