package tui

import (
	"context"
	"fmt"
	"strings"
	"time"

	tea "github.com/charmbracelet/bubbletea"

	"dbbackup/internal/config"
	"dbbackup/internal/logger"
	"dbbackup/internal/restore"
)

// SafetyCheck represents a pre-restore safety check
type SafetyCheck struct {
	Name     string
	Status   string // "pending", "checking", "passed", "failed", "warning"
	Message  string
	Critical bool
}

// RestorePreviewModel shows restore preview and safety checks
// WorkDirMode represents which work directory source is selected
type WorkDirMode int

const (
	WorkDirSystemTemp WorkDirMode = iota // Use system temp (/tmp)
	WorkDirConfig                        // Use config.WorkDir
	WorkDirBackup                        // Use config.BackupDir
)

type RestorePreviewModel struct {
	config            *config.Config
	logger            logger.Logger
	parent            tea.Model
	ctx               context.Context
	archive           ArchiveInfo
	mode              string
	targetDB          string
	cleanFirst        bool
	createIfMissing   bool
	cleanClusterFirst bool     // For cluster restore: drop all user databases first
	existingDBCount   int      // Number of existing user databases
	existingDBs       []string // List of existing user databases
	existingDBError   string   // Error message if database listing failed
	safetyChecks      []SafetyCheck
	checking          bool
	canProceed        bool
	message           string
	saveDebugLog      bool        // Save detailed error report on failure
	debugLocks        bool        // Enable detailed lock debugging
	workDir           string      // Resolved work directory path
	workDirMode       WorkDirMode // Which source is selected
}

// NewRestorePreview creates a new restore preview
func NewRestorePreview(cfg *config.Config, log logger.Logger, parent tea.Model, ctx context.Context, archive ArchiveInfo, mode string) RestorePreviewModel {
	// Default target database name from archive
	targetDB := archive.DatabaseName
	if targetDB == "" {
		targetDB = cfg.Database
	}

	return RestorePreviewModel{
		config:          cfg,
		logger:          log,
		parent:          parent,
		ctx:             ctx,
		archive:         archive,
		mode:            mode,
		targetDB:        targetDB,
		cleanFirst:      false,
		createIfMissing: true,
		checking:        true,
		workDir:         cfg.WorkDir, // Use configured work directory
		safetyChecks: []SafetyCheck{
			{Name: "Archive integrity", Status: "pending", Critical: true},
			{Name: "Dump validity", Status: "pending", Critical: true},
			{Name: "Disk space", Status: "pending", Critical: true},
			{Name: "Required tools", Status: "pending", Critical: true},
			{Name: "Target database", Status: "pending", Critical: false},
		},
	}
}

func (m RestorePreviewModel) Init() tea.Cmd {
	return runSafetyChecks(m.config, m.logger, m.archive, m.targetDB)
}

type safetyCheckCompleteMsg struct {
	checks          []SafetyCheck
	canProceed      bool
	existingDBCount int
	existingDBs     []string
	existingDBError string
}

func runSafetyChecks(cfg *config.Config, log logger.Logger, archive ArchiveInfo, targetDB string) tea.Cmd {
	return func() tea.Msg {
		// Dynamic timeout based on archive size for large database support
		// Base: 10 minutes + 1 minute per 5 GB, max 120 minutes
		timeoutMinutes := 10
		if archive.Size > 0 {
			sizeGB := archive.Size / (1024 * 1024 * 1024)
			estimatedMinutes := int(sizeGB/5) + 10
			if estimatedMinutes > timeoutMinutes {
				timeoutMinutes = estimatedMinutes
			}
			if timeoutMinutes > 120 {
				timeoutMinutes = 120
			}
		}

		ctx, cancel := context.WithTimeout(context.Background(), time.Duration(timeoutMinutes)*time.Minute)
		defer cancel()
		_ = ctx // Used by database checks below

		safety := restore.NewSafety(cfg, log)
		checks := []SafetyCheck{}
		canProceed := true

		// 1. Archive integrity
		check := SafetyCheck{Name: "Archive integrity", Status: "checking", Critical: true}
		if err := safety.ValidateArchive(archive.Path); err != nil {
			check.Status = "failed"
			check.Message = err.Error()
			canProceed = false
		} else {
			check.Status = "passed"
			check.Message = "Valid backup archive"
		}
		checks = append(checks, check)

		// 2. Dump validity (deep diagnosis)
		check = SafetyCheck{Name: "Dump validity", Status: "checking", Critical: true}
		diagnoser := restore.NewDiagnoser(log, false)
		diagResult, diagErr := diagnoser.DiagnoseFile(archive.Path)
		if diagErr != nil {
			check.Status = "warning"
			check.Message = fmt.Sprintf("Cannot diagnose: %v", diagErr)
		} else if !diagResult.IsValid {
			check.Status = "failed"
			check.Critical = true
			if diagResult.IsTruncated {
				check.Message = "Dump is TRUNCATED - restore will fail"
			} else if diagResult.IsCorrupted {
				check.Message = "Dump is CORRUPTED - restore will fail"
			} else if len(diagResult.Errors) > 0 {
				check.Message = diagResult.Errors[0]
			} else {
				check.Message = "Dump has validation errors"
			}
			canProceed = false
		} else {
			check.Status = "passed"
			check.Message = "Dump structure verified"
		}
		checks = append(checks, check)

		// 3. Disk space
		check = SafetyCheck{Name: "Disk space", Status: "checking", Critical: true}
		multiplier := 3.0
		if archive.Format.IsClusterBackup() {
			multiplier = 4.0
		}
		if err := safety.CheckDiskSpace(archive.Path, multiplier); err != nil {
			check.Status = "warning"
			check.Message = err.Error()
			// Not critical - just warning
		} else {
			check.Status = "passed"
			check.Message = "Sufficient space available"
		}
		checks = append(checks, check)

		// 4. Required tools
		check = SafetyCheck{Name: "Required tools", Status: "checking", Critical: true}
		dbType := "postgres"
		if archive.Format.IsMySQL() {
			dbType = "mysql"
		}
		if err := safety.VerifyTools(dbType); err != nil {
			check.Status = "failed"
			check.Message = err.Error()
			canProceed = false
		} else {
			check.Status = "passed"
			check.Message = "All required tools available"
		}
		checks = append(checks, check)

		// 5. Target database check (skip for cluster restores)
		existingDBCount := 0
		existingDBs := []string{}

		if !archive.Format.IsClusterBackup() {
			check = SafetyCheck{Name: "Target database", Status: "checking", Critical: false}
			exists, err := safety.CheckDatabaseExists(ctx, targetDB)
			if err != nil {
				check.Status = "warning"
				check.Message = fmt.Sprintf("Cannot check: %v", err)
			} else if exists {
				check.Status = "warning"
				check.Message = fmt.Sprintf("Database '%s' exists - will be overwritten if clean-first enabled", targetDB)
			} else {
				check.Status = "passed"
				check.Message = fmt.Sprintf("Database '%s' does not exist - will be created", targetDB)
			}
			checks = append(checks, check)
		} else {
			// For cluster restores, detect existing user databases
			check = SafetyCheck{Name: "Existing databases", Status: "checking", Critical: false}

			// Get list of existing user databases (exclude templates and system DBs)
			var existingDBError string
			dbList, err := safety.ListUserDatabases(ctx)
			if err != nil {
				check.Status = "warning"
				check.Message = fmt.Sprintf("Cannot list databases: %v", err)
				existingDBError = err.Error()
			} else {
				existingDBCount = len(dbList)
				existingDBs = dbList

				if existingDBCount > 0 {
					check.Status = "warning"
					check.Message = fmt.Sprintf("Found %d existing user database(s) - can be cleaned before restore", existingDBCount)
				} else {
					check.Status = "passed"
					check.Message = "No existing user databases - clean slate"
				}
			}
			checks = append(checks, check)

			return safetyCheckCompleteMsg{
				checks:          checks,
				canProceed:      canProceed,
				existingDBCount: existingDBCount,
				existingDBs:     existingDBs,
				existingDBError: existingDBError,
			}
		}

		return safetyCheckCompleteMsg{
			checks:          checks,
			canProceed:      canProceed,
			existingDBCount: existingDBCount,
			existingDBs:     existingDBs,
		}
	}
}

func (m RestorePreviewModel) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case safetyCheckCompleteMsg:
		m.checking = false
		m.safetyChecks = msg.checks
		m.canProceed = msg.canProceed
		m.existingDBCount = msg.existingDBCount
		m.existingDBs = msg.existingDBs
		m.existingDBError = msg.existingDBError
		// Auto-forward in auto-confirm mode
		if m.config.TUIAutoConfirm {
			return m.parent, tea.Quit
		}
		return m, nil

	case tea.KeyMsg:
		switch msg.String() {
		case "ctrl+c", "q", "esc":
			return m.parent, nil

		case "t":
			// Toggle clean-first
			m.cleanFirst = !m.cleanFirst
			m.message = fmt.Sprintf("Clean-first: %v", m.cleanFirst)

		case "c":
			if m.mode == "restore-cluster" {
				// Toggle cluster cleanup - databases will be re-detected at execution time
				m.cleanClusterFirst = !m.cleanClusterFirst
				if m.cleanClusterFirst {
					if m.existingDBError != "" {
						// Detection failed in preview - will re-detect at execution
						m.message = CheckWarningStyle.Render("[WARN] Will clean existing databases before restore (detection pending)")
					} else if m.existingDBCount > 0 {
						m.message = CheckWarningStyle.Render(fmt.Sprintf("[WARN] Will drop %d existing database(s) before restore", m.existingDBCount))
					} else {
						m.message = infoStyle.Render("[INFO] Cleanup enabled (no databases currently detected)")
					}
				} else {
					m.message = "Clean cluster first: disabled"
				}
			} else {
				// Toggle create if missing
				m.createIfMissing = !m.createIfMissing
				m.message = "Create if missing: " + fmt.Sprintf("%v", m.createIfMissing)
			}

		case "d":
			// Toggle debug log saving
			m.saveDebugLog = !m.saveDebugLog
			if m.saveDebugLog {
				m.message = infoStyle.Render("[DEBUG] Debug log: enabled (will save detailed report on failure)")
			} else {
				m.message = "Debug log: disabled"
			}

		case "l":
			// Toggle lock debugging
			m.debugLocks = !m.debugLocks
			if m.debugLocks {
				m.message = infoStyle.Render("🔍 [LOCK-DEBUG] Lock debugging: ENABLED (captures PostgreSQL lock config, Guard decisions, boost attempts)")
			} else {
				m.message = "Lock debugging: disabled"
			}

		case "w":
			// 3-way toggle: System Temp → Config WorkDir → Backup Dir → System Temp
			switch m.workDirMode {
			case WorkDirSystemTemp:
				// Try config WorkDir next (if set)
				if m.config.WorkDir != "" {
					m.workDirMode = WorkDirConfig
					m.workDir = m.config.WorkDir
					m.message = infoStyle.Render(fmt.Sprintf("[1/3 CONFIG] Work directory: %s", m.workDir))
				} else {
					// Skip to backup dir if no config WorkDir
					m.workDirMode = WorkDirBackup
					m.workDir = m.config.BackupDir
					m.message = infoStyle.Render(fmt.Sprintf("[2/3 BACKUP] Work directory: %s", m.workDir))
				}
			case WorkDirConfig:
				m.workDirMode = WorkDirBackup
				m.workDir = m.config.BackupDir
				m.message = infoStyle.Render(fmt.Sprintf("[2/3 BACKUP] Work directory: %s", m.workDir))
			case WorkDirBackup:
				m.workDirMode = WorkDirSystemTemp
				m.workDir = ""
				m.message = infoStyle.Render("[3/3 SYSTEM] Work directory: /tmp (system temp)")
			}

		case "enter", " ":
			if m.checking {
				m.message = "Please wait for safety checks to complete..."
				return m, nil
			}

			if !m.canProceed {
				m.message = errorStyle.Render("[FAIL] Cannot proceed - critical safety checks failed")
				return m, nil
			}

			// Cluster-specific check: must enable cleanup if existing databases found
			if m.mode == "restore-cluster" && m.existingDBCount > 0 && !m.cleanClusterFirst {
				m.message = errorStyle.Render("[FAIL] Cannot proceed - press 'c' to enable cleanup of " + fmt.Sprintf("%d", m.existingDBCount) + " existing database(s) first")
				return m, nil
			}

			// Proceed to restore execution (enable lock debugging in Config)
			if m.debugLocks {
				m.config.DebugLocks = true
			}
			exec := NewRestoreExecution(m.config, m.logger, m.parent, m.ctx, m.archive, m.targetDB, m.cleanFirst, m.createIfMissing, m.mode, m.cleanClusterFirst, m.existingDBs, m.saveDebugLog, m.workDir)
			return exec, exec.Init()
		}
	}

	return m, nil
}

func (m RestorePreviewModel) View() string {
	var s strings.Builder

	// Title
	title := "[CHECK] Restore Preview"
	if m.mode == "restore-cluster" {
		title = "[CHECK] Cluster Restore Preview"
	}
	s.WriteString(titleStyle.Render(title))
	s.WriteString("\n\n")

	// Archive Information
	s.WriteString(archiveHeaderStyle.Render("[ARCHIVE] Information"))
	s.WriteString("\n")
	s.WriteString(fmt.Sprintf("  File: %s\n", m.archive.Name))
	s.WriteString(fmt.Sprintf("  Format: %s\n", m.archive.Format.String()))
	s.WriteString(fmt.Sprintf("  Size: %s\n", formatSize(m.archive.Size)))
	s.WriteString(fmt.Sprintf("  Created: %s\n", m.archive.Modified.Format("2006-01-02 15:04:05")))
	if m.archive.DatabaseName != "" {
		s.WriteString(fmt.Sprintf("  Database: %s\n", m.archive.DatabaseName))
	}

	// Estimate uncompressed size and RTO
	if m.archive.Format.IsCompressed() {
		// Rough estimate: 3x compression ratio typical for DB dumps
		uncompressedEst := m.archive.Size * 3
		s.WriteString(fmt.Sprintf("  Estimated uncompressed: ~%s\n", formatSize(uncompressedEst)))

		// Estimate RTO
		profile := m.config.GetCurrentProfile()
		if profile != nil {
			extractTime := m.archive.Size / (500 * 1024 * 1024) // 500 MB/s extraction
			if extractTime < 1 {
				extractTime = 1
			}
			restoreSpeed := int64(50 * 1024 * 1024 * int64(profile.Jobs)) // 50MB/s per job
			restoreTime := uncompressedEst / restoreSpeed
			if restoreTime < 1 {
				restoreTime = 1
			}
			totalMinutes := extractTime + restoreTime
			s.WriteString(fmt.Sprintf("  Estimated RTO: ~%dm (with %s profile)\n", totalMinutes, profile.Name))
		}
	}
	s.WriteString("\n")

	// Target Information
	if m.mode == "restore-single" {
		s.WriteString(archiveHeaderStyle.Render("[TARGET] Information"))
		s.WriteString("\n")
		s.WriteString(fmt.Sprintf("  Database: %s\n", m.targetDB))
		s.WriteString(fmt.Sprintf("  Host: %s:%d\n", m.config.Host, m.config.Port))

		cleanIcon := "[N]"
		if m.cleanFirst {
			cleanIcon = "[Y]"
		}
		s.WriteString(fmt.Sprintf("  Clean First: %s %v\n", cleanIcon, m.cleanFirst))

		createIcon := "[N]"
		if m.createIfMissing {
			createIcon = "[Y]"
		}
		s.WriteString(fmt.Sprintf("  Create If Missing: %s %v\n", createIcon, m.createIfMissing))
		s.WriteString("\n")
	} else if m.mode == "restore-cluster" {
		s.WriteString(archiveHeaderStyle.Render("[CLUSTER] Restore Options"))
		s.WriteString("\n")
		s.WriteString(fmt.Sprintf("  Host: %s:%d\n", m.config.Host, m.config.Port))

		// Show Resource Profile and CPU Workload settings
		profile := m.config.GetCurrentProfile()
		if profile != nil {
			s.WriteString(fmt.Sprintf("  Resource Profile: %s (Parallel:%d, Jobs:%d)\n",
				profile.Name, profile.ClusterParallelism, profile.Jobs))
		} else {
			s.WriteString(fmt.Sprintf("  Resource Profile: %s\n", m.config.ResourceProfile))
		}
		// Show Large DB Mode status
		if m.config.LargeDBMode {
			s.WriteString("  Large DB Mode: ON (reduced parallelism, high locks)\n")
		}
		s.WriteString(fmt.Sprintf("  CPU Workload: %s\n", m.config.CPUWorkloadType))
		s.WriteString(fmt.Sprintf("  Cluster Parallelism: %d databases\n", m.config.ClusterParallelism))

		if m.existingDBError != "" {
			// Show warning when database listing failed - but still allow cleanup toggle
			s.WriteString(CheckWarningStyle.Render("  Existing Databases: Detection failed\n"))
			s.WriteString(infoStyle.Render(fmt.Sprintf("    (%s)\n", m.existingDBError)))
			s.WriteString(infoStyle.Render("    (Will re-detect at restore time)\n"))
		} else if m.existingDBCount > 0 {
			s.WriteString(fmt.Sprintf("  Existing Databases: %d found\n", m.existingDBCount))

			// Show first few database names
			maxShow := 5
			for i, db := range m.existingDBs {
				if i >= maxShow {
					remaining := len(m.existingDBs) - maxShow
					s.WriteString(fmt.Sprintf("    ... and %d more\n", remaining))
					break
				}
				s.WriteString(fmt.Sprintf("    - %s\n", db))
			}
		} else {
			s.WriteString("  Existing Databases: None (clean slate)\n")
		}

		// Always show cleanup toggle for cluster restore
		cleanIcon := "[N]"
		cleanStyle := infoStyle
		if m.cleanClusterFirst {
			cleanIcon := "[Y]"
			cleanStyle = CheckWarningStyle
			s.WriteString(cleanStyle.Render(fmt.Sprintf("  Clean All First: %s enabled (press 'c' to toggle)\n", cleanIcon)))
		} else {
			s.WriteString(cleanStyle.Render(fmt.Sprintf("  Clean All First: %s disabled (press 'c' to toggle)\n", cleanIcon)))
		}
		s.WriteString("\n")
	}

	// Safety Checks
	s.WriteString(archiveHeaderStyle.Render("[SAFETY] Checks"))
	s.WriteString("\n")

	if m.checking {
		s.WriteString(infoStyle.Render("  Running safety checks..."))
		s.WriteString("\n")
	} else {
		for _, check := range m.safetyChecks {
			icon := "[ ]"
			style := CheckPendingStyle

			switch check.Status {
			case "passed":
				icon = "[+]"
				style = CheckPassedStyle
			case "failed":
				icon = "[-]"
				style = CheckFailedStyle
			case "warning":
				icon = "[!]"
				style = CheckWarningStyle
			case "checking":
				icon = "[~]"
				style = CheckPendingStyle
			}

			line := fmt.Sprintf("  %s %s", icon, check.Name)
			if check.Message != "" {
				line += fmt.Sprintf(" ... %s", check.Message)
			}
			s.WriteString(style.Render(line))
			s.WriteString("\n")
		}
	}
	s.WriteString("\n")

	// Warnings
	if m.cleanFirst {
		s.WriteString(CheckWarningStyle.Render("[WARN] Warning: Clean-first enabled"))
		s.WriteString("\n")
		s.WriteString(infoStyle.Render("   All existing data in target database will be dropped!"))
		s.WriteString("\n\n")
	}
	if m.cleanClusterFirst {
		s.WriteString(CheckWarningStyle.Render("[DANGER] WARNING: Cluster cleanup enabled"))
		s.WriteString("\n")
		if m.existingDBError != "" {
			s.WriteString(CheckWarningStyle.Render("   Existing databases will be DROPPED before restore!"))
			s.WriteString("\n")
			s.WriteString(infoStyle.Render("   (Database count will be detected at restore time)"))
		} else if m.existingDBCount > 0 {
			s.WriteString(CheckWarningStyle.Render(fmt.Sprintf("   %d existing database(s) will be DROPPED before restore!", m.existingDBCount)))
		} else {
			s.WriteString(infoStyle.Render("   No databases currently detected - cleanup will verify at restore time"))
		}
		s.WriteString("\n")
		s.WriteString(infoStyle.Render("   This ensures a clean disaster recovery scenario"))
		s.WriteString("\n\n")
	}

	// Advanced Options
	s.WriteString(archiveHeaderStyle.Render("[OPTIONS] Advanced"))
	s.WriteString("\n")

	// Work directory option - show current mode clearly
	var workDirIcon, workDirSource, workDirValue string
	workDirStyle := infoStyle

	switch m.workDirMode {
	case WorkDirSystemTemp:
		workDirIcon = "[SYS]"
		workDirSource = "SYSTEM TEMP"
		workDirValue = "/tmp"
	case WorkDirConfig:
		workDirIcon = "[CFG]"
		workDirSource = "CONFIG"
		workDirValue = m.config.WorkDir
		workDirStyle = CheckPassedStyle
	case WorkDirBackup:
		workDirIcon = "[BKP]"
		workDirSource = "BACKUP DIR"
		workDirValue = m.config.BackupDir
		workDirStyle = CheckPassedStyle
	}

	s.WriteString(workDirStyle.Render(fmt.Sprintf("  %s Work Dir [%s]: %s", workDirIcon, workDirSource, workDirValue)))
	s.WriteString("\n")
	s.WriteString(infoStyle.Render("      Press 'w' to cycle: SYSTEM → CONFIG → BACKUP → SYSTEM"))
	s.WriteString("\n")
	if m.workDirMode == WorkDirSystemTemp {
		s.WriteString(CheckWarningStyle.Render("      ⚠ WARN: Large archives need more space than /tmp may have!"))
		s.WriteString("\n")
	}

	// Debug log option
	debugIcon := "[-]"
	debugStyle := infoStyle
	if m.saveDebugLog {
		debugIcon = "[+]"
		debugStyle = CheckPassedStyle
	}
	s.WriteString(debugStyle.Render(fmt.Sprintf("  %s Debug Log: %v (press 'd' to toggle)", debugIcon, m.saveDebugLog)))
	s.WriteString("\n")
	if m.saveDebugLog {
		s.WriteString(infoStyle.Render(fmt.Sprintf("    Saves detailed error report to %s on failure", m.config.GetEffectiveWorkDir())))
		s.WriteString("\n")
	}

	// Lock debugging option
	lockDebugIcon := "[-]"
	lockDebugStyle := infoStyle
	if m.debugLocks {
		lockDebugIcon = "[🔍]"
		lockDebugStyle = CheckPassedStyle
	}
	s.WriteString(lockDebugStyle.Render(fmt.Sprintf("  %s Lock Debug: %v (press 'l' to toggle)", lockDebugIcon, m.debugLocks)))
	s.WriteString("\n")
	if m.debugLocks {
		s.WriteString(infoStyle.Render("    Captures PostgreSQL lock config, Guard decisions, boost attempts"))
		s.WriteString("\n")
	}
	s.WriteString("\n")

	// Message
	if m.message != "" {
		s.WriteString(m.message)
		s.WriteString("\n\n")
	}

	// Footer
	if m.checking {
		s.WriteString(infoStyle.Render("Please wait..."))
	} else if m.canProceed {
		s.WriteString(successStyle.Render("[OK] Ready to restore"))
		s.WriteString("\n")
		if m.mode == "restore-single" {
			s.WriteString(infoStyle.Render("t: Clean-first | c: Create | w: WorkDir | d: Debug | l: LockDebug | Enter: Proceed | Esc: Cancel"))
		} else if m.mode == "restore-cluster" {
			if m.existingDBCount > 0 {
				s.WriteString(infoStyle.Render("c: Cleanup | w: WorkDir | d: Debug | l: LockDebug | Enter: Proceed | Esc: Cancel"))
			} else {
				s.WriteString(infoStyle.Render("w: WorkDir | d: Debug | Enter: Proceed | Esc: Cancel"))
			}
		} else {
			s.WriteString(infoStyle.Render("w: WorkDir | d: Debug | Enter: Proceed | Esc: Cancel"))
		}
	} else {
		s.WriteString(errorStyle.Render("[FAIL] Cannot proceed - please fix errors above"))
		s.WriteString("\n")
		s.WriteString(infoStyle.Render("Esc: Go back"))
	}

	return s.String()
}
