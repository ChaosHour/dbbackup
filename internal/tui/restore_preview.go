package tui

import (
	"context"
	"fmt"
	"strings"
	"time"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"

	"dbbackup/internal/config"
	"dbbackup/internal/logger"
	"dbbackup/internal/restore"
)

var (
	previewBoxStyle = lipgloss.NewStyle().
			Border(lipgloss.RoundedBorder()).
			BorderForeground(lipgloss.Color("240")).
			Padding(1, 2)

	checkPassedStyle = lipgloss.NewStyle().
				Foreground(lipgloss.Color("2"))

	checkFailedStyle = lipgloss.NewStyle().
				Foreground(lipgloss.Color("1"))

	checkWarningStyle = lipgloss.NewStyle().
				Foreground(lipgloss.Color("3"))

	checkPendingStyle = lipgloss.NewStyle().
				Foreground(lipgloss.Color("244"))
)

// SafetyCheck represents a pre-restore safety check
type SafetyCheck struct {
	Name     string
	Status   string // "pending", "checking", "passed", "failed", "warning"
	Message  string
	Critical bool
}

// RestorePreviewModel shows restore preview and safety checks
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
	saveDebugLog      bool   // Save detailed error report on failure
	workDir           string // Custom work directory for extraction
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
				// Prevent toggle if we couldn't detect existing databases
				if m.existingDBError != "" {
					m.message = checkWarningStyle.Render("[WARN] Cannot enable cleanup - database detection failed")
				} else {
					// Toggle cluster cleanup
					m.cleanClusterFirst = !m.cleanClusterFirst
					if m.cleanClusterFirst {
						m.message = checkWarningStyle.Render(fmt.Sprintf("[WARN] Will drop %d existing database(s) before restore", m.existingDBCount))
					} else {
						m.message = fmt.Sprintf("Clean cluster first: disabled")
					}
				}
			} else {
				// Toggle create if missing
				m.createIfMissing = !m.createIfMissing
				m.message = fmt.Sprintf("Create if missing: %v", m.createIfMissing)
			}

		case "d":
			// Toggle debug log saving
			m.saveDebugLog = !m.saveDebugLog
			if m.saveDebugLog {
				m.message = infoStyle.Render("[DEBUG] Debug log: enabled (will save detailed report on failure)")
			} else {
				m.message = "Debug log: disabled"
			}

		case "w":
			// Toggle/set work directory
			if m.workDir == "" {
				// Set to backup directory as default alternative
				m.workDir = m.config.BackupDir
				m.message = infoStyle.Render(fmt.Sprintf("[DIR] Work directory set to: %s", m.workDir))
			} else {
				// Clear work directory (use system temp)
				m.workDir = ""
				m.message = "Work directory: using system temp"
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

			// Proceed to restore execution
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

		if m.existingDBError != "" {
			// Show error when database listing failed
			s.WriteString(checkWarningStyle.Render(fmt.Sprintf("  Existing Databases: Unable to detect (%s)\n", m.existingDBError)))
			s.WriteString(infoStyle.Render("  (Cleanup option disabled - cannot verify database status)\n"))
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

			cleanIcon := "[N]"
			cleanStyle := infoStyle
			if m.cleanClusterFirst {
				cleanIcon = "[Y]"
				cleanStyle = checkWarningStyle
			}
			s.WriteString(cleanStyle.Render(fmt.Sprintf("  Clean All First: %s %v (press 'c' to toggle)\n", cleanIcon, m.cleanClusterFirst)))
		} else {
			s.WriteString("  Existing Databases: None (clean slate)\n")
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
			style := checkPendingStyle

			switch check.Status {
			case "passed":
				icon = "[+]"
				style = checkPassedStyle
			case "failed":
				icon = "[-]"
				style = checkFailedStyle
			case "warning":
				icon = "[!]"
				style = checkWarningStyle
			case "checking":
				icon = "[~]"
				style = checkPendingStyle
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
		s.WriteString(checkWarningStyle.Render("[WARN] Warning: Clean-first enabled"))
		s.WriteString("\n")
		s.WriteString(infoStyle.Render("   All existing data in target database will be dropped!"))
		s.WriteString("\n\n")
	}
	if m.cleanClusterFirst && m.existingDBCount > 0 {
		s.WriteString(checkWarningStyle.Render("[DANGER] WARNING: Cluster cleanup enabled"))
		s.WriteString("\n")
		s.WriteString(checkWarningStyle.Render(fmt.Sprintf("   %d existing database(s) will be DROPPED before restore!", m.existingDBCount)))
		s.WriteString("\n")
		s.WriteString(infoStyle.Render("   This ensures a clean disaster recovery scenario"))
		s.WriteString("\n\n")
	}

	// Advanced Options
	s.WriteString(archiveHeaderStyle.Render("[OPTIONS] Advanced"))
	s.WriteString("\n")

	// Work directory option
	workDirIcon := "[-]"
	workDirStyle := infoStyle
	workDirValue := "(system temp)"
	if m.workDir != "" {
		workDirIcon = "[+]"
		workDirStyle = checkPassedStyle
		workDirValue = m.workDir
	}
	s.WriteString(workDirStyle.Render(fmt.Sprintf("  %s Work Dir: %s (press 'w' to toggle)", workDirIcon, workDirValue)))
	s.WriteString("\n")
	if m.workDir == "" {
		s.WriteString(infoStyle.Render("    [WARN] Large archives need more space than /tmp may have"))
		s.WriteString("\n")
	}

	// Debug log option
	debugIcon := "[-]"
	debugStyle := infoStyle
	if m.saveDebugLog {
		debugIcon = "[+]"
		debugStyle = checkPassedStyle
	}
	s.WriteString(debugStyle.Render(fmt.Sprintf("  %s Debug Log: %v (press 'd' to toggle)", debugIcon, m.saveDebugLog)))
	s.WriteString("\n")
	if m.saveDebugLog {
		s.WriteString(infoStyle.Render(fmt.Sprintf("    Saves detailed error report to %s on failure", m.config.GetEffectiveWorkDir())))
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
			s.WriteString(infoStyle.Render("t: Clean-first | c: Create | w: WorkDir | d: Debug | Enter: Proceed | Esc: Cancel"))
		} else if m.mode == "restore-cluster" {
			if m.existingDBCount > 0 {
				s.WriteString(infoStyle.Render("c: Cleanup | w: WorkDir | d: Debug | Enter: Proceed | Esc: Cancel"))
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
