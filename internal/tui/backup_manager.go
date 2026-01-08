package tui

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"time"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/mattn/go-runewidth"

	"dbbackup/internal/config"
	"dbbackup/internal/logger"
)

// OperationState represents the current operation state
type OperationState int

const (
	OpIdle OperationState = iota
	OpVerifying
	OpDeleting
)

// BackupManagerModel manages backup archives
type BackupManagerModel struct {
	config       *config.Config
	logger       logger.Logger
	parent       tea.Model
	ctx          context.Context
	archives     []ArchiveInfo
	cursor       int
	loading      bool
	err          error
	message      string
	totalSize    int64
	freeSpace    int64
	opState      OperationState
	opTarget     string // Name of archive being operated on
	spinnerFrame int
}

// NewBackupManager creates a new backup manager
func NewBackupManager(cfg *config.Config, log logger.Logger, parent tea.Model, ctx context.Context) BackupManagerModel {
	return BackupManagerModel{
		config:       cfg,
		logger:       log,
		parent:       parent,
		ctx:          ctx,
		loading:      true,
		opState:      OpIdle,
		spinnerFrame: 0,
	}
}

func (m BackupManagerModel) Init() tea.Cmd {
	return tea.Batch(loadArchives(m.config, m.logger), managerTickCmd())
}

// Tick for spinner animation
type managerTickMsg time.Time

func managerTickCmd() tea.Cmd {
	return tea.Tick(100*time.Millisecond, func(t time.Time) tea.Msg {
		return managerTickMsg(t)
	})
}

// Verify result message
type verifyResultMsg struct {
	archive string
	valid   bool
	err     error
	details string
}

func (m BackupManagerModel) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case managerTickMsg:
		// Update spinner frame
		m.spinnerFrame = (m.spinnerFrame + 1) % len(spinnerFrames)
		return m, managerTickCmd()

	case verifyResultMsg:
		m.opState = OpIdle
		m.opTarget = ""
		if msg.err != nil {
			m.message = fmt.Sprintf("[-] Verify failed: %v", msg.err)
		} else if msg.valid {
			m.message = fmt.Sprintf("[+] %s: Valid - %s", msg.archive, msg.details)
			// Update archive validity in list
			for i := range m.archives {
				if m.archives[i].Name == msg.archive {
					m.archives[i].Valid = true
					break
				}
			}
		} else {
			m.message = fmt.Sprintf("[-] %s: Invalid - %s", msg.archive, msg.details)
			for i := range m.archives {
				if m.archives[i].Name == msg.archive {
					m.archives[i].Valid = false
					break
				}
			}
		}
		return m, nil

	case archiveListMsg:
		m.loading = false
		if msg.err != nil {
			m.err = msg.err
			return m, nil
		}
		m.archives = msg.archives

		// Calculate total size
		m.totalSize = 0
		for _, archive := range m.archives {
			m.totalSize += archive.Size
		}

		// Get free space (simplified - just show message)
		m.message = fmt.Sprintf("Loaded %d archive(s)", len(m.archives))
		// Auto-forward in auto-confirm mode
		if m.config.TUIAutoConfirm {
			return m.parent, tea.Quit
		}
		return m, nil

	case tea.KeyMsg:
		// Allow escape/cancel even during operations
		if msg.String() == "ctrl+c" || msg.String() == "esc" || msg.String() == "q" {
			if m.opState != OpIdle {
				// Cancel current operation
				m.opState = OpIdle
				m.opTarget = ""
				m.message = "Operation cancelled"
				return m, nil
			}
			return m.parent, nil
		}

		// Block other input during operations
		if m.opState != OpIdle {
			return m, nil
		}

		switch msg.String() {
		case "up", "k":
			if m.cursor > 0 {
				m.cursor--
			}

		case "down", "j":
			if m.cursor < len(m.archives)-1 {
				m.cursor++
			}

		case "v":
			// Verify archive with real verification
			if len(m.archives) > 0 && m.cursor < len(m.archives) {
				selected := m.archives[m.cursor]
				m.opState = OpVerifying
				m.opTarget = selected.Name
				m.message = ""
				return m, verifyArchiveCmd(selected)
			}

		case "d":
			// Delete archive (with confirmation)
			if len(m.archives) > 0 && m.cursor < len(m.archives) {
				selected := m.archives[m.cursor]
				archivePath := selected.Path
				confirm := NewConfirmationModelWithAction(m.config, m.logger, m,
					"[DELETE]  Delete Archive",
					fmt.Sprintf("Delete archive '%s'? This cannot be undone.", selected.Name),
					func() (tea.Model, tea.Cmd) {
						// Delete the archive
						err := deleteArchive(archivePath)
						if err != nil {
							m.err = fmt.Errorf("failed to delete archive: %v", err)
							m.message = fmt.Sprintf("[FAIL] Failed to delete: %v", err)
						} else {
							m.message = fmt.Sprintf("[OK] Deleted: %s", selected.Name)
						}
						// Refresh the archive list
						m.loading = true
						return m, loadArchives(m.config, m.logger)
					})
				return confirm, nil
			}

		case "i":
			// Show info
			if len(m.archives) > 0 && m.cursor < len(m.archives) {
				selected := m.archives[m.cursor]
				m.message = fmt.Sprintf("[PKG] %s | %s | %s | Modified: %s",
					selected.Name,
					selected.Format.String(),
					formatSize(selected.Size),
					selected.Modified.Format("2006-01-02 15:04:05"))
			}

		case "r":
			// Restore selected archive
			if len(m.archives) > 0 && m.cursor < len(m.archives) {
				selected := m.archives[m.cursor]
				mode := "restore-single"
				if selected.Format.IsClusterBackup() {
					mode = "restore-cluster"
				}
				preview := NewRestorePreview(m.config, m.logger, m.parent, m.ctx, selected, mode)
				return preview, preview.Init()
			}

		case "R":
			// Refresh list
			m.loading = true
			m.message = "Refreshing..."
			return m, loadArchives(m.config, m.logger)
		}
	}

	return m, nil
}

func (m BackupManagerModel) View() string {
	var s strings.Builder
	const boxWidth = 60

	// Helper to pad string to box width (handles UTF-8)
	padToWidth := func(text string, width int) string {
		textWidth := runewidth.StringWidth(text)
		if textWidth >= width {
			return runewidth.Truncate(text, width-3, "...")
		}
		return text + strings.Repeat(" ", width-textWidth)
	}

	// Title
	s.WriteString(titleStyle.Render("[DB]  Backup Archive Manager"))
	s.WriteString("\n\n")

	// Operation Status Box (always visible)
	s.WriteString("+--[ STATUS ]" + strings.Repeat("-", boxWidth-13) + "+\n")
	switch m.opState {
	case OpVerifying:
		spinner := spinnerFrames[m.spinnerFrame]
		statusText := fmt.Sprintf(" %s Verifying: %s", spinner, m.opTarget)
		s.WriteString("|" + padToWidth(statusText, boxWidth) + "|\n")
	case OpDeleting:
		spinner := spinnerFrames[m.spinnerFrame]
		statusText := fmt.Sprintf(" %s Deleting: %s", spinner, m.opTarget)
		s.WriteString("|" + padToWidth(statusText, boxWidth) + "|\n")
	default:
		if m.loading {
			spinner := spinnerFrames[m.spinnerFrame]
			statusText := fmt.Sprintf(" %s Loading archives...", spinner)
			s.WriteString("|" + padToWidth(statusText, boxWidth) + "|\n")
		} else if m.message != "" {
			msgText := " " + m.message
			s.WriteString("|" + padToWidth(msgText, boxWidth) + "|\n")
		} else {
			s.WriteString("|" + padToWidth(" Ready", boxWidth) + "|\n")
		}
	}
	s.WriteString("+" + strings.Repeat("-", boxWidth) + "+\n\n")

	if m.loading {
		return s.String()
	}

	if m.err != nil {
		s.WriteString(errorStyle.Render(fmt.Sprintf("[FAIL] Error: %v", m.err)))
		s.WriteString("\n\n")
		s.WriteString(infoStyle.Render("Press Esc to go back"))
		return s.String()
	}

	// Summary
	s.WriteString(infoStyle.Render(fmt.Sprintf("Total Archives: %d  |  Total Size: %s",
		len(m.archives), formatSize(m.totalSize))))
	s.WriteString("\n\n")

	// Archives list
	if len(m.archives) == 0 {
		s.WriteString(infoStyle.Render("No backup archives found"))
		s.WriteString("\n\n")
		s.WriteString(infoStyle.Render("Press Esc to go back"))
		return s.String()
	}

	// Column headers with better alignment
	s.WriteString(archiveHeaderStyle.Render(fmt.Sprintf("     %-32s %-22s %10s  %-16s",
		"FILENAME", "FORMAT", "SIZE", "MODIFIED")))
	s.WriteString("\n")
	s.WriteString(strings.Repeat("-", 90))
	s.WriteString("\n")

	// Show archives (limit to visible area)
	start := m.cursor - 5
	if start < 0 {
		start = 0
	}
	end := start + 12
	if end > len(m.archives) {
		end = len(m.archives)
	}

	for i := start; i < end; i++ {
		archive := m.archives[i]
		cursor := "  "
		style := archiveNormalStyle

		if i == m.cursor {
			cursor = "> "
			style = archiveSelectedStyle
		}

		// Status icon - consistent 4-char width
		statusIcon := " [+]"
		if !archive.Valid {
			statusIcon = " [-]"
			style = archiveInvalidStyle
		} else if time.Since(archive.Modified) > 30*24*time.Hour {
			statusIcon = " [!]"
		}

		filename := truncate(archive.Name, 32)
		format := truncate(archive.Format.String(), 22)

		line := fmt.Sprintf("%s%s %-32s %-22s %10s  %-16s",
			cursor,
			statusIcon,
			filename,
			format,
			formatSize(archive.Size),
			archive.Modified.Format("2006-01-02 15:04"))

		s.WriteString(style.Render(line))
		s.WriteString("\n")
	}

	// Footer
	s.WriteString("\n")

	s.WriteString(infoStyle.Render(fmt.Sprintf("Selected: %d/%d", m.cursor+1, len(m.archives))))
	s.WriteString("\n\n")

	// Grouped keyboard shortcuts - simple aligned format
	s.WriteString("SHORTCUTS: Up/Down=Move | r=Restore | v=Verify | d=Delete | i=Info | R=Refresh | Esc=Back | q=Quit")

	return s.String()
}

// verifyArchiveCmd runs actual archive verification
func verifyArchiveCmd(archive ArchiveInfo) tea.Cmd {
	return func() tea.Msg {
		// Determine verification method based on format
		var valid bool
		var details string
		var err error

		switch {
		case strings.HasSuffix(archive.Path, ".tar.gz") || strings.HasSuffix(archive.Path, ".tgz"):
			// Verify tar.gz archive
			cmd := exec.Command("tar", "-tzf", archive.Path)
			output, cmdErr := cmd.CombinedOutput()
			if cmdErr != nil {
				return verifyResultMsg{archive: archive.Name, valid: false, err: nil, details: "Archive corrupt or incomplete"}
			}
			lines := strings.Split(string(output), "\n")
			fileCount := 0
			for _, l := range lines {
				if l != "" {
					fileCount++
				}
			}
			valid = true
			details = fmt.Sprintf("%d files in archive", fileCount)

		case strings.HasSuffix(archive.Path, ".dump") || strings.HasSuffix(archive.Path, ".sql"):
			// Verify PostgreSQL dump with pg_restore --list
			cmd := exec.Command("pg_restore", "--list", archive.Path)
			output, cmdErr := cmd.CombinedOutput()
			if cmdErr != nil {
				// Try as plain SQL
				if strings.HasSuffix(archive.Path, ".sql") {
					// Just check file is readable and has content
					fi, statErr := os.Stat(archive.Path)
					if statErr == nil && fi.Size() > 0 {
						valid = true
						details = "Plain SQL file readable"
					} else {
						return verifyResultMsg{archive: archive.Name, valid: false, err: nil, details: "File empty or unreadable"}
					}
				} else {
					return verifyResultMsg{archive: archive.Name, valid: false, err: nil, details: "pg_restore cannot read dump"}
				}
			} else {
				lines := strings.Split(string(output), "\n")
				objectCount := 0
				for _, l := range lines {
					if l != "" && !strings.HasPrefix(l, ";") {
						objectCount++
					}
				}
				valid = true
				details = fmt.Sprintf("%d objects in dump", objectCount)
			}

		case strings.HasSuffix(archive.Path, ".sql.gz"):
			// Verify gzipped SQL
			cmd := exec.Command("gzip", "-t", archive.Path)
			if cmdErr := cmd.Run(); cmdErr != nil {
				return verifyResultMsg{archive: archive.Name, valid: false, err: nil, details: "Gzip archive corrupt"}
			}
			valid = true
			details = "Gzip integrity OK"

		default:
			// Unknown format - just check file exists and has size
			fi, statErr := os.Stat(archive.Path)
			if statErr != nil {
				return verifyResultMsg{archive: archive.Name, valid: false, err: statErr, details: "Cannot access file"}
			}
			if fi.Size() == 0 {
				return verifyResultMsg{archive: archive.Name, valid: false, err: nil, details: "File is empty"}
			}
			valid = true
			details = "File exists and has content"
		}

		return verifyResultMsg{archive: archive.Name, valid: valid, err: err, details: details}
	}
}

// deleteArchive deletes a backup archive (to be called from confirmation)
func deleteArchive(archivePath string) error {
	return os.Remove(archivePath)
}
