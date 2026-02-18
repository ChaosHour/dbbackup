package tui

import (
	"context"
	"fmt"
	"strings"
	"time"

	tea "github.com/charmbracelet/bubbletea"

	"dbbackup/internal/config"
	"dbbackup/internal/database"
	"dbbackup/internal/logger"
)

// DatabaseSelectorModel for selecting a database
type DatabaseSelectorModel struct {
	config     *config.Config
	logger     logger.Logger
	parent     tea.Model
	ctx        context.Context
	databases  []string
	cursor     int
	selected   string
	loading    bool
	err        error
	title      string
	message    string
	backupType string // "single" or "sample"
}

func NewDatabaseSelector(cfg *config.Config, log logger.Logger, parent tea.Model, ctx context.Context, title string, backupType string) DatabaseSelectorModel {
	return DatabaseSelectorModel{
		config:     cfg,
		logger:     log,
		parent:     parent,
		ctx:        ctx,
		databases:  []string{"Loading databases..."},
		title:      title,
		loading:    true,
		backupType: backupType,
	}
}

func (m DatabaseSelectorModel) Init() tea.Cmd {
	return fetchDatabases(m.config, m.logger)
}

type databaseListMsg struct {
	databases []string
	err       error
}

func fetchDatabases(cfg *config.Config, log logger.Logger) tea.Cmd {
	return func() tea.Msg {
		// 60 seconds for database listing - busy servers may be slow
		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()

		// Use admin database for connection to avoid failures when
		// the configured database no longer exists (stale config).
		connCfg := *cfg
		if connCfg.IsPostgreSQL() {
			connCfg.Database = "postgres"
		} else if connCfg.IsMySQL() && connCfg.Database == "" {
			connCfg.Database = "mysql"
		}

		dbClient, err := database.New(&connCfg, log)
		if err != nil {
			return databaseListMsg{databases: nil, err: fmt.Errorf("failed to create database client: %w", err)}
		}
		defer dbClient.Close()

		if err := dbClient.Connect(ctx); err != nil {
			return databaseListMsg{databases: nil, err: fmt.Errorf("connection failed: %w", err)}
		}

		databases, err := dbClient.ListDatabases(ctx)
		if err != nil {
			return databaseListMsg{databases: nil, err: fmt.Errorf("failed to list databases: %w", err)}
		}

		return databaseListMsg{databases: databases, err: nil}
	}
}

func (m DatabaseSelectorModel) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	tuiDebugLog(m.config, m.logger, "dbselector", msg)
	switch msg := msg.(type) {
	case databaseListMsg:
		m.loading = false
		if msg.err != nil {
			m.err = msg.err
			m.databases = []string{"Error loading databases"}
		} else {
			m.databases = msg.databases

			// Auto-select database if specified, or first database if auto-confirm is enabled
			autoSelectDB := m.config.TUIAutoDatabase
			if autoSelectDB == "" && m.config.TUIAutoConfirm && len(m.databases) > 0 {
				// Auto-confirm mode: select first database automatically
				autoSelectDB = m.databases[0]
				m.logger.Info("Auto-confirm mode: selecting first database", "database", autoSelectDB)
			}

			if autoSelectDB != "" {
				for i, db := range m.databases {
					if db == autoSelectDB {
						m.cursor = i
						m.selected = db
						m.logger.Info("Auto-selected database", "database", db)

						// If sample backup, ask for ratio (or auto-use default)
						if m.backupType == "sample" {
							if m.config.TUIDryRun || m.config.TUIAutoConfirm {
								// In dry-run or auto-confirm, use default ratio
								executor := NewBackupExecution(m.config, m.logger, m.parent, m.ctx, m.backupType, m.selected, 10)
								return executor, executor.Init()
							}
							inputModel := NewInputModel(m.config, m.logger, m,
								"[STATS] Sample Ratio",
								"Enter sample ratio (1-100):",
								"10",
								ValidateInt(1, 100))
							return inputModel, nil
						}

						// For single backup, go directly to execution
						executor := NewBackupExecution(m.config, m.logger, m.parent, m.ctx, m.backupType, m.selected, 0)
						return executor, executor.Init()
					}
				}
				m.logger.Warn("Auto-database not found in list", "requested", m.config.TUIAutoDatabase)
			}
		}
		return m, nil

	case tea.InterruptMsg:
		// Handle Ctrl+C signal (SIGINT) - Bubbletea v1.3+ sends this instead of KeyMsg for ctrl+c
		return m.parent, nil

	case tea.KeyMsg:
		// Auto-forward ESC/quit in auto-confirm mode
		if m.config.TUIAutoConfirm {
			return m.parent, tea.Quit
		}
		switch msg.String() {
		case "ctrl+c", "q", "esc":
			return m.parent, nil

		case "up", "k":
			if m.cursor > 0 {
				m.cursor--
			}

		case "down", "j":
			if m.cursor < len(m.databases)-1 {
				m.cursor++
			}

		case "p":
			// Show system profile before backup
			profile := NewProfileModel(m.config, m.logger, m)
			return profile, profile.Init()

		case "enter":
			if !m.loading && m.err == nil && len(m.databases) > 0 {
				m.selected = m.databases[m.cursor]

				// If sample backup, ask for ratio first
				if m.backupType == "sample" {
					inputModel := NewInputModel(m.config, m.logger, m,
						"[STATS] Sample Ratio",
						"Enter sample ratio (1-100):",
						"10",
						ValidateInt(1, 100))
					return inputModel, nil
				}

				// For single backup, go directly to execution
				executor := NewBackupExecution(m.config, m.logger, m.parent, m.ctx, m.backupType, m.selected, 0)
				return executor, executor.Init()
			}
		}
	}

	return m, nil
}

func (m DatabaseSelectorModel) View() string {
	var s strings.Builder

	header := titleStyle.Render(m.title)
	s.WriteString(fmt.Sprintf("\n%s\n\n", header))

	if m.loading {
		s.WriteString("[WAIT] Loading databases...\n")
		return s.String()
	}

	if m.err != nil {
		s.WriteString(fmt.Sprintf("[FAIL] Error: %v\n", m.err))
		s.WriteString("\nPress ESC to go back\n")
		return s.String()
	}

	s.WriteString("Select a database:\n\n")

	for i, db := range m.databases {
		cursor := " "
		if m.cursor == i {
			cursor = ">"
			s.WriteString(selectedStyle.Render(fmt.Sprintf("%s %s", cursor, db)))
		} else {
			s.WriteString(fmt.Sprintf("%s %s", cursor, db))
		}
		s.WriteString("\n")
	}

	if m.message != "" {
		s.WriteString(fmt.Sprintf("\n%s\n", m.message))
	}

	s.WriteString("\n[KEYS]  Up/Down: Navigate | Enter: Select | p: Profile | ESC: Back | q: Quit\n")

	return s.String()
}

func (m DatabaseSelectorModel) GetSelected() string {
	return m.selected
}
