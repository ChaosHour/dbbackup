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

const dropDatabaseTimeout = 30 * time.Second

// DropDatabaseView handles database drop with confirmation
type DropDatabaseView struct {
	config      *config.Config
	logger      logger.Logger
	parent      tea.Model
	ctx         context.Context
	database    string
	dbType      string
	databases   []string
	cursor      int
	offset      int
	message     string
	loading     bool
	err         error
	confirmStep int // 0=none, 1=first confirm, 2=type name
	typedName   string
}

// NewDropDatabaseView creates a new drop database view
func NewDropDatabaseView(cfg *config.Config, log logger.Logger, parent tea.Model, ctx context.Context) *DropDatabaseView {
	return &DropDatabaseView{
		config:   cfg,
		logger:   log,
		parent:   parent,
		ctx:      ctx,
		database: cfg.Database,
		dbType:   cfg.DatabaseType,
		loading:  true,
	}
}

// databasesLoadedMsg is sent when database list is loaded
type databasesLoadedMsg struct {
	databases []string
	err       error
}

// databaseDroppedMsg is sent when a database is dropped
type databaseDroppedMsg struct {
	database string
	err      error
}

// Init initializes the view
func (v *DropDatabaseView) Init() tea.Cmd {
	return v.loadDatabases()
}

// loadDatabases fetches available databases
func (v *DropDatabaseView) loadDatabases() tea.Cmd {
	return func() tea.Msg {
		databases, err := v.fetchDatabases()
		return databasesLoadedMsg{databases: databases, err: err}
	}
}

// fetchDatabases queries the database server for database list
func (v *DropDatabaseView) fetchDatabases() ([]string, error) {
	db, err := openTUIDatabase(v.config, "postgres")
	if err != nil {
		return nil, fmt.Errorf("failed to connect: %w", err)
	}
	defer db.Close()

	ctx, cancel := context.WithTimeout(v.ctx, dropDatabaseTimeout)
	defer cancel()

	var query string
	switch v.dbType {
	case "mysql":
		query = "SHOW DATABASES"
	default:
		query = "SELECT datname FROM pg_database WHERE datistemplate = false ORDER BY datname"
	}

	rows, err := db.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("query failed: %w", err)
	}
	defer rows.Close()

	var databases []string
	systemDBs := map[string]bool{
		"postgres":           true,
		"template0":          true,
		"template1":          true,
		"information_schema": true,
		"mysql":              true,
		"performance_schema": true,
		"sys":                true,
	}

	for rows.Next() {
		var dbName string
		if err := rows.Scan(&dbName); err != nil {
			continue
		}
		// Skip system databases
		if !systemDBs[dbName] {
			databases = append(databases, dbName)
		}
	}

	return databases, nil
}

// dropDatabase executes the drop command
func (v *DropDatabaseView) dropDatabase(dbName string) tea.Cmd {
	return func() tea.Msg {
		err := v.doDropDatabase(dbName)
		return databaseDroppedMsg{database: dbName, err: err}
	}
}

// doDropDatabase executes the DROP DATABASE command
func (v *DropDatabaseView) doDropDatabase(dbName string) error {
	db, err := openTUIDatabase(v.config, "postgres")
	if err != nil {
		return fmt.Errorf("failed to connect: %w", err)
	}
	defer db.Close()

	ctx, cancel := context.WithTimeout(v.ctx, dropDatabaseTimeout*2)
	defer cancel()

	// First, terminate all connections to the database
	switch v.dbType {
	case "mysql":
		// MySQL: kill connections
		rows, err := db.QueryContext(ctx, "SELECT ID FROM information_schema.PROCESSLIST WHERE DB = ?", dbName)
		if err == nil {
			defer rows.Close()
			for rows.Next() {
				var pid int
				if rows.Scan(&pid) == nil {
					db.ExecContext(ctx, fmt.Sprintf("KILL %d", pid))
				}
			}
		}
	default:
		// PostgreSQL: terminate backends
		_, _ = db.ExecContext(ctx, `
			SELECT pg_terminate_backend(pid) 
			FROM pg_stat_activity 
			WHERE datname = $1 
			  AND pid != pg_backend_pid()`, dbName)
	}

	// Now drop the database
	// Note: DDL statements can't use parameterized queries for identifiers.
	// Use proper validation + quoting to prevent SQL injection.
	switch v.dbType {
	case "mysql":
		if err := validateDBIdentifier(dbName, 64); err != nil {
			return fmt.Errorf("invalid database name: %w", err)
		}
		_, err = db.ExecContext(ctx, fmt.Sprintf("DROP DATABASE %s", database.QuoteMySQLIdentifier(dbName)))
	default:
		if err := validateDBIdentifier(dbName, 63); err != nil {
			return fmt.Errorf("invalid database name: %w", err)
		}
		_, err = db.ExecContext(ctx, fmt.Sprintf("DROP DATABASE %s", database.QuotePGIdentifier(dbName)))
	}

	if err != nil {
		return fmt.Errorf("DROP DATABASE failed: %w", err)
	}

	v.logger.Info("Dropped database", "database", dbName)
	return nil
}

// Update handles messages
func (v *DropDatabaseView) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	tuiDebugLog(v.config, v.logger, "drop_database", msg)
	switch msg := msg.(type) {
	case tea.InterruptMsg:
		return v.parent, nil

	case databasesLoadedMsg:
		v.loading = false
		v.databases = msg.databases
		v.err = msg.err
		return v, nil

	case databaseDroppedMsg:
		if msg.err != nil {
			v.message = errorStyle.Render(fmt.Sprintf("Failed to drop: %v", msg.err))
		} else {
			v.message = successStyle.Render(fmt.Sprintf("✓ Database '%s' dropped successfully", msg.database))
		}
		v.confirmStep = 0
		v.typedName = ""
		v.loading = true
		return v, v.loadDatabases()

	case tea.KeyMsg:
		// Handle confirmation steps
		if v.confirmStep == 2 {
			switch msg.String() {
			case "enter":
				if v.typedName == v.databases[v.cursor] {
					return v, v.dropDatabase(v.databases[v.cursor])
				}
				v.message = errorStyle.Render("Database name doesn't match. Cancelled.")
				v.confirmStep = 0
				v.typedName = ""
				return v, nil
			case "ctrl+c", "esc":
				v.confirmStep = 0
				v.typedName = ""
				v.message = ""
				return v, nil
			case "backspace":
				if len(v.typedName) > 0 {
					v.typedName = v.typedName[:len(v.typedName)-1]
				}
				return v, nil
			default:
				// Add character if printable
				if len(msg.String()) == 1 {
					v.typedName += msg.String()
				}
				return v, nil
			}
		}

		if v.confirmStep == 1 {
			switch msg.String() {
			case "y", "Y":
				v.confirmStep = 2
				v.typedName = ""
				v.message = StatusWarningStyle.Render(fmt.Sprintf("Type '%s' to confirm: ", v.databases[v.cursor]))
				return v, nil
			case "ctrl+c", "n", "N", "esc":
				v.confirmStep = 0
				v.message = ""
				return v, nil
			}
			return v, nil
		}

		switch msg.String() {
		case "ctrl+c", "q", "esc":
			return v.parent, nil

		case "up", "k":
			if v.cursor > 0 {
				v.cursor--
				if v.cursor < v.offset {
					v.offset = v.cursor
				}
			}

		case "down", "j":
			if v.cursor < len(v.databases)-1 {
				v.cursor++
				if v.cursor >= v.offset+15 {
					v.offset++
				}
			}

		case "enter", "d":
			if len(v.databases) > 0 {
				v.confirmStep = 1
				v.message = StatusWarningStyle.Render(fmt.Sprintf("⚠ DROP DATABASE '%s'? This is IRREVERSIBLE! [y/N]", v.databases[v.cursor]))
			}

		case "r":
			v.loading = true
			return v, v.loadDatabases()
		}
	}

	return v, nil
}

// View renders the drop database view
func (v *DropDatabaseView) View() string {
	var s strings.Builder

	s.WriteString("\n")
	s.WriteString(titleStyle.Render("⚠ Drop Database"))
	s.WriteString("\n\n")

	s.WriteString(errorStyle.Render("WARNING: This operation is IRREVERSIBLE!"))
	s.WriteString("\n")
	s.WriteString(infoStyle.Render(fmt.Sprintf("Server: %s:%d (%s)", v.config.Host, v.config.Port, v.dbType)))
	s.WriteString("\n\n")

	if v.loading {
		s.WriteString(infoStyle.Render("Loading databases..."))
		s.WriteString("\n")
		return s.String()
	}

	if v.err != nil {
		s.WriteString(errorStyle.Render(fmt.Sprintf("Error: %v", v.err)))
		s.WriteString("\n\n")
		s.WriteString(infoStyle.Render("[KEYS] Esc to go back | r to retry"))
		return s.String()
	}

	if len(v.databases) == 0 {
		s.WriteString(infoStyle.Render("No user databases found"))
		s.WriteString("\n\n")
		s.WriteString(infoStyle.Render("[KEYS] Esc to go back"))
		return s.String()
	}

	s.WriteString(infoStyle.Render(fmt.Sprintf("User databases: %d (system databases hidden)", len(v.databases))))
	s.WriteString("\n\n")

	// Database list (show 15 at a time)
	displayCount := 15
	if v.offset+displayCount > len(v.databases) {
		displayCount = len(v.databases) - v.offset
	}

	for i := v.offset; i < v.offset+displayCount; i++ {
		dbName := v.databases[i]

		if i == v.cursor {
			s.WriteString(menuSelectedStyle.Render(fmt.Sprintf("> %s", dbName)))
		} else {
			s.WriteString(menuStyle.Render(fmt.Sprintf("  %s", dbName)))
		}
		s.WriteString("\n")
	}

	// Scroll indicator
	if len(v.databases) > 15 {
		s.WriteString("\n")
		s.WriteString(infoStyle.Render(fmt.Sprintf("Showing %d-%d of %d databases",
			v.offset+1, v.offset+displayCount, len(v.databases))))
	}

	// Message area (confirmation prompts)
	if v.message != "" {
		s.WriteString("\n\n")
		s.WriteString(v.message)
		if v.confirmStep == 2 {
			s.WriteString(v.typedName)
			s.WriteString("_")
		}
	}

	s.WriteString("\n\n")
	if v.confirmStep == 0 {
		s.WriteString(infoStyle.Render("[KEYS] Enter/d=drop selected | r=refresh | Esc=back"))
	}

	return s.String()
}

// validateDBIdentifier checks that a database name is safe for use in DDL statements.
// Rejects empty names, names exceeding maxLen, and names with control characters or null bytes.
func validateDBIdentifier(name string, maxLen int) error {
	if len(name) == 0 {
		return fmt.Errorf("database name cannot be empty")
	}
	if len(name) > maxLen {
		return fmt.Errorf("database name too long (max %d chars)", maxLen)
	}
	for _, c := range name {
		if c == 0 { // null byte
			return fmt.Errorf("database name contains null byte")
		}
	}
	return nil
}

// quotePGIdent safely quotes a PostgreSQL identifier by doubling internal double-quotes.
// Deprecated: Use database.QuotePGIdentifier instead.
func quotePGIdent(name string) string {
	return database.QuotePGIdentifier(name)
}

// quoteMySQLIdent safely quotes a MySQL identifier by doubling internal backticks.
// Deprecated: Use database.QuoteMySQLIdentifier instead.
func quoteMySQLIdent(name string) string {
	return database.QuoteMySQLIdentifier(name)
}
