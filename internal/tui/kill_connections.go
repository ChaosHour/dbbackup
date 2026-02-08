package tui

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	tea "github.com/charmbracelet/bubbletea"
	_ "github.com/go-sql-driver/mysql"
	_ "github.com/jackc/pgx/v5/stdlib"

	"dbbackup/internal/config"
	"dbbackup/internal/logger"
)

const killConnectionsTimeout = 30 * time.Second

// ConnectionInfo holds database connection information
type ConnectionInfo struct {
	PID       int
	User      string
	Database  string
	State     string
	Query     string
	Duration  string
	ClientIP  string
}

// KillConnectionsView displays and manages database connections
type KillConnectionsView struct {
	config      *config.Config
	logger      logger.Logger
	parent      tea.Model
	ctx         context.Context
	database    string
	dbType      string
	connections []ConnectionInfo
	cursor      int
	offset      int
	message     string
	loading     bool
	err         error
	confirming  bool
	confirmPID  int
}

// NewKillConnectionsView creates a new kill connections view
func NewKillConnectionsView(cfg *config.Config, log logger.Logger, parent tea.Model, ctx context.Context) *KillConnectionsView {
	return &KillConnectionsView{
		config:   cfg,
		logger:   log,
		parent:   parent,
		ctx:      ctx,
		database: cfg.Database,
		dbType:   cfg.DatabaseType,
		loading:  true,
	}
}

// connectionsLoadedMsg is sent when connection data is loaded
type connectionsLoadedMsg struct {
	connections []ConnectionInfo
	err         error
}

// connectionKilledMsg is sent when a connection is killed
type connectionKilledMsg struct {
	pid int
	err error
}

// Init initializes the view
func (v *KillConnectionsView) Init() tea.Cmd {
	return v.loadConnections()
}

// loadConnections fetches active connections
func (v *KillConnectionsView) loadConnections() tea.Cmd {
	return func() tea.Msg {
		connections, err := v.fetchConnections()
		return connectionsLoadedMsg{connections: connections, err: err}
	}
}

// fetchConnections queries the database for active connections
func (v *KillConnectionsView) fetchConnections() ([]ConnectionInfo, error) {
	var db *sql.DB
	var err error

	switch v.dbType {
	case "mysql":
		dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/",
			v.config.User,
			v.config.Password,
			v.config.Host,
			v.config.Port,
		)
		db, err = sql.Open("mysql", dsn)
	default: // postgres
		connStr := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=postgres sslmode=disable",
			v.config.Host,
			v.config.Port,
			v.config.User,
			v.config.Password,
		)
		db, err = sql.Open("pgx", connStr)
	}

	if err != nil {
		return nil, fmt.Errorf("failed to connect: %w", err)
	}
	defer db.Close()

	ctx, cancel := context.WithTimeout(v.ctx, killConnectionsTimeout)
	defer cancel()

	var connections []ConnectionInfo

	switch v.dbType {
	case "mysql":
		connections, err = v.fetchMySQLConnections(ctx, db)
	default:
		connections, err = v.fetchPostgresConnections(ctx, db)
	}

	return connections, err
}

// fetchPostgresConnections fetches connections for PostgreSQL
func (v *KillConnectionsView) fetchPostgresConnections(ctx context.Context, db *sql.DB) ([]ConnectionInfo, error) {
	query := `
		SELECT 
			pid,
			usename,
			datname,
			state,
			COALESCE(LEFT(query, 50), ''),
			COALESCE(EXTRACT(EPOCH FROM (now() - query_start))::text, '0'),
			COALESCE(client_addr::text, 'local')
		FROM pg_stat_activity
		WHERE pid != pg_backend_pid()
		  AND datname IS NOT NULL
		ORDER BY query_start DESC NULLS LAST
		LIMIT 50
	`

	rows, err := db.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("query failed: %w", err)
	}
	defer rows.Close()

	var connections []ConnectionInfo
	for rows.Next() {
		var c ConnectionInfo
		var duration string
		if err := rows.Scan(&c.PID, &c.User, &c.Database, &c.State, &c.Query, &duration, &c.ClientIP); err != nil {
			continue
		}
		c.Duration = formatDurationFromSeconds(duration)
		connections = append(connections, c)
	}

	return connections, nil
}

// fetchMySQLConnections fetches connections for MySQL
func (v *KillConnectionsView) fetchMySQLConnections(ctx context.Context, db *sql.DB) ([]ConnectionInfo, error) {
	query := `
		SELECT 
			ID,
			USER,
			COALESCE(DB, ''),
			COMMAND,
			COALESCE(LEFT(INFO, 50), ''),
			COALESCE(TIME, 0),
			COALESCE(HOST, '')
		FROM information_schema.PROCESSLIST
		WHERE ID != CONNECTION_ID()
		ORDER BY TIME DESC
		LIMIT 50
	`

	rows, err := db.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("query failed: %w", err)
	}
	defer rows.Close()

	var connections []ConnectionInfo
	for rows.Next() {
		var c ConnectionInfo
		var timeVal int
		if err := rows.Scan(&c.PID, &c.User, &c.Database, &c.State, &c.Query, &timeVal, &c.ClientIP); err != nil {
			continue
		}
		c.Duration = fmt.Sprintf("%ds", timeVal)
		connections = append(connections, c)
	}

	return connections, nil
}

// killConnection terminates a database connection
func (v *KillConnectionsView) killConnection(pid int) tea.Cmd {
	return func() tea.Msg {
		err := v.doKillConnection(pid)
		return connectionKilledMsg{pid: pid, err: err}
	}
}

// doKillConnection executes the kill command
func (v *KillConnectionsView) doKillConnection(pid int) error {
	var db *sql.DB
	var err error

	switch v.dbType {
	case "mysql":
		dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/",
			v.config.User,
			v.config.Password,
			v.config.Host,
			v.config.Port,
		)
		db, err = sql.Open("mysql", dsn)
	default: // postgres
		connStr := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=postgres sslmode=disable",
			v.config.Host,
			v.config.Port,
			v.config.User,
			v.config.Password,
		)
		db, err = sql.Open("pgx", connStr)
	}

	if err != nil {
		return fmt.Errorf("failed to connect: %w", err)
	}
	defer db.Close()

	ctx, cancel := context.WithTimeout(v.ctx, killConnectionsTimeout)
	defer cancel()

	switch v.dbType {
	case "mysql":
		_, err = db.ExecContext(ctx, fmt.Sprintf("KILL %d", pid))
	default:
		_, err = db.ExecContext(ctx, "SELECT pg_terminate_backend($1)", pid)
	}

	return err
}

// killAllConnections terminates all connections to the selected database
func (v *KillConnectionsView) killAllConnections() tea.Cmd {
	return func() tea.Msg {
		err := v.doKillAllConnections()
		return connectionKilledMsg{pid: -1, err: err}
	}
}

// doKillAllConnections executes kill for all connections to a database
func (v *KillConnectionsView) doKillAllConnections() error {
	if v.database == "" {
		return fmt.Errorf("no database selected")
	}

	var db *sql.DB
	var err error

	switch v.dbType {
	case "mysql":
		dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/",
			v.config.User,
			v.config.Password,
			v.config.Host,
			v.config.Port,
		)
		db, err = sql.Open("mysql", dsn)
	default: // postgres
		connStr := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=postgres sslmode=disable",
			v.config.Host,
			v.config.Port,
			v.config.User,
			v.config.Password,
		)
		db, err = sql.Open("pgx", connStr)
	}

	if err != nil {
		return fmt.Errorf("failed to connect: %w", err)
	}
	defer db.Close()

	ctx, cancel := context.WithTimeout(v.ctx, killConnectionsTimeout)
	defer cancel()

	switch v.dbType {
	case "mysql":
		// MySQL: need to get PIDs and kill them one by one
		rows, err := db.QueryContext(ctx, "SELECT ID FROM information_schema.PROCESSLIST WHERE DB = ? AND ID != CONNECTION_ID()", v.database)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var pid int
			if err := rows.Scan(&pid); err != nil {
				continue
			}
			db.ExecContext(ctx, fmt.Sprintf("KILL %d", pid))
		}
	default:
		// PostgreSQL: terminate all backends for the database
		_, err = db.ExecContext(ctx, `
			SELECT pg_terminate_backend(pid) 
			FROM pg_stat_activity 
			WHERE datname = $1 
			  AND pid != pg_backend_pid()`, v.database)
	}

	return err
}

// Update handles messages
func (v *KillConnectionsView) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case connectionsLoadedMsg:
		v.loading = false
		v.connections = msg.connections
		v.err = msg.err
		return v, nil

	case connectionKilledMsg:
		if msg.err != nil {
			v.message = errorStyle.Render(fmt.Sprintf("Failed to kill: %v", msg.err))
		} else if msg.pid == -1 {
			v.message = successStyle.Render(fmt.Sprintf("Killed all connections to %s", v.database))
		} else {
			v.message = successStyle.Render(fmt.Sprintf("Killed connection PID %d", msg.pid))
		}
		v.confirming = false
		v.loading = true
		return v, v.loadConnections()

	case tea.KeyMsg:
		if v.confirming {
			switch msg.String() {
			case "y", "Y":
				v.confirming = false
				if v.confirmPID == -1 {
					return v, v.killAllConnections()
				}
				return v, v.killConnection(v.confirmPID)
			case "ctrl+c", "n", "N", "esc":
				v.confirming = false
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
			if v.cursor < len(v.connections)-1 {
				v.cursor++
				if v.cursor >= v.offset+12 {
					v.offset++
				}
			}

		case "enter", "x":
			if len(v.connections) > 0 {
				v.confirming = true
				v.confirmPID = v.connections[v.cursor].PID
				v.message = StatusWarningStyle.Render(fmt.Sprintf("Kill connection PID %d? [y/N]", v.confirmPID))
			}

		case "a", "A":
			if v.database != "" {
				v.confirming = true
				v.confirmPID = -1
				v.message = StatusWarningStyle.Render(fmt.Sprintf("Kill ALL connections to '%s'? [y/N]", v.database))
			}

		case "r":
			v.loading = true
			return v, v.loadConnections()
		}
	}

	return v, nil
}

// View renders the kill connections view
func (v *KillConnectionsView) View() string {
	var s strings.Builder

	s.WriteString("\n")
	s.WriteString(titleStyle.Render("Kill Connections"))
	s.WriteString("\n\n")

	dbInfo := fmt.Sprintf("Server: %s:%d (%s)", v.config.Host, v.config.Port, v.dbType)
	if v.database != "" {
		dbInfo += fmt.Sprintf(" | Filter: %s", v.database)
	}
	s.WriteString(infoStyle.Render(dbInfo))
	s.WriteString("\n\n")

	if v.loading {
		s.WriteString(infoStyle.Render("Loading connections..."))
		s.WriteString("\n")
		return s.String()
	}

	if v.err != nil {
		s.WriteString(errorStyle.Render(fmt.Sprintf("Error: %v", v.err)))
		s.WriteString("\n\n")
		s.WriteString(infoStyle.Render("[KEYS] Esc to go back | r to retry"))
		return s.String()
	}

	if len(v.connections) == 0 {
		s.WriteString(infoStyle.Render("No active connections found"))
		s.WriteString("\n\n")
		s.WriteString(infoStyle.Render("[KEYS] Esc to go back | r to refresh"))
		return s.String()
	}

	s.WriteString(infoStyle.Render(fmt.Sprintf("Active connections: %d", len(v.connections))))
	s.WriteString("\n\n")

	// Header
	header := fmt.Sprintf("%-7s %-12s %-15s %-10s %-8s %-25s",
		"PID", "USER", "DATABASE", "STATE", "TIME", "QUERY")
	s.WriteString(headerStyle.Render(header))
	s.WriteString("\n")
	s.WriteString(strings.Repeat("─", 80))
	s.WriteString("\n")

	// Connection rows (show 12 at a time)
	displayCount := 12
	if v.offset+displayCount > len(v.connections) {
		displayCount = len(v.connections) - v.offset
	}

	for i := v.offset; i < v.offset+displayCount; i++ {
		c := v.connections[i]
		
		user := c.User
		if len(user) > 10 {
			user = user[:10] + ".."
		}
		
		database := c.Database
		if len(database) > 13 {
			database = database[:13] + ".."
		}
		
		state := c.State
		if len(state) > 8 {
			state = state[:8] + ".."
		}
		
		query := strings.ReplaceAll(c.Query, "\n", " ")
		if len(query) > 23 {
			query = query[:23] + ".."
		}

		line := fmt.Sprintf("%-7d %-12s %-15s %-10s %-8s %-25s",
			c.PID, user, database, state, c.Duration, query)

		if i == v.cursor {
			s.WriteString(menuSelectedStyle.Render("> " + line))
		} else {
			s.WriteString(menuStyle.Render("  " + line))
		}
		s.WriteString("\n")
	}

	// Message area
	if v.message != "" {
		s.WriteString("\n")
		s.WriteString(v.message)
	}

	s.WriteString("\n\n")
	if v.database != "" {
		s.WriteString(infoStyle.Render("[KEYS] Enter/x=kill selected | a=kill ALL | r=refresh | Esc=back"))
	} else {
		s.WriteString(infoStyle.Render("[KEYS] Enter/x=kill selected | r=refresh | Esc=back"))
	}

	return s.String()
}

// formatDurationFromSeconds formats duration from seconds string
func formatDurationFromSeconds(seconds string) string {
	// Parse the duration and format nicely
	var secs float64
	fmt.Sscanf(seconds, "%f", &secs)
	if secs < 60 {
		return fmt.Sprintf("%.0fs", secs)
	}
	if secs < 3600 {
		return fmt.Sprintf("%.0fm", secs/60)
	}
	return fmt.Sprintf("%.1fh", secs/3600)
}
