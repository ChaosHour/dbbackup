package tui

import (
	"context"
	"database/sql"
	"fmt"
	"sort"
	"strings"
	"time"

	tea "github.com/charmbracelet/bubbletea"
	_ "github.com/go-sql-driver/mysql"
	_ "github.com/jackc/pgx/v5/stdlib"

	"dbbackup/internal/config"
	"dbbackup/internal/logger"
)

const tableSizesTimeout = 30 * time.Second

// TableInfo holds table size information
type TableInfo struct {
	Schema    string
	TableName string
	RowCount  int64
	TotalSize int64
	DataSize  int64
	IndexSize int64
}

// TableSizesView displays table sizes for a database
type TableSizesView struct {
	config   *config.Config
	logger   logger.Logger
	parent   tea.Model
	ctx      context.Context
	database string
	dbType   string
	tables   []TableInfo
	cursor   int
	offset   int
	message  string
	loading  bool
	err      error
}

// NewTableSizesView creates a new table sizes view
func NewTableSizesView(cfg *config.Config, log logger.Logger, parent tea.Model, ctx context.Context) *TableSizesView {
	return &TableSizesView{
		config:   cfg,
		logger:   log,
		parent:   parent,
		ctx:      ctx,
		database: cfg.Database,
		dbType:   cfg.DatabaseType,
		loading:  true,
	}
}

// tableSizesLoadedMsg is sent when table data is loaded
type tableSizesLoadedMsg struct {
	tables []TableInfo
	err    error
}

// Init initializes the view
func (v *TableSizesView) Init() tea.Cmd {
	return v.loadTableSizes()
}

// loadTableSizes fetches table size information
func (v *TableSizesView) loadTableSizes() tea.Cmd {
	return func() tea.Msg {
		tables, err := v.fetchTableSizes()
		return tableSizesLoadedMsg{tables: tables, err: err}
	}
}

// fetchTableSizes queries the database for table sizes
func (v *TableSizesView) fetchTableSizes() ([]TableInfo, error) {
	if v.database == "" {
		return nil, fmt.Errorf("no database specified")
	}

	var db *sql.DB
	var err error

	switch v.dbType {
	case "mysql":
		dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s",
			v.config.User,
			v.config.Password,
			v.config.Host,
			v.config.Port,
			v.database,
		)
		db, err = sql.Open("mysql", dsn)
	default: // postgres
		connStr := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable",
			v.config.Host,
			v.config.Port,
			v.config.User,
			v.config.Password,
			v.database,
		)
		db, err = sql.Open("pgx", connStr)
	}

	if err != nil {
		return nil, fmt.Errorf("failed to connect: %w", err)
	}
	defer db.Close()

	ctx, cancel := context.WithTimeout(v.ctx, tableSizesTimeout)
	defer cancel()

	var tables []TableInfo

	switch v.dbType {
	case "mysql":
		tables, err = v.fetchMySQLTableSizes(ctx, db)
	default:
		tables, err = v.fetchPostgresTableSizes(ctx, db)
	}

	if err != nil {
		return nil, err
	}

	// Sort by total size descending
	sort.Slice(tables, func(i, j int) bool {
		return tables[i].TotalSize > tables[j].TotalSize
	})

	return tables, nil
}

// fetchPostgresTableSizes fetches table sizes for PostgreSQL
func (v *TableSizesView) fetchPostgresTableSizes(ctx context.Context, db *sql.DB) ([]TableInfo, error) {
	query := `
		SELECT 
			schemaname,
			relname,
			n_live_tup,
			pg_total_relation_size(schemaname || '.' || relname) as total_size,
			pg_relation_size(schemaname || '.' || relname) as data_size,
			pg_indexes_size(schemaname || '.' || relname) as index_size
		FROM pg_stat_user_tables
		ORDER BY pg_total_relation_size(schemaname || '.' || relname) DESC
		LIMIT 100
	`

	rows, err := db.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("query failed: %w", err)
	}
	defer rows.Close()

	var tables []TableInfo
	for rows.Next() {
		var t TableInfo
		if err := rows.Scan(&t.Schema, &t.TableName, &t.RowCount, &t.TotalSize, &t.DataSize, &t.IndexSize); err != nil {
			continue
		}
		tables = append(tables, t)
	}

	return tables, nil
}

// fetchMySQLTableSizes fetches table sizes for MySQL
func (v *TableSizesView) fetchMySQLTableSizes(ctx context.Context, db *sql.DB) ([]TableInfo, error) {
	query := `
		SELECT 
			TABLE_SCHEMA,
			TABLE_NAME,
			TABLE_ROWS,
			(DATA_LENGTH + INDEX_LENGTH) as total_size,
			DATA_LENGTH,
			INDEX_LENGTH
		FROM information_schema.TABLES
		WHERE TABLE_SCHEMA = ?
		ORDER BY (DATA_LENGTH + INDEX_LENGTH) DESC
		LIMIT 100
	`

	rows, err := db.QueryContext(ctx, query, v.database)
	if err != nil {
		return nil, fmt.Errorf("query failed: %w", err)
	}
	defer rows.Close()

	var tables []TableInfo
	for rows.Next() {
		var t TableInfo
		var totalSize, dataSize, indexSize sql.NullInt64
		var rowCount sql.NullInt64
		if err := rows.Scan(&t.Schema, &t.TableName, &rowCount, &totalSize, &dataSize, &indexSize); err != nil {
			continue
		}
		t.RowCount = rowCount.Int64
		t.TotalSize = totalSize.Int64
		t.DataSize = dataSize.Int64
		t.IndexSize = indexSize.Int64
		tables = append(tables, t)
	}

	return tables, nil
}

// Update handles messages
func (v *TableSizesView) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tableSizesLoadedMsg:
		v.loading = false
		v.tables = msg.tables
		v.err = msg.err
		return v, nil

	case tea.KeyMsg:
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
			if v.cursor < len(v.tables)-1 {
				v.cursor++
				if v.cursor >= v.offset+15 {
					v.offset++
				}
			}

		case "r":
			v.loading = true
			return v, v.loadTableSizes()
		}
	}

	return v, nil
}

// View renders the table sizes view
func (v *TableSizesView) View() string {
	var s strings.Builder

	s.WriteString("\n")
	s.WriteString(titleStyle.Render("Table Sizes"))
	s.WriteString("\n\n")

	if v.database != "" {
		s.WriteString(infoStyle.Render(fmt.Sprintf("Database: %s (%s)", v.database, v.dbType)))
		s.WriteString("\n\n")
	}

	if v.loading {
		s.WriteString(infoStyle.Render("Loading table sizes..."))
		s.WriteString("\n")
		return s.String()
	}

	if v.err != nil {
		s.WriteString(errorStyle.Render(fmt.Sprintf("Error: %v", v.err)))
		s.WriteString("\n\n")
		s.WriteString(infoStyle.Render("[KEYS] Esc to go back | r to retry"))
		return s.String()
	}

	if len(v.tables) == 0 {
		s.WriteString(infoStyle.Render("No tables found"))
		s.WriteString("\n\n")
		s.WriteString(infoStyle.Render("[KEYS] Esc to go back"))
		return s.String()
	}

	// Calculate totals
	var totalSize, totalRows int64
	for _, t := range v.tables {
		totalSize += t.TotalSize
		totalRows += t.RowCount
	}

	s.WriteString(infoStyle.Render(fmt.Sprintf("Total: %s in %d tables (%s rows)",
		formatBytesTableSize(totalSize), len(v.tables), formatNumber(totalRows))))
	s.WriteString("\n\n")

	// Header
	header := fmt.Sprintf("%-30s %12s %12s %12s %12s",
		"TABLE", "ROWS", "TOTAL", "DATA", "INDEX")
	s.WriteString(headerStyle.Render(header))
	s.WriteString("\n")
	s.WriteString(strings.Repeat("─", 80))
	s.WriteString("\n")

	// Table rows (show 15 at a time)
	displayCount := 15
	if v.offset+displayCount > len(v.tables) {
		displayCount = len(v.tables) - v.offset
	}

	for i := v.offset; i < v.offset+displayCount; i++ {
		t := v.tables[i]
		tableName := t.TableName
		if len(tableName) > 28 {
			tableName = tableName[:28] + ".."
		}
		if t.Schema != "" && t.Schema != v.database && t.Schema != "public" {
			tableName = t.Schema + "." + tableName
			if len(tableName) > 28 {
				tableName = tableName[:28] + ".."
			}
		}

		line := fmt.Sprintf("%-30s %12s %12s %12s %12s",
			tableName,
			formatNumber(t.RowCount),
			formatBytesTableSize(t.TotalSize),
			formatBytesTableSize(t.DataSize),
			formatBytesTableSize(t.IndexSize),
		)

		if i == v.cursor {
			s.WriteString(menuSelectedStyle.Render("> " + line))
		} else {
			s.WriteString(menuStyle.Render("  " + line))
		}
		s.WriteString("\n")
	}

	// Scroll indicator
	if len(v.tables) > 15 {
		s.WriteString("\n")
		s.WriteString(infoStyle.Render(fmt.Sprintf("Showing %d-%d of %d tables",
			v.offset+1, v.offset+displayCount, len(v.tables))))
	}

	s.WriteString("\n\n")
	s.WriteString(infoStyle.Render("[KEYS] Up/Down to navigate | r to refresh | Esc to go back"))

	return s.String()
}

// formatBytesTableSize formats bytes to human readable format
func formatBytesTableSize(b int64) string {
	const unit = 1024
	if b < unit {
		return fmt.Sprintf("%d B", b)
	}
	div, exp := int64(unit), 0
	for n := b / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %cB", float64(b)/float64(div), "KMGTPE"[exp])
}

// formatNumber formats a number with thousand separators
func formatNumber(n int64) string {
	if n < 1000 {
		return fmt.Sprintf("%d", n)
	}
	if n < 1000000 {
		return fmt.Sprintf("%.1fK", float64(n)/1000)
	}
	if n < 1000000000 {
		return fmt.Sprintf("%.1fM", float64(n)/1000000)
	}
	return fmt.Sprintf("%.1fB", float64(n)/1000000000)
}
