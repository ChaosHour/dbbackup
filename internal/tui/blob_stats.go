package tui

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"

	"dbbackup/internal/config"
	"dbbackup/internal/logger"
)

// BlobColumn represents a blob/bytea column in the database
type BlobColumn struct {
	Schema    string
	Table     string
	Column    string
	DataType  string
	RowCount  int64
	TotalSize int64
	AvgSize   int64
	MaxSize   int64
	NullCount int64
	Scanned   bool
	ScanError string
}

// BlobStatsView displays blob statistics for a database
type BlobStatsView struct {
	config     *config.Config
	logger     logger.Logger
	parent     tea.Model
	ctx        context.Context
	columns    []BlobColumn
	scanning   bool
	scanned    bool
	err        error
	cursor     int
	totalBlobs int64
	totalSize  int64
}

// NewBlobStatsView creates a new blob statistics view
func NewBlobStatsView(cfg *config.Config, log logger.Logger, parent tea.Model, ctx context.Context) *BlobStatsView {
	return &BlobStatsView{
		config: cfg,
		logger: log,
		parent: parent,
		ctx:    ctx,
	}
}

// blobScanMsg is sent when blob scan completes
type blobScanMsg struct {
	columns    []BlobColumn
	totalBlobs int64
	totalSize  int64
	err        error
}

// Init initializes the model and starts scanning
func (b *BlobStatsView) Init() tea.Cmd {
	b.scanning = true
	return b.scanBlobColumns()
}

// scanBlobColumns scans the database for blob columns
func (b *BlobStatsView) scanBlobColumns() tea.Cmd {
	return func() tea.Msg {
		columns, totalBlobs, totalSize, err := b.discoverBlobColumns()
		return blobScanMsg{
			columns:    columns,
			totalBlobs: totalBlobs,
			totalSize:  totalSize,
			err:        err,
		}
	}
}

// discoverBlobColumns queries information_schema for blob columns
func (b *BlobStatsView) discoverBlobColumns() ([]BlobColumn, int64, int64, error) {
	db, err := openTUIDatabase(b.config, "")
	if err != nil {
		return nil, 0, 0, fmt.Errorf("failed to connect: %w", err)
	}
	defer db.Close()

	ctx, cancel := context.WithTimeout(b.ctx, 30*time.Second)
	defer cancel()

	var columns []BlobColumn
	var totalBlobs, totalSize int64

	if b.config.IsPostgreSQL() {
		columns, err = b.scanPostgresBlobColumns(ctx, db)
	} else {
		columns, err = b.scanMySQLBlobColumns(ctx, db)
	}
	if err != nil {
		return nil, 0, 0, err
	}

	// Calculate sizes for each column (with limits to avoid long scans)
	for i := range columns {
		b.scanColumnStats(ctx, db, &columns[i])
		totalBlobs += columns[i].RowCount - columns[i].NullCount
		totalSize += columns[i].TotalSize
	}

	return columns, totalBlobs, totalSize, nil
}

// scanPostgresBlobColumns finds bytea columns in PostgreSQL
func (b *BlobStatsView) scanPostgresBlobColumns(ctx context.Context, db *sql.DB) ([]BlobColumn, error) {
	query := `
		SELECT 
			table_schema,
			table_name,
			column_name,
			data_type
		FROM information_schema.columns
		WHERE data_type IN ('bytea', 'oid')
		  AND table_schema NOT IN ('pg_catalog', 'information_schema')
		ORDER BY table_schema, table_name, column_name
	`

	rows, err := db.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to query columns: %w", err)
	}
	defer rows.Close()

	var columns []BlobColumn
	for rows.Next() {
		var col BlobColumn
		if err := rows.Scan(&col.Schema, &col.Table, &col.Column, &col.DataType); err != nil {
			continue
		}
		columns = append(columns, col)
	}

	return columns, rows.Err()
}

// scanMySQLBlobColumns finds blob columns in MySQL/MariaDB
func (b *BlobStatsView) scanMySQLBlobColumns(ctx context.Context, db *sql.DB) ([]BlobColumn, error) {
	query := `
		SELECT 
			TABLE_SCHEMA,
			TABLE_NAME,
			COLUMN_NAME,
			DATA_TYPE
		FROM information_schema.COLUMNS
		WHERE DATA_TYPE IN ('blob', 'mediumblob', 'longblob', 'tinyblob', 'binary', 'varbinary')
		  AND TABLE_SCHEMA NOT IN ('mysql', 'information_schema', 'performance_schema', 'sys')
		ORDER BY TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME
	`

	rows, err := db.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to query columns: %w", err)
	}
	defer rows.Close()

	var columns []BlobColumn
	for rows.Next() {
		var col BlobColumn
		if err := rows.Scan(&col.Schema, &col.Table, &col.Column, &col.DataType); err != nil {
			continue
		}
		columns = append(columns, col)
	}

	return columns, rows.Err()
}

// scanColumnStats gets size statistics for a specific column
func (b *BlobStatsView) scanColumnStats(ctx context.Context, db *sql.DB, col *BlobColumn) {
	// Build a safe query to get column stats
	// Use a sampling approach for large tables
	var query string
	fullName := fmt.Sprintf(`"%s"."%s"`, col.Schema, col.Table)
	colName := fmt.Sprintf(`"%s"`, col.Column)

	if b.config.IsPostgreSQL() {
		query = fmt.Sprintf(`
			SELECT 
				COUNT(*),
				COALESCE(SUM(COALESCE(octet_length(%s), 0)), 0),
				COALESCE(AVG(COALESCE(octet_length(%s), 0)), 0),
				COALESCE(MAX(COALESCE(octet_length(%s), 0)), 0),
				COUNT(*) - COUNT(%s)
			FROM %s
		`, colName, colName, colName, colName, fullName)
	} else {
		fullName = fmt.Sprintf("`%s`.`%s`", col.Schema, col.Table)
		colName = fmt.Sprintf("`%s`", col.Column)
		query = fmt.Sprintf(`
			SELECT 
				COUNT(*),
				COALESCE(SUM(COALESCE(LENGTH(%s), 0)), 0),
				COALESCE(AVG(COALESCE(LENGTH(%s), 0)), 0),
				COALESCE(MAX(COALESCE(LENGTH(%s), 0)), 0),
				COUNT(*) - COUNT(%s)
			FROM %s
		`, colName, colName, colName, colName, fullName)
	}

	// Use a timeout for individual table scans
	scanCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	row := db.QueryRowContext(scanCtx, query)
	var avgSize float64
	err := row.Scan(&col.RowCount, &col.TotalSize, &avgSize, &col.MaxSize, &col.NullCount)
	col.AvgSize = int64(avgSize)
	col.Scanned = true

	if err != nil {
		col.ScanError = err.Error()
		if b.logger != nil {
			b.logger.Warn("Failed to scan blob column stats",
				"schema", col.Schema,
				"table", col.Table,
				"column", col.Column,
				"error", err)
		}
	}
}

// Update handles messages
func (b *BlobStatsView) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	tuiDebugLog(b.config, b.logger, "blob_stats", msg)
	switch msg := msg.(type) {
	case tea.InterruptMsg:
		return b.parent, nil

	case blobScanMsg:
		b.scanning = false
		b.scanned = true
		b.columns = msg.columns
		b.totalBlobs = msg.totalBlobs
		b.totalSize = msg.totalSize
		b.err = msg.err
		return b, nil

	case tea.KeyMsg:
		switch msg.String() {
		case "ctrl+c", "q", "esc":
			return b.parent, nil

		case "up", "k":
			if b.cursor > 0 {
				b.cursor--
			}

		case "down", "j":
			if b.cursor < len(b.columns)-1 {
				b.cursor++
			}

		case "r":
			// Refresh scan
			b.scanning = true
			b.scanned = false
			return b, b.scanBlobColumns()
		}
	}

	return b, nil
}

// View renders the blob statistics
func (b *BlobStatsView) View() string {
	var s strings.Builder

	// Header
	s.WriteString("\n")
	s.WriteString(titleStyle.Render("Blob Statistics"))
	s.WriteString("\n\n")

	// Connection info
	dbInfo := fmt.Sprintf("Database: %s@%s:%d/%s (%s)",
		b.config.User, b.config.Host, b.config.Port,
		b.config.Database, b.config.DisplayDatabaseType())
	s.WriteString(infoStyle.Render(dbInfo))
	s.WriteString("\n\n")

	if b.scanning {
		s.WriteString(infoStyle.Render("Scanning database for blob columns..."))
		s.WriteString("\n")
		return s.String()
	}

	if b.err != nil {
		s.WriteString(errorStyle.Render(fmt.Sprintf("Error: %v", b.err)))
		s.WriteString("\n\n")
		s.WriteString(infoStyle.Render("[KEYS] Press Esc to go back | r to retry"))
		return s.String()
	}

	if len(b.columns) == 0 {
		s.WriteString(successStyle.Render("✓ No blob columns found in this database"))
		s.WriteString("\n\n")
		s.WriteString(infoStyle.Render("This database does not contain bytea/blob columns."))
		s.WriteString("\n")
		s.WriteString(infoStyle.Render("[KEYS] Press Esc to go back"))
		return s.String()
	}

	// Summary stats
	summaryStyle := lipgloss.NewStyle().
		Border(lipgloss.RoundedBorder()).
		Padding(0, 1).
		BorderForeground(lipgloss.Color("240"))

	summary := fmt.Sprintf(
		"Found %d blob columns | %s total blob data | %s blobs",
		len(b.columns),
		formatBlobBytes(b.totalSize),
		formatBlobNumber(b.totalBlobs),
	)
	s.WriteString(summaryStyle.Render(summary))
	s.WriteString("\n\n")

	// Column list header
	headerStyle := lipgloss.NewStyle().Bold(true).Foreground(lipgloss.Color("6"))
	s.WriteString(headerStyle.Render(fmt.Sprintf(
		"%-20s %-25s %-15s %10s %12s %12s",
		"Schema", "Table", "Column", "Rows", "Total Size", "Avg Size",
	)))
	s.WriteString("\n")
	s.WriteString(strings.Repeat("─", 100))
	s.WriteString("\n")

	// Column list (show up to 15 visible)
	startIdx := 0
	visibleCount := 15
	if b.cursor >= visibleCount {
		startIdx = b.cursor - visibleCount + 1
	}
	endIdx := startIdx + visibleCount
	if endIdx > len(b.columns) {
		endIdx = len(b.columns)
	}

	for i := startIdx; i < endIdx; i++ {
		col := b.columns[i]
		cursor := " "
		style := menuStyle

		if i == b.cursor {
			cursor = ">"
			style = menuSelectedStyle
		}

		var line string
		if col.ScanError != "" {
			line = fmt.Sprintf("%s %-20s %-25s %-15s %s",
				cursor,
				truncateBlobStr(col.Schema, 20),
				truncateBlobStr(col.Table, 25),
				truncateBlobStr(col.Column, 15),
				errorStyle.Render("scan error"),
			)
		} else {
			line = fmt.Sprintf("%s %-20s %-25s %-15s %10s %12s %12s",
				cursor,
				truncateBlobStr(col.Schema, 20),
				truncateBlobStr(col.Table, 25),
				truncateBlobStr(col.Column, 15),
				formatBlobNumber(col.RowCount),
				formatBlobBytes(col.TotalSize),
				formatBlobBytes(col.AvgSize),
			)
		}
		s.WriteString(style.Render(line))
		s.WriteString("\n")
	}

	// Show scroll indicator if needed
	if len(b.columns) > visibleCount {
		s.WriteString(infoStyle.Render(fmt.Sprintf("\n... showing %d-%d of %d columns", startIdx+1, endIdx, len(b.columns))))
		s.WriteString("\n")
	}

	// Selected column details
	if b.cursor < len(b.columns) {
		col := b.columns[b.cursor]
		s.WriteString("\n")
		detailStyle := lipgloss.NewStyle().
			Border(lipgloss.RoundedBorder()).
			Padding(0, 1).
			BorderForeground(lipgloss.Color("240"))

		detail := fmt.Sprintf(
			"Selected: %s.%s.%s\n"+
				"Type: %s | Rows: %s | Non-NULL: %s | Max Size: %s",
			col.Schema, col.Table, col.Column,
			col.DataType,
			formatBlobNumber(col.RowCount),
			formatBlobNumber(col.RowCount-col.NullCount),
			formatBlobBytes(col.MaxSize),
		)
		s.WriteString(detailStyle.Render(detail))
		s.WriteString("\n")
	}

	// Footer
	s.WriteString("\n")
	s.WriteString(infoStyle.Render("[KEYS] Up/Down to navigate | r to refresh | Esc to go back"))

	return s.String()
}

// formatBlobBytes formats bytes to human readable string
func formatBlobBytes(bytes int64) string {
	const unit = 1024
	if bytes < unit {
		return fmt.Sprintf("%d B", bytes)
	}
	div, exp := int64(unit), 0
	for n := bytes / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %cB", float64(bytes)/float64(div), "KMGTPE"[exp])
}

// formatBlobNumber formats large numbers with commas
func formatBlobNumber(n int64) string {
	if n < 1000 {
		return fmt.Sprintf("%d", n)
	}
	return fmt.Sprintf("%d,%03d", n/1000, n%1000)
}

// truncateBlobStr truncates a string to max length
func truncateBlobStr(s string, max int) string {
	if len(s) <= max {
		return s
	}
	return s[:max-1] + "…"
}
