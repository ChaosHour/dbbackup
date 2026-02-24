package tui

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	tea "github.com/charmbracelet/bubbletea"

	"dbbackup/internal/config"
	"dbbackup/internal/logger"
)

// BlobExtractView provides a TUI for extracting/externalizing LOBs from a database
type BlobExtractView struct {
	config    *config.Config
	logger    logger.Logger
	parent    tea.Model
	ctx       context.Context
	columns   []blobExtractColumn
	scanning  bool
	scanned   bool
	err       error
	cursor    int
	message   string
	exporting bool
	exported  int
	phase     string // "scan" | "confirm" | "export" | "done"
}

type blobExtractColumn struct {
	Schema   string
	Table    string
	Column   string
	DataType string
	RowCount int64
	TotalMB  float64
	Selected bool
}

// blobExtractScanMsg carries scan results
type blobExtractScanMsg struct {
	columns []blobExtractColumn
	err     error
}

// blobExtractExportMsg carries export results
type blobExtractExportMsg struct {
	exported int
	outputDir string
	err      error
}

func NewBlobExtractView(cfg *config.Config, log logger.Logger, parent tea.Model, ctx context.Context) *BlobExtractView {
	return &BlobExtractView{
		config: cfg,
		logger: log,
		parent: parent,
		ctx:    ctx,
		phase:  "scan",
	}
}

func (v *BlobExtractView) Init() tea.Cmd {
	v.scanning = true
	return v.scanBlobColumns()
}

func (v *BlobExtractView) scanBlobColumns() tea.Cmd {
	cfg := v.config
	ctx := v.ctx
	return func() tea.Msg {
		db, err := openTUIDatabase(cfg, "")
		if err != nil {
			return blobExtractScanMsg{err: fmt.Errorf("failed to connect: %w", err)}
		}
		defer func() { _ = db.Close() }()

		queryCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
		defer cancel()

		var columns []blobExtractColumn
		if cfg.IsPostgreSQL() {
			columns, err = scanPostgreSQLBlobColumns(queryCtx, db)
		} else {
			columns, err = scanMySQLBlobColumns(queryCtx, db, cfg.Database)
		}
		return blobExtractScanMsg{columns: columns, err: err}
	}
}

func scanPostgreSQLBlobColumns(ctx context.Context, db *sql.DB) ([]blobExtractColumn, error) {
	query := `
		SELECT c.table_schema, c.table_name, c.column_name, c.data_type,
		       COALESCE(s.n_live_tup, 0) as row_count
		FROM information_schema.columns c
		LEFT JOIN pg_stat_user_tables s 
		  ON s.schemaname = c.table_schema AND s.relname = c.table_name
		WHERE c.data_type IN ('bytea', 'oid')
		  AND c.table_schema NOT IN ('pg_catalog', 'information_schema')
		ORDER BY COALESCE(s.n_live_tup, 0) DESC`

	rows, err := db.QueryContext(ctx, query)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	var columns []blobExtractColumn
	for rows.Next() {
		var col blobExtractColumn
		if err := rows.Scan(&col.Schema, &col.Table, &col.Column, &col.DataType, &col.RowCount); err != nil {
			continue
		}
		col.Selected = true
		columns = append(columns, col)
	}

	// Try to get sizes for each column (may be slow for large tables)
	for i := range columns {
		col := &columns[i]
		sizeQuery := fmt.Sprintf(
			`SELECT COALESCE(SUM(octet_length(%s))/1048576.0, 0) FROM %s.%s WHERE %s IS NOT NULL`,
			col.Column, col.Schema, col.Table, col.Column)
		ctx2, cancel := context.WithTimeout(ctx, 5*time.Second)
		_ = db.QueryRowContext(ctx2, sizeQuery).Scan(&col.TotalMB)
		cancel()
	}

	return columns, nil
}

func scanMySQLBlobColumns(ctx context.Context, db *sql.DB, database string) ([]blobExtractColumn, error) {
	query := `
		SELECT c.TABLE_SCHEMA, c.TABLE_NAME, c.COLUMN_NAME, c.DATA_TYPE,
		       COALESCE(t.TABLE_ROWS, 0) as row_count
		FROM information_schema.COLUMNS c
		LEFT JOIN information_schema.TABLES t 
		  ON t.TABLE_SCHEMA = c.TABLE_SCHEMA AND t.TABLE_NAME = c.TABLE_NAME
		WHERE c.DATA_TYPE IN ('blob', 'mediumblob', 'longblob', 'tinyblob', 'binary', 'varbinary')
		  AND c.TABLE_SCHEMA = ?
		ORDER BY COALESCE(t.TABLE_ROWS, 0) DESC`

	rows, err := db.QueryContext(ctx, query, database)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	var columns []blobExtractColumn
	for rows.Next() {
		var col blobExtractColumn
		if err := rows.Scan(&col.Schema, &col.Table, &col.Column, &col.DataType, &col.RowCount); err != nil {
			continue
		}
		col.Selected = true
		columns = append(columns, col)
	}

	return columns, nil
}

func (v *BlobExtractView) exportBlobData() tea.Cmd {
	cfg := v.config
	log := v.logger
	ctx := v.ctx
	columns := v.columns
	return func() tea.Msg {
		outputDir := filepath.Join(cfg.BackupDir, "blob_extract_"+time.Now().Format("20060102_150405"))
		if err := os.MkdirAll(outputDir, 0700); err != nil {
			return blobExtractExportMsg{err: fmt.Errorf("failed to create output directory: %w", err)}
		}

		db, err := openTUIDatabase(cfg, "")
		if err != nil {
			return blobExtractExportMsg{err: fmt.Errorf("failed to connect: %w", err)}
		}
		defer func() { _ = db.Close() }()

		exported := 0
		for _, col := range columns {
			if !col.Selected {
				continue
			}

			colDir := filepath.Join(outputDir, fmt.Sprintf("%s_%s_%s", col.Schema, col.Table, col.Column))
			if err := os.MkdirAll(colDir, 0700); err != nil {
				continue
			}

			var pkCol string
			if cfg.IsPostgreSQL() {
				// Get primary key column
				pkQuery := `SELECT a.attname FROM pg_index i
					JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
					WHERE i.indrelid = ($1 || '.' || $2)::regclass AND i.indisprimary
					LIMIT 1`
				_ = db.QueryRowContext(ctx, pkQuery, col.Schema, col.Table).Scan(&pkCol)
			} else {
				pkQuery := `SELECT COLUMN_NAME FROM information_schema.KEY_COLUMN_USAGE 
					WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND CONSTRAINT_NAME = 'PRIMARY' LIMIT 1`
				_ = db.QueryRowContext(ctx, pkQuery, col.Schema, col.Table).Scan(&pkCol)
			}

			if pkCol == "" {
				pkCol = "ctid" // PostgreSQL row identifier fallback
				if cfg.IsMySQL() {
					continue // MySQL needs a PK for blob export
				}
			}

			// Export blobs as individual files
			var exportQuery string
			if cfg.IsPostgreSQL() {
				exportQuery = fmt.Sprintf(
					`SELECT %s::text, %s FROM %s.%s WHERE %s IS NOT NULL LIMIT 10000`,
					pkCol, col.Column, col.Schema, col.Table, col.Column)
			} else {
				exportQuery = fmt.Sprintf(
					"SELECT CAST(`%s` AS CHAR), `%s` FROM `%s`.`%s` WHERE `%s` IS NOT NULL LIMIT 10000",
					pkCol, col.Column, col.Schema, col.Table, col.Column)
			}

			rows, err := db.QueryContext(ctx, exportQuery)
			if err != nil {
				log.Warn("Failed to export blob column", "table", col.Table, "column", col.Column, "error", err)
				continue
			}

			for rows.Next() {
				var pkValue string
				var blobData []byte
				if err := rows.Scan(&pkValue, &blobData); err != nil {
					continue
				}
				if len(blobData) == 0 {
					continue
				}

				// Sanitize filename
				safePK := strings.ReplaceAll(pkValue, "/", "_")
				safePK = strings.ReplaceAll(safePK, "..", "_")
				filename := filepath.Join(colDir, fmt.Sprintf("row_%s.bin", safePK))
				if err := os.WriteFile(filename, blobData, 0600); err != nil {
					continue
				}
				exported++
			}
			_ = rows.Close()
		}

		return blobExtractExportMsg{exported: exported, outputDir: outputDir}
	}
}

func (v *BlobExtractView) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	tuiDebugLog(v.config, v.logger, "blob_extract", msg)
	switch msg := msg.(type) {
	case blobExtractScanMsg:
		v.scanning = false
		v.scanned = true
		if msg.err != nil {
			v.err = msg.err
		} else {
			v.columns = msg.columns
			v.phase = "confirm"
		}
		return v, nil

	case blobExtractExportMsg:
		v.exporting = false
		if msg.err != nil {
			v.err = msg.err
			v.phase = "confirm"
		} else {
			v.exported = msg.exported
			v.message = fmt.Sprintf("[OK] Exported %d BLOBs to %s", msg.exported, msg.outputDir)
			v.phase = "done"
		}
		return v, nil

	case tea.InterruptMsg:
		return v.parent, nil

	case tea.KeyMsg:
		switch msg.String() {
		case "ctrl+c", "q", "esc":
			return v.parent, nil

		case "up", "k":
			if v.cursor > 0 {
				v.cursor--
			}

		case "down", "j":
			if v.cursor < len(v.columns)-1 {
				v.cursor++
			}

		case " ": // Toggle selection
			if v.phase == "confirm" && v.cursor < len(v.columns) {
				v.columns[v.cursor].Selected = !v.columns[v.cursor].Selected
			}

		case "a": // Select all
			if v.phase == "confirm" {
				for i := range v.columns {
					v.columns[i].Selected = true
				}
			}

		case "n": // Deselect all
			if v.phase == "confirm" {
				for i := range v.columns {
					v.columns[i].Selected = false
				}
			}

		case "enter":
			if v.phase == "confirm" && len(v.columns) > 0 {
				hasSelected := false
				for _, col := range v.columns {
					if col.Selected {
						hasSelected = true
						break
					}
				}
				if hasSelected {
					v.exporting = true
					v.phase = "export"
					return v, v.exportBlobData()
				}
				v.message = warnStyle.Render("[WARN] No columns selected")
			}
			if v.phase == "done" {
				return v.parent, nil
			}
		}
	}

	return v, nil
}

func (v *BlobExtractView) View() string {
	var s strings.Builder

	s.WriteString("\n" + titleStyle.Render("Blob Extract (Externalize LOBs)") + "\n\n")

	if v.scanning {
		s.WriteString(infoStyle.Render("[SCAN] Scanning database for BLOB/bytea columns...") + "\n")
		return s.String()
	}

	if v.err != nil {
		s.WriteString(errorStyle.Render(fmt.Sprintf("[FAIL] %v", v.err)) + "\n\n")
		s.WriteString("[KEYS] Press Esc to go back\n")
		return s.String()
	}

	if v.exporting {
		s.WriteString(infoStyle.Render("[EXPORT] Exporting BLOB data to files...") + "\n")
		return s.String()
	}

	if v.phase == "done" {
		s.WriteString(successStyle.Render(v.message) + "\n\n")
		s.WriteString("[KEYS] Press Enter or Esc to go back\n")
		return s.String()
	}

	if len(v.columns) == 0 {
		s.WriteString(infoStyle.Render("[NONE] No BLOB/bytea columns found in this database") + "\n\n")
		s.WriteString("[KEYS] Press Esc to go back\n")
		return s.String()
	}

	s.WriteString(fmt.Sprintf("Found %d BLOB/bytea columns:\n\n", len(v.columns)))

	// Column headers
	s.WriteString(fmt.Sprintf("  %-3s %-30s %-15s %-10s %-12s\n",
		"SEL", "TABLE.COLUMN", "TYPE", "ROWS", "SIZE (MB)"))
	s.WriteString("  " + strings.Repeat("-", 72) + "\n")

	for i, col := range v.columns {
		sel := "[ ]"
		if col.Selected {
			sel = "[x]"
		}
		cursor := "  "
		if v.cursor == i {
			cursor = "> "
		}

		tableCol := fmt.Sprintf("%s.%s.%s", col.Schema, col.Table, col.Column)
		if len(tableCol) > 30 {
			tableCol = tableCol[:27] + "..."
		}

		line := fmt.Sprintf("%s%s %-30s %-15s %-10d %-12.1f",
			cursor, sel, tableCol, col.DataType, col.RowCount, col.TotalMB)

		if v.cursor == i {
			s.WriteString(menuSelectedStyle.Render(line) + "\n")
		} else {
			s.WriteString(menuStyle.Render(line) + "\n")
		}
	}

	s.WriteString("\n")
	if v.message != "" {
		s.WriteString(v.message + "\n")
	}

	s.WriteString(infoStyle.Render("BLOBs will be exported as individual files (max 10,000 per column)") + "\n")
	s.WriteString(infoStyle.Render(fmt.Sprintf("Output: %s/blob_extract_<timestamp>/", v.config.BackupDir)) + "\n\n")
	s.WriteString("[KEYS] Space: Toggle | a: All | n: None | Enter: Export | Esc: Back\n")

	return s.String()
}
