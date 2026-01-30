package cmd

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"html"
	"os"
	"path/filepath"
	"strings"
	"time"

	"dbbackup/internal/catalog"

	"github.com/spf13/cobra"
)

var (
	exportOutput string
	exportFormat string
)

// catalogExportCmd exports catalog to various formats
var catalogExportCmd = &cobra.Command{
	Use:   "export",
	Short: "Export catalog to file (CSV/HTML/JSON)",
	Long: `Export backup catalog to various formats for analysis, reporting, or archival.

Supports:
  - CSV format for spreadsheet import (Excel, LibreOffice)
  - HTML format for web-based reports and documentation
  - JSON format for programmatic access and integration

Examples:
  # Export to CSV
  dbbackup catalog export --format csv --output backups.csv

  # Export to HTML report
  dbbackup catalog export --format html --output report.html

  # Export specific database
  dbbackup catalog export --format csv --database myapp --output myapp_backups.csv

  # Export date range
  dbbackup catalog export --format html --after 2026-01-01 --output january_report.html`,
	RunE: runCatalogExport,
}

func init() {
	catalogCmd.AddCommand(catalogExportCmd)
	catalogExportCmd.Flags().StringVarP(&exportOutput, "output", "o", "", "Output file path (required)")
	catalogExportCmd.Flags().StringVarP(&exportFormat, "format", "f", "csv", "Export format: csv, html, json")
	catalogExportCmd.Flags().StringVar(&catalogDatabase, "database", "", "Filter by database name")
	catalogExportCmd.Flags().StringVar(&catalogStartDate, "after", "", "Show backups after date (YYYY-MM-DD)")
	catalogExportCmd.Flags().StringVar(&catalogEndDate, "before", "", "Show backups before date (YYYY-MM-DD)")
	catalogExportCmd.MarkFlagRequired("output")
}

func runCatalogExport(cmd *cobra.Command, args []string) error {
	if exportOutput == "" {
		return fmt.Errorf("--output flag required")
	}

	// Validate format
	exportFormat = strings.ToLower(exportFormat)
	if exportFormat != "csv" && exportFormat != "html" && exportFormat != "json" {
		return fmt.Errorf("invalid format: %s (supported: csv, html, json)", exportFormat)
	}

	cat, err := openCatalog()
	if err != nil {
		return err
	}
	defer cat.Close()

	ctx := context.Background()

	// Build query
	query := &catalog.SearchQuery{
		Database:  catalogDatabase,
		Limit:     0, // No limit - export all
		OrderBy:   "created_at",
		OrderDesc: false, // Chronological order for exports
	}

	// Parse dates if provided
	if catalogStartDate != "" {
		after, err := time.Parse("2006-01-02", catalogStartDate)
		if err != nil {
			return fmt.Errorf("invalid --after date format (use YYYY-MM-DD): %w", err)
		}
		query.StartDate = &after
	}

	if catalogEndDate != "" {
		before, err := time.Parse("2006-01-02", catalogEndDate)
		if err != nil {
			return fmt.Errorf("invalid --before date format (use YYYY-MM-DD): %w", err)
		}
		query.EndDate = &before
	}

	// Search backups
	entries, err := cat.Search(ctx, query)
	if err != nil {
		return fmt.Errorf("failed to search catalog: %w", err)
	}

	if len(entries) == 0 {
		fmt.Println("No backups found matching criteria")
		return nil
	}

	// Export based on format
	switch exportFormat {
	case "csv":
		return exportCSV(entries, exportOutput)
	case "html":
		return exportHTML(entries, exportOutput, catalogDatabase)
	case "json":
		return exportJSON(entries, exportOutput)
	default:
		return fmt.Errorf("unsupported format: %s", exportFormat)
	}
}

// exportCSV exports entries to CSV format
func exportCSV(entries []*catalog.Entry, outputPath string) error {
	file, err := os.Create(outputPath)
	if err != nil {
		return fmt.Errorf("failed to create output file: %w", err)
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	// Header
	header := []string{
		"ID",
		"Database",
		"DatabaseType",
		"Host",
		"Port",
		"BackupPath",
		"BackupType",
		"SizeBytes",
		"SizeHuman",
		"SHA256",
		"Compression",
		"Encrypted",
		"CreatedAt",
		"DurationSeconds",
		"Status",
		"VerifiedAt",
		"VerifyValid",
		"TestedAt",
		"TestSuccess",
		"RetentionPolicy",
	}

	if err := writer.Write(header); err != nil {
		return fmt.Errorf("failed to write CSV header: %w", err)
	}

	// Data rows
	for _, entry := range entries {
		row := []string{
			fmt.Sprintf("%d", entry.ID),
			entry.Database,
			entry.DatabaseType,
			entry.Host,
			fmt.Sprintf("%d", entry.Port),
			entry.BackupPath,
			entry.BackupType,
			fmt.Sprintf("%d", entry.SizeBytes),
			catalog.FormatSize(entry.SizeBytes),
			entry.SHA256,
			entry.Compression,
			fmt.Sprintf("%t", entry.Encrypted),
			entry.CreatedAt.Format(time.RFC3339),
			fmt.Sprintf("%.2f", entry.Duration),
			string(entry.Status),
			formatTime(entry.VerifiedAt),
			formatBool(entry.VerifyValid),
			formatTime(entry.DrillTestedAt),
			formatBool(entry.DrillSuccess),
			entry.RetentionPolicy,
		}

		if err := writer.Write(row); err != nil {
			return fmt.Errorf("failed to write CSV row: %w", err)
		}
	}

	fmt.Printf("✅ Exported %d backups to CSV: %s\n", len(entries), outputPath)
	fmt.Printf("   Open with Excel, LibreOffice, or other spreadsheet software\n")
	return nil
}

// exportHTML exports entries to HTML format with styling
func exportHTML(entries []*catalog.Entry, outputPath string, database string) error {
	file, err := os.Create(outputPath)
	if err != nil {
		return fmt.Errorf("failed to create output file: %w", err)
	}
	defer file.Close()

	title := "Backup Catalog Report"
	if database != "" {
		title = fmt.Sprintf("Backup Catalog Report: %s", database)
	}

	// Write HTML header with embedded CSS
	htmlHeader := fmt.Sprintf(`<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>%s</title>
    <style>
        body { font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; margin: 20px; background: #f5f5f5; }
        .container { max-width: 1400px; margin: 0 auto; background: white; padding: 30px; box-shadow: 0 2px 10px rgba(0,0,0,0.1); }
        h1 { color: #2c3e50; border-bottom: 3px solid #3498db; padding-bottom: 10px; }
        .summary { background: #ecf0f1; padding: 15px; margin: 20px 0; border-radius: 5px; }
        .summary-item { display: inline-block; margin-right: 30px; }
        .summary-label { font-weight: bold; color: #7f8c8d; }
        .summary-value { color: #2c3e50; font-size: 18px; }
        table { width: 100%%; border-collapse: collapse; margin-top: 20px; }
        th { background: #34495e; color: white; padding: 12px; text-align: left; font-weight: 600; }
        td { padding: 10px; border-bottom: 1px solid #ecf0f1; }
        tr:hover { background: #f8f9fa; }
        .status-success { color: #27ae60; font-weight: bold; }
        .status-fail { color: #e74c3c; font-weight: bold; }
        .badge { padding: 3px 8px; border-radius: 3px; font-size: 12px; font-weight: bold; }
        .badge-encrypted { background: #3498db; color: white; }
        .badge-verified { background: #27ae60; color: white; }
        .badge-tested { background: #9b59b6; color: white; }
        .footer { margin-top: 30px; text-align: center; color: #95a5a6; font-size: 12px; }
    </style>
</head>
<body>
    <div class="container">
        <h1>%s</h1>
`, title, title)

	file.WriteString(htmlHeader)

	// Summary section
	totalSize := int64(0)
	encryptedCount := 0
	verifiedCount := 0
	testedCount := 0

	for _, entry := range entries {
		totalSize += entry.SizeBytes
		if entry.Encrypted {
			encryptedCount++
		}
		if entry.VerifyValid != nil && *entry.VerifyValid {
			verifiedCount++
		}
		if entry.DrillSuccess != nil && *entry.DrillSuccess {
			testedCount++
		}
	}

	var oldestBackup, newestBackup time.Time
	if len(entries) > 0 {
		oldestBackup = entries[0].CreatedAt
		newestBackup = entries[len(entries)-1].CreatedAt
	}

	summaryHTML := fmt.Sprintf(`
        <div class="summary">
            <div class="summary-item">
                <div class="summary-label">Total Backups:</div>
                <div class="summary-value">%d</div>
            </div>
            <div class="summary-item">
                <div class="summary-label">Total Size:</div>
                <div class="summary-value">%s</div>
            </div>
            <div class="summary-item">
                <div class="summary-label">Encrypted:</div>
                <div class="summary-value">%d (%.1f%%)</div>
            </div>
            <div class="summary-item">
                <div class="summary-label">Verified:</div>
                <div class="summary-value">%d (%.1f%%)</div>
            </div>
            <div class="summary-item">
                <div class="summary-label">DR Tested:</div>
                <div class="summary-value">%d (%.1f%%)</div>
            </div>
        </div>
        <div class="summary">
            <div class="summary-item">
                <div class="summary-label">Oldest Backup:</div>
                <div class="summary-value">%s</div>
            </div>
            <div class="summary-item">
                <div class="summary-label">Newest Backup:</div>
                <div class="summary-value">%s</div>
            </div>
            <div class="summary-item">
                <div class="summary-label">Time Span:</div>
                <div class="summary-value">%s</div>
            </div>
        </div>
`,
		len(entries),
		catalog.FormatSize(totalSize),
		encryptedCount, float64(encryptedCount)/float64(len(entries))*100,
		verifiedCount, float64(verifiedCount)/float64(len(entries))*100,
		testedCount, float64(testedCount)/float64(len(entries))*100,
		oldestBackup.Format("2006-01-02 15:04"),
		newestBackup.Format("2006-01-02 15:04"),
		formatTimeSpan(newestBackup.Sub(oldestBackup)),
	)

	file.WriteString(summaryHTML)

	// Table header
	tableHeader := `
        <table>
            <thead>
                <tr>
                    <th>Database</th>
                    <th>Created</th>
                    <th>Size</th>
                    <th>Type</th>
                    <th>Duration</th>
                    <th>Status</th>
                    <th>Attributes</th>
                </tr>
            </thead>
            <tbody>
`
	file.WriteString(tableHeader)

	// Table rows
	for _, entry := range entries {
		badges := []string{}
		if entry.Encrypted {
			badges = append(badges, `<span class="badge badge-encrypted">Encrypted</span>`)
		}
		if entry.VerifyValid != nil && *entry.VerifyValid {
			badges = append(badges, `<span class="badge badge-verified">Verified</span>`)
		}
		if entry.DrillSuccess != nil && *entry.DrillSuccess {
			badges = append(badges, `<span class="badge badge-tested">DR Tested</span>`)
		}

		statusClass := "status-success"
		statusText := string(entry.Status)
		if entry.Status == catalog.StatusFailed {
			statusClass = "status-fail"
		}

		row := fmt.Sprintf(`
                <tr>
                    <td>%s</td>
                    <td>%s</td>
                    <td>%s</td>
                    <td>%s</td>
                    <td>%.1fs</td>
                    <td class="%s">%s</td>
                    <td>%s</td>
                </tr>`,
			html.EscapeString(entry.Database),
			entry.CreatedAt.Format("2006-01-02 15:04:05"),
			catalog.FormatSize(entry.SizeBytes),
			html.EscapeString(entry.BackupType),
			entry.Duration,
			statusClass,
			html.EscapeString(statusText),
			strings.Join(badges, " "),
		)
		file.WriteString(row)
	}

	// Table footer and close HTML
	htmlFooter := `
            </tbody>
        </table>
        <div class="footer">
            Generated by dbbackup on ` + time.Now().Format("2006-01-02 15:04:05") + `
        </div>
    </div>
</body>
</html>
`
	file.WriteString(htmlFooter)

	fmt.Printf("✅ Exported %d backups to HTML: %s\n", len(entries), outputPath)
	fmt.Printf("   Open in browser: file://%s\n", filepath.Join(os.Getenv("PWD"), exportOutput))
	return nil
}

// exportJSON exports entries to JSON format
func exportJSON(entries []*catalog.Entry, outputPath string) error {
	file, err := os.Create(outputPath)
	if err != nil {
		return fmt.Errorf("failed to create output file: %w", err)
	}
	defer file.Close()

	encoder := json.NewEncoder(file)
	encoder.SetIndent("", "  ")

	if err := encoder.Encode(entries); err != nil {
		return fmt.Errorf("failed to encode JSON: %w", err)
	}

	fmt.Printf("✅ Exported %d backups to JSON: %s\n", len(entries), outputPath)
	return nil
}

// formatTime formats *time.Time to string
func formatTime(t *time.Time) string {
	if t == nil {
		return ""
	}
	return t.Format(time.RFC3339)
}

// formatBool formats *bool to string
func formatBool(b *bool) string {
	if b == nil {
		return ""
	}
	if *b {
		return "true"
	}
	return "false"
}

// formatExportDuration formats *time.Duration to string
func formatExportDuration(d *time.Duration) string {
	if d == nil {
		return ""
	}
	return d.String()
}

// formatTimeSpan formats a duration in human-readable form
func formatTimeSpan(d time.Duration) string {
	days := int(d.Hours() / 24)
	if days > 365 {
		years := days / 365
		return fmt.Sprintf("%d years", years)
	}
	if days > 30 {
		months := days / 30
		return fmt.Sprintf("%d months", months)
	}
	if days > 0 {
		return fmt.Sprintf("%d days", days)
	}
	return fmt.Sprintf("%.0f hours", d.Hours())
}
