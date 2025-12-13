package cmd

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"dbbackup/internal/catalog"
	"dbbackup/internal/report"

	"github.com/spf13/cobra"
)

var reportCmd = &cobra.Command{
	Use:   "report",
	Short: "Generate compliance reports",
	Long: `Generate compliance reports for various regulatory frameworks.

Supported frameworks:
  - soc2      SOC 2 Type II Trust Service Criteria
  - gdpr      General Data Protection Regulation
  - hipaa     Health Insurance Portability and Accountability Act
  - pci-dss   Payment Card Industry Data Security Standard
  - iso27001  ISO 27001 Information Security Management

Examples:
  # Generate SOC2 report for the last 90 days
  dbbackup report generate --type soc2 --days 90

  # Generate HIPAA report as HTML
  dbbackup report generate --type hipaa --format html --output report.html

  # Show report summary for current period
  dbbackup report summary --type soc2`,
}

var reportGenerateCmd = &cobra.Command{
	Use:   "generate",
	Short: "Generate a compliance report",
	Long:  "Generate a compliance report for a specified framework and time period",
	RunE:  runReportGenerate,
}

var reportSummaryCmd = &cobra.Command{
	Use:   "summary",
	Short: "Show compliance summary",
	Long:  "Display a quick compliance summary for the specified framework",
	RunE:  runReportSummary,
}

var reportListCmd = &cobra.Command{
	Use:   "list",
	Short: "List available frameworks",
	Long:  "Display all available compliance frameworks",
	RunE:  runReportList,
}

var reportControlsCmd = &cobra.Command{
	Use:   "controls [framework]",
	Short: "List controls for a framework",
	Long:  "Display all controls for a specific compliance framework",
	Args:  cobra.ExactArgs(1),
	RunE:  runReportControls,
}

var (
	reportType       string
	reportDays       int
	reportStartDate  string
	reportEndDate    string
	reportFormat     string
	reportOutput     string
	reportCatalog    string
	reportTitle      string
	includeEvidence  bool
)

func init() {
	rootCmd.AddCommand(reportCmd)
	reportCmd.AddCommand(reportGenerateCmd)
	reportCmd.AddCommand(reportSummaryCmd)
	reportCmd.AddCommand(reportListCmd)
	reportCmd.AddCommand(reportControlsCmd)

	// Generate command flags
	reportGenerateCmd.Flags().StringVarP(&reportType, "type", "t", "soc2", "Report type (soc2, gdpr, hipaa, pci-dss, iso27001)")
	reportGenerateCmd.Flags().IntVarP(&reportDays, "days", "d", 90, "Number of days to include in report")
	reportGenerateCmd.Flags().StringVar(&reportStartDate, "start", "", "Start date (YYYY-MM-DD)")
	reportGenerateCmd.Flags().StringVar(&reportEndDate, "end", "", "End date (YYYY-MM-DD)")
	reportGenerateCmd.Flags().StringVarP(&reportFormat, "format", "f", "markdown", "Output format (json, markdown, html)")
	reportGenerateCmd.Flags().StringVarP(&reportOutput, "output", "o", "", "Output file path")
	reportGenerateCmd.Flags().StringVar(&reportCatalog, "catalog", "", "Path to backup catalog database")
	reportGenerateCmd.Flags().StringVar(&reportTitle, "title", "", "Custom report title")
	reportGenerateCmd.Flags().BoolVar(&includeEvidence, "evidence", true, "Include evidence in report")

	// Summary command flags
	reportSummaryCmd.Flags().StringVarP(&reportType, "type", "t", "soc2", "Report type")
	reportSummaryCmd.Flags().IntVarP(&reportDays, "days", "d", 90, "Number of days to include")
	reportSummaryCmd.Flags().StringVar(&reportCatalog, "catalog", "", "Path to backup catalog database")
}

func runReportGenerate(cmd *cobra.Command, args []string) error {
	// Determine time period
	var startDate, endDate time.Time
	endDate = time.Now()

	if reportStartDate != "" {
		parsed, err := time.Parse("2006-01-02", reportStartDate)
		if err != nil {
			return fmt.Errorf("invalid start date: %w", err)
		}
		startDate = parsed
	} else {
		startDate = endDate.AddDate(0, 0, -reportDays)
	}

	if reportEndDate != "" {
		parsed, err := time.Parse("2006-01-02", reportEndDate)
		if err != nil {
			return fmt.Errorf("invalid end date: %w", err)
		}
		endDate = parsed
	}

	// Determine report type
	rptType := parseReportType(reportType)
	if rptType == "" {
		return fmt.Errorf("unknown report type: %s", reportType)
	}

	// Get catalog path
	catalogPath := reportCatalog
	if catalogPath == "" {
		homeDir, _ := os.UserHomeDir()
		catalogPath = filepath.Join(homeDir, ".dbbackup", "catalog.db")
	}

	// Open catalog
	cat, err := catalog.NewSQLiteCatalog(catalogPath)
	if err != nil {
		return fmt.Errorf("failed to open catalog: %w", err)
	}
	defer cat.Close()

	// Configure generator
	config := report.ReportConfig{
		Type:            rptType,
		PeriodStart:     startDate,
		PeriodEnd:       endDate,
		CatalogPath:     catalogPath,
		OutputFormat:    parseOutputFormat(reportFormat),
		OutputPath:      reportOutput,
		IncludeEvidence: includeEvidence,
	}

	if reportTitle != "" {
		config.Title = reportTitle
	}

	// Generate report
	gen := report.NewGenerator(cat, config)
	rpt, err := gen.Generate()
	if err != nil {
		return fmt.Errorf("failed to generate report: %w", err)
	}

	// Get formatter
	formatter := report.GetFormatter(config.OutputFormat)

	// Write output
	var output *os.File
	if reportOutput != "" {
		output, err = os.Create(reportOutput)
		if err != nil {
			return fmt.Errorf("failed to create output file: %w", err)
		}
		defer output.Close()
	} else {
		output = os.Stdout
	}

	if err := formatter.Format(rpt, output); err != nil {
		return fmt.Errorf("failed to format report: %w", err)
	}

	if reportOutput != "" {
		fmt.Printf("Report generated: %s\n", reportOutput)
		fmt.Printf("  Type:       %s\n", rpt.Type)
		fmt.Printf("  Status:     %s %s\n", report.StatusIcon(rpt.Status), rpt.Status)
		fmt.Printf("  Score:      %.1f%%\n", rpt.Score)
		fmt.Printf("  Findings:   %d open\n", rpt.Summary.OpenFindings)
	}

	return nil
}

func runReportSummary(cmd *cobra.Command, args []string) error {
	endDate := time.Now()
	startDate := endDate.AddDate(0, 0, -reportDays)

	rptType := parseReportType(reportType)
	if rptType == "" {
		return fmt.Errorf("unknown report type: %s", reportType)
	}

	// Get catalog path
	catalogPath := reportCatalog
	if catalogPath == "" {
		homeDir, _ := os.UserHomeDir()
		catalogPath = filepath.Join(homeDir, ".dbbackup", "catalog.db")
	}

	// Open catalog
	cat, err := catalog.NewSQLiteCatalog(catalogPath)
	if err != nil {
		return fmt.Errorf("failed to open catalog: %w", err)
	}
	defer cat.Close()

	// Configure and generate
	config := report.ReportConfig{
		Type:        rptType,
		PeriodStart: startDate,
		PeriodEnd:   endDate,
		CatalogPath: catalogPath,
	}

	gen := report.NewGenerator(cat, config)
	rpt, err := gen.Generate()
	if err != nil {
		return fmt.Errorf("failed to generate report: %w", err)
	}

	// Display console summary
	formatter := &report.ConsoleFormatter{}
	return formatter.Format(rpt, os.Stdout)
}

func runReportList(cmd *cobra.Command, args []string) error {
	fmt.Println("\nAvailable Compliance Frameworks:")
	fmt.Println(strings.Repeat("-", 50))
	fmt.Printf("  %-12s %s\n", "soc2", "SOC 2 Type II Trust Service Criteria")
	fmt.Printf("  %-12s %s\n", "gdpr", "General Data Protection Regulation (EU)")
	fmt.Printf("  %-12s %s\n", "hipaa", "Health Insurance Portability and Accountability Act")
	fmt.Printf("  %-12s %s\n", "pci-dss", "Payment Card Industry Data Security Standard")
	fmt.Printf("  %-12s %s\n", "iso27001", "ISO 27001 Information Security Management")
	fmt.Println()
	fmt.Println("Usage: dbbackup report generate --type <framework>")
	fmt.Println()
	return nil
}

func runReportControls(cmd *cobra.Command, args []string) error {
	rptType := parseReportType(args[0])
	if rptType == "" {
		return fmt.Errorf("unknown report type: %s", args[0])
	}

	framework := report.GetFramework(rptType)
	if framework == nil {
		return fmt.Errorf("no framework defined for: %s", args[0])
	}

	fmt.Printf("\n%s Controls\n", strings.ToUpper(args[0]))
	fmt.Println(strings.Repeat("=", 60))

	for _, cat := range framework {
		fmt.Printf("\n%s\n", cat.Name)
		fmt.Printf("%s\n", cat.Description)
		fmt.Println(strings.Repeat("-", 40))

		for _, ctrl := range cat.Controls {
			fmt.Printf("  [%s] %s\n", ctrl.Reference, ctrl.Name)
			fmt.Printf("          %s\n", ctrl.Description)
		}
	}

	fmt.Println()
	return nil
}

func parseReportType(s string) report.ReportType {
	switch strings.ToLower(s) {
	case "soc2", "soc-2", "soc2-type2":
		return report.ReportSOC2
	case "gdpr":
		return report.ReportGDPR
	case "hipaa":
		return report.ReportHIPAA
	case "pci-dss", "pcidss", "pci":
		return report.ReportPCIDSS
	case "iso27001", "iso-27001", "iso":
		return report.ReportISO27001
	case "custom":
		return report.ReportCustom
	default:
		return ""
	}
}

func parseOutputFormat(s string) report.OutputFormat {
	switch strings.ToLower(s) {
	case "json":
		return report.FormatJSON
	case "html":
		return report.FormatHTML
	case "md", "markdown":
		return report.FormatMarkdown
	case "pdf":
		return report.FormatPDF
	default:
		return report.FormatMarkdown
	}
}
