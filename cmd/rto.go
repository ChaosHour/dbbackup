package cmd

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"dbbackup/internal/catalog"
	"dbbackup/internal/rto"

	"github.com/spf13/cobra"
)

var rtoCmd = &cobra.Command{
	Use:   "rto",
	Short: "RTO/RPO analysis and monitoring",
	Long: `Analyze and monitor Recovery Time Objective (RTO) and 
Recovery Point Objective (RPO) metrics.

RTO: How long to recover from a failure
RPO: How much data you can afford to lose

Examples:
  # Analyze RTO/RPO for all databases
  dbbackup rto analyze

  # Analyze specific database
  dbbackup rto analyze --database mydb

  # Show summary status
  dbbackup rto status

  # Set targets and check compliance
  dbbackup rto check --target-rto 4h --target-rpo 1h`,
}

var rtoAnalyzeCmd = &cobra.Command{
	Use:   "analyze",
	Short: "Analyze RTO/RPO for databases",
	Long:  "Perform detailed RTO/RPO analysis based on backup history",
	RunE:  runRTOAnalyze,
}

var rtoStatusCmd = &cobra.Command{
	Use:   "status",
	Short: "Show RTO/RPO status summary",
	Long:  "Display current RTO/RPO compliance status for all databases",
	RunE:  runRTOStatus,
}

var rtoCheckCmd = &cobra.Command{
	Use:   "check",
	Short: "Check RTO/RPO compliance",
	Long:  "Check if databases meet RTO/RPO targets",
	RunE:  runRTOCheck,
}

var (
	rtoDatabase  string
	rtoTargetRTO string
	rtoTargetRPO string
	rtoCatalog   string
	rtoFormat    string
	rtoOutput    string
)

func init() {
	rootCmd.AddCommand(rtoCmd)
	rtoCmd.AddCommand(rtoAnalyzeCmd)
	rtoCmd.AddCommand(rtoStatusCmd)
	rtoCmd.AddCommand(rtoCheckCmd)

	// Analyze command flags
	rtoAnalyzeCmd.Flags().StringVarP(&rtoDatabase, "database", "d", "", "Database to analyze (all if not specified)")
	rtoAnalyzeCmd.Flags().StringVar(&rtoTargetRTO, "target-rto", "4h", "Target RTO (e.g., 4h, 30m)")
	rtoAnalyzeCmd.Flags().StringVar(&rtoTargetRPO, "target-rpo", "1h", "Target RPO (e.g., 1h, 15m)")
	rtoAnalyzeCmd.Flags().StringVar(&rtoCatalog, "catalog", "", "Path to backup catalog")
	rtoAnalyzeCmd.Flags().StringVarP(&rtoFormat, "format", "f", "text", "Output format (text, json)")
	rtoAnalyzeCmd.Flags().StringVarP(&rtoOutput, "output", "o", "", "Output file")

	// Status command flags
	rtoStatusCmd.Flags().StringVar(&rtoCatalog, "catalog", "", "Path to backup catalog")
	rtoStatusCmd.Flags().StringVar(&rtoTargetRTO, "target-rto", "4h", "Target RTO")
	rtoStatusCmd.Flags().StringVar(&rtoTargetRPO, "target-rpo", "1h", "Target RPO")

	// Check command flags
	rtoCheckCmd.Flags().StringVarP(&rtoDatabase, "database", "d", "", "Database to check")
	rtoCheckCmd.Flags().StringVar(&rtoTargetRTO, "target-rto", "4h", "Target RTO")
	rtoCheckCmd.Flags().StringVar(&rtoTargetRPO, "target-rpo", "1h", "Target RPO")
	rtoCheckCmd.Flags().StringVar(&rtoCatalog, "catalog", "", "Path to backup catalog")
}

func runRTOAnalyze(cmd *cobra.Command, args []string) error {
	ctx := context.Background()

	// Parse duration targets
	targetRTO, err := time.ParseDuration(rtoTargetRTO)
	if err != nil {
		return fmt.Errorf("invalid target-rto: %w", err)
	}
	targetRPO, err := time.ParseDuration(rtoTargetRPO)
	if err != nil {
		return fmt.Errorf("invalid target-rpo: %w", err)
	}

	// Get catalog
	cat, err := openRTOCatalog()
	if err != nil {
		return err
	}
	defer cat.Close()

	// Create calculator
	config := rto.DefaultConfig()
	config.TargetRTO = targetRTO
	config.TargetRPO = targetRPO
	calc := rto.NewCalculator(cat, config)

	var analyses []*rto.Analysis

	if rtoDatabase != "" {
		// Analyze single database
		analysis, err := calc.Analyze(ctx, rtoDatabase)
		if err != nil {
			return fmt.Errorf("analysis failed: %w", err)
		}
		analyses = append(analyses, analysis)
	} else {
		// Analyze all databases
		analyses, err = calc.AnalyzeAll(ctx)
		if err != nil {
			return fmt.Errorf("analysis failed: %w", err)
		}
	}

	// Output
	if rtoFormat == "json" {
		return outputJSON(analyses, rtoOutput)
	}

	return outputAnalysisText(analyses)
}

func runRTOStatus(cmd *cobra.Command, args []string) error {
	ctx := context.Background()

	// Parse targets
	targetRTO, err := time.ParseDuration(rtoTargetRTO)
	if err != nil {
		return fmt.Errorf("invalid target-rto: %w", err)
	}
	targetRPO, err := time.ParseDuration(rtoTargetRPO)
	if err != nil {
		return fmt.Errorf("invalid target-rpo: %w", err)
	}

	// Get catalog
	cat, err := openRTOCatalog()
	if err != nil {
		return err
	}
	defer cat.Close()

	// Create calculator and analyze all
	config := rto.DefaultConfig()
	config.TargetRTO = targetRTO
	config.TargetRPO = targetRPO
	calc := rto.NewCalculator(cat, config)

	analyses, err := calc.AnalyzeAll(ctx)
	if err != nil {
		return fmt.Errorf("analysis failed: %w", err)
	}

	// Create summary
	summary := rto.Summarize(analyses)

	// Display status
	fmt.Println()
	fmt.Println("╔═══════════════════════════════════════════════════════════╗")
	fmt.Println("║              RTO/RPO STATUS SUMMARY                       ║")
	fmt.Println("╠═══════════════════════════════════════════════════════════╣")
	fmt.Printf("║  Target RTO: %-15s  Target RPO: %-15s ║\n",
		formatDuration(config.TargetRTO),
		formatDuration(config.TargetRPO))
	fmt.Println("╠═══════════════════════════════════════════════════════════╣")

	// Compliance status
	rpoRate := 0.0
	rtoRate := 0.0
	fullRate := 0.0
	if summary.TotalDatabases > 0 {
		rpoRate = float64(summary.RPOCompliant) / float64(summary.TotalDatabases) * 100
		rtoRate = float64(summary.RTOCompliant) / float64(summary.TotalDatabases) * 100
		fullRate = float64(summary.FullyCompliant) / float64(summary.TotalDatabases) * 100
	}

	fmt.Printf("║  Databases:     %-5d                                      ║\n", summary.TotalDatabases)
	fmt.Printf("║  RPO Compliant: %-5d  (%.0f%%)                              ║\n", summary.RPOCompliant, rpoRate)
	fmt.Printf("║  RTO Compliant: %-5d  (%.0f%%)                              ║\n", summary.RTOCompliant, rtoRate)
	fmt.Printf("║  Fully Compliant: %-3d  (%.0f%%)                             ║\n", summary.FullyCompliant, fullRate)

	if summary.CriticalIssues > 0 {
		fmt.Printf("║  ⚠️  Critical Issues: %-3d                                  ║\n", summary.CriticalIssues)
	}

	fmt.Println("╠═══════════════════════════════════════════════════════════╣")
	fmt.Printf("║  Average RPO: %-15s  Worst: %-15s    ║\n",
		formatDuration(summary.AverageRPO),
		formatDuration(summary.WorstRPO))
	fmt.Printf("║  Average RTO: %-15s  Worst: %-15s    ║\n",
		formatDuration(summary.AverageRTO),
		formatDuration(summary.WorstRTO))

	if summary.WorstRPODatabase != "" {
		fmt.Printf("║  Worst RPO Database: %-38s║\n", summary.WorstRPODatabase)
	}
	if summary.WorstRTODatabase != "" {
		fmt.Printf("║  Worst RTO Database: %-38s║\n", summary.WorstRTODatabase)
	}

	fmt.Println("╚═══════════════════════════════════════════════════════════╝")
	fmt.Println()

	// Per-database status
	if len(analyses) > 0 {
		fmt.Println("Database Status:")
		fmt.Println(strings.Repeat("-", 70))
		fmt.Printf("%-25s %-12s %-12s %-12s\n", "DATABASE", "RPO", "RTO", "STATUS")
		fmt.Println(strings.Repeat("-", 70))

		for _, a := range analyses {
			status := "✅"
			if !a.RPOCompliant || !a.RTOCompliant {
				status = "❌"
			}

			rpoStr := formatDuration(a.CurrentRPO)
			rtoStr := formatDuration(a.CurrentRTO)

			if !a.RPOCompliant {
				rpoStr = "⚠️ " + rpoStr
			}
			if !a.RTOCompliant {
				rtoStr = "⚠️ " + rtoStr
			}

			fmt.Printf("%-25s %-12s %-12s %s\n",
				truncateRTO(a.Database, 24),
				rpoStr,
				rtoStr,
				status)
		}
		fmt.Println(strings.Repeat("-", 70))
	}

	return nil
}

func runRTOCheck(cmd *cobra.Command, args []string) error {
	ctx := context.Background()

	// Parse targets
	targetRTO, err := time.ParseDuration(rtoTargetRTO)
	if err != nil {
		return fmt.Errorf("invalid target-rto: %w", err)
	}
	targetRPO, err := time.ParseDuration(rtoTargetRPO)
	if err != nil {
		return fmt.Errorf("invalid target-rpo: %w", err)
	}

	// Get catalog
	cat, err := openRTOCatalog()
	if err != nil {
		return err
	}
	defer cat.Close()

	// Create calculator
	config := rto.DefaultConfig()
	config.TargetRTO = targetRTO
	config.TargetRPO = targetRPO
	calc := rto.NewCalculator(cat, config)

	var analyses []*rto.Analysis

	if rtoDatabase != "" {
		analysis, err := calc.Analyze(ctx, rtoDatabase)
		if err != nil {
			return fmt.Errorf("analysis failed: %w", err)
		}
		analyses = append(analyses, analysis)
	} else {
		analyses, err = calc.AnalyzeAll(ctx)
		if err != nil {
			return fmt.Errorf("analysis failed: %w", err)
		}
	}

	// Check compliance
	exitCode := 0
	for _, a := range analyses {
		if !a.RPOCompliant {
			fmt.Printf("❌ %s: RPO violation - current %s exceeds target %s\n",
				a.Database,
				formatDuration(a.CurrentRPO),
				formatDuration(config.TargetRPO))
			exitCode = 1
		}
		if !a.RTOCompliant {
			fmt.Printf("❌ %s: RTO violation - estimated %s exceeds target %s\n",
				a.Database,
				formatDuration(a.CurrentRTO),
				formatDuration(config.TargetRTO))
			exitCode = 1
		}
		if a.RPOCompliant && a.RTOCompliant {
			fmt.Printf("✅ %s: Compliant (RPO: %s, RTO: %s)\n",
				a.Database,
				formatDuration(a.CurrentRPO),
				formatDuration(a.CurrentRTO))
		}
	}

	if exitCode != 0 {
		os.Exit(exitCode)
	}

	return nil
}

func openRTOCatalog() (*catalog.SQLiteCatalog, error) {
	catalogPath := rtoCatalog
	if catalogPath == "" {
		homeDir, _ := os.UserHomeDir()
		catalogPath = filepath.Join(homeDir, ".dbbackup", "catalog.db")
	}

	cat, err := catalog.NewSQLiteCatalog(catalogPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open catalog: %w", err)
	}

	return cat, nil
}

func outputJSON(data interface{}, outputPath string) error {
	jsonData, err := json.MarshalIndent(data, "", "  ")
	if err != nil {
		return err
	}

	if outputPath != "" {
		return os.WriteFile(outputPath, jsonData, 0644)
	}

	fmt.Println(string(jsonData))
	return nil
}

func outputAnalysisText(analyses []*rto.Analysis) error {
	for _, a := range analyses {
		fmt.Println()
		fmt.Println(strings.Repeat("=", 60))
		fmt.Printf("  Database: %s\n", a.Database)
		fmt.Println(strings.Repeat("=", 60))

		// Status
		rpoStatus := "✅ Compliant"
		if !a.RPOCompliant {
			rpoStatus = "❌ Violation"
		}
		rtoStatus := "✅ Compliant"
		if !a.RTOCompliant {
			rtoStatus = "❌ Violation"
		}

		fmt.Println()
		fmt.Println("  Recovery Objectives:")
		fmt.Println(strings.Repeat("-", 50))
		fmt.Printf("  RPO (Current):  %-15s  Target: %s\n",
			formatDuration(a.CurrentRPO), formatDuration(a.TargetRPO))
		fmt.Printf("  RPO Status:     %s\n", rpoStatus)
		fmt.Printf("  RTO (Estimated): %-14s  Target: %s\n",
			formatDuration(a.CurrentRTO), formatDuration(a.TargetRTO))
		fmt.Printf("  RTO Status:     %s\n", rtoStatus)

		if a.LastBackup != nil {
			fmt.Printf("  Last Backup:    %s\n", a.LastBackup.Format("2006-01-02 15:04:05"))
		}
		if a.BackupInterval > 0 {
			fmt.Printf("  Backup Interval: %s\n", formatDuration(a.BackupInterval))
		}

		// RTO Breakdown
		fmt.Println()
		fmt.Println("  RTO Breakdown:")
		fmt.Println(strings.Repeat("-", 50))
		b := a.RTOBreakdown
		fmt.Printf("    Detection:    %s\n", formatDuration(b.DetectionTime))
		fmt.Printf("    Decision:     %s\n", formatDuration(b.DecisionTime))
		if b.DownloadTime > 0 {
			fmt.Printf("    Download:     %s\n", formatDuration(b.DownloadTime))
		}
		fmt.Printf("    Restore:      %s\n", formatDuration(b.RestoreTime))
		fmt.Printf("    Startup:      %s\n", formatDuration(b.StartupTime))
		fmt.Printf("    Validation:   %s\n", formatDuration(b.ValidationTime))
		fmt.Printf("    Switchover:   %s\n", formatDuration(b.SwitchoverTime))
		fmt.Println(strings.Repeat("-", 30))
		fmt.Printf("    Total:        %s\n", formatDuration(b.TotalTime))

		// Recommendations
		if len(a.Recommendations) > 0 {
			fmt.Println()
			fmt.Println("  Recommendations:")
			fmt.Println(strings.Repeat("-", 50))
			for _, r := range a.Recommendations {
				icon := "💡"
				switch r.Priority {
				case rto.PriorityCritical:
					icon = "🔴"
				case rto.PriorityHigh:
					icon = "🟠"
				case rto.PriorityMedium:
					icon = "🟡"
				}
				fmt.Printf("  %s [%s] %s\n", icon, r.Priority, r.Title)
				fmt.Printf("      %s\n", r.Description)
			}
		}
	}

	return nil
}

func formatDuration(d time.Duration) string {
	if d < time.Minute {
		return fmt.Sprintf("%.0fs", d.Seconds())
	}
	if d < time.Hour {
		return fmt.Sprintf("%.0fm", d.Minutes())
	}
	hours := int(d.Hours())
	mins := int(d.Minutes()) - hours*60
	return fmt.Sprintf("%dh %dm", hours, mins)
}

func truncateRTO(s string, maxLen int) string {
	if len(s) <= maxLen {
		return s
	}
	return s[:maxLen-3] + "..."
}
