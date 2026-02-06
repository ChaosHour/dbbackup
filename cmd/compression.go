package cmd

import (
	"context"
	"fmt"
	"os"
	"time"

	"dbbackup/internal/compression"
	"dbbackup/internal/config"
	"dbbackup/internal/logger"

	"github.com/spf13/cobra"
)

var compressionCmd = &cobra.Command{
	Use:   "compression",
	Short: "Compression analysis and optimization",
	Long: `Analyze database content to optimize compression settings.

The compression advisor scans blob/bytea columns to determine if 
compression would be beneficial. Already compressed data (images, 
archives, videos) won't benefit from additional compression.

Examples:
  # Analyze database and show recommendation
  dbbackup compression analyze --database mydb
  
  # Quick scan (faster, less thorough)
  dbbackup compression analyze --database mydb --quick
  
  # Force fresh analysis (ignore cache)
  dbbackup compression analyze --database mydb --no-cache
  
  # Apply recommended settings automatically
  dbbackup compression analyze --database mydb --apply
  
  # View/manage cache
  dbbackup compression cache list
  dbbackup compression cache clear`,
}

var (
	compressionQuick   bool
	compressionApply   bool
	compressionOutput  string
	compressionNoCache bool
)

var compressionAnalyzeCmd = &cobra.Command{
	Use:   "analyze",
	Short: "Analyze database for optimal compression settings",
	Long: `Scan blob columns in the database to determine optimal compression settings.

This command:
1. Discovers all blob/bytea columns (including pg_largeobject)
2. Samples data from each column
3. Tests compression on samples
4. Detects pre-compressed content (JPEG, PNG, ZIP, etc.)
5. Estimates backup time with different compression levels
6. Recommends compression level or suggests skipping compression

Results are cached for 7 days to avoid repeated scanning.
Use --no-cache to force a fresh analysis.

For databases with large amounts of already-compressed data (images, 
documents, archives), disabling compression can:
- Speed up backup/restore by 2-5x
- Prevent backup files from growing larger than source data
- Reduce CPU usage significantly`,
	RunE: func(cmd *cobra.Command, args []string) error {
		return runCompressionAnalyze(cmd.Context())
	},
}

var compressionCacheCmd = &cobra.Command{
	Use:   "cache",
	Short: "Manage compression analysis cache",
	Long:  `View and manage cached compression analysis results.`,
}

var compressionCacheListCmd = &cobra.Command{
	Use:   "list",
	Short: "List cached compression analyses",
	RunE: func(cmd *cobra.Command, args []string) error {
		return runCompressionCacheList()
	},
}

var compressionCacheClearCmd = &cobra.Command{
	Use:   "clear",
	Short: "Clear all cached compression analyses",
	RunE: func(cmd *cobra.Command, args []string) error {
		return runCompressionCacheClear()
	},
}

func init() {
	rootCmd.AddCommand(compressionCmd)
	compressionCmd.AddCommand(compressionAnalyzeCmd)
	compressionCmd.AddCommand(compressionCacheCmd)
	compressionCacheCmd.AddCommand(compressionCacheListCmd)
	compressionCacheCmd.AddCommand(compressionCacheClearCmd)

	// Flags for analyze command
	compressionAnalyzeCmd.Flags().BoolVar(&compressionQuick, "quick", false, "Quick scan (samples fewer blobs)")
	compressionAnalyzeCmd.Flags().BoolVar(&compressionApply, "apply", false, "Apply recommended settings to config")
	compressionAnalyzeCmd.Flags().StringVar(&compressionOutput, "output", "", "Write report to file (- for stdout)")
	compressionAnalyzeCmd.Flags().BoolVar(&compressionNoCache, "no-cache", false, "Force fresh analysis (ignore cache)")
}

func runCompressionAnalyze(ctx context.Context) error {
	log := logger.New(cfg.LogLevel, cfg.LogFormat)

	if cfg.Database == "" {
		return fmt.Errorf("database name required (use --database)")
	}

	fmt.Println("🔍 Compression Advisor")
	fmt.Println("━━━━━━━━━━━━━━━━━━━━━━")
	fmt.Printf("Database: %s@%s:%d/%s (%s)\n\n", 
		cfg.User, cfg.Host, cfg.Port, cfg.Database, cfg.DisplayDatabaseType())

	// Create analyzer
	analyzer := compression.NewAnalyzer(cfg, log)
	defer analyzer.Close()
	
	// Disable cache if requested
	if compressionNoCache {
		analyzer.DisableCache()
		fmt.Println("Cache disabled - performing fresh analysis...")
	}

	fmt.Println("Scanning blob columns...")
	startTime := time.Now()

	// Run analysis
	var analysis *compression.DatabaseAnalysis
	var err error

	if compressionQuick {
		analysis, err = analyzer.QuickScan(ctx)
	} else {
		analysis, err = analyzer.Analyze(ctx)
	}

	if err != nil {
		return fmt.Errorf("analysis failed: %w", err)
	}

	// Show if result was cached
	if !analysis.CachedAt.IsZero() && !compressionNoCache {
		age := time.Since(analysis.CachedAt)
		fmt.Printf("📦 Using cached result (age: %v)\n\n", age.Round(time.Minute))
	} else {
		fmt.Printf("Scan completed in %v\n\n", time.Since(startTime).Round(time.Millisecond))
	}

	// Generate and display report
	report := analysis.FormatReport()
	
	if compressionOutput != "" && compressionOutput != "-" {
		// Write to file
		if err := os.WriteFile(compressionOutput, []byte(report), 0644); err != nil {
			return fmt.Errorf("failed to write report: %w", err)
		}
		fmt.Printf("Report saved to: %s\n", compressionOutput)
	}
	
	// Always print to stdout
	fmt.Println(report)

	// Apply if requested
	if compressionApply {
		cfg.CompressionLevel = analysis.RecommendedLevel
		cfg.AutoDetectCompression = true
		cfg.CompressionMode = "auto"
		
		fmt.Println("\n✅ Applied settings:")
		fmt.Printf("   compression-level = %d\n", analysis.RecommendedLevel)
		fmt.Println("   auto-detect-compression = true")
		fmt.Println("\nThese settings will be used for future backups.")
		
		// Note: Settings are applied to runtime config
		// To persist, user should save config
		fmt.Println("\nTip: Use 'dbbackup config save' to persist these settings.")
	}

	// Return non-zero exit if compression should be skipped
	if analysis.Advice == compression.AdviceSkip && !compressionApply {
		fmt.Println("\n💡 Tip: Use --apply to automatically configure optimal settings")
	}

	return nil
}

func runCompressionCacheList() error {
	cache := compression.NewCache("")
	
	entries, err := cache.List()
	if err != nil {
		return fmt.Errorf("failed to list cache: %w", err)
	}
	
	if len(entries) == 0 {
		fmt.Println("No cached compression analyses found.")
		return nil
	}
	
	fmt.Println("📦 Cached Compression Analyses")
	fmt.Println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
	fmt.Printf("%-30s %-20s %-20s %s\n", "DATABASE", "ADVICE", "CACHED", "EXPIRES")
	fmt.Println("─────────────────────────────────────────────────────────────────────────────")
	
	now := time.Now()
	for _, entry := range entries {
		dbName := fmt.Sprintf("%s:%d/%s", entry.Host, entry.Port, entry.Database)
		if len(dbName) > 30 {
			dbName = dbName[:27] + "..."
		}
		
		advice := "N/A"
		if entry.Analysis != nil {
			advice = entry.Analysis.Advice.String()
		}
		
		age := now.Sub(entry.CreatedAt).Round(time.Hour)
		ageStr := fmt.Sprintf("%v ago", age)
		
		expiresIn := entry.ExpiresAt.Sub(now).Round(time.Hour)
		expiresStr := fmt.Sprintf("in %v", expiresIn)
		if expiresIn < 0 {
			expiresStr = "EXPIRED"
		}
		
		fmt.Printf("%-30s %-20s %-20s %s\n", dbName, advice, ageStr, expiresStr)
	}
	
	fmt.Printf("\nTotal: %d cached entries\n", len(entries))
	return nil
}

func runCompressionCacheClear() error {
	cache := compression.NewCache("")
	
	if err := cache.InvalidateAll(); err != nil {
		return fmt.Errorf("failed to clear cache: %w", err)
	}
	
	fmt.Println("✅ Compression analysis cache cleared.")
	return nil
}

// AutoAnalyzeBeforeBackup performs automatic compression analysis before backup
// Returns the recommended compression level (or current level if analysis fails/skipped)
func AutoAnalyzeBeforeBackup(ctx context.Context, cfg *config.Config, log logger.Logger) int {
	if !cfg.ShouldAutoDetectCompression() {
		return cfg.CompressionLevel
	}
	
	analyzer := compression.NewAnalyzer(cfg, log)
	defer analyzer.Close()
	
	// Use quick scan for auto-analyze to minimize delay
	analysis, err := analyzer.QuickScan(ctx)
	if err != nil {
		if log != nil {
			log.Warn("Auto compression analysis failed, using default", "error", err)
		}
		return cfg.CompressionLevel
	}
	
	if log != nil {
		log.Info("Auto-detected compression settings",
			"advice", analysis.Advice.String(),
			"recommended_level", analysis.RecommendedLevel,
			"incompressible_pct", fmt.Sprintf("%.1f%%", analysis.IncompressiblePct),
			"cached", !analysis.CachedAt.IsZero())
	}
	
	return analysis.RecommendedLevel
}
