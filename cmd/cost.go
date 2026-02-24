package cmd

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"dbbackup/internal/catalog"

	"github.com/spf13/cobra"
)

var (
	costDatabase string
	costFormat   string
	costRegion   string
	costProvider string
	costDays     int
)

// costCmd analyzes backup storage costs
var costCmd = &cobra.Command{
	Use:   "cost",
	Short: "Analyze cloud storage costs for backups",
	Long: `Calculate and compare cloud storage costs for your backups.

Analyzes storage costs across providers:
  - AWS S3 (Standard, IA, Glacier, Deep Archive)
  - Google Cloud Storage (Standard, Nearline, Coldline, Archive)
  - Azure Blob Storage (Hot, Cool, Archive)
  - Backblaze B2
  - Wasabi

Pricing is based on standard rates and may vary by region.

Examples:
  # Analyze all backups
  dbbackup cost analyze

  # Specific database
  dbbackup cost analyze --database mydb

  # Compare providers for 90 days
  dbbackup cost analyze --days 90 --format table

  # Estimate for specific region
  dbbackup cost analyze --region us-east-1

  # JSON output for automation
  dbbackup cost analyze --format json`,
}

var costAnalyzeCmd = &cobra.Command{
	Use:   "analyze",
	Short: "Analyze backup storage costs",
	Args:  cobra.NoArgs,
	RunE:  runCostAnalyze,
}

func init() {
	rootCmd.AddCommand(costCmd)
	costCmd.AddCommand(costAnalyzeCmd)

	costAnalyzeCmd.Flags().StringVar(&costDatabase, "database", "", "Filter by database")
	costAnalyzeCmd.Flags().StringVar(&costFormat, "format", "table", "Output format (table, json)")
	costAnalyzeCmd.Flags().StringVar(&costRegion, "region", "us-east-1", "Cloud region for pricing")
	costAnalyzeCmd.Flags().StringVar(&costProvider, "provider", "all", "Show specific provider (all, aws, gcs, azure, b2, wasabi)")
	costAnalyzeCmd.Flags().IntVar(&costDays, "days", 30, "Number of days to calculate")
}

func runCostAnalyze(cmd *cobra.Command, args []string) error {
	cat, err := openCatalog()
	if err != nil {
		return err
	}
	defer func() { _ = cat.Close() }()

	ctx := context.Background()

	// Get backup statistics
	var stats *catalog.Stats
	if costDatabase != "" {
		stats, err = cat.StatsByDatabase(ctx, costDatabase)
	} else {
		stats, err = cat.Stats(ctx)
	}
	if err != nil {
		return err
	}

	if stats.TotalBackups == 0 {
		fmt.Println("No backups found in catalog. Run 'dbbackup catalog sync' first.")
		return nil
	}

	// Calculate costs
	analysis := calculateCosts(stats.TotalSize, costDays, costRegion)

	if costFormat == "json" {
		return outputCostJSON(analysis, stats)
	}

	return outputCostTable(analysis, stats)
}

// StorageTier represents a storage class/tier
type StorageTier struct {
	Provider    string
	Tier        string
	Description string
	StorageGB   float64 // $ per GB/month
	RetrievalGB float64 // $ per GB retrieved
	Requests    float64 // $ per 1000 requests
	MinDays     int     // Minimum storage duration
}

// CostAnalysis represents the cost breakdown
type CostAnalysis struct {
	TotalSizeGB     float64
	Days            int
	Region          string
	Recommendations []TierRecommendation
}

type TierRecommendation struct {
	Provider       string
	Tier           string
	Description    string
	MonthlyStorage float64
	AnnualStorage  float64
	RetrievalCost  float64
	TotalMonthly   float64
	TotalAnnual    float64
	SavingsVsS3    float64
	SavingsPct     float64
	BestFor        string
}

func calculateCosts(totalBytes int64, days int, region string) *CostAnalysis {
	sizeGB := float64(totalBytes) / (1024 * 1024 * 1024)

	analysis := &CostAnalysis{
		TotalSizeGB: sizeGB,
		Days:        days,
		Region:      region,
	}

	// Define storage tiers (pricing as of 2026, approximate)
	tiers := []StorageTier{
		// AWS S3
		{Provider: "AWS S3", Tier: "Standard", Description: "Frequent access",
			StorageGB: 0.023, RetrievalGB: 0.0, Requests: 0.0004, MinDays: 0},
		{Provider: "AWS S3", Tier: "Intelligent-Tiering", Description: "Auto-optimization",
			StorageGB: 0.023, RetrievalGB: 0.0, Requests: 0.0004, MinDays: 0},
		{Provider: "AWS S3", Tier: "Standard-IA", Description: "Infrequent access",
			StorageGB: 0.0125, RetrievalGB: 0.01, Requests: 0.001, MinDays: 30},
		{Provider: "AWS S3", Tier: "Glacier Instant", Description: "Archive instant",
			StorageGB: 0.004, RetrievalGB: 0.03, Requests: 0.01, MinDays: 90},
		{Provider: "AWS S3", Tier: "Glacier Flexible", Description: "Archive flexible",
			StorageGB: 0.0036, RetrievalGB: 0.02, Requests: 0.05, MinDays: 90},
		{Provider: "AWS S3", Tier: "Deep Archive", Description: "Long-term archive",
			StorageGB: 0.00099, RetrievalGB: 0.02, Requests: 0.05, MinDays: 180},

		// Google Cloud Storage
		{Provider: "GCS", Tier: "Standard", Description: "Frequent access",
			StorageGB: 0.020, RetrievalGB: 0.0, Requests: 0.0004, MinDays: 0},
		{Provider: "GCS", Tier: "Nearline", Description: "Monthly access",
			StorageGB: 0.010, RetrievalGB: 0.01, Requests: 0.001, MinDays: 30},
		{Provider: "GCS", Tier: "Coldline", Description: "Quarterly access",
			StorageGB: 0.004, RetrievalGB: 0.02, Requests: 0.005, MinDays: 90},
		{Provider: "GCS", Tier: "Archive", Description: "Annual access",
			StorageGB: 0.0012, RetrievalGB: 0.05, Requests: 0.05, MinDays: 365},

		// Azure Blob Storage
		{Provider: "Azure", Tier: "Hot", Description: "Frequent access",
			StorageGB: 0.0184, RetrievalGB: 0.0, Requests: 0.0004, MinDays: 0},
		{Provider: "Azure", Tier: "Cool", Description: "Infrequent access",
			StorageGB: 0.010, RetrievalGB: 0.01, Requests: 0.001, MinDays: 30},
		{Provider: "Azure", Tier: "Archive", Description: "Long-term archive",
			StorageGB: 0.00099, RetrievalGB: 0.02, Requests: 0.05, MinDays: 180},

		// Backblaze B2
		{Provider: "Backblaze B2", Tier: "Standard", Description: "Affordable cloud",
			StorageGB: 0.005, RetrievalGB: 0.01, Requests: 0.0004, MinDays: 0},

		// Wasabi
		{Provider: "Wasabi", Tier: "Hot Cloud", Description: "No egress fees",
			StorageGB: 0.0059, RetrievalGB: 0.0, Requests: 0.0, MinDays: 90},
	}

	// Calculate costs for each tier
	s3StandardCost := 0.0
	for _, tier := range tiers {
		if costProvider != "all" {
			providerLower := strings.ToLower(tier.Provider)
			filterLower := strings.ToLower(costProvider)
			if !strings.Contains(providerLower, filterLower) {
				continue
			}
		}

		rec := TierRecommendation{
			Provider:    tier.Provider,
			Tier:        tier.Tier,
			Description: tier.Description,
		}

		// Monthly storage cost
		rec.MonthlyStorage = sizeGB * tier.StorageGB

		// Annual storage cost
		rec.AnnualStorage = rec.MonthlyStorage * 12

		// Estimate retrieval cost (assume 1 retrieval per month for DR testing)
		rec.RetrievalCost = sizeGB * tier.RetrievalGB

		// Total costs
		rec.TotalMonthly = rec.MonthlyStorage + rec.RetrievalCost
		rec.TotalAnnual = rec.AnnualStorage + (rec.RetrievalCost * 12)

		// Track S3 Standard for comparison
		if tier.Provider == "AWS S3" && tier.Tier == "Standard" {
			s3StandardCost = rec.TotalMonthly
		}

		// Recommendations
		switch {
		case tier.MinDays >= 180:
			rec.BestFor = "Long-term archives (6+ months)"
		case tier.MinDays >= 90:
			rec.BestFor = "Compliance archives (3+ months)"
		case tier.MinDays >= 30:
			rec.BestFor = "Recent backups (monthly rotation)"
		default:
			rec.BestFor = "Active/hot backups (daily access)"
		}

		analysis.Recommendations = append(analysis.Recommendations, rec)
	}

	// Calculate savings vs S3 Standard
	if s3StandardCost > 0 {
		for i := range analysis.Recommendations {
			rec := &analysis.Recommendations[i]
			rec.SavingsVsS3 = s3StandardCost - rec.TotalMonthly
			if s3StandardCost > 0 {
				rec.SavingsPct = (rec.SavingsVsS3 / s3StandardCost) * 100.0
			}
		}
	}

	return analysis
}

func outputCostTable(analysis *CostAnalysis, stats *catalog.Stats) error {
	fmt.Println()
	fmt.Println("═══════════════════════════════════════════════════════════════════════════")
	fmt.Printf("  Cloud Storage Cost Analysis\n")
	fmt.Println("═══════════════════════════════════════════════════════════════════════════")
	fmt.Println()

	fmt.Printf("[CURRENT BACKUP INVENTORY]\n")
	fmt.Printf("  Total Backups:     %d\n", stats.TotalBackups)
	fmt.Printf("  Total Size:        %.2f GB (%s)\n", analysis.TotalSizeGB, stats.TotalSizeHuman)
	if costDatabase != "" {
		fmt.Printf("  Database:          %s\n", costDatabase)
	} else {
		fmt.Printf("  Databases:         %d\n", len(stats.ByDatabase))
	}
	fmt.Printf("  Region:            %s\n", analysis.Region)
	fmt.Printf("  Analysis Period:   %d days\n", analysis.Days)
	fmt.Println()

	fmt.Println("───────────────────────────────────────────────────────────────────────────")
	fmt.Printf("%-20s %-20s %12s %12s %12s\n",
		"PROVIDER", "TIER", "MONTHLY", "ANNUAL", "SAVINGS")
	fmt.Println("───────────────────────────────────────────────────────────────────────────")

	for _, rec := range analysis.Recommendations {
		savings := ""
		if rec.SavingsVsS3 > 0 {
			savings = fmt.Sprintf("↓ $%.2f (%.0f%%)", rec.SavingsVsS3, rec.SavingsPct)
		} else if rec.SavingsVsS3 < 0 {
			savings = fmt.Sprintf("↑ $%.2f", -rec.SavingsVsS3)
		} else {
			savings = "baseline"
		}

		fmt.Printf("%-20s %-20s $%10.2f $%10.2f  %s\n",
			rec.Provider,
			rec.Tier,
			rec.TotalMonthly,
			rec.TotalAnnual,
			savings,
		)
	}

	fmt.Println("───────────────────────────────────────────────────────────────────────────")
	fmt.Println()

	// Top recommendations
	fmt.Println("[COST OPTIMIZATION RECOMMENDATIONS]")
	fmt.Println()

	// Find cheapest option
	cheapest := analysis.Recommendations[0]
	for _, rec := range analysis.Recommendations {
		if rec.TotalAnnual < cheapest.TotalAnnual {
			cheapest = rec
		}
	}

	fmt.Printf("💰 CHEAPEST OPTION: %s %s\n", cheapest.Provider, cheapest.Tier)
	fmt.Printf("   Annual Cost: $%.2f (save $%.2f/year vs S3 Standard)\n",
		cheapest.TotalAnnual, cheapest.SavingsVsS3*12)
	fmt.Printf("   Best For: %s\n", cheapest.BestFor)
	fmt.Println()

	// Find best balance
	fmt.Printf("⚖️  BALANCED OPTION: AWS S3 Standard-IA or GCS Nearline\n")
	fmt.Printf("   Good balance of cost and accessibility\n")
	fmt.Printf("   Suitable for 30-day retention backups\n")
	fmt.Println()

	// Find hot storage
	fmt.Printf("🔥 HOT STORAGE: Wasabi or Backblaze B2\n")
	fmt.Printf("   No egress fees (Wasabi) or low retrieval costs\n")
	fmt.Printf("   Perfect for frequent restore testing\n")
	fmt.Println()

	// Strategy recommendation
	fmt.Println("[TIERED STORAGE STRATEGY]")
	fmt.Println()
	fmt.Printf("   Day 0-7:     S3 Standard or Wasabi        (frequent access)\n")
	fmt.Printf("   Day 8-30:    S3 Standard-IA or GCS Nearline  (weekly access)\n")
	fmt.Printf("   Day 31-90:   S3 Glacier or GCS Coldline      (monthly access)\n")
	fmt.Printf("   Day 90+:     S3 Deep Archive or GCS Archive  (compliance)\n")
	fmt.Println()

	potentialSaving := 0.0
	for _, rec := range analysis.Recommendations {
		if rec.Provider == "AWS S3" && rec.Tier == "Deep Archive" {
			potentialSaving = rec.SavingsVsS3 * 12
		}
	}

	if potentialSaving > 0 {
		fmt.Printf("💡 With tiered lifecycle policies, you could save ~$%.2f/year\n", potentialSaving)
	}

	fmt.Println()
	fmt.Println("═══════════════════════════════════════════════════════════════════════════")
	fmt.Println()
	fmt.Println("Note: Costs are estimates based on standard pricing.")
	fmt.Println("Actual costs may vary by region, usage patterns, and current pricing.")
	fmt.Println()

	return nil
}

func outputCostJSON(analysis *CostAnalysis, stats *catalog.Stats) error {
	output := map[string]interface{}{
		"inventory": map[string]interface{}{
			"total_backups":    stats.TotalBackups,
			"total_size_gb":    analysis.TotalSizeGB,
			"total_size_human": stats.TotalSizeHuman,
			"region":           analysis.Region,
			"analysis_days":    analysis.Days,
		},
		"recommendations": analysis.Recommendations,
	}

	// Find cheapest
	cheapest := analysis.Recommendations[0]
	for _, rec := range analysis.Recommendations {
		if rec.TotalAnnual < cheapest.TotalAnnual {
			cheapest = rec
		}
	}

	output["cheapest"] = map[string]interface{}{
		"provider":     cheapest.Provider,
		"tier":         cheapest.Tier,
		"annual_cost":  cheapest.TotalAnnual,
		"monthly_cost": cheapest.TotalMonthly,
	}

	data, err := json.MarshalIndent(output, "", "  ")
	if err != nil {
		return err
	}

	fmt.Println(string(data))
	return nil
}
