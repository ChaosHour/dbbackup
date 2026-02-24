package cmd

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"strings"
	"text/tabwriter"
	"time"

	"dbbackup/internal/catalog"

	"github.com/spf13/cobra"
)

var forecastCmd = &cobra.Command{
	Use:   "forecast [database]",
	Short: "Predict future disk space requirements",
	Long: `Analyze backup growth patterns and predict future disk space needs.

This command helps with:
  - Capacity planning (when will we run out of space?)
  - Budget forecasting (how much storage to provision?)
  - Growth trend analysis (is growth accelerating?)
  - Alert thresholds (when to add capacity?)

Uses historical backup data to calculate:
  - Average daily growth rate
  - Growth acceleration/deceleration
  - Time until space limit reached
  - Projected size at future dates

Examples:
  # Forecast for specific database
  dbbackup forecast mydb

  # Forecast all databases
  dbbackup forecast --all

  # Show projection for 90 days
  dbbackup forecast mydb --days 90

  # Set capacity limit (alert when approaching)
  dbbackup forecast mydb --limit 100GB

  # JSON output for automation
  dbbackup forecast mydb --format json`,
	Args: cobra.MaximumNArgs(1),
	RunE: runForecast,
}

var (
	forecastFormat    string
	forecastAll       bool
	forecastDays      int
	forecastLimitSize string
)

type ForecastResult struct {
	Database          string               `json:"database"`
	CurrentSize       int64                `json:"current_size_bytes"`
	TotalBackups      int                  `json:"total_backups"`
	OldestBackup      time.Time            `json:"oldest_backup"`
	NewestBackup      time.Time            `json:"newest_backup"`
	ObservationPeriod time.Duration        `json:"observation_period_seconds"`
	DailyGrowthRate   float64              `json:"daily_growth_bytes"`
	DailyGrowthPct    float64              `json:"daily_growth_percent"`
	Projections       []ForecastProjection `json:"projections"`
	TimeToLimit       *time.Duration       `json:"time_to_limit_seconds,omitempty"`
	SizeAtLimit       *time.Time           `json:"date_reaching_limit,omitempty"`
	Confidence        string               `json:"confidence"` // "high", "medium", "low"
}

type ForecastProjection struct {
	Days          int       `json:"days_from_now"`
	Date          time.Time `json:"date"`
	PredictedSize int64     `json:"predicted_size_bytes"`
	Confidence    float64   `json:"confidence_percent"`
}

func init() {
	rootCmd.AddCommand(forecastCmd)

	forecastCmd.Flags().StringVar(&forecastFormat, "format", "table", "Output format (table, json)")
	forecastCmd.Flags().BoolVar(&forecastAll, "all", false, "Show forecast for all databases")
	forecastCmd.Flags().IntVar(&forecastDays, "days", 90, "Days to project into future")
	forecastCmd.Flags().StringVar(&forecastLimitSize, "limit", "", "Capacity limit (e.g., '100GB', '1TB')")
}

func runForecast(cmd *cobra.Command, args []string) error {
	cat, err := openCatalog()
	if err != nil {
		return err
	}
	defer cat.Close()

	ctx := context.Background()

	var forecasts []*ForecastResult

	if forecastAll || len(args) == 0 {
		// Get all databases
		databases, err := cat.ListDatabases(ctx)
		if err != nil {
			return err
		}

		for _, db := range databases {
			forecast, err := calculateForecast(ctx, cat, db)
			if err != nil {
				return err
			}
			if forecast != nil {
				forecasts = append(forecasts, forecast)
			}
		}
	} else {
		database := args[0]
		forecast, err := calculateForecast(ctx, cat, database)
		if err != nil {
			return err
		}
		if forecast != nil {
			forecasts = append(forecasts, forecast)
		}
	}

	if len(forecasts) == 0 {
		fmt.Println("No forecast data available.")
		fmt.Println("\nRun 'dbbackup catalog sync <directory>' to import backups.")
		return nil
	}

	// Parse limit if provided
	var limitBytes int64
	if forecastLimitSize != "" {
		limitBytes, err = parseSize(forecastLimitSize)
		if err != nil {
			return fmt.Errorf("invalid limit size: %w", err)
		}
	}

	// Output results
	if forecastFormat == "json" {
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		return enc.Encode(forecasts)
	}

	// Table output
	for i, forecast := range forecasts {
		if i > 0 {
			fmt.Println()
		}
		printForecast(forecast, limitBytes)
	}

	return nil
}

func calculateForecast(ctx context.Context, cat *catalog.SQLiteCatalog, database string) (*ForecastResult, error) {
	// Get all backups for this database
	query := &catalog.SearchQuery{
		Database:  database,
		Limit:     1000,
		OrderBy:   "created_at",
		OrderDesc: false,
	}

	entries, err := cat.Search(ctx, query)
	if err != nil {
		return nil, err
	}

	if len(entries) < 2 {
		return nil, nil // Need at least 2 backups for growth rate
	}

	// Calculate metrics
	var totalSize int64
	oldest := entries[0].CreatedAt
	newest := entries[len(entries)-1].CreatedAt

	for _, entry := range entries {
		totalSize += entry.SizeBytes
	}

	// Calculate observation period
	observationPeriod := newest.Sub(oldest)
	if observationPeriod == 0 {
		return nil, nil
	}

	// Calculate daily growth rate
	firstSize := entries[0].SizeBytes
	lastSize := entries[len(entries)-1].SizeBytes
	sizeDelta := float64(lastSize - firstSize)

	daysObserved := observationPeriod.Hours() / 24
	dailyGrowthRate := sizeDelta / daysObserved

	// Calculate daily growth percentage
	var dailyGrowthPct float64
	if firstSize > 0 {
		dailyGrowthPct = (dailyGrowthRate / float64(firstSize)) * 100
	}

	// Determine confidence based on sample size and consistency
	confidence := determineConfidence(entries, dailyGrowthRate)

	// Generate projections
	projections := make([]ForecastProjection, 0)
	projectionDates := []int{7, 30, 60, 90, 180, 365}

	if forecastDays > 0 {
		// Use user-specified days
		projectionDates = []int{forecastDays}
		if forecastDays > 30 {
			projectionDates = []int{7, 30, forecastDays}
		}
	}

	for _, days := range projectionDates {
		if days > 365 && forecastDays == 90 {
			continue // Skip longer projections unless explicitly requested
		}

		predictedSize := lastSize + int64(dailyGrowthRate*float64(days))
		if predictedSize < 0 {
			predictedSize = 0
		}

		// Confidence decreases with time
		confidencePct := calculateConfidence(days, confidence)

		projections = append(projections, ForecastProjection{
			Days:          days,
			Date:          newest.Add(time.Duration(days) * 24 * time.Hour),
			PredictedSize: predictedSize,
			Confidence:    confidencePct,
		})
	}

	result := &ForecastResult{
		Database:          database,
		CurrentSize:       lastSize,
		TotalBackups:      len(entries),
		OldestBackup:      oldest,
		NewestBackup:      newest,
		ObservationPeriod: observationPeriod,
		DailyGrowthRate:   dailyGrowthRate,
		DailyGrowthPct:    dailyGrowthPct,
		Projections:       projections,
		Confidence:        confidence,
	}

	return result, nil
}

func determineConfidence(entries []*catalog.Entry, avgGrowth float64) string {
	if len(entries) < 5 {
		return "low"
	}
	if len(entries) < 15 {
		return "medium"
	}

	// Calculate variance in growth rates
	var variance float64
	for i := 1; i < len(entries); i++ {
		timeDiff := entries[i].CreatedAt.Sub(entries[i-1].CreatedAt).Hours() / 24
		if timeDiff == 0 {
			continue
		}
		sizeDiff := float64(entries[i].SizeBytes - entries[i-1].SizeBytes)
		growthRate := sizeDiff / timeDiff
		variance += (growthRate - avgGrowth) * (growthRate - avgGrowth)
	}
	variance /= float64(len(entries) - 1)
	stdDev := math.Sqrt(variance)

	// If standard deviation is more than 50% of average growth, confidence is low
	if stdDev > math.Abs(avgGrowth)*0.5 {
		return "medium"
	}

	return "high"
}

func calculateConfidence(daysAhead int, baseConfidence string) float64 {
	var base float64
	switch baseConfidence {
	case "high":
		base = 95.0
	case "medium":
		base = 75.0
	case "low":
		base = 50.0
	}

	// Decay confidence over time (10% per 30 days)
	decay := float64(daysAhead) / 30.0 * 10.0
	confidence := base - decay

	if confidence < 30 {
		confidence = 30
	}
	return confidence
}

func printForecast(f *ForecastResult, limitBytes int64) {
	fmt.Printf("[FORECAST] %s\n", f.Database)
	fmt.Println(strings.Repeat("=", 60))

	fmt.Printf("\n[CURRENT STATE]\n")
	fmt.Printf("  Size:         %s\n", catalog.FormatSize(f.CurrentSize))
	fmt.Printf("  Backups:      %d backups\n", f.TotalBackups)
	fmt.Printf("  Observed:     %s (%.0f days)\n",
		formatForecastDuration(f.ObservationPeriod),
		f.ObservationPeriod.Hours()/24)

	fmt.Printf("\n[GROWTH RATE]\n")
	if f.DailyGrowthRate > 0 {
		fmt.Printf("  Daily:        +%s/day (%.2f%%/day)\n",
			catalog.FormatSize(int64(f.DailyGrowthRate)), f.DailyGrowthPct)
		fmt.Printf("  Weekly:       +%s/week\n", catalog.FormatSize(int64(f.DailyGrowthRate*7)))
		fmt.Printf("  Monthly:      +%s/month\n", catalog.FormatSize(int64(f.DailyGrowthRate*30)))
		fmt.Printf("  Annual:       +%s/year\n", catalog.FormatSize(int64(f.DailyGrowthRate*365)))
	} else if f.DailyGrowthRate < 0 {
		fmt.Printf("  Daily:        %s/day (shrinking)\n", catalog.FormatSize(int64(f.DailyGrowthRate)))
	} else {
		fmt.Printf("  Daily:        No growth detected\n")
	}
	fmt.Printf("  Confidence:   %s (%d samples)\n", f.Confidence, f.TotalBackups)

	if len(f.Projections) > 0 {
		fmt.Printf("\n[PROJECTIONS]\n")
		w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
		fmt.Fprintf(w, "  Days\tDate\tPredicted Size\tConfidence\n")
		fmt.Fprintf(w, "  ----\t----\t--------------\t----------\n")

		for _, proj := range f.Projections {
			fmt.Fprintf(w, "  %d\t%s\t%s\t%.0f%%\n",
				proj.Days,
				proj.Date.Format("2006-01-02"),
				catalog.FormatSize(proj.PredictedSize),
				proj.Confidence)
		}
		w.Flush()
	}

	// Check against limit
	if limitBytes > 0 {
		fmt.Printf("\n[CAPACITY LIMIT]\n")
		fmt.Printf("  Limit:        %s\n", catalog.FormatSize(limitBytes))

		currentPct := float64(f.CurrentSize) / float64(limitBytes) * 100
		fmt.Printf("  Current:      %.1f%% used\n", currentPct)

		if f.CurrentSize >= limitBytes {
			fmt.Printf("  Status:       [WARN] LIMIT EXCEEDED\n")
		} else if currentPct >= 80 {
			fmt.Printf("  Status:       [WARN] Approaching limit\n")
		} else {
			fmt.Printf("  Status:       [OK] Within limit\n")
		}

		// Calculate when we'll hit the limit
		if f.DailyGrowthRate > 0 {
			remaining := limitBytes - f.CurrentSize
			daysToLimit := float64(remaining) / f.DailyGrowthRate

			if daysToLimit > 0 && daysToLimit < 1000 {
				dateAtLimit := f.NewestBackup.Add(time.Duration(daysToLimit*24) * time.Hour)
				fmt.Printf("  Estimated:    Limit reached in %.0f days (%s)\n",
					daysToLimit, dateAtLimit.Format("2006-01-02"))

				if daysToLimit < 30 {
					fmt.Printf("  Alert:        [CRITICAL] Less than 30 days remaining!\n")
				} else if daysToLimit < 90 {
					fmt.Printf("  Alert:        [WARN] Less than 90 days remaining\n")
				}
			}
		}
	}

	fmt.Println()
}

func formatForecastDuration(d time.Duration) string {
	hours := d.Hours()
	if hours < 24 {
		return fmt.Sprintf("%.1f hours", hours)
	}
	days := hours / 24
	if days < 7 {
		return fmt.Sprintf("%.1f days", days)
	}
	weeks := days / 7
	if weeks < 4 {
		return fmt.Sprintf("%.1f weeks", weeks)
	}
	months := days / 30
	if months < 12 {
		return fmt.Sprintf("%.1f months", months)
	}
	years := days / 365
	return fmt.Sprintf("%.1f years", years)
}

func parseSize(s string) (int64, error) {
	// Simple size parser (supports KB, MB, GB, TB)
	s = strings.ToUpper(strings.TrimSpace(s))

	var multiplier int64 = 1
	var numStr string

	if strings.HasSuffix(s, "TB") {
		multiplier = 1024 * 1024 * 1024 * 1024
		numStr = strings.TrimSuffix(s, "TB")
	} else if strings.HasSuffix(s, "GB") {
		multiplier = 1024 * 1024 * 1024
		numStr = strings.TrimSuffix(s, "GB")
	} else if strings.HasSuffix(s, "MB") {
		multiplier = 1024 * 1024
		numStr = strings.TrimSuffix(s, "MB")
	} else if strings.HasSuffix(s, "KB") {
		multiplier = 1024
		numStr = strings.TrimSuffix(s, "KB")
	} else {
		numStr = s
	}

	var num float64
	_, err := fmt.Sscanf(numStr, "%f", &num)
	if err != nil {
		return 0, fmt.Errorf("invalid size format: %s", s)
	}

	return int64(num * float64(multiplier)), nil
}
