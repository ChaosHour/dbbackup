// Package performance provides report generation for performance comparison results
package performance

import (
	"fmt"
	"strings"
	"time"
)

// FormatComparisonReport generates a Markdown report from comparison results
func FormatComparisonReport(results []ComparisonResult) string {
	var sb strings.Builder

	sb.WriteString("# Performance Comparison Report\n\n")
	sb.WriteString(fmt.Sprintf("Generated: %s\n\n", time.Now().Format("2006-01-02 15:04:05")))

	// Group results by database
	mysqlResults := filterByDatabase(results, DatabaseBackendMySQL)
	pgResults := filterByDatabase(results, DatabaseBackendPostgreSQL)

	if len(mysqlResults) > 0 {
		sb.WriteString("## MySQL\n\n")
		writeComparisonSection(&sb, mysqlResults, "backup")
		writeComparisonSection(&sb, mysqlResults, "restore")
	}

	if len(pgResults) > 0 {
		sb.WriteString("## PostgreSQL\n\n")
		writeComparisonSection(&sb, pgResults, "backup")
		writeComparisonSection(&sb, pgResults, "restore")
	}

	// Summary
	sb.WriteString("## Summary\n\n")
	writeSummary(&sb, results)

	return sb.String()
}

func filterByDatabase(results []ComparisonResult, db DatabaseBackend) []ComparisonResult {
	var filtered []ComparisonResult
	for _, r := range results {
		if r.Database == db {
			filtered = append(filtered, r)
		}
	}
	return filtered
}

func filterByOperation(results []ComparisonResult, op string) []ComparisonResult {
	var filtered []ComparisonResult
	for _, r := range results {
		if r.Operation == op {
			filtered = append(filtered, r)
		}
	}
	return filtered
}

func writeComparisonSection(sb *strings.Builder, results []ComparisonResult, operation string) {
	opResults := filterByOperation(results, operation)
	if len(opResults) == 0 {
		return
	}

	title := strings.ToUpper(operation[:1]) + operation[1:]
	sb.WriteString(fmt.Sprintf("### %s\n\n", title))

	sb.WriteString("| Size | Native (MB/s) | Tool (MB/s) | Speedup | Native Mem (MB) | Tool Mem (MB) | Native Duration | Tool Duration |\n")
	sb.WriteString("|------|--------------|-------------|---------|----------------|--------------|-----------------|---------------|\n")

	for _, r := range opResults {
		nativeThroughput := "-"
		toolThroughput := "-"
		speedup := "-"
		nativeMem := "-"
		toolMem := "-"
		nativeDuration := "-"
		toolDuration := "-"

		if r.NativeResult != nil {
			nativeThroughput = fmt.Sprintf("%.1f", r.NativeResult.Throughput)
			nativeMem = fmt.Sprintf("%.1f", float64(r.NativeResult.AllocBytes)/(1024*1024))
			nativeDuration = r.NativeResult.Duration.Round(time.Millisecond).String()
		}
		if r.ToolResult != nil {
			toolThroughput = fmt.Sprintf("%.1f", r.ToolResult.Throughput)
			toolMem = fmt.Sprintf("%.1f", float64(r.ToolResult.AllocBytes)/(1024*1024))
			toolDuration = r.ToolResult.Duration.Round(time.Millisecond).String()
		}
		if r.SpeedupRatio > 0 {
			speedup = fmt.Sprintf("%.2fx", r.SpeedupRatio)
		}

		sb.WriteString(fmt.Sprintf("| %s | %s | %s | %s | %s | %s | %s | %s |\n",
			r.DataSize, nativeThroughput, toolThroughput, speedup,
			nativeMem, toolMem, nativeDuration, toolDuration))
	}

	sb.WriteString("\n")
}

func writeSummary(sb *strings.Builder, results []ComparisonResult) {
	if len(results) == 0 {
		sb.WriteString("No results available.\n")
		return
	}

	var totalSpeedup float64
	var count int

	for _, r := range results {
		if r.SpeedupRatio > 0 {
			totalSpeedup += r.SpeedupRatio
			count++
		}
	}

	if count > 0 {
		avgSpeedup := totalSpeedup / float64(count)
		sb.WriteString(fmt.Sprintf("- **Average speedup**: %.2fx across %d comparisons\n", avgSpeedup, count))
	}

	// Find best and worst
	var bestResult, worstResult *ComparisonResult
	for i := range results {
		r := &results[i]
		if r.SpeedupRatio <= 0 {
			continue
		}
		if bestResult == nil || r.SpeedupRatio > bestResult.SpeedupRatio {
			bestResult = r
		}
		if worstResult == nil || r.SpeedupRatio < worstResult.SpeedupRatio {
			worstResult = r
		}
	}

	if bestResult != nil {
		sb.WriteString(fmt.Sprintf("- **Best speedup**: %.2fx (%s %s %s)\n",
			bestResult.SpeedupRatio, bestResult.Database, bestResult.Operation, bestResult.DataSize))
	}
	if worstResult != nil {
		sb.WriteString(fmt.Sprintf("- **Worst speedup**: %.2fx (%s %s %s)\n",
			worstResult.SpeedupRatio, worstResult.Database, worstResult.Operation, worstResult.DataSize))
	}

	sb.WriteString("\n")
}

// FormatBenchmarkResult formats a single benchmark result as a readable string
func FormatBenchmarkResult(r *BenchmarkResult) string {
	return fmt.Sprintf("%-30s  %8.1f MB/s  %6.1f MB mem  %s",
		r.Name,
		r.Throughput,
		float64(r.AllocBytes)/(1024*1024),
		r.Duration.Round(time.Millisecond))
}
