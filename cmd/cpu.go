package cmd

import (
	"context"
	"fmt"

	"dbbackup/internal/cpu"

	"github.com/spf13/cobra"
)

var cpuCmd = &cobra.Command{
	Use:   "cpu",
	Short: "Show CPU information and optimization settings",
	Long: `Display detailed CPU information, ISA features, NUMA topology,
cache hierarchy, frequency governor, memory bandwidth, and
vendor-specific tuning recommendations.

Detects Intel vs AMD architecture and adjusts parallelism, batch sizes,
and compression algorithm accordingly. On hybrid Intel CPUs (Alder Lake+)
it identifies P-cores and E-cores and pins heavy work to P-cores.`,
	RunE: func(cmd *cobra.Command, args []string) error {
		return runCPUInfo(cmd.Context())
	},
}

func runCPUInfo(ctx context.Context) error {
	log.Info("Detecting CPU information...")

	// Optimize CPU settings if auto-detect is enabled
	if cfg.AutoDetectCores {
		if err := cfg.OptimizeForCPU(); err != nil {
			log.Warn("CPU optimization failed", "error", err)
		}
	}

	// Get CPU information
	cpuInfo, err := cfg.GetCPUInfo()
	if err != nil {
		return fmt.Errorf("failed to detect CPU: %w", err)
	}

	fmt.Println("=== CPU Information ===")
	fmt.Print(cpuInfo.FormatCPUInfo())

	fmt.Println("\n=== Current Configuration ===")
	fmt.Printf("Auto-detect cores: %t\n", cfg.AutoDetectCores)
	fmt.Printf("CPU auto-tune: %t\n", cfg.CPUAutoTune)
	fmt.Printf("CPU workload type: %s\n", cfg.CPUWorkloadType)
	fmt.Printf("Parallel jobs (restore): %d\n", cfg.Jobs)
	fmt.Printf("Dump jobs (backup): %d\n", cfg.DumpJobs)
	fmt.Printf("Maximum cores limit: %d\n", cfg.MaxCores)
	fmt.Printf("Compression algorithm: %s\n", cfg.CompressionAlgorithm)
	fmt.Printf("Buffer size: %d KB\n", cfg.BufferSize/1024)
	fmt.Printf("MySQL batch size: %d rows\n", cfg.MySQLBatchSize)

	// Show optimization recommendations
	fmt.Println("\n=== Optimization Recommendations ===")
	if cpuInfo.PhysicalCores > 1 {
		if cfg.CPUWorkloadType == "balanced" {
			optimal, _ := cfg.CPUDetector.CalculateOptimalJobs("balanced", cfg.MaxCores)
			fmt.Printf("Recommended jobs (balanced): %d\n", optimal)
		}
		if cfg.CPUWorkloadType == "io-intensive" {
			optimal, _ := cfg.CPUDetector.CalculateOptimalJobs("io-intensive", cfg.MaxCores)
			fmt.Printf("Recommended jobs (I/O intensive): %d\n", optimal)
		}
		if cfg.CPUWorkloadType == "cpu-intensive" {
			optimal, _ := cfg.CPUDetector.CalculateOptimalJobs("cpu-intensive", cfg.MaxCores)
			fmt.Printf("Recommended jobs (CPU intensive): %d\n", optimal)
		}
	}

	// Run full optimisation detection and print report
	if cfg.CPUOptimizations == nil {
		cfg.CPUOptimizations = cpu.DetectOptimizations(cpuInfo)
	}
	fmt.Println()
	fmt.Print(cfg.CPUOptimizations.FormatReport())

	// Show current vs optimal
	if cfg.AutoDetectCores {
		fmt.Println("\n[OK] CPU optimization is enabled")
		if cfg.CPUAutoTune {
			fmt.Println("[OK] Vendor-aware auto-tuning is active (Intel/AMD/ARM-specific settings)")
		}
		fmt.Println("Job counts are automatically optimized based on detected hardware")
	} else {
		fmt.Println("\n[WARN] CPU optimization is disabled")
		fmt.Println("Consider enabling --auto-detect-cores for better performance")
	}

	return nil
}

func init() {
	rootCmd.AddCommand(cpuCmd)
}
