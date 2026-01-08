package cmd

import (
	"context"
	"fmt"
	"strings"

	"dbbackup/internal/engine"

	"github.com/spf13/cobra"
)

var engineCmd = &cobra.Command{
	Use:   "engine",
	Short: "Backup engine management commands",
	Long: `Commands for managing and selecting backup engines.

Available engines:
  - mysqldump: Traditional mysqldump backup (all MySQL versions)
  - clone:     MySQL Clone Plugin (MySQL 8.0.17+)
  - snapshot:  Filesystem snapshot (LVM/ZFS/Btrfs)
  - streaming: Direct cloud streaming backup`,
}

var engineListCmd = &cobra.Command{
	Use:   "list",
	Short: "List available backup engines",
	Long:  "List all registered backup engines and their availability status",
	RunE:  runEngineList,
}

var engineInfoCmd = &cobra.Command{
	Use:   "info [engine-name]",
	Short: "Show detailed information about an engine",
	Long:  "Display detailed information about a specific backup engine",
	Args:  cobra.ExactArgs(1),
	RunE:  runEngineInfo,
}

func init() {
	rootCmd.AddCommand(engineCmd)
	engineCmd.AddCommand(engineListCmd)
	engineCmd.AddCommand(engineInfoCmd)
}

func runEngineList(cmd *cobra.Command, args []string) error {
	ctx := context.Background()
	registry := engine.DefaultRegistry

	fmt.Println("Available Backup Engines:")
	fmt.Println(strings.Repeat("-", 70))

	for _, info := range registry.List() {
		eng, err := registry.Get(info.Name)
		if err != nil {
			continue
		}

		avail, err := eng.CheckAvailability(ctx)
		if err != nil {
			fmt.Printf("\n%s (%s)\n", info.Name, info.Description)
			fmt.Printf("  Status:     Error checking availability\n")
			continue
		}

		status := "[Y] Available"
		if !avail.Available {
			status = "[N] Not available"
		}

		fmt.Printf("\n%s (%s)\n", info.Name, info.Description)
		fmt.Printf("  Status:     %s\n", status)
		if !avail.Available && avail.Reason != "" {
			fmt.Printf("  Reason:     %s\n", avail.Reason)
		}
		fmt.Printf("  Restore:    %v\n", eng.SupportsRestore())
		fmt.Printf("  Incremental: %v\n", eng.SupportsIncremental())
		fmt.Printf("  Streaming:  %v\n", eng.SupportsStreaming())
	}

	return nil
}

func runEngineInfo(cmd *cobra.Command, args []string) error {
	ctx := context.Background()
	registry := engine.DefaultRegistry

	eng, err := registry.Get(args[0])
	if err != nil {
		return fmt.Errorf("engine not found: %s", args[0])
	}

	avail, err := eng.CheckAvailability(ctx)
	if err != nil {
		return fmt.Errorf("failed to check availability: %w", err)
	}

	fmt.Printf("Engine: %s\n", eng.Name())
	fmt.Printf("Description: %s\n", eng.Description())
	fmt.Println(strings.Repeat("-", 50))
	fmt.Printf("Available:     %v\n", avail.Available)
	if avail.Reason != "" {
		fmt.Printf("Reason:        %s\n", avail.Reason)
	}
	fmt.Printf("Restore:       %v\n", eng.SupportsRestore())
	fmt.Printf("Incremental:   %v\n", eng.SupportsIncremental())
	fmt.Printf("Streaming:     %v\n", eng.SupportsStreaming())

	return nil
}
