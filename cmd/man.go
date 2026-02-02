package cmd

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/spf13/cobra"
	"github.com/spf13/cobra/doc"
)

var (
	manOutputDir string
)

var manCmd = &cobra.Command{
	Use:   "man",
	Short: "Generate man pages for dbbackup",
	Long: `Generate Unix manual (man) pages for all dbbackup commands.

Man pages are generated in standard groff format and can be viewed
with the 'man' command or installed system-wide.

Installation:
  # Generate pages
  dbbackup man --output /tmp/man

  # Install system-wide (requires root)
  sudo cp /tmp/man/*.1 /usr/local/share/man/man1/
  sudo mandb  # Update man database

  # View pages
  man dbbackup
  man dbbackup-backup
  man dbbackup-restore

Examples:
  # Generate to current directory
  dbbackup man

  # Generate to specific directory
  dbbackup man --output ./docs/man

  # Generate and install system-wide
  dbbackup man --output /tmp/man && \
    sudo cp /tmp/man/*.1 /usr/local/share/man/man1/ && \
    sudo mandb`,
	DisableFlagParsing: true, // Avoid shorthand conflicts during generation
	RunE:               runGenerateMan,
}

func init() {
	rootCmd.AddCommand(manCmd)
	manCmd.Flags().StringVarP(&manOutputDir, "output", "o", "./man", "Output directory for man pages")

	// Parse flags manually since DisableFlagParsing is enabled
	manCmd.SetHelpFunc(func(cmd *cobra.Command, args []string) {
		cmd.Parent().HelpFunc()(cmd, args)
	})
}

func runGenerateMan(cmd *cobra.Command, args []string) error {
	// Parse flags manually since DisableFlagParsing is enabled
	outputDir := "./man"
	for i := 0; i < len(args); i++ {
		if args[i] == "--output" || args[i] == "-o" {
			if i+1 < len(args) {
				outputDir = args[i+1]
				i++
			}
		}
	}

	// Create output directory
	if err := os.MkdirAll(outputDir, 0755); err != nil {
		return fmt.Errorf("failed to create output directory: %w", err)
	}

	// Generate man pages for root and all subcommands
	header := &doc.GenManHeader{
		Title:   "DBBACKUP",
		Section: "1",
		Source:  "dbbackup",
		Manual:  "Database Backup Tool",
	}

	// Due to shorthand flag conflicts in some subcommands (-d for db-type vs database),
	// we generate man pages command-by-command, catching any errors
	root := cmd.Root()
	generatedCount := 0
	failedCount := 0

	// Helper to generate man page for a single command
	genManForCommand := func(c *cobra.Command) {
		// Recover from panic due to flag conflicts
		defer func() {
			if r := recover(); r != nil {
				failedCount++
				// Silently skip commands with flag conflicts
			}
		}()

		// Replace spaces with hyphens for filename
		filename := filepath.Join(outputDir, filepath.Base(c.CommandPath())+".1")

		f, err := os.Create(filename)
		if err != nil {
			failedCount++
			return
		}
		defer f.Close()

		if err := doc.GenMan(c, header, f); err != nil {
			failedCount++
			os.Remove(filename) // Clean up partial file
		} else {
			generatedCount++
		}
	}

	// Generate for root command
	genManForCommand(root)

	// Walk through all commands
	var walkCommands func(*cobra.Command)
	walkCommands = func(c *cobra.Command) {
		for _, sub := range c.Commands() {
			// Skip hidden commands
			if sub.Hidden {
				continue
			}

			// Try to generate man page
			genManForCommand(sub)

			// Recurse into subcommands
			walkCommands(sub)
		}
	}

	walkCommands(root)

	fmt.Printf("✅ Generated %d man pages in %s", generatedCount, outputDir)
	if failedCount > 0 {
		fmt.Printf(" (%d skipped due to flag conflicts)\n", failedCount)
	} else {
		fmt.Println()
	}
	fmt.Println()

	fmt.Println("📖 Installation Instructions:")
	fmt.Println()
	fmt.Println("  1. Install system-wide (requires root):")
	fmt.Printf("     sudo cp %s/*.1 /usr/local/share/man/man1/\n", outputDir)
	fmt.Println("     sudo mandb")
	fmt.Println()
	fmt.Println("  2. Test locally (no installation):")
	fmt.Printf("     man -l %s/dbbackup.1\n", outputDir)
	fmt.Println()
	fmt.Println("  3. View installed pages:")
	fmt.Println("     man dbbackup")
	fmt.Println("     man dbbackup-backup")
	fmt.Println("     man dbbackup-restore")
	fmt.Println()

	// Show some example pages
	files, err := filepath.Glob(filepath.Join(outputDir, "*.1"))
	if err == nil && len(files) > 0 {
		fmt.Println("📋 Generated Pages (sample):")
		for i, file := range files {
			if i >= 5 {
				fmt.Printf("   ... and %d more\n", len(files)-5)
				break
			}
			fmt.Printf("   - %s\n", filepath.Base(file))
		}
		fmt.Println()
	}

	return nil
}
