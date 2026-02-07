package cmd

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"dbbackup/internal/metadata"

	"github.com/spf13/cobra"
)

// CheckResult holds the results of a catalog check
type CheckResult struct {
	Directory       string   `json:"directory"`
	TotalArchives   int      `json:"total_archives"`
	WithMeta        int      `json:"with_meta"`
	OrphanArchives  []string `json:"orphan_archives,omitempty"`  // Archives without .meta.json
	StaleMeta       []string `json:"stale_meta,omitempty"`       // .meta.json without archive
	InvalidMeta     []string `json:"invalid_meta,omitempty"`     // Malformed .meta.json
	SizeMismatch    []string `json:"size_mismatch,omitempty"`    // .meta.json size != actual file size
	HealthyBackups  int      `json:"healthy_backups"`
	Issues          int      `json:"issues"`
}

var catalogCheckCmd = &cobra.Command{
	Use:   "check <directory>",
	Short: "Verify backup archives and their metadata",
	Long: `Check backup archives in a directory for consistency issues.

This command detects:
  - Orphan archives: backup files without .meta.json
  - Stale metadata: .meta.json files where the archive is missing
  - Invalid metadata: malformed or unreadable .meta.json files
  - Size mismatches: .meta.json reports different size than actual file

Examples:
  # Check a backup directory
  dbbackup catalog check /backups

  # Check with verbose output
  dbbackup catalog check /backups --verbose

  # Check a single file
  dbbackup catalog check /backups/cluster-2026-02-06.tar.gz

  # Output as JSON
  dbbackup catalog check /backups --format json

  # Auto-fix: generate missing .meta.json files
  dbbackup catalog check /backups --fix`,
	Args: cobra.MinimumNArgs(1),
	RunE: runCatalogCheck,
}

func init() {
	catalogCmd.AddCommand(catalogCheckCmd)
	catalogCheckCmd.Flags().BoolVarP(&catalogVerbose, "verbose", "v", false, "Show detailed output")
	catalogCheckCmd.Flags().Bool("fix", false, "Auto-generate missing .meta.json files (runs catalog generate)")
}

func runCatalogCheck(cmd *cobra.Command, args []string) error {
	target := args[0]
	fix, _ := cmd.Flags().GetBool("fix")

	// Check if target is a single file
	info, err := os.Stat(target)
	if err != nil {
		return fmt.Errorf("cannot access %s: %w", target, err)
	}

	if !info.IsDir() {
		return checkSingleFile(cmd, target, fix)
	}

	return checkDirectory(cmd, target, fix)
}

func checkSingleFile(cmd *cobra.Command, filePath string, fix bool) error {
	fmt.Printf("Checking: %s\n", filePath)
	fmt.Printf("Size:     %s\n\n", formatFileSize(fileSize(filePath)))

	metaPath := filePath + ".meta.json"
	if _, err := os.Stat(metaPath); os.IsNotExist(err) {
		fmt.Printf("❌ No .meta.json found\n")
		if fix {
			fmt.Printf("   → Generating metadata...\n")
			return runCatalogGenerate(cmd, []string{filePath})
		}
		fmt.Printf("   Run: dbbackup catalog generate %s\n", filePath)
		return nil
	}

	// Validate meta
	issues := validateMetaFile(filePath, metaPath)
	if len(issues) == 0 {
		fmt.Printf("✅ Healthy — .meta.json is valid\n")
		if catalogVerbose {
			printMetaSummary(metaPath)
		}
	} else {
		for _, issue := range issues {
			fmt.Printf("❌ %s\n", issue)
		}
		if fix {
			fmt.Printf("   → Regenerating metadata...\n")
			return runCatalogGenerate(cmd, []string{filePath})
		}
	}

	return nil
}

func checkDirectory(cmd *cobra.Command, dir string, fix bool) error {
	result := &CheckResult{Directory: dir}

	// Find all backup archives
	archives := findArchives(dir)
	result.TotalArchives = len(archives)

	// Find all .meta.json files
	metaFiles := findMetaFiles(dir)
	metaSet := make(map[string]bool)
	for _, m := range metaFiles {
		metaSet[strings.TrimSuffix(m, ".meta.json")] = true
	}

	archiveSet := make(map[string]bool)
	for _, a := range archives {
		archiveSet[a] = true
	}

	// Check each archive
	var fixTargets []string
	for _, archive := range archives {
		metaPath := archive + ".meta.json"
		if _, err := os.Stat(metaPath); os.IsNotExist(err) {
			result.OrphanArchives = append(result.OrphanArchives, filepath.Base(archive))
			fixTargets = append(fixTargets, archive)
			continue
		}

		// Validate existing meta
		issues := validateMetaFile(archive, metaPath)
		if len(issues) > 0 {
			for _, issue := range issues {
				result.InvalidMeta = append(result.InvalidMeta,
					fmt.Sprintf("%s: %s", filepath.Base(archive), issue))
			}
			fixTargets = append(fixTargets, archive)
			continue
		}

		result.WithMeta++
		result.HealthyBackups++
	}

	// Check for stale .meta.json (archive deleted but meta remains)
	for _, metaFile := range metaFiles {
		archivePath := strings.TrimSuffix(metaFile, ".meta.json")
		if !archiveSet[archivePath] {
			result.StaleMeta = append(result.StaleMeta, filepath.Base(metaFile))
		}
	}

	result.Issues = len(result.OrphanArchives) + len(result.StaleMeta) +
		len(result.InvalidMeta) + len(result.SizeMismatch)

	// Output results
	if catalogFormat == "json" {
		data, _ := json.MarshalIndent(result, "", "  ")
		fmt.Println(string(data))
		return nil
	}

	// Table output
	printCheckResult(result)

	// Auto-fix if requested
	if fix && len(fixTargets) > 0 {
		fmt.Printf("\n🔧 Fixing %d archives...\n\n", len(fixTargets))
		for _, target := range fixTargets {
			fmt.Printf("  Generating: %s\n", filepath.Base(target))
			if err := runCatalogGenerate(cmd, []string{target}); err != nil {
				fmt.Printf("    ❌ Failed: %v\n", err)
			} else {
				fmt.Printf("    ✅ Done\n")
			}
		}
	}

	if result.Issues > 0 && !fix {
		return fmt.Errorf("%d issue(s) found — run with --fix to auto-repair", result.Issues)
	}

	return nil
}

func printCheckResult(r *CheckResult) {
	fmt.Printf("╔══════════════════════════════════════════╗\n")
	fmt.Printf("║         Catalog Health Check             ║\n")
	fmt.Printf("╠══════════════════════════════════════════╣\n")
	fmt.Printf("║  Directory:  %-27s ║\n", truncPath(r.Directory, 27))
	fmt.Printf("║  Archives:   %-27d ║\n", r.TotalArchives)
	fmt.Printf("║  With meta:  %-27d ║\n", r.WithMeta)
	fmt.Printf("║  Healthy:    %-27d ║\n", r.HealthyBackups)
	fmt.Printf("║  Issues:     %-27d ║\n", r.Issues)
	fmt.Printf("╚══════════════════════════════════════════╝\n")

	if len(r.OrphanArchives) > 0 {
		fmt.Printf("\n⚠️  Orphan archives (no .meta.json):\n")
		for _, f := range r.OrphanArchives {
			fmt.Printf("   • %s\n", f)
		}
		fmt.Printf("   → Fix: dbbackup catalog generate <file> or --fix\n")
	}

	if len(r.StaleMeta) > 0 {
		fmt.Printf("\n⚠️  Stale metadata (archive missing):\n")
		for _, f := range r.StaleMeta {
			fmt.Printf("   • %s\n", f)
		}
		fmt.Printf("   → These .meta.json files can be safely deleted\n")
	}

	if len(r.InvalidMeta) > 0 {
		fmt.Printf("\n❌ Invalid metadata:\n")
		for _, f := range r.InvalidMeta {
			fmt.Printf("   • %s\n", f)
		}
		fmt.Printf("   → Fix: dbbackup catalog generate <file> --force or --fix\n")
	}

	if len(r.SizeMismatch) > 0 {
		fmt.Printf("\n⚠️  Size mismatches:\n")
		for _, f := range r.SizeMismatch {
			fmt.Printf("   • %s\n", f)
		}
		fmt.Printf("   → Fix: dbbackup catalog generate <file> --force\n")
	}

	if r.Issues == 0 {
		fmt.Printf("\n✅ All backups healthy!\n")
	}
}

func validateMetaFile(archivePath, metaPath string) []string {
	var issues []string

	data, err := os.ReadFile(metaPath)
	if err != nil {
		return []string{fmt.Sprintf("cannot read .meta.json: %v", err)}
	}

	// Try parsing as BackupMetadata first
	var backupMeta metadata.BackupMetadata
	var clusterMeta metadata.ClusterMetadata

	errBackup := json.Unmarshal(data, &backupMeta)
	errCluster := json.Unmarshal(data, &clusterMeta)

	if errBackup != nil && errCluster != nil {
		return []string{fmt.Sprintf("malformed JSON: %v", errBackup)}
	}

	// Check if it's a valid structure (has required fields)
	isCluster := clusterMeta.ClusterName != "" || len(clusterMeta.Databases) > 0
	isSingle := backupMeta.Database != ""

	if !isCluster && !isSingle {
		issues = append(issues, "meta.json has no database or cluster_name field")
	}

	// Check file size match
	archiveInfo, err := os.Stat(archivePath)
	if err == nil {
		actualSize := archiveInfo.Size()
		if isSingle && backupMeta.SizeBytes > 0 && backupMeta.SizeBytes != actualSize {
			issues = append(issues, fmt.Sprintf("size mismatch: meta=%s actual=%s",
				formatFileSize(backupMeta.SizeBytes), formatFileSize(actualSize)))
		}
		if isCluster && clusterMeta.TotalSize > 0 && clusterMeta.TotalSize != actualSize {
			issues = append(issues, fmt.Sprintf("size mismatch: meta=%s actual=%s",
				formatFileSize(clusterMeta.TotalSize), formatFileSize(actualSize)))
		}
	}

	// Check timestamp sanity
	if isSingle && backupMeta.Timestamp.IsZero() {
		issues = append(issues, "missing timestamp")
	}
	if isCluster && clusterMeta.Timestamp.IsZero() {
		issues = append(issues, "missing timestamp")
	}

	return issues
}

func findArchives(dir string) []string {
	var archives []string
	patterns := []string{
		"*.tar.gz", "*.sql.gz", "*.sql.lz4", "*.sql.zst",
		"*.dump", "*.dump.gz", "*.sql", "*.backup",
	}

	for _, pattern := range patterns {
		matches, _ := filepath.Glob(filepath.Join(dir, pattern))
		archives = append(archives, matches...)
	}

	// Also check one level of subdirectories
	subdirs, _ := filepath.Glob(filepath.Join(dir, "*"))
	for _, sub := range subdirs {
		info, err := os.Stat(sub)
		if err != nil || !info.IsDir() {
			continue
		}
		for _, pattern := range patterns {
			matches, _ := filepath.Glob(filepath.Join(sub, pattern))
			archives = append(archives, matches...)
		}
	}

	return archives
}

func findMetaFiles(dir string) []string {
	var metas []string
	matches, _ := filepath.Glob(filepath.Join(dir, "*.meta.json"))
	metas = append(metas, matches...)

	// Also one level deep
	subMatches, _ := filepath.Glob(filepath.Join(dir, "*", "*.meta.json"))
	metas = append(metas, subMatches...)

	return metas
}

func printMetaSummary(metaPath string) {
	data, err := os.ReadFile(metaPath)
	if err != nil {
		return
	}

	var m map[string]interface{}
	if json.Unmarshal(data, &m) != nil {
		return
	}

	fmt.Println()
	if db, ok := m["database"].(string); ok {
		fmt.Printf("   Database:  %s\n", db)
	}
	if cn, ok := m["cluster_name"].(string); ok {
		fmt.Printf("   Cluster:   %s\n", cn)
	}
	if dt, ok := m["database_type"].(string); ok {
		fmt.Printf("   Type:      %s\n", dt)
	}
	if ts, ok := m["timestamp"].(string); ok {
		fmt.Printf("   Timestamp: %s\n", ts)
	}
	if dbs, ok := m["databases"].([]interface{}); ok {
		fmt.Printf("   Databases: %d\n", len(dbs))
	}
}

// formatFileSize is already defined in placeholder.go — reused here

func fileSize(path string) int64 {
	info, err := os.Stat(path)
	if err != nil {
		return 0
	}
	return info.Size()
}

func truncPath(p string, maxLen int) string {
	if len(p) <= maxLen {
		return p
	}
	return "..." + p[len(p)-(maxLen-3):]
}
