// Package cmd - cross-region sync command
package cmd

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"dbbackup/internal/cloud"
	"dbbackup/internal/logger"

	"github.com/spf13/cobra"
)

var (
	// Source cloud configuration
	sourceProvider  string
	sourceBucket    string
	sourceRegion    string
	sourceEndpoint  string
	sourceAccessKey string
	sourceSecretKey string
	sourcePrefix    string

	// Destination cloud configuration
	destProvider  string
	destBucket    string
	destRegion    string
	destEndpoint  string
	destAccessKey string
	destSecretKey string
	destPrefix    string

	// Sync options
	crossSyncDryRun    bool
	crossSyncDelete    bool
	crossSyncNewerOnly bool
	crossSyncParallel  int
	crossSyncFilterDB  string
	crossSyncFilterAge int // days
)

var crossRegionSyncCmd = &cobra.Command{
	Use:   "cross-region-sync",
	Short: "Sync backups between cloud regions",
	Long: `Sync backups from one cloud region to another for disaster recovery.

This command copies backups from a source cloud storage location to a 
destination cloud storage location, which can be in a different region,
provider, or even different cloud service.

Use Cases:
- Geographic redundancy (EU → US, Asia → EU)
- Provider redundancy (AWS → GCS, Azure → S3)
- Cost optimization (Standard → Archive tier)
- Compliance (keep copies in specific regions)

Examples:
  # Sync S3 us-east-1 to us-west-2
  dbbackup cross-region-sync \
    --source-provider s3 --source-bucket prod-backups --source-region us-east-1 \
    --dest-provider s3 --dest-bucket dr-backups --dest-region us-west-2

  # Dry run to preview what would be copied
  dbbackup cross-region-sync --dry-run \
    --source-provider s3 --source-bucket backups --source-region eu-west-1 \
    --dest-provider gcs --dest-bucket backups-dr --dest-region us-central1

  # Sync with deletion of orphaned files
  dbbackup cross-region-sync --delete \
    --source-provider s3 --source-bucket primary \
    --dest-provider s3 --dest-bucket secondary

  # Sync only recent backups (last 30 days)
  dbbackup cross-region-sync --age 30 \
    --source-provider azure --source-bucket backups \
    --dest-provider s3 --dest-bucket dr-backups

  # Sync specific database with parallel uploads
  dbbackup cross-region-sync --database mydb --parallel 3 \
    --source-provider s3 --source-bucket prod \
    --dest-provider s3 --dest-bucket dr

  # Use environment variables for credentials
  export DBBACKUP_SOURCE_ACCESS_KEY=xxx
  export DBBACKUP_SOURCE_SECRET_KEY=xxx
  export DBBACKUP_DEST_ACCESS_KEY=yyy
  export DBBACKUP_DEST_SECRET_KEY=yyy
  dbbackup cross-region-sync \
    --source-provider s3 --source-bucket prod --source-region us-east-1 \
    --dest-provider s3 --dest-bucket dr --dest-region us-west-2`,
	RunE: runCrossRegionSync,
}

func init() {
	cloudCmd.AddCommand(crossRegionSyncCmd)

	// Source configuration
	crossRegionSyncCmd.Flags().StringVar(&sourceProvider, "source-provider", getEnv("DBBACKUP_SOURCE_PROVIDER", "s3"), "Source cloud provider (s3, minio, b2, azure, gcs)")
	crossRegionSyncCmd.Flags().StringVar(&sourceBucket, "source-bucket", getEnv("DBBACKUP_SOURCE_BUCKET", ""), "Source bucket/container name")
	crossRegionSyncCmd.Flags().StringVar(&sourceRegion, "source-region", getEnv("DBBACKUP_SOURCE_REGION", ""), "Source region")
	crossRegionSyncCmd.Flags().StringVar(&sourceEndpoint, "source-endpoint", getEnv("DBBACKUP_SOURCE_ENDPOINT", ""), "Source custom endpoint (for MinIO/B2)")
	crossRegionSyncCmd.Flags().StringVar(&sourceAccessKey, "source-access-key", getEnv("DBBACKUP_SOURCE_ACCESS_KEY", ""), "Source access key")
	crossRegionSyncCmd.Flags().StringVar(&sourceSecretKey, "source-secret-key", getEnv("DBBACKUP_SOURCE_SECRET_KEY", ""), "Source secret key")
	crossRegionSyncCmd.Flags().StringVar(&sourcePrefix, "source-prefix", getEnv("DBBACKUP_SOURCE_PREFIX", ""), "Source path prefix")

	// Destination configuration
	crossRegionSyncCmd.Flags().StringVar(&destProvider, "dest-provider", getEnv("DBBACKUP_DEST_PROVIDER", "s3"), "Destination cloud provider (s3, minio, b2, azure, gcs)")
	crossRegionSyncCmd.Flags().StringVar(&destBucket, "dest-bucket", getEnv("DBBACKUP_DEST_BUCKET", ""), "Destination bucket/container name")
	crossRegionSyncCmd.Flags().StringVar(&destRegion, "dest-region", getEnv("DBBACKUP_DEST_REGION", ""), "Destination region")
	crossRegionSyncCmd.Flags().StringVar(&destEndpoint, "dest-endpoint", getEnv("DBBACKUP_DEST_ENDPOINT", ""), "Destination custom endpoint (for MinIO/B2)")
	crossRegionSyncCmd.Flags().StringVar(&destAccessKey, "dest-access-key", getEnv("DBBACKUP_DEST_ACCESS_KEY", ""), "Destination access key")
	crossRegionSyncCmd.Flags().StringVar(&destSecretKey, "dest-secret-key", getEnv("DBBACKUP_DEST_SECRET_KEY", ""), "Destination secret key")
	crossRegionSyncCmd.Flags().StringVar(&destPrefix, "dest-prefix", getEnv("DBBACKUP_DEST_PREFIX", ""), "Destination path prefix")

	// Sync options
	crossRegionSyncCmd.Flags().BoolVar(&crossSyncDryRun, "dry-run", false, "Preview what would be synced without copying")
	crossRegionSyncCmd.Flags().BoolVar(&crossSyncDelete, "delete", false, "Delete destination files that don't exist in source")
	crossRegionSyncCmd.Flags().BoolVar(&crossSyncNewerOnly, "newer-only", false, "Only copy files newer than destination version")
	crossRegionSyncCmd.Flags().IntVar(&crossSyncParallel, "parallel", 2, "Number of parallel transfers")
	crossRegionSyncCmd.Flags().StringVar(&crossSyncFilterDB, "database", "", "Only sync backups for specific database")
	crossRegionSyncCmd.Flags().IntVar(&crossSyncFilterAge, "age", 0, "Only sync backups from last N days (0 = all)")

	// Mark required flags
	crossRegionSyncCmd.MarkFlagRequired("source-bucket")
	crossRegionSyncCmd.MarkFlagRequired("dest-bucket")
}

func runCrossRegionSync(cmd *cobra.Command, args []string) error {
	ctx := context.Background()

	// Validate configuration
	if sourceBucket == "" {
		return fmt.Errorf("source bucket is required")
	}
	if destBucket == "" {
		return fmt.Errorf("destination bucket is required")
	}

	// Create source backend
	sourceBackend, err := createCloudBackend("source", &cloud.Config{
		Provider:  sourceProvider,
		Bucket:    sourceBucket,
		Region:    sourceRegion,
		Endpoint:  sourceEndpoint,
		AccessKey: sourceAccessKey,
		SecretKey: sourceSecretKey,
		Prefix:    sourcePrefix,
	})
	if err != nil {
		return fmt.Errorf("failed to create source backend: %w", err)
	}

	// Create destination backend
	destBackend, err := createCloudBackend("destination", &cloud.Config{
		Provider:  destProvider,
		Bucket:    destBucket,
		Region:    destRegion,
		Endpoint:  destEndpoint,
		AccessKey: destAccessKey,
		SecretKey: destSecretKey,
		Prefix:    destPrefix,
	})
	if err != nil {
		return fmt.Errorf("failed to create destination backend: %w", err)
	}

	// Display configuration
	fmt.Printf("Cross-Region Sync Configuration\n")
	fmt.Printf("================================\n\n")
	fmt.Printf("Source:\n")
	fmt.Printf("  Provider: %s\n", sourceProvider)
	fmt.Printf("  Bucket:   %s\n", sourceBucket)
	if sourceRegion != "" {
		fmt.Printf("  Region:   %s\n", sourceRegion)
	}
	if sourcePrefix != "" {
		fmt.Printf("  Prefix:   %s\n", sourcePrefix)
	}
	fmt.Printf("\nDestination:\n")
	fmt.Printf("  Provider: %s\n", destProvider)
	fmt.Printf("  Bucket:   %s\n", destBucket)
	if destRegion != "" {
		fmt.Printf("  Region:   %s\n", destRegion)
	}
	if destPrefix != "" {
		fmt.Printf("  Prefix:   %s\n", destPrefix)
	}
	fmt.Printf("\nOptions:\n")
	fmt.Printf("  Parallel: %d\n", crossSyncParallel)
	if crossSyncFilterDB != "" {
		fmt.Printf("  Database: %s\n", crossSyncFilterDB)
	}
	if crossSyncFilterAge > 0 {
		fmt.Printf("  Age:      last %d days\n", crossSyncFilterAge)
	}
	if crossSyncDryRun {
		fmt.Printf("  Mode:     DRY RUN (no changes will be made)\n")
	}
	fmt.Printf("\n")

	// List source backups
	logger.Info("Listing source backups...")
	sourceBackups, err := sourceBackend.List(ctx, "")
	if err != nil {
		return fmt.Errorf("failed to list source backups: %w", err)
	}

	// Apply filters
	sourceBackups = filterBackups(sourceBackups, crossSyncFilterDB, crossSyncFilterAge)

	if len(sourceBackups) == 0 {
		fmt.Printf("No backups found in source matching filters\n")
		return nil
	}

	fmt.Printf("Found %d backups in source\n", len(sourceBackups))

	// List destination backups
	logger.Info("Listing destination backups...")
	destBackups, err := destBackend.List(ctx, "")
	if err != nil {
		return fmt.Errorf("failed to list destination backups: %w", err)
	}

	fmt.Printf("Found %d backups in destination\n\n", len(destBackups))

	// Build destination map for quick lookup
	destMap := make(map[string]cloud.BackupInfo)
	for _, backup := range destBackups {
		destMap[backup.Name] = backup
	}

	// Determine what needs to be copied
	var toCopy []cloud.BackupInfo
	var toDelete []cloud.BackupInfo

	for _, srcBackup := range sourceBackups {
		destBackup, existsInDest := destMap[srcBackup.Name]

		if !existsInDest {
			// File doesn't exist in destination - needs copy
			toCopy = append(toCopy, srcBackup)
		} else if crossSyncNewerOnly && srcBackup.LastModified.After(destBackup.LastModified) {
			// Newer file in source - needs copy
			toCopy = append(toCopy, srcBackup)
		} else if !crossSyncNewerOnly && srcBackup.Size != destBackup.Size {
			// Size mismatch - needs copy
			toCopy = append(toCopy, srcBackup)
		}

		// Mark as found in source
		delete(destMap, srcBackup.Name)
	}

	// Remaining files in destMap are orphaned (exist in dest but not in source)
	if crossSyncDelete {
		for _, backup := range destMap {
			toDelete = append(toDelete, backup)
		}
	}

	// Sort for consistent output
	sort.Slice(toCopy, func(i, j int) bool {
		return toCopy[i].Name < toCopy[j].Name
	})
	sort.Slice(toDelete, func(i, j int) bool {
		return toDelete[i].Name < toDelete[j].Name
	})

	// Display sync plan
	fmt.Printf("Sync Plan\n")
	fmt.Printf("=========\n\n")

	if len(toCopy) > 0 {
		totalSize := int64(0)
		for _, backup := range toCopy {
			totalSize += backup.Size
		}
		fmt.Printf("To Copy: %d files (%s)\n", len(toCopy), cloud.FormatSize(totalSize))
		if len(toCopy) <= 10 {
			for _, backup := range toCopy {
				fmt.Printf("  - %s (%s)\n", backup.Name, cloud.FormatSize(backup.Size))
			}
		} else {
			for i := 0; i < 5; i++ {
				fmt.Printf("  - %s (%s)\n", toCopy[i].Name, cloud.FormatSize(toCopy[i].Size))
			}
			fmt.Printf("  ... and %d more files\n", len(toCopy)-5)
		}
		fmt.Printf("\n")
	} else {
		fmt.Printf("To Copy: 0 files (all in sync)\n\n")
	}

	if crossSyncDelete && len(toDelete) > 0 {
		totalSize := int64(0)
		for _, backup := range toDelete {
			totalSize += backup.Size
		}
		fmt.Printf("To Delete: %d files (%s)\n", len(toDelete), cloud.FormatSize(totalSize))
		if len(toDelete) <= 10 {
			for _, backup := range toDelete {
				fmt.Printf("  - %s (%s)\n", backup.Name, cloud.FormatSize(backup.Size))
			}
		} else {
			for i := 0; i < 5; i++ {
				fmt.Printf("  - %s (%s)\n", toDelete[i].Name, cloud.FormatSize(toDelete[i].Size))
			}
			fmt.Printf("  ... and %d more files\n", len(toDelete)-5)
		}
		fmt.Printf("\n")
	}

	if crossSyncDryRun {
		fmt.Printf("DRY RUN - No changes made\n")
		return nil
	}

	if len(toCopy) == 0 && len(toDelete) == 0 {
		fmt.Printf("Nothing to sync\n")
		return nil
	}

	// Confirm if not in dry-run mode
	fmt.Printf("Proceed with sync? (y/n): ")
	var response string
	_, _ = fmt.Scanln(&response)
	if !strings.HasPrefix(strings.ToLower(response), "y") {
		fmt.Printf("Sync cancelled\n")
		return nil
	}

	fmt.Printf("\n")

	// Execute copies
	if len(toCopy) > 0 {
		fmt.Printf("Copying files...\n")
		if err := copyBackups(ctx, sourceBackend, destBackend, toCopy, crossSyncParallel); err != nil {
			return fmt.Errorf("copy failed: %w", err)
		}
		fmt.Printf("\n")
	}

	// Execute deletions
	if crossSyncDelete && len(toDelete) > 0 {
		fmt.Printf("Deleting orphaned files...\n")
		if err := deleteBackups(ctx, destBackend, toDelete); err != nil {
			return fmt.Errorf("delete failed: %w", err)
		}
		fmt.Printf("\n")
	}

	fmt.Printf("Sync completed successfully\n")
	return nil
}

func createCloudBackend(label string, cfg *cloud.Config) (cloud.Backend, error) {
	if cfg.Bucket == "" {
		return nil, fmt.Errorf("%s bucket is required", label)
	}

	// Set defaults
	if cfg.MaxRetries == 0 {
		cfg.MaxRetries = 3
	}
	if cfg.Timeout == 0 {
		cfg.Timeout = 300
	}
	cfg.UseSSL = true

	backend, err := cloud.NewBackend(cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to create %s backend: %w", label, err)
	}

	return backend, nil
}

func filterBackups(backups []cloud.BackupInfo, database string, ageInDays int) []cloud.BackupInfo {
	filtered := make([]cloud.BackupInfo, 0, len(backups))

	cutoffTime := time.Time{}
	if ageInDays > 0 {
		cutoffTime = time.Now().AddDate(0, 0, -ageInDays)
	}

	for _, backup := range backups {
		// Filter by database name
		if database != "" && !strings.Contains(backup.Name, database) {
			continue
		}

		// Filter by age
		if ageInDays > 0 && backup.LastModified.Before(cutoffTime) {
			continue
		}

		filtered = append(filtered, backup)
	}

	return filtered
}

func copyBackups(ctx context.Context, source, dest cloud.Backend, backups []cloud.BackupInfo, parallel int) error {
	if parallel < 1 {
		parallel = 1
	}

	var wg sync.WaitGroup
	semaphore := make(chan struct{}, parallel)
	errChan := make(chan error, len(backups))

	successCount := 0
	var mu sync.Mutex

	for i, backup := range backups {
		wg.Add(1)
		go func(idx int, bkp cloud.BackupInfo) {
			defer wg.Done()

			// Acquire semaphore
			semaphore <- struct{}{}
			defer func() { <-semaphore }()

			// Download to temp file
			tempFile := filepath.Join(os.TempDir(), fmt.Sprintf("dbbackup-sync-%d-%s", idx, filepath.Base(bkp.Key)))
			defer func() { _ = os.Remove(tempFile) }()

			// Download from source
			err := source.Download(ctx, bkp.Key, tempFile, func(transferred, total int64) {
				// Progress callback - could be enhanced
			})
			if err != nil {
				errChan <- fmt.Errorf("download %s failed: %w", bkp.Name, err)
				return
			}

			// Upload to destination
			err = dest.Upload(ctx, tempFile, bkp.Key, func(transferred, total int64) {
				// Progress callback - could be enhanced
			})
			if err != nil {
				errChan <- fmt.Errorf("upload %s failed: %w", bkp.Name, err)
				return
			}

			mu.Lock()
			successCount++
			fmt.Printf("  [%d/%d] Copied %s (%s)\n", successCount, len(backups), bkp.Name, cloud.FormatSize(bkp.Size))
			mu.Unlock()

		}(i, backup)
	}

	wg.Wait()
	close(errChan)

	// Check for errors
	var errors []error
	for err := range errChan {
		errors = append(errors, err)
	}

	if len(errors) > 0 {
		fmt.Printf("\nEncountered %d errors during copy:\n", len(errors))
		for _, err := range errors {
			fmt.Printf("  - %v\n", err)
		}
		return fmt.Errorf("%d files failed to copy", len(errors))
	}

	return nil
}

func deleteBackups(ctx context.Context, backend cloud.Backend, backups []cloud.BackupInfo) error {
	successCount := 0

	for _, backup := range backups {
		err := backend.Delete(ctx, backup.Key)
		if err != nil {
			fmt.Printf("  Failed to delete %s: %v\n", backup.Name, err)
			continue
		}
		successCount++
		fmt.Printf("  Deleted %s\n", backup.Name)
	}

	if successCount < len(backups) {
		return fmt.Errorf("deleted %d/%d files (some failed)", successCount, len(backups))
	}

	return nil
}
