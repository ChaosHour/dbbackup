// Package cmd - cloud sync command
package cmd

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"dbbackup/internal/cloud"

	"github.com/spf13/cobra"
)

var (
	syncDryRun       bool
	syncDelete       bool
	syncNewerOnly    bool
	syncDatabaseFilter string
)

var cloudSyncCmd = &cobra.Command{
	Use:   "sync [local-dir]",
	Short: "Sync local backups to cloud storage",
	Long: `Sync local backup directory with cloud storage.

Uploads new and updated backups to cloud, optionally deleting
files in cloud that no longer exist locally.

Examples:
  # Sync backup directory to cloud
  dbbackup cloud sync /backups

  # Dry run - show what would be synced
  dbbackup cloud sync /backups --dry-run

  # Sync and delete orphaned cloud files
  dbbackup cloud sync /backups --delete

  # Only upload newer files
  dbbackup cloud sync /backups --newer-only

  # Sync specific database backups
  dbbackup cloud sync /backups --database mydb`,
	Args: cobra.ExactArgs(1),
	RunE: runCloudSync,
}

func init() {
	cloudCmd.AddCommand(cloudSyncCmd)

	// Sync-specific flags
	cloudSyncCmd.Flags().BoolVar(&syncDryRun, "dry-run", false, "Show what would be synced without uploading")
	cloudSyncCmd.Flags().BoolVar(&syncDelete, "delete", false, "Delete cloud files that don't exist locally")
	cloudSyncCmd.Flags().BoolVar(&syncNewerOnly, "newer-only", false, "Only upload files newer than cloud version")
	cloudSyncCmd.Flags().StringVar(&syncDatabaseFilter, "database", "", "Only sync backups for specific database")

	// Cloud configuration flags
	cloudSyncCmd.Flags().StringVar(&cloudProvider, "cloud-provider", getEnv("DBBACKUP_CLOUD_PROVIDER", "s3"), "Cloud provider (s3, minio, b2)")
	cloudSyncCmd.Flags().StringVar(&cloudBucket, "cloud-bucket", getEnv("DBBACKUP_CLOUD_BUCKET", ""), "Bucket name")
	cloudSyncCmd.Flags().StringVar(&cloudRegion, "cloud-region", getEnv("DBBACKUP_CLOUD_REGION", "us-east-1"), "Region")
	cloudSyncCmd.Flags().StringVar(&cloudEndpoint, "cloud-endpoint", getEnv("DBBACKUP_CLOUD_ENDPOINT", ""), "Custom endpoint (for MinIO)")
	cloudSyncCmd.Flags().StringVar(&cloudAccessKey, "cloud-access-key", getEnv("DBBACKUP_CLOUD_ACCESS_KEY", getEnv("AWS_ACCESS_KEY_ID", "")), "Access key")
	cloudSyncCmd.Flags().StringVar(&cloudSecretKey, "cloud-secret-key", getEnv("DBBACKUP_CLOUD_SECRET_KEY", getEnv("AWS_SECRET_ACCESS_KEY", "")), "Secret key")
	cloudSyncCmd.Flags().StringVar(&cloudPrefix, "cloud-prefix", getEnv("DBBACKUP_CLOUD_PREFIX", ""), "Key prefix")
	cloudSyncCmd.Flags().StringVar(&cloudBandwidthLimit, "bandwidth-limit", getEnv("DBBACKUP_BANDWIDTH_LIMIT", ""), "Bandwidth limit (e.g., 10MB/s, 100Mbps)")
	cloudSyncCmd.Flags().BoolVarP(&cloudVerbose, "verbose", "v", false, "Verbose output")
}

type syncAction struct {
	Action   string // "upload", "skip", "delete"
	Filename string
	Size     int64
	Reason   string
}

func runCloudSync(cmd *cobra.Command, args []string) error {
	localDir := args[0]

	// Validate local directory
	info, err := os.Stat(localDir)
	if err != nil {
		return fmt.Errorf("cannot access directory: %w", err)
	}
	if !info.IsDir() {
		return fmt.Errorf("not a directory: %s", localDir)
	}

	backend, err := getCloudBackend()
	if err != nil {
		return err
	}

	ctx := context.Background()

	fmt.Println()
	fmt.Println("╔═══════════════════════════════════════════════════════════════╗")
	fmt.Println("║                    Cloud Sync                                  ║")
	fmt.Println("╠═══════════════════════════════════════════════════════════════╣")
	fmt.Printf("║  Local:   %-52s ║\n", truncateSyncString(localDir, 52))
	fmt.Printf("║  Cloud:   %-52s ║\n", truncateSyncString(fmt.Sprintf("%s/%s", backend.Name(), cloudBucket), 52))
	if syncDryRun {
		fmt.Println("║  Mode:    DRY RUN (no changes will be made)                   ║")
	}
	fmt.Println("╚═══════════════════════════════════════════════════════════════╝")
	fmt.Println()

	// Get local files
	localFiles := make(map[string]os.FileInfo)
	err = filepath.Walk(localDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			return nil
		}

		// Only include backup files
		ext := strings.ToLower(filepath.Ext(path))
		if !isSyncBackupFile(ext) {
			return nil
		}

		// Apply database filter
		if syncDatabaseFilter != "" && !strings.Contains(filepath.Base(path), syncDatabaseFilter) {
			return nil
		}

		relPath, _ := filepath.Rel(localDir, path)
		localFiles[relPath] = info
		return nil
	})
	if err != nil {
		return fmt.Errorf("failed to scan local directory: %w", err)
	}

	// Get cloud files
	cloudBackups, err := backend.List(ctx, cloudPrefix)
	if err != nil {
		return fmt.Errorf("failed to list cloud files: %w", err)
	}

	cloudFiles := make(map[string]cloud.BackupInfo)
	for _, b := range cloudBackups {
		cloudFiles[b.Name] = b
	}

	// Analyze sync actions
	var actions []syncAction
	var uploadCount, skipCount, deleteCount int
	var uploadSize int64

	// Check local files
	for filename, info := range localFiles {
		cloudInfo, existsInCloud := cloudFiles[filename]

		if !existsInCloud {
			// New file - needs upload
			actions = append(actions, syncAction{
				Action:   "upload",
				Filename: filename,
				Size:     info.Size(),
				Reason:   "new file",
			})
			uploadCount++
			uploadSize += info.Size()
		} else if syncNewerOnly {
			// Check if local is newer
			if info.ModTime().After(cloudInfo.LastModified) {
				actions = append(actions, syncAction{
					Action:   "upload",
					Filename: filename,
					Size:     info.Size(),
					Reason:   "local is newer",
				})
				uploadCount++
				uploadSize += info.Size()
			} else {
				actions = append(actions, syncAction{
					Action:   "skip",
					Filename: filename,
					Size:     info.Size(),
					Reason:   "cloud is up to date",
				})
				skipCount++
			}
		} else {
			// Check by size (simpler than hash)
			if info.Size() != cloudInfo.Size {
				actions = append(actions, syncAction{
					Action:   "upload",
					Filename: filename,
					Size:     info.Size(),
					Reason:   "size mismatch",
				})
				uploadCount++
				uploadSize += info.Size()
			} else {
				actions = append(actions, syncAction{
					Action:   "skip",
					Filename: filename,
					Size:     info.Size(),
					Reason:   "already synced",
				})
				skipCount++
			}
		}
	}

	// Check for cloud files to delete
	if syncDelete {
		for cloudFile := range cloudFiles {
			if _, existsLocally := localFiles[cloudFile]; !existsLocally {
				actions = append(actions, syncAction{
					Action:   "delete",
					Filename: cloudFile,
					Size:     cloudFiles[cloudFile].Size,
					Reason:   "not in local",
				})
				deleteCount++
			}
		}
	}

	// Show summary
	fmt.Printf("📊 Sync Summary\n")
	fmt.Printf("   Local files:  %d\n", len(localFiles))
	fmt.Printf("   Cloud files:  %d\n", len(cloudFiles))
	fmt.Printf("   To upload:    %d (%s)\n", uploadCount, cloud.FormatSize(uploadSize))
	fmt.Printf("   To skip:      %d\n", skipCount)
	if syncDelete {
		fmt.Printf("   To delete:    %d\n", deleteCount)
	}
	fmt.Println()

	if uploadCount == 0 && deleteCount == 0 {
		fmt.Println("✅ Already in sync - nothing to do!")
		return nil
	}

	// Verbose action list
	if cloudVerbose || syncDryRun {
		fmt.Println("📋 Actions:")
		for _, action := range actions {
			if action.Action == "skip" && !cloudVerbose {
				continue
			}
			icon := "📤"
			if action.Action == "skip" {
				icon = "⏭️"
			} else if action.Action == "delete" {
				icon = "🗑️"
			}
			fmt.Printf("   %s %-8s %-40s (%s)\n", icon, action.Action, truncateSyncString(action.Filename, 40), action.Reason)
		}
		fmt.Println()
	}

	if syncDryRun {
		fmt.Println("🔍 Dry run complete - no changes made")
		return nil
	}

	// Execute sync
	fmt.Println("🚀 Starting sync...")
	fmt.Println()

	var successUploads, successDeletes int
	var failedUploads, failedDeletes int

	for _, action := range actions {
		switch action.Action {
		case "upload":
			localPath := filepath.Join(localDir, action.Filename)
			fmt.Printf("📤 Uploading: %s\n", action.Filename)

			err := backend.Upload(ctx, localPath, action.Filename, nil)
			if err != nil {
				fmt.Printf("   ❌ Failed: %v\n", err)
				failedUploads++
			} else {
				fmt.Printf("   ✅ Done (%s)\n", cloud.FormatSize(action.Size))
				successUploads++
			}

		case "delete":
			fmt.Printf("🗑️ Deleting: %s\n", action.Filename)

			err := backend.Delete(ctx, action.Filename)
			if err != nil {
				fmt.Printf("   ❌ Failed: %v\n", err)
				failedDeletes++
			} else {
				fmt.Printf("   ✅ Deleted\n")
				successDeletes++
			}
		}
	}

	// Final summary
	fmt.Println()
	fmt.Println("═══════════════════════════════════════════════════════════════")
	fmt.Printf("✅ Sync Complete\n")
	fmt.Printf("   Uploaded: %d/%d\n", successUploads, uploadCount)
	if syncDelete {
		fmt.Printf("   Deleted:  %d/%d\n", successDeletes, deleteCount)
	}
	if failedUploads > 0 || failedDeletes > 0 {
		fmt.Printf("   ⚠️ Failures: %d\n", failedUploads+failedDeletes)
	}
	fmt.Println("═══════════════════════════════════════════════════════════════")

	return nil
}

func isSyncBackupFile(ext string) bool {
	backupExts := []string{
		".dump", ".sql", ".gz", ".xz", ".zst",
		".backup", ".bak", ".dmp",
	}
	for _, e := range backupExts {
		if ext == e {
			return true
		}
	}
	return false
}

func truncateSyncString(s string, max int) string {
	if len(s) <= max {
		return s
	}
	return s[:max-3] + "..."
}
