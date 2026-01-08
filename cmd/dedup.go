package cmd

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"dbbackup/internal/dedup"

	"github.com/spf13/cobra"
)

var dedupCmd = &cobra.Command{
	Use:   "dedup",
	Short: "Deduplicated backup operations",
	Long: `Content-defined chunking deduplication for space-efficient backups.

Similar to restic/borgbackup but with native database dump support.

Features:
- Content-defined chunking (CDC) with Buzhash rolling hash
- SHA-256 content-addressed storage
- AES-256-GCM encryption (optional)
- Gzip compression (optional)
- SQLite index for fast lookups

Storage Structure:
  <dedup-dir>/
    chunks/       # Content-addressed chunk files
      ab/cdef...  # Sharded by first 2 chars of hash
    manifests/    # JSON manifest per backup
    chunks.db     # SQLite index`,
}

var dedupBackupCmd = &cobra.Command{
	Use:   "backup <file>",
	Short: "Create a deduplicated backup of a file",
	Long: `Chunk a file using content-defined chunking and store deduplicated chunks.

Example:
  dbbackup dedup backup /path/to/database.dump
  dbbackup dedup backup mydb.sql --compress --encrypt`,
	Args: cobra.ExactArgs(1),
	RunE: runDedupBackup,
}

var dedupRestoreCmd = &cobra.Command{
	Use:   "restore <manifest-id> <output-file>",
	Short: "Restore a backup from its manifest",
	Long: `Reconstruct a file from its deduplicated chunks.

Example:
  dbbackup dedup restore 2026-01-07_120000_mydb /tmp/restored.dump
  dbbackup dedup list  # to see available manifests`,
	Args: cobra.ExactArgs(2),
	RunE: runDedupRestore,
}

var dedupListCmd = &cobra.Command{
	Use:   "list",
	Short: "List all deduplicated backups",
	RunE:  runDedupList,
}

var dedupStatsCmd = &cobra.Command{
	Use:   "stats",
	Short: "Show deduplication statistics",
	RunE:  runDedupStats,
}

var dedupGCCmd = &cobra.Command{
	Use:   "gc",
	Short: "Garbage collect unreferenced chunks",
	Long: `Remove chunks that are no longer referenced by any manifest.

Run after deleting old backups to reclaim space.`,
	RunE: runDedupGC,
}

var dedupDeleteCmd = &cobra.Command{
	Use:   "delete <manifest-id>",
	Short: "Delete a backup manifest (chunks cleaned by gc)",
	Args:  cobra.ExactArgs(1),
	RunE:  runDedupDelete,
}

// Flags
var (
	dedupDir       string
	dedupCompress  bool
	dedupEncrypt   bool
	dedupKey       string
	dedupName      string
	dedupDBType    string
	dedupDBName    string
	dedupDBHost    string
)

func init() {
	rootCmd.AddCommand(dedupCmd)
	dedupCmd.AddCommand(dedupBackupCmd)
	dedupCmd.AddCommand(dedupRestoreCmd)
	dedupCmd.AddCommand(dedupListCmd)
	dedupCmd.AddCommand(dedupStatsCmd)
	dedupCmd.AddCommand(dedupGCCmd)
	dedupCmd.AddCommand(dedupDeleteCmd)

	// Global dedup flags
	dedupCmd.PersistentFlags().StringVar(&dedupDir, "dedup-dir", "", "Dedup storage directory (default: $BACKUP_DIR/dedup)")
	dedupCmd.PersistentFlags().BoolVar(&dedupCompress, "compress", true, "Compress chunks with gzip")
	dedupCmd.PersistentFlags().BoolVar(&dedupEncrypt, "encrypt", false, "Encrypt chunks with AES-256-GCM")
	dedupCmd.PersistentFlags().StringVar(&dedupKey, "key", "", "Encryption key (hex) or use DBBACKUP_DEDUP_KEY env")

	// Backup-specific flags
	dedupBackupCmd.Flags().StringVar(&dedupName, "name", "", "Optional backup name")
	dedupBackupCmd.Flags().StringVar(&dedupDBType, "db-type", "", "Database type (postgres/mysql)")
	dedupBackupCmd.Flags().StringVar(&dedupDBName, "db-name", "", "Database name")
	dedupBackupCmd.Flags().StringVar(&dedupDBHost, "db-host", "", "Database host")
}

func getDedupDir() string {
	if dedupDir != "" {
		return dedupDir
	}
	if cfg != nil && cfg.BackupDir != "" {
		return filepath.Join(cfg.BackupDir, "dedup")
	}
	return filepath.Join(os.Getenv("HOME"), "db_backups", "dedup")
}

func getEncryptionKey() string {
	if dedupKey != "" {
		return dedupKey
	}
	return os.Getenv("DBBACKUP_DEDUP_KEY")
}

func runDedupBackup(cmd *cobra.Command, args []string) error {
	inputPath := args[0]

	// Open input file
	file, err := os.Open(inputPath)
	if err != nil {
		return fmt.Errorf("failed to open input file: %w", err)
	}
	defer file.Close()

	info, err := file.Stat()
	if err != nil {
		return fmt.Errorf("failed to stat input file: %w", err)
	}

	// Setup dedup storage
	basePath := getDedupDir()
	encKey := ""
	if dedupEncrypt {
		encKey = getEncryptionKey()
		if encKey == "" {
			return fmt.Errorf("encryption enabled but no key provided (use --key or DBBACKUP_DEDUP_KEY)")
		}
	}

	store, err := dedup.NewChunkStore(dedup.StoreConfig{
		BasePath:      basePath,
		Compress:      dedupCompress,
		EncryptionKey: encKey,
	})
	if err != nil {
		return fmt.Errorf("failed to open chunk store: %w", err)
	}

	manifestStore, err := dedup.NewManifestStore(basePath)
	if err != nil {
		return fmt.Errorf("failed to open manifest store: %w", err)
	}

	index, err := dedup.NewChunkIndex(basePath)
	if err != nil {
		return fmt.Errorf("failed to open chunk index: %w", err)
	}
	defer index.Close()

	// Generate manifest ID
	now := time.Now()
	manifestID := now.Format("2006-01-02_150405")
	if dedupDBName != "" {
		manifestID += "_" + dedupDBName
	} else {
		base := filepath.Base(inputPath)
		ext := filepath.Ext(base)
		manifestID += "_" + strings.TrimSuffix(base, ext)
	}

	fmt.Printf("Creating deduplicated backup: %s\n", manifestID)
	fmt.Printf("Input: %s (%s)\n", inputPath, formatBytes(info.Size()))
	fmt.Printf("Store: %s\n", basePath)

	// Hash the entire file for verification
	file.Seek(0, 0)
	h := sha256.New()
	io.Copy(h, file)
	fileHash := hex.EncodeToString(h.Sum(nil))
	file.Seek(0, 0)

	// Chunk the file
	chunker := dedup.NewChunker(file, dedup.DefaultChunkerConfig())
	var chunks []dedup.ChunkRef
	var totalSize, storedSize int64
	var chunkCount, newChunks int

	startTime := time.Now()

	for {
		chunk, err := chunker.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("chunking failed: %w", err)
		}

		chunkCount++
		totalSize += int64(chunk.Length)

		// Store chunk (deduplication happens here)
		isNew, err := store.Put(chunk)
		if err != nil {
			return fmt.Errorf("failed to store chunk: %w", err)
		}

		if isNew {
			newChunks++
			storedSize += int64(chunk.Length)
			// Record in index
			index.AddChunk(chunk.Hash, chunk.Length, chunk.Length)
		}

		chunks = append(chunks, dedup.ChunkRef{
			Hash:   chunk.Hash,
			Offset: chunk.Offset,
			Length: chunk.Length,
		})

		// Progress
		if chunkCount%1000 == 0 {
			fmt.Printf("\r  Processed %d chunks, %d new...", chunkCount, newChunks)
		}
	}

	duration := time.Since(startTime)

	// Calculate dedup ratio
	dedupRatio := 0.0
	if totalSize > 0 {
		dedupRatio = 1.0 - float64(storedSize)/float64(totalSize)
	}

	// Create manifest
	manifest := &dedup.Manifest{
		ID:           manifestID,
		Name:         dedupName,
		CreatedAt:    now,
		DatabaseType: dedupDBType,
		DatabaseName: dedupDBName,
		DatabaseHost: dedupDBHost,
		Chunks:       chunks,
		OriginalSize: totalSize,
		StoredSize:   storedSize,
		ChunkCount:   chunkCount,
		NewChunks:    newChunks,
		DedupRatio:   dedupRatio,
		Encrypted:    dedupEncrypt,
		Compressed:   dedupCompress,
		SHA256:       fileHash,
	}

	if err := manifestStore.Save(manifest); err != nil {
		return fmt.Errorf("failed to save manifest: %w", err)
	}

	if err := index.AddManifest(manifest); err != nil {
		log.Warn("Failed to index manifest", "error", err)
	}

	fmt.Printf("\r                                        \r")
	fmt.Printf("\nBackup complete!\n")
	fmt.Printf("  Manifest:    %s\n", manifestID)
	fmt.Printf("  Chunks:      %d total, %d new\n", chunkCount, newChunks)
	fmt.Printf("  Original:    %s\n", formatBytes(totalSize))
	fmt.Printf("  Stored:      %s (new data)\n", formatBytes(storedSize))
	fmt.Printf("  Dedup ratio: %.1f%%\n", dedupRatio*100)
	fmt.Printf("  Duration:    %s\n", duration.Round(time.Millisecond))
	fmt.Printf("  Throughput:  %s/s\n", formatBytes(int64(float64(totalSize)/duration.Seconds())))

	return nil
}

func runDedupRestore(cmd *cobra.Command, args []string) error {
	manifestID := args[0]
	outputPath := args[1]

	basePath := getDedupDir()
	encKey := ""
	if dedupEncrypt {
		encKey = getEncryptionKey()
	}

	store, err := dedup.NewChunkStore(dedup.StoreConfig{
		BasePath:      basePath,
		Compress:      dedupCompress,
		EncryptionKey: encKey,
	})
	if err != nil {
		return fmt.Errorf("failed to open chunk store: %w", err)
	}

	manifestStore, err := dedup.NewManifestStore(basePath)
	if err != nil {
		return fmt.Errorf("failed to open manifest store: %w", err)
	}

	manifest, err := manifestStore.Load(manifestID)
	if err != nil {
		return fmt.Errorf("failed to load manifest: %w", err)
	}

	fmt.Printf("Restoring backup: %s\n", manifestID)
	fmt.Printf("  Created:  %s\n", manifest.CreatedAt.Format(time.RFC3339))
	fmt.Printf("  Size:     %s\n", formatBytes(manifest.OriginalSize))
	fmt.Printf("  Chunks:   %d\n", manifest.ChunkCount)

	// Create output file
	outFile, err := os.Create(outputPath)
	if err != nil {
		return fmt.Errorf("failed to create output file: %w", err)
	}
	defer outFile.Close()

	h := sha256.New()
	writer := io.MultiWriter(outFile, h)

	startTime := time.Now()

	for i, ref := range manifest.Chunks {
		chunk, err := store.Get(ref.Hash)
		if err != nil {
			return fmt.Errorf("failed to read chunk %d (%s): %w", i, ref.Hash[:8], err)
		}

		if _, err := writer.Write(chunk.Data); err != nil {
			return fmt.Errorf("failed to write chunk %d: %w", i, err)
		}

		if (i+1)%1000 == 0 {
			fmt.Printf("\r  Restored %d/%d chunks...", i+1, manifest.ChunkCount)
		}
	}

	duration := time.Since(startTime)
	restoredHash := hex.EncodeToString(h.Sum(nil))

	fmt.Printf("\r                                        \r")
	fmt.Printf("\nRestore complete!\n")
	fmt.Printf("  Output:      %s\n", outputPath)
	fmt.Printf("  Duration:    %s\n", duration.Round(time.Millisecond))

	// Verify hash
	if manifest.SHA256 != "" {
		if restoredHash == manifest.SHA256 {
			fmt.Printf("  Verification: [OK] SHA-256 matches\n")
		} else {
			fmt.Printf("  Verification: [FAIL] SHA-256 MISMATCH!\n")
			fmt.Printf("    Expected: %s\n", manifest.SHA256)
			fmt.Printf("    Got:      %s\n", restoredHash)
			return fmt.Errorf("integrity verification failed")
		}
	}

	return nil
}

func runDedupList(cmd *cobra.Command, args []string) error {
	basePath := getDedupDir()

	manifestStore, err := dedup.NewManifestStore(basePath)
	if err != nil {
		return fmt.Errorf("failed to open manifest store: %w", err)
	}

	manifests, err := manifestStore.ListAll()
	if err != nil {
		return fmt.Errorf("failed to list manifests: %w", err)
	}

	if len(manifests) == 0 {
		fmt.Println("No deduplicated backups found.")
		fmt.Printf("Store: %s\n", basePath)
		return nil
	}

	fmt.Printf("Deduplicated Backups (%s)\n\n", basePath)
	fmt.Printf("%-30s %-12s %-10s %-10s %s\n", "ID", "SIZE", "DEDUP", "CHUNKS", "CREATED")
	fmt.Println(strings.Repeat("-", 80))

	for _, m := range manifests {
		fmt.Printf("%-30s %-12s %-10.1f%% %-10d %s\n",
			truncateStr(m.ID, 30),
			formatBytes(m.OriginalSize),
			m.DedupRatio*100,
			m.ChunkCount,
			m.CreatedAt.Format("2006-01-02 15:04"),
		)
	}

	return nil
}

func runDedupStats(cmd *cobra.Command, args []string) error {
	basePath := getDedupDir()

	index, err := dedup.NewChunkIndex(basePath)
	if err != nil {
		return fmt.Errorf("failed to open chunk index: %w", err)
	}
	defer index.Close()

	stats, err := index.Stats()
	if err != nil {
		return fmt.Errorf("failed to get stats: %w", err)
	}

	store, err := dedup.NewChunkStore(dedup.StoreConfig{BasePath: basePath})
	if err != nil {
		return fmt.Errorf("failed to open chunk store: %w", err)
	}

	storeStats, err := store.Stats()
	if err != nil {
		log.Warn("Failed to get store stats", "error", err)
	}

	fmt.Printf("Deduplication Statistics\n")
	fmt.Printf("========================\n\n")
	fmt.Printf("Store:           %s\n", basePath)
	fmt.Printf("Manifests:       %d\n", stats.TotalManifests)
	fmt.Printf("Unique chunks:   %d\n", stats.TotalChunks)
	fmt.Printf("Total raw size:  %s\n", formatBytes(stats.TotalSizeRaw))
	fmt.Printf("Stored size:     %s\n", formatBytes(stats.TotalSizeStored))
	fmt.Printf("Dedup ratio:     %.1f%%\n", stats.DedupRatio*100)
	fmt.Printf("Space saved:     %s\n", formatBytes(stats.TotalSizeRaw-stats.TotalSizeStored))

	if storeStats != nil {
		fmt.Printf("Disk usage:      %s\n", formatBytes(storeStats.TotalSize))
		fmt.Printf("Directories:     %d\n", storeStats.Directories)
	}

	return nil
}

func runDedupGC(cmd *cobra.Command, args []string) error {
	basePath := getDedupDir()

	index, err := dedup.NewChunkIndex(basePath)
	if err != nil {
		return fmt.Errorf("failed to open chunk index: %w", err)
	}
	defer index.Close()

	store, err := dedup.NewChunkStore(dedup.StoreConfig{
		BasePath: basePath,
		Compress: dedupCompress,
	})
	if err != nil {
		return fmt.Errorf("failed to open chunk store: %w", err)
	}

	// Find orphaned chunks
	orphans, err := index.ListOrphanedChunks()
	if err != nil {
		return fmt.Errorf("failed to find orphaned chunks: %w", err)
	}

	if len(orphans) == 0 {
		fmt.Println("No orphaned chunks to clean up.")
		return nil
	}

	fmt.Printf("Found %d orphaned chunks\n", len(orphans))

	var freed int64
	for _, hash := range orphans {
		if meta, _ := index.GetChunk(hash); meta != nil {
			freed += meta.SizeStored
		}
		if err := store.Delete(hash); err != nil {
			log.Warn("Failed to delete chunk", "hash", hash[:8], "error", err)
			continue
		}
		if err := index.RemoveChunk(hash); err != nil {
			log.Warn("Failed to remove chunk from index", "hash", hash[:8], "error", err)
		}
	}

	fmt.Printf("Deleted %d chunks, freed %s\n", len(orphans), formatBytes(freed))

	// Vacuum the index
	if err := index.Vacuum(); err != nil {
		log.Warn("Failed to vacuum index", "error", err)
	}

	return nil
}

func runDedupDelete(cmd *cobra.Command, args []string) error {
	manifestID := args[0]
	basePath := getDedupDir()

	manifestStore, err := dedup.NewManifestStore(basePath)
	if err != nil {
		return fmt.Errorf("failed to open manifest store: %w", err)
	}

	index, err := dedup.NewChunkIndex(basePath)
	if err != nil {
		return fmt.Errorf("failed to open chunk index: %w", err)
	}
	defer index.Close()

	// Load manifest to decrement chunk refs
	manifest, err := manifestStore.Load(manifestID)
	if err != nil {
		return fmt.Errorf("failed to load manifest: %w", err)
	}

	// Decrement reference counts
	for _, ref := range manifest.Chunks {
		index.DecrementRef(ref.Hash)
	}

	// Delete manifest
	if err := manifestStore.Delete(manifestID); err != nil {
		return fmt.Errorf("failed to delete manifest: %w", err)
	}

	if err := index.RemoveManifest(manifestID); err != nil {
		log.Warn("Failed to remove manifest from index", "error", err)
	}

	fmt.Printf("Deleted backup: %s\n", manifestID)
	fmt.Println("Run 'dbbackup dedup gc' to reclaim space from unreferenced chunks.")

	return nil
}

// Helper functions
func formatBytes(b int64) string {
	const unit = 1024
	if b < unit {
		return fmt.Sprintf("%d B", b)
	}
	div, exp := int64(unit), 0
	for n := b / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %cB", float64(b)/float64(div), "KMGTPE"[exp])
}

func truncateStr(s string, max int) string {
	if len(s) <= max {
		return s
	}
	return s[:max-3] + "..."
}
