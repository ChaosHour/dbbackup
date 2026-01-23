package cmd

import (
	"compress/gzip"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"os/exec"
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
    chunks.db     # SQLite index

NFS/CIFS NOTICE:
  SQLite may have locking issues on network storage.
  Use --index-db to put the SQLite index on local storage while keeping
  chunks on network storage:
  
    dbbackup dedup backup mydb.sql \
      --dedup-dir /mnt/nfs/backups/dedup \
      --index-db /var/lib/dbbackup/dedup-index.db

  This avoids "database is locked" errors while still storing chunks remotely.

COMPRESSED INPUT NOTICE:
  Pre-compressed files (.gz) have poor deduplication ratios (<10%).
  Use --decompress-input to decompress before chunking for better results:
  
    dbbackup dedup backup mydb.sql.gz --decompress-input`,
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

var dedupVerifyCmd = &cobra.Command{
	Use:   "verify [manifest-id]",
	Short: "Verify chunk integrity against manifests",
	Long: `Verify that all chunks referenced by manifests exist and have correct hashes.

Without arguments, verifies all backups. With a manifest ID, verifies only that backup.

Examples:
  dbbackup dedup verify                    # Verify all backups
  dbbackup dedup verify 2026-01-07_mydb    # Verify specific backup`,
	RunE: runDedupVerify,
}

var dedupPruneCmd = &cobra.Command{
	Use:   "prune",
	Short: "Apply retention policy to manifests",
	Long: `Delete old manifests based on retention policy (like borg prune).

Keeps a specified number of recent backups per database and deletes the rest.

Examples:
  dbbackup dedup prune --keep-last 7                    # Keep 7 most recent
  dbbackup dedup prune --keep-daily 7 --keep-weekly 4   # Keep 7 daily + 4 weekly`,
	RunE: runDedupPrune,
}

var dedupBackupDBCmd = &cobra.Command{
	Use:   "backup-db",
	Short: "Direct database dump with deduplication",
	Long: `Dump a database directly into deduplicated chunks without temp files.

Streams the database dump through the chunker for efficient deduplication.

Examples:
  dbbackup dedup backup-db --db-type postgres --db-name mydb
  dbbackup dedup backup-db -d mariadb --database production_db --host db.local`,
	RunE: runDedupBackupDB,
}

// Prune flags
var (
	pruneKeepLast   int
	pruneKeepDaily  int
	pruneKeepWeekly int
	pruneDryRun     bool
)

// backup-db flags
var (
	backupDBDatabase string
	backupDBUser     string
	backupDBPassword string
)

// metrics flags
var (
	dedupMetricsOutput   string
	dedupMetricsInstance string
)

var dedupMetricsCmd = &cobra.Command{
	Use:   "metrics",
	Short: "Export dedup statistics as Prometheus metrics",
	Long: `Export deduplication statistics in Prometheus format.

Can write to a textfile for node_exporter's textfile collector,
or print to stdout for custom integrations.

Examples:
  dbbackup dedup metrics                                    # Print to stdout
  dbbackup dedup metrics --output /var/lib/node_exporter/textfile_collector/dedup.prom
  dbbackup dedup metrics --instance prod-db-1`,
	RunE: runDedupMetrics,
}

// Flags
var (
	dedupDir        string
	dedupIndexDB    string // Separate path for SQLite index (for NFS/CIFS support)
	dedupCompress   bool
	dedupEncrypt    bool
	dedupKey        string
	dedupName       string
	dedupDBType     string
	dedupDBName     string
	dedupDBHost     string
	dedupDecompress bool // Auto-decompress gzip input
)

func init() {
	rootCmd.AddCommand(dedupCmd)
	dedupCmd.AddCommand(dedupBackupCmd)
	dedupCmd.AddCommand(dedupRestoreCmd)
	dedupCmd.AddCommand(dedupListCmd)
	dedupCmd.AddCommand(dedupStatsCmd)
	dedupCmd.AddCommand(dedupGCCmd)
	dedupCmd.AddCommand(dedupDeleteCmd)
	dedupCmd.AddCommand(dedupVerifyCmd)
	dedupCmd.AddCommand(dedupPruneCmd)
	dedupCmd.AddCommand(dedupBackupDBCmd)
	dedupCmd.AddCommand(dedupMetricsCmd)

	// Global dedup flags
	dedupCmd.PersistentFlags().StringVar(&dedupDir, "dedup-dir", "", "Dedup storage directory (default: $BACKUP_DIR/dedup)")
	dedupCmd.PersistentFlags().StringVar(&dedupIndexDB, "index-db", "", "SQLite index path (local recommended for NFS/CIFS chunk dirs)")
	dedupCmd.PersistentFlags().BoolVar(&dedupCompress, "compress", true, "Compress chunks with gzip")
	dedupCmd.PersistentFlags().BoolVar(&dedupEncrypt, "encrypt", false, "Encrypt chunks with AES-256-GCM")
	dedupCmd.PersistentFlags().StringVar(&dedupKey, "key", "", "Encryption key (hex) or use DBBACKUP_DEDUP_KEY env")

	// Backup-specific flags
	dedupBackupCmd.Flags().StringVar(&dedupName, "name", "", "Optional backup name")
	dedupBackupCmd.Flags().StringVar(&dedupDBType, "db-type", "", "Database type (postgres/mysql)")
	dedupBackupCmd.Flags().StringVar(&dedupDBName, "db-name", "", "Database name")
	dedupBackupCmd.Flags().StringVar(&dedupDBHost, "db-host", "", "Database host")
	dedupBackupCmd.Flags().BoolVar(&dedupDecompress, "decompress-input", false, "Auto-decompress gzip input before chunking (improves dedup ratio)")

	// Prune flags
	dedupPruneCmd.Flags().IntVar(&pruneKeepLast, "keep-last", 0, "Keep the last N backups")
	dedupPruneCmd.Flags().IntVar(&pruneKeepDaily, "keep-daily", 0, "Keep N daily backups")
	dedupPruneCmd.Flags().IntVar(&pruneKeepWeekly, "keep-weekly", 0, "Keep N weekly backups")
	dedupPruneCmd.Flags().BoolVar(&pruneDryRun, "dry-run", false, "Show what would be deleted without actually deleting")

	// backup-db flags
	dedupBackupDBCmd.Flags().StringVarP(&dedupDBType, "db-type", "d", "", "Database type (postgres/mariadb/mysql)")
	dedupBackupDBCmd.Flags().StringVar(&backupDBDatabase, "database", "", "Database name to backup")
	dedupBackupDBCmd.Flags().StringVar(&dedupDBHost, "host", "localhost", "Database host")
	dedupBackupDBCmd.Flags().StringVarP(&backupDBUser, "user", "u", "", "Database user")
	dedupBackupDBCmd.Flags().StringVarP(&backupDBPassword, "password", "p", "", "Database password (or use env)")
	dedupBackupDBCmd.MarkFlagRequired("db-type")
	dedupBackupDBCmd.MarkFlagRequired("database")

	// Metrics flags
	dedupMetricsCmd.Flags().StringVarP(&dedupMetricsOutput, "output", "o", "", "Output file path (default: stdout)")
	dedupMetricsCmd.Flags().StringVar(&dedupMetricsInstance, "instance", "", "Instance label for metrics (default: hostname)")
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

func getIndexDBPath() string {
	if dedupIndexDB != "" {
		return dedupIndexDB
	}
	// Default: same directory as chunks (may have issues on NFS/CIFS)
	return filepath.Join(getDedupDir(), "chunks.db")
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

	// Check for compressed input and warn/handle
	var reader io.Reader = file
	isGzipped := strings.HasSuffix(strings.ToLower(inputPath), ".gz")
	if isGzipped && !dedupDecompress {
		fmt.Printf("Warning: Input appears to be gzip compressed (.gz)\n")
		fmt.Printf("  Compressed data typically has poor dedup ratios (<10%%).\n")
		fmt.Printf("  Consider using --decompress-input for better deduplication.\n\n")
	}

	if isGzipped && dedupDecompress {
		fmt.Printf("Auto-decompressing gzip input for better dedup ratio...\n")
		gzReader, err := gzip.NewReader(file)
		if err != nil {
			return fmt.Errorf("failed to decompress gzip input: %w", err)
		}
		defer gzReader.Close()
		reader = gzReader
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

	index, err := dedup.NewChunkIndexAt(getIndexDBPath())
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
		// Remove .gz extension if decompressing
		if isGzipped && dedupDecompress {
			base = strings.TrimSuffix(base, ext)
			ext = filepath.Ext(base)
		}
		manifestID += "_" + strings.TrimSuffix(base, ext)
	}

	fmt.Printf("Creating deduplicated backup: %s\n", manifestID)
	fmt.Printf("Input: %s (%s)\n", inputPath, formatBytes(info.Size()))
	if isGzipped && dedupDecompress {
		fmt.Printf("Mode: Decompressing before chunking\n")
	}
	fmt.Printf("Store: %s\n", basePath)
	if dedupIndexDB != "" {
		fmt.Printf("Index: %s\n", getIndexDBPath())
	}

	// For decompressed input, we can't seek - use TeeReader to hash while chunking
	h := sha256.New()
	var chunkReader io.Reader

	if isGzipped && dedupDecompress {
		// Can't seek on gzip stream - hash will be computed inline
		chunkReader = io.TeeReader(reader, h)
	} else {
		// Regular file - hash first, then reset and chunk
		file.Seek(0, 0)
		io.Copy(h, file)
		file.Seek(0, 0)
		chunkReader = file
		h = sha256.New() // Reset for inline hashing
		chunkReader = io.TeeReader(file, h)
	}

	// Chunk the file
	chunker := dedup.NewChunker(chunkReader, dedup.DefaultChunkerConfig())
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

	// Get final hash (computed inline via TeeReader)
	fileHash := hex.EncodeToString(h.Sum(nil))

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
		Decompressed: isGzipped && dedupDecompress, // Track if we decompressed
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

	index, err := dedup.NewChunkIndexAt(getIndexDBPath())
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
	fmt.Printf("\n")
	fmt.Printf("Backup Statistics (accurate dedup calculation):\n")
	fmt.Printf("  Total backed up:  %s (across all backups)\n", formatBytes(stats.TotalBackupSize))
	fmt.Printf("  New data stored:  %s\n", formatBytes(stats.TotalNewData))
	fmt.Printf("  Space saved:      %s\n", formatBytes(stats.SpaceSaved))
	fmt.Printf("  Dedup ratio:      %.1f%%\n", stats.DedupRatio*100)

	if storeStats != nil {
		fmt.Printf("Disk usage:      %s\n", formatBytes(storeStats.TotalSize))
		fmt.Printf("Directories:     %d\n", storeStats.Directories)
	}

	return nil
}

func runDedupGC(cmd *cobra.Command, args []string) error {
	basePath := getDedupDir()

	index, err := dedup.NewChunkIndexAt(getIndexDBPath())
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

	index, err := dedup.NewChunkIndexAt(getIndexDBPath())
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

func runDedupVerify(cmd *cobra.Command, args []string) error {
	basePath := getDedupDir()

	store, err := dedup.NewChunkStore(dedup.StoreConfig{
		BasePath: basePath,
		Compress: dedupCompress,
	})
	if err != nil {
		return fmt.Errorf("failed to open chunk store: %w", err)
	}

	manifestStore, err := dedup.NewManifestStore(basePath)
	if err != nil {
		return fmt.Errorf("failed to open manifest store: %w", err)
	}

	index, err := dedup.NewChunkIndexAt(getIndexDBPath())
	if err != nil {
		return fmt.Errorf("failed to open chunk index: %w", err)
	}
	defer index.Close()

	var manifests []*dedup.Manifest

	if len(args) > 0 {
		// Verify specific manifest
		m, err := manifestStore.Load(args[0])
		if err != nil {
			return fmt.Errorf("failed to load manifest: %w", err)
		}
		manifests = []*dedup.Manifest{m}
	} else {
		// Verify all manifests
		manifests, err = manifestStore.ListAll()
		if err != nil {
			return fmt.Errorf("failed to list manifests: %w", err)
		}
	}

	if len(manifests) == 0 {
		fmt.Println("No manifests to verify.")
		return nil
	}

	fmt.Printf("Verifying %d backup(s)...\n\n", len(manifests))

	var totalChunks, missingChunks, corruptChunks int
	var allOK = true

	for _, m := range manifests {
		fmt.Printf("Verifying: %s (%d chunks)\n", m.ID, m.ChunkCount)

		var missing, corrupt int
		seenHashes := make(map[string]bool)

		for i, ref := range m.Chunks {
			if seenHashes[ref.Hash] {
				continue // Already verified this chunk
			}
			seenHashes[ref.Hash] = true
			totalChunks++

			// Check if chunk exists
			if !store.Has(ref.Hash) {
				missing++
				missingChunks++
				if missing <= 5 {
					fmt.Printf("  [MISSING] chunk %d: %s\n", i, ref.Hash[:16])
				}
				continue
			}

			// Verify chunk hash by reading it
			chunk, err := store.Get(ref.Hash)
			if err != nil {
				corrupt++
				corruptChunks++
				if corrupt <= 5 {
					fmt.Printf("  [CORRUPT] chunk %d: %s - %v\n", i, ref.Hash[:16], err)
				}
				continue
			}

			// Verify size
			if chunk.Length != ref.Length {
				corrupt++
				corruptChunks++
				if corrupt <= 5 {
					fmt.Printf("  [SIZE MISMATCH] chunk %d: expected %d, got %d\n", i, ref.Length, chunk.Length)
				}
			}
		}

		if missing > 0 || corrupt > 0 {
			allOK = false
			fmt.Printf("  Result: FAILED (%d missing, %d corrupt)\n", missing, corrupt)
			if missing > 5 || corrupt > 5 {
				fmt.Printf("  ... and %d more errors\n", (missing+corrupt)-10)
			}
		} else {
			fmt.Printf("  Result: OK (%d unique chunks verified)\n", len(seenHashes))
			// Update verified timestamp
			m.VerifiedAt = time.Now()
			manifestStore.Save(m)
			index.UpdateManifestVerified(m.ID, m.VerifiedAt)
		}
		fmt.Println()
	}

	fmt.Println("========================================")
	if allOK {
		fmt.Printf("All %d backup(s) verified successfully!\n", len(manifests))
		fmt.Printf("Total unique chunks checked: %d\n", totalChunks)
	} else {
		fmt.Printf("Verification FAILED!\n")
		fmt.Printf("Missing chunks: %d\n", missingChunks)
		fmt.Printf("Corrupt chunks: %d\n", corruptChunks)
		return fmt.Errorf("verification failed: %d missing, %d corrupt chunks", missingChunks, corruptChunks)
	}

	return nil
}

func runDedupPrune(cmd *cobra.Command, args []string) error {
	if pruneKeepLast == 0 && pruneKeepDaily == 0 && pruneKeepWeekly == 0 {
		return fmt.Errorf("at least one of --keep-last, --keep-daily, or --keep-weekly must be specified")
	}

	basePath := getDedupDir()

	manifestStore, err := dedup.NewManifestStore(basePath)
	if err != nil {
		return fmt.Errorf("failed to open manifest store: %w", err)
	}

	index, err := dedup.NewChunkIndexAt(getIndexDBPath())
	if err != nil {
		return fmt.Errorf("failed to open chunk index: %w", err)
	}
	defer index.Close()

	manifests, err := manifestStore.ListAll()
	if err != nil {
		return fmt.Errorf("failed to list manifests: %w", err)
	}

	if len(manifests) == 0 {
		fmt.Println("No backups to prune.")
		return nil
	}

	// Group by database name
	byDatabase := make(map[string][]*dedup.Manifest)
	for _, m := range manifests {
		key := m.DatabaseName
		if key == "" {
			key = "_default"
		}
		byDatabase[key] = append(byDatabase[key], m)
	}

	var toDelete []*dedup.Manifest

	for dbName, dbManifests := range byDatabase {
		// Already sorted by time (newest first from ListAll)
		kept := make(map[string]bool)
		var keepReasons = make(map[string]string)

		// Keep last N
		if pruneKeepLast > 0 {
			for i := 0; i < pruneKeepLast && i < len(dbManifests); i++ {
				kept[dbManifests[i].ID] = true
				keepReasons[dbManifests[i].ID] = "keep-last"
			}
		}

		// Keep daily (one per day)
		if pruneKeepDaily > 0 {
			seenDays := make(map[string]bool)
			count := 0
			for _, m := range dbManifests {
				day := m.CreatedAt.Format("2006-01-02")
				if !seenDays[day] {
					seenDays[day] = true
					if count < pruneKeepDaily {
						kept[m.ID] = true
						if keepReasons[m.ID] == "" {
							keepReasons[m.ID] = "keep-daily"
						}
						count++
					}
				}
			}
		}

		// Keep weekly (one per week)
		if pruneKeepWeekly > 0 {
			seenWeeks := make(map[string]bool)
			count := 0
			for _, m := range dbManifests {
				year, week := m.CreatedAt.ISOWeek()
				weekKey := fmt.Sprintf("%d-W%02d", year, week)
				if !seenWeeks[weekKey] {
					seenWeeks[weekKey] = true
					if count < pruneKeepWeekly {
						kept[m.ID] = true
						if keepReasons[m.ID] == "" {
							keepReasons[m.ID] = "keep-weekly"
						}
						count++
					}
				}
			}
		}

		if dbName != "_default" {
			fmt.Printf("\nDatabase: %s\n", dbName)
		} else {
			fmt.Printf("\nUnnamed backups:\n")
		}

		for _, m := range dbManifests {
			if kept[m.ID] {
				fmt.Printf("  [KEEP]   %s (%s) - %s\n", m.ID, m.CreatedAt.Format("2006-01-02"), keepReasons[m.ID])
			} else {
				fmt.Printf("  [DELETE] %s (%s)\n", m.ID, m.CreatedAt.Format("2006-01-02"))
				toDelete = append(toDelete, m)
			}
		}
	}

	if len(toDelete) == 0 {
		fmt.Printf("\nNo backups to prune (all match retention policy).\n")
		return nil
	}

	fmt.Printf("\n%d backup(s) will be deleted.\n", len(toDelete))

	if pruneDryRun {
		fmt.Println("\n[DRY RUN] No changes made. Remove --dry-run to actually delete.")
		return nil
	}

	// Actually delete
	for _, m := range toDelete {
		// Decrement chunk references
		for _, ref := range m.Chunks {
			index.DecrementRef(ref.Hash)
		}

		if err := manifestStore.Delete(m.ID); err != nil {
			log.Warn("Failed to delete manifest", "id", m.ID, "error", err)
		}
		index.RemoveManifest(m.ID)
	}

	fmt.Printf("\nDeleted %d backup(s).\n", len(toDelete))
	fmt.Println("Run 'dbbackup dedup gc' to reclaim space from unreferenced chunks.")

	return nil
}

func runDedupBackupDB(cmd *cobra.Command, args []string) error {
	dbType := strings.ToLower(dedupDBType)
	dbName := backupDBDatabase

	// Validate db type
	var dumpCmd string
	var dumpArgs []string

	switch dbType {
	case "postgres", "postgresql", "pg":
		dbType = "postgres"
		dumpCmd = "pg_dump"
		dumpArgs = []string{"-Fc"} // Custom format for better compression
		if dedupDBHost != "" && dedupDBHost != "localhost" {
			dumpArgs = append(dumpArgs, "-h", dedupDBHost)
		}
		if backupDBUser != "" {
			dumpArgs = append(dumpArgs, "-U", backupDBUser)
		}
		dumpArgs = append(dumpArgs, dbName)

	case "mysql":
		dumpCmd = "mysqldump"
		dumpArgs = []string{
			"--single-transaction",
			"--routines",
			"--triggers",
			"--events",
		}
		if dedupDBHost != "" {
			dumpArgs = append(dumpArgs, "-h", dedupDBHost)
		}
		if backupDBUser != "" {
			dumpArgs = append(dumpArgs, "-u", backupDBUser)
		}
		if backupDBPassword != "" {
			dumpArgs = append(dumpArgs, "-p"+backupDBPassword)
		}
		dumpArgs = append(dumpArgs, dbName)

	case "mariadb":
		dumpCmd = "mariadb-dump"
		// Fall back to mysqldump if mariadb-dump not available
		if _, err := exec.LookPath(dumpCmd); err != nil {
			dumpCmd = "mysqldump"
		}
		dumpArgs = []string{
			"--single-transaction",
			"--routines",
			"--triggers",
			"--events",
		}
		if dedupDBHost != "" {
			dumpArgs = append(dumpArgs, "-h", dedupDBHost)
		}
		if backupDBUser != "" {
			dumpArgs = append(dumpArgs, "-u", backupDBUser)
		}
		if backupDBPassword != "" {
			dumpArgs = append(dumpArgs, "-p"+backupDBPassword)
		}
		dumpArgs = append(dumpArgs, dbName)

	default:
		return fmt.Errorf("unsupported database type: %s (use postgres, mysql, or mariadb)", dbType)
	}

	// Verify dump command exists
	if _, err := exec.LookPath(dumpCmd); err != nil {
		return fmt.Errorf("%s not found in PATH: %w", dumpCmd, err)
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

	index, err := dedup.NewChunkIndexAt(getIndexDBPath())
	if err != nil {
		return fmt.Errorf("failed to open chunk index: %w", err)
	}
	defer index.Close()

	// Generate manifest ID
	now := time.Now()
	manifestID := now.Format("2006-01-02_150405") + "_" + dbName

	fmt.Printf("Creating deduplicated database backup: %s\n", manifestID)
	fmt.Printf("Database: %s (%s)\n", dbName, dbType)
	fmt.Printf("Command: %s %s\n", dumpCmd, strings.Join(dumpArgs, " "))
	fmt.Printf("Store: %s\n", basePath)

	// Start the dump command
	dumpExec := exec.Command(dumpCmd, dumpArgs...)

	// Set password via environment for postgres
	if dbType == "postgres" && backupDBPassword != "" {
		dumpExec.Env = append(os.Environ(), "PGPASSWORD="+backupDBPassword)
	}

	stdout, err := dumpExec.StdoutPipe()
	if err != nil {
		return fmt.Errorf("failed to get stdout pipe: %w", err)
	}

	stderr, err := dumpExec.StderrPipe()
	if err != nil {
		return fmt.Errorf("failed to get stderr pipe: %w", err)
	}

	if err := dumpExec.Start(); err != nil {
		return fmt.Errorf("failed to start %s: %w", dumpCmd, err)
	}

	// Hash while chunking using TeeReader
	h := sha256.New()
	reader := io.TeeReader(stdout, h)

	// Chunk the stream directly
	chunker := dedup.NewChunker(reader, dedup.DefaultChunkerConfig())
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
			index.AddChunk(chunk.Hash, chunk.Length, chunk.Length)
		}

		chunks = append(chunks, dedup.ChunkRef{
			Hash:   chunk.Hash,
			Offset: chunk.Offset,
			Length: chunk.Length,
		})

		if chunkCount%1000 == 0 {
			fmt.Printf("\r  Processed %d chunks, %d new, %s...", chunkCount, newChunks, formatBytes(totalSize))
		}
	}

	// Read any stderr
	stderrBytes, _ := io.ReadAll(stderr)

	// Wait for command to complete
	if err := dumpExec.Wait(); err != nil {
		return fmt.Errorf("%s failed: %w\nstderr: %s", dumpCmd, err, string(stderrBytes))
	}

	duration := time.Since(startTime)
	fileHash := hex.EncodeToString(h.Sum(nil))

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
		DatabaseType: dbType,
		DatabaseName: dbName,
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

	fmt.Printf("\r                                                    \r")
	fmt.Printf("\nBackup complete!\n")
	fmt.Printf("  Manifest:    %s\n", manifestID)
	fmt.Printf("  Chunks:      %d total, %d new\n", chunkCount, newChunks)
	fmt.Printf("  Dump size:   %s\n", formatBytes(totalSize))
	fmt.Printf("  Stored:      %s (new data)\n", formatBytes(storedSize))
	fmt.Printf("  Dedup ratio: %.1f%%\n", dedupRatio*100)
	fmt.Printf("  Duration:    %s\n", duration.Round(time.Millisecond))
	fmt.Printf("  Throughput:  %s/s\n", formatBytes(int64(float64(totalSize)/duration.Seconds())))

	return nil
}

func runDedupMetrics(cmd *cobra.Command, args []string) error {
	basePath := getDedupDir()
	indexPath := getIndexDBPath()

	instance := dedupMetricsInstance
	if instance == "" {
		hostname, _ := os.Hostname()
		instance = hostname
	}

	metrics, err := dedup.CollectMetrics(basePath, indexPath)
	if err != nil {
		return fmt.Errorf("failed to collect metrics: %w", err)
	}

	output := dedup.FormatPrometheusMetrics(metrics, instance)

	if dedupMetricsOutput != "" {
		if err := dedup.WritePrometheusTextfile(dedupMetricsOutput, instance, basePath, indexPath); err != nil {
			return fmt.Errorf("failed to write metrics: %w", err)
		}
		fmt.Printf("Wrote metrics to %s\n", dedupMetricsOutput)
	} else {
		fmt.Print(output)
	}

	return nil
}
