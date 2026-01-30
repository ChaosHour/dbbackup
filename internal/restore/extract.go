package restore

import (
	"archive/tar"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"dbbackup/internal/fs"
	"dbbackup/internal/logger"
	"dbbackup/internal/progress"

	"github.com/klauspost/pgzip"
)

// DatabaseInfo represents metadata about a database in a cluster backup
type DatabaseInfo struct {
	Name     string
	Filename string
	Size     int64
}

// ListDatabasesFromExtractedDir lists databases from an already-extracted cluster directory
// This is much faster than scanning the tar.gz archive
func ListDatabasesFromExtractedDir(ctx context.Context, extractedDir string, log logger.Logger) ([]DatabaseInfo, error) {
	dumpsDir := filepath.Join(extractedDir, "dumps")
	entries, err := os.ReadDir(dumpsDir)
	if err != nil {
		return nil, fmt.Errorf("cannot read dumps directory: %w", err)
	}

	databases := make([]DatabaseInfo, 0)
	for _, entry := range entries {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		if entry.IsDir() {
			continue
		}

		filename := entry.Name()
		// Extract database name from filename
		dbName := filename
		dbName = strings.TrimSuffix(dbName, ".dump.gz")
		dbName = strings.TrimSuffix(dbName, ".dump")
		dbName = strings.TrimSuffix(dbName, ".sql.gz")
		dbName = strings.TrimSuffix(dbName, ".sql")

		info, err := entry.Info()
		if err != nil {
			log.Warn("Cannot stat dump file", "file", filename, "error", err)
			continue
		}

		databases = append(databases, DatabaseInfo{
			Name:     dbName,
			Filename: filename,
			Size:     info.Size(),
		})
	}

	// Sort by name for consistent output
	sort.Slice(databases, func(i, j int) bool {
		return databases[i].Name < databases[j].Name
	})

	if len(databases) == 0 {
		return nil, fmt.Errorf("no databases found in extracted directory")
	}

	log.Info("Listed databases from extracted directory", "count", len(databases))
	return databases, nil
}

// ListDatabasesInCluster lists all databases in a cluster backup archive
func ListDatabasesInCluster(ctx context.Context, archivePath string, log logger.Logger) ([]DatabaseInfo, error) {
	file, err := os.Open(archivePath)
	if err != nil {
		return nil, fmt.Errorf("cannot open archive: %w", err)
	}
	defer file.Close()

	gz, err := pgzip.NewReader(file)
	if err != nil {
		return nil, fmt.Errorf("not a valid gzip archive: %w", err)
	}
	defer gz.Close()

	tarReader := tar.NewReader(gz)
	databases := make([]DatabaseInfo, 0)

	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		header, err := tarReader.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("error reading tar archive: %w", err)
		}

		// Look for files in dumps/ directory
		if !header.FileInfo().IsDir() && strings.HasPrefix(header.Name, "dumps/") {
			filename := filepath.Base(header.Name)

			// Extract database name from filename (remove .dump, .dump.gz, .sql, .sql.gz)
			dbName := filename
			dbName = strings.TrimSuffix(dbName, ".dump.gz")
			dbName = strings.TrimSuffix(dbName, ".dump")
			dbName = strings.TrimSuffix(dbName, ".sql.gz")
			dbName = strings.TrimSuffix(dbName, ".sql")

			databases = append(databases, DatabaseInfo{
				Name:     dbName,
				Filename: filename,
				Size:     header.Size,
			})
		}
	}

	// Sort by name for consistent output
	sort.Slice(databases, func(i, j int) bool {
		return databases[i].Name < databases[j].Name
	})

	if len(databases) == 0 {
		return nil, fmt.Errorf("no databases found in cluster backup")
	}

	return databases, nil
}

// ExtractDatabaseFromCluster extracts a single database dump from cluster backup
func ExtractDatabaseFromCluster(ctx context.Context, archivePath, dbName, outputDir string, log logger.Logger, prog progress.Indicator) (string, error) {
	file, err := os.Open(archivePath)
	if err != nil {
		return "", fmt.Errorf("cannot open archive: %w", err)
	}
	defer file.Close()

	stat, err := file.Stat()
	if err != nil {
		return "", fmt.Errorf("cannot stat archive: %w", err)
	}
	archiveSize := stat.Size()

	gz, err := pgzip.NewReader(file)
	if err != nil {
		return "", fmt.Errorf("not a valid gzip archive: %w", err)
	}
	defer gz.Close()

	tarReader := tar.NewReader(gz)

	// Create output directory if needed
	if err := os.MkdirAll(outputDir, 0755); err != nil {
		return "", fmt.Errorf("cannot create output directory: %w", err)
	}

	targetPattern := fmt.Sprintf("dumps/%s.", dbName) // Match dbName.dump, dbName.sql, etc.
	var extractedPath string
	found := false

	if prog != nil {
		prog.Start(fmt.Sprintf("Extracting database: %s", dbName))
		defer prog.Stop()
	}

	var bytesRead int64
	ticker := make(chan struct{})
	stopTicker := make(chan struct{})
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-stopTicker:
				return
			case <-ticker:
				if prog != nil && archiveSize > 0 {
					percentage := float64(bytesRead) / float64(archiveSize) * 100
					prog.Update(fmt.Sprintf("Scanning: %.1f%%", percentage))
				}
			}
		}
	}()

	for {
		select {
		case <-ctx.Done():
			close(stopTicker)
			return "", ctx.Err()
		default:
		}

		header, err := tarReader.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			close(stopTicker)
			return "", fmt.Errorf("error reading tar archive: %w", err)
		}

		bytesRead += header.Size
		select {
		case ticker <- struct{}{}:
		default:
		}

		// Check if this is the database we're looking for
		if strings.HasPrefix(header.Name, targetPattern) && !header.FileInfo().IsDir() {
			filename := filepath.Base(header.Name)
			extractedPath = filepath.Join(outputDir, filename)

			// Extract the file
			outFile, err := os.Create(extractedPath)
			if err != nil {
				close(stopTicker)
				return "", fmt.Errorf("cannot create output file: %w", err)
			}

			if prog != nil {
				prog.Update(fmt.Sprintf("Extracting: %s", filename))
			}

			written, err := fs.CopyWithContext(ctx, outFile, tarReader)
			outFile.Close()
			if err != nil {
				close(stopTicker)
				os.Remove(extractedPath) // Clean up partial file
				return "", fmt.Errorf("extraction failed: %w", err)
			}

			log.Info("Database extracted successfully", "database", dbName, "size", formatBytes(written), "path", extractedPath)
			found = true
			break
		}
	}

	close(stopTicker)

	if !found {
		return "", fmt.Errorf("database '%s' not found in cluster backup", dbName)
	}

	return extractedPath, nil
}

// ExtractMultipleDatabasesFromCluster extracts multiple databases from cluster backup
func ExtractMultipleDatabasesFromCluster(ctx context.Context, archivePath string, dbNames []string, outputDir string, log logger.Logger, prog progress.Indicator) (map[string]string, error) {
	file, err := os.Open(archivePath)
	if err != nil {
		return nil, fmt.Errorf("cannot open archive: %w", err)
	}
	defer file.Close()

	stat, err := file.Stat()
	if err != nil {
		return nil, fmt.Errorf("cannot stat archive: %w", err)
	}
	archiveSize := stat.Size()

	gz, err := pgzip.NewReader(file)
	if err != nil {
		return nil, fmt.Errorf("not a valid gzip archive: %w", err)
	}
	defer gz.Close()

	tarReader := tar.NewReader(gz)

	// Create output directory if needed
	if err := os.MkdirAll(outputDir, 0755); err != nil {
		return nil, fmt.Errorf("cannot create output directory: %w", err)
	}

	// Build lookup map
	targetDBs := make(map[string]bool)
	for _, dbName := range dbNames {
		targetDBs[dbName] = true
	}

	extractedPaths := make(map[string]string)

	if prog != nil {
		prog.Start(fmt.Sprintf("Extracting %d databases", len(dbNames)))
		defer prog.Stop()
	}

	var bytesRead int64
	ticker := make(chan struct{})
	stopTicker := make(chan struct{})
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-stopTicker:
				return
			case <-ticker:
				if prog != nil && archiveSize > 0 {
					percentage := float64(bytesRead) / float64(archiveSize) * 100
					prog.Update(fmt.Sprintf("Scanning: %.1f%% (%d/%d found)", percentage, len(extractedPaths), len(dbNames)))
				}
			}
		}
	}()

	for {
		select {
		case <-ctx.Done():
			close(stopTicker)
			return nil, ctx.Err()
		default:
		}

		header, err := tarReader.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			close(stopTicker)
			return nil, fmt.Errorf("error reading tar archive: %w", err)
		}

		bytesRead += header.Size
		select {
		case ticker <- struct{}{}:
		default:
		}

		// Check if this is one of the databases we're looking for
		if strings.HasPrefix(header.Name, "dumps/") && !header.FileInfo().IsDir() {
			filename := filepath.Base(header.Name)

			// Extract database name
			dbName := filename
			dbName = strings.TrimSuffix(dbName, ".dump.gz")
			dbName = strings.TrimSuffix(dbName, ".dump")
			dbName = strings.TrimSuffix(dbName, ".sql.gz")
			dbName = strings.TrimSuffix(dbName, ".sql")

			if targetDBs[dbName] {
				extractedPath := filepath.Join(outputDir, filename)

				// Extract the file
				outFile, err := os.Create(extractedPath)
				if err != nil {
					close(stopTicker)
					return nil, fmt.Errorf("cannot create output file for %s: %w", dbName, err)
				}

				if prog != nil {
					prog.Update(fmt.Sprintf("Extracting: %s (%d/%d)", dbName, len(extractedPaths)+1, len(dbNames)))
				}

				written, err := fs.CopyWithContext(ctx, outFile, tarReader)
				outFile.Close()
				if err != nil {
					close(stopTicker)
					os.Remove(extractedPath) // Clean up partial file
					return nil, fmt.Errorf("extraction failed for %s: %w", dbName, err)
				}

				log.Info("Database extracted", "database", dbName, "size", formatBytes(written))
				extractedPaths[dbName] = extractedPath

				// Stop early if we found all databases
				if len(extractedPaths) == len(dbNames) {
					break
				}
			}
		}
	}

	close(stopTicker)

	// Check if all requested databases were found
	missing := make([]string, 0)
	for _, dbName := range dbNames {
		if _, found := extractedPaths[dbName]; !found {
			missing = append(missing, dbName)
		}
	}

	if len(missing) > 0 {
		return extractedPaths, fmt.Errorf("databases not found in cluster backup: %s", strings.Join(missing, ", "))
	}

	return extractedPaths, nil
}
