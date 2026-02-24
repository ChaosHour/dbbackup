package backup

import (
	"fmt"
	"os"
	"path/filepath"

	"dbbackup/internal/crypto"
	"dbbackup/internal/logger"
	"dbbackup/internal/metadata"
)

// EncryptBackupFile encrypts a backup file in-place
// The original file is replaced with the encrypted version
func EncryptBackupFile(backupPath string, key []byte, log logger.Logger) error {
	log.Info("Encrypting backup file", "file", filepath.Base(backupPath))

	// Validate key
	if err := crypto.ValidateKey(key); err != nil {
		return fmt.Errorf("invalid encryption key: %w", err)
	}

	// Create encryptor
	encryptor := crypto.NewAESEncryptor()

	// Generate encrypted file path
	encryptedPath := backupPath + ".encrypted.tmp"

	// Encrypt file
	if err := encryptor.EncryptFile(backupPath, encryptedPath, key); err != nil {
		// Clean up temp file on failure
		_ = os.Remove(encryptedPath)
		return fmt.Errorf("encryption failed: %w", err)
	}

	// Update metadata to indicate encryption
	metaPath := backupPath + ".meta.json"
	if _, err := os.Stat(metaPath); err == nil {
		// Load existing metadata (Load expects backup path, not meta path)
		meta, err := metadata.Load(backupPath)
		if err != nil {
			log.Warn("Failed to load metadata for encryption update", "error", err)
		} else {
			// Mark as encrypted
			meta.Encrypted = true
			meta.EncryptionAlgorithm = string(crypto.AlgorithmAES256GCM)

			// Save updated metadata (Save expects meta path)
			if err := metadata.Save(metaPath, meta); err != nil {
				log.Warn("Failed to update metadata with encryption info", "error", err)
			}
		}
	}

	// Remove original unencrypted file
	if err := os.Remove(backupPath); err != nil {
		log.Warn("Failed to remove original unencrypted file", "error", err)
		// Don't fail - encrypted file exists
	}

	// Rename encrypted file to original name
	if err := os.Rename(encryptedPath, backupPath); err != nil {
		return fmt.Errorf("failed to rename encrypted file: %w", err)
	}

	log.Info("Backup encrypted successfully", "file", filepath.Base(backupPath))
	return nil
}

// IsBackupEncrypted checks if a backup file is encrypted
func IsBackupEncrypted(backupPath string) bool {
	// Check metadata first - try cluster metadata (for cluster backups)
	// Only treat as cluster if it actually has databases
	if clusterMeta, err := metadata.LoadCluster(backupPath); err == nil && len(clusterMeta.Databases) > 0 {
		// For cluster backups, check if ANY database is encrypted
		for _, db := range clusterMeta.Databases {
			if db.Encrypted {
				return true
			}
		}
		// All databases are unencrypted
		return false
	}

	// Try single database metadata
	if meta, err := metadata.Load(backupPath); err == nil {
		return meta.Encrypted
	}

	// No metadata found - check file format to determine if encrypted
	// Known unencrypted formats have specific magic bytes:
	// - Gzip: 1f 8b
	// - PGDMP (PostgreSQL custom): 50 47 44 4d 50 (PGDMP)
	// - Plain SQL: starts with text (-- or SET or CREATE)
	// - Tar: 75 73 74 61 72 (ustar) at offset 257
	//
	// If file doesn't match any known format, it MIGHT be encrypted,
	// but we return false to avoid false positives. User must provide
	// metadata file or use --encrypt flag explicitly.
	file, err := os.Open(backupPath)
	if err != nil {
		return false
	}
	defer func() { _ = file.Close() }()

	header := make([]byte, 6)
	if n, err := file.Read(header); err != nil || n < 2 {
		return false
	}

	// Check for known unencrypted formats
	// Gzip magic: 1f 8b
	if header[0] == 0x1f && header[1] == 0x8b {
		return false // Gzip compressed - not encrypted
	}

	// Zstd magic: 28 B5 2F FD
	if len(header) >= 4 && header[0] == 0x28 && header[1] == 0xB5 && header[2] == 0x2F && header[3] == 0xFD {
		return false // Zstd compressed - not encrypted
	}

	// PGDMP magic (PostgreSQL custom format)
	if len(header) >= 5 && string(header[:5]) == "PGDMP" {
		return false // PostgreSQL custom dump - not encrypted
	}

	// Plain text SQL (starts with --, SET, CREATE, etc.)
	if header[0] == '-' || header[0] == 'S' || header[0] == 'C' || header[0] == '/' {
		return false // Plain text SQL - not encrypted
	}

	// Without metadata, we cannot reliably determine encryption status
	// Return false to avoid blocking restores with false positives
	return false
}

// DecryptBackupFile decrypts an encrypted backup file
// Creates a new decrypted file
func DecryptBackupFile(encryptedPath, outputPath string, key []byte, log logger.Logger) error {
	log.Info("Decrypting backup file", "file", filepath.Base(encryptedPath))

	// Validate key
	if err := crypto.ValidateKey(key); err != nil {
		return fmt.Errorf("invalid decryption key: %w", err)
	}

	// Create encryptor
	encryptor := crypto.NewAESEncryptor()

	// Decrypt file
	if err := encryptor.DecryptFile(encryptedPath, outputPath, key); err != nil {
		return fmt.Errorf("decryption failed (wrong key?): %w", err)
	}

	log.Info("Backup decrypted successfully", "output", filepath.Base(outputPath))
	return nil
}
