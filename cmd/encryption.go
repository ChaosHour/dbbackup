package cmd

import (
	"encoding/base64"
	"fmt"
	"os"
	"strings"

	"dbbackup/internal/crypto"
	"github.com/spf13/cobra"
)

var encryptionCmd = &cobra.Command{
	Use:   "encryption",
	Short: "Encryption key management",
	Long: `Manage encryption keys for database backups.

This command group provides encryption key management utilities:
  - rotate: Generate new encryption keys and rotate existing ones

Examples:
  # Generate new encryption key
  dbbackup encryption rotate

  # Show rotation workflow
  dbbackup encryption rotate --show-reencrypt`,
}

func init() {
	rootCmd.AddCommand(encryptionCmd)
}

// loadEncryptionKey loads encryption key from file or environment variable
func loadEncryptionKey(keyFile, keyEnvVar string) ([]byte, error) {
	// Priority 1: Key file
	if keyFile != "" {
		keyData, err := os.ReadFile(keyFile)
		if err != nil {
			return nil, fmt.Errorf("failed to read encryption key file: %w", err)
		}

		// Try to decode as base64 first
		if decoded, err := base64.StdEncoding.DecodeString(strings.TrimSpace(string(keyData))); err == nil && len(decoded) == crypto.KeySize {
			return decoded, nil
		}

		// Use raw bytes if exactly 32 bytes
		if len(keyData) == crypto.KeySize {
			return keyData, nil
		}

		// Otherwise treat as passphrase and derive key
		salt, err := crypto.GenerateSalt()
		if err != nil {
			return nil, fmt.Errorf("failed to generate salt: %w", err)
		}
		key := crypto.DeriveKey([]byte(strings.TrimSpace(string(keyData))), salt)
		return key, nil
	}

	// Priority 2: Environment variable
	if keyEnvVar != "" {
		keyData := os.Getenv(keyEnvVar)
		if keyData == "" {
			return nil, fmt.Errorf("encryption enabled but %s environment variable not set", keyEnvVar)
		}

		// Try to decode as base64 first
		if decoded, err := base64.StdEncoding.DecodeString(strings.TrimSpace(keyData)); err == nil && len(decoded) == crypto.KeySize {
			return decoded, nil
		}

		// Otherwise treat as passphrase and derive key
		salt, err := crypto.GenerateSalt()
		if err != nil {
			return nil, fmt.Errorf("failed to generate salt: %w", err)
		}
		key := crypto.DeriveKey([]byte(strings.TrimSpace(keyData)), salt)
		return key, nil
	}

	return nil, fmt.Errorf("encryption enabled but no key source specified (use --encryption-key-file or set %s)", keyEnvVar)
}

// isEncryptionEnabled checks if encryption is requested
func isEncryptionEnabled() bool {
	return encryptBackupFlag
}
