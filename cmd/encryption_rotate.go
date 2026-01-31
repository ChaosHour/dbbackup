package cmd

import (
	"crypto/rand"
	"encoding/base64"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/spf13/cobra"
)

var encryptionRotateCmd = &cobra.Command{
	Use:   "rotate",
	Short: "Rotate encryption keys",
	Long: `Generate new encryption keys and provide migration instructions.

This command helps with encryption key management:
  - Generates new secure encryption keys
  - Provides safe key rotation workflow
  - Creates backup of old keys
  - Shows re-encryption commands for existing backups

Key Rotation Workflow:
  1. Generate new key with this command
  2. Back up existing backups with old key
  3. Update configuration with new key
  4. Re-encrypt old backups (optional)
  5. Securely delete old key

Security Best Practices:
  - Rotate keys every 90-365 days
  - Never store keys in version control
  - Use key management systems (AWS KMS, HashiCorp Vault)
  - Keep old keys until all backups are re-encrypted
  - Test decryption before deleting old keys

Examples:
  # Generate new encryption key
  dbbackup encryption rotate

  # Generate key with specific strength
  dbbackup encryption rotate --key-size 256

  # Save key to file
  dbbackup encryption rotate --output /secure/path/new.key

  # Show re-encryption commands
  dbbackup encryption rotate --show-reencrypt`,
	RunE: runEncryptionRotate,
}

var (
	rotateKeySize       int
	rotateOutput        string
	rotateShowReencrypt bool
	rotateFormat        string
)

func init() {
	encryptionCmd.AddCommand(encryptionRotateCmd)

	encryptionRotateCmd.Flags().IntVar(&rotateKeySize, "key-size", 256, "Key size in bits (128, 192, 256)")
	encryptionRotateCmd.Flags().StringVar(&rotateOutput, "output", "", "Save new key to file (default: display only)")
	encryptionRotateCmd.Flags().BoolVar(&rotateShowReencrypt, "show-reencrypt", true, "Show re-encryption commands")
	encryptionRotateCmd.Flags().StringVar(&rotateFormat, "format", "base64", "Key format (base64, hex)")
}

func runEncryptionRotate(cmd *cobra.Command, args []string) error {
	fmt.Println("[KEY ROTATION] Encryption Key Management")
	fmt.Println("=========================================")
	fmt.Println()

	// Validate key size
	if rotateKeySize != 128 && rotateKeySize != 192 && rotateKeySize != 256 {
		return fmt.Errorf("invalid key size: %d (must be 128, 192, or 256)", rotateKeySize)
	}

	keyBytes := rotateKeySize / 8

	// Generate new key
	fmt.Printf("[GENERATE] Creating new %d-bit encryption key...\n", rotateKeySize)

	key := make([]byte, keyBytes)
	if _, err := rand.Read(key); err != nil {
		return fmt.Errorf("failed to generate random key: %w", err)
	}

	// Format key
	var keyString string
	switch rotateFormat {
	case "base64":
		keyString = base64.StdEncoding.EncodeToString(key)
	case "hex":
		keyString = fmt.Sprintf("%x", key)
	default:
		return fmt.Errorf("invalid format: %s (use base64 or hex)", rotateFormat)
	}

	fmt.Println("[OK] New encryption key generated")
	fmt.Println()

	// Display new key
	fmt.Println("[NEW KEY]")
	fmt.Println("=========================================")
	fmt.Printf("Format:    %s\n", rotateFormat)
	fmt.Printf("Size:      %d bits (%d bytes)\n", rotateKeySize, keyBytes)
	fmt.Printf("Generated: %s\n", time.Now().Format(time.RFC3339))
	fmt.Println()
	fmt.Println("Key:")
	fmt.Printf("  %s\n", keyString)
	fmt.Println()

	// Save to file if requested
	if rotateOutput != "" {
		if err := saveKeyToFile(rotateOutput, keyString); err != nil {
			return fmt.Errorf("failed to save key: %w", err)
		}
		fmt.Printf("[SAVED] Key written to: %s\n", rotateOutput)
		fmt.Println("[WARN] Secure this file with proper permissions!")
		fmt.Printf("       chmod 600 %s\n", rotateOutput)
		fmt.Println()
	}

	// Show rotation workflow
	fmt.Println("[KEY ROTATION WORKFLOW]")
	fmt.Println("=========================================")
	fmt.Println()
	fmt.Println("1. [BACKUP] Back up your old key:")
	fmt.Println("   export OLD_KEY=\"$DBBACKUP_ENCRYPTION_KEY\"")
	fmt.Println("   echo $OLD_KEY > /secure/backup/old-key.txt")
	fmt.Println()
	fmt.Println("2. [UPDATE] Update your configuration:")
	if rotateOutput != "" {
		fmt.Printf("   export DBBACKUP_ENCRYPTION_KEY=$(cat %s)\n", rotateOutput)
	} else {
		fmt.Printf("   export DBBACKUP_ENCRYPTION_KEY=\"%s\"\n", keyString)
	}
	fmt.Println("   # Or update .dbbackup.conf or systemd environment")
	fmt.Println()
	fmt.Println("3. [VERIFY] Test new key with a backup:")
	fmt.Println("   dbbackup backup single testdb --encryption-key-env DBBACKUP_ENCRYPTION_KEY")
	fmt.Println()
	fmt.Println("4. [RE-ENCRYPT] Re-encrypt existing backups (optional):")
	if rotateShowReencrypt {
		showReencryptCommands()
	}
	fmt.Println()
	fmt.Println("5. [CLEANUP] After all backups re-encrypted:")
	fmt.Println("   # Securely delete old key")
	fmt.Println("   shred -u /secure/backup/old-key.txt")
	fmt.Println("   unset OLD_KEY")
	fmt.Println()

	// Security warnings
	fmt.Println("[SECURITY WARNINGS]")
	fmt.Println("=========================================")
	fmt.Println()
	fmt.Println("⚠  DO NOT store keys in:")
	fmt.Println("   - Version control (git, svn)")
	fmt.Println("   - Unencrypted files")
	fmt.Println("   - Email or chat logs")
	fmt.Println("   - Shell history (use env vars)")
	fmt.Println()
	fmt.Println("✓  DO store keys in:")
	fmt.Println("   - Hardware Security Modules (HSM)")
	fmt.Println("   - Key Management Systems (AWS KMS, Vault)")
	fmt.Println("   - Encrypted password managers")
	fmt.Println("   - Encrypted environment files (0600 permissions)")
	fmt.Println()
	fmt.Println("✓  Key Rotation Schedule:")
	fmt.Println("   - Production: Every 90 days")
	fmt.Println("   - Development: Every 180 days")
	fmt.Println("   - After security incident: Immediately")
	fmt.Println()

	return nil
}

func saveKeyToFile(path string, key string) error {
	// Create directory if needed
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0700); err != nil {
		return fmt.Errorf("failed to create directory: %w", err)
	}

	// Write key file with restricted permissions
	if err := os.WriteFile(path, []byte(key+"\n"), 0600); err != nil {
		return fmt.Errorf("failed to write file: %w", err)
	}

	return nil
}

func showReencryptCommands() {
	// Use explicit string to avoid go vet warnings about % in shell parameter expansion
	pctEnc := "${backup%.enc}"
	
	fmt.Println("   # Option A: Re-encrypt with openssl")
	fmt.Println("   for backup in /path/to/backups/*.enc; do")
	fmt.Println("     # Decrypt with old key")
	fmt.Println("     openssl enc -aes-256-cbc -d \\")
	fmt.Println("       -in \"$backup\" \\")
	fmt.Printf("       -out \"%s.tmp\" \\\n", pctEnc)
	fmt.Println("       -k \"$OLD_KEY\"")
	fmt.Println()
	fmt.Println("     # Encrypt with new key")
	fmt.Println("     openssl enc -aes-256-cbc \\")
	fmt.Printf("       -in \"%s.tmp\" \\\n", pctEnc)
	fmt.Println("       -out \"${backup}.new\" \\")
	fmt.Println("       -k \"$DBBACKUP_ENCRYPTION_KEY\"")
	fmt.Println()
	fmt.Println("     # Verify and replace")
	fmt.Println("     if [ -f \"${backup}.new\" ]; then")
	fmt.Println("       mv \"${backup}.new\" \"$backup\"")
	fmt.Printf("       rm \"%s.tmp\"\n", pctEnc)
	fmt.Println("     fi")
	fmt.Println("   done")
	fmt.Println()
	fmt.Println("   # Option B: Decrypt and re-backup")
	fmt.Println("   # 1. Restore from old encrypted backups")
	fmt.Println("   # 2. Create new backups with new key")
	fmt.Println("   # 3. Verify new backups")
	fmt.Println("   # 4. Delete old backups")
}
