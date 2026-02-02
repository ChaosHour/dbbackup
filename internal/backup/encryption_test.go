package backup

import (
	"crypto/rand"
	"os"
	"path/filepath"
	"testing"

	"dbbackup/internal/logger"
)

// generateTestKey generates a 32-byte key for testing
func generateTestKey() ([]byte, error) {
	key := make([]byte, 32)
	_, err := rand.Read(key)
	return key, err
}

// TestEncryptBackupFile tests backup encryption
func TestEncryptBackupFile(t *testing.T) {
	tmpDir := t.TempDir()
	log := logger.New("info", "text")

	// Create a test backup file
	backupPath := filepath.Join(tmpDir, "test_backup.dump")
	testData := []byte("-- PostgreSQL dump\nCREATE TABLE test (id int);\n")
	if err := os.WriteFile(backupPath, testData, 0644); err != nil {
		t.Fatalf("failed to create test backup: %v", err)
	}

	// Generate encryption key
	key, err := generateTestKey()
	if err != nil {
		t.Fatalf("failed to generate key: %v", err)
	}

	// Encrypt the backup
	err = EncryptBackupFile(backupPath, key, log)
	if err != nil {
		t.Fatalf("encryption failed: %v", err)
	}

	// Verify file exists
	if _, err := os.Stat(backupPath); err != nil {
		t.Fatalf("encrypted file should exist: %v", err)
	}

	// Encrypted data should be different from original
	encryptedData, err := os.ReadFile(backupPath)
	if err != nil {
		t.Fatalf("failed to read encrypted file: %v", err)
	}

	if string(encryptedData) == string(testData) {
		t.Error("encrypted data should be different from original")
	}
}

// TestEncryptBackupFileInvalidKey tests encryption with invalid key
func TestEncryptBackupFileInvalidKey(t *testing.T) {
	tmpDir := t.TempDir()
	log := logger.New("info", "text")

	// Create a test backup file
	backupPath := filepath.Join(tmpDir, "test_backup.dump")
	testData := []byte("-- PostgreSQL dump\nCREATE TABLE test (id int);\n")
	if err := os.WriteFile(backupPath, testData, 0644); err != nil {
		t.Fatalf("failed to create test backup: %v", err)
	}

	// Try with invalid key (too short)
	invalidKey := []byte("short")
	err := EncryptBackupFile(backupPath, invalidKey, log)
	if err == nil {
		t.Error("encryption should fail with invalid key")
	}
}

// TestIsBackupEncrypted tests encrypted backup detection
func TestIsBackupEncrypted(t *testing.T) {
	tmpDir := t.TempDir()

	tests := []struct {
		name      string
		data      []byte
		encrypted bool
	}{
		{
			name:      "gzip_file",
			data:      []byte{0x1f, 0x8b, 0x08, 0x00}, // gzip magic
			encrypted: false,
		},
		{
			name:      "PGDMP_file",
			data:      []byte("PGDMP"), // PostgreSQL custom format magic
			encrypted: false,
		},
		{
			name:      "plain_SQL",
			data:      []byte("-- PostgreSQL dump\nSET statement_timeout = 0;"),
			encrypted: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			backupPath := filepath.Join(tmpDir, tt.name+".dump")
			if err := os.WriteFile(backupPath, tt.data, 0644); err != nil {
				t.Fatalf("failed to create test file: %v", err)
			}

			got := IsBackupEncrypted(backupPath)
			if got != tt.encrypted {
				t.Errorf("IsBackupEncrypted() = %v, want %v", got, tt.encrypted)
			}
		})
	}
}

// TestIsBackupEncryptedNonexistent tests with nonexistent file
func TestIsBackupEncryptedNonexistent(t *testing.T) {
	result := IsBackupEncrypted("/nonexistent/path/backup.dump")
	if result {
		t.Error("should return false for nonexistent file")
	}
}

// TestDecryptBackupFile tests backup decryption
func TestDecryptBackupFile(t *testing.T) {
	tmpDir := t.TempDir()
	log := logger.New("info", "text")

	// Create and encrypt a test backup file
	backupPath := filepath.Join(tmpDir, "test_backup.dump")
	testData := []byte("-- PostgreSQL dump\nCREATE TABLE test (id int);\n")
	if err := os.WriteFile(backupPath, testData, 0644); err != nil {
		t.Fatalf("failed to create test backup: %v", err)
	}

	// Generate encryption key
	key, err := generateTestKey()
	if err != nil {
		t.Fatalf("failed to generate key: %v", err)
	}

	// Encrypt the backup
	err = EncryptBackupFile(backupPath, key, log)
	if err != nil {
		t.Fatalf("encryption failed: %v", err)
	}

	// Decrypt the backup
	decryptedPath := filepath.Join(tmpDir, "decrypted.dump")
	err = DecryptBackupFile(backupPath, decryptedPath, key, log)
	if err != nil {
		t.Fatalf("decryption failed: %v", err)
	}

	// Verify decrypted content matches original
	decryptedData, err := os.ReadFile(decryptedPath)
	if err != nil {
		t.Fatalf("failed to read decrypted file: %v", err)
	}

	if string(decryptedData) != string(testData) {
		t.Error("decrypted data should match original")
	}
}

// TestDecryptBackupFileWrongKey tests decryption with wrong key
func TestDecryptBackupFileWrongKey(t *testing.T) {
	tmpDir := t.TempDir()
	log := logger.New("info", "text")

	// Create and encrypt a test backup file
	backupPath := filepath.Join(tmpDir, "test_backup.dump")
	testData := []byte("-- PostgreSQL dump\nCREATE TABLE test (id int);\n")
	if err := os.WriteFile(backupPath, testData, 0644); err != nil {
		t.Fatalf("failed to create test backup: %v", err)
	}

	// Generate encryption key
	key1, err := generateTestKey()
	if err != nil {
		t.Fatalf("failed to generate key: %v", err)
	}

	// Encrypt the backup
	err = EncryptBackupFile(backupPath, key1, log)
	if err != nil {
		t.Fatalf("encryption failed: %v", err)
	}

	// Generate a different key
	key2, err := generateTestKey()
	if err != nil {
		t.Fatalf("failed to generate key: %v", err)
	}

	// Try to decrypt with wrong key
	decryptedPath := filepath.Join(tmpDir, "decrypted.dump")
	err = DecryptBackupFile(backupPath, decryptedPath, key2, log)
	if err == nil {
		t.Error("decryption should fail with wrong key")
	}
}

// TestEncryptDecryptRoundTrip tests full encrypt/decrypt cycle
func TestEncryptDecryptRoundTrip(t *testing.T) {
	tmpDir := t.TempDir()
	log := logger.New("info", "text")

	// Create a larger test file
	testData := make([]byte, 10240) // 10KB
	for i := range testData {
		testData[i] = byte(i % 256)
	}

	backupPath := filepath.Join(tmpDir, "test_backup.dump")
	if err := os.WriteFile(backupPath, testData, 0644); err != nil {
		t.Fatalf("failed to create test backup: %v", err)
	}

	// Generate encryption key
	key, err := generateTestKey()
	if err != nil {
		t.Fatalf("failed to generate key: %v", err)
	}

	// Encrypt
	err = EncryptBackupFile(backupPath, key, log)
	if err != nil {
		t.Fatalf("encryption failed: %v", err)
	}

	// Decrypt to new path
	decryptedPath := filepath.Join(tmpDir, "decrypted.dump")
	err = DecryptBackupFile(backupPath, decryptedPath, key, log)
	if err != nil {
		t.Fatalf("decryption failed: %v", err)
	}

	// Verify content matches
	decryptedData, err := os.ReadFile(decryptedPath)
	if err != nil {
		t.Fatalf("failed to read decrypted file: %v", err)
	}

	if len(decryptedData) != len(testData) {
		t.Errorf("length mismatch: got %d, want %d", len(decryptedData), len(testData))
	}

	for i := range testData {
		if decryptedData[i] != testData[i] {
			t.Errorf("data mismatch at byte %d: got %d, want %d", i, decryptedData[i], testData[i])
			break
		}
	}
}
