package security

import (
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"time"

	"dbbackup/internal/logger"
)

// AuditEvent represents an auditable event
type AuditEvent struct {
	Timestamp time.Time
	User      string
	Action    string
	Resource  string
	Result    string
	Details   map[string]interface{}
}

// AuditLogger provides audit logging functionality
type AuditLogger struct {
	log     logger.Logger
	enabled bool

	// For signed audit log support
	mu         sync.Mutex
	entries    []SignedAuditEntry
	privateKey ed25519.PrivateKey
	publicKey  ed25519.PublicKey
	prevHash   string // Hash of previous entry for chaining
}

// SignedAuditEntry represents an audit entry with cryptographic signature
type SignedAuditEntry struct {
	Sequence  int64  `json:"seq"`
	Timestamp string `json:"ts"`
	User      string `json:"user"`
	Action    string `json:"action"`
	Resource  string `json:"resource"`
	Result    string `json:"result"`
	Details   string `json:"details,omitempty"`
	PrevHash  string `json:"prev_hash"` // Hash chain for tamper detection
	Hash      string `json:"hash"`      // SHA-256 of this entry (without signature)
	Signature string `json:"sig"`       // Ed25519 signature of Hash
}

// NewAuditLogger creates a new audit logger
func NewAuditLogger(log logger.Logger, enabled bool) *AuditLogger {
	return &AuditLogger{
		log:      log,
		enabled:  enabled,
		entries:  make([]SignedAuditEntry, 0),
		prevHash: "genesis", // Initial hash for first entry
	}
}

// LogBackupStart logs backup operation start
func (a *AuditLogger) LogBackupStart(user, database, backupType string) {
	if !a.enabled {
		return
	}

	event := AuditEvent{
		Timestamp: time.Now(),
		User:      user,
		Action:    "BACKUP_START",
		Resource:  database,
		Result:    "INITIATED",
		Details: map[string]interface{}{
			"backup_type": backupType,
		},
	}

	a.logEvent(event)
}

// LogBackupComplete logs successful backup completion
func (a *AuditLogger) LogBackupComplete(user, database, archivePath string, sizeBytes int64) {
	if !a.enabled {
		return
	}

	event := AuditEvent{
		Timestamp: time.Now(),
		User:      user,
		Action:    "BACKUP_COMPLETE",
		Resource:  database,
		Result:    "SUCCESS",
		Details: map[string]interface{}{
			"archive_path": archivePath,
			"size_bytes":   sizeBytes,
		},
	}

	a.logEvent(event)
}

// LogBackupFailed logs backup failure
func (a *AuditLogger) LogBackupFailed(user, database string, err error) {
	if !a.enabled {
		return
	}

	event := AuditEvent{
		Timestamp: time.Now(),
		User:      user,
		Action:    "BACKUP_FAILED",
		Resource:  database,
		Result:    "FAILURE",
		Details: map[string]interface{}{
			"error": err.Error(),
		},
	}

	a.logEvent(event)
}

// LogRestoreStart logs restore operation start
func (a *AuditLogger) LogRestoreStart(user, database, archivePath string) {
	if !a.enabled {
		return
	}

	event := AuditEvent{
		Timestamp: time.Now(),
		User:      user,
		Action:    "RESTORE_START",
		Resource:  database,
		Result:    "INITIATED",
		Details: map[string]interface{}{
			"archive_path": archivePath,
		},
	}

	a.logEvent(event)
}

// LogRestoreComplete logs successful restore completion
func (a *AuditLogger) LogRestoreComplete(user, database string, duration time.Duration) {
	if !a.enabled {
		return
	}

	event := AuditEvent{
		Timestamp: time.Now(),
		User:      user,
		Action:    "RESTORE_COMPLETE",
		Resource:  database,
		Result:    "SUCCESS",
		Details: map[string]interface{}{
			"duration_seconds": duration.Seconds(),
		},
	}

	a.logEvent(event)
}

// LogRestoreFailed logs restore failure
func (a *AuditLogger) LogRestoreFailed(user, database string, err error) {
	if !a.enabled {
		return
	}

	event := AuditEvent{
		Timestamp: time.Now(),
		User:      user,
		Action:    "RESTORE_FAILED",
		Resource:  database,
		Result:    "FAILURE",
		Details: map[string]interface{}{
			"error": err.Error(),
		},
	}

	a.logEvent(event)
}

// LogConfigChange logs configuration changes
func (a *AuditLogger) LogConfigChange(user, setting, oldValue, newValue string) {
	if !a.enabled {
		return
	}

	event := AuditEvent{
		Timestamp: time.Now(),
		User:      user,
		Action:    "CONFIG_CHANGE",
		Resource:  setting,
		Result:    "SUCCESS",
		Details: map[string]interface{}{
			"old_value": oldValue,
			"new_value": newValue,
		},
	}

	a.logEvent(event)
}

// LogConnectionAttempt logs database connection attempts
func (a *AuditLogger) LogConnectionAttempt(user, host string, success bool, err error) {
	if !a.enabled {
		return
	}

	result := "SUCCESS"
	details := map[string]interface{}{
		"host": host,
	}

	if !success {
		result = "FAILURE"
		if err != nil {
			details["error"] = err.Error()
		}
	}

	event := AuditEvent{
		Timestamp: time.Now(),
		User:      user,
		Action:    "DB_CONNECTION",
		Resource:  host,
		Result:    result,
		Details:   details,
	}

	a.logEvent(event)
}

// logEvent writes the audit event to log
func (a *AuditLogger) logEvent(event AuditEvent) {
	fields := map[string]interface{}{
		"audit":     true,
		"timestamp": event.Timestamp.Format(time.RFC3339),
		"user":      event.User,
		"action":    event.Action,
		"resource":  event.Resource,
		"result":    event.Result,
	}

	// Merge event details
	for k, v := range event.Details {
		fields[k] = v
	}

	a.log.WithFields(fields).Info("AUDIT")
}

// GetCurrentUser returns the current system user
func GetCurrentUser() string {
	if user := os.Getenv("USER"); user != "" {
		return user
	}
	if user := os.Getenv("USERNAME"); user != "" {
		return user
	}
	return "unknown"
}

// =============================================================================
// Audit Log Signing and Verification
// =============================================================================

// GenerateSigningKeys generates a new Ed25519 key pair for audit log signing
func GenerateSigningKeys() (privateKey ed25519.PrivateKey, publicKey ed25519.PublicKey, err error) {
	publicKey, privateKey, err = ed25519.GenerateKey(rand.Reader)
	return
}

// SavePrivateKey saves the private key to a file (PEM-like format)
func SavePrivateKey(path string, key ed25519.PrivateKey) error {
	encoded := base64.StdEncoding.EncodeToString(key)
	content := fmt.Sprintf("-----BEGIN DBBACKUP AUDIT PRIVATE KEY-----\n%s\n-----END DBBACKUP AUDIT PRIVATE KEY-----\n", encoded)
	return os.WriteFile(path, []byte(content), 0600) // Restrictive permissions
}

// SavePublicKey saves the public key to a file (PEM-like format)
func SavePublicKey(path string, key ed25519.PublicKey) error {
	encoded := base64.StdEncoding.EncodeToString(key)
	content := fmt.Sprintf("-----BEGIN DBBACKUP AUDIT PUBLIC KEY-----\n%s\n-----END DBBACKUP AUDIT PUBLIC KEY-----\n", encoded)
	return os.WriteFile(path, []byte(content), 0644)
}

// LoadPrivateKey loads a private key from file
func LoadPrivateKey(path string) (ed25519.PrivateKey, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read private key: %w", err)
	}

	// Extract base64 content between PEM markers
	content := extractPEMContent(string(data))
	if content == "" {
		return nil, fmt.Errorf("invalid private key format")
	}

	decoded, err := base64.StdEncoding.DecodeString(content)
	if err != nil {
		return nil, fmt.Errorf("failed to decode private key: %w", err)
	}

	if len(decoded) != ed25519.PrivateKeySize {
		return nil, fmt.Errorf("invalid private key size")
	}

	return ed25519.PrivateKey(decoded), nil
}

// LoadPublicKey loads a public key from file
func LoadPublicKey(path string) (ed25519.PublicKey, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read public key: %w", err)
	}

	content := extractPEMContent(string(data))
	if content == "" {
		return nil, fmt.Errorf("invalid public key format")
	}

	decoded, err := base64.StdEncoding.DecodeString(content)
	if err != nil {
		return nil, fmt.Errorf("failed to decode public key: %w", err)
	}

	if len(decoded) != ed25519.PublicKeySize {
		return nil, fmt.Errorf("invalid public key size")
	}

	return ed25519.PublicKey(decoded), nil
}

// extractPEMContent extracts base64 content from PEM-like format
func extractPEMContent(data string) string {
	// Simple extraction - find content between markers
	start := 0
	for i := 0; i < len(data); i++ {
		if data[i] == '\n' && i > 0 && data[i-1] == '-' {
			start = i + 1
			break
		}
	}

	end := len(data)
	for i := len(data) - 1; i > start; i-- {
		if data[i] == '\n' && i+1 < len(data) && data[i+1] == '-' {
			end = i
			break
		}
	}

	if start >= end {
		return ""
	}

	// Remove whitespace
	result := ""
	for _, c := range data[start:end] {
		if c != '\n' && c != '\r' && c != ' ' {
			result += string(c)
		}
	}
	return result
}

// EnableSigning enables cryptographic signing for audit entries
func (a *AuditLogger) EnableSigning(privateKey ed25519.PrivateKey) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.privateKey = privateKey
	a.publicKey = privateKey.Public().(ed25519.PublicKey)
}

// AddSignedEntry adds a signed entry to the audit log
func (a *AuditLogger) AddSignedEntry(event AuditEvent) error {
	if !a.enabled {
		return nil
	}

	a.mu.Lock()
	defer a.mu.Unlock()

	// Serialize details
	detailsJSON := ""
	if len(event.Details) > 0 {
		if data, err := json.Marshal(event.Details); err == nil {
			detailsJSON = string(data)
		}
	}

	entry := SignedAuditEntry{
		Sequence:  int64(len(a.entries) + 1),
		Timestamp: event.Timestamp.Format(time.RFC3339Nano),
		User:      event.User,
		Action:    event.Action,
		Resource:  event.Resource,
		Result:    event.Result,
		Details:   detailsJSON,
		PrevHash:  a.prevHash,
	}

	// Calculate hash of entry (without signature)
	entry.Hash = a.calculateEntryHash(entry)

	// Sign if private key is available
	if a.privateKey != nil {
		hashBytes, _ := hex.DecodeString(entry.Hash)
		signature := ed25519.Sign(a.privateKey, hashBytes)
		entry.Signature = base64.StdEncoding.EncodeToString(signature)
	}

	// Update chain
	a.prevHash = entry.Hash
	a.entries = append(a.entries, entry)

	// Also log to standard logger
	a.logEvent(event)

	return nil
}

// calculateEntryHash computes SHA-256 hash of an entry (without signature field)
func (a *AuditLogger) calculateEntryHash(entry SignedAuditEntry) string {
	// Create canonical representation for hashing
	data := fmt.Sprintf("%d|%s|%s|%s|%s|%s|%s|%s",
		entry.Sequence,
		entry.Timestamp,
		entry.User,
		entry.Action,
		entry.Resource,
		entry.Result,
		entry.Details,
		entry.PrevHash,
	)

	hash := sha256.Sum256([]byte(data))
	return hex.EncodeToString(hash[:])
}

// ExportSignedLog exports the signed audit log to a file
func (a *AuditLogger) ExportSignedLog(path string) error {
	a.mu.Lock()
	defer a.mu.Unlock()

	data, err := json.MarshalIndent(a.entries, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal audit log: %w", err)
	}

	return os.WriteFile(path, data, 0644)
}

// VerifyAuditLog verifies the integrity of an exported audit log
func VerifyAuditLog(logPath string, publicKeyPath string) (*AuditVerificationResult, error) {
	// Load public key
	publicKey, err := LoadPublicKey(publicKeyPath)
	if err != nil {
		return nil, fmt.Errorf("failed to load public key: %w", err)
	}

	// Load audit log
	data, err := os.ReadFile(logPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read audit log: %w", err)
	}

	var entries []SignedAuditEntry
	if err := json.Unmarshal(data, &entries); err != nil {
		return nil, fmt.Errorf("failed to parse audit log: %w", err)
	}

	result := &AuditVerificationResult{
		TotalEntries: len(entries),
		ValidEntries: 0,
		Errors:       make([]string, 0),
	}

	prevHash := "genesis"

	for i, entry := range entries {
		// Verify hash chain
		if entry.PrevHash != prevHash {
			result.Errors = append(result.Errors,
				fmt.Sprintf("Entry %d: hash chain broken (expected %s, got %s)",
					i+1, prevHash[:16]+"...", entry.PrevHash[:min(16, len(entry.PrevHash))]+"..."))
		}

		// Recalculate hash
		expectedHash := calculateVerifyHash(entry)
		if entry.Hash != expectedHash {
			result.Errors = append(result.Errors,
				fmt.Sprintf("Entry %d: hash mismatch (entry may be tampered)", i+1))
		}

		// Verify signature
		if entry.Signature != "" {
			hashBytes, _ := hex.DecodeString(entry.Hash)
			sigBytes, err := base64.StdEncoding.DecodeString(entry.Signature)
			if err != nil {
				result.Errors = append(result.Errors,
					fmt.Sprintf("Entry %d: invalid signature encoding", i+1))
			} else if !ed25519.Verify(publicKey, hashBytes, sigBytes) {
				result.Errors = append(result.Errors,
					fmt.Sprintf("Entry %d: signature verification failed", i+1))
			} else {
				result.ValidEntries++
			}
		} else {
			result.Errors = append(result.Errors,
				fmt.Sprintf("Entry %d: missing signature", i+1))
		}

		prevHash = entry.Hash
	}

	result.ChainValid = len(result.Errors) == 0 ||
		!containsChainError(result.Errors)
	result.AllSignaturesValid = result.ValidEntries == result.TotalEntries

	return result, nil
}

// AuditVerificationResult contains the result of audit log verification
type AuditVerificationResult struct {
	TotalEntries       int
	ValidEntries       int
	ChainValid         bool
	AllSignaturesValid bool
	Errors             []string
}

// IsValid returns true if the audit log is completely valid
func (r *AuditVerificationResult) IsValid() bool {
	return r.ChainValid && r.AllSignaturesValid && len(r.Errors) == 0
}

// String returns a human-readable summary
func (r *AuditVerificationResult) String() string {
	if r.IsValid() {
		return fmt.Sprintf("✅ Audit log verified: %d entries, chain intact, all signatures valid",
			r.TotalEntries)
	}

	return fmt.Sprintf("❌ Audit log verification failed: %d/%d valid entries, %d errors",
		r.ValidEntries, r.TotalEntries, len(r.Errors))
}

// calculateVerifyHash recalculates hash for verification
func calculateVerifyHash(entry SignedAuditEntry) string {
	data := fmt.Sprintf("%d|%s|%s|%s|%s|%s|%s|%s",
		entry.Sequence,
		entry.Timestamp,
		entry.User,
		entry.Action,
		entry.Resource,
		entry.Result,
		entry.Details,
		entry.PrevHash,
	)

	hash := sha256.Sum256([]byte(data))
	return hex.EncodeToString(hash[:])
}

// containsChainError checks if errors include hash chain issues
func containsChainError(errors []string) bool {
	for _, err := range errors {
		if len(err) > 0 && (err[0:min(20, len(err))] == "Entry" &&
			(contains(err, "hash chain") || contains(err, "hash mismatch"))) {
			return true
		}
	}
	return false
}

// contains is a simple string contains helper
func contains(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}

// min returns the minimum of two ints
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
