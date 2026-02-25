package cloud

import (
	"context"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/pkg/sftp"
	"golang.org/x/crypto/ssh"
	"golang.org/x/crypto/ssh/knownhosts"
)

// SFTPBackend implements the Backend interface for SFTP remote servers
type SFTPBackend struct {
	mu         sync.Mutex
	sshClient  *ssh.Client
	sftpClient *sftp.Client
	host       string // "host:port"
	user       string
	prefix     string // remote base directory
	config     *Config
	sshConfig  *ssh.ClientConfig
}

// NewSFTPBackend creates a new SFTP backend
func NewSFTPBackend(cfg *Config) (*SFTPBackend, error) {
	if cfg.Endpoint == "" {
		return nil, fmt.Errorf("endpoint is required for SFTP backend (use sftp://user@host/path)")
	}

	// Parse endpoint: "user@host:port"
	user, host, err := parseSFTPEndpoint(cfg.Endpoint)
	if err != nil {
		return nil, fmt.Errorf("invalid SFTP endpoint %q: %w", cfg.Endpoint, err)
	}

	// Build SSH auth methods
	authMethods, err := buildSSHAuthMethods(cfg)
	if err != nil {
		return nil, fmt.Errorf("no SSH authentication method available: %w", err)
	}

	// Build host key callback
	hostKeyCallback, err := buildHostKeyCallback(cfg)
	if err != nil {
		return nil, err
	}

	sshConfig := &ssh.ClientConfig{
		User:            user,
		Auth:            authMethods,
		HostKeyCallback: hostKeyCallback,
		Timeout:         30 * time.Second,
	}

	prefix := strings.TrimRight(cfg.Prefix, "/")

	return &SFTPBackend{
		host:      host,
		user:      user,
		prefix:    prefix,
		config:    cfg,
		sshConfig: sshConfig,
	}, nil
}

// parseSFTPEndpoint parses "user@host:port" into user and host:port
func parseSFTPEndpoint(endpoint string) (user, host string, err error) {
	parts := strings.SplitN(endpoint, "@", 2)
	if len(parts) != 2 || parts[0] == "" {
		return "", "", fmt.Errorf("must be in format user@host:port")
	}
	user = parts[0]
	host = parts[1]

	// Ensure host has a port
	if _, _, err := net.SplitHostPort(host); err != nil {
		host = host + ":22"
	}

	return user, host, nil
}

// buildSSHAuthMethods builds the SSH auth chain: key file → SSH agent → password
func buildSSHAuthMethods(cfg *Config) ([]ssh.AuthMethod, error) {
	var methods []ssh.AuthMethod

	// 1. SSH key file
	if cfg.SFTPKeyPath != "" {
		keyData, err := os.ReadFile(cfg.SFTPKeyPath)
		if err != nil {
			return nil, fmt.Errorf("failed to read SSH key %s: %w", cfg.SFTPKeyPath, err)
		}
		var signer ssh.Signer
		if cfg.SFTPKeyPassphrase != "" {
			signer, err = ssh.ParsePrivateKeyWithPassphrase(keyData, []byte(cfg.SFTPKeyPassphrase))
		} else {
			signer, err = ssh.ParsePrivateKey(keyData)
		}
		if err != nil {
			return nil, fmt.Errorf("failed to parse SSH key %s: %w", cfg.SFTPKeyPath, err)
		}
		methods = append(methods, ssh.PublicKeys(signer))
	}

	// 2. Try default SSH keys if no explicit key specified
	if cfg.SFTPKeyPath == "" {
		home, err := os.UserHomeDir()
		if err == nil && home != "/" {
			for _, keyName := range []string{"id_ed25519", "id_rsa", "id_ecdsa"} {
				keyPath := filepath.Join(home, ".ssh", keyName)
				keyData, err := os.ReadFile(keyPath)
				if err != nil {
					continue
				}
				signer, err := ssh.ParsePrivateKey(keyData)
				if err != nil {
					continue
				}
				methods = append(methods, ssh.PublicKeys(signer))
				break // Use first available key
			}
		}
	}

	// 3. SSH agent via SSH_AUTH_SOCK
	if agentAuth := sshAgentAuth(); agentAuth != nil {
		methods = append(methods, agentAuth)
	}

	// 4. Password
	if cfg.SFTPPassword != "" {
		methods = append(methods, ssh.Password(cfg.SFTPPassword))
	}

	if len(methods) == 0 {
		return nil, fmt.Errorf("no SSH key, agent, or password configured (use --sftp-key, --sftp-password, or SSH_AUTH_SOCK)")
	}

	return methods, nil
}

// sshAgentAuth is a placeholder — SSH agent support requires the agent package.
// For now, users should provide key files or passwords directly.
func sshAgentAuth() ssh.AuthMethod {
	return nil
}

// buildHostKeyCallback returns the host key verification callback
func buildHostKeyCallback(cfg *Config) (ssh.HostKeyCallback, error) {
	if cfg.SFTPInsecure {
		return ssh.InsecureIgnoreHostKey(), nil // #nosec G106 - user-requested
	}

	// Try explicit known_hosts path
	knownHostsPath := cfg.SFTPKnownHostsPath
	if knownHostsPath == "" {
		home, err := os.UserHomeDir()
		if err != nil || home == "/" {
			home = "/root"
		}
		knownHostsPath = filepath.Join(home, ".ssh", "known_hosts")
	}

	// Check if known_hosts file exists
	if _, err := os.Stat(knownHostsPath); os.IsNotExist(err) {
		return nil, fmt.Errorf("known_hosts file not found at %s (use --sftp-known-hosts or --sftp-insecure)", knownHostsPath)
	}

	callback, err := knownhosts.New(knownHostsPath)
	if err != nil {
		return nil, fmt.Errorf("failed to parse known_hosts %s: %w", knownHostsPath, err)
	}
	return callback, nil
}

// connect establishes SSH+SFTP connection (lazy, called on first operation)
func (s *SFTPBackend) connect() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.sftpClient != nil {
		return nil
	}

	sshClient, err := ssh.Dial("tcp", s.host, s.sshConfig)
	if err != nil {
		return fmt.Errorf("SSH connection to %s failed: %w", s.host, err)
	}

	sftpClient, err := sftp.NewClient(sshClient)
	if err != nil {
		_ = sshClient.Close()
		return fmt.Errorf("SFTP session failed: %w", err)
	}

	s.sshClient = sshClient
	s.sftpClient = sftpClient
	return nil
}

// Close closes the SFTP and SSH connections
func (s *SFTPBackend) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.sftpClient != nil {
		_ = s.sftpClient.Close()
		s.sftpClient = nil
	}
	if s.sshClient != nil {
		_ = s.sshClient.Close()
		s.sshClient = nil
	}
	return nil
}

// Name returns the backend name
func (s *SFTPBackend) Name() string {
	return "sftp"
}

// buildFilePath constructs the full remote path including prefix
func (s *SFTPBackend) buildFilePath(remotePath string) string {
	if s.prefix == "" {
		return remotePath
	}
	return s.prefix + "/" + remotePath
}

// Upload uploads a file to the SFTP server
func (s *SFTPBackend) Upload(ctx context.Context, localPath, remotePath string, progress ProgressCallback) error {
	return RetryOperationWithNotify(ctx, DefaultRetryConfig(), func() error {
		if err := s.connect(); err != nil {
			return err
		}

		// Open local file
		file, err := os.Open(localPath)
		if err != nil {
			return fmt.Errorf("failed to open file: %w", err)
		}
		defer func() { _ = file.Close() }()

		stat, err := file.Stat()
		if err != nil {
			return fmt.Errorf("failed to stat file: %w", err)
		}
		fileSize := stat.Size()

		// Build remote path
		filePath := s.buildFilePath(remotePath)

		// Create remote directory if needed
		remoteDir := filepath.Dir(filePath)
		if remoteDir != "." && remoteDir != "/" {
			if err := s.sftpClient.MkdirAll(remoteDir); err != nil {
				return fmt.Errorf("failed to create remote directory %s: %w", remoteDir, err)
			}
		}

		// Create remote file
		remoteFile, err := s.sftpClient.Create(filePath)
		if err != nil {
			return fmt.Errorf("failed to create remote file %s: %w", filePath, err)
		}
		defer func() { _ = remoteFile.Close() }()

		// Wrap reader with progress tracking
		var reader io.Reader = file
		if progress != nil {
			reader = NewProgressReader(file, fileSize, progress)
		}

		// Apply bandwidth throttling if configured
		if s.config.BandwidthLimit > 0 {
			reader = NewThrottledReader(ctx, reader, s.config.BandwidthLimit)
		}

		// Copy with context cancellation support
		_, err = CopyWithContext(ctx, remoteFile, reader)
		if err != nil {
			return fmt.Errorf("failed to upload file: %w", err)
		}

		return nil
	}, func(err error, duration time.Duration) {
		fmt.Printf("[SFTP] Upload retry in %v: %v\n", duration, err)
	})
}

// Download downloads a file from the SFTP server
func (s *SFTPBackend) Download(ctx context.Context, remotePath, localPath string, progress ProgressCallback) error {
	return RetryOperationWithNotify(ctx, DefaultRetryConfig(), func() error {
		if err := s.connect(); err != nil {
			return err
		}

		filePath := s.buildFilePath(remotePath)

		// Open remote file
		remoteFile, err := s.sftpClient.Open(filePath)
		if err != nil {
			return fmt.Errorf("failed to open remote file %s: %w", filePath, err)
		}
		defer func() { _ = remoteFile.Close() }()

		// Get remote file size for progress
		stat, err := remoteFile.Stat()
		if err != nil {
			return fmt.Errorf("failed to stat remote file: %w", err)
		}
		totalSize := stat.Size()

		// Create parent directory if needed
		if dir := filepath.Dir(localPath); dir != "." {
			if err := os.MkdirAll(dir, 0750); err != nil {
				return fmt.Errorf("failed to create directory: %w", err)
			}
		}

		// Create output file
		outFile, err := os.Create(localPath)
		if err != nil {
			return fmt.Errorf("failed to create output file: %w", err)
		}
		defer func() { _ = outFile.Close() }()

		// Wrap reader with progress tracking
		var reader io.Reader = remoteFile
		if progress != nil && totalSize > 0 {
			reader = NewProgressReader(remoteFile, totalSize, progress)
		}

		// Apply bandwidth throttling if configured
		if s.config.BandwidthLimit > 0 {
			reader = NewThrottledReader(ctx, reader, s.config.BandwidthLimit)
		}

		// Copy with context cancellation support
		_, err = CopyWithContext(ctx, outFile, reader)
		if err != nil {
			return fmt.Errorf("failed to download file: %w", err)
		}

		return nil
	}, func(err error, duration time.Duration) {
		fmt.Printf("[SFTP] Download retry in %v: %v\n", duration, err)
	})
}

// List lists files on the SFTP server under the prefix
func (s *SFTPBackend) List(ctx context.Context, prefix string) ([]BackupInfo, error) {
	if err := s.connect(); err != nil {
		return nil, err
	}

	// Combine backend prefix with user-supplied prefix
	fullPrefix := s.prefix
	if prefix != "" {
		if fullPrefix != "" {
			fullPrefix = fullPrefix + "/" + prefix
		} else {
			fullPrefix = prefix
		}
	}

	// Determine directory to list
	listDir := fullPrefix
	if listDir == "" {
		listDir = "."
	}

	entries, err := s.sftpClient.ReadDir(listDir)
	if err != nil {
		return nil, fmt.Errorf("failed to list directory %s: %w", listDir, err)
	}

	var files []BackupInfo
	for _, entry := range entries {
		// Check context
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		if entry.IsDir() {
			continue
		}

		key := entry.Name()
		if fullPrefix != "" {
			key = fullPrefix + "/" + entry.Name()
		}

		files = append(files, BackupInfo{
			Key:          key,
			Name:         entry.Name(),
			Size:         entry.Size(),
			LastModified: entry.ModTime(),
		})
	}

	return files, nil
}

// Delete deletes a file from the SFTP server
func (s *SFTPBackend) Delete(ctx context.Context, remotePath string) error {
	if err := s.connect(); err != nil {
		return err
	}

	filePath := s.buildFilePath(remotePath)
	if err := s.sftpClient.Remove(filePath); err != nil {
		return fmt.Errorf("failed to delete %s: %w", filePath, err)
	}
	return nil
}

// Exists checks if a file exists on the SFTP server
func (s *SFTPBackend) Exists(ctx context.Context, remotePath string) (bool, error) {
	if err := s.connect(); err != nil {
		return false, err
	}

	filePath := s.buildFilePath(remotePath)
	_, err := s.sftpClient.Stat(filePath)
	if err != nil {
		if os.IsNotExist(err) {
			return false, nil
		}
		return false, fmt.Errorf("failed to stat %s: %w", filePath, err)
	}
	return true, nil
}

// GetSize returns the size of a remote file on the SFTP server
func (s *SFTPBackend) GetSize(ctx context.Context, remotePath string) (int64, error) {
	if err := s.connect(); err != nil {
		return 0, err
	}

	filePath := s.buildFilePath(remotePath)
	info, err := s.sftpClient.Stat(filePath)
	if err != nil {
		if os.IsNotExist(err) {
			return 0, fmt.Errorf("file not found: %s", filePath)
		}
		return 0, fmt.Errorf("failed to stat %s: %w", filePath, err)
	}
	return info.Size(), nil
}
