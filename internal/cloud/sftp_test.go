package cloud

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestNewSFTPBackend_Validation(t *testing.T) {
	tests := []struct {
		name    string
		cfg     *Config
		wantErr string
	}{
		{
			name: "missing endpoint",
			cfg: &Config{
				Provider: "sftp",
			},
			wantErr: "endpoint is required",
		},
		{
			name: "invalid endpoint format",
			cfg: &Config{
				Provider: "sftp",
				Endpoint: "justhost",
			},
			wantErr: "must be in format user@host:port",
		},
		{
			name: "no auth methods with nonexistent key",
			cfg: &Config{
				Provider:    "sftp",
				Endpoint:    "backup@nas.local:22",
				SFTPKeyPath: "/nonexistent/key",
				SFTPInsecure: true,
			},
			wantErr: "failed to read SSH key",
		},
		{
			name: "valid config with password",
			cfg: &Config{
				Provider:     "sftp",
				Endpoint:     "backup@nas.local:22",
				SFTPPassword: "secret",
				SFTPInsecure: true,
			},
			wantErr: "",
		},
		{
			name: "valid config with prefix",
			cfg: &Config{
				Provider:     "sftp",
				Endpoint:     "root@10.0.0.5:2222",
				SFTPPassword: "secret",
				SFTPInsecure: true,
				Prefix:       "backups/myhost",
			},
			wantErr: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			backend, err := NewSFTPBackend(tt.cfg)
			if tt.wantErr != "" {
				if err == nil {
					t.Fatalf("expected error containing %q, got nil", tt.wantErr)
				}
				if !strings.Contains(err.Error(), tt.wantErr) {
					t.Errorf("error = %q, want containing %q", err.Error(), tt.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if backend == nil {
				t.Fatal("backend is nil")
			}
		})
	}
}

func TestSFTPBackend_Name(t *testing.T) {
	backend := &SFTPBackend{}
	if got := backend.Name(); got != "sftp" {
		t.Errorf("Name() = %q, want %q", got, "sftp")
	}
}

func TestSFTPBuildFilePath(t *testing.T) {
	tests := []struct {
		name       string
		prefix     string
		remotePath string
		want       string
	}{
		{"no prefix", "", "file.dump", "file.dump"},
		{"with prefix", "backups/myhost", "file.dump", "backups/myhost/file.dump"},
		{"nested prefix", "backups/host/pg", "db.dump.gz", "backups/host/pg/db.dump.gz"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &SFTPBackend{prefix: tt.prefix}
			got := s.buildFilePath(tt.remotePath)
			if got != tt.want {
				t.Errorf("buildFilePath(%q) = %q, want %q", tt.remotePath, got, tt.want)
			}
		})
	}
}

func TestParseSFTPEndpoint(t *testing.T) {
	tests := []struct {
		name     string
		endpoint string
		wantUser string
		wantHost string
		wantErr  bool
	}{
		{"full endpoint", "backup@nas.local:22", "backup", "nas.local:22", false},
		{"custom port", "root@10.0.0.5:2222", "root", "10.0.0.5:2222", false},
		{"default port", "user@host.example.com", "user", "host.example.com:22", false},
		{"no user", "host.example.com:22", "", "", true},
		{"empty user", "@host.example.com:22", "", "", true},
		{"empty string", "", "", "", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			user, host, err := parseSFTPEndpoint(tt.endpoint)
			if tt.wantErr {
				if err == nil {
					t.Fatal("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if user != tt.wantUser {
				t.Errorf("user = %q, want %q", user, tt.wantUser)
			}
			if host != tt.wantHost {
				t.Errorf("host = %q, want %q", host, tt.wantHost)
			}
		})
	}
}

func TestBuildSSHAuthMethods(t *testing.T) {
	t.Run("password only", func(t *testing.T) {
		methods, err := buildSSHAuthMethods(&Config{
			SFTPPassword: "secret",
		})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(methods) == 0 {
			t.Fatal("expected at least one auth method")
		}
	})

	t.Run("key file", func(t *testing.T) {
		// Create a temp SSH key for testing
		tmpDir := t.TempDir()
		keyPath := filepath.Join(tmpDir, "test_key")

		// Generate a minimal test key using ssh-keygen would require exec,
		// so we test with a non-existent path to verify error handling
		_, err := buildSSHAuthMethods(&Config{
			SFTPKeyPath: keyPath,
		})
		if err == nil {
			t.Fatal("expected error for nonexistent key")
		}
		if !strings.Contains(err.Error(), "failed to read SSH key") {
			t.Errorf("error = %q, want containing 'failed to read SSH key'", err.Error())
		}
	})

	t.Run("no methods", func(t *testing.T) {
		// Unset SSH_AUTH_SOCK to ensure agent doesn't provide a method
		oldSock := os.Getenv("SSH_AUTH_SOCK")
		os.Unsetenv("SSH_AUTH_SOCK")
		defer func() {
			if oldSock != "" {
				os.Setenv("SSH_AUTH_SOCK", oldSock)
			}
		}()

		// Also make sure no default keys are found
		oldHome := os.Getenv("HOME")
		os.Setenv("HOME", t.TempDir())
		defer os.Setenv("HOME", oldHome)

		_, err := buildSSHAuthMethods(&Config{})
		if err == nil {
			t.Fatal("expected error for no auth methods")
		}
		if !strings.Contains(err.Error(), "no SSH key") {
			t.Errorf("error = %q, want containing 'no SSH key'", err.Error())
		}
	})
}

func TestBuildHostKeyCallback(t *testing.T) {
	t.Run("insecure mode", func(t *testing.T) {
		cb, err := buildHostKeyCallback(&Config{SFTPInsecure: true})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if cb == nil {
			t.Fatal("callback is nil")
		}
	})

	t.Run("missing known_hosts", func(t *testing.T) {
		_, err := buildHostKeyCallback(&Config{
			SFTPKnownHostsPath: "/nonexistent/known_hosts",
		})
		if err == nil {
			t.Fatal("expected error for missing known_hosts")
		}
		if !strings.Contains(err.Error(), "known_hosts file not found") {
			t.Errorf("error = %q, want containing 'known_hosts file not found'", err.Error())
		}
	})

	t.Run("explicit known_hosts path", func(t *testing.T) {
		// Create a temp known_hosts file
		tmpDir := t.TempDir()
		khPath := filepath.Join(tmpDir, "known_hosts")
		if err := os.WriteFile(khPath, []byte(""), 0644); err != nil {
			t.Fatalf("write known_hosts: %v", err)
		}

		cb, err := buildHostKeyCallback(&Config{
			SFTPKnownHostsPath: khPath,
		})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if cb == nil {
			t.Fatal("callback is nil")
		}
	})
}
