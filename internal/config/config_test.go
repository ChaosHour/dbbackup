package config

import (
	"os"
	"testing"
)

func TestNew(t *testing.T) {
	cfg := New()
	if cfg == nil {
		t.Fatal("expected non-nil config")
	}

	// Check defaults
	if cfg.Host == "" {
		t.Error("expected non-empty host")
	}
	if cfg.Port == 0 {
		t.Error("expected non-zero port")
	}
	if cfg.User == "" {
		t.Error("expected non-empty user")
	}
	if cfg.DatabaseType != "postgres" && cfg.DatabaseType != "mysql" {
		t.Errorf("expected valid database type, got %q", cfg.DatabaseType)
	}
}

func TestIsPostgreSQL(t *testing.T) {
	tests := []struct {
		dbType   string
		expected bool
	}{
		{"postgres", true},
		{"mysql", false},
		{"mariadb", false},
		{"", false},
	}

	for _, tt := range tests {
		t.Run(tt.dbType, func(t *testing.T) {
			cfg := &Config{DatabaseType: tt.dbType}
			if got := cfg.IsPostgreSQL(); got != tt.expected {
				t.Errorf("IsPostgreSQL() = %v, want %v", got, tt.expected)
			}
		})
	}
}

func TestIsMySQL(t *testing.T) {
	tests := []struct {
		dbType   string
		expected bool
	}{
		{"mysql", true},
		{"mariadb", true},
		{"postgres", false},
		{"", false},
	}

	for _, tt := range tests {
		t.Run(tt.dbType, func(t *testing.T) {
			cfg := &Config{DatabaseType: tt.dbType}
			if got := cfg.IsMySQL(); got != tt.expected {
				t.Errorf("IsMySQL() = %v, want %v", got, tt.expected)
			}
		})
	}
}

func TestSetDatabaseType(t *testing.T) {
	tests := []struct {
		input       string
		expected    string
		shouldError bool
	}{
		{"postgres", "postgres", false},
		{"postgresql", "postgres", false},
		{"POSTGRES", "postgres", false},
		{"mysql", "mysql", false},
		{"MYSQL", "mysql", false},
		{"mariadb", "mariadb", false},
		{"invalid", "", true},
		{"", "", true},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			cfg := &Config{Port: 0}
			err := cfg.SetDatabaseType(tt.input)

			if tt.shouldError {
				if err == nil {
					t.Error("expected error, got nil")
				}
			} else {
				if err != nil {
					t.Errorf("unexpected error: %v", err)
				}
				if cfg.DatabaseType != tt.expected {
					t.Errorf("DatabaseType = %q, want %q", cfg.DatabaseType, tt.expected)
				}
			}
		})
	}
}

func TestSetDatabaseTypePortDefaults(t *testing.T) {
	cfg := &Config{Port: 0}
	_ = cfg.SetDatabaseType("postgres")
	if cfg.Port != 5432 {
		t.Errorf("expected PostgreSQL default port 5432, got %d", cfg.Port)
	}

	cfg = &Config{Port: 0}
	_ = cfg.SetDatabaseType("mysql")
	if cfg.Port != 3306 {
		t.Errorf("expected MySQL default port 3306, got %d", cfg.Port)
	}
}

func TestGetEnvString(t *testing.T) {
	os.Setenv("TEST_CONFIG_VAR", "test_value")
	defer os.Unsetenv("TEST_CONFIG_VAR")

	if got := getEnvString("TEST_CONFIG_VAR", "default"); got != "test_value" {
		t.Errorf("getEnvString() = %q, want %q", got, "test_value")
	}

	if got := getEnvString("NONEXISTENT_VAR", "default"); got != "default" {
		t.Errorf("getEnvString() = %q, want %q", got, "default")
	}
}

func TestGetEnvInt(t *testing.T) {
	os.Setenv("TEST_INT_VAR", "42")
	defer os.Unsetenv("TEST_INT_VAR")

	if got := getEnvInt("TEST_INT_VAR", 0); got != 42 {
		t.Errorf("getEnvInt() = %d, want %d", got, 42)
	}

	os.Setenv("TEST_INT_VAR", "invalid")
	if got := getEnvInt("TEST_INT_VAR", 10); got != 10 {
		t.Errorf("getEnvInt() with invalid = %d, want %d", got, 10)
	}

	if got := getEnvInt("NONEXISTENT_INT_VAR", 99); got != 99 {
		t.Errorf("getEnvInt() nonexistent = %d, want %d", got, 99)
	}
}

func TestGetEnvBool(t *testing.T) {
	tests := []struct {
		envValue string
		expected bool
	}{
		{"true", true},
		{"TRUE", true},
		{"1", true},
		{"false", false},
		{"FALSE", false},
		{"0", false},
	}

	for _, tt := range tests {
		t.Run(tt.envValue, func(t *testing.T) {
			os.Setenv("TEST_BOOL_VAR", tt.envValue)
			defer os.Unsetenv("TEST_BOOL_VAR")

			if got := getEnvBool("TEST_BOOL_VAR", false); got != tt.expected {
				t.Errorf("getEnvBool(%q) = %v, want %v", tt.envValue, got, tt.expected)
			}
		})
	}
}

func TestCanonicalDatabaseType(t *testing.T) {
	tests := []struct {
		input    string
		expected string
		ok       bool
	}{
		{"postgres", "postgres", true},
		{"postgresql", "postgres", true},
		{"pg", "postgres", true},
		{"POSTGRES", "postgres", true},
		{"mysql", "mysql", true},
		{"MYSQL", "mysql", true},
		{"mariadb", "mariadb", true},
		{"maria", "mariadb", true},
		{"invalid", "", false},
		{"", "", false},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got, ok := canonicalDatabaseType(tt.input)
			if ok != tt.ok {
				t.Errorf("canonicalDatabaseType(%q) ok = %v, want %v", tt.input, ok, tt.ok)
			}
			if got != tt.expected {
				t.Errorf("canonicalDatabaseType(%q) = %q, want %q", tt.input, got, tt.expected)
			}
		})
	}
}

func TestDisplayDatabaseType(t *testing.T) {
	tests := []struct {
		dbType   string
		expected string
	}{
		{"postgres", "PostgreSQL"},
		{"mysql", "MySQL"},
		{"mariadb", "MariaDB"},
		{"unknown", "unknown"},
	}

	for _, tt := range tests {
		t.Run(tt.dbType, func(t *testing.T) {
			cfg := &Config{DatabaseType: tt.dbType}
			if got := cfg.DisplayDatabaseType(); got != tt.expected {
				t.Errorf("DisplayDatabaseType() = %q, want %q", got, tt.expected)
			}
		})
	}
}

func TestConfigError(t *testing.T) {
	err := &ConfigError{
		Field:   "port",
		Value:   "invalid",
		Message: "must be a valid port number",
	}

	errStr := err.Error()
	if errStr == "" {
		t.Error("expected non-empty error string")
	}
}

func TestGetCurrentOSUser(t *testing.T) {
	user := GetCurrentOSUser()
	if user == "" {
		t.Error("expected non-empty user")
	}
}

func TestDefaultPortFor(t *testing.T) {
	if port := defaultPortFor("postgres"); port != 5432 {
		t.Errorf("defaultPortFor(postgres) = %d, want 5432", port)
	}
	if port := defaultPortFor("mysql"); port != 3306 {
		t.Errorf("defaultPortFor(mysql) = %d, want 3306", port)
	}
	if port := defaultPortFor("unknown"); port != 5432 {
		t.Errorf("defaultPortFor(unknown) = %d, want 5432 (default)", port)
	}
}
