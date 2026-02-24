package database

import (
	"strings"
	"testing"

	"dbbackup/internal/config"
	"dbbackup/internal/logger"
)

// --- helpers ---

func newTestPostgreSQL(cfg *config.Config) *PostgreSQL {
	return NewPostgreSQL(cfg, logger.NewNullLogger())
}

func newTestMySQL(cfg *config.Config) *MySQL {
	return NewMySQL(cfg, logger.NewNullLogger())
}

// =============================================================================
// New()
// =============================================================================

func TestNew(t *testing.T) {
	tests := []struct {
		name    string
		dbType  string
		wantErr bool
		wantPG  bool
	}{
		{"postgres returns PostgreSQL", "postgres", false, true},
		{"mysql returns MySQL", "mysql", false, false},
		{"unknown errors", "oracle", true, false},
		{"empty errors", "", true, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := &config.Config{DatabaseType: tt.dbType}
			db, err := New(cfg, logger.NewNullLogger())
			if tt.wantErr {
				if err == nil {
					t.Fatal("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if tt.wantPG {
				if _, ok := db.(*PostgreSQL); !ok {
					t.Fatalf("expected *PostgreSQL, got %T", db)
				}
			} else {
				if _, ok := db.(*MySQL); !ok {
					t.Fatalf("expected *MySQL, got %T", db)
				}
			}
		})
	}
}

// =============================================================================
// PostgreSQL — BuildBackupCommand
// =============================================================================

func TestPG_BuildBackupCommand(t *testing.T) {
	tests := []struct {
		name     string
		cfg      *config.Config
		db       string
		outFile  string
		opts     BackupOptions
		wantAll  []string // all of these must appear
		wantNone []string // none of these must appear
	}{
		{
			name:    "socket path",
			cfg:     &config.Config{Host: "/var/run/postgresql", User: "pg"},
			db:      "mydb",
			outFile: "/tmp/out.dump",
			opts:    BackupOptions{},
			wantAll: []string{"pg_dump", "-h", "/var/run/postgresql", "-U", "pg", "--format=custom", "--dbname=mydb", "--file=/tmp/out.dump"},
		},
		{
			name:    "remote host with port",
			cfg:     &config.Config{Host: "db.example.com", Port: 5433, User: "admin"},
			db:      "prod",
			outFile: "/tmp/prod.dump",
			opts:    BackupOptions{},
			wantAll: []string{"-h", "db.example.com", "-p", "5433", "--no-password", "-U", "admin"},
		},
		{
			name:    "localhost uses port only",
			cfg:     &config.Config{Host: "localhost", Port: 5432, User: "pg"},
			db:      "test",
			outFile: "/tmp/t.dump",
			opts:    BackupOptions{},
			wantAll:  []string{"-p", "5432"},
			wantNone: []string{"--no-password"},
		},
		{
			name:    "explicit format plain",
			cfg:     &config.Config{Host: "localhost", Port: 5432, User: "pg"},
			db:      "test",
			outFile: "/tmp/t.sql",
			opts:    BackupOptions{Format: "plain"},
			wantAll: []string{"--format=plain"},
		},
		{
			name:    "compression level",
			cfg:     &config.Config{Host: "localhost", Port: 5432, User: "pg"},
			db:      "test",
			outFile: "/tmp/t.dump",
			opts:    BackupOptions{Compression: 6},
			wantAll: []string{"--compress=6"},
		},
		{
			name:     "parallel only for directory format",
			cfg:      &config.Config{Host: "localhost", Port: 5432, User: "pg"},
			db:       "test",
			outFile:  "/tmp/t",
			opts:     BackupOptions{Format: "directory", Parallel: 4},
			wantAll:  []string{"--jobs=4", "--format=directory"},
		},
		{
			name:     "parallel ignored for custom format",
			cfg:      &config.Config{Host: "localhost", Port: 5432, User: "pg"},
			db:       "test",
			outFile:  "/tmp/t.dump",
			opts:     BackupOptions{Format: "custom", Parallel: 4},
			wantNone: []string{"--jobs=4"},
		},
		{
			name:    "schema only",
			cfg:     &config.Config{Host: "localhost", Port: 5432, User: "pg"},
			db:      "test",
			outFile: "/tmp/t.dump",
			opts:    BackupOptions{SchemaOnly: true},
			wantAll: []string{"--schema-only"},
		},
		{
			name:    "data only",
			cfg:     &config.Config{Host: "localhost", Port: 5432, User: "pg"},
			db:      "test",
			outFile: "/tmp/t.dump",
			opts:    BackupOptions{DataOnly: true},
			wantAll: []string{"--data-only"},
		},
		{
			name:    "role",
			cfg:     &config.Config{Host: "localhost", Port: 5432, User: "pg"},
			db:      "test",
			outFile: "/tmp/t.dump",
			opts:    BackupOptions{Role: "backup_role"},
			wantAll: []string{"--role=backup_role"},
		},
		{
			name:    "blobs flag",
			cfg:     &config.Config{Host: "localhost", Port: 5432, User: "pg"},
			db:      "test",
			outFile: "/tmp/t.dump",
			opts:    BackupOptions{Blobs: true},
			wantAll: []string{"--blobs"},
		},
		{
			name:     "plain format no compression omits --file (stdout)",
			cfg:      &config.Config{Host: "localhost", Port: 5432, User: "pg"},
			db:       "test",
			outFile:  "/tmp/t.sql",
			opts:     BackupOptions{Format: "plain", Compression: 0},
			wantNone: []string{"--file="},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pg := newTestPostgreSQL(tt.cfg)
			cmd := pg.BuildBackupCommand(tt.db, tt.outFile, tt.opts)
			joined := strings.Join(cmd, " ")

			for _, w := range tt.wantAll {
				if !strings.Contains(joined, w) {
					t.Errorf("missing %q in command: %s", w, joined)
				}
			}
			for _, w := range tt.wantNone {
				if strings.Contains(joined, w) {
					t.Errorf("unwanted %q found in command: %s", w, joined)
				}
			}
		})
	}
}

// =============================================================================
// PostgreSQL — BuildRestoreCommand
// =============================================================================

func TestPG_BuildRestoreCommand(t *testing.T) {
	tests := []struct {
		name     string
		cfg      *config.Config
		db       string
		inFile   string
		opts     RestoreOptions
		wantAll  []string
		wantNone []string
	}{
		{
			name:    "socket path",
			cfg:     &config.Config{Host: "/var/run/postgresql", User: "pg"},
			db:      "mydb",
			inFile:  "/tmp/in.dump",
			opts:    RestoreOptions{},
			wantAll: []string{"pg_restore", "-h", "/var/run/postgresql", "-U", "pg", "--dbname=mydb", "/tmp/in.dump"},
		},
		{
			name:    "remote host",
			cfg:     &config.Config{Host: "remote.host", Port: 5433, User: "admin"},
			db:      "prod",
			inFile:  "/tmp/prod.dump",
			opts:    RestoreOptions{},
			wantAll: []string{"-h", "remote.host", "--no-password", "-p", "5433"},
		},
		{
			name:    "parallel without single-tx",
			cfg:     &config.Config{Host: "localhost", Port: 5432, User: "pg"},
			db:      "test",
			inFile:  "/tmp/t.dump",
			opts:    RestoreOptions{Parallel: 4},
			wantAll: []string{"--jobs=4"},
		},
		{
			name:     "parallel suppressed with single-tx",
			cfg:      &config.Config{Host: "localhost", Port: 5432, User: "pg"},
			db:       "test",
			inFile:   "/tmp/t.dump",
			opts:     RestoreOptions{Parallel: 4, SingleTransaction: true},
			wantAll:  []string{"--single-transaction"},
			wantNone: []string{"--jobs="},
		},
		{
			name:    "clean and if-exists",
			cfg:     &config.Config{Host: "localhost", Port: 5432, User: "pg"},
			db:      "test",
			inFile:  "/tmp/t.dump",
			opts:    RestoreOptions{Clean: true, IfExists: true},
			wantAll: []string{"--clean", "--if-exists", "--no-data-for-failed-tables"},
		},
		{
			name:     "no-data-for-failed-tables only with clean",
			cfg:      &config.Config{Host: "localhost", Port: 5432, User: "pg"},
			db:       "test",
			inFile:   "/tmp/t.dump",
			opts:     RestoreOptions{Clean: false},
			wantNone: []string{"--no-data-for-failed-tables"},
		},
		{
			name:    "no-owner and no-privileges",
			cfg:     &config.Config{Host: "localhost", Port: 5432, User: "pg"},
			db:      "test",
			inFile:  "/tmp/t.dump",
			opts:    RestoreOptions{NoOwner: true, NoPrivileges: true},
			wantAll: []string{"--no-owner", "--no-privileges"},
		},
		{
			name:    "verbose flag",
			cfg:     &config.Config{Host: "localhost", Port: 5432, User: "pg"},
			db:      "test",
			inFile:  "/tmp/t.dump",
			opts:    RestoreOptions{Verbose: true},
			wantAll: []string{"--verbose"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pg := newTestPostgreSQL(tt.cfg)
			cmd := pg.BuildRestoreCommand(tt.db, tt.inFile, tt.opts)
			joined := strings.Join(cmd, " ")

			for _, w := range tt.wantAll {
				if !strings.Contains(joined, w) {
					t.Errorf("missing %q in command: %s", w, joined)
				}
			}
			for _, w := range tt.wantNone {
				if strings.Contains(joined, w) {
					t.Errorf("unwanted %q found in command: %s", w, joined)
				}
			}
		})
	}
}

// =============================================================================
// PostgreSQL — BuildSampleQuery
// =============================================================================

func TestPG_BuildSampleQuery(t *testing.T) {
	tests := []struct {
		name     string
		strategy SampleStrategy
		want     string
	}{
		{"ratio", SampleStrategy{Type: "ratio", Value: 10}, "row_number"},
		{"percent", SampleStrategy{Type: "percent", Value: 25}, "TABLESAMPLE BERNOULLI(25)"},
		{"count", SampleStrategy{Type: "count", Value: 500}, "LIMIT 500"},
		{"default", SampleStrategy{Type: "unknown"}, "LIMIT 1000"},
	}

	pg := newTestPostgreSQL(&config.Config{Host: "localhost", Port: 5432, User: "pg"})

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			q := pg.BuildSampleQuery("mydb", "mytable", tt.strategy)
			if !strings.Contains(q, tt.want) {
				t.Errorf("query %q does not contain %q", q, tt.want)
			}
		})
	}
}

// =============================================================================
// PostgreSQL — GetPasswordEnvVar
// =============================================================================

func TestPG_GetPasswordEnvVar(t *testing.T) {
	tests := []struct {
		name     string
		password string
		want     string
	}{
		{"with password", "secret", "PGPASSWORD=secret"},
		{"no password", "", ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pg := newTestPostgreSQL(&config.Config{Password: tt.password})
			got := pg.GetPasswordEnvVar()
			if got != tt.want {
				t.Errorf("got %q, want %q", got, tt.want)
			}
		})
	}
}

// =============================================================================
// MySQL — BuildBackupCommand
// =============================================================================

func TestMySQL_BuildBackupCommand(t *testing.T) {
	tests := []struct {
		name     string
		cfg      *config.Config
		db       string
		outFile  string
		opts     BackupOptions
		wantAll  []string
		wantNone []string
	}{
		{
			name:    "explicit socket",
			cfg:     &config.Config{Socket: "/run/mysqld/mysqld.sock", User: "root"},
			db:      "mydb",
			outFile: "/tmp/out.sql",
			opts:    BackupOptions{},
			wantAll: []string{"mysqldump", "-S", "/run/mysqld/mysqld.sock", "-u", "root"},
		},
		{
			name:     "localhost no socket no host",
			cfg:      &config.Config{Host: "localhost", User: "root"},
			db:       "mydb",
			outFile:  "/tmp/out.sql",
			opts:     BackupOptions{},
			wantAll:  []string{"mysqldump", "-u", "root"},
			wantNone: []string{"-h", "-P"},
		},
		{
			name:    "remote host with port",
			cfg:     &config.Config{Host: "db.example.com", Port: 3307, User: "admin"},
			db:      "prod",
			outFile: "/tmp/out.sql",
			opts:    BackupOptions{},
			wantAll: []string{"-h", "db.example.com", "-P", "3307", "-u", "admin"},
		},
		{
			name:    "ssl require",
			cfg:     &config.Config{Host: "remote", Port: 3306, User: "u", SSLMode: "require"},
			db:      "db",
			outFile: "/tmp/out.sql",
			opts:    BackupOptions{},
			wantAll: []string{"--ssl-mode=REQUIRED"},
		},
		{
			name:    "ssl disable",
			cfg:     &config.Config{Host: "remote", Port: 3306, User: "u", SSLMode: "disable"},
			db:      "db",
			outFile: "/tmp/out.sql",
			opts:    BackupOptions{},
			wantAll: []string{"--skip-ssl"},
		},
		{
			name:    "insecure flag",
			cfg:     &config.Config{Host: "remote", Port: 3306, User: "u", Insecure: true},
			db:      "db",
			outFile: "/tmp/out.sql",
			opts:    BackupOptions{},
			wantAll: []string{"--skip-ssl"},
		},
		{
			name: "performance flags",
			cfg: &config.Config{
				Host: "localhost", User: "root",
				MySQLQuickDump: true, MySQLExtendedInsert: true,
				MySQLOrderByPrimary: true, MySQLNetBufferLen: 1048576,
				MySQLMaxPacket: "256M", MySQLDisableKeys: true,
			},
			db:      "db",
			outFile: "/tmp/out.sql",
			opts:    BackupOptions{},
			wantAll: []string{"--quick", "--extended-insert", "--order-by-primary",
				"--net-buffer-length=1048576", "--max-allowed-packet=256M", "--disable-keys"},
		},
		{
			name:    "schema only",
			cfg:     &config.Config{Host: "localhost", User: "root"},
			db:      "db",
			outFile: "/tmp/out.sql",
			opts:    BackupOptions{SchemaOnly: true},
			wantAll: []string{"--no-data"},
		},
		{
			name:    "data only",
			cfg:     &config.Config{Host: "localhost", User: "root"},
			db:      "db",
			outFile: "/tmp/out.sql",
			opts:    BackupOptions{DataOnly: true},
			wantAll: []string{"--no-create-info"},
		},
		{
			name:    "no owner / no privileges",
			cfg:     &config.Config{Host: "localhost", User: "root"},
			db:      "db",
			outFile: "/tmp/out.sql",
			opts:    BackupOptions{NoOwner: true, NoPrivileges: true},
			wantAll: []string{"--skip-add-drop-table"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			my := newTestMySQL(tt.cfg)
			cmd := my.BuildBackupCommand(tt.db, tt.outFile, tt.opts)
			joined := strings.Join(cmd, " ")

			for _, w := range tt.wantAll {
				if !strings.Contains(joined, w) {
					t.Errorf("missing %q in command: %s", w, joined)
				}
			}
			for _, w := range tt.wantNone {
				if strings.Contains(joined, w) {
					t.Errorf("unwanted %q found in command: %s", w, joined)
				}
			}
		})
	}
}

// =============================================================================
// MySQL — BuildRestoreCommand
// =============================================================================

func TestMySQL_BuildRestoreCommand(t *testing.T) {
	tests := []struct {
		name     string
		cfg      *config.Config
		db       string
		inFile   string
		opts     RestoreOptions
		wantAll  []string
		wantNone []string
	}{
		{
			name:    "explicit socket",
			cfg:     &config.Config{Socket: "/run/mysqld/mysqld.sock", User: "root"},
			db:      "mydb",
			inFile:  "/tmp/in.sql",
			opts:    RestoreOptions{},
			wantAll: []string{"mysql", "-S", "/run/mysqld/mysqld.sock", "-u", "root", "mydb"},
		},
		{
			name:    "remote host",
			cfg:     &config.Config{Host: "db.example.com", Port: 3307, User: "admin"},
			db:      "prod",
			inFile:  "/tmp/in.sql",
			opts:    RestoreOptions{},
			wantAll: []string{"-h", "db.example.com", "-P", "3307"},
		},
		{
			name:    "single transaction",
			cfg:     &config.Config{Host: "localhost", User: "root"},
			db:      "db",
			inFile:  "/tmp/in.sql",
			opts:    RestoreOptions{SingleTransaction: true},
			wantAll: []string{"--single-transaction"},
		},
		{
			name:    "fast restore",
			cfg:     &config.Config{Host: "localhost", User: "root", MySQLFastRestore: true},
			db:      "db",
			inFile:  "/tmp/in.sql",
			opts:    RestoreOptions{},
			wantAll: []string{"--init-command=SET foreign_key_checks=0"},
		},
		{
			name:    "max packet",
			cfg:     &config.Config{Host: "localhost", User: "root", MySQLMaxPacket: "512M"},
			db:      "db",
			inFile:  "/tmp/in.sql",
			opts:    RestoreOptions{},
			wantAll: []string{"--max-allowed-packet=512M"},
		},
		{
			name:    "insecure skip ssl",
			cfg:     &config.Config{Host: "localhost", User: "root", Insecure: true},
			db:      "db",
			inFile:  "/tmp/in.sql",
			opts:    RestoreOptions{},
			wantAll: []string{"--skip-ssl"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			my := newTestMySQL(tt.cfg)
			cmd := my.BuildRestoreCommand(tt.db, tt.inFile, tt.opts)
			joined := strings.Join(cmd, " ")

			for _, w := range tt.wantAll {
				if !strings.Contains(joined, w) {
					t.Errorf("missing %q in command: %s", w, joined)
				}
			}
			for _, w := range tt.wantNone {
				if strings.Contains(joined, w) {
					t.Errorf("unwanted %q found in command: %s", w, joined)
				}
			}
		})
	}
}

// =============================================================================
// MySQL — BuildSampleQuery
// =============================================================================

func TestMySQL_BuildSampleQuery(t *testing.T) {
	tests := []struct {
		name     string
		strategy SampleStrategy
		want     string
	}{
		{"ratio", SampleStrategy{Type: "ratio", Value: 5}, "row_number"},
		{"percent", SampleStrategy{Type: "percent", Value: 50}, "RAND()"},
		{"count", SampleStrategy{Type: "count", Value: 100}, "LIMIT 100"},
		{"default", SampleStrategy{Type: ""}, "LIMIT 1000"},
	}

	my := newTestMySQL(&config.Config{Host: "localhost", Port: 3306, User: "root"})

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			q := my.BuildSampleQuery("testdb", "users", tt.strategy)
			if !strings.Contains(q, tt.want) {
				t.Errorf("query %q does not contain %q", q, tt.want)
			}
		})
	}
}

// =============================================================================
// MySQL — GetPasswordEnvVar
// =============================================================================

func TestMySQL_GetPasswordEnvVar(t *testing.T) {
	tests := []struct {
		name     string
		password string
		want     string
	}{
		{"with password", "s3cret", "MYSQL_PWD=s3cret"},
		{"no password", "", ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			my := newTestMySQL(&config.Config{Password: tt.password})
			got := my.GetPasswordEnvVar()
			if got != tt.want {
				t.Errorf("got %q, want %q", got, tt.want)
			}
		})
	}
}

// =============================================================================
// validateIdentifier (PostgreSQL)
// =============================================================================

func TestValidateIdentifier(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		wantErr bool
	}{
		{"valid simple", "my_table", false},
		{"valid underscore start", "_private", false},
		{"valid mixed case", "MyTable", false},
		{"empty", "", true},
		{"too long", strings.Repeat("a", 64), true},
		{"max length ok", strings.Repeat("a", 63), false},
		{"starts with digit", "1table", true},
		{"special chars", "my-table", true},
		{"space", "my table", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateIdentifier(tt.input)
			if tt.wantErr && err == nil {
				t.Error("expected error, got nil")
			}
			if !tt.wantErr && err != nil {
				t.Errorf("unexpected error: %v", err)
			}
		})
	}
}

// =============================================================================
// validateMySQLIdentifier
// =============================================================================

func TestValidateMySQLIdentifier(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		wantErr bool
	}{
		{"valid simple", "users", false},
		{"valid underscore", "_meta", false},
		{"valid mixed", "UserTable", false},
		{"empty", "", true},
		{"too long", strings.Repeat("b", 65), true},
		{"max length ok", strings.Repeat("b", 64), false},
		{"starts with digit", "0col", true},
		{"special chars", "my.table", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateMySQLIdentifier(tt.input)
			if tt.wantErr && err == nil {
				t.Error("expected error, got nil")
			}
			if !tt.wantErr && err != nil {
				t.Errorf("unexpected error: %v", err)
			}
		})
	}
}

// =============================================================================
// sanitizeDSN (PostgreSQL)
// =============================================================================

func TestSanitizeDSN(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  string
	}{
		{
			"URL with password",
			"postgres://admin:secret123@db.host:5432/mydb",
			"postgres://admin:***@db.host:5432/mydb",
		},
		{
			"URL no password",
			"postgres://admin@db.host:5432/mydb",
			"postgres://admin@db.host:5432/mydb",
		},
		{
			"keyword value format",
			"host=db.host port=5432 user=admin password=secret123 dbname=mydb",
			"host=db.host port=5432 user=admin password=*** dbname=mydb",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := sanitizeDSN(tt.input)
			if got != tt.want {
				t.Errorf("got %q, want %q", got, tt.want)
			}
		})
	}
}

// =============================================================================
// sanitizeMySQLDSN
// =============================================================================

func TestSanitizeMySQLDSN(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  string
	}{
		{
			"user:pass@tcp",
			"root:secret@tcp(localhost:3306)/mydb",
			"root:***@tcp(localhost:3306)/mydb",
		},
		{
			"no password",
			"root@tcp(localhost:3306)/mydb",
			"root@tcp(localhost:3306)/mydb",
		},
		{
			"user:pass@unix",
			"admin:pw123@unix(/run/mysqld/mysqld.sock)/testdb",
			"admin:***@unix(/run/mysqld/mysqld.sock)/testdb",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := sanitizeMySQLDSN(tt.input)
			if got != tt.want {
				t.Errorf("got %q, want %q", got, tt.want)
			}
		})
	}
}

// =============================================================================
// getConnectionHint
// =============================================================================

func TestGetConnectionHint(t *testing.T) {
	tests := []struct {
		name           string
		errMsg         string
		host           string
		port           int
		user           string
		passwordSource string
		wantContains   string
	}{
		{
			"auth failed pgpass",
			"password authentication failed for user admin",
			"localhost", 5432, "admin", "pgpass",
			"pgpass",
		},
		{
			"auth failed none",
			"password authentication failed for user admin",
			"localhost", 5432, "admin", "none",
			"No password was provided",
		},
		{
			"auth failed env",
			"password authentication failed for user admin",
			"localhost", 5432, "admin", "env",
			"Verify the password",
		},
		{
			"peer auth",
			"peer authentication failed for user postgres",
			"localhost", 5432, "postgres", "none",
			"peer auth requires OS user",
		},
		{
			"connection refused",
			"connection refused",
			"dbhost", 5432, "pg", "env",
			"not listening",
		},
		{
			"no such host",
			"no such host",
			"bad.host", 5432, "pg", "env",
			"could not be resolved",
		},
		{
			"timeout",
			"connection timed out",
			"slow.host", 5432, "pg", "env",
			"timed out",
		},
		{
			"role missing",
			"role \"newuser\" does not exist",
			"localhost", 5432, "newuser", "env",
			"does not exist",
		},
		{
			"database missing",
			"database \"baddb\" does not exist",
			"localhost", 5432, "pg", "env",
			"target database does not exist",
		},
		{
			"ssl not supported",
			"ssl is not supported by this server",
			"localhost", 5432, "pg", "env",
			"SSL",
		},
		{
			"unknown error returns empty",
			"some random unmatched error",
			"localhost", 5432, "pg", "env",
			"",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := getConnectionHint(tt.errMsg, tt.host, tt.port, tt.user, tt.passwordSource)
			if tt.wantContains == "" {
				if got != "" {
					t.Errorf("expected empty hint, got %q", got)
				}
				return
			}
			if !strings.Contains(got, tt.wantContains) {
				t.Errorf("hint %q does not contain %q", got, tt.wantContains)
			}
		})
	}
}
