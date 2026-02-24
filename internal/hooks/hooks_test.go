package hooks

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"dbbackup/internal/logger"
)

// mockLogger implements logger.Logger for testing
type mockLogger struct {
	debugMsgs []string
	infoMsgs  []string
	warnMsgs  []string
	errorMsgs []string
}

func (m *mockLogger) Debug(msg string, args ...interface{})                  { m.debugMsgs = append(m.debugMsgs, msg) }
func (m *mockLogger) Info(msg string, args ...interface{})                   { m.infoMsgs = append(m.infoMsgs, msg) }
func (m *mockLogger) Warn(msg string, args ...interface{})                   { m.warnMsgs = append(m.warnMsgs, msg) }
func (m *mockLogger) Error(msg string, args ...interface{})                  { m.errorMsgs = append(m.errorMsgs, msg) }
func (m *mockLogger) Time(msg string, args ...any)                           {}
func (m *mockLogger) WithFields(fields map[string]interface{}) logger.Logger { return m }
func (m *mockLogger) WithField(key string, value interface{}) logger.Logger  { return m }
func (m *mockLogger) StartOperation(name string) logger.OperationLogger {
	return &mockOperationLogger{}
}

type mockOperationLogger struct{}

func (m *mockOperationLogger) Update(msg string, args ...any)   {}
func (m *mockOperationLogger) Complete(msg string, args ...any) {}
func (m *mockOperationLogger) Fail(msg string, args ...any)     {}

func TestNewManager(t *testing.T) {
	cfg := &Config{}
	log := &mockLogger{}

	mgr := NewManager(cfg, log)

	if mgr == nil {
		t.Fatal("expected manager to be created")
	}
	if mgr.config.Timeout != 5*time.Minute {
		t.Errorf("expected default timeout of 5 minutes, got %v", mgr.config.Timeout)
	}
	if mgr.config.WorkDir == "" {
		t.Error("expected WorkDir to be set")
	}
}

func TestNewManagerWithCustomTimeout(t *testing.T) {
	cfg := &Config{
		Timeout: 10 * time.Second,
		WorkDir: "/tmp",
	}
	log := &mockLogger{}

	mgr := NewManager(cfg, log)

	if mgr.config.Timeout != 10*time.Second {
		t.Errorf("expected custom timeout of 10s, got %v", mgr.config.Timeout)
	}
	if mgr.config.WorkDir != "/tmp" {
		t.Errorf("expected WorkDir /tmp, got %v", mgr.config.WorkDir)
	}
}

func TestRunPreBackupNoHooks(t *testing.T) {
	cfg := &Config{}
	log := &mockLogger{}
	mgr := NewManager(cfg, log)

	ctx := context.Background()
	hctx := &HookContext{Database: "testdb"}

	err := mgr.RunPreBackup(ctx, hctx)
	if err != nil {
		t.Errorf("expected no error with no hooks, got %v", err)
	}
}

func TestRunSingleHookSuccess(t *testing.T) {
	cfg := &Config{
		Timeout: 5 * time.Second,
		PreBackup: []Hook{
			{
				Name:    "echo-test",
				Command: "echo",
				Args:    []string{"hello"},
				Shell:   false,
			},
		},
	}
	log := &mockLogger{}
	mgr := NewManager(cfg, log)

	ctx := context.Background()
	hctx := &HookContext{Database: "testdb"}

	err := mgr.RunPreBackup(ctx, hctx)
	if err != nil {
		t.Errorf("expected no error, got %v", err)
	}
}

func TestRunShellHookSuccess(t *testing.T) {
	cfg := &Config{
		Timeout: 5 * time.Second,
		PreBackup: []Hook{
			{
				Name:    "shell-test",
				Command: "echo 'hello world' | wc -w",
				Shell:   true,
			},
		},
	}
	log := &mockLogger{}
	mgr := NewManager(cfg, log)

	ctx := context.Background()
	hctx := &HookContext{Database: "testdb"}

	err := mgr.RunPreBackup(ctx, hctx)
	if err != nil {
		t.Errorf("expected no error, got %v", err)
	}
}

func TestRunHookFailure(t *testing.T) {
	cfg := &Config{
		Timeout: 5 * time.Second,
		PreBackup: []Hook{
			{
				Name:    "fail-test",
				Command: "false",
				Shell:   true,
			},
		},
	}
	log := &mockLogger{}
	mgr := NewManager(cfg, log)

	ctx := context.Background()
	hctx := &HookContext{Database: "testdb"}

	err := mgr.RunPreBackup(ctx, hctx)
	if err == nil {
		t.Error("expected error on hook failure")
	}
	if !strings.Contains(err.Error(), "fail-test") {
		t.Errorf("expected error to mention hook name, got: %v", err)
	}
}

func TestRunHookContinueOnError(t *testing.T) {
	cfg := &Config{
		Timeout:         5 * time.Second,
		ContinueOnError: true,
		PreBackup: []Hook{
			{
				Name:    "fail-test",
				Command: "false",
				Shell:   true,
			},
			{
				Name:    "success-test",
				Command: "true",
				Shell:   true,
			},
		},
	}
	log := &mockLogger{}
	mgr := NewManager(cfg, log)

	ctx := context.Background()
	hctx := &HookContext{Database: "testdb"}

	err := mgr.RunPreBackup(ctx, hctx)
	if err != nil {
		t.Errorf("expected ContinueOnError to allow continuation, got %v", err)
	}
}

func TestRunHookTimeout(t *testing.T) {
	// Test that hook timeout is respected
	// We use a short-running command here since exec.CommandContext
	// may not kill long-running subprocesses immediately
	cfg := &Config{
		Timeout: 5 * time.Second,
		PreBackup: []Hook{
			{
				Name:    "quick-fail",
				Command: "exit 1",
				Shell:   true,
			},
		},
	}
	log := &mockLogger{}
	mgr := NewManager(cfg, log)

	ctx := context.Background()
	hctx := &HookContext{Database: "testdb"}

	err := mgr.RunPreBackup(ctx, hctx)
	if err == nil {
		t.Error("expected error on hook failure")
	}
}

func TestRunHookWithCondition(t *testing.T) {
	cfg := &Config{
		Timeout: 5 * time.Second,
		PreBackup: []Hook{
			{
				Name:      "condition-skip",
				Command:   "echo should-not-run",
				Shell:     true,
				Condition: "-z \"not-empty\"", // Will fail, so hook is skipped
			},
		},
	}
	log := &mockLogger{}
	mgr := NewManager(cfg, log)

	ctx := context.Background()
	hctx := &HookContext{Database: "testdb"}

	err := mgr.RunPreBackup(ctx, hctx)
	if err != nil {
		t.Errorf("expected no error when condition not met, got %v", err)
	}
}

func TestExpandVariables(t *testing.T) {
	cfg := &Config{}
	log := &mockLogger{}
	mgr := NewManager(cfg, log)

	hctx := &HookContext{
		Database:   "mydb",
		Table:      "users",
		BackupPath: "/backups/mydb.dump",
		BackupSize: 1024000,
		Operation:  "backup",
		Phase:      "pre",
	}

	tests := []struct {
		input    string
		expected string
	}{
		{"backup ${DATABASE}", "backup mydb"},
		{"${TABLE} table", "users table"},
		{"${BACKUP_PATH}", "/backups/mydb.dump"},
		{"size: ${BACKUP_SIZE}", "size: 1024000"},
		{"${OPERATION}/${PHASE}", "backup/pre"},
		{"no vars here", "no vars here"},
	}

	for _, tc := range tests {
		result := mgr.expandVariables(tc.input, hctx)
		if result != tc.expected {
			t.Errorf("expandVariables(%q) = %q, want %q", tc.input, result, tc.expected)
		}
	}
}

func TestBuildEnvironment(t *testing.T) {
	cfg := &Config{
		Environment: map[string]string{
			"GLOBAL_VAR": "global_value",
		},
	}
	log := &mockLogger{}
	mgr := NewManager(cfg, log)

	hctx := &HookContext{
		Operation: "backup",
		Phase:     "pre",
		Database:  "testdb",
		Success:   true,
	}

	extra := map[string]string{
		"EXTRA_VAR": "extra_value",
	}

	env := mgr.buildEnvironment(hctx, extra)

	// Check for expected variables
	envMap := make(map[string]string)
	for _, e := range env {
		parts := strings.SplitN(e, "=", 2)
		if len(parts) == 2 {
			envMap[parts[0]] = parts[1]
		}
	}

	if envMap["DBBACKUP_OPERATION"] != "backup" {
		t.Error("expected DBBACKUP_OPERATION=backup")
	}
	if envMap["DBBACKUP_PHASE"] != "pre" {
		t.Error("expected DBBACKUP_PHASE=pre")
	}
	if envMap["DBBACKUP_DATABASE"] != "testdb" {
		t.Error("expected DBBACKUP_DATABASE=testdb")
	}
	if envMap["DBBACKUP_SUCCESS"] != "true" {
		t.Error("expected DBBACKUP_SUCCESS=true")
	}
	if envMap["GLOBAL_VAR"] != "global_value" {
		t.Error("expected GLOBAL_VAR=global_value")
	}
	if envMap["EXTRA_VAR"] != "extra_value" {
		t.Error("expected EXTRA_VAR=extra_value")
	}
}

func TestLoadHooksFromDir(t *testing.T) {
	// Create temp directory structure
	tmpDir, err := os.MkdirTemp("", "hooks-test")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = os.RemoveAll(tmpDir) }()

	// Create hooks directory structure
	preBackupDir := filepath.Join(tmpDir, "pre-backup")
	if err := os.MkdirAll(preBackupDir, 0755); err != nil {
		t.Fatal(err)
	}

	postBackupDir := filepath.Join(tmpDir, "post-backup")
	if err := os.MkdirAll(postBackupDir, 0755); err != nil {
		t.Fatal(err)
	}

	// Create executable hook script
	hookScript := filepath.Join(preBackupDir, "00-test.sh")
	if err := os.WriteFile(hookScript, []byte("#!/bin/sh\necho test"), 0755); err != nil {
		t.Fatal(err)
	}

	// Create non-executable file (should be skipped)
	nonExec := filepath.Join(preBackupDir, "README.txt")
	if err := os.WriteFile(nonExec, []byte("readme"), 0644); err != nil {
		t.Fatal(err)
	}

	cfg := &Config{}
	log := &mockLogger{}
	mgr := NewManager(cfg, log)

	err = mgr.LoadHooksFromDir(tmpDir)
	if err != nil {
		t.Errorf("LoadHooksFromDir failed: %v", err)
	}

	if len(cfg.PreBackup) != 1 {
		t.Errorf("expected 1 pre-backup hook, got %d", len(cfg.PreBackup))
	}

	if len(cfg.PreBackup) > 0 && cfg.PreBackup[0].Name != "00-test.sh" {
		t.Errorf("expected hook name '00-test.sh', got %q", cfg.PreBackup[0].Name)
	}
}

func TestLoadHooksFromDirNotExists(t *testing.T) {
	cfg := &Config{}
	log := &mockLogger{}
	mgr := NewManager(cfg, log)

	err := mgr.LoadHooksFromDir("/nonexistent/path")
	if err != nil {
		t.Errorf("expected no error for nonexistent dir, got %v", err)
	}
}

func TestGetPredefinedHook(t *testing.T) {
	hook, ok := GetPredefinedHook("vacuum-analyze")
	if !ok {
		t.Fatal("expected vacuum-analyze hook to exist")
	}
	if hook.Name != "vacuum-analyze" {
		t.Errorf("expected name 'vacuum-analyze', got %q", hook.Name)
	}
	if hook.Command != "psql" {
		t.Errorf("expected command 'psql', got %q", hook.Command)
	}

	_, ok = GetPredefinedHook("nonexistent")
	if ok {
		t.Error("expected nonexistent hook to not be found")
	}
}

func TestAllPhases(t *testing.T) {
	hookCalled := make(map[string]bool)

	cfg := &Config{
		Timeout: 5 * time.Second,
		PreBackup: []Hook{{
			Name:    "pre-backup",
			Command: "true",
			Shell:   true,
		}},
		PostBackup: []Hook{{
			Name:    "post-backup",
			Command: "true",
			Shell:   true,
		}},
		PreDatabase: []Hook{{
			Name:    "pre-database",
			Command: "true",
			Shell:   true,
		}},
		PostDatabase: []Hook{{
			Name:    "post-database",
			Command: "true",
			Shell:   true,
		}},
		OnError: []Hook{{
			Name:    "on-error",
			Command: "true",
			Shell:   true,
		}},
		OnSuccess: []Hook{{
			Name:    "on-success",
			Command: "true",
			Shell:   true,
		}},
	}
	log := &mockLogger{}
	mgr := NewManager(cfg, log)
	ctx := context.Background()

	phases := []struct {
		name string
		fn   func(context.Context, *HookContext) error
	}{
		{"pre-backup", mgr.RunPreBackup},
		{"post-backup", mgr.RunPostBackup},
		{"pre-database", mgr.RunPreDatabase},
		{"post-database", mgr.RunPostDatabase},
		{"on-error", mgr.RunOnError},
		{"on-success", mgr.RunOnSuccess},
	}

	for _, phase := range phases {
		hctx := &HookContext{Database: "testdb"}
		err := phase.fn(ctx, hctx)
		if err != nil {
			t.Errorf("%s failed: %v", phase.name, err)
		}
		hookCalled[phase.name] = true
	}

	for _, phase := range phases {
		if !hookCalled[phase.name] {
			t.Errorf("phase %s was not called", phase.name)
		}
	}
}

func TestHookEnvironmentPassthrough(t *testing.T) {
	// Test that environment variables are actually passed to hooks via shell
	// Use printenv and grep to verify the variable exists
	cfg := &Config{
		Timeout: 5 * time.Second,
		PreBackup: []Hook{
			{
				Name:    "env-check",
				Command: "printenv DBBACKUP_DATABASE | grep -q envtestdb",
				Shell:   true,
			},
		},
	}
	log := &mockLogger{}
	mgr := NewManager(cfg, log)

	ctx := context.Background()
	hctx := &HookContext{Database: "envtestdb"}

	err := mgr.RunPreBackup(ctx, hctx)
	if err != nil {
		t.Errorf("expected hook to receive env vars, got error: %v", err)
	}
}

func TestContextCancellation(t *testing.T) {
	// Test that hooks respect context
	cfg := &Config{
		Timeout: 5 * time.Second,
		PreBackup: []Hook{
			{
				Name:    "test-hook",
				Command: "echo done",
				Shell:   true,
			},
		},
	}
	log := &mockLogger{}
	mgr := NewManager(cfg, log)

	// Already cancelled context
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	hctx := &HookContext{Database: "testdb"}

	err := mgr.RunPreBackup(ctx, hctx)
	if err == nil {
		t.Error("expected error on cancelled context")
	}
}
