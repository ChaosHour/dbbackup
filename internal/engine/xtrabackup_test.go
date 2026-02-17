package engine

import (
	"context"
	"os"
	"path/filepath"
	"testing"
)

func TestXtraBackupEngineName(t *testing.T) {
	engine := NewXtraBackupEngine(nil, nil, nil)
	if engine.Name() != "xtrabackup" {
		t.Errorf("expected name 'xtrabackup', got %s", engine.Name())
	}
}

func TestXtraBackupEngineDescription(t *testing.T) {
	engine := NewXtraBackupEngine(nil, nil, nil)
	desc := engine.Description()
	if desc == "" {
		t.Error("description should not be empty")
	}
}

func TestXtraBackupEngineCapabilities(t *testing.T) {
	engine := NewXtraBackupEngine(nil, nil, nil)

	if !engine.SupportsRestore() {
		t.Error("xtrabackup should support restore")
	}
	if !engine.SupportsIncremental() {
		t.Error("xtrabackup should support incremental backups")
	}
	if !engine.SupportsStreaming() {
		t.Error("xtrabackup should support streaming")
	}
}

func TestXtraBackupDefaultConfig(t *testing.T) {
	engine := NewXtraBackupEngine(nil, nil, nil)

	if engine.config.Parallel != 4 {
		t.Errorf("expected default parallel=4, got %d", engine.config.Parallel)
	}
	if engine.config.CompressFormat != "gzip" {
		t.Errorf("expected default compress format 'gzip', got %s", engine.config.CompressFormat)
	}
	if engine.config.CompressLevel != 6 {
		t.Errorf("expected default compress level 6, got %d", engine.config.CompressLevel)
	}
	if engine.config.StreamFormat != "xbstream" {
		t.Errorf("expected default stream format 'xbstream', got %s", engine.config.StreamFormat)
	}
	if engine.config.UseMemory != "1G" {
		t.Errorf("expected default use-memory '1G', got %s", engine.config.UseMemory)
	}
}

func TestXtraBackupCustomConfig(t *testing.T) {
	cfg := &XtraBackupConfig{
		Host:           "localhost",
		Port:           3306,
		User:           "backup_user",
		Password:       "secret",
		Parallel:       8,
		UseMemory:      "2G",
		CompressFormat: "zstd",
		CompressLevel:  3,
		NoLock:         true,
		SlaveInfo:      true,
	}

	engine := NewXtraBackupEngine(nil, cfg, nil)

	if engine.config.Host != "localhost" {
		t.Errorf("expected host 'localhost', got %s", engine.config.Host)
	}
	if engine.config.Parallel != 8 {
		t.Errorf("expected parallel=8, got %d", engine.config.Parallel)
	}
	if !engine.config.NoLock {
		t.Error("expected no-lock to be true")
	}
	if !engine.config.SlaveInfo {
		t.Error("expected slave-info to be true")
	}
}

func TestXtraBackupBuildBackupArgs(t *testing.T) {
	cfg := &XtraBackupConfig{
		Host:     "testhost",
		Port:     3307,
		User:     "root",
		Password: "pass",
		Parallel: 4,
		NoLock:   true,
	}

	engine := NewXtraBackupEngine(nil, cfg, nil)

	opts := &BackupOptions{
		Database:  "testdb",
		OutputDir: "/backup",
	}

	args := engine.buildBackupArgs("/backup/target", opts, false)

	// Check essential args are present
	found := make(map[string]bool)
	for _, arg := range args {
		found[arg] = true
	}

	if !found["--backup"] {
		t.Error("expected --backup in args")
	}
	if !found["--target-dir=/backup/target"] {
		t.Error("expected --target-dir=/backup/target in args")
	}
	if !found["--host=testhost"] {
		t.Error("expected --host=testhost in args")
	}
	if !found["--port=3307"] {
		t.Error("expected --port=3307 in args")
	}
	if !found["--user=root"] {
		t.Error("expected --user=root in args")
	}
	if !found["--password=pass"] {
		t.Error("expected --password=pass in args")
	}
	if !found["--databases=testdb"] {
		t.Error("expected --databases=testdb in args")
	}
	if !found["--no-lock"] {
		t.Error("expected --no-lock in args")
	}
	if !found["--parallel=4"] {
		t.Error("expected --parallel=4 in args")
	}
}

func TestXtraBackupBuildIncrementalArgs(t *testing.T) {
	cfg := &XtraBackupConfig{
		Host:               "localhost",
		Port:               3306,
		User:               "root",
		IncrementalBasedir: "/backup/base",
	}

	engine := NewXtraBackupEngine(nil, cfg, nil)

	opts := &BackupOptions{
		Database:  "testdb",
		OutputDir: "/backup",
	}

	args := engine.buildBackupArgs("/backup/incr", opts, true)

	found := make(map[string]bool)
	for _, arg := range args {
		found[arg] = true
	}

	if !found["--incremental-basedir=/backup/base"] {
		t.Error("expected --incremental-basedir=/backup/base in incremental args")
	}
}

func TestXtraBackupBuildStreamingArgs(t *testing.T) {
	cfg := &XtraBackupConfig{
		Host:         "localhost",
		Port:         3306,
		User:         "root",
		StreamFormat: "xbstream",
		Parallel:     2,
	}

	engine := NewXtraBackupEngine(nil, cfg, nil)

	opts := &BackupOptions{
		Database: "testdb",
	}

	args := engine.buildStreamingArgs(opts)

	found := make(map[string]bool)
	for _, arg := range args {
		found[arg] = true
	}

	if !found["--backup"] {
		t.Error("expected --backup in streaming args")
	}
	if !found["--stream=xbstream"] {
		t.Error("expected --stream=xbstream in streaming args")
	}
	if !found["--parallel=2"] {
		t.Error("expected --parallel=2 in streaming args")
	}
}

func TestXtraBackupBuildEncryptionArgs(t *testing.T) {
	cfg := &XtraBackupConfig{
		Host:              "localhost",
		Port:              3306,
		User:              "root",
		EncryptionMethod:  "AES256",
		EncryptionKeyFile: "/etc/xtrabackup.key",
	}

	engine := NewXtraBackupEngine(nil, cfg, nil)

	opts := &BackupOptions{
		OutputDir: "/backup",
	}

	args := engine.buildBackupArgs("/backup/target", opts, false)

	found := make(map[string]bool)
	for _, arg := range args {
		found[arg] = true
	}

	if !found["--encrypt=AES256"] {
		t.Error("expected --encrypt=AES256 in args")
	}
	if !found["--encrypt-key-file=/etc/xtrabackup.key"] {
		t.Error("expected --encrypt-key-file in args")
	}
}

func TestXtraBackupBuildGaleraArgs(t *testing.T) {
	cfg := &XtraBackupConfig{
		Host:       "localhost",
		Port:       3306,
		User:       "root",
		SafeSlave:  true,
		SlaveInfo:  true,
		GaleraInfo: true,
	}

	engine := NewXtraBackupEngine(nil, cfg, nil)

	opts := &BackupOptions{
		OutputDir: "/backup",
	}

	args := engine.buildBackupArgs("/backup/target", opts, false)

	found := make(map[string]bool)
	for _, arg := range args {
		found[arg] = true
	}

	if !found["--safe-slave-backup"] {
		t.Error("expected --safe-slave-backup in args")
	}
	if !found["--slave-info"] {
		t.Error("expected --slave-info in args")
	}
	if !found["--galera-info"] {
		t.Error("expected --galera-info in args")
	}
}

func TestXtraBackupCheckAvailabilityNoBinary(t *testing.T) {
	// Test with no xtrabackup/mariabackup in PATH
	cfg := &XtraBackupConfig{
		BinaryPath:      "/nonexistent/xtrabackup",
		MariaBackupPath: "/nonexistent/mariabackup",
	}

	engine := NewXtraBackupEngine(nil, cfg, nil)

	ctx := context.Background()
	result, err := engine.CheckAvailability(ctx)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if result.Available {
		t.Error("expected engine to be unavailable with nonexistent binary")
	}
}

func TestXtraBackupParseBinlogInfo(t *testing.T) {
	// Create a temporary directory with fake xtrabackup_binlog_info
	tmpDir := t.TempDir()

	// Test with binlog + GTID
	infoContent := "mysql-bin.000003\t73425\tuuid1:1-100,uuid2:1-50"
	if err := os.WriteFile(filepath.Join(tmpDir, "xtrabackup_binlog_info"), []byte(infoContent), 0644); err != nil {
		t.Fatal(err)
	}

	engine := NewXtraBackupEngine(nil, nil, nil)
	file, pos, gtid := engine.parseBinlogInfo(tmpDir)

	if file != "mysql-bin.000003" {
		t.Errorf("expected binlog file 'mysql-bin.000003', got %s", file)
	}
	if pos != 73425 {
		t.Errorf("expected binlog pos 73425, got %d", pos)
	}
	if gtid != "uuid1:1-100,uuid2:1-50" {
		t.Errorf("expected GTID set, got %s", gtid)
	}
}

func TestXtraBackupParseBinlogInfoNoGTID(t *testing.T) {
	tmpDir := t.TempDir()

	infoContent := "mysql-bin.000001\t12345"
	if err := os.WriteFile(filepath.Join(tmpDir, "xtrabackup_binlog_info"), []byte(infoContent), 0644); err != nil {
		t.Fatal(err)
	}

	engine := NewXtraBackupEngine(nil, nil, nil)
	file, pos, gtid := engine.parseBinlogInfo(tmpDir)

	if file != "mysql-bin.000001" {
		t.Errorf("expected binlog file 'mysql-bin.000001', got %s", file)
	}
	if pos != 12345 {
		t.Errorf("expected binlog pos 12345, got %d", pos)
	}
	if gtid != "" {
		t.Errorf("expected empty GTID, got %s", gtid)
	}
}

func TestXtraBackupParseBinlogInfoMissing(t *testing.T) {
	tmpDir := t.TempDir()

	engine := NewXtraBackupEngine(nil, nil, nil)
	file, pos, gtid := engine.parseBinlogInfo(tmpDir)

	if file != "" || pos != 0 || gtid != "" {
		t.Error("expected empty results for missing binlog info file")
	}
}

func TestXtraBackupParseCheckpoints(t *testing.T) {
	tmpDir := t.TempDir()

	checkpointContent := `backup_type = full-backuped
from_lsn = 0
to_lsn = 123456789
last_lsn = 123456800
compact = 0
recover_binlog_info = 0
flushed_lsn = 123456789`

	if err := os.WriteFile(filepath.Join(tmpDir, "xtrabackup_checkpoints"), []byte(checkpointContent), 0644); err != nil {
		t.Fatal(err)
	}

	engine := NewXtraBackupEngine(nil, nil, nil)
	info := engine.parseCheckpoints(tmpDir)

	if info["backup_type"] != "full-backuped" {
		t.Errorf("expected backup_type 'full-backuped', got %s", info["backup_type"])
	}
	if info["to_lsn"] != "123456789" {
		t.Errorf("expected to_lsn '123456789', got %s", info["to_lsn"])
	}
	if info["from_lsn"] != "0" {
		t.Errorf("expected from_lsn '0', got %s", info["from_lsn"])
	}
	if info["compact"] != "0" {
		t.Errorf("expected compact '0', got %s", info["compact"])
	}
}

func TestXtraBackupParseCheckpointsMissing(t *testing.T) {
	tmpDir := t.TempDir()

	engine := NewXtraBackupEngine(nil, nil, nil)
	info := engine.parseCheckpoints(tmpDir)

	if len(info) != 0 {
		t.Errorf("expected empty map for missing checkpoints file, got %d entries", len(info))
	}
}

func TestXtraBackupParseProgressLine(t *testing.T) {
	engine := NewXtraBackupEngine(nil, nil, nil)

	tests := []struct {
		name     string
		line     string
		hasMatch bool
		stage    string
	}{
		{
			name:     "compressing line",
			line:     "Compressing and streaming ./ibdata1 ... done",
			hasMatch: true,
			stage:    "BACKUP",
		},
		{
			name:     "streaming line",
			line:     "[01] ...streaming ./ibdata1",
			hasMatch: true,
			stage:    "BACKUP",
		},
		{
			name:     "copying line",
			line:     "Copying ./test/table.ibd to /backup/test/table.ibd",
			hasMatch: true,
			stage:    "BACKUP",
		},
		{
			name:     "lsn progress",
			line:     ">> log scanned up to (123456789)",
			hasMatch: true,
			stage:    "BACKUP",
		},
		{
			name:     "unrelated line",
			line:     "xtrabackup: recognized server arguments: --datadir",
			hasMatch: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			progress := engine.parseProgressLine(tt.line)
			if tt.hasMatch && progress == nil {
				t.Error("expected progress match")
			}
			if !tt.hasMatch && progress != nil {
				t.Error("expected no progress match")
			}
			if tt.hasMatch && progress != nil && progress.Stage != tt.stage {
				t.Errorf("expected stage %s, got %s", tt.stage, progress.Stage)
			}
		})
	}
}

func TestXtraBackupExtraArgs(t *testing.T) {
	cfg := &XtraBackupConfig{
		Host:      "localhost",
		Port:      3306,
		User:      "root",
		ExtraArgs: []string{"--history", "--rsync"},
	}

	engine := NewXtraBackupEngine(nil, cfg, nil)

	opts := &BackupOptions{
		OutputDir: "/backup",
	}

	args := engine.buildBackupArgs("/backup/target", opts, false)

	found := make(map[string]bool)
	for _, arg := range args {
		found[arg] = true
	}

	if !found["--history"] {
		t.Error("expected --history from extra args")
	}
	if !found["--rsync"] {
		t.Error("expected --rsync from extra args")
	}
}

func TestXtraBackupRegistration(t *testing.T) {
	registry := NewRegistry()
	engine := NewXtraBackupEngine(nil, nil, nil)

	registry.Register(engine)

	got, err := registry.Get("xtrabackup")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got.Name() != "xtrabackup" {
		t.Errorf("expected name 'xtrabackup', got %s", got.Name())
	}
	if !got.SupportsIncremental() {
		t.Error("registered engine should support incremental")
	}
	if !got.SupportsStreaming() {
		t.Error("registered engine should support streaming")
	}
}

func TestXtraBackupCompressAndExtract(t *testing.T) {
	engine := NewXtraBackupEngine(nil, nil, nil)
	ctx := context.Background()

	// Create source directory with test files
	srcDir := t.TempDir()
	if err := os.WriteFile(filepath.Join(srcDir, "ibdata1"), []byte("fake innodb data file"), 0644); err != nil {
		t.Fatal(err)
	}
	subDir := filepath.Join(srcDir, "testdb")
	if err := os.MkdirAll(subDir, 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(subDir, "table.ibd"), []byte("fake table data"), 0644); err != nil {
		t.Fatal(err)
	}

	// Compress
	tarFile := filepath.Join(t.TempDir(), "backup.tar.gz")
	if err := engine.compressBackup(ctx, srcDir, tarFile, nil); err != nil {
		t.Fatalf("compress failed: %v", err)
	}

	// Verify tar.gz was created
	info, err := os.Stat(tarFile)
	if err != nil {
		t.Fatalf("tar.gz file not created: %v", err)
	}
	if info.Size() == 0 {
		t.Error("tar.gz file is empty")
	}

	// Extract to a new directory
	extractDir := filepath.Join(t.TempDir(), "extracted")
	if err := engine.extractBackup(ctx, tarFile, extractDir); err != nil {
		t.Fatalf("extract failed: %v", err)
	}

	// Verify extracted files
	if _, err := os.Stat(filepath.Join(extractDir, "ibdata1")); err != nil {
		t.Error("ibdata1 not extracted")
	}
	if _, err := os.Stat(filepath.Join(extractDir, "testdb", "table.ibd")); err != nil {
		t.Error("testdb/table.ibd not extracted")
	}

	// Verify content
	data, err := os.ReadFile(filepath.Join(extractDir, "ibdata1"))
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != "fake innodb data file" {
		t.Errorf("expected 'fake innodb data file', got '%s'", string(data))
	}
}

// Benchmark for arg building (hot path during backup)
func BenchmarkXtraBackupBuildArgs(b *testing.B) {
	cfg := &XtraBackupConfig{
		Host:     "localhost",
		Port:     3306,
		User:     "root",
		Password: "pass",
		Parallel: 4,
		NoLock:   true,
	}
	engine := NewXtraBackupEngine(nil, cfg, nil)
	opts := &BackupOptions{
		Database:  "testdb",
		OutputDir: "/backup",
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		engine.buildBackupArgs("/backup/target", opts, false)
	}
}
