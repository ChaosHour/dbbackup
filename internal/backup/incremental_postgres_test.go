package backup

import (
	"os"
	"testing"
	"time"

	"dbbackup/internal/logger"
)

// fakeFileInfo implements os.FileInfo for testing shouldSkipFile
type fakeFileInfo struct {
	name    string
	mode    os.FileMode
	size    int64
	modTime time.Time
	isDir   bool
}

func (f fakeFileInfo) Name() string      { return f.name }
func (f fakeFileInfo) Size() int64        { return f.size }
func (f fakeFileInfo) Mode() os.FileMode  { return f.mode }
func (f fakeFileInfo) ModTime() time.Time { return f.modTime }
func (f fakeFileInfo) IsDir() bool        { return f.isDir }
func (f fakeFileInfo) Sys() interface{}   { return nil }

func TestShouldSkipFile(t *testing.T) {
	engine := NewPostgresIncrementalEngine(logger.NewNullLogger())

	tests := []struct {
		name     string
		path     string
		info     fakeFileInfo
		wantSkip bool
	}{
		{
			name:     "tmp file",
			path:     "/data/base/123/temp.tmp",
			info:     fakeFileInfo{name: "temp.tmp"},
			wantSkip: true,
		},
		{
			name:     "lock file",
			path:     "/data/pg_global/something.lock",
			info:     fakeFileInfo{name: "something.lock"},
			wantSkip: true,
		},
		{
			name:     "postmaster.pid",
			path:     "/data/postmaster.pid",
			info:     fakeFileInfo{name: "postmaster.pid"},
			wantSkip: true,
		},
		{
			name:     "postmaster.opts",
			path:     "/data/postmaster.opts",
			info:     fakeFileInfo{name: "postmaster.opts"},
			wantSkip: true,
		},
		{
			name:     "pg_wal directory",
			path:     "/data/pg_wal/000000010000000000000001",
			info:     fakeFileInfo{name: "000000010000000000000001"},
			wantSkip: true,
		},
		{
			name:     "pg_replslot",
			path:     "/data/pg_replslot/my_slot/state",
			info:     fakeFileInfo{name: "state"},
			wantSkip: true,
		},
		{
			name:     "unix socket",
			path:     "/data/.s.PGSQL.5432",
			info:     fakeFileInfo{name: ".s.PGSQL.5432", mode: os.ModeSocket},
			wantSkip: true,
		},
		{
			name:     "regular data file",
			path:     "/data/base/16384/12345",
			info:     fakeFileInfo{name: "12345"},
			wantSkip: false,
		},
		{
			name:     "pg_xlog directory (old WAL)",
			path:     "/data/pg_xlog/000000010000000000000002",
			info:     fakeFileInfo{name: "000000010000000000000002"},
			wantSkip: true,
		},
		{
			name:     "config file not skipped",
			path:     "/data/postgresql.conf",
			info:     fakeFileInfo{name: "postgresql.conf"},
			wantSkip: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := engine.shouldSkipFile(tt.path, tt.info)
			if got != tt.wantSkip {
				t.Errorf("shouldSkipFile(%q, %q) = %v, want %v", tt.path, tt.info.name, got, tt.wantSkip)
			}
		})
	}
}
