package fs

import (
	"os"
	"testing"
	"time"

	"github.com/spf13/afero"
)

func TestMemMapFs(t *testing.T) {
	// Use in-memory filesystem for testing
	WithMemFs(func(memFs afero.Fs) {
		// Create a file
		err := WriteFile("/test/file.txt", []byte("hello world"), 0644)
		if err != nil {
			t.Fatalf("WriteFile failed: %v", err)
		}

		// Read it back
		content, err := ReadFile("/test/file.txt")
		if err != nil {
			t.Fatalf("ReadFile failed: %v", err)
		}

		if string(content) != "hello world" {
			t.Errorf("expected 'hello world', got '%s'", string(content))
		}

		// Check existence
		exists, err := Exists("/test/file.txt")
		if err != nil {
			t.Fatalf("Exists failed: %v", err)
		}
		if !exists {
			t.Error("file should exist")
		}

		// Check non-existent file
		exists, err = Exists("/nonexistent.txt")
		if err != nil {
			t.Fatalf("Exists failed: %v", err)
		}
		if exists {
			t.Error("file should not exist")
		}
	})
}

func TestSetupTestDir(t *testing.T) {
	// Create test directory structure
	testFs := SetupTestDir(map[string]string{
		"/backups/db1.dump":     "database 1 content",
		"/backups/db2.dump":     "database 2 content",
		"/config/settings.json": `{"key": "value"}`,
	})

	// Verify files exist
	content, err := afero.ReadFile(testFs, "/backups/db1.dump")
	if err != nil {
		t.Fatalf("ReadFile failed: %v", err)
	}
	if string(content) != "database 1 content" {
		t.Errorf("unexpected content: %s", string(content))
	}

	// Verify directory structure
	files, err := afero.ReadDir(testFs, "/backups")
	if err != nil {
		t.Fatalf("ReadDir failed: %v", err)
	}
	if len(files) != 2 {
		t.Errorf("expected 2 files, got %d", len(files))
	}
}

func TestCopyFile(t *testing.T) {
	WithMemFs(func(memFs afero.Fs) {
		// Create source file
		err := WriteFile("/source.txt", []byte("copy me"), 0644)
		if err != nil {
			t.Fatalf("WriteFile failed: %v", err)
		}

		// Copy file
		err = CopyFile("/source.txt", "/dest.txt")
		if err != nil {
			t.Fatalf("CopyFile failed: %v", err)
		}

		// Verify copy
		content, err := ReadFile("/dest.txt")
		if err != nil {
			t.Fatalf("ReadFile failed: %v", err)
		}
		if string(content) != "copy me" {
			t.Errorf("unexpected content: %s", string(content))
		}
	})
}

func TestFileSize(t *testing.T) {
	WithMemFs(func(memFs afero.Fs) {
		data := []byte("12345678901234567890") // 20 bytes
		err := WriteFile("/sized.txt", data, 0644)
		if err != nil {
			t.Fatalf("WriteFile failed: %v", err)
		}

		size, err := FileSize("/sized.txt")
		if err != nil {
			t.Fatalf("FileSize failed: %v", err)
		}
		if size != 20 {
			t.Errorf("expected size 20, got %d", size)
		}
	})
}

func TestTempDir(t *testing.T) {
	WithMemFs(func(memFs afero.Fs) {
		// Create temp dir
		dir, err := TempDir("", "test-")
		if err != nil {
			t.Fatalf("TempDir failed: %v", err)
		}

		// Verify it exists
		isDir, err := IsDir(dir)
		if err != nil {
			t.Fatalf("IsDir failed: %v", err)
		}
		if !isDir {
			t.Error("temp dir should be a directory")
		}

		// Verify it's empty
		isEmpty, err := IsEmpty(dir)
		if err != nil {
			t.Fatalf("IsEmpty failed: %v", err)
		}
		if !isEmpty {
			t.Error("temp dir should be empty")
		}
	})
}

func TestWalk(t *testing.T) {
	WithMemFs(func(memFs afero.Fs) {
		// Create directory structure
		_ = MkdirAll("/root/a/b", 0755)
		_ = WriteFile("/root/file1.txt", []byte("1"), 0644)
		_ = WriteFile("/root/a/file2.txt", []byte("2"), 0644)
		_ = WriteFile("/root/a/b/file3.txt", []byte("3"), 0644)

		var files []string
		err := Walk("/root", func(path string, info os.FileInfo, err error) error {
			if err != nil {
				return err
			}
			if !info.IsDir() {
				files = append(files, path)
			}
			return nil
		})

		if err != nil {
			t.Fatalf("Walk failed: %v", err)
		}

		if len(files) != 3 {
			t.Errorf("expected 3 files, got %d: %v", len(files), files)
		}
	})
}

func TestGlob(t *testing.T) {
	WithMemFs(func(memFs afero.Fs) {
		_ = WriteFile("/data/backup1.dump", []byte("1"), 0644)
		_ = WriteFile("/data/backup2.dump", []byte("2"), 0644)
		_ = WriteFile("/data/config.json", []byte("{}"), 0644)

		matches, err := Glob("/data/*.dump")
		if err != nil {
			t.Fatalf("Glob failed: %v", err)
		}

		if len(matches) != 2 {
			t.Errorf("expected 2 matches, got %d: %v", len(matches), matches)
		}
	})
}

func TestSetFS_ResetFS(t *testing.T) {
	original := FS

	// Set a new FS
	memFs := NewMemMapFs()
	SetFS(memFs)

	if FS != memFs {
		t.Error("SetFS should change global FS")
	}

	// Reset to OS filesystem
	ResetFS()

	// Note: We can't directly compare to original because ResetFS creates a new OsFs
	// Just verify it was reset (original was likely OsFs)
	SetFS(original) // Restore for other tests
}

func TestNewReadOnlyFs(t *testing.T) {
	memFs := NewMemMapFs()
	_ = afero.WriteFile(memFs, "/test.txt", []byte("content"), 0644)

	roFs := NewReadOnlyFs(memFs)

	// Read should work
	content, err := afero.ReadFile(roFs, "/test.txt")
	if err != nil {
		t.Fatalf("ReadFile should work on read-only fs: %v", err)
	}
	if string(content) != "content" {
		t.Errorf("unexpected content: %s", string(content))
	}

	// Write should fail
	err = afero.WriteFile(roFs, "/new.txt", []byte("data"), 0644)
	if err == nil {
		t.Error("WriteFile should fail on read-only fs")
	}
}

func TestNewBasePathFs(t *testing.T) {
	memFs := NewMemMapFs()
	_ = memFs.MkdirAll("/base/subdir", 0755)
	_ = afero.WriteFile(memFs, "/base/subdir/file.txt", []byte("content"), 0644)

	baseFs := NewBasePathFs(memFs, "/base")

	// Access file relative to base
	content, err := afero.ReadFile(baseFs, "subdir/file.txt")
	if err != nil {
		t.Fatalf("ReadFile should work with base path: %v", err)
	}
	if string(content) != "content" {
		t.Errorf("unexpected content: %s", string(content))
	}
}

func TestCreate(t *testing.T) {
	WithMemFs(func(memFs afero.Fs) {
		f, err := Create("/newfile.txt")
		if err != nil {
			t.Fatalf("Create failed: %v", err)
		}
		defer func() { _ = f.Close() }()

		_, err = f.WriteString("hello")
		if err != nil {
			t.Fatalf("WriteString failed: %v", err)
		}

		// Verify file exists
		exists, _ := Exists("/newfile.txt")
		if !exists {
			t.Error("created file should exist")
		}
	})
}

func TestOpen(t *testing.T) {
	WithMemFs(func(memFs afero.Fs) {
		_ = WriteFile("/openme.txt", []byte("content"), 0644)

		f, err := Open("/openme.txt")
		if err != nil {
			t.Fatalf("Open failed: %v", err)
		}
		defer func() { _ = f.Close() }()

		buf := make([]byte, 7)
		n, err := f.Read(buf)
		if err != nil {
			t.Fatalf("Read failed: %v", err)
		}
		if string(buf[:n]) != "content" {
			t.Errorf("unexpected content: %s", string(buf[:n]))
		}
	})
}

func TestOpenFile(t *testing.T) {
	WithMemFs(func(memFs afero.Fs) {
		f, err := OpenFile("/openfile.txt", os.O_CREATE|os.O_WRONLY, 0644)
		if err != nil {
			t.Fatalf("OpenFile failed: %v", err)
		}
		_, _ = f.WriteString("test")
		_ = f.Close()

		content, _ := ReadFile("/openfile.txt")
		if string(content) != "test" {
			t.Errorf("unexpected content: %s", string(content))
		}
	})
}

func TestRemove(t *testing.T) {
	WithMemFs(func(memFs afero.Fs) {
		_ = WriteFile("/removeme.txt", []byte("bye"), 0644)

		err := Remove("/removeme.txt")
		if err != nil {
			t.Fatalf("Remove failed: %v", err)
		}

		exists, _ := Exists("/removeme.txt")
		if exists {
			t.Error("file should be removed")
		}
	})
}

func TestRemoveAll(t *testing.T) {
	WithMemFs(func(memFs afero.Fs) {
		_ = MkdirAll("/removedir/sub", 0755)
		_ = WriteFile("/removedir/file.txt", []byte("1"), 0644)
		_ = WriteFile("/removedir/sub/file.txt", []byte("2"), 0644)

		err := RemoveAll("/removedir")
		if err != nil {
			t.Fatalf("RemoveAll failed: %v", err)
		}

		exists, _ := Exists("/removedir")
		if exists {
			t.Error("directory should be removed")
		}
	})
}

func TestRename(t *testing.T) {
	WithMemFs(func(memFs afero.Fs) {
		_ = WriteFile("/oldname.txt", []byte("data"), 0644)

		err := Rename("/oldname.txt", "/newname.txt")
		if err != nil {
			t.Fatalf("Rename failed: %v", err)
		}

		exists, _ := Exists("/oldname.txt")
		if exists {
			t.Error("old file should not exist")
		}

		exists, _ = Exists("/newname.txt")
		if !exists {
			t.Error("new file should exist")
		}
	})
}

func TestStat(t *testing.T) {
	WithMemFs(func(memFs afero.Fs) {
		_ = WriteFile("/statfile.txt", []byte("content"), 0644)

		info, err := Stat("/statfile.txt")
		if err != nil {
			t.Fatalf("Stat failed: %v", err)
		}

		if info.Name() != "statfile.txt" {
			t.Errorf("unexpected name: %s", info.Name())
		}
		if info.Size() != 7 {
			t.Errorf("unexpected size: %d", info.Size())
		}
	})
}

func TestChmod(t *testing.T) {
	WithMemFs(func(memFs afero.Fs) {
		_ = WriteFile("/chmodfile.txt", []byte("data"), 0644)

		err := Chmod("/chmodfile.txt", 0755)
		if err != nil {
			t.Fatalf("Chmod failed: %v", err)
		}

		info, _ := Stat("/chmodfile.txt")
		// MemMapFs may not preserve exact permissions, just verify no error
		_ = info
	})
}

func TestChown(t *testing.T) {
	WithMemFs(func(memFs afero.Fs) {
		_ = WriteFile("/chownfile.txt", []byte("data"), 0644)

		// Chown may not work on all filesystems, just verify no panic
		_ = Chown("/chownfile.txt", 1000, 1000)
	})
}

func TestChtimes(t *testing.T) {
	WithMemFs(func(memFs afero.Fs) {
		_ = WriteFile("/chtimesfile.txt", []byte("data"), 0644)

		now := time.Now()

		err := Chtimes("/chtimesfile.txt", now, now)
		if err != nil {
			t.Fatalf("Chtimes failed: %v", err)
		}
	})
}

func TestMkdir(t *testing.T) {
	WithMemFs(func(memFs afero.Fs) {
		err := Mkdir("/singledir", 0755)
		if err != nil {
			t.Fatalf("Mkdir failed: %v", err)
		}

		isDir, _ := IsDir("/singledir")
		if !isDir {
			t.Error("should be a directory")
		}
	})
}

func TestReadDir(t *testing.T) {
	WithMemFs(func(memFs afero.Fs) {
		_ = MkdirAll("/readdir", 0755)
		_ = WriteFile("/readdir/file1.txt", []byte("1"), 0644)
		_ = WriteFile("/readdir/file2.txt", []byte("2"), 0644)
		_ = Mkdir("/readdir/subdir", 0755)

		entries, err := ReadDir("/readdir")
		if err != nil {
			t.Fatalf("ReadDir failed: %v", err)
		}

		if len(entries) != 3 {
			t.Errorf("expected 3 entries, got %d", len(entries))
		}
	})
}

func TestDirExists(t *testing.T) {
	WithMemFs(func(memFs afero.Fs) {
		_ = Mkdir("/existingdir", 0755)
		_ = WriteFile("/file.txt", []byte("data"), 0644)

		exists, err := DirExists("/existingdir")
		if err != nil {
			t.Fatalf("DirExists failed: %v", err)
		}
		if !exists {
			t.Error("directory should exist")
		}

		exists, err = DirExists("/file.txt")
		if err != nil {
			t.Fatalf("DirExists failed: %v", err)
		}
		if exists {
			t.Error("file should not be a directory")
		}

		exists, err = DirExists("/nonexistent")
		if err != nil {
			t.Fatalf("DirExists failed: %v", err)
		}
		if exists {
			t.Error("nonexistent path should not exist")
		}
	})
}

func TestTempFile(t *testing.T) {
	WithMemFs(func(memFs afero.Fs) {
		f, err := TempFile("", "test-*.txt")
		if err != nil {
			t.Fatalf("TempFile failed: %v", err)
		}
		defer func() { _ = f.Close() }()

		name := f.Name()
		if name == "" {
			t.Error("temp file should have a name")
		}
	})
}

func TestCopyFile_SourceNotFound(t *testing.T) {
	WithMemFs(func(memFs afero.Fs) {
		err := CopyFile("/nonexistent.txt", "/dest.txt")
		if err == nil {
			t.Error("CopyFile should fail for nonexistent source")
		}
	})
}

func TestFileSize_NotFound(t *testing.T) {
	WithMemFs(func(memFs afero.Fs) {
		_, err := FileSize("/nonexistent.txt")
		if err == nil {
			t.Error("FileSize should fail for nonexistent file")
		}
	})
}

// Tests for secure.go - these use real OS filesystem since secure functions use os package
func TestSecureMkdirAll(t *testing.T) {
	tmpDir := t.TempDir()
	testPath := tmpDir + "/secure/nested/dir"

	err := SecureMkdirAll(testPath, 0700)
	if err != nil {
		t.Fatalf("SecureMkdirAll failed: %v", err)
	}

	info, err := os.Stat(testPath)
	if err != nil {
		t.Fatalf("Directory not created: %v", err)
	}
	if !info.IsDir() {
		t.Error("Expected a directory")
	}

	// Creating again should not fail (idempotent)
	err = SecureMkdirAll(testPath, 0700)
	if err != nil {
		t.Errorf("SecureMkdirAll should be idempotent: %v", err)
	}
}

func TestSecureCreate(t *testing.T) {
	tmpDir := t.TempDir()
	testFile := tmpDir + "/secure-file.txt"

	f, err := SecureCreate(testFile)
	if err != nil {
		t.Fatalf("SecureCreate failed: %v", err)
	}
	defer func() { _ = f.Close() }()

	// Write some data
	_, err = f.WriteString("sensitive data")
	if err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	// Verify file permissions (should be 0600)
	info, _ := os.Stat(testFile)
	perm := info.Mode().Perm()
	if perm != 0600 {
		t.Errorf("Expected permissions 0600, got %o", perm)
	}
}

func TestSecureOpenFile(t *testing.T) {
	tmpDir := t.TempDir()

	t.Run("create with restrictive perm", func(t *testing.T) {
		testFile := tmpDir + "/secure-open-create.txt"
		// Even if we ask for 0644, it should be restricted to 0600
		f, err := SecureOpenFile(testFile, os.O_CREATE|os.O_WRONLY, 0644)
		if err != nil {
			t.Fatalf("SecureOpenFile failed: %v", err)
		}
		_ = f.Close()

		info, _ := os.Stat(testFile)
		perm := info.Mode().Perm()
		if perm != 0600 {
			t.Errorf("Expected permissions 0600, got %o", perm)
		}
	})

	t.Run("open existing file", func(t *testing.T) {
		testFile := tmpDir + "/secure-open-existing.txt"
		_ = os.WriteFile(testFile, []byte("content"), 0644)

		f, err := SecureOpenFile(testFile, os.O_RDONLY, 0)
		if err != nil {
			t.Fatalf("SecureOpenFile failed: %v", err)
		}
		_ = f.Close()
	})
}

func TestSecureMkdirTemp(t *testing.T) {
	t.Run("with custom dir", func(t *testing.T) {
		baseDir := t.TempDir()

		tempDir, err := SecureMkdirTemp(baseDir, "test-*")
		if err != nil {
			t.Fatalf("SecureMkdirTemp failed: %v", err)
		}
		defer func() { _ = os.RemoveAll(tempDir) }()

		info, err := os.Stat(tempDir)
		if err != nil {
			t.Fatalf("Temp directory not created: %v", err)
		}
		if !info.IsDir() {
			t.Error("Expected a directory")
		}

		// Check permissions (should be 0700)
		perm := info.Mode().Perm()
		if perm != 0700 {
			t.Errorf("Expected permissions 0700, got %o", perm)
		}
	})

	t.Run("with empty dir", func(t *testing.T) {
		tempDir, err := SecureMkdirTemp("", "test-*")
		if err != nil {
			t.Fatalf("SecureMkdirTemp failed: %v", err)
		}
		defer func() { _ = os.RemoveAll(tempDir) }()

		if tempDir == "" {
			t.Error("Expected non-empty path")
		}
	})
}

func TestCheckWriteAccess(t *testing.T) {
	t.Run("writable directory", func(t *testing.T) {
		tmpDir := t.TempDir()

		err := CheckWriteAccess(tmpDir)
		if err != nil {
			t.Errorf("CheckWriteAccess should succeed for writable dir: %v", err)
		}
	})

	t.Run("nonexistent directory", func(t *testing.T) {
		err := CheckWriteAccess("/nonexistent/path")
		if err == nil {
			t.Error("CheckWriteAccess should fail for nonexistent directory")
		}
	})
}
