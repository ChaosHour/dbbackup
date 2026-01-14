package fs

import (
	"os"
	"testing"

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
