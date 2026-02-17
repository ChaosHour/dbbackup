package compression

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
)

func TestVerifyStream_ValidGzip(t *testing.T) {
	// Create a valid gzip file
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "test.sql.gz")

	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}

	comp, err := NewCompressor(f, AlgorithmGzip, 1)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := comp.Write([]byte("CREATE TABLE test (id int);\n")); err != nil {
		t.Fatal(err)
	}
	comp.Close()
	f.Close()

	result, err := VerifyStream(path)
	if err != nil {
		t.Fatalf("VerifyStream error: %v", err)
	}
	if !result.Valid {
		t.Fatalf("expected valid, got invalid: %v", result.Error)
	}
	if result.Algorithm != AlgorithmGzip {
		t.Fatalf("expected gzip, got %s", result.Algorithm)
	}
	if result.BytesDecompressed == 0 {
		t.Fatal("expected non-zero decompressed bytes")
	}
}

func TestVerifyStream_ValidZstd(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "test.sql.zst")

	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}

	comp, err := NewCompressor(f, AlgorithmZstd, 3)
	if err != nil {
		t.Fatal(err)
	}
	data := bytes.Repeat([]byte("INSERT INTO test VALUES (1);\n"), 1000)
	if _, err := comp.Write(data); err != nil {
		t.Fatal(err)
	}
	comp.Close()
	f.Close()

	result, err := VerifyStream(path)
	if err != nil {
		t.Fatalf("VerifyStream error: %v", err)
	}
	if !result.Valid {
		t.Fatalf("expected valid, got invalid: %v", result.Error)
	}
	if result.Algorithm != AlgorithmZstd {
		t.Fatalf("expected zstd, got %s", result.Algorithm)
	}
	if result.BytesDecompressed != int64(len(data)) {
		t.Fatalf("expected %d decompressed bytes, got %d", len(data), result.BytesDecompressed)
	}
}

func TestVerifyStream_TruncatedZstd(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "truncated.sql.zst")

	// Create a valid zstd file first
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	comp, err := NewCompressor(f, AlgorithmZstd, 3)
	if err != nil {
		t.Fatal(err)
	}
	data := bytes.Repeat([]byte("INSERT INTO test VALUES (1);\n"), 10000)
	if _, err := comp.Write(data); err != nil {
		t.Fatal(err)
	}
	comp.Close()
	f.Close()

	// Now truncate it
	fi, _ := os.Stat(path)
	origSize := fi.Size()
	if err := os.Truncate(path, origSize/2); err != nil {
		t.Fatal(err)
	}

	result, err := VerifyStream(path)
	if err != nil {
		t.Fatalf("VerifyStream should not return error, got: %v", err)
	}
	if result.Valid {
		t.Fatal("expected invalid for truncated zstd, got valid")
	}
	if result.Error == nil {
		t.Fatal("expected error in result for truncated stream")
	}
	t.Logf("Correctly detected truncated stream: %v", result.Error)
}

func TestVerifyStream_TruncatedGzip(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "truncated.dump.gz")

	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	comp, err := NewCompressor(f, AlgorithmGzip, 1)
	if err != nil {
		t.Fatal(err)
	}
	data := bytes.Repeat([]byte("CREATE INDEX idx ON test (id);\n"), 10000)
	if _, err := comp.Write(data); err != nil {
		t.Fatal(err)
	}
	comp.Close()
	f.Close()

	fi, _ := os.Stat(path)
	if err := os.Truncate(path, fi.Size()/2); err != nil {
		t.Fatal(err)
	}

	result, err := VerifyStream(path)
	if err != nil {
		t.Fatalf("VerifyStream should not return error, got: %v", err)
	}
	if result.Valid {
		t.Fatal("expected invalid for truncated gzip, got valid")
	}
	t.Logf("Correctly detected truncated stream: %v", result.Error)
}

func TestVerifyStream_CorruptMagic(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "corrupt.sql.zst")

	// Write garbage with zstd extension
	if err := os.WriteFile(path, []byte("this is not zstd data at all"), 0644); err != nil {
		t.Fatal(err)
	}

	result, err := VerifyStream(path)
	if err != nil {
		t.Fatalf("VerifyStream should not return error, got: %v", err)
	}
	if result.Valid {
		t.Fatal("expected invalid for corrupt magic bytes")
	}
	t.Logf("Correctly detected corrupt stream: %v", result.Error)
}

func TestVerifyStreamQuick_ValidZstd(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "test.sql.zst")

	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	comp, err := NewCompressor(f, AlgorithmZstd, 3)
	if err != nil {
		t.Fatal(err)
	}
	comp.Write([]byte("SELECT 1;\n"))
	comp.Close()
	f.Close()

	result, err := VerifyStreamQuick(path)
	if err != nil {
		t.Fatalf("VerifyStreamQuick error: %v", err)
	}
	if !result.Valid {
		t.Fatalf("expected valid quick check, got: %v", result.Error)
	}
}

func TestVerifyStreamQuick_InvalidMagic(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "bad.sql.zst")

	if err := os.WriteFile(path, []byte{0x00, 0x00, 0x00, 0x00, 0x00}, 0644); err != nil {
		t.Fatal(err)
	}

	result, err := VerifyStreamQuick(path)
	if err != nil {
		t.Fatalf("VerifyStreamQuick should not return error, got: %v", err)
	}
	if result.Valid {
		t.Fatal("expected invalid for bad magic")
	}
}

func TestValidatingReader_Normal(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "test.sql.zst")

	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	comp, err := NewCompressor(f, AlgorithmZstd, 3)
	if err != nil {
		t.Fatal(err)
	}
	payload := []byte("DROP TABLE IF EXISTS test; CREATE TABLE test (id int);\n")
	comp.Write(payload)
	comp.Close()
	f.Close()

	// Open and wrap with ValidatingReader
	rf, _ := os.Open(path)
	decomp, _ := NewDecompressor(rf, path)
	vr := NewValidatingReader(decomp, rf)

	buf := make([]byte, 4096)
	var total int
	for {
		n, readErr := vr.Read(buf)
		total += n
		if readErr != nil {
			break
		}
	}

	if err := vr.Close(); err != nil {
		t.Fatalf("ValidatingReader.Close() returned error: %v", err)
	}
	if int64(total) != int64(len(payload)) {
		t.Fatalf("expected %d bytes, got %d", len(payload), total)
	}
}

func TestEncoderCRC_Enabled(t *testing.T) {
	// Verify that our encoder writes CRC (file should be 4 bytes larger than without CRC)
	var buf bytes.Buffer
	comp, err := NewCompressor(&buf, AlgorithmZstd, 3)
	if err != nil {
		t.Fatal(err)
	}
	comp.Write([]byte("test data"))
	comp.Close()

	data := buf.Bytes()
	// Zstd frame with CRC has a 4-byte checksum at the end
	// The frame header byte should have the content checksum flag set (bit 2)
	if len(data) < 5 {
		t.Fatal("compressed data too short")
	}
	fhd := data[4] // frame header descriptor
	if fhd&0x04 == 0 {
		t.Fatal("expected Content_Checksum_flag (bit 2) to be set in frame header descriptor")
	}
	t.Logf("CRC flag confirmed set: FHD=0x%02x", fhd)
}
