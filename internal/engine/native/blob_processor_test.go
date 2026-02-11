package native

import (
	"testing"

	"dbbackup/internal/blob"
)

func TestBLOBProcessorDetection(t *testing.T) {
	cfg := DefaultBLOBProcessorConfig()
	p := NewBLOBProcessor(cfg, nil)

	tests := []struct {
		name     string
		data     []byte
		wantType blob.BLOBType
		wantComp bool
	}{
		{
			name:     "JPEG",
			data:     append([]byte{0xFF, 0xD8, 0xFF, 0xE0}, make([]byte, 100)...),
			wantType: blob.BLOBTypeCompressed,
			wantComp: false,
		},
		{
			name:     "PNG",
			data:     append([]byte{0x89, 0x50, 0x4E, 0x47, 0x0D, 0x0A, 0x1A, 0x0A}, make([]byte, 100)...),
			wantType: blob.BLOBTypeCompressed,
			wantComp: false,
		},
		{
			name:     "GZIP",
			data:     append([]byte{0x1F, 0x8B}, make([]byte, 100)...),
			wantType: blob.BLOBTypeCompressed,
			wantComp: false,
		},
		{
			name:     "JSON text",
			data:     []byte(`{"key": "value", "numbers": [1, 2, 3], "nested": {"a": "b"}}`),
			wantType: blob.BLOBTypeText,
			wantComp: true,
		},
		{
			name:     "XML text",
			data:     []byte(`<?xml version="1.0"?><root><item>test data</item></root>`),
			wantType: blob.BLOBTypeText,
			wantComp: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			decision := p.Process(tt.data)
			if decision.Type != tt.wantType {
				t.Errorf("type = %v, want %v", decision.Type, tt.wantType)
			}
			if decision.ShouldCompress != tt.wantComp {
				t.Errorf("shouldCompress = %v, want %v", decision.ShouldCompress, tt.wantComp)
			}
		})
	}
}

func TestBLOBProcessorCompressionModes(t *testing.T) {
	jpeg := append([]byte{0xFF, 0xD8, 0xFF, 0xE0}, make([]byte, 100)...)

	tests := []struct {
		mode     string
		wantComp bool
	}{
		{"auto", false},   // JPEG is pre-compressed, skip
		{"always", true},  // Force compress everything
		{"never", false},  // Never compress
	}

	for _, tt := range tests {
		t.Run(tt.mode, func(t *testing.T) {
			cfg := DefaultBLOBProcessorConfig()
			cfg.CompressionMode = tt.mode
			p := NewBLOBProcessor(cfg, nil)

			decision := p.Process(jpeg)
			if decision.ShouldCompress != tt.wantComp {
				t.Errorf("mode=%s: shouldCompress = %v, want %v", tt.mode, decision.ShouldCompress, tt.wantComp)
			}
		})
	}
}

func TestBLOBProcessorDedup(t *testing.T) {
	cfg := DefaultBLOBProcessorConfig()
	cfg.Deduplicate = true
	cfg.ExpectedBLOBs = 1000
	p := NewBLOBProcessor(cfg, nil)

	data := []byte("identical blob content that repeats")

	// First time: not duplicate
	d1 := p.Process(data)
	if d1.IsDuplicate {
		t.Error("first occurrence should not be duplicate")
	}
	if d1.Hash == "" {
		t.Error("hash should not be empty when dedup enabled")
	}

	// Second time: duplicate
	d2 := p.Process(data)
	if !d2.IsDuplicate {
		t.Error("second occurrence should be duplicate")
	}
	if d2.Hash != d1.Hash {
		t.Error("hash should match for identical data")
	}

	// Different data: not duplicate
	d3 := p.Process([]byte("different blob content entirely"))
	if d3.IsDuplicate {
		t.Error("different data should not be duplicate")
	}
}

func TestBLOBProcessorSplit(t *testing.T) {
	cfg := DefaultBLOBProcessorConfig()
	cfg.SplitMode = true
	cfg.Threshold = 1024 // 1KB threshold
	p := NewBLOBProcessor(cfg, nil)

	// Small BLOB: should stay inline
	small := make([]byte, 512)
	d1 := p.Process(small)
	if d1.ShouldSplit {
		t.Error("small BLOB should not be split")
	}

	// Large BLOB: should be split to stream
	large := make([]byte, 2048)
	d2 := p.Process(large)
	if !d2.ShouldSplit {
		t.Error("large BLOB should be split")
	}

	stats := p.Stats()
	if stats.InlineBLOBs != 1 {
		t.Errorf("inline = %d, want 1", stats.InlineBLOBs)
	}
	if stats.SplitBLOBs != 1 {
		t.Errorf("split = %d, want 1", stats.SplitBLOBs)
	}
}

func TestBLOBProcessorStats(t *testing.T) {
	cfg := DefaultBLOBProcessorConfig()
	cfg.Deduplicate = true
	cfg.ExpectedBLOBs = 100
	p := NewBLOBProcessor(cfg, nil)

	// Process various types
	jpeg := append([]byte{0xFF, 0xD8, 0xFF, 0xE0}, make([]byte, 1000)...)
	json := []byte(`{"key": "value", "another": "field", "list": [1,2,3]}`)
	gzip := append([]byte{0x1F, 0x8B}, make([]byte, 500)...)

	p.Process(jpeg)
	p.Process(json)
	p.Process(gzip)
	p.Process(jpeg) // duplicate

	stats := p.Stats()

	if stats.TotalBLOBs != 4 {
		t.Errorf("total = %d, want 4", stats.TotalBLOBs)
	}
	if stats.CompressedBLOBs != 3 { // jpeg + gzip + dup jpeg
		t.Errorf("compressed = %d, want 3", stats.CompressedBLOBs)
	}
	if stats.SkippedCompress != 3 {
		t.Errorf("skipped = %d, want 3", stats.SkippedCompress)
	}
	if stats.DuplicateCount != 1 {
		t.Errorf("dupes = %d, want 1", stats.DuplicateCount)
	}

	summary := stats.Summary()
	if summary == "" {
		t.Error("summary should not be empty")
	}
}

func TestBLOBProcessorNoDetection(t *testing.T) {
	cfg := DefaultBLOBProcessorConfig()
	cfg.DetectTypes = false
	p := NewBLOBProcessor(cfg, nil)

	jpeg := append([]byte{0xFF, 0xD8, 0xFF, 0xE0}, make([]byte, 100)...)
	decision := p.Process(jpeg)

	// Without detection, should default to compress
	if !decision.ShouldCompress {
		t.Error("without detection, should default to compress")
	}
	if decision.Type != blob.BLOBTypeUnknown {
		t.Errorf("without detection, type should be unknown, got %v", decision.Type)
	}
}

func TestHashBLOBData(t *testing.T) {
	hash := HashBLOBData([]byte("test"))
	if len(hash) != 64 {
		t.Errorf("hash length = %d, want 64", len(hash))
	}

	// Deterministic
	hash2 := HashBLOBData([]byte("test"))
	if hash != hash2 {
		t.Error("hash should be deterministic")
	}

	// Different input = different hash
	hash3 := HashBLOBData([]byte("other"))
	if hash == hash3 {
		t.Error("different input should produce different hash")
	}
}

func BenchmarkBLOBProcessorProcess(b *testing.B) {
	cfg := DefaultBLOBProcessorConfig()
	cfg.Deduplicate = true
	cfg.ExpectedBLOBs = 100000
	p := NewBLOBProcessor(cfg, nil)

	data := make([]byte, 4096)
	copy(data, []byte{0xFF, 0xD8, 0xFF, 0xE0}) // JPEG header

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		p.Process(data)
	}
}
