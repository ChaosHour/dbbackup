package native

import (
	"testing"
)

func TestParseHugePagesFromContent(t *testing.T) {
	tests := []struct {
		name               string
		content            string
		wantTotal          int
		wantFree           int
		wantPageSize       uint64 // bytes
		wantAvailable      bool
	}{
		{
			name: "HugePages enabled — 4096 pages, 2 MB each",
			content: `MemTotal:       65536000 kB
MemFree:        32000000 kB
HugePages_Total:    4096
HugePages_Free:     2048
HugePages_Rsvd:        0
HugePages_Surp:        0
Hugepagesize:       2048 kB
`,
			wantTotal:     4096,
			wantFree:      2048,
			wantPageSize:  2048 * 1024,
			wantAvailable: true,
		},
		{
			name: "HugePages disabled — zeros",
			content: `MemTotal:       16384000 kB
MemFree:         8000000 kB
HugePages_Total:       0
HugePages_Free:        0
Hugepagesize:       2048 kB
`,
			wantTotal:     0,
			wantFree:      0,
			wantPageSize:  2048 * 1024,
			wantAvailable: false,
		},
		{
			name: "1 GB HugePages",
			content: `HugePages_Total:     16
HugePages_Free:       8
Hugepagesize:    1048576 kB
`,
			wantTotal:     16,
			wantFree:      8,
			wantPageSize:  1048576 * 1024,
			wantAvailable: true,
		},
		{
			name:          "Empty content",
			content:       "",
			wantTotal:     0,
			wantFree:      0,
			wantPageSize:  0,
			wantAvailable: false,
		},
		{
			name: "Partial content — only Total line",
			content: `HugePages_Total:     512
`,
			wantTotal:     512,
			wantFree:      0,
			wantPageSize:  0,
			wantAvailable: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var p SystemProfile
			parseHugePagesFromContent(&p, tt.content)

			if p.HugePagesTotal != tt.wantTotal {
				t.Errorf("HugePagesTotal = %d, want %d", p.HugePagesTotal, tt.wantTotal)
			}
			if p.HugePagesFree != tt.wantFree {
				t.Errorf("HugePagesFree = %d, want %d", p.HugePagesFree, tt.wantFree)
			}
			if p.HugePageSize != tt.wantPageSize {
				t.Errorf("HugePageSize = %d, want %d", p.HugePageSize, tt.wantPageSize)
			}
			if p.HugePagesAvailable != tt.wantAvailable {
				t.Errorf("HugePagesAvailable = %v, want %v", p.HugePagesAvailable, tt.wantAvailable)
			}
		})
	}
}

func TestFormatBytesHuman(t *testing.T) {
	tests := []struct {
		input uint64
		want  string
	}{
		{8 * 1024 * 1024 * 1024, "8 GB"},
		{512 * 1024 * 1024, "512 MB"},
		{1024 * 1024 * 1024, "1 GB"},
		{0, "0 MB"},
	}
	for _, tt := range tests {
		got := formatBytesHuman(tt.input)
		if got != tt.want {
			t.Errorf("formatBytesHuman(%d) = %q, want %q", tt.input, got, tt.want)
		}
	}
}

func TestRecommendedSharedBuffers(t *testing.T) {
	// 4096 pages × 2 MB = 8 GB → 75% = 6 GB
	var p SystemProfile
	content := `HugePages_Total:    4096
HugePages_Free:     2048
Hugepagesize:       2048 kB
`
	parseHugePagesFromContent(&p, content)
	if !p.HugePagesAvailable {
		t.Fatal("expected HugePagesAvailable = true")
	}
	totalHP := uint64(p.HugePagesTotal) * p.HugePageSize
	recommended := totalHP * 3 / 4
	result := formatBytesHuman(recommended)
	if result != "6 GB" {
		t.Errorf("recommended shared_buffers = %q, want %q", result, "6 GB")
	}
}
