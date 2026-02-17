package tui

import (
	"testing"

	"dbbackup/internal/config"
	"dbbackup/internal/cpu"
	"dbbackup/internal/logger"
)

// ──────────────────────────────────────────────────────────────────────────────
// CPU Optimization TUI Panel Tests
// ──────────────────────────────────────────────────────────────────────────────

func newTestCPUOptModel() *CPUOptModel {
	cfg := &config.Config{
		Jobs:                 8,
		DumpJobs:             4,
		MySQLBatchSize:       5000,
		BufferSize:           262144,
		CompressionAlgorithm: "gzip",
		CPUAutoTune:          true,
		CPUAutoCompression:   true,
		CPUAutoCacheBuffer:   true,
		CPUAutoNUMA:          true,
		CPUBoostGovernor:     false,
	}
	log := logger.NewNullLogger()
	return NewCPUOptModel(cfg, log, nil)
}

func TestNewCPUOptModel(t *testing.T) {
	m := newTestCPUOptModel()

	if m == nil {
		t.Fatal("NewCPUOptModel returned nil")
	}
	if !m.autoTune {
		t.Error("autoTune should default to true")
	}
	if !m.autoCompression {
		t.Error("autoCompression should default to true")
	}
	if !m.autoCacheBuffer {
		t.Error("autoCacheBuffer should default to true")
	}
	if !m.autoNUMA {
		t.Error("autoNUMA should default to true")
	}
	if m.boostGovernor {
		t.Error("boostGovernor should default to false")
	}
	if m.loading != true {
		t.Error("should start in loading state")
	}
	if len(m.rows) != 13 {
		t.Errorf("expected 13 rows, got %d", len(m.rows))
	}
}

func TestCPUOptModel_ToggleByKey(t *testing.T) {
	m := newTestCPUOptModel()

	// All start true except boostGovernor
	m.toggleByKey("auto_tune")
	if m.autoTune {
		t.Error("auto_tune should be false after toggle")
	}
	m.toggleByKey("auto_tune")
	if !m.autoTune {
		t.Error("auto_tune should be true after double toggle")
	}

	m.toggleByKey("auto_compression")
	if m.autoCompression {
		t.Error("auto_compression should be false")
	}

	m.toggleByKey("auto_cache_buffer")
	if m.autoCacheBuffer {
		t.Error("auto_cache_buffer should be false")
	}

	m.toggleByKey("auto_numa")
	if m.autoNUMA {
		t.Error("auto_numa should be false")
	}

	m.toggleByKey("boost_governor")
	if !m.boostGovernor {
		t.Error("boost_governor should be true after toggle")
	}
}

func TestCPUOptModel_GetToggle(t *testing.T) {
	m := newTestCPUOptModel()

	tests := []struct {
		key  string
		want bool
	}{
		{"auto_tune", true},
		{"auto_compression", true},
		{"auto_cache_buffer", true},
		{"auto_numa", true},
		{"boost_governor", false},
		{"unknown", false},
	}

	for _, tt := range tests {
		got := m.getToggle(tt.key)
		if got != tt.want {
			t.Errorf("getToggle(%q) = %v, want %v", tt.key, got, tt.want)
		}
	}
}

func TestCPUOptModel_GetOverrideValue(t *testing.T) {
	m := newTestCPUOptModel()

	tests := []struct {
		key  string
		want string
	}{
		{"jobs", "8"},
		{"dump_jobs", "4"},
		{"batch_size", "5000"},
		{"buffer_kb", "256"},
		{"compression", "gzip"},
		{"unknown", ""},
	}

	for _, tt := range tests {
		got := m.getOverrideValue(tt.key)
		if got != tt.want {
			t.Errorf("getOverrideValue(%q) = %q, want %q", tt.key, got, tt.want)
		}
	}
}

func TestCPUOptModel_GetAutoValue(t *testing.T) {
	m := newTestCPUOptModel()

	// Without summary — should return empty
	for _, key := range []string{"jobs", "dump_jobs", "batch_size", "buffer_kb", "compression"} {
		if got := m.getAutoValue(key); got != "" {
			t.Errorf("getAutoValue(%q) without summary = %q, want empty", key, got)
		}
	}

	// With summary
	m.summary = &cpu.OptimizationSummary{
		Tuning: &cpu.VendorTuning{
			RecommendedJobs: 16,
			RecommendedDump: 12,
			BatchSizeHint:   7500,
			StreamBufferKB:  256,
			CompressionAlgo: "zstd",
		},
	}

	tests := []struct {
		key  string
		want string
	}{
		{"jobs", "16"},
		{"dump_jobs", "12"},
		{"batch_size", "7500"},
		{"buffer_kb", "256"},
		{"compression", "zstd"},
		{"unknown", ""},
	}

	for _, tt := range tests {
		got := m.getAutoValue(tt.key)
		if got != tt.want {
			t.Errorf("getAutoValue(%q) = %q, want %q", tt.key, got, tt.want)
		}
	}
}

func TestCPUOptModel_ApplyEditBuf(t *testing.T) {
	m := newTestCPUOptModel()

	// Test jobs edit
	m.cursor = 5 // jobs row
	m.editBuf = "24"
	m.applyEditBuf()
	if m.overrideJobs != "24" {
		t.Errorf("overrideJobs = %q, want 24", m.overrideJobs)
	}

	// Test dump_jobs edit
	m.cursor = 6
	m.editBuf = "8"
	m.applyEditBuf()
	if m.overrideDumpJobs != "8" {
		t.Errorf("overrideDumpJobs = %q, want 8", m.overrideDumpJobs)
	}

	// Test batch_size edit
	m.cursor = 7
	m.editBuf = "10000"
	m.applyEditBuf()
	if m.overrideBatchSize != "10000" {
		t.Errorf("overrideBatchSize = %q, want 10000", m.overrideBatchSize)
	}

	// Test buffer_kb edit
	m.cursor = 8
	m.editBuf = "512"
	m.applyEditBuf()
	if m.overrideBufferKB != "512" {
		t.Errorf("overrideBufferKB = %q, want 512", m.overrideBufferKB)
	}
}

func TestCPUOptModel_SaveSettings(t *testing.T) {
	m := newTestCPUOptModel()
	m.config.NoSaveConfig = true // don't write to disk in test

	// Modify toggles
	m.autoTune = false
	m.autoCompression = false
	m.boostGovernor = true

	// Modify overrides
	m.overrideJobs = "24"
	m.overrideDumpJobs = "12"
	m.overrideBatchSize = "10000"
	m.overrideBufferKB = "512"
	m.overrideCompress = "zstd"

	m.summary = &cpu.OptimizationSummary{}

	_, _ = m.saveSettings()

	// Check config was updated
	if m.config.CPUAutoTune != false {
		t.Error("CPUAutoTune should be false")
	}
	if m.config.CPUAutoCompression != false {
		t.Error("CPUAutoCompression should be false")
	}
	if m.config.CPUBoostGovernor != true {
		t.Error("CPUBoostGovernor should be true")
	}
	if m.config.Jobs != 24 {
		t.Errorf("Jobs = %d, want 24", m.config.Jobs)
	}
	if m.config.DumpJobs != 12 {
		t.Errorf("DumpJobs = %d, want 12", m.config.DumpJobs)
	}
	if m.config.MySQLBatchSize != 10000 {
		t.Errorf("MySQLBatchSize = %d, want 10000", m.config.MySQLBatchSize)
	}
	if m.config.BufferSize != 512*1024 {
		t.Errorf("BufferSize = %d, want %d", m.config.BufferSize, 512*1024)
	}
	if m.config.CompressionAlgorithm != "zstd" {
		t.Errorf("CompressionAlgorithm = %q, want zstd", m.config.CompressionAlgorithm)
	}
	if m.config.CPUOptimizations != m.summary {
		t.Error("CPUOptimizations should be set to summary")
	}
}

func TestCPUOptModel_ResetToAuto(t *testing.T) {
	m := newTestCPUOptModel()

	// Dirty the state
	m.autoTune = false
	m.autoCompression = false
	m.autoCacheBuffer = false
	m.autoNUMA = false
	m.boostGovernor = true

	_, _ = m.resetToAuto()

	if !m.autoTune {
		t.Error("autoTune should be true after reset")
	}
	if !m.autoCompression {
		t.Error("autoCompression should be true after reset")
	}
	if !m.autoCacheBuffer {
		t.Error("autoCacheBuffer should be true after reset")
	}
	if !m.autoNUMA {
		t.Error("autoNUMA should be true after reset")
	}
	if m.boostGovernor {
		t.Error("boostGovernor should be false after reset")
	}
	if !m.loading {
		t.Error("should be loading after reset")
	}
}

func TestCPUOptModel_BuildRows(t *testing.T) {
	m := newTestCPUOptModel()

	// Verify the row structure
	toggleCount := 0
	overrideCount := 0
	actionCount := 0
	for _, r := range m.rows {
		switch r.section {
		case cpuOptSectionToggles:
			toggleCount++
		case cpuOptSectionOverride:
			overrideCount++
		case cpuOptSectionActions:
			actionCount++
		}
	}

	if toggleCount != 5 {
		t.Errorf("expected 5 toggle rows, got %d", toggleCount)
	}
	if overrideCount != 5 {
		t.Errorf("expected 5 override rows, got %d", overrideCount)
	}
	if actionCount != 3 {
		t.Errorf("expected 3 action rows, got %d", actionCount)
	}
}

func TestCPUOptModel_ViewLoading(t *testing.T) {
	m := newTestCPUOptModel()
	m.loading = true

	view := m.View()
	if view == "" {
		t.Error("View should not be empty")
	}
	if !contains(view, "Detecting hardware") {
		t.Error("loading view should mention 'Detecting hardware'")
	}
}

func TestCPUOptModel_ViewWithSummary(t *testing.T) {
	m := newTestCPUOptModel()
	m.loading = false
	m.config.CPUInfo = &cpu.CPUInfo{
		Vendor:    "GenuineIntel",
		ModelName: "Intel Core i9-12900K",
		Features:  []string{"avx2", "avx", "sse4_2", "aes"},
	}
	m.summary = &cpu.OptimizationSummary{
		Features: &cpu.CPUFeatures{
			HasSSE42: true,
			HasAVX:   true,
			HasAVX2:  true,
			HasAESNI: true,
		},
		Cache: &cpu.CacheTopology{
			L1DataKB: 48,
			L2KB:     1280,
			L3KB:     30720,
		},
		Hybrid: &cpu.HybridTopology{
			IsHybrid:   true,
			PcoreCount: 8,
			EcoreCount: 8,
		},
		FreqGov: &cpu.FreqGovernor{
			Governor:  "performance",
			IsOptimal: true,
		},
		Tuning: &cpu.VendorTuning{
			RecommendedJobs: 8,
			RecommendedDump: 6,
			BatchSizeHint:   5000,
			StreamBufferKB:  256,
			CompressionAlgo: "zstd",
			Notes:           "Intel hybrid: using P-cores only",
		},
	}

	view := m.View()

	// Check key content appears
	for _, want := range []string{
		"CPU Optimization",
		"Tuning Toggles",
		"Parameter Overrides",
		"Save to config",
		"Reset to auto",
	} {
		if !contains(view, want) {
			t.Errorf("view should contain %q", want)
		}
	}
}

func TestCPUOptModel_FormatISAFlags(t *testing.T) {
	m := newTestCPUOptModel()

	// No features
	got := m.formatISAFlags(&cpu.CPUFeatures{})
	if got != "(none detected)" {
		t.Errorf("empty features = %q, want '(none detected)'", got)
	}

	// Some features
	got = m.formatISAFlags(&cpu.CPUFeatures{
		HasSSE42: true,
		HasAVX2:  true,
		HasAESNI: true,
	})
	if !contains(got, "SSE4.2") || !contains(got, "AVX2") || !contains(got, "AES-NI") {
		t.Errorf("flags = %q, missing expected entries", got)
	}
}

func TestStripAnsi(t *testing.T) {
	tests := []struct {
		input string
		want  string
	}{
		{"hello", "hello"},
		{"\033[1mhello\033[0m", "hello"},
		{"\033[38;5;42m[OK]\033[0m text", "[OK] text"},
		{"no escapes here", "no escapes here"},
	}

	for _, tt := range tests {
		got := stripAnsi(tt.input)
		if got != tt.want {
			t.Errorf("stripAnsi(%q) = %q, want %q", tt.input, got, tt.want)
		}
	}
}

func TestCPUOptModel_DetectedMsg(t *testing.T) {
	m := newTestCPUOptModel()
	m.loading = true

	summary := &cpu.OptimizationSummary{
		Tuning: &cpu.VendorTuning{
			RecommendedJobs: 16,
			RecommendedDump: 12,
			BatchSizeHint:   7500,
			StreamBufferKB:  256,
			CompressionAlgo: "zstd",
		},
	}

	msg := cpuOptDetectedMsg{summary: summary}
	result, _ := m.Update(msg)
	updated := result.(*CPUOptModel)

	if updated.loading {
		t.Error("should not be loading after detected msg")
	}
	if updated.summary == nil {
		t.Error("summary should be set")
	}
	if updated.overrideJobs != "16" {
		t.Errorf("overrideJobs = %q, want 16", updated.overrideJobs)
	}
	if updated.overrideCompress != "zstd" {
		t.Errorf("overrideCompress = %q, want zstd", updated.overrideCompress)
	}
}

// contains is a simple string-in-string helper.
func contains(s, substr string) bool {
	return len(s) >= len(substr) && searchString(s, substr)
}

func searchString(s, sub string) bool {
	for i := 0; i <= len(s)-len(sub); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}
