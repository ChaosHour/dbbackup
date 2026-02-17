package cpu

import (
	"runtime"
	"testing"
)

// ──────────────────────────────────────────────────────────────────────────────
// CPUFeatures / ISA Detection
// ──────────────────────────────────────────────────────────────────────────────

func TestDetectFeatures_Nil(t *testing.T) {
	f := DetectFeatures(nil)
	if f == nil {
		t.Fatal("DetectFeatures(nil) should return non-nil")
	}
	// All fields should be false on nil input
	if f.HasAVX || f.HasAVX2 || f.HasSSE42 {
		t.Error("expected all features false for nil CPUInfo")
	}
}

func TestDetectFeatures_x86Flags(t *testing.T) {
	info := &CPUInfo{
		Features: []string{"sse4_2", "avx", "avx2", "avx512f", "avx512vl", "avx512bw", "avx512vbmi", "aes", "pclmulqdq", "sha_ni"},
	}
	f := DetectFeatures(info)
	checks := []struct {
		name string
		got  bool
	}{
		{"SSE4.2", f.HasSSE42},
		{"AVX", f.HasAVX},
		{"AVX2", f.HasAVX2},
		{"AVX-512", f.HasAVX512},
		{"AVX-512VL", f.HasAVX512VL},
		{"AVX-512BW", f.HasAVX512BW},
		{"AVX-512VBMI", f.HasAVX512VBI},
		{"AES-NI", f.HasAESNI},
		{"PCLMULQDQ", f.HasPCLMULQDQ},
		{"SHA-NI", f.HasSHA},
	}
	for _, c := range checks {
		if !c.got {
			t.Errorf("expected %s = true", c.name)
		}
	}
}

func TestRecommendCompression_AVX2(t *testing.T) {
	f := &CPUFeatures{HasAVX2: true}
	if got := f.RecommendCompression(""); got != "zstd" {
		t.Errorf("expected zstd with AVX2; got %s", got)
	}
}

func TestRecommendCompression_VBMI(t *testing.T) {
	f := &CPUFeatures{HasAVX512VBI: true, HasAVX2: true}
	if got := f.RecommendCompression("AuthenticAMD"); got != "zstd" {
		t.Errorf("expected zstd with AVX-512 VBMI; got %s", got)
	}
}

func TestRecommendCompression_None(t *testing.T) {
	f := &CPUFeatures{}
	if got := f.RecommendCompression(""); got != "gzip" {
		t.Errorf("expected gzip fallback; got %s", got)
	}
}

func TestRecommendCompression_Nil(t *testing.T) {
	var f *CPUFeatures
	if got := f.RecommendCompression(""); got != "gzip" {
		t.Errorf("expected gzip for nil features; got %s", got)
	}
}

func TestSupportsHardwareCRC(t *testing.T) {
	f := &CPUFeatures{HasSSE42: true}
	if !f.SupportsHardwareCRC() {
		t.Error("SSE 4.2 should report hardware CRC support")
	}
	f2 := &CPUFeatures{}
	if f2.SupportsHardwareCRC() {
		t.Error("no CRC flags should report false")
	}
}

func TestSupportsHardwareAES(t *testing.T) {
	f := &CPUFeatures{HasAESNI: true}
	if !f.SupportsHardwareAES() {
		t.Error("AES-NI should report hardware AES support")
	}
}

func TestGOAMD64Level(t *testing.T) {
	if runtime.GOARCH != "amd64" {
		t.Skip("GOAMD64Level only meaningful on amd64")
	}
	tests := []struct {
		name     string
		features CPUFeatures
		want     int
	}{
		{"baseline", CPUFeatures{}, 1},
		{"SSE4.2", CPUFeatures{HasSSE42: true}, 2},
		{"AVX2", CPUFeatures{HasSSE42: true, HasAVX2: true}, 3},
		{"AVX-512", CPUFeatures{HasSSE42: true, HasAVX2: true, HasAVX512: true}, 4},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.features.GOAMD64Level(); got != tt.want {
				t.Errorf("GOAMD64Level() = %d, want %d", got, tt.want)
			}
		})
	}
}

// ──────────────────────────────────────────────────────────────────────────────
// Hybrid Topology
// ──────────────────────────────────────────────────────────────────────────────

func TestDetectHybridTopology_Runs(t *testing.T) {
	ht := DetectHybridTopology()
	if ht == nil {
		t.Fatal("DetectHybridTopology should never return nil")
	}
	// We can't assert the result is hybrid (depends on hardware), but
	// the function should not panic.
	t.Logf("Hybrid: %v (P=%d E=%d)", ht.IsHybrid, ht.PcoreCount, ht.EcoreCount)
}

func TestEffectiveCoresForWork_NonHybrid(t *testing.T) {
	ht := &HybridTopology{IsHybrid: false}
	got := ht.EffectiveCoresForWork()
	if got != runtime.NumCPU() {
		t.Errorf("non-hybrid should return NumCPU (%d), got %d", runtime.NumCPU(), got)
	}
}

func TestEffectiveCoresForWork_Hybrid(t *testing.T) {
	ht := &HybridTopology{
		IsHybrid:   true,
		PcoreCount: 6,
		EcoreCount: 8,
	}
	if got := ht.EffectiveCoresForWork(); got != 6 {
		t.Errorf("hybrid should return PcoreCount (6), got %d", got)
	}
}

// ──────────────────────────────────────────────────────────────────────────────
// Cache Topology
// ──────────────────────────────────────────────────────────────────────────────

func TestDetectCacheTopology_Runs(t *testing.T) {
	ct := DetectCacheTopology()
	if ct == nil {
		t.Fatal("DetectCacheTopology should never return nil")
	}
	// On CI / containers the cache values might be 0 — that's OK
	t.Logf("L1d=%dKB L2=%dKB L3=%dKB", ct.L1DataKB, ct.L2KB, ct.L3KB)
}

func TestOptimalBufferSizeKB_Defaults(t *testing.T) {
	ct := &CacheTopology{L3KB: 0}
	if got := ct.OptimalBufferSizeKB(); got != 256 {
		t.Errorf("zero L3 → default 256KB, got %d", got)
	}
}

func TestOptimalBufferSizeKB_LargeL3(t *testing.T) {
	ct := &CacheTopology{L3KB: 256 * 1024} // 256 MB (EPYC)
	got := ct.OptimalBufferSizeKB()
	if got < 256 || got > 16384 {
		t.Errorf("buffer should be 256-16384KB, got %d", got)
	}
}

func TestOptimalBatchSize(t *testing.T) {
	tests := []struct {
		l3KB int
		min  int
		max  int
	}{
		{0, 5000, 5000},       // default
		{8 * 1024, 5000, 5000},    // 8 MB
		{16 * 1024, 7500, 7500},   // 16 MB
		{32 * 1024, 10000, 10000}, // 32 MB
		{256 * 1024, 20000, 20000}, // 256 MB (EPYC)
	}
	for _, tt := range tests {
		ct := &CacheTopology{L3KB: tt.l3KB}
		got := ct.OptimalBatchSize()
		if got < tt.min || got > tt.max {
			t.Errorf("L3=%dKB → batch %d, want [%d, %d]", tt.l3KB, got, tt.min, tt.max)
		}
	}
}

func TestParseCacheSize(t *testing.T) {
	tests := []struct {
		input string
		want  int
	}{
		{"32K", 32},
		{"1024K", 1024},
		{"16M", 16384},
		{"32MiB", 32768},
		{"32768", 32768},
		{"", 0},
	}
	for _, tt := range tests {
		if got := parseCacheSize(tt.input); got != tt.want {
			t.Errorf("parseCacheSize(%q) = %d, want %d", tt.input, got, tt.want)
		}
	}
}

// ──────────────────────────────────────────────────────────────────────────────
// Vendor Classification & Tuning
// ──────────────────────────────────────────────────────────────────────────────

func TestClassifyVendor(t *testing.T) {
	tests := []struct {
		vendor string
		model  string
		want   VendorClass
	}{
		{"GenuineIntel", "Intel Xeon E5-2680", VendorIntel},
		{"AuthenticAMD", "AMD EPYC 7763", VendorAMD},
		{"Apple", "Apple M2 Pro", VendorApple},
		{"", "", VendorUnknown},
	}
	for _, tt := range tests {
		if got := ClassifyVendor(tt.vendor, tt.model); got != tt.want {
			t.Errorf("ClassifyVendor(%q, %q) = %d, want %d", tt.vendor, tt.model, got, tt.want)
		}
	}
}

func TestComputeVendorTuning_Intel(t *testing.T) {
	info := &CPUInfo{
		Vendor:        "GenuineIntel",
		ModelName:     "Intel Core i9-12900K",
		PhysicalCores: 16,
		LogicalCores:  24,
	}
	features := &CPUFeatures{HasAVX2: true, HasAVX512: true}
	cache := &CacheTopology{L3KB: 30 * 1024}
	hybrid := &HybridTopology{IsHybrid: true, PcoreCount: 8, EcoreCount: 8}

	t1 := ComputeVendorTuning(info, features, cache, hybrid)
	if t1.Vendor != VendorIntel {
		t.Errorf("vendor = %d, want Intel", t1.Vendor)
	}
	// Should use P-core count (8), then 75% for Intel = 6
	if t1.RecommendedJobs != 6 {
		t.Errorf("Intel hybrid recommended jobs = %d, want 6", t1.RecommendedJobs)
	}
	if t1.CompressionAlgo != "zstd" {
		t.Errorf("compression = %s, want zstd (AVX2)", t1.CompressionAlgo)
	}
	// AVX-512 throttle should be true for i9-12900K (not Sapphire Rapids)
	if !t1.AVX512Throttle {
		t.Error("expected AVX512Throttle=true for consumer Intel")
	}
}

func TestComputeVendorTuning_AMD(t *testing.T) {
	info := &CPUInfo{
		Vendor:        "AuthenticAMD",
		ModelName:     "AMD EPYC 7763 64-Core Processor",
		PhysicalCores: 64,
		LogicalCores:  128,
	}
	features := &CPUFeatures{HasAVX2: true}
	cache := &CacheTopology{L3KB: 256 * 1024}
	hybrid := &HybridTopology{IsHybrid: false}

	t1 := ComputeVendorTuning(info, features, cache, hybrid)
	if t1.Vendor != VendorAMD {
		t.Errorf("vendor = %d, want AMD", t1.Vendor)
	}
	// AMD uses full physical cores, capped at 64
	if t1.RecommendedJobs != 64 {
		t.Errorf("AMD EPYC jobs = %d, want 64", t1.RecommendedJobs)
	}
	if t1.AVX512Throttle {
		t.Error("AMD should not have AVX512 throttle")
	}
	// Large L3 → batch size 20000
	if t1.BatchSizeHint != 20000 {
		t.Errorf("batch hint = %d, want 20000 for 256MB L3", t1.BatchSizeHint)
	}
}

func TestComputeVendorTuning_Nil(t *testing.T) {
	t1 := ComputeVendorTuning(nil, nil, nil, nil)
	if t1 == nil {
		t.Fatal("should not return nil")
	}
	if t1.Vendor != VendorUnknown {
		t.Errorf("nil CPUInfo → VendorUnknown, got %d", t1.Vendor)
	}
}

func TestComputeVendorTuning_IntelSapphireRapids(t *testing.T) {
	info := &CPUInfo{
		Vendor:        "GenuineIntel",
		ModelName:     "Intel Xeon Sapphire Rapids 8474C",
		PhysicalCores: 48,
		LogicalCores:  96,
	}
	features := &CPUFeatures{HasAVX2: true, HasAVX512: true}

	t1 := ComputeVendorTuning(info, features, nil, nil)
	// Sapphire Rapids should NOT have AVX512 throttle
	if t1.AVX512Throttle {
		t.Error("Sapphire Rapids should not have AVX-512 throttle")
	}
}

// ──────────────────────────────────────────────────────────────────────────────
// Frequency Governor
// ──────────────────────────────────────────────────────────────────────────────

func TestDetectFreqGovernor_Runs(t *testing.T) {
	fg := DetectFreqGovernor()
	if fg == nil {
		t.Fatal("DetectFreqGovernor should never return nil")
	}
	t.Logf("Governor: %s, Optimal: %v", fg.Governor, fg.IsOptimal)
}

func TestFreqGovernor_Recommendation(t *testing.T) {
	// Verify that performance governor is optimal
	fg := &FreqGovernor{Governor: "performance", IsOptimal: true}
	if !fg.IsOptimal {
		t.Error("performance governor should be optimal")
	}
}

// ──────────────────────────────────────────────────────────────────────────────
// NUMA
// ──────────────────────────────────────────────────────────────────────────────

func TestDetectNUMATopology_Runs(t *testing.T) {
	nt := DetectNUMATopology()
	if nt == nil {
		t.Fatal("DetectNUMATopology should never return nil")
	}
	t.Logf("NUMA: %v, Nodes: %d", nt.IsNUMA, nt.NodeCount)
}

func TestParseCPUList(t *testing.T) {
	tests := []struct {
		input string
		want  int // expected length
	}{
		{"0-3", 4},
		{"0,1,2,3", 4},
		{"0-1,4-5", 4},
		{"5", 1},
		{"0-7,16-23", 16},
	}
	for _, tt := range tests {
		got := parseCPUList(tt.input)
		if len(got) != tt.want {
			t.Errorf("parseCPUList(%q) → len %d, want %d", tt.input, len(got), tt.want)
		}
	}
}

func TestNUMAWorkerDistribution_Single(t *testing.T) {
	nt := &NUMATopology{IsNUMA: false}
	dist := nt.NUMARecommendedWorkerDistribution(8, -1)
	if dist[0] != 8 {
		t.Errorf("single-node: expected 8 on node 0, got %d", dist[0])
	}
}

func TestNUMAWorkerDistribution_Multi(t *testing.T) {
	nt := &NUMATopology{
		IsNUMA:    true,
		NodeCount: 2,
		Nodes:     []NUMANode{{ID: 0}, {ID: 1}},
	}
	dist := nt.NUMARecommendedWorkerDistribution(10, 0)
	// 70% on preferred node 0
	if dist[0] != 7 {
		t.Errorf("preferred node: expected 7, got %d", dist[0])
	}
	if dist[1] != 3 {
		t.Errorf("other node: expected 3, got %d", dist[1])
	}
}

// ──────────────────────────────────────────────────────────────────────────────
// Memory Bandwidth
// ──────────────────────────────────────────────────────────────────────────────

func TestDetectMemoryBandwidth_Runs(t *testing.T) {
	mb := DetectMemoryBandwidth()
	if mb == nil {
		t.Fatal("DetectMemoryBandwidth should never return nil")
	}
	if mb.MaxStreams < 2 {
		t.Errorf("MaxStreams should be >= 2, got %d", mb.MaxStreams)
	}
	t.Logf("Bandwidth: %.0f GB/s, Channels: %d, MaxStreams: %d", mb.EstBandwidthGBps, mb.ChannelCount, mb.MaxStreams)
}

// ──────────────────────────────────────────────────────────────────────────────
// Unified Optimization
// ──────────────────────────────────────────────────────────────────────────────

func TestDetectOptimizations_NotNil(t *testing.T) {
	info := &CPUInfo{
		Vendor:        "AuthenticAMD",
		ModelName:     "AMD Ryzen 9 7950X",
		PhysicalCores: 16,
		LogicalCores:  32,
		Features:      []string{"sse4_2", "avx", "avx2", "aes"},
	}
	opt := DetectOptimizations(info)
	if opt == nil {
		t.Fatal("DetectOptimizations should never return nil")
	}
	if opt.Features == nil {
		t.Error("Features should not be nil")
	}
	if opt.Tuning == nil {
		t.Error("Tuning should not be nil")
	}
}

func TestFormatReport_Runs(t *testing.T) {
	info := &CPUInfo{
		Vendor:        "GenuineIntel",
		ModelName:     "Intel Core i7-13700K",
		PhysicalCores: 16,
		LogicalCores:  24,
		Features:      []string{"sse4_2", "avx", "avx2", "aes"},
	}
	opt := DetectOptimizations(info)
	report := opt.FormatReport()
	if len(report) < 100 {
		t.Errorf("report too short (%d chars)", len(report))
	}
	// Should contain key sections
	for _, section := range []string{"ISA Features", "Hybrid Core", "Cache", "NUMA", "Frequency", "Memory Bandwidth", "Vendor-Specific"} {
		if !containsSubstring(report, section) {
			t.Errorf("report missing section: %s", section)
		}
	}
}

func TestDetectOptimizations_LiveSystem(t *testing.T) {
	// End-to-end test using actual hardware
	d := NewDetector()
	info, err := d.DetectCPU()
	if err != nil {
		t.Skipf("could not detect CPU: %v", err)
	}

	opt := DetectOptimizations(info)
	if opt.Tuning == nil {
		t.Fatal("Tuning should not be nil")
	}

	t.Logf("Vendor: %s", opt.Tuning.VendorName)
	t.Logf("Jobs: %d, Dump: %d", opt.Tuning.RecommendedJobs, opt.Tuning.RecommendedDump)
	t.Logf("Compression: %s", opt.Tuning.CompressionAlgo)
	t.Logf("Batch: %d, Buffer: %dKB", opt.Tuning.BatchSizeHint, opt.Tuning.StreamBufferKB)

	if opt.Tuning.RecommendedJobs < 1 {
		t.Error("recommended jobs should be >= 1")
	}
	if opt.Tuning.RecommendedDump < 1 {
		t.Error("recommended dump jobs should be >= 1")
	}

	report := opt.FormatReport()
	t.Logf("\n%s", report)
}

// ──────────────────────────────────────────────────────────────────────────────
// Benchmark
// ──────────────────────────────────────────────────────────────────────────────

func BenchmarkDetectOptimizations(b *testing.B) {
	info := &CPUInfo{
		Vendor:        "AuthenticAMD",
		ModelName:     "AMD EPYC 7763",
		PhysicalCores: 64,
		LogicalCores:  128,
		Features:      []string{"sse4_2", "avx", "avx2", "aes", "sha_ni"},
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		DetectOptimizations(info)
	}
}

// containsSubstring is a helper for tests
func containsSubstring(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(s) > 0 && searchSubstring(s, substr))
}

func searchSubstring(s, sub string) bool {
	for i := 0; i <= len(s)-len(sub); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}
