package cpu

import (
	"bufio"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
)

// ──────────────────────────────────────────────────────────────────────────────
// CPU Feature / ISA Extension Detection
// ──────────────────────────────────────────────────────────────────────────────

// CPUFeatures summarises hardware capabilities relevant to backup workloads.
type CPUFeatures struct {
	HasSSE42     bool `json:"has_sse42"`      // CRC32C hardware
	HasAVX       bool `json:"has_avx"`        // 256-bit SIMD
	HasAVX2      bool `json:"has_avx2"`       // Improved 256-bit SIMD (zstd fast path)
	HasAVX512    bool `json:"has_avx512"`     // 512-bit SIMD (zstd, some Intel down-clocks)
	HasAVX512VL  bool `json:"has_avx512vl"`   // AVX-512 Vector Length (Zen 4+)
	HasAVX512BW  bool `json:"has_avx512bw"`   // AVX-512 Byte and Word
	HasAVX512VBI bool `json:"has_avx512vbmi"` // AVX-512 VBMI  (Zen 4 zstd fast path)
	HasAESNI     bool `json:"has_aesni"`      // AES-NI (encryption perf)
	HasPCLMULQDQ bool `json:"has_pclmulqdq"`  // Carry-less multiply (checksums)
	HasSHA       bool `json:"has_sha"`        // SHA-NI
	HasNEON      bool `json:"has_neon"`       // ARM NEON (ARM64 always has it)
	HasSVE       bool `json:"has_sve"`        // ARM SVE (Graviton 3+)
	HasCRC32     bool `json:"has_crc32"`      // ARM CRC32 extension
}

// DetectFeatures populates CPUFeatures from the flags already collected in
// CPUInfo.Features (Linux /proc/cpuinfo "flags" or "Features" line).
// On ARM64 some features are always present.
func DetectFeatures(info *CPUInfo) *CPUFeatures {
	f := &CPUFeatures{}
	if info == nil {
		return f
	}

	set := make(map[string]bool, len(info.Features))
	for _, flag := range info.Features {
		set[strings.ToLower(flag)] = true
	}

	// x86 flags (from /proc/cpuinfo "flags" line)
	f.HasSSE42 = set["sse4_2"]
	f.HasAVX = set["avx"]
	f.HasAVX2 = set["avx2"]
	f.HasAVX512 = set["avx512f"]
	f.HasAVX512VL = set["avx512vl"]
	f.HasAVX512BW = set["avx512bw"]
	f.HasAVX512VBI = set["avx512vbmi"]
	f.HasAESNI = set["aes"]
	f.HasPCLMULQDQ = set["pclmulqdq"]
	f.HasSHA = set["sha_ni"] || set["sha"]

	// ARM flags (from /proc/cpuinfo "Features" or always-present on aarch64)
	if runtime.GOARCH == "arm64" {
		f.HasNEON = true // aarch64 mandates NEON
		f.HasCRC32 = set["crc32"] || true
	} else {
		f.HasNEON = set["neon"] || set["asimd"]
		f.HasCRC32 = set["crc32"]
	}
	f.HasSVE = set["sve"]

	return f
}

// RecommendCompression returns the best compression algorithm based on ISA.
//
//	"zstd"  — when AVX2 or better is available (zstd has SIMD fast-paths)
//	"gzip"  — safe fallback that works everywhere
func (f *CPUFeatures) RecommendCompression(vendor string) string {
	if f == nil {
		return "gzip"
	}
	// AMD Zen 4+ has excellent AVX-512 VBMI for zstd
	if f.HasAVX512VBI {
		return "zstd"
	}
	// AVX2 → zstd is still a clear win
	if f.HasAVX2 {
		return "zstd"
	}
	// ARM SVE (Graviton 3+) → zstd
	if f.HasSVE {
		return "zstd"
	}
	return "gzip"
}

// SupportsHardwareCRC returns true when hardware CRC32C is available (faster
// checksum verification for backup integrity).
func (f *CPUFeatures) SupportsHardwareCRC() bool {
	return f != nil && (f.HasSSE42 || f.HasCRC32)
}

// SupportsHardwareAES returns true when AES-NI / ARM crypto is available,
// meaning encryption overhead during backup will be negligible.
func (f *CPUFeatures) SupportsHardwareAES() bool {
	return f != nil && f.HasAESNI
}

// GOAMD64Level returns the highest microarchitecture level supported on
// this CPU.  v1 = baseline x86-64, v2 = SSE4.2+POPCNT, v3 = AVX2+MOVBE,
// v4 = AVX-512.
func (f *CPUFeatures) GOAMD64Level() int {
	if f == nil || runtime.GOARCH != "amd64" {
		return 1
	}
	if f.HasAVX512 {
		return 4
	}
	if f.HasAVX2 {
		return 3
	}
	if f.HasSSE42 {
		return 2
	}
	return 1
}

// ──────────────────────────────────────────────────────────────────────────────
// Intel Hybrid (P-core / E-core) Detection
// ──────────────────────────────────────────────────────────────────────────────

// HybridTopology describes an Intel Alder Lake+ (or similar) hybrid CPU.
type HybridTopology struct {
	IsHybrid    bool  `json:"is_hybrid"`
	PCores      []int `json:"p_cores"`       // Logical CPU IDs of P-cores
	ECores      []int `json:"e_cores"`       // Logical CPU IDs of E-cores
	PcoreCount  int   `json:"p_core_count"`
	EcoreCount  int   `json:"e_core_count"`
	TotalCores  int   `json:"total_cores"`
}

// DetectHybridTopology checks for Intel hybrid (big.LITTLE) topology by
// reading each CPU's core_cpus_list and intel_pstate/base_frequency or
// comparing max frequencies.  Only meaningful on Linux x86_64.
func DetectHybridTopology() *HybridTopology {
	ht := &HybridTopology{}
	if runtime.GOOS != "linux" || runtime.GOARCH != "amd64" {
		return ht
	}

	// Strategy: read max_freq for each logical CPU.  P-cores run at a
	// higher max frequency than E-cores.  Partition by the median.
	type cpuFreq struct {
		id      int
		maxFreq int
	}

	var cpus []cpuFreq
	entries, err := os.ReadDir("/sys/devices/system/cpu")
	if err != nil {
		return ht
	}

	for _, entry := range entries {
		name := entry.Name()
		if !strings.HasPrefix(name, "cpu") {
			continue
		}
		idStr := strings.TrimPrefix(name, "cpu")
		id, err := strconv.Atoi(idStr)
		if err != nil {
			continue
		}

		freqPath := filepath.Join("/sys/devices/system/cpu", name, "cpufreq", "cpuinfo_max_freq")
		data, err := os.ReadFile(freqPath)
		if err != nil {
			continue
		}
		freq, err := strconv.Atoi(strings.TrimSpace(string(data)))
		if err != nil {
			continue
		}
		cpus = append(cpus, cpuFreq{id: id, maxFreq: freq})
	}

	if len(cpus) < 2 {
		return ht
	}

	// Sort by frequency descending
	sort.Slice(cpus, func(i, j int) bool {
		return cpus[i].maxFreq > cpus[j].maxFreq
	})

	// Find the frequency gap — P-cores have distinctly higher max freq.
	// If max - min < 15% of max, this is not a hybrid chip.
	maxF := cpus[0].maxFreq
	minF := cpus[len(cpus)-1].maxFreq
	spread := float64(maxF-minF) / float64(maxF)
	if spread < 0.15 {
		// All cores roughly the same → not hybrid
		return ht
	}

	// Determine threshold: use midpoint between highest and lowest
	threshold := (maxF + minF) / 2

	for _, c := range cpus {
		if c.maxFreq > threshold {
			ht.PCores = append(ht.PCores, c.id)
		} else {
			ht.ECores = append(ht.ECores, c.id)
		}
	}

	if len(ht.ECores) > 0 && len(ht.PCores) > 0 {
		ht.IsHybrid = true
		ht.PcoreCount = len(ht.PCores)
		ht.EcoreCount = len(ht.ECores)
		ht.TotalCores = ht.PcoreCount + ht.EcoreCount
	}

	return ht
}

// EffectiveCoresForWork returns the number of cores that should be used for
// heavy work (backup/compression).  On hybrid chips this equals the P-core
// count; on homogeneous chips it returns the full logical core count.
func (ht *HybridTopology) EffectiveCoresForWork() int {
	if ht != nil && ht.IsHybrid {
		return ht.PcoreCount
	}
	return runtime.NumCPU()
}

// ──────────────────────────────────────────────────────────────────────────────
// Cache-Aware Buffer Sizing
// ──────────────────────────────────────────────────────────────────────────────

// CacheTopology holds L1/L2/L3 sizes (bytes).
type CacheTopology struct {
	L1DataKB int `json:"l1d_kb"`
	L2KB     int `json:"l2_kb"`
	L3KB     int `json:"l3_kb"`
}

// DetectCacheTopology reads cache sizes from the system.
func DetectCacheTopology() *CacheTopology {
	ct := &CacheTopology{}

	switch runtime.GOOS {
	case "linux":
		detectLinuxCache(ct)
	case "darwin":
		detectDarwinCache(ct)
	}

	return ct
}

func detectLinuxCache(ct *CacheTopology) {
	// Try /sys/devices/system/cpu/cpu0/cache/
	base := "/sys/devices/system/cpu/cpu0/cache"
	for i := 0; i < 10; i++ {
		dir := filepath.Join(base, fmt.Sprintf("index%d", i))
		typeData, err := os.ReadFile(filepath.Join(dir, "type"))
		if err != nil {
			continue
		}
		levelData, err := os.ReadFile(filepath.Join(dir, "level"))
		if err != nil {
			continue
		}
		sizeData, err := os.ReadFile(filepath.Join(dir, "size"))
		if err != nil {
			continue
		}

		cacheType := strings.TrimSpace(string(typeData))
		level, _ := strconv.Atoi(strings.TrimSpace(string(levelData)))
		sizeKB := parseCacheSize(strings.TrimSpace(string(sizeData)))

		switch {
		case level == 1 && cacheType == "Data":
			ct.L1DataKB = sizeKB
		case level == 2:
			ct.L2KB = sizeKB
		case level == 3:
			ct.L3KB = sizeKB
		}
	}

	// Fallback to lscpu
	if ct.L3KB == 0 {
		if out, err := exec.Command("lscpu").Output(); err == nil {
			for _, line := range strings.Split(string(out), "\n") {
				if strings.Contains(line, "L3 cache:") {
					parts := strings.SplitN(line, ":", 2)
					if len(parts) == 2 {
						ct.L3KB = parseCacheSize(strings.TrimSpace(parts[1]))
					}
				}
			}
		}
	}
}

func detectDarwinCache(ct *CacheTopology) {
	read := func(key string) int {
		out, err := exec.Command("sysctl", "-n", key).Output()
		if err != nil {
			return 0
		}
		v, _ := strconv.Atoi(strings.TrimSpace(string(out)))
		return v / 1024 // bytes → KB
	}
	ct.L1DataKB = read("hw.l1dcachesize")
	ct.L2KB = read("hw.l2cachesize")
	ct.L3KB = read("hw.l3cachesize")
}

// parseCacheSize parses strings like "32K", "1024K", "16M", "32768".
func parseCacheSize(s string) int {
	s = strings.TrimSpace(s)
	s = strings.ReplaceAll(s, " ", "")

	if strings.HasSuffix(s, "M") || strings.HasSuffix(s, "MiB") {
		numStr := strings.TrimSuffix(strings.TrimSuffix(s, "MiB"), "M")
		if v, err := strconv.Atoi(numStr); err == nil {
			return v * 1024
		}
	}
	if strings.HasSuffix(s, "K") || strings.HasSuffix(s, "KiB") {
		numStr := strings.TrimSuffix(strings.TrimSuffix(s, "KiB"), "K")
		if v, err := strconv.Atoi(numStr); err == nil {
			return v
		}
	}
	if v, err := strconv.Atoi(s); err == nil {
		return v // assume KB already
	}
	return 0
}

// OptimalBufferSizeKB returns a buffer size (KB) that fits well in L3 cache.
// For very large L3s (EPYC 256 MB) it caps at 16 MB to avoid excessive
// single-buffer allocation.
func (ct *CacheTopology) OptimalBufferSizeKB() int {
	if ct == nil || ct.L3KB == 0 {
		return 256 // 256 KB default
	}

	// Use ~1/4 of L3 per core, capped between 256 KB and 16 MB
	cores := runtime.NumCPU()
	if cores < 1 {
		cores = 1
	}
	perCore := ct.L3KB / cores
	bufferKB := perCore / 4

	if bufferKB < 256 {
		bufferKB = 256
	}
	if bufferKB > 16384 { // 16 MB cap
		bufferKB = 16384
	}
	return bufferKB
}

// OptimalBatchSize returns a good row-batch size for the native SQL engine.
// Larger L3 → larger batches stay hot.
func (ct *CacheTopology) OptimalBatchSize() int {
	if ct == nil || ct.L3KB == 0 {
		return 5000 // current default
	}

	l3MB := ct.L3KB / 1024
	switch {
	case l3MB >= 128: // EPYC
		return 20000
	case l3MB >= 32: // Xeon / Ryzen 9
		return 10000
	case l3MB >= 16: // Mainstream desktop
		return 7500
	case l3MB >= 8: // Mainstream mobile
		return 5000
	default:
		return 3000
	}
}

// ──────────────────────────────────────────────────────────────────────────────
// Vendor-Aware Parallelism Tuning
// ──────────────────────────────────────────────────────────────────────────────

// VendorClass categorises the CPU vendor into archetypes.
type VendorClass int

const (
	VendorUnknown VendorClass = iota
	VendorIntel
	VendorAMD
	VendorARM    // Ampere, Graviton, Apple Silicon
	VendorApple  // Apple M-series
)

// ClassifyVendor maps vendor string → VendorClass.
func ClassifyVendor(vendor, model string) VendorClass {
	v := strings.ToLower(vendor)
	m := strings.ToLower(model)

	switch {
	case strings.Contains(v, "genuineintel"):
		return VendorIntel
	case strings.Contains(v, "authenticamd"):
		return VendorAMD
	case strings.Contains(v, "apple") || strings.Contains(m, "apple"):
		return VendorApple
	case runtime.GOARCH == "arm64":
		return VendorARM
	}
	return VendorUnknown
}

// VendorTuning holds vendor-specific parallelism / batch recommendations.
type VendorTuning struct {
	Vendor            VendorClass `json:"vendor"`
	VendorName        string      `json:"vendor_name"`
	RecommendedJobs   int         `json:"recommended_jobs"`
	RecommendedDump   int         `json:"recommended_dump_jobs"`
	BatchSizeHint     int         `json:"batch_size_hint"`    // Native engine rows/INSERT
	StreamBufferKB    int         `json:"stream_buffer_kb"`   // Streaming I/O buffer
	CompressionAlgo   string      `json:"compression_algo"`   // "zstd" or "gzip"
	Notes             string      `json:"notes"`
	AVX512Throttle    bool        `json:"avx512_throttle"`    // Intel may down-clock with AVX-512
}

// ComputeVendorTuning produces parallelism / batch recommendations that
// respect the architectural strengths of Intel vs AMD vs ARM chips.
func ComputeVendorTuning(info *CPUInfo, features *CPUFeatures, cache *CacheTopology, hybrid *HybridTopology) *VendorTuning {
	if info == nil {
		return &VendorTuning{Vendor: VendorUnknown, VendorName: "unknown"}
	}

	vc := ClassifyVendor(info.Vendor, info.ModelName)
	t := &VendorTuning{
		Vendor:     vc,
		VendorName: info.Vendor,
	}

	physical := info.PhysicalCores
	if physical < 1 {
		physical = info.LogicalCores
	}
	if physical < 1 {
		physical = runtime.NumCPU()
	}

	// Effective cores for heavy work (respects P/E-core split)
	eff := physical
	if hybrid != nil && hybrid.IsHybrid {
		eff = hybrid.PcoreCount
	}

	switch vc {
	case VendorAMD:
		// AMD: many physical cores, excellent multi-core scaling.
		// Use more parallel workers.
		t.RecommendedJobs = eff       // 1 job per physical core
		t.RecommendedDump = eff * 3 / 4
		if t.RecommendedDump < 2 {
			t.RecommendedDump = 2
		}
		t.Notes = "AMD: high core count, using aggressive parallelism"

		// AMD Zen 4+ with AVX-512 doesn't down-clock
		t.AVX512Throttle = false

	case VendorIntel:
		// Intel: strong single-thread but some SKUs down-clock with AVX-512.
		// Use slightly fewer workers with larger batch sizes.
		t.RecommendedJobs = eff * 3 / 4
		if t.RecommendedJobs < 1 {
			t.RecommendedJobs = 1
		}
		t.RecommendedDump = eff / 2
		if t.RecommendedDump < 2 {
			t.RecommendedDump = 2
		}
		t.Notes = "Intel: strong single-thread, moderate parallelism"

		// AVX-512 down-clocking on non-Sapphire-Rapids
		if features != nil && features.HasAVX512 {
			model := strings.ToLower(info.ModelName)
			// Sapphire Rapids, Emerald Rapids, Sierra Forest don't throttle
			safeAVX512 := strings.Contains(model, "sapphire") ||
				strings.Contains(model, "emerald") ||
				strings.Contains(model, "sierra") ||
				strings.Contains(model, "granite")
			t.AVX512Throttle = !safeAVX512
		}

	case VendorApple:
		// Apple Silicon: unified memory, excellent efficiency.
		t.RecommendedJobs = eff
		t.RecommendedDump = eff * 3 / 4
		if t.RecommendedDump < 2 {
			t.RecommendedDump = 2
		}
		t.Notes = "Apple Silicon: unified memory, full core utilisation"

	case VendorARM:
		// Generic ARM (Graviton, Ampere): high core count like AMD.
		t.RecommendedJobs = eff
		t.RecommendedDump = eff * 3 / 4
		if t.RecommendedDump < 2 {
			t.RecommendedDump = 2
		}
		t.Notes = "ARM server: high core count, aggressive parallelism"

	default:
		t.RecommendedJobs = eff
		t.RecommendedDump = eff / 2
		if t.RecommendedDump < 2 {
			t.RecommendedDump = 2
		}
		t.Notes = "Unknown vendor: using defaults"
	}

	// Cap sanity
	if t.RecommendedJobs > 64 {
		t.RecommendedJobs = 64
	}
	if t.RecommendedDump > 32 {
		t.RecommendedDump = 32
	}

	// Compression recommendation
	if features != nil {
		t.CompressionAlgo = features.RecommendCompression(info.Vendor)
	} else {
		t.CompressionAlgo = "gzip"
	}

	// Buffer / batch sizing from cache
	if cache != nil {
		t.StreamBufferKB = cache.OptimalBufferSizeKB()
		t.BatchSizeHint = cache.OptimalBatchSize()
	} else {
		t.StreamBufferKB = 256
		t.BatchSizeHint = 5000
	}

	return t
}

// ──────────────────────────────────────────────────────────────────────────────
// CPU Frequency Governor Detection
// ──────────────────────────────────────────────────────────────────────────────

// FreqGovernor describes the active frequency scaling policy.
type FreqGovernor struct {
	Governor        string `json:"governor"`           // e.g. "performance", "powersave", "schedutil"
	CurrentFreqKHz  int    `json:"current_freq_khz"`
	MinFreqKHz      int    `json:"min_freq_khz"`
	MaxFreqKHz      int    `json:"max_freq_khz"`
	IsOptimal       bool   `json:"is_optimal"`         // true when "performance" or "schedutil"
	Recommendation  string `json:"recommendation"`
}

// DetectFreqGovernor reads the CPU frequency governor for cpu0.
func DetectFreqGovernor() *FreqGovernor {
	fg := &FreqGovernor{}
	if runtime.GOOS != "linux" {
		fg.Governor = "n/a"
		fg.IsOptimal = true
		fg.Recommendation = "Frequency governor detection not available on this OS"
		return fg
	}

	readInt := func(path string) int {
		data, err := os.ReadFile(path)
		if err != nil {
			return 0
		}
		v, _ := strconv.Atoi(strings.TrimSpace(string(data)))
		return v
	}

	base := "/sys/devices/system/cpu/cpu0/cpufreq"
	fg.CurrentFreqKHz = readInt(filepath.Join(base, "scaling_cur_freq"))
	fg.MinFreqKHz = readInt(filepath.Join(base, "scaling_min_freq"))
	fg.MaxFreqKHz = readInt(filepath.Join(base, "scaling_max_freq"))

	data, err := os.ReadFile(filepath.Join(base, "scaling_governor"))
	if err != nil {
		fg.Governor = "unknown"
		fg.Recommendation = "Could not read frequency governor"
		return fg
	}
	fg.Governor = strings.TrimSpace(string(data))

	switch fg.Governor {
	case "performance":
		fg.IsOptimal = true
		fg.Recommendation = "Governor is 'performance' — CPU runs at max frequency. Optimal for backup workloads."
	case "schedutil":
		fg.IsOptimal = true
		fg.Recommendation = "Governor is 'schedutil' — kernel scheduler drives frequency. Good for backup workloads."
	case "ondemand":
		fg.IsOptimal = false
		fg.Recommendation = "Governor is 'ondemand' — consider 'performance' for backup windows: sudo cpupower frequency-set -g performance"
	case "powersave":
		fg.IsOptimal = false
		fg.Recommendation = "Governor is 'powersave' — CPU throttled! For backup windows: sudo cpupower frequency-set -g performance"
	case "conservative":
		fg.IsOptimal = false
		fg.Recommendation = "Governor is 'conservative' — slow ramp-up may hurt burst I/O. Consider 'performance' during backups."
	default:
		fg.IsOptimal = true
		fg.Recommendation = fmt.Sprintf("Governor '%s' detected. If backups are slow, try: sudo cpupower frequency-set -g performance", fg.Governor)
	}

	return fg
}

// ──────────────────────────────────────────────────────────────────────────────
// NUMA Topology Detection
// ──────────────────────────────────────────────────────────────────────────────

// NUMATopology describes the system's NUMA layout.
type NUMATopology struct {
	NodeCount  int        `json:"node_count"`
	Nodes      []NUMANode `json:"nodes"`
	IsNUMA     bool       `json:"is_numa"`       // more than 1 node
}

// NUMANode describes one NUMA node.
type NUMANode struct {
	ID        int    `json:"id"`
	CPUs      []int  `json:"cpus"`       // logical CPU IDs
	MemoryMB  int    `json:"memory_mb"`
	DistanceToSelf int `json:"distance_self"` // normally 10
}

// DetectNUMATopology reads NUMA information from /sys on Linux.
func DetectNUMATopology() *NUMATopology {
	nt := &NUMATopology{}
	if runtime.GOOS != "linux" {
		return nt
	}

	nodesDir := "/sys/devices/system/node"
	entries, err := os.ReadDir(nodesDir)
	if err != nil {
		return nt
	}

	for _, entry := range entries {
		name := entry.Name()
		if !strings.HasPrefix(name, "node") {
			continue
		}
		idStr := strings.TrimPrefix(name, "node")
		id, err := strconv.Atoi(idStr)
		if err != nil {
			continue
		}

		node := NUMANode{ID: id}

		// Read CPUs for this node
		cpuListPath := filepath.Join(nodesDir, name, "cpulist")
		if data, err := os.ReadFile(cpuListPath); err == nil {
			node.CPUs = parseCPUList(strings.TrimSpace(string(data)))
		}

		// Read memory for this node
		meminfoPath := filepath.Join(nodesDir, name, "meminfo")
		if file, err := os.Open(meminfoPath); err == nil {
			scanner := bufio.NewScanner(file)
			for scanner.Scan() {
				line := scanner.Text()
				if strings.Contains(line, "MemTotal") {
					fields := strings.Fields(line)
					if len(fields) >= 4 {
						if kb, err := strconv.Atoi(fields[3]); err == nil {
							node.MemoryMB = kb / 1024
						}
					}
				}
			}
			_ = file.Close()
		}

		nt.Nodes = append(nt.Nodes, node)
	}

	nt.NodeCount = len(nt.Nodes)
	nt.IsNUMA = nt.NodeCount > 1

	return nt
}

// parseCPUList parses "0-3,5,7-9" → [0,1,2,3,5,7,8,9]
func parseCPUList(s string) []int {
	var result []int
	for _, part := range strings.Split(s, ",") {
		part = strings.TrimSpace(part)
		if strings.Contains(part, "-") {
			bounds := strings.SplitN(part, "-", 2)
			lo, err1 := strconv.Atoi(bounds[0])
			hi, err2 := strconv.Atoi(bounds[1])
			if err1 == nil && err2 == nil {
				for i := lo; i <= hi; i++ {
					result = append(result, i)
				}
			}
		} else {
			if v, err := strconv.Atoi(part); err == nil {
				result = append(result, v)
			}
		}
	}
	return result
}

// NUMARecommendedWorkerDistribution divides N workers across NUMA nodes,
// returning the count of workers to run on each node.
// When a DB socket lives on a known node, bias workers toward that node.
func (nt *NUMATopology) NUMARecommendedWorkerDistribution(totalWorkers int, preferNodeID int) map[int]int {
	dist := make(map[int]int)
	if nt == nil || !nt.IsNUMA || len(nt.Nodes) == 0 {
		dist[0] = totalWorkers
		return dist
	}

	if preferNodeID >= 0 {
		// 70% of workers on preferred node, rest spread evenly
		preferred := totalWorkers * 70 / 100
		if preferred < 1 {
			preferred = 1
		}
		remainder := totalWorkers - preferred
		dist[preferNodeID] = preferred

		otherNodes := 0
		for _, n := range nt.Nodes {
			if n.ID != preferNodeID {
				otherNodes++
			}
		}
		if otherNodes > 0 {
			each := remainder / otherNodes
			if each < 1 {
				each = 1
			}
			for _, n := range nt.Nodes {
				if n.ID != preferNodeID {
					dist[n.ID] = each
				}
			}
		}
	} else {
		// Distribute evenly
		each := totalWorkers / len(nt.Nodes)
		if each < 1 {
			each = 1
		}
		for _, n := range nt.Nodes {
			dist[n.ID] = each
		}
	}

	return dist
}

// ──────────────────────────────────────────────────────────────────────────────
// Memory Bandwidth Estimation
// ──────────────────────────────────────────────────────────────────────────────

// MemoryBandwidthInfo describes estimated memory bandwidth capability.
type MemoryBandwidthInfo struct {
	ChannelCount     int    `json:"channel_count"`      // detected channels
	SpeedMT          int    `json:"speed_mt"`           // MT/s (megatransfers)
	TechnologyGen    string `json:"technology_gen"`      // DDR4 / DDR5
	EstBandwidthGBps float64 `json:"est_bandwidth_gbps"` // estimated bandwidth
	MaxStreams       int    `json:"max_parallel_streams"` // recommended parallel I/O streams
}

// DetectMemoryBandwidth estimates memory bandwidth from dmidecode or lshw.
func DetectMemoryBandwidth() *MemoryBandwidthInfo {
	mb := &MemoryBandwidthInfo{}
	if runtime.GOOS != "linux" {
		return mb
	}

	// Try dmidecode first (needs root but most DB servers run as root)
	if out, err := exec.Command("dmidecode", "-t", "memory").Output(); err == nil {
		parseMemoryDMI(string(out), mb)
	}

	// Fallback estimate
	if mb.ChannelCount == 0 {
		// Detect from number of NUMA nodes — rough heuristic
		numa := DetectNUMATopology()
		if numa.IsNUMA {
			// Multi-node usually means multi-channel
			mb.ChannelCount = numa.NodeCount * 4 // typical 4 channels per socket
		} else {
			mb.ChannelCount = 2 // assume dual-channel
		}
	}

	// Estimate bandwidth
	if mb.SpeedMT > 0 && mb.ChannelCount > 0 {
		// Bandwidth = channels × speed × 8 bytes per transfer
		mb.EstBandwidthGBps = float64(mb.ChannelCount) * float64(mb.SpeedMT) * 8.0 / 1000.0
	} else {
		// Default estimates based on channel count
		switch {
		case mb.ChannelCount >= 8:
			mb.EstBandwidthGBps = 300 // 8-channel DDR5
		case mb.ChannelCount >= 4:
			mb.EstBandwidthGBps = 100 // 4-channel DDR4/5
		default:
			mb.EstBandwidthGBps = 40 // dual-channel DDR4
		}
	}

	// Max parallel streams: ~20 GB/s per stream needed for full-speed I/O
	mb.MaxStreams = int(mb.EstBandwidthGBps / 20)
	if mb.MaxStreams < 2 {
		mb.MaxStreams = 2
	}
	if mb.MaxStreams > 32 {
		mb.MaxStreams = 32
	}

	return mb
}

func parseMemoryDMI(output string, mb *MemoryBandwidthInfo) {
	lines := strings.Split(output, "\n")
	dimmCount := 0
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if strings.HasPrefix(line, "Speed:") && !strings.Contains(line, "Unknown") && !strings.Contains(line, "Configured") {
			parts := strings.Fields(line)
			if len(parts) >= 2 {
				if speed, err := strconv.Atoi(parts[1]); err == nil {
					if speed > mb.SpeedMT {
						mb.SpeedMT = speed
					}
				}
			}
		}
		if strings.HasPrefix(line, "Type:") {
			if strings.Contains(line, "DDR5") {
				mb.TechnologyGen = "DDR5"
			} else if strings.Contains(line, "DDR4") {
				mb.TechnologyGen = "DDR4"
			} else if strings.Contains(line, "DDR3") {
				mb.TechnologyGen = "DDR3"
			}
		}
		if strings.HasPrefix(line, "Size:") && !strings.Contains(line, "No Module") {
			dimmCount++
		}
	}
	mb.ChannelCount = dimmCount
}

// ──────────────────────────────────────────────────────────────────────────────
// Unified Optimization Summary
// ──────────────────────────────────────────────────────────────────────────────

// OptimizationSummary collects all optimisation data into one struct.
type OptimizationSummary struct {
	Features     *CPUFeatures         `json:"features"`
	Hybrid       *HybridTopology      `json:"hybrid"`
	Cache        *CacheTopology       `json:"cache"`
	NUMA         *NUMATopology        `json:"numa"`
	FreqGov      *FreqGovernor        `json:"freq_governor"`
	MemBandwidth *MemoryBandwidthInfo `json:"mem_bandwidth"`
	Tuning       *VendorTuning        `json:"tuning"`
}

// DetectOptimizations runs all detection passes and returns a unified summary.
func DetectOptimizations(info *CPUInfo) *OptimizationSummary {
	features := DetectFeatures(info)
	hybrid := DetectHybridTopology()
	cache := DetectCacheTopology()
	numa := DetectNUMATopology()
	freqGov := DetectFreqGovernor()
	memBW := DetectMemoryBandwidth()
	tuning := ComputeVendorTuning(info, features, cache, hybrid)

	return &OptimizationSummary{
		Features:     features,
		Hybrid:       hybrid,
		Cache:        cache,
		NUMA:         numa,
		FreqGov:      freqGov,
		MemBandwidth: memBW,
		Tuning:       tuning,
	}
}

// FormatReport produces a human-readable text report of all optimisations.
func (s *OptimizationSummary) FormatReport() string {
	var sb strings.Builder

	sb.WriteString("=== CPU Optimization Report ===\n\n")

	// ISA Features
	sb.WriteString("[ISA Features]\n")
	if s.Features != nil {
		features := []struct {
			name string
			has  bool
		}{
			{"SSE 4.2 (CRC32C)", s.Features.HasSSE42},
			{"AVX", s.Features.HasAVX},
			{"AVX2", s.Features.HasAVX2},
			{"AVX-512", s.Features.HasAVX512},
			{"AVX-512 VBMI", s.Features.HasAVX512VBI},
			{"AES-NI", s.Features.HasAESNI},
			{"PCLMULQDQ", s.Features.HasPCLMULQDQ},
			{"SHA-NI", s.Features.HasSHA},
			{"NEON", s.Features.HasNEON},
			{"SVE", s.Features.HasSVE},
		}
		for _, feat := range features {
			mark := "✗"
			if feat.has {
				mark = "✓"
			}
			sb.WriteString(fmt.Sprintf("  %s %s\n", mark, feat.name))
		}
		sb.WriteString(fmt.Sprintf("  GOAMD64 level: v%d\n", s.Features.GOAMD64Level()))
		sb.WriteString(fmt.Sprintf("  Recommended compression: %s\n", s.Features.RecommendCompression("")))
	}
	sb.WriteString("\n")

	// Hybrid Topology
	sb.WriteString("[Hybrid Core Topology]\n")
	if s.Hybrid != nil && s.Hybrid.IsHybrid {
		sb.WriteString("  Hybrid: YES (Intel P/E-core)\n")
		sb.WriteString(fmt.Sprintf("  P-cores: %d  (used for heavy work)\n", s.Hybrid.PcoreCount))
		sb.WriteString(fmt.Sprintf("  E-cores: %d  (background / monitoring)\n", s.Hybrid.EcoreCount))
	} else {
		sb.WriteString("  Hybrid: NO (homogeneous cores)\n")
	}
	sb.WriteString("\n")

	// Cache
	sb.WriteString("[Cache Topology]\n")
	if s.Cache != nil {
		if s.Cache.L1DataKB > 0 {
			sb.WriteString(fmt.Sprintf("  L1d: %d KB\n", s.Cache.L1DataKB))
		}
		if s.Cache.L2KB > 0 {
			sb.WriteString(fmt.Sprintf("  L2:  %d KB\n", s.Cache.L2KB))
		}
		if s.Cache.L3KB > 0 {
			if s.Cache.L3KB >= 1024 {
				sb.WriteString(fmt.Sprintf("  L3:  %d MB\n", s.Cache.L3KB/1024))
			} else {
				sb.WriteString(fmt.Sprintf("  L3:  %d KB\n", s.Cache.L3KB))
			}
		}
		sb.WriteString(fmt.Sprintf("  Optimal buffer: %d KB\n", s.Cache.OptimalBufferSizeKB()))
		sb.WriteString(fmt.Sprintf("  Optimal batch size: %d rows\n", s.Cache.OptimalBatchSize()))
	}
	sb.WriteString("\n")

	// NUMA
	sb.WriteString("[NUMA Topology]\n")
	if s.NUMA != nil && s.NUMA.IsNUMA {
		sb.WriteString(fmt.Sprintf("  NUMA: YES (%d nodes)\n", s.NUMA.NodeCount))
		for _, node := range s.NUMA.Nodes {
			sb.WriteString(fmt.Sprintf("  Node %d: %d CPUs, %d MB memory\n", node.ID, len(node.CPUs), node.MemoryMB))
		}
		sb.WriteString("  Tip: Workers will be biased toward DB socket's NUMA node\n")
	} else {
		sb.WriteString("  NUMA: NO (single node / UMA)\n")
	}
	sb.WriteString("\n")

	// Frequency Governor
	sb.WriteString("[Frequency Governor]\n")
	if s.FreqGov != nil {
		sb.WriteString(fmt.Sprintf("  Governor: %s\n", s.FreqGov.Governor))
		if s.FreqGov.MaxFreqKHz > 0 {
			sb.WriteString(fmt.Sprintf("  Freq range: %d - %d MHz\n", s.FreqGov.MinFreqKHz/1000, s.FreqGov.MaxFreqKHz/1000))
		}
		if s.FreqGov.IsOptimal {
			sb.WriteString("  Status: ✓ Optimal\n")
		} else {
			sb.WriteString(fmt.Sprintf("  Status: ⚠ %s\n", s.FreqGov.Recommendation))
		}
	}
	sb.WriteString("\n")

	// Memory Bandwidth
	sb.WriteString("[Memory Bandwidth]\n")
	if s.MemBandwidth != nil {
		if s.MemBandwidth.TechnologyGen != "" {
			sb.WriteString(fmt.Sprintf("  Technology: %s\n", s.MemBandwidth.TechnologyGen))
		}
		sb.WriteString(fmt.Sprintf("  Channels: %d\n", s.MemBandwidth.ChannelCount))
		if s.MemBandwidth.SpeedMT > 0 {
			sb.WriteString(fmt.Sprintf("  Speed: %d MT/s\n", s.MemBandwidth.SpeedMT))
		}
		sb.WriteString(fmt.Sprintf("  Est. bandwidth: %.0f GB/s\n", s.MemBandwidth.EstBandwidthGBps))
		sb.WriteString(fmt.Sprintf("  Max parallel streams: %d\n", s.MemBandwidth.MaxStreams))
	}
	sb.WriteString("\n")

	// Vendor Tuning
	sb.WriteString("[Vendor-Specific Tuning]\n")
	if s.Tuning != nil {
		sb.WriteString(fmt.Sprintf("  %s\n", s.Tuning.Notes))
		sb.WriteString(fmt.Sprintf("  Recommended jobs: %d\n", s.Tuning.RecommendedJobs))
		sb.WriteString(fmt.Sprintf("  Recommended dump jobs: %d\n", s.Tuning.RecommendedDump))
		sb.WriteString(fmt.Sprintf("  Batch size hint: %d rows\n", s.Tuning.BatchSizeHint))
		sb.WriteString(fmt.Sprintf("  Stream buffer: %d KB\n", s.Tuning.StreamBufferKB))
		sb.WriteString(fmt.Sprintf("  Compression: %s\n", s.Tuning.CompressionAlgo))
		if s.Tuning.AVX512Throttle {
			sb.WriteString("  ⚠ AVX-512 may cause frequency throttling on this Intel SKU\n")
		}
	}

	return sb.String()
}
