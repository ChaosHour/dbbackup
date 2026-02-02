// Package performance provides comprehensive performance benchmarking and profiling
// infrastructure for dbbackup dump/restore operations.
//
// Performance Targets:
// - Dump throughput: 500 MB/s
// - Restore throughput: 300 MB/s
// - Memory usage: < 2GB regardless of database size
package performance

import (
	"context"
	"fmt"
	"io"
	"os"
	"runtime"
	"runtime/pprof"
	"sync"
	"sync/atomic"
	"time"
)

// BenchmarkResult contains the results of a performance benchmark
type BenchmarkResult struct {
	Name          string        `json:"name"`
	Operation     string        `json:"operation"` // "dump" or "restore"
	DataSizeBytes int64         `json:"data_size_bytes"`
	Duration      time.Duration `json:"duration"`
	Throughput    float64       `json:"throughput_mb_s"` // MB/s

	// Memory metrics
	AllocBytes      uint64 `json:"alloc_bytes"`
	TotalAllocBytes uint64 `json:"total_alloc_bytes"`
	HeapObjects     uint64 `json:"heap_objects"`
	NumGC           uint32 `json:"num_gc"`
	GCPauseTotal    uint64 `json:"gc_pause_total_ns"`

	// Goroutine metrics
	GoroutineCount int `json:"goroutine_count"`
	MaxGoroutines  int `json:"max_goroutines"`
	WorkerCount    int `json:"worker_count"`

	// CPU metrics
	CPUCores       int     `json:"cpu_cores"`
	CPUUtilization float64 `json:"cpu_utilization_percent"`

	// I/O metrics
	IOWaitPercent float64 `json:"io_wait_percent"`
	ReadBytes     int64   `json:"read_bytes"`
	WriteBytes    int64   `json:"write_bytes"`

	// Timing breakdown
	CompressionTime time.Duration `json:"compression_time"`
	IOTime          time.Duration `json:"io_time"`
	DBOperationTime time.Duration `json:"db_operation_time"`

	// Pass/Fail against targets
	MeetsTarget bool   `json:"meets_target"`
	TargetNotes string `json:"target_notes,omitempty"`
}

// PerformanceTargets defines the performance targets to benchmark against
var PerformanceTargets = struct {
	DumpThroughputMBs    float64
	RestoreThroughputMBs float64
	MaxMemoryBytes       int64
	MaxGoroutines        int
}{
	DumpThroughputMBs:    500.0,   // 500 MB/s dump throughput target
	RestoreThroughputMBs: 300.0,   // 300 MB/s restore throughput target
	MaxMemoryBytes:       2 << 30, // 2GB max memory
	MaxGoroutines:        1000,    // Reasonable goroutine limit
}

// Profiler manages CPU and memory profiling during benchmarks
type Profiler struct {
	cpuProfilePath string
	memProfilePath string
	cpuFile        *os.File
	enabled        bool
	mu             sync.Mutex
}

// NewProfiler creates a new profiler with the given output paths
func NewProfiler(cpuPath, memPath string) *Profiler {
	return &Profiler{
		cpuProfilePath: cpuPath,
		memProfilePath: memPath,
		enabled:        cpuPath != "" || memPath != "",
	}
}

// Start begins CPU profiling
func (p *Profiler) Start() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if !p.enabled || p.cpuProfilePath == "" {
		return nil
	}

	f, err := os.Create(p.cpuProfilePath)
	if err != nil {
		return fmt.Errorf("could not create CPU profile: %w", err)
	}
	p.cpuFile = f

	if err := pprof.StartCPUProfile(f); err != nil {
		f.Close()
		return fmt.Errorf("could not start CPU profile: %w", err)
	}

	return nil
}

// Stop stops CPU profiling and writes memory profile
func (p *Profiler) Stop() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if !p.enabled {
		return nil
	}

	// Stop CPU profile
	if p.cpuFile != nil {
		pprof.StopCPUProfile()
		if err := p.cpuFile.Close(); err != nil {
			return fmt.Errorf("could not close CPU profile: %w", err)
		}
	}

	// Write memory profile
	if p.memProfilePath != "" {
		f, err := os.Create(p.memProfilePath)
		if err != nil {
			return fmt.Errorf("could not create memory profile: %w", err)
		}
		defer f.Close()

		runtime.GC() // Get up-to-date statistics
		if err := pprof.WriteHeapProfile(f); err != nil {
			return fmt.Errorf("could not write memory profile: %w", err)
		}
	}

	return nil
}

// MemStats captures memory statistics at a point in time
type MemStats struct {
	Alloc          uint64
	TotalAlloc     uint64
	Sys            uint64
	HeapAlloc      uint64
	HeapObjects    uint64
	NumGC          uint32
	PauseTotalNs   uint64
	GoroutineCount int
	Timestamp      time.Time
}

// CaptureMemStats captures current memory statistics
func CaptureMemStats() MemStats {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	return MemStats{
		Alloc:          m.Alloc,
		TotalAlloc:     m.TotalAlloc,
		Sys:            m.Sys,
		HeapAlloc:      m.HeapAlloc,
		HeapObjects:    m.HeapObjects,
		NumGC:          m.NumGC,
		PauseTotalNs:   m.PauseTotalNs,
		GoroutineCount: runtime.NumGoroutine(),
		Timestamp:      time.Now(),
	}
}

// MetricsCollector collects performance metrics during operations
type MetricsCollector struct {
	startTime time.Time
	startMem  MemStats

	// Atomic counters for concurrent updates
	bytesRead    atomic.Int64
	bytesWritten atomic.Int64

	// Goroutine tracking
	maxGoroutines atomic.Int64
	sampleCount   atomic.Int64

	// Timing breakdown
	compressionNs atomic.Int64
	ioNs          atomic.Int64
	dbOperationNs atomic.Int64

	// Sampling goroutine
	stopCh chan struct{}
	doneCh chan struct{}
}

// NewMetricsCollector creates a new metrics collector
func NewMetricsCollector() *MetricsCollector {
	return &MetricsCollector{
		stopCh: make(chan struct{}),
		doneCh: make(chan struct{}),
	}
}

// Start begins collecting metrics
func (mc *MetricsCollector) Start() {
	mc.startTime = time.Now()
	mc.startMem = CaptureMemStats()
	mc.maxGoroutines.Store(int64(runtime.NumGoroutine()))

	// Start goroutine sampling
	go mc.sampleGoroutines()
}

func (mc *MetricsCollector) sampleGoroutines() {
	defer close(mc.doneCh)
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-mc.stopCh:
			return
		case <-ticker.C:
			count := int64(runtime.NumGoroutine())
			mc.sampleCount.Add(1)

			// Update max goroutines using compare-and-swap
			for {
				current := mc.maxGoroutines.Load()
				if count <= current {
					break
				}
				if mc.maxGoroutines.CompareAndSwap(current, count) {
					break
				}
			}
		}
	}
}

// Stop stops collecting metrics and returns the result
func (mc *MetricsCollector) Stop(name, operation string, dataSize int64) *BenchmarkResult {
	close(mc.stopCh)
	<-mc.doneCh

	duration := time.Since(mc.startTime)
	endMem := CaptureMemStats()

	// Calculate throughput in MB/s
	durationSecs := duration.Seconds()
	throughput := 0.0
	if durationSecs > 0 {
		throughput = float64(dataSize) / (1024 * 1024) / durationSecs
	}

	result := &BenchmarkResult{
		Name:          name,
		Operation:     operation,
		DataSizeBytes: dataSize,
		Duration:      duration,
		Throughput:    throughput,

		AllocBytes:      endMem.HeapAlloc,
		TotalAllocBytes: endMem.TotalAlloc - mc.startMem.TotalAlloc,
		HeapObjects:     endMem.HeapObjects,
		NumGC:           endMem.NumGC - mc.startMem.NumGC,
		GCPauseTotal:    endMem.PauseTotalNs - mc.startMem.PauseTotalNs,

		GoroutineCount: runtime.NumGoroutine(),
		MaxGoroutines:  int(mc.maxGoroutines.Load()),
		WorkerCount:    runtime.NumCPU(),

		CPUCores: runtime.NumCPU(),

		ReadBytes:  mc.bytesRead.Load(),
		WriteBytes: mc.bytesWritten.Load(),

		CompressionTime: time.Duration(mc.compressionNs.Load()),
		IOTime:          time.Duration(mc.ioNs.Load()),
		DBOperationTime: time.Duration(mc.dbOperationNs.Load()),
	}

	// Check against targets
	result.checkTargets(operation)

	return result
}

// checkTargets evaluates whether the result meets performance targets
func (r *BenchmarkResult) checkTargets(operation string) {
	var notes []string
	meetsAll := true

	// Throughput target
	var targetThroughput float64
	if operation == "dump" {
		targetThroughput = PerformanceTargets.DumpThroughputMBs
	} else {
		targetThroughput = PerformanceTargets.RestoreThroughputMBs
	}

	if r.Throughput < targetThroughput {
		meetsAll = false
		notes = append(notes, fmt.Sprintf("throughput %.1f MB/s < target %.1f MB/s",
			r.Throughput, targetThroughput))
	}

	// Memory target
	if int64(r.AllocBytes) > PerformanceTargets.MaxMemoryBytes {
		meetsAll = false
		notes = append(notes, fmt.Sprintf("memory %d MB > target %d MB",
			r.AllocBytes/(1<<20), PerformanceTargets.MaxMemoryBytes/(1<<20)))
	}

	// Goroutine target
	if r.MaxGoroutines > PerformanceTargets.MaxGoroutines {
		meetsAll = false
		notes = append(notes, fmt.Sprintf("goroutines %d > target %d",
			r.MaxGoroutines, PerformanceTargets.MaxGoroutines))
	}

	r.MeetsTarget = meetsAll
	if len(notes) > 0 {
		r.TargetNotes = fmt.Sprintf("%v", notes)
	}
}

// RecordRead records bytes read
func (mc *MetricsCollector) RecordRead(bytes int64) {
	mc.bytesRead.Add(bytes)
}

// RecordWrite records bytes written
func (mc *MetricsCollector) RecordWrite(bytes int64) {
	mc.bytesWritten.Add(bytes)
}

// RecordCompression records time spent on compression
func (mc *MetricsCollector) RecordCompression(d time.Duration) {
	mc.compressionNs.Add(int64(d))
}

// RecordIO records time spent on I/O
func (mc *MetricsCollector) RecordIO(d time.Duration) {
	mc.ioNs.Add(int64(d))
}

// RecordDBOperation records time spent on database operations
func (mc *MetricsCollector) RecordDBOperation(d time.Duration) {
	mc.dbOperationNs.Add(int64(d))
}

// CountingReader wraps a reader to count bytes read
type CountingReader struct {
	reader    io.Reader
	collector *MetricsCollector
}

// NewCountingReader creates a reader that counts bytes
func NewCountingReader(r io.Reader, mc *MetricsCollector) *CountingReader {
	return &CountingReader{reader: r, collector: mc}
}

func (cr *CountingReader) Read(p []byte) (int, error) {
	n, err := cr.reader.Read(p)
	if n > 0 && cr.collector != nil {
		cr.collector.RecordRead(int64(n))
	}
	return n, err
}

// CountingWriter wraps a writer to count bytes written
type CountingWriter struct {
	writer    io.Writer
	collector *MetricsCollector
}

// NewCountingWriter creates a writer that counts bytes
func NewCountingWriter(w io.Writer, mc *MetricsCollector) *CountingWriter {
	return &CountingWriter{writer: w, collector: mc}
}

func (cw *CountingWriter) Write(p []byte) (int, error) {
	n, err := cw.writer.Write(p)
	if n > 0 && cw.collector != nil {
		cw.collector.RecordWrite(int64(n))
	}
	return n, err
}

// BenchmarkSuite runs a series of benchmarks
type BenchmarkSuite struct {
	name     string
	results  []*BenchmarkResult
	profiler *Profiler
	mu       sync.Mutex
}

// NewBenchmarkSuite creates a new benchmark suite
func NewBenchmarkSuite(name string, profiler *Profiler) *BenchmarkSuite {
	return &BenchmarkSuite{
		name:     name,
		profiler: profiler,
	}
}

// Run executes a benchmark function and records results
func (bs *BenchmarkSuite) Run(ctx context.Context, name string, fn func(ctx context.Context, mc *MetricsCollector) (int64, error)) (*BenchmarkResult, error) {
	mc := NewMetricsCollector()

	// Start profiling if enabled
	if bs.profiler != nil {
		if err := bs.profiler.Start(); err != nil {
			return nil, fmt.Errorf("failed to start profiler: %w", err)
		}
		defer bs.profiler.Stop()
	}

	mc.Start()

	dataSize, err := fn(ctx, mc)

	result := mc.Stop(name, "benchmark", dataSize)

	bs.mu.Lock()
	bs.results = append(bs.results, result)
	bs.mu.Unlock()

	return result, err
}

// Results returns all benchmark results
func (bs *BenchmarkSuite) Results() []*BenchmarkResult {
	bs.mu.Lock()
	defer bs.mu.Unlock()
	return append([]*BenchmarkResult(nil), bs.results...)
}

// Summary returns a summary of all benchmark results
func (bs *BenchmarkSuite) Summary() string {
	bs.mu.Lock()
	defer bs.mu.Unlock()

	var passed, failed int
	for _, r := range bs.results {
		if r.MeetsTarget {
			passed++
		} else {
			failed++
		}
	}

	return fmt.Sprintf("Benchmark Suite: %s\n"+
		"Total: %d benchmarks\n"+
		"Passed: %d\n"+
		"Failed: %d\n",
		bs.name, len(bs.results), passed, failed)
}
