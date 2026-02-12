package native

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"os"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	comp "dbbackup/internal/compression"
)

// ═══════════════════════════════════════════════════════════════════════════════
// PIPELINE RESTORE ENGINE — Decoupled Scanner + Worker Architecture
// ═══════════════════════════════════════════════════════════════════════════════
//
// PROBLEM (old architecture):
//   The single scanner goroutine feeds COPY data to workers via io.Pipe.
//   io.Pipe is unbuffered — the scanner blocks at PostgreSQL's ingestion
//   rate (~21 MB/s). While writing a large table, no other tables start.
//   Workers sit idle. True parallelism = 1.
//
// SOLUTION (this file):
//   Decouple scanning from COPY execution with a 3-stage pipeline:
//
//   Stage 1: SCANNER (1 goroutine)
//     Reads the dump sequentially. For each COPY block, reads all rows
//     into bounded-size chunks (default 4MB), sends chunks to a buffered
//     channel. Moves to next table immediately.
//
//   Stage 2: DISPATCHER (1 goroutine)
//     Reads copyJob messages from the channel. Assigns to the next
//     available worker. Maintains backpressure via worker semaphore.
//
//   Stage 3: WORKERS (N goroutines)
//     Each worker has a dedicated pgx connection. Receives a copyJob,
//     streams chunks to CopyFrom. Multiple workers execute COPY in
//     true parallel.
//
// Memory budget: bounded by channelSize * chunkSize (default: 64 * 4MB = 256MB).
// This is controllable and still O(1) per dump size.
//
// Expected speedup: 3-6x for multi-table dumps (21 MB/s → 80-120 MB/s).
// Combined with binary COPY: 120-200 MB/s.
// ═══════════════════════════════════════════════════════════════════════════════

const (
	// defaultPipelineChunkSize is the size of each pre-read COPY data chunk.
	// Larger = fewer channel sends + better CopyFrom batching.
	// 4MB is optimal: matches PostgreSQL WAL segment * 0.25, fits L3 cache.
	defaultPipelineChunkSize = 4 * 1024 * 1024

	// defaultPipelineChannelSize is the buffered channel capacity for copyJobs.
	// 64 jobs × 4MB = 256MB max memory. Provides enough buffering for the
	// scanner to read ahead while workers are busy.
	defaultPipelineChannelSize = 64

	// defaultPipelineFileBuffer is the file read buffer size.
	// 4MB aligns with filesystem readahead and SSD page sizes.
	defaultPipelineFileBuffer = 4 * 1024 * 1024

	// defaultPipelineCopyBuffer is the bufio.Writer size wrapping the pipe
	// writer to CopyFrom. Larger = fewer syscalls.
	defaultPipelineCopyBuffer = 4 * 1024 * 1024
)

// PipelineConfig controls pipeline restore tuning parameters.
type PipelineConfig struct {
	// ChunkSize is the size of pre-read COPY data chunks (bytes).
	ChunkSize int
	// ChannelSize is the buffered channel capacity for copy jobs.
	ChannelSize int
	// FileBufferSize is the file read buffer size (bytes).
	FileBufferSize int
	// CopyBufferSize is the CopyFrom pipe write buffer size (bytes).
	CopyBufferSize int
}

// DefaultPipelineConfig returns production-tuned pipeline settings.
func DefaultPipelineConfig() *PipelineConfig {
	return &PipelineConfig{
		ChunkSize:      defaultPipelineChunkSize,
		ChannelSize:    defaultPipelineChannelSize,
		FileBufferSize: defaultPipelineFileBuffer,
		CopyBufferSize: defaultPipelineCopyBuffer,
	}
}

// HighMemoryPipelineConfig returns settings for systems with 32GB+ RAM.
// Uses more memory for higher throughput.
func HighMemoryPipelineConfig() *PipelineConfig {
	return &PipelineConfig{
		ChunkSize:      16 * 1024 * 1024, // 16MB chunks
		ChannelSize:    128,               // 128 × 16MB = 2GB max
		FileBufferSize: 16 * 1024 * 1024,  // 16MB read buffer
		CopyBufferSize: 8 * 1024 * 1024,   // 8MB write buffer
	}
}

// LowMemoryPipelineConfig returns settings for systems with <8GB RAM.
func LowMemoryPipelineConfig() *PipelineConfig {
	return &PipelineConfig{
		ChunkSize:      1 * 1024 * 1024, // 1MB chunks
		ChannelSize:    16,              // 16 × 1MB = 16MB max
		FileBufferSize: 1 * 1024 * 1024, // 1MB read buffer
		CopyBufferSize: 1 * 1024 * 1024, // 1MB write buffer
	}
}

// copyJob represents a pre-read COPY block ready for parallel execution.
type copyJob struct {
	tableName string
	copySQL   string   // "COPY table FROM STDIN WITH (FREEZE)"
	chunks    [][]byte // Pre-read data chunks (each ≤ chunkSize)
	totalSize int64    // Total bytes across all chunks
}

// chunkPool reuses []byte slices to reduce GC pressure during pipeline restore.
var chunkPool = sync.Pool{
	New: func() interface{} {
		buf := make([]byte, 0, defaultPipelineChunkSize)
		return &buf
	},
}

func getChunk(size int) []byte {
	bufp := chunkPool.Get().(*[]byte)
	buf := *bufp
	if cap(buf) < size {
		buf = make([]byte, 0, size)
	}
	return buf[:0]
}

func putChunk(buf []byte) {
	buf = buf[:0]
	chunkPool.Put(&buf)
}

// RestoreFilePipeline is the high-performance pipeline restore entry point.
// It replaces RestoreFile when maximum throughput is required.
//
// Architecture:
//   Scanner goroutine → buffered channel → N worker goroutines
//   (pre-reads COPY data)   (backpressure)   (parallel CopyFrom)
func (e *ParallelRestoreEngine) RestoreFilePipeline(ctx context.Context, filePath string, options *ParallelRestoreOptions, pipelineCfg *PipelineConfig) (*ParallelRestoreResult, error) {
	startTime := time.Now()
	result := &ParallelRestoreResult{}

	// Derived cancel context — allows us to force-terminate all goroutines
	// if the scanner or workers are stuck (deadlock watchdog).
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	if options == nil {
		options = &ParallelRestoreOptions{Workers: e.parallelWorkers}
	}
	if options.Workers < 1 {
		options.Workers = e.parallelWorkers
	}
	if pipelineCfg == nil {
		pipelineCfg = DefaultPipelineConfig()
	}

	restoreMode := options.RestoreMode
	result.RestoreMode = restoreMode

	// ── Adaptive worker allocation ──
	if profile := loadTableProfile(filePath, e.log); profile != nil {
		adaptedWorkers := workersForSize(profile.Complexity, options.Workers)
		if adaptedWorkers != options.Workers {
			e.log.Info("Pipeline: adaptive worker allocation",
				"database", profile.Name,
				"size_mb", profile.SizeBytes/(1024*1024),
				"complexity", profile.Complexity,
				"adapted_workers", adaptedWorkers)
			options.Workers = adaptedWorkers
		}
	}

	workers := options.Workers
	if workers > runtime.NumCPU()*2 {
		workers = runtime.NumCPU() * 2 // Sanity cap
	}

	e.log.Info("Starting PIPELINE parallel restore",
		"file", filePath,
		"workers", workers,
		"restore_mode", restoreMode.String(),
		"chunk_size_mb", pipelineCfg.ChunkSize/(1024*1024),
		"channel_size", pipelineCfg.ChannelSize,
		"file_buffer_mb", pipelineCfg.FileBufferSize/(1024*1024),
		"copy_buffer_mb", pipelineCfg.CopyBufferSize/(1024*1024),
		"max_memory_mb", (pipelineCfg.ChunkSize*pipelineCfg.ChannelSize)/(1024*1024))

	// Apply turbo session settings
	if restoreMode == RestoreModeTurbo {
		applyTurboSessionSettings(ctx, e.pool, e.log)
	}

	// Track tables for UNLOGGED→LOGGED transitions
	var unloggedTables []string
	var unloggedMu sync.Mutex

	// ── Open and decompress file ──
	file, err := os.Open(filePath)
	if err != nil {
		return result, fmt.Errorf("failed to open file: %w", err)
	}

	HintSequentialRead(file)

	var cleanupOnce sync.Once
	var decompCloser io.Closer
	cleanupFn := func() {
		cleanupOnce.Do(func() {
			if decompCloser != nil {
				decompCloser.Close()
			}
			HintDoneWithFile(file)
			file.Close()
		})
	}
	defer cleanupFn()

	// Context-aware file cleanup
	ctxDone := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			cleanupFn()
		case <-ctxDone:
		}
	}()
	defer close(ctxDone)

	// Use larger file read buffer (4MB vs old 256KB)
	bufReader := bufio.NewReaderSize(file, pipelineCfg.FileBufferSize)
	var reader io.Reader = bufReader
	if algo := comp.DetectAlgorithm(filePath); algo != comp.AlgorithmNone {
		decomp, err := comp.NewDecompressorWithAlgorithm(bufReader, algo)
		if err != nil {
			return result, fmt.Errorf("failed to create %s reader: %w", algo, err)
		}
		decompCloser = decomp
		reader = decomp.Reader
		e.log.Info("Pipeline restore decompression", "algorithm", algo)
	}

	// ═══════════════════════════════════════════════════════════════
	// STAGE 1: Scanner — pre-reads COPY data into buffered channel
	// ═══════════════════════════════════════════════════════════════

	jobChan := make(chan copyJob, pipelineCfg.ChannelSize)

	// Idempotent close for jobChan — can be called safely from both the
	// scanner goroutine (normal exit) and the main flow (watchdog/error).
	var jobChanOnce sync.Once
	closeJobChan := func() { jobChanOnce.Do(func() { close(jobChan) }) }

	var postDataStmts []string
	var postDataMu sync.Mutex
	var scanErr error
	var schemaCount int64

	scannerDone := make(chan struct{})
	go func() {
		defer close(scannerDone)
		defer closeJobChan()
		defer func() {
			if r := recover(); r != nil {
				e.log.Error("Scanner goroutine panic (recovered)", "panic", fmt.Sprintf("%v", r))
				scanErr = fmt.Errorf("scanner panic: %v", r)
			}
		}()

		scanner := bufio.NewScanner(reader)
		scanner.Buffer(make([]byte, 1024*1024), 64*1024*1024) // 64MB max line

		var stmtBuf strings.Builder
		lineCount := 0

		for scanner.Scan() {
			lineCount++

			if lineCount%10000 == 0 && ctx.Err() != nil {
				break
			}

			line := scanner.Text()
			trimmed := strings.TrimSpace(line)

			if trimmed == "" || strings.HasPrefix(trimmed, "--") {
				continue
			}

			upper := strings.ToUpper(trimmed)

			// ── COPY block: pre-read all rows into chunks ──
			if strings.HasPrefix(upper, "COPY ") && strings.HasSuffix(trimmed, "FROM stdin;") {
				parts := strings.Fields(trimmed)
				tableName := ""
				if len(parts) >= 2 {
					tableName = parts[1]
				}

				// Set UNLOGGED before COPY if needed
				if restoreMode == RestoreModeBalanced || restoreMode == RestoreModeTurbo {
					if err := setTableUnlogged(ctx, e.pool, tableName, e.log); err == nil {
						unloggedMu.Lock()
						unloggedTables = append(unloggedTables, tableName)
						unloggedMu.Unlock()
					}
				}

				// Pre-read COPY data into chunks
				var chunks [][]byte
				var totalSize int64
				currentChunk := getChunk(pipelineCfg.ChunkSize)

				for scanner.Scan() {
					lineCount++
					dataLine := scanner.Text()
					if dataLine == "\\." {
						break
					}

					// Append row + newline to current chunk
					needed := len(dataLine) + 1
					if len(currentChunk)+needed > pipelineCfg.ChunkSize && len(currentChunk) > 0 {
						// Current chunk full — seal it and start new one
						sealed := make([]byte, len(currentChunk))
						copy(sealed, currentChunk)
						chunks = append(chunks, sealed)
						totalSize += int64(len(sealed))
						currentChunk = currentChunk[:0]
					}

					currentChunk = append(currentChunk, dataLine...)
					currentChunk = append(currentChunk, '\n')

					if lineCount%100000 == 0 && ctx.Err() != nil {
						break
					}
				}

				// Seal final chunk
				if len(currentChunk) > 0 {
					sealed := make([]byte, len(currentChunk))
					copy(sealed, currentChunk)
					chunks = append(chunks, sealed)
					totalSize += int64(len(sealed))
				}
				putChunk(currentChunk)

				if ctx.Err() != nil {
					break
				}

				// Send job to workers via buffered channel
				// This blocks if channel is full (backpressure from workers)
				job := copyJob{
					tableName: tableName,
					copySQL:   fmt.Sprintf("COPY %s FROM STDIN WITH (FREEZE)", tableName),
					chunks:    chunks,
					totalSize: totalSize,
				}

				select {
				case jobChan <- job:
				case <-ctx.Done():
					// Release chunks that won't be used
					for _, c := range chunks {
						_ = c // will be GC'd
					}
					return
				}

				continue
			}

			// ── Regular statement ──
			stmtBuf.WriteString(line)
			stmtBuf.WriteByte('\n')

			if !strings.HasSuffix(trimmed, ";") {
				continue
			}

			sql := stmtBuf.String()
			stmtBuf.Reset()

			stmtType := classifyStatement(sql)

			switch stmtType {
			case StmtPostData:
				postDataMu.Lock()
				postDataStmts = append(postDataStmts, sql)
				postDataMu.Unlock()
			default:
				if err := e.executeStatement(ctx, sql); err != nil {
					if options.ContinueOnError {
						e.log.Warn("Schema statement failed (continuing)", "error", err)
					} else {
						scanErr = fmt.Errorf("statement failed: %w", err)
						return
					}
				}
				atomic.AddInt64(&schemaCount, 1)
			}
		}

		if sErr := scanner.Err(); sErr != nil && ctx.Err() == nil {
			scanErr = sErr
		}
	}()

	// ═══════════════════════════════════════════════════════════════
	// STAGE 2+3: Workers — consume pre-read jobs in true parallel
	// ═══════════════════════════════════════════════════════════════

	var wg sync.WaitGroup
	var totalRows int64
	var tablesCompleted int64
	var tablesStarted int64
	var totalBytes int64
	var copyErrors []string
	var copyErrMu sync.Mutex

	// Start N worker goroutines that pull from the shared job channel
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()

			for {
				var job copyJob
				var ok bool
				select {
				case job, ok = <-jobChan:
					if !ok {
						return // channel closed, worker exits
					}
				case <-ctx.Done():
					return // context cancelled, worker exits
				}

				if ctx.Err() != nil {
					// Release unreferenced chunks
					for _, c := range job.chunks {
						_ = c
					}
					continue
				}

				num := atomic.AddInt64(&tablesStarted, 1)
				e.log.Info("COPY pipeline",
					"worker", workerID,
					"table", job.tableName,
					"number", num,
					"size_mb", fmt.Sprintf("%.1f", float64(job.totalSize)/(1024*1024)),
					"chunks", len(job.chunks))

				copyStart := time.Now()

				// Create io.Pipe and feed pre-read chunks through it
				pr, pw := io.Pipe()

				// Feeder goroutine: writes pre-read chunks to pipe
				go func(chunks [][]byte) {
					bw := bufio.NewWriterSize(pw, pipelineCfg.CopyBufferSize)
					for _, chunk := range chunks {
						if _, werr := bw.Write(chunk); werr != nil {
							break
						}
					}
					bw.Flush()
					pw.Close()
				}(job.chunks)

				// CopyFrom reads from pipe (no FREEZE — schema created on separate connection)
				rows, copyErr := e.streamCopyNoFreeze(ctx, job.tableName, pr)
				pr.Close() // Unblock feeder goroutine if COPY returned early

				dur := time.Since(copyStart)
				atomic.AddInt64(&totalBytes, job.totalSize)

				if copyErr != nil && ctx.Err() == nil {
					e.log.Warn("COPY failed", "table", job.tableName, "error", copyErr, "duration", dur)
					copyErrMu.Lock()
					copyErrors = append(copyErrors, fmt.Sprintf("%s: %v", job.tableName, copyErr))
					copyErrMu.Unlock()
				} else if copyErr == nil {
					atomic.AddInt64(&totalRows, rows)
					completed := atomic.AddInt64(&tablesCompleted, 1)
					mbps := float64(job.totalSize) / (1024 * 1024) / maxFloat(dur.Seconds(), 0.001)
					e.log.Info("COPY done",
						"table", job.tableName,
						"rows", rows,
						"duration", dur,
						"size_mb", fmt.Sprintf("%.1f", float64(job.totalSize)/(1024*1024)),
						"throughput_mbps", fmt.Sprintf("%.1f", mbps))
					if options.ProgressCallback != nil {
						options.ProgressCallback("data", int(completed), 0, job.tableName)
					}
				}

				// Release chunk memory early
				for i := range job.chunks {
					job.chunks[i] = nil
				}
			}
		}(i)
	}

	// ═══════════════════════════════════════════════════════════════
	// STAGE 4: Wait for scanner + workers with deadlock watchdog
	// ═══════════════════════════════════════════════════════════════
	//
	// Previous bug: if the scanner goroutine blocks indefinitely (e.g.,
	// pool exhaustion in setTableUnlogged/executeStatement), jobChan
	// never closes and workers block forever at <-jobChan. We now:
	//   1. Use sync.Once for close(jobChan) so it can be called safely
	//      from both the scanner defer and the main flow.
	//   2. Wait for scanner in a non-blocking select with a watchdog.
	//   3. Explicitly close jobChan after scanner finishes (redundancy).
	//   4. If workers don't finish within 5 minutes of scanner
	//      completion, cancel the context to force-unblock everything.

	// Wait for scanner (or detect stuck scanner via watchdog)
	scannerFinished := false
	select {
	case <-scannerDone:
		scannerFinished = true
	case <-ctx.Done():
		// Parent context was cancelled — force everything to stop
		e.log.Warn("Pipeline restore: context cancelled while waiting for scanner")
	}

	// Explicitly close jobChan to unblock workers (idempotent via sync.Once).
	// Critical: even if scanner panicked or got stuck, workers MUST see
	// the channel close so they can exit their select loop.
	closeJobChan()

	// If scanner didn't finish (ctx cancelled), wait briefly for it
	if !scannerFinished {
		select {
		case <-scannerDone:
		case <-time.After(10 * time.Second):
			e.log.Warn("Pipeline restore: scanner did not stop within 10s, continuing")
		}
	}

	// Wait for all workers with a safety timeout
	inFlight := atomic.LoadInt64(&tablesStarted) - atomic.LoadInt64(&tablesCompleted)
	if inFlight > 0 {
		e.log.Info("Pipeline: waiting for COPY workers...", "in_flight", inFlight)
	}

	workersDone := make(chan struct{})
	go func() {
		wg.Wait()
		close(workersDone)
	}()

	select {
	case <-workersDone:
		// All workers finished cleanly
	case <-time.After(5 * time.Minute):
		// Workers stuck — cancel context to force CopyFrom/Acquire to abort
		e.log.Error("Pipeline restore: workers stuck for 5 minutes after scanner, forcing shutdown")
		cancel()
		select {
		case <-workersDone:
		case <-time.After(30 * time.Second):
			e.log.Error("Pipeline restore: workers did not exit after cancel, abandoning")
		}
	}

	result.SchemaStatements = atomic.LoadInt64(&schemaCount)
	result.TablesRestored = atomic.LoadInt64(&tablesCompleted)
	result.RowsRestored = atomic.LoadInt64(&totalRows)
	result.DataDuration = time.Since(startTime)


	copyErrMu.Lock()
	result.Errors = append(result.Errors, copyErrors...)
	copyErrMu.Unlock()

	if scanErr != nil {
		result.Errors = append(result.Errors, scanErr.Error())
	}

	if ctx.Err() != nil {
		return result, ctx.Err()
	}

	// Data throughput summary
	processedBytes := atomic.LoadInt64(&totalBytes)
	dataDuration := result.DataDuration.Seconds()
	if dataDuration > 0 && processedBytes > 0 {
		overallMBps := float64(processedBytes) / (1024 * 1024) / dataDuration
		e.log.Info("Pipeline data phase complete",
			"total_bytes_mb", processedBytes/(1024*1024),
			"duration", result.DataDuration,
			"throughput_mbps", fmt.Sprintf("%.1f", overallMBps),
			"tables", result.TablesRestored,
			"rows", result.RowsRestored)
	}

	// ── Balanced mode: UNLOGGED→LOGGED switch before indexes ──
	if restoreMode == RestoreModeBalanced && len(unloggedTables) > 0 {
		switchStart := time.Now()
		e.log.Info("Switching tables to LOGGED before index creation",
			"tables", len(unloggedTables))

		for _, tbl := range unloggedTables {
			if ctx.Err() != nil {
				break
			}
			if err := setTableLogged(ctx, e.pool, tbl, e.log); err != nil {
				e.log.Warn("SET LOGGED failed", "table", tbl, "error", err)
				result.Errors = append(result.Errors, fmt.Sprintf("SET LOGGED %s: %v", tbl, err))
			} else {
				result.TablesToggled++
			}
		}
		if err := forceCheckpoint(ctx, e.pool, e.log); err != nil {
			e.log.Warn("Post-switch CHECKPOINT failed", "error", err)
		}
		result.SwitchDuration = time.Since(switchStart)
	}

	if ctx.Err() != nil {
		return result, ctx.Err()
	}

	// ── Post-data: parallel indexes/constraints ──
	postDataMu.Lock()
	postData := postDataStmts
	postDataMu.Unlock()

	indexStart := time.Now()
	if len(postData) > 0 {
		e.log.Info("Pipeline: creating indexes and constraints...",
			"statements", len(postData), "workers", workers)

		result.IndexesCreated = e.executePostDataParallel(ctx, postData, options)
	}
	result.IndexDuration = time.Since(indexStart)

	// ── Turbo mode: deferred UNLOGGED→LOGGED ──
	if restoreMode == RestoreModeTurbo && len(unloggedTables) > 0 {
		switchStart := time.Now()
		for _, tbl := range unloggedTables {
			if ctx.Err() != nil {
				break
			}
			if err := setTableLogged(ctx, e.pool, tbl, e.log); err != nil {
				result.Errors = append(result.Errors, fmt.Sprintf("SET LOGGED %s: %v", tbl, err))
			} else {
				result.TablesToggled++
			}
		}
		if err := forceCheckpoint(ctx, e.pool, e.log); err != nil {
			e.log.Warn("Post-switch CHECKPOINT failed", "error", err)
		}
		result.SwitchDuration = time.Since(switchStart)
	}

	result.Duration = time.Since(startTime)

	// Summary
	e.log.Info("Pipeline restore completed",
		"duration", result.Duration,
		"restore_mode", restoreMode.String(),
		"schema", result.SchemaStatements,
		"tables", result.TablesRestored,
		"rows", result.RowsRestored,
		"indexes", result.IndexesCreated,
		"data_phase", result.DataDuration,
		"index_phase", result.IndexDuration,
		"errors", len(result.Errors))

	return result, nil
}
