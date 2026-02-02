// Package performance provides pipeline stage optimization utilities
package performance

import (
	"context"
	"io"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

// PipelineStage represents a processing stage in a data pipeline
type PipelineStage struct {
	name     string
	workers  int
	inputCh  chan *ChunkData
	outputCh chan *ChunkData
	process  ProcessFunc
	errorCh  chan error
	metrics  *StageMetrics
	ctx      context.Context
	cancel   context.CancelFunc
	wg       sync.WaitGroup
}

// ChunkData represents a chunk of data flowing through the pipeline
type ChunkData struct {
	Data     []byte
	Sequence int64
	Size     int
	Metadata map[string]interface{}
}

// ProcessFunc is the function type for processing a chunk
type ProcessFunc func(ctx context.Context, chunk *ChunkData) (*ChunkData, error)

// StageMetrics tracks performance metrics for a pipeline stage
type StageMetrics struct {
	ChunksProcessed atomic.Int64
	BytesProcessed  atomic.Int64
	ProcessingTime  atomic.Int64 // nanoseconds
	WaitTime        atomic.Int64 // nanoseconds waiting for input
	Errors          atomic.Int64
}

// NewPipelineStage creates a new pipeline stage
func NewPipelineStage(name string, workers int, bufferSize int, process ProcessFunc) *PipelineStage {
	if workers <= 0 {
		workers = runtime.NumCPU()
	}

	ctx, cancel := context.WithCancel(context.Background())

	return &PipelineStage{
		name:     name,
		workers:  workers,
		inputCh:  make(chan *ChunkData, bufferSize),
		outputCh: make(chan *ChunkData, bufferSize),
		process:  process,
		errorCh:  make(chan error, workers),
		metrics:  &StageMetrics{},
		ctx:      ctx,
		cancel:   cancel,
	}
}

// Start starts the pipeline stage workers
func (ps *PipelineStage) Start() {
	for i := 0; i < ps.workers; i++ {
		ps.wg.Add(1)
		go ps.worker(i)
	}
}

func (ps *PipelineStage) worker(id int) {
	defer ps.wg.Done()

	for {
		select {
		case <-ps.ctx.Done():
			return
		case chunk, ok := <-ps.inputCh:
			if !ok {
				return
			}

			waitStart := time.Now()

			// Process the chunk
			start := time.Now()
			result, err := ps.process(ps.ctx, chunk)
			processingTime := time.Since(start)

			// Update metrics
			ps.metrics.ProcessingTime.Add(int64(processingTime))
			ps.metrics.WaitTime.Add(int64(time.Since(waitStart) - processingTime))

			if err != nil {
				ps.metrics.Errors.Add(1)
				select {
				case ps.errorCh <- err:
				default:
				}
				continue
			}

			ps.metrics.ChunksProcessed.Add(1)
			if result != nil {
				ps.metrics.BytesProcessed.Add(int64(result.Size))

				select {
				case ps.outputCh <- result:
				case <-ps.ctx.Done():
					return
				}
			}
		}
	}
}

// Input returns the input channel for sending data to the stage
func (ps *PipelineStage) Input() chan<- *ChunkData {
	return ps.inputCh
}

// Output returns the output channel for receiving processed data
func (ps *PipelineStage) Output() <-chan *ChunkData {
	return ps.outputCh
}

// Errors returns the error channel
func (ps *PipelineStage) Errors() <-chan error {
	return ps.errorCh
}

// Stop gracefully stops the pipeline stage
func (ps *PipelineStage) Stop() {
	close(ps.inputCh)
	ps.wg.Wait()
	close(ps.outputCh)
	ps.cancel()
}

// Metrics returns the stage metrics
func (ps *PipelineStage) Metrics() *StageMetrics {
	return ps.metrics
}

// Pipeline chains multiple stages together
type Pipeline struct {
	stages    []*PipelineStage
	chunkPool *sync.Pool
	sequence  atomic.Int64
	ctx       context.Context
	cancel    context.CancelFunc
}

// NewPipeline creates a new pipeline
func NewPipeline() *Pipeline {
	ctx, cancel := context.WithCancel(context.Background())
	return &Pipeline{
		chunkPool: &sync.Pool{
			New: func() interface{} {
				return &ChunkData{
					Data:     make([]byte, LargeBufferSize),
					Metadata: make(map[string]interface{}),
				}
			},
		},
		ctx:    ctx,
		cancel: cancel,
	}
}

// AddStage adds a stage to the pipeline
func (p *Pipeline) AddStage(name string, workers int, process ProcessFunc) *Pipeline {
	stage := NewPipelineStage(name, workers, 4, process)

	// Connect to previous stage if exists
	if len(p.stages) > 0 {
		prevStage := p.stages[len(p.stages)-1]
		// Replace the input channel with previous stage's output
		stage.inputCh = make(chan *ChunkData, 4)
		go func() {
			for chunk := range prevStage.outputCh {
				select {
				case stage.inputCh <- chunk:
				case <-p.ctx.Done():
					return
				}
			}
			close(stage.inputCh)
		}()
	}

	p.stages = append(p.stages, stage)
	return p
}

// Start starts all pipeline stages
func (p *Pipeline) Start() {
	for _, stage := range p.stages {
		stage.Start()
	}
}

// Input returns the input to the first stage
func (p *Pipeline) Input() chan<- *ChunkData {
	if len(p.stages) == 0 {
		return nil
	}
	return p.stages[0].inputCh
}

// Output returns the output of the last stage
func (p *Pipeline) Output() <-chan *ChunkData {
	if len(p.stages) == 0 {
		return nil
	}
	return p.stages[len(p.stages)-1].outputCh
}

// Stop stops all pipeline stages
func (p *Pipeline) Stop() {
	// Close input to first stage
	if len(p.stages) > 0 {
		close(p.stages[0].inputCh)
	}

	// Wait for all stages to complete
	for _, stage := range p.stages {
		stage.wg.Wait()
		stage.cancel()
	}

	p.cancel()
}

// GetChunk gets a chunk from the pool
func (p *Pipeline) GetChunk() *ChunkData {
	chunk := p.chunkPool.Get().(*ChunkData)
	chunk.Sequence = p.sequence.Add(1)
	chunk.Size = 0
	return chunk
}

// PutChunk returns a chunk to the pool
func (p *Pipeline) PutChunk(chunk *ChunkData) {
	if chunk != nil {
		chunk.Size = 0
		chunk.Sequence = 0
		p.chunkPool.Put(chunk)
	}
}

// StreamReader wraps an io.Reader to produce chunks for a pipeline
type StreamReader struct {
	reader    io.Reader
	pipeline  *Pipeline
	chunkSize int
}

// NewStreamReader creates a new stream reader
func NewStreamReader(r io.Reader, p *Pipeline, chunkSize int) *StreamReader {
	if chunkSize <= 0 {
		chunkSize = LargeBufferSize
	}
	return &StreamReader{
		reader:    r,
		pipeline:  p,
		chunkSize: chunkSize,
	}
}

// Feed reads from the reader and feeds chunks to the pipeline
func (sr *StreamReader) Feed(ctx context.Context) error {
	input := sr.pipeline.Input()
	if input == nil {
		return nil
	}

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		chunk := sr.pipeline.GetChunk()

		// Resize if needed
		if len(chunk.Data) < sr.chunkSize {
			chunk.Data = make([]byte, sr.chunkSize)
		}

		n, err := sr.reader.Read(chunk.Data[:sr.chunkSize])
		if n > 0 {
			chunk.Size = n
			select {
			case input <- chunk:
			case <-ctx.Done():
				sr.pipeline.PutChunk(chunk)
				return ctx.Err()
			}
		} else {
			sr.pipeline.PutChunk(chunk)
		}

		if err != nil {
			if err == io.EOF {
				return nil
			}
			return err
		}
	}
}

// StreamWriter wraps an io.Writer to consume chunks from a pipeline
type StreamWriter struct {
	writer   io.Writer
	pipeline *Pipeline
}

// NewStreamWriter creates a new stream writer
func NewStreamWriter(w io.Writer, p *Pipeline) *StreamWriter {
	return &StreamWriter{
		writer:   w,
		pipeline: p,
	}
}

// Drain reads from the pipeline and writes to the writer
func (sw *StreamWriter) Drain(ctx context.Context) error {
	output := sw.pipeline.Output()
	if output == nil {
		return nil
	}

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case chunk, ok := <-output:
			if !ok {
				return nil
			}

			if chunk.Size > 0 {
				_, err := sw.writer.Write(chunk.Data[:chunk.Size])
				if err != nil {
					sw.pipeline.PutChunk(chunk)
					return err
				}
			}

			sw.pipeline.PutChunk(chunk)
		}
	}
}

// CompressionStage creates a pipeline stage for compression
// This is a placeholder - actual implementation would use pgzip
func CompressionStage(level int) ProcessFunc {
	return func(ctx context.Context, chunk *ChunkData) (*ChunkData, error) {
		// In a real implementation, this would compress the chunk
		// For now, just pass through
		return chunk, nil
	}
}

// DecompressionStage creates a pipeline stage for decompression
func DecompressionStage() ProcessFunc {
	return func(ctx context.Context, chunk *ChunkData) (*ChunkData, error) {
		// In a real implementation, this would decompress the chunk
		// For now, just pass through
		return chunk, nil
	}
}
