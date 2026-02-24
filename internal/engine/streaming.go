package engine

import (
	"context"
	"fmt"
	"io"
	"sync"
	"time"

	"dbbackup/internal/engine/parallel"
	"dbbackup/internal/logger"
)

// StreamingBackupEngine wraps a backup engine with streaming capability
type StreamingBackupEngine struct {
	engine   BackupEngine
	cloudCfg parallel.Config
	log      logger.Logger

	mu        sync.Mutex
	streamer  *parallel.CloudStreamer
	started   bool
	completed bool
}

// StreamingConfig holds streaming configuration
type StreamingConfig struct {
	// Cloud configuration
	Bucket   string
	Key      string
	Region   string
	Endpoint string

	// Performance
	PartSize    int64
	WorkerCount int

	// Security
	Encryption string
	KMSKeyID   string

	// Progress callback
	OnProgress func(progress parallel.Progress)
}

// NewStreamingBackupEngine creates a streaming wrapper for a backup engine
func NewStreamingBackupEngine(engine BackupEngine, cfg StreamingConfig, log logger.Logger) (*StreamingBackupEngine, error) {
	if !engine.SupportsStreaming() {
		return nil, fmt.Errorf("engine %s does not support streaming", engine.Name())
	}

	cloudCfg := parallel.DefaultConfig()
	cloudCfg.Bucket = cfg.Bucket
	cloudCfg.Key = cfg.Key
	cloudCfg.Region = cfg.Region
	cloudCfg.Endpoint = cfg.Endpoint

	if cfg.PartSize > 0 {
		cloudCfg.PartSize = cfg.PartSize
	}
	if cfg.WorkerCount > 0 {
		cloudCfg.WorkerCount = cfg.WorkerCount
	}
	if cfg.Encryption != "" {
		cloudCfg.ServerSideEncryption = cfg.Encryption
	}
	if cfg.KMSKeyID != "" {
		cloudCfg.KMSKeyID = cfg.KMSKeyID
	}

	return &StreamingBackupEngine{
		engine:   engine,
		cloudCfg: cloudCfg,
		log:      log,
	}, nil
}

// StreamBackup performs backup directly to cloud storage
func (s *StreamingBackupEngine) StreamBackup(ctx context.Context, opts *BackupOptions) (*BackupResult, error) {
	s.mu.Lock()
	if s.started {
		s.mu.Unlock()
		return nil, fmt.Errorf("backup already in progress")
	}
	s.started = true
	s.mu.Unlock()

	// Create cloud streamer
	streamer, err := parallel.NewCloudStreamer(s.cloudCfg)
	if err != nil {
		return nil, fmt.Errorf("failed to create cloud streamer: %w", err)
	}
	s.streamer = streamer

	// Start multipart upload
	if err := streamer.Start(ctx); err != nil {
		return nil, fmt.Errorf("failed to start upload: %w", err)
	}

	s.log.Info("Started streaming backup to s3://%s/%s", s.cloudCfg.Bucket, s.cloudCfg.Key)

	// Start progress monitoring
	progressDone := make(chan struct{})
	go s.monitorProgress(progressDone)

	// Get streaming engine
	streamEngine, ok := s.engine.(StreamingEngine)
	if !ok {
		_ = streamer.Cancel()
		return nil, fmt.Errorf("engine does not implement StreamingEngine")
	}

	// Perform streaming backup
	startTime := time.Now()
	result, err := streamEngine.BackupToWriter(ctx, streamer, opts)
	close(progressDone)

	if err != nil {
		_ = streamer.Cancel()
		return nil, fmt.Errorf("backup failed: %w", err)
	}

	// Complete upload
	location, err := streamer.Complete(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to complete upload: %w", err)
	}

	s.log.Info("Backup completed: %s", location)

	// Update result with cloud location
	progress := streamer.Progress()
	result.Files = append(result.Files, BackupFile{
		Path:     location,
		Size:     progress.BytesUploaded,
		Checksum: "", // Could compute from streamed data
		IsCloud:  true,
	})
	result.TotalSize = progress.BytesUploaded
	result.Duration = time.Since(startTime)

	s.mu.Lock()
	s.completed = true
	s.mu.Unlock()

	return result, nil
}

// monitorProgress monitors and reports upload progress
func (s *StreamingBackupEngine) monitorProgress(done chan struct{}) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-done:
			return
		case <-ticker.C:
			if s.streamer != nil {
				progress := s.streamer.Progress()
				s.log.Info("Upload progress: %d parts, %.2f MB uploaded, %.2f MB/s",
					progress.PartsUploaded,
					float64(progress.BytesUploaded)/(1024*1024),
					progress.Speed()/(1024*1024))
			}
		}
	}
}

// Cancel cancels the streaming backup
func (s *StreamingBackupEngine) Cancel() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.streamer != nil {
		return s.streamer.Cancel()
	}
	return nil
}

// DirectCloudBackupEngine performs backup directly to cloud without local storage
type DirectCloudBackupEngine struct {
	registry *Registry
	log      logger.Logger
}

// NewDirectCloudBackupEngine creates a new direct cloud backup engine
func NewDirectCloudBackupEngine(registry *Registry, log logger.Logger) *DirectCloudBackupEngine {
	return &DirectCloudBackupEngine{
		registry: registry,
		log:      log,
	}
}

// DirectBackupConfig holds configuration for direct cloud backup
type DirectBackupConfig struct {
	// Database
	DBType string
	DSN    string

	// Cloud
	CloudURI string // s3://bucket/path or gs://bucket/path
	Region   string
	Endpoint string

	// Engine selection
	PreferredEngine string // clone, snapshot, dump

	// Performance
	PartSize    int64
	WorkerCount int

	// Options
	Compression          bool
	CompressionAlgorithm string // "gzip" or "zstd" (default: "gzip")
	Encryption           string
	EncryptionKey        string
}

// Backup performs a direct backup to cloud
func (d *DirectCloudBackupEngine) Backup(ctx context.Context, cfg DirectBackupConfig) (*BackupResult, error) {
	// Parse cloud URI
	provider, bucket, key, err := parseCloudURI(cfg.CloudURI)
	if err != nil {
		return nil, err
	}

	// Find suitable engine
	var engine BackupEngine
	if cfg.PreferredEngine != "" {
		var engineErr error
		engine, engineErr = d.registry.Get(cfg.PreferredEngine)
		if engineErr != nil {
			return nil, fmt.Errorf("engine not found: %s", cfg.PreferredEngine)
		}
	} else {
		// Use first streaming-capable engine
		for _, info := range d.registry.List() {
			eng, err := d.registry.Get(info.Name)
			if err == nil && eng.SupportsStreaming() {
				engine = eng
				break
			}
		}
	}

	if engine == nil {
		return nil, fmt.Errorf("no streaming-capable engine available")
	}

	// Check availability
	avail, err := engine.CheckAvailability(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to check availability: %w", err)
	}
	if !avail.Available {
		return nil, fmt.Errorf("engine %s not available: %s", engine.Name(), avail.Reason)
	}

	d.log.Info("Using engine %s for direct cloud backup to %s", engine.Name(), cfg.CloudURI)

	// Build streaming config
	streamCfg := StreamingConfig{
		Bucket:      bucket,
		Key:         key,
		Region:      cfg.Region,
		Endpoint:    cfg.Endpoint,
		PartSize:    cfg.PartSize,
		WorkerCount: cfg.WorkerCount,
		Encryption:  cfg.Encryption,
	}

	// S3 is currently supported; GCS would need different implementation
	if provider != "s3" {
		return nil, fmt.Errorf("direct streaming only supported for S3 currently")
	}

	// Create streaming wrapper
	streaming, err := NewStreamingBackupEngine(engine, streamCfg, d.log)
	if err != nil {
		return nil, err
	}

	// Build backup options
	compFmt := cfg.CompressionAlgorithm
	if compFmt == "" {
		compFmt = "gzip"
	}
	opts := &BackupOptions{
		Compress:       cfg.Compression,
		CompressFormat: compFmt,
		EngineOptions: map[string]interface{}{
			"encryption_key": cfg.EncryptionKey,
		},
	}

	// Perform backup
	return streaming.StreamBackup(ctx, opts)
}

// parseCloudURI parses a cloud URI like s3://bucket/path
func parseCloudURI(uri string) (provider, bucket, key string, err error) {
	if len(uri) < 6 {
		return "", "", "", fmt.Errorf("invalid cloud URI: %s", uri)
	}

	if uri[:5] == "s3://" {
		provider = "s3"
		uri = uri[5:]
	} else if uri[:5] == "gs://" {
		provider = "gcs"
		uri = uri[5:]
	} else if len(uri) > 8 && uri[:8] == "azure://" {
		provider = "azure"
		uri = uri[8:]
	} else {
		return "", "", "", fmt.Errorf("unknown cloud provider in URI: %s", uri)
	}

	// Split bucket/key
	for i := 0; i < len(uri); i++ {
		if uri[i] == '/' {
			bucket = uri[:i]
			key = uri[i+1:]
			return
		}
	}

	bucket = uri
	return
}

// PipeReader creates a pipe for streaming backup data
type PipeReader struct {
	reader *io.PipeReader
	writer *io.PipeWriter
}

// NewPipeReader creates a new pipe reader
func NewPipeReader() *PipeReader {
	r, w := io.Pipe()
	return &PipeReader{
		reader: r,
		writer: w,
	}
}

// Reader returns the read end of the pipe
func (p *PipeReader) Reader() io.Reader {
	return p.reader
}

// Writer returns the write end of the pipe
func (p *PipeReader) Writer() io.WriteCloser {
	return p.writer
}

// Close closes both ends of the pipe
func (p *PipeReader) Close() error {
	_ = p.writer.Close()
	return p.reader.Close()
}
