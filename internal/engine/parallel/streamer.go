// Package parallel provides parallel cloud streaming capabilities
package parallel

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

// Config holds parallel upload configuration
type Config struct {
	// Bucket is the S3 bucket name
	Bucket string

	// Key is the object key
	Key string

	// Region is the AWS region
	Region string

	// Endpoint is optional custom endpoint (for MinIO, etc.)
	Endpoint string

	// PartSize is the size of each part (default 10MB)
	PartSize int64

	// WorkerCount is the number of parallel upload workers
	WorkerCount int

	// BufferSize is the size of the part channel buffer
	BufferSize int

	// ChecksumEnabled enables SHA256 checksums per part
	ChecksumEnabled bool

	// RetryCount is the number of retries per part
	RetryCount int

	// RetryDelay is the delay between retries
	RetryDelay time.Duration

	// ServerSideEncryption sets the encryption algorithm
	ServerSideEncryption string

	// KMSKeyID is the KMS key for encryption
	KMSKeyID string
}

// DefaultConfig returns default configuration
func DefaultConfig() Config {
	return Config{
		PartSize:        10 * 1024 * 1024, // 10MB
		WorkerCount:     4,
		BufferSize:      8,
		ChecksumEnabled: true,
		RetryCount:      3,
		RetryDelay:      time.Second,
	}
}

// part represents a part to upload
type part struct {
	Number int32
	Data   []byte
	Hash   string
}

// partResult represents the result of uploading a part
type partResult struct {
	Number int32
	ETag   string
	Error  error
}

// CloudStreamer provides parallel streaming uploads to S3
type CloudStreamer struct {
	cfg    Config
	client *s3.Client

	mu       sync.Mutex
	uploadID string
	key      string

	// Channels for worker pool
	partsCh   chan part
	resultsCh chan partResult
	workers   sync.WaitGroup
	cancel    context.CancelFunc

	// Current part buffer
	buffer     []byte
	bufferLen  int
	partNumber int32

	// Results tracking
	results      map[int32]string // partNumber -> ETag
	resultsMu    sync.RWMutex
	uploadErrors []error

	// Metrics
	bytesUploaded int64
	partsUploaded int64
	startTime     time.Time
}

// NewCloudStreamer creates a new parallel cloud streamer
func NewCloudStreamer(cfg Config) (*CloudStreamer, error) {
	if cfg.Bucket == "" {
		return nil, fmt.Errorf("bucket required")
	}
	if cfg.Key == "" {
		return nil, fmt.Errorf("key required")
	}

	// Apply defaults
	if cfg.PartSize == 0 {
		cfg.PartSize = 10 * 1024 * 1024
	}
	if cfg.WorkerCount == 0 {
		cfg.WorkerCount = 4
	}
	if cfg.BufferSize == 0 {
		cfg.BufferSize = cfg.WorkerCount * 2
	}
	if cfg.RetryCount == 0 {
		cfg.RetryCount = 3
	}

	// Load AWS config
	opts := []func(*config.LoadOptions) error{
		config.WithRegion(cfg.Region),
	}

	awsCfg, err := config.LoadDefaultConfig(context.Background(), opts...)
	if err != nil {
		return nil, fmt.Errorf("failed to load AWS config: %w", err)
	}

	// Create S3 client
	clientOpts := []func(*s3.Options){}
	if cfg.Endpoint != "" {
		clientOpts = append(clientOpts, func(o *s3.Options) {
			o.BaseEndpoint = aws.String(cfg.Endpoint)
			o.UsePathStyle = true
		})
	}

	client := s3.NewFromConfig(awsCfg, clientOpts...)

	return &CloudStreamer{
		cfg:     cfg,
		client:  client,
		buffer:  make([]byte, cfg.PartSize),
		results: make(map[int32]string),
	}, nil
}

// Start initiates the multipart upload and starts workers
func (cs *CloudStreamer) Start(ctx context.Context) error {
	cs.mu.Lock()
	defer cs.mu.Unlock()

	cs.startTime = time.Now()

	// Create multipart upload
	input := &s3.CreateMultipartUploadInput{
		Bucket: aws.String(cs.cfg.Bucket),
		Key:    aws.String(cs.cfg.Key),
	}

	if cs.cfg.ServerSideEncryption != "" {
		input.ServerSideEncryption = types.ServerSideEncryption(cs.cfg.ServerSideEncryption)
	}
	if cs.cfg.KMSKeyID != "" {
		input.SSEKMSKeyId = aws.String(cs.cfg.KMSKeyID)
	}

	result, err := cs.client.CreateMultipartUpload(ctx, input)
	if err != nil {
		return fmt.Errorf("failed to create multipart upload: %w", err)
	}

	cs.uploadID = *result.UploadId
	cs.key = *result.Key

	// Create channels
	cs.partsCh = make(chan part, cs.cfg.BufferSize)
	cs.resultsCh = make(chan partResult, cs.cfg.BufferSize)

	// Create cancellable context
	workerCtx, cancel := context.WithCancel(ctx)
	cs.cancel = cancel

	// Start workers
	for i := 0; i < cs.cfg.WorkerCount; i++ {
		cs.workers.Add(1)
		go cs.worker(workerCtx, i)
	}

	// Start result collector
	go cs.collectResults()

	return nil
}

// worker uploads parts from the channel
func (cs *CloudStreamer) worker(ctx context.Context, id int) {
	defer cs.workers.Done()

	for {
		select {
		case <-ctx.Done():
			return
		case p, ok := <-cs.partsCh:
			if !ok {
				return
			}

			etag, err := cs.uploadPart(ctx, p)
			cs.resultsCh <- partResult{
				Number: p.Number,
				ETag:   etag,
				Error:  err,
			}
		}
	}
}

// uploadPart uploads a single part with retries
func (cs *CloudStreamer) uploadPart(ctx context.Context, p part) (string, error) {
	var lastErr error

	for attempt := 0; attempt <= cs.cfg.RetryCount; attempt++ {
		if attempt > 0 {
			select {
			case <-ctx.Done():
				return "", ctx.Err()
			case <-time.After(cs.cfg.RetryDelay * time.Duration(attempt)):
			}
		}

		input := &s3.UploadPartInput{
			Bucket:     aws.String(cs.cfg.Bucket),
			Key:        aws.String(cs.cfg.Key),
			UploadId:   aws.String(cs.uploadID),
			PartNumber: aws.Int32(p.Number),
			Body:       newBytesReader(p.Data),
		}

		result, err := cs.client.UploadPart(ctx, input)
		if err != nil {
			lastErr = err
			continue
		}

		atomic.AddInt64(&cs.bytesUploaded, int64(len(p.Data)))
		atomic.AddInt64(&cs.partsUploaded, 1)

		return *result.ETag, nil
	}

	return "", fmt.Errorf("failed after %d retries: %w", cs.cfg.RetryCount, lastErr)
}

// collectResults collects results from workers
func (cs *CloudStreamer) collectResults() {
	for result := range cs.resultsCh {
		cs.resultsMu.Lock()
		if result.Error != nil {
			cs.uploadErrors = append(cs.uploadErrors, result.Error)
		} else {
			cs.results[result.Number] = result.ETag
		}
		cs.resultsMu.Unlock()
	}
}

// Write implements io.Writer for streaming data
func (cs *CloudStreamer) Write(p []byte) (int, error) {
	written := 0

	for len(p) > 0 {
		// Calculate how much we can write to the buffer
		available := int(cs.cfg.PartSize) - cs.bufferLen
		toWrite := len(p)
		if toWrite > available {
			toWrite = available
		}

		// Copy to buffer
		copy(cs.buffer[cs.bufferLen:], p[:toWrite])
		cs.bufferLen += toWrite
		written += toWrite
		p = p[toWrite:]

		// If buffer is full, send part
		if cs.bufferLen >= int(cs.cfg.PartSize) {
			if err := cs.sendPart(); err != nil {
				return written, err
			}
		}
	}

	return written, nil
}

// sendPart sends the current buffer as a part
func (cs *CloudStreamer) sendPart() error {
	if cs.bufferLen == 0 {
		return nil
	}

	cs.partNumber++

	// Copy buffer data
	data := make([]byte, cs.bufferLen)
	copy(data, cs.buffer[:cs.bufferLen])

	// Calculate hash if enabled
	var hash string
	if cs.cfg.ChecksumEnabled {
		h := sha256.Sum256(data)
		hash = hex.EncodeToString(h[:])
	}

	// Send to workers
	cs.partsCh <- part{
		Number: cs.partNumber,
		Data:   data,
		Hash:   hash,
	}

	// Reset buffer
	cs.bufferLen = 0

	return nil
}

// Complete finishes the upload
func (cs *CloudStreamer) Complete(ctx context.Context) (string, error) {
	// Send any remaining data
	if cs.bufferLen > 0 {
		if err := cs.sendPart(); err != nil {
			return "", err
		}
	}

	// Close parts channel and wait for workers
	close(cs.partsCh)
	cs.workers.Wait()
	close(cs.resultsCh)

	// Check for errors
	cs.resultsMu.RLock()
	if len(cs.uploadErrors) > 0 {
		err := cs.uploadErrors[0]
		cs.resultsMu.RUnlock()
		// Abort upload
		cs.abort(ctx)
		return "", err
	}

	// Build completed parts list
	parts := make([]types.CompletedPart, 0, len(cs.results))
	for num, etag := range cs.results {
		parts = append(parts, types.CompletedPart{
			PartNumber: aws.Int32(num),
			ETag:       aws.String(etag),
		})
	}
	cs.resultsMu.RUnlock()

	// Sort parts by number
	sortParts(parts)

	// Complete multipart upload
	result, err := cs.client.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{
		Bucket:   aws.String(cs.cfg.Bucket),
		Key:      aws.String(cs.cfg.Key),
		UploadId: aws.String(cs.uploadID),
		MultipartUpload: &types.CompletedMultipartUpload{
			Parts: parts,
		},
	})
	if err != nil {
		cs.abort(ctx)
		return "", fmt.Errorf("failed to complete upload: %w", err)
	}

	location := ""
	if result.Location != nil {
		location = *result.Location
	}

	return location, nil
}

// abort aborts the multipart upload
func (cs *CloudStreamer) abort(ctx context.Context) {
	if cs.uploadID == "" {
		return
	}

	cs.client.AbortMultipartUpload(ctx, &s3.AbortMultipartUploadInput{
		Bucket:   aws.String(cs.cfg.Bucket),
		Key:      aws.String(cs.cfg.Key),
		UploadId: aws.String(cs.uploadID),
	})
}

// Cancel cancels the upload
func (cs *CloudStreamer) Cancel() error {
	if cs.cancel != nil {
		cs.cancel()
	}
	cs.abort(context.Background())
	return nil
}

// Progress returns upload progress
func (cs *CloudStreamer) Progress() Progress {
	return Progress{
		BytesUploaded: atomic.LoadInt64(&cs.bytesUploaded),
		PartsUploaded: atomic.LoadInt64(&cs.partsUploaded),
		TotalParts:    int64(cs.partNumber),
		Duration:      time.Since(cs.startTime),
	}
}

// Progress represents upload progress
type Progress struct {
	BytesUploaded int64
	PartsUploaded int64
	TotalParts    int64
	Duration      time.Duration
}

// Speed returns the upload speed in bytes per second
func (p Progress) Speed() float64 {
	if p.Duration == 0 {
		return 0
	}
	return float64(p.BytesUploaded) / p.Duration.Seconds()
}

// bytesReader wraps a byte slice as an io.ReadSeekCloser
type bytesReader struct {
	data []byte
	pos  int
}

func newBytesReader(data []byte) *bytesReader {
	return &bytesReader{data: data}
}

func (r *bytesReader) Read(p []byte) (int, error) {
	if r.pos >= len(r.data) {
		return 0, io.EOF
	}
	n := copy(p, r.data[r.pos:])
	r.pos += n
	return n, nil
}

func (r *bytesReader) Seek(offset int64, whence int) (int64, error) {
	var newPos int64
	switch whence {
	case io.SeekStart:
		newPos = offset
	case io.SeekCurrent:
		newPos = int64(r.pos) + offset
	case io.SeekEnd:
		newPos = int64(len(r.data)) + offset
	}
	if newPos < 0 || newPos > int64(len(r.data)) {
		return 0, fmt.Errorf("invalid seek position")
	}
	r.pos = int(newPos)
	return newPos, nil
}

func (r *bytesReader) Close() error {
	return nil
}

// sortParts sorts completed parts by number
func sortParts(parts []types.CompletedPart) {
	for i := range parts {
		for j := i + 1; j < len(parts); j++ {
			if *parts[i].PartNumber > *parts[j].PartNumber {
				parts[i], parts[j] = parts[j], parts[i]
			}
		}
	}
}

// MultiFileUploader uploads multiple files in parallel
type MultiFileUploader struct {
	cfg       Config
	client    *s3.Client
	semaphore chan struct{}
}

// NewMultiFileUploader creates a new multi-file uploader
func NewMultiFileUploader(cfg Config) (*MultiFileUploader, error) {
	// Load AWS config
	awsCfg, err := config.LoadDefaultConfig(context.Background(),
		config.WithRegion(cfg.Region),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to load AWS config: %w", err)
	}

	clientOpts := []func(*s3.Options){}
	if cfg.Endpoint != "" {
		clientOpts = append(clientOpts, func(o *s3.Options) {
			o.BaseEndpoint = aws.String(cfg.Endpoint)
			o.UsePathStyle = true
		})
	}

	client := s3.NewFromConfig(awsCfg, clientOpts...)

	return &MultiFileUploader{
		cfg:       cfg,
		client:    client,
		semaphore: make(chan struct{}, cfg.WorkerCount),
	}, nil
}

// UploadFile represents a file to upload
type UploadFile struct {
	Key    string
	Reader io.Reader
	Size   int64
}

// UploadResult represents the result of an upload
type UploadResult struct {
	Key      string
	Location string
	Error    error
}

// Upload uploads multiple files in parallel
func (u *MultiFileUploader) Upload(ctx context.Context, files []UploadFile) []UploadResult {
	results := make([]UploadResult, len(files))
	var wg sync.WaitGroup

	for i, file := range files {
		wg.Add(1)
		go func(idx int, f UploadFile) {
			defer wg.Done()

			// Acquire semaphore
			select {
			case u.semaphore <- struct{}{}:
				defer func() { <-u.semaphore }()
			case <-ctx.Done():
				results[idx] = UploadResult{Key: f.Key, Error: ctx.Err()}
				return
			}

			// Upload file
			location, err := u.uploadFile(ctx, f)
			results[idx] = UploadResult{
				Key:      f.Key,
				Location: location,
				Error:    err,
			}
		}(i, file)
	}

	wg.Wait()
	return results
}

// uploadFile uploads a single file
func (u *MultiFileUploader) uploadFile(ctx context.Context, file UploadFile) (string, error) {
	// For small files, use PutObject
	if file.Size < u.cfg.PartSize {
		data, err := io.ReadAll(file.Reader)
		if err != nil {
			return "", err
		}

		result, err := u.client.PutObject(ctx, &s3.PutObjectInput{
			Bucket: aws.String(u.cfg.Bucket),
			Key:    aws.String(file.Key),
			Body:   newBytesReader(data),
		})
		if err != nil {
			return "", err
		}

		_ = result
		return fmt.Sprintf("s3://%s/%s", u.cfg.Bucket, file.Key), nil
	}

	// For large files, use multipart upload
	cfg := u.cfg
	cfg.Key = file.Key

	streamer, err := NewCloudStreamer(cfg)
	if err != nil {
		return "", err
	}

	if err := streamer.Start(ctx); err != nil {
		return "", err
	}

	if _, err := io.Copy(streamer, file.Reader); err != nil {
		streamer.Cancel()
		return "", err
	}

	return streamer.Complete(ctx)
}
