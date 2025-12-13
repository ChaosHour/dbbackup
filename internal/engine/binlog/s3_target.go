package binlog

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

// S3Target writes binlog events to S3
type S3Target struct {
	client   *s3.Client
	bucket   string
	prefix   string
	region   string
	partSize int64

	mu          sync.Mutex
	buffer      *bytes.Buffer
	bufferSize  int
	currentKey  string
	uploadID    string
	parts       []types.CompletedPart
	partNumber  int32
	fileNum     int
	healthy     bool
	lastErr     error
	lastWrite   time.Time
}

// NewS3Target creates a new S3 target
func NewS3Target(bucket, prefix, region string) (*S3Target, error) {
	if bucket == "" {
		return nil, fmt.Errorf("bucket required for S3 target")
	}

	// Load AWS config
	cfg, err := config.LoadDefaultConfig(context.Background(),
		config.WithRegion(region),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to load AWS config: %w", err)
	}

	client := s3.NewFromConfig(cfg)

	return &S3Target{
		client:   client,
		bucket:   bucket,
		prefix:   prefix,
		region:   region,
		partSize: 10 * 1024 * 1024, // 10MB parts
		buffer:   bytes.NewBuffer(nil),
		healthy:  true,
	}, nil
}

// Name returns the target name
func (s *S3Target) Name() string {
	return fmt.Sprintf("s3://%s/%s", s.bucket, s.prefix)
}

// Type returns the target type
func (s *S3Target) Type() string {
	return "s3"
}

// Write writes events to S3 buffer
func (s *S3Target) Write(ctx context.Context, events []*Event) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Write events to buffer
	for _, ev := range events {
		data, err := json.Marshal(ev)
		if err != nil {
			continue
		}

		data = append(data, '\n')
		s.buffer.Write(data)
		s.bufferSize += len(data)
	}

	// Upload part if buffer exceeds threshold
	if int64(s.bufferSize) >= s.partSize {
		if err := s.uploadPart(ctx); err != nil {
			s.healthy = false
			s.lastErr = err
			return err
		}
	}

	s.healthy = true
	s.lastWrite = time.Now()
	return nil
}

// uploadPart uploads the current buffer as a part
func (s *S3Target) uploadPart(ctx context.Context) error {
	if s.bufferSize == 0 {
		return nil
	}

	// Start multipart upload if not started
	if s.uploadID == "" {
		s.fileNum++
		s.currentKey = fmt.Sprintf("%sbinlog_%s_%04d.jsonl",
			s.prefix,
			time.Now().Format("20060102_150405"),
			s.fileNum)

		result, err := s.client.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{
			Bucket: aws.String(s.bucket),
			Key:    aws.String(s.currentKey),
		})
		if err != nil {
			return fmt.Errorf("failed to create multipart upload: %w", err)
		}
		s.uploadID = *result.UploadId
		s.parts = nil
		s.partNumber = 0
	}

	// Upload part
	s.partNumber++
	result, err := s.client.UploadPart(ctx, &s3.UploadPartInput{
		Bucket:     aws.String(s.bucket),
		Key:        aws.String(s.currentKey),
		UploadId:   aws.String(s.uploadID),
		PartNumber: aws.Int32(s.partNumber),
		Body:       bytes.NewReader(s.buffer.Bytes()),
	})
	if err != nil {
		return fmt.Errorf("failed to upload part: %w", err)
	}

	s.parts = append(s.parts, types.CompletedPart{
		ETag:       result.ETag,
		PartNumber: aws.Int32(s.partNumber),
	})

	// Reset buffer
	s.buffer.Reset()
	s.bufferSize = 0

	return nil
}

// Flush completes the current multipart upload
func (s *S3Target) Flush(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Upload remaining buffer
	if s.bufferSize > 0 {
		if err := s.uploadPart(ctx); err != nil {
			return err
		}
	}

	// Complete multipart upload
	if s.uploadID != "" && len(s.parts) > 0 {
		_, err := s.client.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{
			Bucket:   aws.String(s.bucket),
			Key:      aws.String(s.currentKey),
			UploadId: aws.String(s.uploadID),
			MultipartUpload: &types.CompletedMultipartUpload{
				Parts: s.parts,
			},
		})
		if err != nil {
			return fmt.Errorf("failed to complete upload: %w", err)
		}

		// Reset for next file
		s.uploadID = ""
		s.parts = nil
		s.partNumber = 0
	}

	return nil
}

// Close closes the target
func (s *S3Target) Close() error {
	return s.Flush(context.Background())
}

// Healthy returns target health status
func (s *S3Target) Healthy() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.healthy
}

// S3StreamingTarget supports larger files with resumable uploads
type S3StreamingTarget struct {
	*S3Target
	rotateSize   int64
	currentSize  int64
}

// NewS3StreamingTarget creates an S3 target with file rotation
func NewS3StreamingTarget(bucket, prefix, region string, rotateSize int64) (*S3StreamingTarget, error) {
	base, err := NewS3Target(bucket, prefix, region)
	if err != nil {
		return nil, err
	}

	if rotateSize == 0 {
		rotateSize = 1024 * 1024 * 1024 // 1GB default
	}

	return &S3StreamingTarget{
		S3Target:   base,
		rotateSize: rotateSize,
	}, nil
}

// Write writes with rotation support
func (s *S3StreamingTarget) Write(ctx context.Context, events []*Event) error {
	// Check if we need to rotate
	if s.currentSize >= s.rotateSize {
		if err := s.Flush(ctx); err != nil {
			return err
		}
		s.currentSize = 0
	}

	// Estimate size
	for _, ev := range events {
		s.currentSize += int64(len(ev.RawData))
	}

	return s.S3Target.Write(ctx, events)
}
