package cloud

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

// S3Backend implements the Backend interface for AWS S3 and compatible services
type S3Backend struct {
	client *s3.Client
	bucket string
	prefix string
	config *Config
}

// NewS3Backend creates a new S3 backend
func NewS3Backend(cfg *Config) (*S3Backend, error) {
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("invalid config: %w", err)
	}

	ctx := context.Background()

	// Build AWS config
	var awsCfg aws.Config
	var err error

	if cfg.AccessKey != "" && cfg.SecretKey != "" {
		// Use explicit credentials
		credsProvider := credentials.NewStaticCredentialsProvider(
			cfg.AccessKey,
			cfg.SecretKey,
			"",
		)

		awsCfg, err = config.LoadDefaultConfig(ctx,
			config.WithCredentialsProvider(credsProvider),
			config.WithRegion(cfg.Region),
		)
	} else {
		// Use default credential chain (environment, IAM role, etc.)
		awsCfg, err = config.LoadDefaultConfig(ctx,
			config.WithRegion(cfg.Region),
		)
	}

	if err != nil {
		return nil, fmt.Errorf("failed to load AWS config: %w", err)
	}

	// Create S3 client with custom options
	clientOptions := []func(*s3.Options){
		func(o *s3.Options) {
			if cfg.Endpoint != "" {
				o.BaseEndpoint = aws.String(cfg.Endpoint)
			}
			if cfg.PathStyle {
				o.UsePathStyle = true
			}
		},
	}

	client := s3.NewFromConfig(awsCfg, clientOptions...)

	return &S3Backend{
		client: client,
		bucket: cfg.Bucket,
		prefix: cfg.Prefix,
		config: cfg,
	}, nil
}

// Name returns the backend name
func (s *S3Backend) Name() string {
	return "s3"
}

// applyObjectLock sets Object Lock retention fields on a PutObjectInput if enabled
func (s *S3Backend) applyObjectLock(input *s3.PutObjectInput) {
	if !s.config.ObjectLockEnabled {
		return
	}

	mode := types.ObjectLockModeGovernance
	if strings.EqualFold(s.config.ObjectLockMode, "COMPLIANCE") {
		mode = types.ObjectLockModeCompliance
	}

	days := s.config.ObjectLockDays
	if days <= 0 {
		days = 30
	}

	retainUntil := time.Now().UTC().Add(time.Duration(days) * 24 * time.Hour)

	input.ObjectLockMode = mode
	input.ObjectLockRetainUntilDate = aws.Time(retainUntil)
}

// ValidateObjectLock checks if the bucket has Object Lock enabled
func (s *S3Backend) ValidateObjectLock(ctx context.Context) error {
	if !s.config.ObjectLockEnabled {
		return nil
	}

	_, err := s.client.GetObjectLockConfiguration(ctx, &s3.GetObjectLockConfigurationInput{
		Bucket: aws.String(s.bucket),
	})
	if err != nil {
		return fmt.Errorf("bucket %q does not have Object Lock enabled or is not accessible: %w\n"+
			"  Hint: Object Lock must be enabled at bucket creation time.\n"+
			"  Create a new bucket with: aws s3api create-bucket --bucket %s --object-lock-enabled-for-bucket",
			s.bucket, err, s.bucket)
	}

	// Validate mode
	mode := strings.ToUpper(s.config.ObjectLockMode)
	if mode != "" && mode != "GOVERNANCE" && mode != "COMPLIANCE" {
		return fmt.Errorf("invalid object lock mode %q: must be GOVERNANCE or COMPLIANCE", s.config.ObjectLockMode)
	}

	return nil
}

// buildKey creates the full S3 key from filename
func (s *S3Backend) buildKey(filename string) string {
	if s.prefix == "" {
		return filename
	}
	return filepath.Join(s.prefix, filename)
}

// Upload uploads a file to S3 with multipart support for large files
func (s *S3Backend) Upload(ctx context.Context, localPath, remotePath string, progress ProgressCallback) error {
	// Open local file
	file, err := os.Open(localPath)
	if err != nil {
		return fmt.Errorf("failed to open file: %w", err)
	}
	defer file.Close()

	// Get file size
	stat, err := file.Stat()
	if err != nil {
		return fmt.Errorf("failed to stat file: %w", err)
	}
	fileSize := stat.Size()

	// Build S3 key
	key := s.buildKey(remotePath)

	// Use multipart upload for files larger than 100MB
	const multipartThreshold = 100 * 1024 * 1024 // 100 MB

	if fileSize > multipartThreshold {
		return s.uploadMultipart(ctx, file, key, fileSize, progress)
	}

	// Simple upload for smaller files
	return s.uploadSimple(ctx, file, key, fileSize, progress)
}

// uploadSimple performs a simple single-part upload with retry
func (s *S3Backend) uploadSimple(ctx context.Context, file *os.File, key string, fileSize int64, progress ProgressCallback) error {
	return RetryOperationWithNotify(ctx, DefaultRetryConfig(), func() error {
		// Reset file position for retry
		if _, err := file.Seek(0, 0); err != nil {
			return fmt.Errorf("failed to reset file position: %w", err)
		}

		// Create progress reader
		var reader io.Reader = file
		if progress != nil {
			reader = NewProgressReader(file, fileSize, progress)
		}

		// Apply bandwidth throttling if configured
		if s.config.BandwidthLimit > 0 {
			reader = NewThrottledReader(ctx, reader, s.config.BandwidthLimit)
		}

		// Upload to S3
		input := &s3.PutObjectInput{
			Bucket: aws.String(s.bucket),
			Key:    aws.String(key),
			Body:   reader,
		}
		s.applyObjectLock(input)

		_, err := s.client.PutObject(ctx, input)

		if err != nil {
			return fmt.Errorf("failed to upload to S3: %w", err)
		}

		return nil
	}, func(err error, duration time.Duration) {
		fmt.Printf("[S3] Upload retry in %v: %v\n", duration, err)
	})
}

// uploadMultipart performs a multipart upload for large files with retry
func (s *S3Backend) uploadMultipart(ctx context.Context, file *os.File, key string, fileSize int64, progress ProgressCallback) error {
	return RetryOperationWithNotify(ctx, AggressiveRetryConfig(), func() error {
		// Reset file position for retry
		if _, err := file.Seek(0, 0); err != nil {
			return fmt.Errorf("failed to reset file position: %w", err)
		}

		// Calculate concurrency based on bandwidth limit
		// If limited, reduce concurrency to make throttling more effective
		concurrency := 10
		if s.config.BandwidthLimit > 0 {
			// With bandwidth limiting, use fewer concurrent parts
			concurrency = 3
		}

		// Create uploader with custom options
		uploader := manager.NewUploader(s.client, func(u *manager.Uploader) {
			// Part size: 10MB
			u.PartSize = 10 * 1024 * 1024

			// Adjust concurrency
			u.Concurrency = concurrency

			// Leave parts on failure for debugging
			u.LeavePartsOnError = false
		})

		// Wrap file with progress reader
		var reader io.Reader = file
		if progress != nil {
			reader = NewProgressReader(file, fileSize, progress)
		}

		// Apply bandwidth throttling if configured
		if s.config.BandwidthLimit > 0 {
			reader = NewThrottledReader(ctx, reader, s.config.BandwidthLimit)
		}

		// Upload with multipart
		input := &s3.PutObjectInput{
			Bucket: aws.String(s.bucket),
			Key:    aws.String(key),
			Body:   reader,
		}
		s.applyObjectLock(input)

		_, err := uploader.Upload(ctx, input)

		if err != nil {
			return fmt.Errorf("multipart upload failed: %w", err)
		}

		return nil
	}, func(err error, duration time.Duration) {
		fmt.Printf("[S3] Multipart upload retry in %v: %v\n", duration, err)
	})
}

// Download downloads a file from S3 with retry
func (s *S3Backend) Download(ctx context.Context, remotePath, localPath string, progress ProgressCallback) error {
	// Build S3 key
	key := s.buildKey(remotePath)

	// Get object size first
	size, err := s.GetSize(ctx, remotePath)
	if err != nil {
		return fmt.Errorf("failed to get object size: %w", err)
	}

	// Create directory for local file
	if err := os.MkdirAll(filepath.Dir(localPath), 0755); err != nil {
		return fmt.Errorf("failed to create directory: %w", err)
	}

	return RetryOperationWithNotify(ctx, DefaultRetryConfig(), func() error {
		// Download from S3
		result, err := s.client.GetObject(ctx, &s3.GetObjectInput{
			Bucket: aws.String(s.bucket),
			Key:    aws.String(key),
		})
		if err != nil {
			return fmt.Errorf("failed to download from S3: %w", err)
		}
		defer result.Body.Close()

		// Create/truncate local file
		outFile, err := os.Create(localPath)
		if err != nil {
			return fmt.Errorf("failed to create local file: %w", err)
		}
		defer outFile.Close()

		// Copy with progress tracking
		var reader io.Reader = result.Body
		if progress != nil {
			reader = NewProgressReader(result.Body, size, progress)
		}

		_, err = CopyWithContext(ctx, outFile, reader)
		if err != nil {
			return fmt.Errorf("failed to write file: %w", err)
		}

		return nil
	}, func(err error, duration time.Duration) {
		fmt.Printf("[S3] Download retry in %v: %v\n", duration, err)
	})
}

// List lists all backup files in S3
func (s *S3Backend) List(ctx context.Context, prefix string) ([]BackupInfo, error) {
	// Build full prefix
	fullPrefix := s.buildKey(prefix)

	// List objects
	result, err := s.client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
		Bucket: aws.String(s.bucket),
		Prefix: aws.String(fullPrefix),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to list objects: %w", err)
	}

	// Convert to BackupInfo
	var backups []BackupInfo
	for _, obj := range result.Contents {
		if obj.Key == nil {
			continue
		}

		key := *obj.Key
		name := filepath.Base(key)

		// Skip if it's just a directory marker
		if strings.HasSuffix(key, "/") {
			continue
		}

		info := BackupInfo{
			Key:          key,
			Name:         name,
			Size:         *obj.Size,
			LastModified: *obj.LastModified,
		}

		if obj.ETag != nil {
			info.ETag = *obj.ETag
		}

		if obj.StorageClass != "" {
			info.StorageClass = string(obj.StorageClass)
		} else {
			info.StorageClass = "STANDARD"
		}

		backups = append(backups, info)
	}

	return backups, nil
}

// Delete deletes a file from S3
func (s *S3Backend) Delete(ctx context.Context, remotePath string) error {
	key := s.buildKey(remotePath)

	_, err := s.client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
	})

	if err != nil {
		return fmt.Errorf("failed to delete object: %w", err)
	}

	return nil
}

// Exists checks if a file exists in S3
func (s *S3Backend) Exists(ctx context.Context, remotePath string) (bool, error) {
	key := s.buildKey(remotePath)

	_, err := s.client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
	})

	if err != nil {
		// Check if it's a "not found" error
		if strings.Contains(err.Error(), "NotFound") || strings.Contains(err.Error(), "404") {
			return false, nil
		}
		return false, fmt.Errorf("failed to check object existence: %w", err)
	}

	return true, nil
}

// GetSize returns the size of a remote file
func (s *S3Backend) GetSize(ctx context.Context, remotePath string) (int64, error) {
	key := s.buildKey(remotePath)

	result, err := s.client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
	})

	if err != nil {
		return 0, fmt.Errorf("failed to get object metadata: %w", err)
	}

	if result.ContentLength == nil {
		return 0, fmt.Errorf("content length not available")
	}

	return *result.ContentLength, nil
}

// BucketExists checks if the bucket exists and is accessible
func (s *S3Backend) BucketExists(ctx context.Context) (bool, error) {
	_, err := s.client.HeadBucket(ctx, &s3.HeadBucketInput{
		Bucket: aws.String(s.bucket),
	})

	if err != nil {
		if strings.Contains(err.Error(), "NotFound") || strings.Contains(err.Error(), "404") {
			return false, nil
		}
		return false, fmt.Errorf("failed to check bucket: %w", err)
	}

	return true, nil
}

// CreateBucket creates the bucket if it doesn't exist
func (s *S3Backend) CreateBucket(ctx context.Context) error {
	exists, err := s.BucketExists(ctx)
	if err != nil {
		return fmt.Errorf("check bucket existence failed: %w", err)
	}

	if exists {
		return nil
	}

	_, err = s.client.CreateBucket(ctx, &s3.CreateBucketInput{
		Bucket: aws.String(s.bucket),
	})

	if err != nil {
		return fmt.Errorf("failed to create bucket: %w", err)
	}

	return nil
}
