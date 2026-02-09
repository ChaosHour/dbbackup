package cloud

import (
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

func TestApplyObjectLock_Disabled(t *testing.T) {
	backend := &S3Backend{
		config: &Config{
			ObjectLockEnabled: false,
		},
	}

	input := &s3.PutObjectInput{}
	backend.applyObjectLock(input)

	if input.ObjectLockMode != "" {
		t.Errorf("expected no ObjectLockMode when disabled, got %q", input.ObjectLockMode)
	}
	if input.ObjectLockRetainUntilDate != nil {
		t.Error("expected no RetainUntilDate when disabled")
	}
}

func TestApplyObjectLock_Governance(t *testing.T) {
	backend := &S3Backend{
		config: &Config{
			ObjectLockEnabled: true,
			ObjectLockMode:    "GOVERNANCE",
			ObjectLockDays:    30,
		},
	}

	input := &s3.PutObjectInput{}
	backend.applyObjectLock(input)

	if input.ObjectLockMode != types.ObjectLockModeGovernance {
		t.Errorf("expected GOVERNANCE, got %q", input.ObjectLockMode)
	}
	if input.ObjectLockRetainUntilDate == nil {
		t.Fatal("expected RetainUntilDate to be set")
	}

	// Should be ~30 days from now
	expected := time.Now().UTC().Add(30 * 24 * time.Hour)
	diff := input.ObjectLockRetainUntilDate.Sub(expected)
	if diff < -time.Minute || diff > time.Minute {
		t.Errorf("RetainUntilDate off by %v (expected ~30 days from now)", diff)
	}
}

func TestApplyObjectLock_Compliance(t *testing.T) {
	backend := &S3Backend{
		config: &Config{
			ObjectLockEnabled: true,
			ObjectLockMode:    "COMPLIANCE",
			ObjectLockDays:    90,
		},
	}

	input := &s3.PutObjectInput{}
	backend.applyObjectLock(input)

	if input.ObjectLockMode != types.ObjectLockModeCompliance {
		t.Errorf("expected COMPLIANCE, got %q", input.ObjectLockMode)
	}
	if input.ObjectLockRetainUntilDate == nil {
		t.Fatal("expected RetainUntilDate to be set")
	}

	// Should be ~90 days from now
	expected := time.Now().UTC().Add(90 * 24 * time.Hour)
	diff := input.ObjectLockRetainUntilDate.Sub(expected)
	if diff < -time.Minute || diff > time.Minute {
		t.Errorf("RetainUntilDate off by %v (expected ~90 days from now)", diff)
	}
}

func TestApplyObjectLock_DefaultDays(t *testing.T) {
	backend := &S3Backend{
		config: &Config{
			ObjectLockEnabled: true,
			ObjectLockMode:    "GOVERNANCE",
			ObjectLockDays:    0, // should default to 30
		},
	}

	input := &s3.PutObjectInput{}
	backend.applyObjectLock(input)

	if input.ObjectLockRetainUntilDate == nil {
		t.Fatal("expected RetainUntilDate to be set")
	}

	// Default 30 days
	expected := time.Now().UTC().Add(30 * 24 * time.Hour)
	diff := input.ObjectLockRetainUntilDate.Sub(expected)
	if diff < -time.Minute || diff > time.Minute {
		t.Errorf("default days should be 30, RetainUntilDate off by %v", diff)
	}
}

func TestApplyObjectLock_CaseInsensitiveMode(t *testing.T) {
	backend := &S3Backend{
		config: &Config{
			ObjectLockEnabled: true,
			ObjectLockMode:    "compliance", // lowercase
			ObjectLockDays:    7,
		},
	}

	input := &s3.PutObjectInput{}
	backend.applyObjectLock(input)

	if input.ObjectLockMode != types.ObjectLockModeCompliance {
		t.Errorf("expected COMPLIANCE for lowercase input, got %q", input.ObjectLockMode)
	}
}

func TestObjectLockConfig_InConfig(t *testing.T) {
	cfg := DefaultConfig()
	if cfg.ObjectLockEnabled {
		t.Error("ObjectLockEnabled should default to false")
	}
	if cfg.ObjectLockMode != "" {
		t.Errorf("ObjectLockMode should default to empty, got %q", cfg.ObjectLockMode)
	}
}
