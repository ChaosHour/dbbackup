# dbbackup Terraform Module - AWS Deployment
# Creates S3 bucket for backup storage with proper security

terraform {
  required_version = ">= 1.0"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = ">= 4.0"
    }
  }
}

# Variables
variable "environment" {
  description = "Environment name (e.g., production, staging)"
  type        = string
  default     = "production"
}

variable "bucket_name" {
  description = "S3 bucket name for backups"
  type        = string
}

variable "retention_days" {
  description = "Days to keep backups before transitioning to Glacier"
  type        = number
  default     = 30
}

variable "glacier_days" {
  description = "Days to keep in Glacier before deletion (0 = keep forever)"
  type        = number
  default     = 365
}

variable "enable_encryption" {
  description = "Enable server-side encryption"
  type        = bool
  default     = true
}

variable "kms_key_arn" {
  description = "KMS key ARN for encryption (leave empty for aws/s3 managed key)"
  type        = string
  default     = ""
}

# S3 Bucket
resource "aws_s3_bucket" "backups" {
  bucket = var.bucket_name
  
  tags = {
    Name        = "Database Backups"
    Environment = var.environment
    ManagedBy   = "terraform"
    Application = "dbbackup"
  }
}

# Versioning
resource "aws_s3_bucket_versioning" "backups" {
  bucket = aws_s3_bucket.backups.id
  versioning_configuration {
    status = "Enabled"
  }
}

# Encryption
resource "aws_s3_bucket_server_side_encryption_configuration" "backups" {
  count  = var.enable_encryption ? 1 : 0
  bucket = aws_s3_bucket.backups.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm     = var.kms_key_arn != "" ? "aws:kms" : "AES256"
      kms_master_key_id = var.kms_key_arn != "" ? var.kms_key_arn : null
    }
    bucket_key_enabled = true
  }
}

# Lifecycle Rules
resource "aws_s3_bucket_lifecycle_configuration" "backups" {
  bucket = aws_s3_bucket.backups.id

  rule {
    id     = "transition-to-glacier"
    status = "Enabled"

    filter {
      prefix = ""
    }

    transition {
      days          = var.retention_days
      storage_class = "GLACIER"
    }

    dynamic "expiration" {
      for_each = var.glacier_days > 0 ? [1] : []
      content {
        days = var.retention_days + var.glacier_days
      }
    }

    noncurrent_version_transition {
      noncurrent_days = 30
      storage_class   = "GLACIER"
    }

    noncurrent_version_expiration {
      noncurrent_days = 90
    }
  }
}

# Block Public Access
resource "aws_s3_bucket_public_access_block" "backups" {
  bucket = aws_s3_bucket.backups.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# IAM User for dbbackup
resource "aws_iam_user" "dbbackup" {
  name = "dbbackup-${var.environment}"
  path = "/service-accounts/"
  
  tags = {
    Application = "dbbackup"
    Environment = var.environment
  }
}

resource "aws_iam_access_key" "dbbackup" {
  user = aws_iam_user.dbbackup.name
}

# IAM Policy
resource "aws_iam_user_policy" "dbbackup" {
  name = "dbbackup-s3-access"
  user = aws_iam_user.dbbackup.name

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:DeleteObject",
          "s3:ListBucket",
          "s3:GetBucketLocation"
        ]
        Resource = [
          aws_s3_bucket.backups.arn,
          "${aws_s3_bucket.backups.arn}/*"
        ]
      }
    ]
  })
}

# Outputs
output "bucket_name" {
  description = "S3 bucket name"
  value       = aws_s3_bucket.backups.id
}

output "bucket_arn" {
  description = "S3 bucket ARN"
  value       = aws_s3_bucket.backups.arn
}

output "access_key_id" {
  description = "IAM access key ID for dbbackup"
  value       = aws_iam_access_key.dbbackup.id
}

output "secret_access_key" {
  description = "IAM secret access key for dbbackup"
  value       = aws_iam_access_key.dbbackup.secret
  sensitive   = true
}

output "dbbackup_cloud_config" {
  description = "Cloud configuration for dbbackup"
  value       = <<-EOT
    # Add to dbbackup environment:
    export AWS_ACCESS_KEY_ID="${aws_iam_access_key.dbbackup.id}"
    export AWS_SECRET_ACCESS_KEY="<run: terraform output -raw secret_access_key>"
    
    # Use with dbbackup:
    dbbackup backup cluster --cloud s3://${aws_s3_bucket.backups.id}/backups/
  EOT
}
