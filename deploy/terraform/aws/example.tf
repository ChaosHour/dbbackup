# dbbackup Terraform - AWS Example

variable "aws_region" {
  default = "us-east-1"
}

provider "aws" {
  region = var.aws_region
}

module "dbbackup_storage" {
  source = "./main.tf"
  
  environment    = "production"
  bucket_name    = "mycompany-database-backups"
  retention_days = 30
  glacier_days   = 365
}

output "bucket_name" {
  value = module.dbbackup_storage.bucket_name
}

output "setup_instructions" {
  value = module.dbbackup_storage.dbbackup_cloud_config
}
