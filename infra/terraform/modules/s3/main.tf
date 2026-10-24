variable "project" {
  description = "Project name for bucket naming and tags."
  type        = string
}

variable "environment" {
  description = "Environment name for bucket naming and tags."
  type        = string
}

variable "team" {
  description = "Owning team tag value."
  type        = string
}

locals {
  tags = {
    Project     = var.project
    Environment = var.environment
    Team        = var.team
  }

  bucket_prefix = "${var.project}-${var.environment}"
}

resource "aws_s3_bucket" "bronze" {
  bucket = "${local.bucket_prefix}-bronze"
  tags   = merge(local.tags, { Layer = "Bronze" })
}

resource "aws_s3_bucket" "silver" {
  bucket = "${local.bucket_prefix}-silver"
  tags   = merge(local.tags, { Layer = "Silver" })
}

resource "aws_s3_bucket" "gold" {
  bucket = "${local.bucket_prefix}-gold"
  tags   = merge(local.tags, { Layer = "Gold" })
}

resource "aws_s3_bucket" "artifacts" {
  bucket = "${local.bucket_prefix}-artifacts"
  tags   = merge(local.tags, { Layer = "Artifacts" })
}

resource "aws_s3_bucket" "checkpoints" {
  bucket = "${local.bucket_prefix}-checkpoints"
  tags   = merge(local.tags, { Layer = "Checkpoints" })
}

locals {
  bucket_ids = {
    bronze      = aws_s3_bucket.bronze.id
    silver      = aws_s3_bucket.silver.id
    gold        = aws_s3_bucket.gold.id
    artifacts   = aws_s3_bucket.artifacts.id
    checkpoints = aws_s3_bucket.checkpoints.id
  }
}

resource "aws_s3_bucket_public_access_block" "this" {
  for_each = local.bucket_ids

  bucket                  = each.value
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_versioning" "bronze" {
  bucket = aws_s3_bucket.bronze.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_versioning" "silver" {
  bucket = aws_s3_bucket.silver.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_versioning" "gold" {
  bucket = aws_s3_bucket.gold.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_versioning" "artifacts" {
  bucket = aws_s3_bucket.artifacts.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_versioning" "checkpoints" {
  bucket = aws_s3_bucket.checkpoints.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "bronze" {
  bucket = aws_s3_bucket.bronze.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "silver" {
  bucket = aws_s3_bucket.silver.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "gold" {
  bucket = aws_s3_bucket.gold.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "artifacts" {
  bucket = aws_s3_bucket.artifacts.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "checkpoints" {
  bucket = aws_s3_bucket.checkpoints.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "checkpoints" {
  bucket = aws_s3_bucket.checkpoints.id

  # Flink prunes its own retained checkpoints. Incremental RocksDB checkpoints
  # keep referencing shared files older than any fixed age, so live objects
  # under flink/ must never expire; only deleted versions are cleaned up.
  rule {
    id     = "expire-graphframes-scratch"
    status = "Enabled"
    filter {
      prefix = "graphframes/"
    }

    expiration {
      days = 14
    }
  }

  rule {
    id     = "expire-noncurrent-versions"
    status = "Enabled"
    filter {}

    noncurrent_version_expiration {
      noncurrent_days = 14
    }

    abort_incomplete_multipart_upload {
      days_after_initiation = 7
    }
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "bronze" {
  bucket = aws_s3_bucket.bronze.id

  rule {
    id     = "intelligent-tiering-bronze"
    status = "Enabled"
    filter {}

    transition {
      days          = 0
      storage_class = "INTELLIGENT_TIERING"
    }
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "silver" {
  bucket = aws_s3_bucket.silver.id

  rule {
    id     = "intelligent-tiering-silver"
    status = "Enabled"
    filter {}

    transition {
      days          = 0
      storage_class = "INTELLIGENT_TIERING"
    }
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "gold" {
  bucket = aws_s3_bucket.gold.id

  rule {
    id     = "intelligent-tiering-gold"
    status = "Enabled"
    filter {}

    transition {
      days          = 0
      storage_class = "INTELLIGENT_TIERING"
    }
  }
}

output "bronze_bucket_name" {
  description = "Name of bronze bucket."
  value       = aws_s3_bucket.bronze.bucket
}

output "silver_bucket_name" {
  description = "Name of silver bucket."
  value       = aws_s3_bucket.silver.bucket
}

output "gold_bucket_name" {
  description = "Name of gold bucket."
  value       = aws_s3_bucket.gold.bucket
}

output "artifacts_bucket_name" {
  description = "Name of artifacts bucket (Flink jobs, JARs, scripts)."
  value       = aws_s3_bucket.artifacts.bucket
}

output "checkpoints_bucket_name" {
  description = "Name of Flink checkpoints/savepoints bucket."
  value       = aws_s3_bucket.checkpoints.bucket
}
