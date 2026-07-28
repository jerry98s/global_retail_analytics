terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

locals {
  name_prefix    = "${var.project}-${var.environment}"
  namespace_name = "${local.name_prefix}-rs"
  workgroup_name = "${local.name_prefix}-rs-wg"
  glue_bronze_db = replace("${local.name_prefix}_bronze", "-", "_")
}

# --- IAM: role Redshift assumes for S3 (Spectrum) + Glue Data Catalog --------

data "aws_iam_policy_document" "assume" {
  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["redshift.amazonaws.com", "redshift-serverless.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "redshift_s3" {
  name               = "${local.name_prefix}-redshift-s3"
  assume_role_policy = data.aws_iam_policy_document.assume.json
}

data "aws_iam_policy_document" "redshift_s3" {
  statement {
    sid       = "ListPlatformBuckets"
    effect    = "Allow"
    actions   = ["s3:ListBucket", "s3:GetBucketLocation"]
    resources = ["arn:aws:s3:::${var.bronze_bucket}", "arn:aws:s3:::${var.silver_bucket}"]
  }

  statement {
    sid       = "ReadPlatformObjects"
    effect    = "Allow"
    actions   = ["s3:GetObject"]
    resources = ["arn:aws:s3:::${var.bronze_bucket}/*", "arn:aws:s3:::${var.silver_bucket}/*"]
  }

  statement {
    sid    = "GlueCatalog"
    effect = "Allow"
    actions = [
      "glue:GetDatabase", "glue:GetDatabases", "glue:GetTable", "glue:GetTables",
      "glue:GetPartition", "glue:GetPartitions", "glue:BatchGetPartition",
      "glue:CreateDatabase", "glue:CreateTable", "glue:UpdateTable",
    ]
    resources = ["*"]
  }
}

resource "aws_iam_role_policy" "redshift_s3" {
  name   = "${local.name_prefix}-redshift-s3-read"
  role   = aws_iam_role.redshift_s3.id
  policy = data.aws_iam_policy_document.redshift_s3.json
}

resource "aws_glue_catalog_database" "bronze" {
  name        = local.glue_bronze_db
  description = "Project=${var.project}; Environment=${var.environment}; bronze parquet for Redshift Spectrum"
}

# --- Security group ----------------------------------------------------------

resource "aws_security_group" "redshift" {
  name_prefix = "${local.name_prefix}-rs-"
  description = "Redshift Serverless (${var.environment}). Inbound 5439 from allowed_cidrs only."
  vpc_id      = var.vpc_id

  ingress {
    description = "Redshift SQL"
    from_port   = 5439
    to_port     = 5439
    protocol    = "tcp"
    cidr_blocks = var.allowed_cidrs
  }

  egress {
    description = "All outbound"
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = { Name = "${local.name_prefix}-rs-sg" }

  lifecycle {
    create_before_destroy = true
  }
}

# --- Redshift Serverless namespace + workgroup -------------------------------

resource "aws_redshiftserverless_namespace" "this" {
  namespace_name      = local.namespace_name
  admin_username      = var.admin_username
  admin_user_password = var.admin_user_password
  db_name             = var.database_name

  iam_roles            = [aws_iam_role.redshift_s3.arn]
  default_iam_role_arn = aws_iam_role.redshift_s3.arn

  tags = { Name = local.namespace_name }
}

resource "aws_redshiftserverless_workgroup" "this" {
  namespace_name = aws_redshiftserverless_namespace.this.namespace_name
  workgroup_name = local.workgroup_name

  base_capacity       = var.base_capacity_rpu
  publicly_accessible = var.publicly_accessible

  subnet_ids         = var.subnet_ids
  security_group_ids = [aws_security_group.redshift.id]

  enhanced_vpc_routing = false

  tags = { Name = local.workgroup_name }
}

# Cost guardrail replacing Snowflake resource monitors: cap monthly RPU-hours.
resource "aws_redshiftserverless_usage_limit" "monthly" {
  resource_arn  = aws_redshiftserverless_workgroup.this.arn
  usage_type    = "serverless-compute"
  amount        = var.monthly_rpu_hour_limit
  period        = "monthly"
  breach_action = var.usage_limit_breach_action
}
