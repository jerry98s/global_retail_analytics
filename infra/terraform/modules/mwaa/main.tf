terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

locals {
  name_prefix = "${var.project}-${var.environment}"
  env_name    = "${local.name_prefix}-airflow"
  tags = {
    Project     = var.project
    Environment = var.environment
    Team        = var.team
  }
}

resource "aws_security_group" "mwaa" {
  name_prefix = "${local.name_prefix}-mwaa-"
  description = "Security group for MWAA (${var.environment})."
  vpc_id      = var.vpc_id

  ingress {
    description = "MWAA intra-SG"
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    self        = true
  }

  egress {
    description = "All outbound"
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = merge(local.tags, { Name = "${local.name_prefix}-mwaa-sg" })

  lifecycle {
    create_before_destroy = true
  }
}

data "aws_iam_policy_document" "assume" {
  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["airflow.amazonaws.com", "airflow-env.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "execution" {
  name               = "${local.name_prefix}-mwaa-exec"
  assume_role_policy = data.aws_iam_policy_document.assume.json
  tags               = local.tags
}

resource "aws_iam_role_policy_attachment" "mwaa_full" {
  role       = aws_iam_role.execution.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonMWAAFullAccess"
}

data "aws_iam_policy_document" "dag_tasks" {
  statement {
    sid    = "ArtifactsAndBronzeS3"
    effect = "Allow"
    actions = [
      "s3:GetObject",
      "s3:PutObject",
      "s3:DeleteObject",
      "s3:ListBucket",
    ]
    resources = [
      "arn:aws:s3:::${var.artifacts_bucket}",
      "arn:aws:s3:::${var.artifacts_bucket}/*",
      "arn:aws:s3:::${var.bronze_bucket}",
      "arn:aws:s3:::${var.bronze_bucket}/*",
    ]
  }

  statement {
    sid    = "EmrBatchSteps"
    effect = "Allow"
    actions = [
      "elasticmapreduce:AddJobFlowSteps",
      "elasticmapreduce:DescribeStep",
      "elasticmapreduce:ListSteps",
      "elasticmapreduce:DescribeCluster",
    ]
    resources = ["*"]
  }

  statement {
    sid    = "RedshiftDataApi"
    effect = "Allow"
    actions = [
      "redshift-data:ExecuteStatement",
      "redshift-data:DescribeStatement",
      "redshift-data:GetStatementResult",
      "redshift-data:CancelStatement",
    ]
    resources = ["*"]
  }

  statement {
    sid    = "RedshiftServerlessCredentials"
    effect = "Allow"
    actions = [
      "redshift-serverless:GetCredentials",
    ]
    resources = ["*"]
  }
}

resource "aws_iam_role_policy" "dag_tasks" {
  name   = "${local.name_prefix}-mwaa-dag-tasks"
  role   = aws_iam_role.execution.id
  policy = data.aws_iam_policy_document.dag_tasks.json
}

# MWAA reads DAGs, requirements, and plugins from the artifacts bucket.
resource "aws_s3_object" "requirements" {
  bucket  = var.artifacts_bucket
  key     = "mwaa/requirements.txt"
  content = file("${path.module}/requirements.txt")
  etag    = filemd5("${path.module}/requirements.txt")
}

resource "aws_s3_object" "plugins" {
  bucket = var.artifacts_bucket
  key    = "mwaa/plugins.zip"
  source = "${path.module}/plugins.zip"
  etag   = filemd5("${path.module}/plugins.zip")
}

resource "aws_mwaa_environment" "this" {
  name               = local.env_name
  airflow_version    = var.airflow_version
  environment_class  = var.environment_class
  execution_role_arn = aws_iam_role.execution.arn

  source_bucket_arn    = "arn:aws:s3:::${var.artifacts_bucket}"
  dag_s3_path          = "mwaa/dags"
  requirements_s3_path = "mwaa/requirements.txt"
  plugins_s3_path      = "mwaa/plugins.zip"

  max_workers = var.max_workers
  min_workers = var.min_workers

  webserver_access_mode = var.webserver_access_mode

  network_configuration {
    security_group_ids = [aws_security_group.mwaa.id]
    subnet_ids         = var.private_subnet_ids
  }

  logging_configuration {
    dag_processing_logs {
      enabled   = true
      log_level = "INFO"
    }
    scheduler_logs {
      enabled   = true
      log_level = "INFO"
    }
    task_logs {
      enabled   = true
      log_level = "INFO"
    }
    webserver_logs {
      enabled   = true
      log_level = "INFO"
    }
    worker_logs {
      enabled   = true
      log_level = "INFO"
    }
  }

  tags = merge(local.tags, { Name = local.env_name })

  depends_on = [aws_s3_object.requirements, aws_s3_object.plugins]
}
