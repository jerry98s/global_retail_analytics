terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

locals {
  name_prefix  = "${var.project}-${var.environment}"
  service_name = "${local.name_prefix}-dashboard"
  use_vpc      = length(var.vpc_connector_subnet_ids) > 0
  tags = {
    Project     = var.project
    Environment = var.environment
    Team        = var.team
  }
}

# --- Container registry -------------------------------------------------------

resource "aws_ecr_repository" "this" {
  name                 = "${local.name_prefix}-dashboard"
  image_tag_mutability = "MUTABLE"
  force_delete         = true

  image_scanning_configuration {
    scan_on_push = true
  }

  tags = merge(local.tags, { Name = "${local.name_prefix}-dashboard" })
}

# --- Secret: Redshift password injected as a runtime secret -------------------

resource "aws_secretsmanager_secret" "rs_password" {
  name_prefix             = "${local.name_prefix}-dashboard-rs-"
  description             = "Redshift password for the ${local.service_name} App Runner service."
  recovery_window_in_days = 0
  tags                    = local.tags
}

resource "aws_secretsmanager_secret_version" "rs_password" {
  secret_id     = aws_secretsmanager_secret.rs_password.id
  secret_string = var.redshift_password
}

# --- IAM: ECR access role (App Runner pulls the image) ------------------------

data "aws_iam_policy_document" "access_assume" {
  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["build.apprunner.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "access" {
  name               = "${local.name_prefix}-dashboard-access"
  assume_role_policy = data.aws_iam_policy_document.access_assume.json
  tags               = local.tags
}

resource "aws_iam_role_policy_attachment" "access_ecr" {
  role       = aws_iam_role.access.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSAppRunnerServicePolicyForECRAccess"
}

# --- IAM: instance role (running task reads the secret) -----------------------

data "aws_iam_policy_document" "instance_assume" {
  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["tasks.apprunner.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "instance" {
  name               = "${local.name_prefix}-dashboard-instance"
  assume_role_policy = data.aws_iam_policy_document.instance_assume.json
  tags               = local.tags
}

data "aws_iam_policy_document" "instance_secret" {
  statement {
    sid       = "ReadRedshiftSecret"
    effect    = "Allow"
    actions   = ["secretsmanager:GetSecretValue"]
    resources = [aws_secretsmanager_secret.rs_password.arn]
  }
}

resource "aws_iam_role_policy" "instance_secret" {
  name   = "${local.name_prefix}-dashboard-secret-read"
  role   = aws_iam_role.instance.id
  policy = data.aws_iam_policy_document.instance_secret.json
}

# --- Optional VPC connector (reach a VPC-only Redshift workgroup) -------------

resource "aws_security_group" "connector" {
  count       = local.use_vpc ? 1 : 0
  name_prefix = "${local.name_prefix}-dash-"
  description = "App Runner VPC connector egress for the dashboard."
  vpc_id      = var.vpc_id

  egress {
    description = "All outbound (Redshift 5439, AWS APIs)."
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = merge(local.tags, { Name = "${local.name_prefix}-dash-sg" })

  lifecycle {
    create_before_destroy = true
  }
}

resource "aws_apprunner_vpc_connector" "this" {
  count              = local.use_vpc ? 1 : 0
  vpc_connector_name = "${local.name_prefix}-dash-vpc"
  subnets            = var.vpc_connector_subnet_ids
  security_groups    = [aws_security_group.connector[0].id]
  tags               = local.tags
}

# --- App Runner service -------------------------------------------------------

resource "aws_apprunner_service" "this" {
  count        = var.create_service ? 1 : 0
  service_name = local.service_name

  source_configuration {
    auto_deployments_enabled = false

    authentication_configuration {
      access_role_arn = aws_iam_role.access.arn
    }

    image_repository {
      image_identifier      = "${aws_ecr_repository.this.repository_url}:${var.image_tag}"
      image_repository_type = "ECR"

      image_configuration {
        port = "8501"

        runtime_environment_variables = {
          DASHBOARD_MODE = "redshift"
          RS_HOST        = var.redshift_host
          RS_PORT        = tostring(var.redshift_port)
          RS_DATABASE    = var.redshift_database
          RS_USER        = var.redshift_user
        }

        runtime_environment_secrets = {
          RS_PASSWORD = aws_secretsmanager_secret.rs_password.arn
        }
      }
    }
  }

  instance_configuration {
    cpu               = var.cpu
    memory            = var.memory
    instance_role_arn = aws_iam_role.instance.arn
  }

  health_check_configuration {
    protocol            = "HTTP"
    path                = "/_stcore/health"
    interval            = 10
    timeout             = 5
    healthy_threshold   = 1
    unhealthy_threshold = 5
  }

  network_configuration {
    egress_configuration {
      egress_type       = local.use_vpc ? "VPC" : "DEFAULT"
      vpc_connector_arn = local.use_vpc ? aws_apprunner_vpc_connector.this[0].arn : null
    }
  }

  tags = merge(local.tags, { Name = local.service_name })

  depends_on = [aws_secretsmanager_secret_version.rs_password]
}
