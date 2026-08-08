terraform {
  required_version = ">= 1.6.0"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
  # Backend lives in backend.tf so it can be initialised with per-env config.
}

provider "aws" {
  region = var.aws_region
}

# K-MON from the Kafka checklist: a shared SNS topic for operational alerts.
# The Kafka module's consumer-lag alarms publish here. Future modules
# (Redshift storage, dbt test failures, etc.) can reuse the same topic so
# the on-call subscribes once. alert_email is optional — set in tfvars to
# wire an email subscription; otherwise the topic is created without
# subscriptions and CloudWatch still records alarm state transitions.
locals {
  alert_topic_name = "${var.project_name}-${var.environment}-alerts"
  common_tags = {
    Project     = var.project_name
    Environment = var.environment
    Team        = var.team_name
  }
}

resource "aws_sns_topic" "alerts" {
  name              = local.alert_topic_name
  kms_master_key_id = "alias/aws/sns"
  tags              = local.common_tags
}

resource "aws_sns_topic_subscription" "alert_email" {
  count     = var.alert_email != "" ? 1 : 0
  topic_arn = aws_sns_topic.alerts.arn
  protocol  = "email"
  endpoint  = var.alert_email
}

# The Redshift admin password is stored here so runtime consumers (MWAA task
# processes, App Runner) resolve it themselves. Airflow receives only this ARN,
# which keeps the plaintext out of rendered templates, task logs, and argv.
# recovery_window_in_days = 0 avoids the 30-day deletion window blocking a
# re-apply that recreates the secret under the same name.
resource "aws_secretsmanager_secret" "redshift_admin" {
  name                    = "${var.project_name}-${var.environment}-redshift-admin"
  description             = "Redshift admin password for dbt, Great Expectations, and metadata tasks."
  recovery_window_in_days = 0
  tags                    = local.common_tags
}

resource "aws_secretsmanager_secret_version" "redshift_admin" {
  secret_id     = aws_secretsmanager_secret.redshift_admin.id
  secret_string = var.redshift_admin_password
}

module "s3" {
  source      = "./modules/s3"
  project     = var.project_name
  environment = var.environment
  team        = var.team_name
}

module "kafka" {
  source             = "./modules/kafka"
  project            = var.project_name
  environment        = var.environment
  team               = var.team_name
  private_subnet_ids = var.private_subnet_ids
  security_group_ids = var.msk_security_group_ids
  alert_topic_arn    = aws_sns_topic.alerts.arn
}

module "emr" {
  source                       = "./modules/emr"
  project                      = var.project_name
  environment                  = var.environment
  team                         = var.team_name
  vpc_id                       = var.vpc_id
  subnet_id                    = var.private_subnet_ids[0]
  emr_service_role_arn         = var.emr_service_role_arn
  emr_ec2_instance_profile_arn = var.emr_ec2_instance_profile_arn
  log_uri                      = "s3://${module.s3.bronze_bucket_name}/emr-logs/"
  artifacts_bucket             = module.s3.artifacts_bucket_name
  checkpoints_bucket           = module.s3.checkpoints_bucket_name
  master_instance_type         = var.emr_master_instance_type
  core_instance_type           = var.emr_core_instance_type
  core_instance_count          = var.emr_core_instance_count
  core_bid_price               = var.emr_core_bid_price
}

module "redshift" {
  source        = "./modules/redshift"
  project       = var.project_name
  environment   = var.environment
  team          = var.team_name
  vpc_id        = var.vpc_id
  subnet_ids    = var.redshift_subnet_ids
  allowed_cidrs = var.redshift_allowed_cidrs

  bronze_bucket = module.s3.bronze_bucket_name
  silver_bucket = module.s3.silver_bucket_name

  admin_username         = var.redshift_admin_username
  admin_user_password    = var.redshift_admin_password
  database_name          = var.redshift_database_name
  base_capacity_rpu      = var.redshift_base_capacity_rpu
  publicly_accessible    = var.redshift_publicly_accessible
  monthly_rpu_hour_limit = var.redshift_monthly_rpu_hour_limit
}

module "dashboard" {
  count       = var.enable_dashboard ? 1 : 0
  source      = "./modules/streamlit"
  project     = var.project_name
  environment = var.environment
  team        = var.team_name

  create_service           = var.dashboard_create_service
  image_tag                = var.dashboard_image_tag
  vpc_id                   = var.vpc_id
  vpc_connector_subnet_ids = var.dashboard_vpc_connector_subnet_ids

  enable_auth         = var.dashboard_enable_auth
  auth_domain_name    = var.dashboard_auth_domain_name
  acm_certificate_arn = var.dashboard_acm_certificate_arn
  alb_subnet_ids      = var.dashboard_alb_subnet_ids
  vpce_subnet_ids     = var.dashboard_vpce_subnet_ids
  auth_allowed_cidrs  = var.dashboard_auth_allowed_cidrs

  redshift_host       = module.redshift.endpoint
  redshift_database   = var.redshift_database_name
  redshift_user       = var.redshift_admin_username
  redshift_secret_arn = aws_secretsmanager_secret.redshift_admin.arn
}

module "mwaa" {
  count  = var.enable_mwaa ? 1 : 0
  source = "./modules/mwaa"

  project     = var.project_name
  environment = var.environment
  team        = var.team_name

  vpc_id                = var.vpc_id
  private_subnet_ids    = length(var.mwaa_subnet_ids) > 0 ? var.mwaa_subnet_ids : var.private_subnet_ids
  artifacts_bucket      = module.s3.artifacts_bucket_name
  bronze_bucket         = module.s3.bronze_bucket_name
  environment_class     = var.mwaa_environment_class
  max_workers           = var.mwaa_max_workers
  webserver_access_mode = var.mwaa_webserver_access_mode
  redshift_secret_arn   = aws_secretsmanager_secret.redshift_admin.arn
}
