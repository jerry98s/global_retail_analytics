variable "project" {
  description = "Project name for naming and tags."
  type        = string
}

variable "environment" {
  description = "Environment name."
  type        = string
}

variable "team" {
  description = "Owning team tag value."
  type        = string
}

variable "subnet_id" {
  description = "Private subnet ID for the EMR primary node."
  type        = string
}

variable "vpc_id" {
  description = "VPC ID where the EMR security groups are created."
  type        = string
}

variable "emr_service_role_arn" {
  description = "Existing EMR service role ARN."
  type        = string
}

variable "emr_ec2_instance_profile_arn" {
  description = "Existing EMR EC2 instance profile ARN."
  type        = string
}

variable "log_uri" {
  description = "S3 URI for EMR logs."
  type        = string
}

variable "artifacts_bucket" {
  description = "S3 bucket where the Flink connector bootstrap script and job code live."
  type        = string
}

variable "checkpoints_bucket" {
  description = "S3 bucket for Flink checkpoints / savepoints."
  type        = string
}

locals {
  cluster_name = "${var.project}-${var.environment}-flink-emr"
  tags = {
    Project     = var.project
    Environment = var.environment
    Layer       = "Streaming"
    Team        = var.team
  }
}

resource "aws_security_group" "emr_master" {
  name_prefix = "${var.project}-${var.environment}-emr-master-"
  description = "Security group for EMR Flink master node."
  vpc_id      = var.vpc_id
  tags        = merge(local.tags, { Name = "${local.cluster_name}-master-sg" })
}

resource "aws_security_group" "emr_core" {
  name_prefix = "${var.project}-${var.environment}-emr-core-"
  description = "Security group for EMR Flink core nodes."
  vpc_id      = aws_security_group.emr_master.vpc_id
  tags        = merge(local.tags, { Name = "${local.cluster_name}-core-sg" })
}

resource "aws_emr_cluster" "this" {
  name          = local.cluster_name
  release_label = "emr-6.15.0"
  applications  = ["Flink", "Spark"]

  service_role = var.emr_service_role_arn

  ec2_attributes {
    subnet_id                         = var.subnet_id
    instance_profile                  = var.emr_ec2_instance_profile_arn
    emr_managed_master_security_group = aws_security_group.emr_master.id
    emr_managed_slave_security_group  = aws_security_group.emr_core.id
  }

  master_instance_group {
    instance_type  = "m5.xlarge"
    instance_count = 1
    name           = "master-nodes"
  }

  core_instance_group {
    name           = "core-nodes-spot"
    instance_type  = "m5.2xlarge"
    instance_count = 2
    bid_price      = "0.28"
  }

  configurations_json = jsonencode([
    {
      Classification = "flink-conf"
      Properties = {
        "state.checkpoints.dir"        = "s3://${var.checkpoints_bucket}/flink/checkpoints"
        "state.savepoints.dir"         = "s3://${var.checkpoints_bucket}/flink/savepoints"
        "state.backend"                = "rocksdb"
        "execution.checkpointing.mode" = "EXACTLY_ONCE"
        "high-availability"            = "zookeeper"
        "rest.flamegraph.enabled"      = "true"
      }
    }
  ])

  bootstrap_action {
    name = "install-flink-connectors"
    path = "s3://${var.artifacts_bucket}/bootstrap/install_flink_connectors.sh"
    args = [var.artifacts_bucket]
  }

  scale_down_behavior  = "TERMINATE_AT_TASK_COMPLETION"
  log_uri              = var.log_uri
  ebs_root_volume_size = 50
  visible_to_all_users = false

  tags = local.tags
}

output "cluster_id" {
  description = "EMR cluster ID."
  value       = aws_emr_cluster.this.id
}

output "cluster_name" {
  description = "EMR cluster name."
  value       = aws_emr_cluster.this.name
}
