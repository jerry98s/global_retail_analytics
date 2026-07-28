variable "project" {
  description = "Project name for naming and tagging."
  type        = string
}

variable "environment" {
  description = "Environment name for naming and tagging."
  type        = string
}

variable "team" {
  description = "Owning team tag value."
  type        = string
}

variable "private_subnet_ids" {
  description = "Private subnet IDs for MSK Serverless."
  type        = list(string)
}

variable "security_group_ids" {
  description = "Security groups for MSK Serverless."
  type        = list(string)
}

variable "consumer_lag_alarm_threshold_pct" {
  description = "PCTConsumerLag (%) above which the alarm fires. PCTConsumerLag is MSK's relative lag metric — 100 means the consumer is one full window behind, 0 means caught up. See docs/runbooks/kafka-operations.md."
  type        = number
  default     = 50
}

variable "consumer_lag_alarm_period_seconds" {
  description = "Evaluation period for the consumer-lag alarm. MSK Serverless emits PCTConsumerLag per minute; 300s = 5 datapoints over 5m."
  type        = number
  default     = 300
}

variable "consumer_lag_alarm_datapoints" {
  description = "Number of evaluation periods that must breach the threshold before the alarm fires."
  type        = number
  default     = 3
}

variable "alert_topic_arn" {
  description = "SNS topic ARN for Kafka consumer-lag alerts. Passed in from the platform stack so the alarm wiring stays in one place."
  type        = string
}

locals {
  cluster_name = "${var.project}-${var.environment}-msk-serverless"
  topic_names = [
    "inventory.events.v1",
    "clickstream.events.v1",
    "dlq.events.v1",
    "dlq.clickstream.schema_violations",
    "dlq.clickstream.business_violations",
    "dlq.inventory.schema_violations",
  ]
  # The two Flink consumer groups that read from the main event topics.
  # Alarms are wired per (consumer_group, topic) pair — the dimensions MSK
  # Serverless exposes for PCTConsumerLag. Keep this list in sync with
  # streaming/config/flink_conf.yaml consumer_group_* keys.
  consumer_lag_pairs = [
    { group = "flink-inventory-bronze-v1", topic = "inventory.events.v1" },
    { group = "flink-inventory-snapshot-v1", topic = "inventory.events.v1" },
    { group = "flink-clickstream-bronze-v1", topic = "clickstream.events.v1" },
  ]
  tags = {
    Project     = var.project
    Environment = var.environment
    Layer       = "Streaming"
    Team        = var.team
  }
}

resource "aws_msk_serverless_cluster" "this" {
  cluster_name = local.cluster_name

  vpc_config {
    subnet_ids         = var.private_subnet_ids
    security_group_ids = var.security_group_ids
  }

  client_authentication {
    sasl {
      iam {
        enabled = true
      }
    }
  }

  tags = local.tags
}

resource "aws_cloudwatch_log_group" "msk" {
  name              = "/aws/msk/${local.cluster_name}"
  retention_in_days = 30
  tags              = local.tags
}

# K-MON from the Kafka checklist: per-(consumer_group, topic) CloudWatch
# alarm on PCTConsumerLag. MSK Serverless publishes this metric under the
# AWS/MSK namespace; the alarm pages the on-call SNS topic when a consumer
# group falls behind by more than consumer_lag_alarm_threshold_pct of the
# max offset for >= consumer_lag_alarm_datapoints consecutive periods.
#
# This replaces the manual Burrow deployment mentioned in the interview
# checklist (item 4.6) — for an MSK Serverless estate, CloudWatch native
# metrics are operationally simpler and avoid running an extra service.
resource "aws_cloudwatch_metric_alarm" "consumer_lag" {
  for_each = {
    for pair in local.consumer_lag_pairs :
    "${pair.group}|${pair.topic}" => pair
  }

  alarm_name          = "${var.project}-${var.environment}-msk-lag-${replace(each.key, "|", "-")}"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = var.consumer_lag_alarm_datapoints
  threshold           = var.consumer_lag_alarm_threshold_pct
  metric_name         = "PCTConsumerLag"
  namespace           = "AWS/MSK"
  period              = var.consumer_lag_alarm_period_seconds
  statistic           = "Maximum"
  treat_missing_data  = "notBreaching"

  dimensions = {
    "Cluster ARN"    = aws_msk_serverless_cluster.this.arn
    "Consumer Group" = each.value.group
    "Topic"          = each.value.topic
  }

  alarm_description = "Flink consumer group ${each.value.group} on topic ${each.value.topic} is more than ${var.consumer_lag_alarm_threshold_pct}% behind the max offset. See docs/runbooks/kafka-operations.md#high-consumer-lag."

  alarm_actions = [var.alert_topic_arn]
  ok_actions    = [var.alert_topic_arn]

  tags = local.tags
}

resource "aws_ssm_parameter" "topic_catalog" {
  name        = "/${var.project}/${var.environment}/kafka/topics"
  description = "Kafka topic catalog for platform bootstrap jobs."
  type        = "String"
  value       = jsonencode(local.topic_names)
  tags        = local.tags
}

output "cluster_arn" {
  description = "MSK Serverless cluster ARN."
  value       = aws_msk_serverless_cluster.this.arn
}

output "bootstrap_brokers_sasl_iam" {
  description = "Bootstrap broker endpoint for SASL IAM."
  value       = aws_msk_serverless_cluster.this.bootstrap_brokers_sasl_iam
}

output "topic_names" {
  description = "Topic names expected by the platform."
  value       = local.topic_names
}

output "consumer_lag_alarm_names" {
  description = "CloudWatch alarm names for MSK consumer lag, for the platform stack outputs."
  value       = { for k, v in aws_cloudwatch_metric_alarm.consumer_lag : k => v.alarm_name }
}
