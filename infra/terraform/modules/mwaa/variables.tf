variable "project" {
  description = "Project name/prefix."
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

variable "vpc_id" {
  description = "VPC for the MWAA security group."
  type        = string
}

variable "private_subnet_ids" {
  description = "Private subnets for MWAA (>= 2 AZs)."
  type        = list(string)
}

variable "artifacts_bucket" {
  description = "S3 artifacts bucket name (hosts mwaa/dags, requirements, plugins)."
  type        = string
}

variable "bronze_bucket" {
  description = "S3 bronze bucket (POS Parquet bronze uploads from Airflow tasks)."
  type        = string
}

variable "airflow_version" {
  description = "Managed Airflow version."
  type        = string
  default     = "2.8.1"
}

variable "environment_class" {
  description = "MWAA environment class (mw1.small for dev, mw1.medium+ for prod)."
  type        = string
  default     = "mw1.small"
}

variable "max_workers" {
  description = "Maximum Airflow workers."
  type        = number
  default     = 2
}

variable "min_workers" {
  description = "Minimum Airflow workers."
  type        = number
  default     = 1
}

variable "webserver_access_mode" {
  description = "PUBLIC_ONLY or PRIVATE_ONLY."
  type        = string
  default     = "PUBLIC_ONLY"
}

variable "emr_cluster_id" {
  description = "EMR cluster the DAGs submit Spark/Flink steps to."
  type        = string
}

variable "redshift_workgroup_arn" {
  description = "Redshift Serverless workgroup the Data API tasks run against."
  type        = string
}

variable "alert_topic_arn" {
  description = "SNS topic for task-failure and data-quality alerts (Airflow Variable alert_topic_arn)."
  type        = string
}

variable "redshift_secret_arn" {
  description = "Secrets Manager ARN holding the Redshift admin password. DAG tasks receive this ARN (not the password) and fetch the value at runtime."
  type        = string
}
