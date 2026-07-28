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
  description = "VPC the Redshift security group is created in."
  type        = string
}

variable "subnet_ids" {
  description = "Subnet IDs (>= 3 AZs) for the Redshift Serverless workgroup."
  type        = list(string)
}

variable "allowed_cidrs" {
  description = "CIDRs allowed to reach Redshift on 5439."
  type        = list(string)
}

variable "bronze_bucket" {
  description = "Bronze S3 bucket name Redshift reads via Spectrum."
  type        = string
}

variable "silver_bucket" {
  description = "Silver S3 bucket name Redshift reads via Spectrum."
  type        = string
}

variable "admin_username" {
  description = "Redshift Serverless admin user."
  type        = string
  default     = "rs_admin"
}

variable "admin_user_password" {
  description = "Redshift Serverless admin password (provide via tfvars/TF_VAR)."
  type        = string
  sensitive   = true
}

variable "database_name" {
  description = "Default database created in the namespace."
  type        = string
  default     = "prod"
}

variable "base_capacity_rpu" {
  description = "Redshift Serverless base capacity in RPUs."
  type        = number
  default     = 32
}

variable "publicly_accessible" {
  description = "Expose the workgroup publicly. Keep false for prod (VPC-only)."
  type        = bool
  default     = false
}

variable "monthly_rpu_hour_limit" {
  description = "Monthly RPU-hour usage cap (cost guardrail; replaces Snowflake resource monitors)."
  type        = number
  default     = 1000
}

variable "usage_limit_breach_action" {
  description = "Action when the monthly usage limit is breached: log, emit-metric, or deactivate."
  type        = string
  default     = "deactivate"
}
