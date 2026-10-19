variable "aws_region" {
  description = "AWS region for the state backend and budget."
  type        = string
  default     = "ap-southeast-1"
}

variable "project_name" {
  description = "Project tag/name prefix."
  type        = string
  default     = "retail-platform"
}

variable "team_name" {
  description = "Owning team tag value."
  type        = string
  default     = "data-platform"
}

variable "state_bucket_name" {
  description = "S3 bucket name for Terraform remote state. Must be globally unique and match `bucket` in every envs/<env>.backend.hcl. Convention: suffix with your AWS account ID, e.g. retail-platform-tfstate-123456789012."
  type        = string
}

variable "lock_table_name" {
  description = "DynamoDB table name for Terraform state locking."
  type        = string
  default     = "retail-platform-terraform-locks"
}

variable "monthly_budget_usd" {
  description = "Monthly cost budget limit in USD. Alerts fire at 50/80/100%."
  type        = string
  default     = "50"
}

variable "budget_alert_email" {
  description = "Email address that receives budget alert notifications."
  type        = string
}
