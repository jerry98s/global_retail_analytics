variable "aws_region" {
  description = "AWS region for retail platform resources."
  type        = string
  default     = "ap-southeast-1"
}

variable "environment" {
  description = "Deployment environment name."
  type        = string
  default     = "dev"
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

variable "vpc_id" {
  description = "VPC for MSK and EMR resources."
  type        = string
}

variable "private_subnet_ids" {
  description = "Private subnet IDs used by MSK Serverless and EMR."
  type        = list(string)
}

variable "msk_security_group_ids" {
  description = "Security groups attached to MSK Serverless."
  type        = list(string)
}

variable "emr_service_role_arn" {
  description = "Existing EMR service role ARN."
  type        = string
}

variable "emr_ec2_instance_profile_arn" {
  description = "Existing EMR EC2 instance profile ARN."
  type        = string
}

variable "emr_master_instance_type" {
  description = "EMR master node instance type."
  type        = string
  default     = "m5.xlarge"
}

variable "emr_core_instance_type" {
  description = "EMR core node instance type."
  type        = string
  default     = "m5.2xlarge"
}

variable "emr_core_instance_count" {
  description = "Number of EMR core nodes."
  type        = number
  default     = 2
}

variable "emr_core_bid_price" {
  description = "Spot bid price for EMR core nodes."
  type        = string
  default     = "0.28"
}

variable "redshift_subnet_ids" {
  description = "Subnet IDs (>= 3 AZs) for the Redshift Serverless workgroup."
  type        = list(string)
}

variable "redshift_allowed_cidrs" {
  description = "CIDRs allowed to reach Redshift on 5439 (keep tight in prod)."
  type        = list(string)
}

variable "redshift_admin_username" {
  description = "Redshift Serverless admin user."
  type        = string
  default     = "rs_admin"
}

variable "redshift_admin_password" {
  description = "Redshift Serverless admin password. Provide via prod.tfvars (git-ignored) or TF_VAR_redshift_admin_password."
  type        = string
  sensitive   = true
}

variable "redshift_database_name" {
  description = "Default database created in the Redshift namespace."
  type        = string
  default     = "prod"
}

variable "redshift_metadata_database_name" {
  description = "Separate operational metadata database on the same workgroup (not the namespace db_name). Created via transformation/redshift/metadata DDL."
  type        = string
  default     = "metadata"
}

variable "redshift_base_capacity_rpu" {
  description = "Redshift Serverless base capacity in RPUs."
  type        = number
  default     = 32
}

variable "redshift_publicly_accessible" {
  description = "Expose the workgroup publicly. Keep false for prod (VPC-only)."
  type        = bool
  default     = false
}

variable "redshift_monthly_rpu_hour_limit" {
  description = "Monthly RPU-hour cap (cost guardrail replacing Snowflake resource monitors)."
  type        = number
  default     = 1000
}

variable "enable_dashboard" {
  description = "Create the Streamlit dashboard module (ECR + IAM + optional App Runner service)."
  type        = bool
  default     = false
}

variable "dashboard_create_service" {
  description = "Create the App Runner service. Keep false until the image is pushed to ECR, then set true and re-apply."
  type        = bool
  default     = false
}

variable "dashboard_image_tag" {
  description = "ECR image tag the dashboard App Runner service runs."
  type        = string
  default     = "latest"
}

variable "dashboard_vpc_connector_subnet_ids" {
  description = "Private subnets for the dashboard's App Runner VPC connector (needed when Redshift is VPC-only). Empty = public egress."
  type        = list(string)
  default     = []
}

variable "dashboard_enable_auth" {
  description = <<-EOT
    Put a Cognito login in front of the dashboard: the App Runner service
    becomes private (VPC-only), and a public ALB terminates HTTPS, requires a
    Cognito Hosted UI login, and forwards authenticated traffic. Requires
    dashboard_auth_domain_name, dashboard_acm_certificate_arn,
    dashboard_alb_subnet_ids, and dashboard_vpce_subnet_ids.
  EOT
  type        = bool
  default     = false
}

variable "dashboard_auth_domain_name" {
  description = "Public FQDN for the dashboard (e.g. dashboard.example.com). Point its DNS at the ALB (output dashboard_alb_dns_name). Required when dashboard_enable_auth = true."
  type        = string
  default     = ""
}

variable "dashboard_acm_certificate_arn" {
  description = "ACM certificate ARN covering dashboard_auth_domain_name, in the same region as the stack. Required when dashboard_enable_auth = true (ALB HTTPS listeners cannot use self-signed or default certs)."
  type        = string
  default     = ""
}

variable "dashboard_alb_subnet_ids" {
  description = "Public subnets (>= 2 AZs, with an internet-gateway route) for the dashboard ALB. Required when dashboard_enable_auth = true."
  type        = list(string)
  default     = []
}

variable "dashboard_vpce_subnet_ids" {
  description = "Subnets for the App Runner VPC interface endpoint ENIs (private subnets are fine; must be routable from the ALB subnets). Required when dashboard_enable_auth = true."
  type        = list(string)
  default     = []
}

variable "dashboard_auth_allowed_cidrs" {
  description = "CIDRs allowed to reach the dashboard ALB on 80/443. Default open to the internet (Cognito still gates access); tighten to office/VPN ranges if the dashboard is internal-only."
  type        = list(string)
  default     = ["0.0.0.0/0"]
}

variable "enable_mwaa" {
  description = "Create an MWAA environment for Airflow DAG orchestration."
  type        = bool
  default     = false
}

variable "mwaa_subnet_ids" {
  description = "Private subnets for MWAA (>= 2 AZs). Defaults to private_subnet_ids when empty."
  type        = list(string)
  default     = []
}

variable "mwaa_environment_class" {
  description = "MWAA worker size (mw1.small for dev, mw1.medium+ for prod)."
  type        = string
  default     = "mw1.small"
}

variable "mwaa_max_workers" {
  description = "Maximum MWAA workers."
  type        = number
  default     = 2
}

variable "mwaa_webserver_access_mode" {
  description = "MWAA UI access: PUBLIC_ONLY or PRIVATE_ONLY."
  type        = string
  default     = "PUBLIC_ONLY"
}

variable "alert_email" {
  description = "Optional email endpoint for the platform alert SNS topic (consumer-lag alarms, future dbt-test alarms). Leave empty in dev; set in prod tfvars."
  type        = string
  default     = ""
}
