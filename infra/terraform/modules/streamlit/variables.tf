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

variable "create_service" {
  description = <<-EOT
    Create the App Runner service. Keep false on the first apply so the ECR repo
    exists to push an image into, then set true and re-apply once the image is
    pushed (App Runner requires the image to exist at create time).
  EOT
  type        = bool
  default     = false
}

variable "image_tag" {
  description = "ECR image tag the App Runner service runs."
  type        = string
  default     = "latest"
}

variable "cpu" {
  description = "App Runner vCPU (e.g. '0.25 vCPU', '0.5 vCPU', '1 vCPU')."
  type        = string
  default     = "0.25 vCPU"
}

variable "memory" {
  description = "App Runner memory (e.g. '0.5 GB', '1 GB', '2 GB')."
  type        = string
  default     = "0.5 GB"
}

variable "vpc_id" {
  description = "VPC for the App Runner VPC connector security group. Required when vpc_connector_subnet_ids is non-empty."
  type        = string
  default     = ""
}

variable "vpc_connector_subnet_ids" {
  description = <<-EOT
    Private subnets for an App Runner VPC connector so the dashboard can reach a
    VPC-only Redshift workgroup. Leave empty to use App Runner's default public
    egress (only works when the Redshift workgroup is publicly accessible).
    The Redshift security group's allowed_cidrs must include these subnets' CIDRs.
  EOT
  type        = list(string)
  default     = []
}

variable "redshift_host" {
  description = "Redshift Serverless endpoint host (RS_HOST)."
  type        = string
}

variable "redshift_port" {
  description = "Redshift port (RS_PORT)."
  type        = number
  default     = 5439
}

variable "redshift_database" {
  description = "Redshift database (RS_DATABASE)."
  type        = string
}

variable "redshift_user" {
  description = "Redshift user (RS_USER)."
  type        = string
}

variable "redshift_password" {
  description = "Redshift password (RS_PASSWORD); stored in Secrets Manager and injected as a runtime secret."
  type        = string
  sensitive   = true
}
