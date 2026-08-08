output "ecr_repository_url" {
  description = "ECR repo to build/push the dashboard image to (docker push <url>:<tag>)."
  value       = aws_ecr_repository.this.repository_url
}

output "service_url" {
  description = "App Runner URL of the dashboard. Unreachable when enable_auth = true (private service) — use dashboard_url instead."
  value       = var.create_service ? "https://${aws_apprunner_service.this[0].service_url}" : null
}

output "dashboard_url" {
  description = "URL to give users: the auth domain behind Cognito when enable_auth = true, else the public App Runner URL."
  value       = var.enable_auth ? "https://${var.auth_domain_name}" : (var.create_service ? "https://${aws_apprunner_service.this[0].service_url}" : null)
}

output "alb_dns_name" {
  description = "ALB DNS name to point auth_domain_name at (CNAME or Route53 alias). Null when enable_auth = false."
  value       = var.enable_auth ? aws_lb.dashboard[0].dns_name : null
}

output "cognito_user_pool_id" {
  description = "Cognito user pool ID; create dashboard users here (aws cognito-idp admin-create-user). Null when enable_auth = false."
  value       = var.enable_auth ? aws_cognito_user_pool.dashboard[0].id : null
}

output "service_arn" {
  description = "App Runner service ARN (null until create_service = true)."
  value       = var.create_service ? aws_apprunner_service.this[0].arn : null
}
