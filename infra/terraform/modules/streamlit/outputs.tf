output "ecr_repository_url" {
  description = "ECR repo to build/push the dashboard image to (docker push <url>:<tag>)."
  value       = aws_ecr_repository.this.repository_url
}

output "service_url" {
  description = "Public HTTPS URL of the App Runner dashboard (null until create_service = true)."
  value       = var.create_service ? "https://${aws_apprunner_service.this[0].service_url}" : null
}

output "service_arn" {
  description = "App Runner service ARN (null until create_service = true)."
  value       = var.create_service ? aws_apprunner_service.this[0].arn : null
}
