output "state_bucket_name" {
  description = "S3 bucket holding Terraform remote state."
  value       = aws_s3_bucket.state.bucket
}

output "lock_table_name" {
  description = "DynamoDB table used for Terraform state locking."
  value       = aws_dynamodb_table.locks.name
}

output "budget_name" {
  description = "Monthly cost budget name."
  value       = aws_budgets_budget.monthly.name
}
