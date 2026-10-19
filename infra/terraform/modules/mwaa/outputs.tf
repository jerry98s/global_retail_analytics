output "environment_name" {
  description = "MWAA environment name."
  value       = aws_mwaa_environment.this.name
}

output "webserver_url" {
  description = "Airflow web UI URL."
  value       = aws_mwaa_environment.this.webserver_url
}

output "execution_role_arn" {
  description = "IAM role MWAA tasks assume."
  value       = aws_iam_role.execution.arn
}

output "dag_s3_prefix" {
  description = "S3 prefix for DAG sync (mwaa/dags/)."
  value       = "mwaa/dags"
}
