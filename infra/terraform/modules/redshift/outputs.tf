output "workgroup_name" {
  description = "Redshift Serverless workgroup name."
  value       = aws_redshiftserverless_workgroup.this.workgroup_name
}

output "namespace_name" {
  description = "Redshift Serverless namespace name."
  value       = aws_redshiftserverless_namespace.this.namespace_name
}

output "database_name" {
  description = "Default database (use as RS_DATABASE for dbt)."
  value       = var.database_name
}

output "endpoint" {
  description = "Redshift host for dbt/JDBC connections."
  value       = try(aws_redshiftserverless_workgroup.this.endpoint[0].address, null)
}

output "jdbc_port" {
  description = "Redshift port."
  value       = try(aws_redshiftserverless_workgroup.this.endpoint[0].port, 5439)
}

output "redshift_iam_role_arn" {
  description = "IAM role Redshift assumes for S3 + Glue (use in CREATE EXTERNAL SCHEMA)."
  value       = aws_iam_role.redshift_s3.arn
}

output "glue_bronze_database" {
  description = "Glue catalog database backing the Spectrum external schema."
  value       = aws_glue_catalog_database.bronze.name
}
