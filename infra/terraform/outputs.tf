output "bronze_bucket_name" {
  description = "Bronze layer S3 bucket name."
  value       = module.s3.bronze_bucket_name
}

output "silver_bucket_name" {
  description = "Silver layer S3 bucket name."
  value       = module.s3.silver_bucket_name
}

output "gold_bucket_name" {
  description = "Gold layer S3 bucket name."
  value       = module.s3.gold_bucket_name
}

output "artifacts_bucket_name" {
  description = "Artifacts S3 bucket (Flink jobs, JARs, bootstrap scripts)."
  value       = module.s3.artifacts_bucket_name
}

output "checkpoints_bucket_name" {
  description = "Flink checkpoints/savepoints S3 bucket."
  value       = module.s3.checkpoints_bucket_name
}

output "msk_cluster_arn" {
  description = "MSK Serverless cluster ARN."
  value       = module.kafka.cluster_arn
}

output "kafka_bootstrap_brokers_sasl_iam" {
  description = "MSK bootstrap brokers endpoint for SASL IAM."
  value       = module.kafka.bootstrap_brokers_sasl_iam
}

output "kafka_topic_names" {
  description = "Kafka topics managed by Terraform."
  value       = module.kafka.topic_names
}

output "emr_cluster_id" {
  description = "EMR cluster ID for Flink jobs."
  value       = module.emr.cluster_id
}

output "emr_cluster_name" {
  description = "EMR cluster name."
  value       = module.emr.cluster_name
}

output "redshift_workgroup_name" {
  description = "Redshift Serverless workgroup name."
  value       = module.redshift.workgroup_name
}

output "redshift_endpoint" {
  description = "Redshift host for dbt/JDBC connections (set RS_HOST)."
  value       = module.redshift.endpoint
}

output "redshift_database_name" {
  description = "Default Redshift database (set RS_DATABASE)."
  value       = module.redshift.database_name
}

output "redshift_metadata_database_name" {
  description = "Operational metadata database name (Airflow redshift_metadata_database). Not the namespace db_name."
  value       = var.redshift_metadata_database_name
}

output "redshift_secret_arn" {
  description = "Secrets Manager ARN holding the Redshift admin password (Airflow redshift_secret_arn). Tasks fetch the value at runtime; the password itself is never an Airflow Variable."
  value       = aws_secretsmanager_secret.redshift_admin.arn
}

output "redshift_iam_role_arn" {
  description = "IAM role Redshift assumes for S3 + Glue (use in CREATE EXTERNAL SCHEMA)."
  value       = module.redshift.redshift_iam_role_arn
}

output "redshift_glue_bronze_database" {
  description = "Glue catalog database backing the Spectrum external schema."
  value       = module.redshift.glue_bronze_database
}

output "dashboard_ecr_repository_url" {
  description = "ECR repo to push the dashboard image to (null unless enable_dashboard = true)."
  value       = var.enable_dashboard ? module.dashboard[0].ecr_repository_url : null
}

output "dashboard_url" {
  description = "Dashboard URL. The Cognito-gated auth domain when dashboard_enable_auth = true, else the public App Runner URL (null until dashboard_create_service = true)."
  value       = var.enable_dashboard ? module.dashboard[0].dashboard_url : null
}

output "dashboard_alb_dns_name" {
  description = "Point dashboard_auth_domain_name's DNS here (CNAME or Route53 alias). Null unless dashboard_enable_auth = true."
  value       = var.enable_dashboard && var.dashboard_enable_auth ? module.dashboard[0].alb_dns_name : null
}

output "dashboard_cognito_user_pool_id" {
  description = "Cognito pool for dashboard logins: aws cognito-idp admin-create-user --user-pool-id <this> --username <email>. Null unless dashboard_enable_auth = true."
  value       = var.enable_dashboard && var.dashboard_enable_auth ? module.dashboard[0].cognito_user_pool_id : null
}

output "mwaa_webserver_url" {
  description = "Airflow web UI URL (null unless enable_mwaa = true)."
  value       = var.enable_mwaa ? module.mwaa[0].webserver_url : null
}

output "mwaa_environment_name" {
  description = "MWAA environment name (null unless enable_mwaa = true)."
  value       = var.enable_mwaa ? module.mwaa[0].environment_name : null
}

output "pos_bronze_s3_path" {
  description = "S3 prefix for daily POS Parquet bronze (Airflow pos_bronze_s3_path)."
  value       = "s3://${module.s3.bronze_bucket_name}/iceberg/bronze/pos_transactions/"
}

output "bronze_iceberg_warehouse" {
  description = "Iceberg bronze warehouse URI (Airflow bronze_iceberg_warehouse)."
  value       = "s3://${module.s3.bronze_bucket_name}/iceberg"
}

output "silver_iceberg_warehouse" {
  description = "Iceberg silver warehouse URI (Airflow silver_iceberg_warehouse)."
  value       = "s3://${module.s3.silver_bucket_name}/iceberg"
}

output "alert_topic_arn" {
  description = "SNS topic ARN for platform operational alerts (Kafka consumer lag, future alarms)."
  value       = aws_sns_topic.alerts.arn
}

output "kafka_consumer_lag_alarm_names" {
  description = "CloudWatch alarm names for MSK consumer-lag monitoring."
  value       = module.kafka.consumer_lag_alarm_names
}

output "airflow_variables" {
  description = <<-EOT
    Airflow Variable name → value map (set in MWAA UI after apply).
    Add redshift_user manually from tfvars. Do NOT create a redshift_password
    Variable — tasks resolve the password from redshift_secret_arn at runtime.
  EOT
  value = {
    emr_cluster_id             = module.emr.cluster_id
    artifacts_bucket           = module.s3.artifacts_bucket_name
    checkpoints_bucket         = module.s3.checkpoints_bucket_name
    bronze_iceberg_warehouse   = "s3://${module.s3.bronze_bucket_name}/iceberg"
    silver_iceberg_warehouse   = "s3://${module.s3.silver_bucket_name}/iceberg"
    msk_bootstrap_brokers      = module.kafka.bootstrap_brokers_sasl_iam
    redshift_workgroup_name    = module.redshift.workgroup_name
    redshift_host              = module.redshift.endpoint
    redshift_database          = module.redshift.database_name
    redshift_metadata_database = var.redshift_metadata_database_name
    redshift_secret_arn        = aws_secretsmanager_secret.redshift_admin.arn
    pos_bronze_s3_path         = "s3://${module.s3.bronze_bucket_name}/iceberg/bronze/pos_transactions/"
  }
}
