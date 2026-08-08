<#
.SYNOPSIS
  Post-apply runtime: sync MWAA assets, submit Flink jobs, print Airflow vars.

.DESCRIPTION
  Companion to scripts/cloud/run_terraform.ps1 (infra). Use run_terraform.ps1 for terraform output/plan/apply.

.PARAMETER Action
  all         - MWAA sync + Flink submit (default, run after code changes)
  flink       - Flink submit only (sync streaming code + EMR steps)
  mwaa-sync   - DAGs, dbt, GE, ingestion -> S3 only
  airflow-vars - print MWAA Variable names/values from terraform output
  status      - EMR cluster state + S3 bronze summary
  verify      - post-deploy smoke checks: EMR state, S3 bronze prefixes,
                optional Redshift row counts, optional dashboard HTTP

.PARAMETER Job
  Flink jobs: clickstream | inventory-bronze | inventory-snapshot | inventory | all

.EXAMPLE
  .\scripts\cloud\deploy_platform.ps1 -Env dev
  .\scripts\cloud\deploy_platform.ps1 -Env dev -Action flink -Job inventory-bronze
  .\scripts\cloud\deploy_platform.ps1 -Env dev -Action airflow-vars
#>
[CmdletBinding()]
param(
    [ValidateSet('dev', 'prod')]
    [string]$Env = 'dev',

    [ValidateSet('all', 'flink', 'mwaa-sync', 'airflow-vars', 'status', 'verify')]
    [string]$Action = 'all',

    [ValidateSet('clickstream', 'inventory-bronze', 'inventory-snapshot', 'inventory', 'all')]
    [string]$Job = 'all'
)

$ErrorActionPreference = 'Stop'

$RepoRoot    = (Resolve-Path (Join-Path $PSScriptRoot '..\..')).Path
$PlatformDir = Join-Path $RepoRoot 'infra/terraform'
$BackendFile = Join-Path $PlatformDir "envs/$Env.backend.hcl"

if (-not (Test-Path $BackendFile)) {
    throw "No backend file for env '$Env' at $BackendFile."
}

$Region = (Select-String -Path $BackendFile -Pattern '^\s*region\s*=\s*"([^"]+)"' | ForEach-Object { $_.Matches[0].Groups[1].Value } | Select-Object -First 1)
if (-not $Region) { $Region = 'ap-southeast-1' }

function Get-PlatformOutputs {
    Push-Location $PlatformDir
    try {
        & terraform init -reconfigure -backend-config="$BackendFile" *> $null
        if ($LASTEXITCODE -ne 0) { throw "terraform init failed for env '$Env'" }
        return (terraform output -json | ConvertFrom-Json)
    }
    finally {
        Pop-Location
    }
}

function Sync-Artifacts([string]$Bucket) {
    Write-Host "==> Syncing ingestion + batch scripts" -ForegroundColor Cyan
    aws s3 sync (Join-Path $RepoRoot 'ingestion') "s3://$Bucket/ingestion/" --exclude "__pycache__/*" --exclude "*.pyc"
    if ($LASTEXITCODE -ne 0) { throw "ingestion sync failed" }
}

function Sync-Mwaa([string]$Bucket) {
    Write-Host "==> Syncing MWAA assets to s3://$Bucket/mwaa/" -ForegroundColor Cyan
    # Keep plugins copy of metadata_observer in sync with scripts/common.
    Copy-Item -Force `
        (Join-Path $RepoRoot 'scripts/common/metadata_observer.py') `
        (Join-Path $RepoRoot 'orchestration/airflow/plugins/metadata_observer.py')
    aws s3 sync (Join-Path $RepoRoot 'orchestration/airflow/dags') "s3://$Bucket/mwaa/dags/" --exclude "__pycache__/*"
    aws s3 sync (Join-Path $RepoRoot 'orchestration/airflow/plugins') "s3://$Bucket/mwaa/plugins/" --exclude "__pycache__/*"
    aws s3 sync (Join-Path $RepoRoot 'scripts/common') "s3://$Bucket/mwaa/scripts/common/" --exclude "__pycache__/*"
    aws s3 sync (Join-Path $RepoRoot 'metadata/catalog') "s3://$Bucket/mwaa/metadata/catalog/" --exclude "__pycache__/*"
    aws s3 sync (Join-Path $RepoRoot 'transformation/dbt_project') "s3://$Bucket/mwaa/dbt_project/" `
        --exclude "target/*" --exclude "dbt_packages/*" --exclude "logs/*" --exclude "*.duckdb"
    aws s3 sync (Join-Path $RepoRoot 'quality/great_expectations') "s3://$Bucket/mwaa/quality/great_expectations/"
    aws s3 sync (Join-Path $RepoRoot 'ingestion') "s3://$Bucket/ingestion/" --exclude "__pycache__/*"
    if ($LASTEXITCODE -ne 0) { throw "MWAA sync failed" }
    Write-Host "DAGs synced. MWAA picks up changes within ~5 minutes."
}

function Submit-FlinkJobs {
    param(
        [object]$Out,
        [string]$JobSelection
    )

    $clusterId         = $Out.emr_cluster_id.value
    $artifactsBucket   = $Out.artifacts_bucket_name.value
    $checkpointsBucket = $Out.checkpoints_bucket_name.value
    $mskBootstrap      = $Out.kafka_bootstrap_brokers_sasl_iam.value

    if (-not $clusterId -or -not $artifactsBucket -or -not $checkpointsBucket -or -not $mskBootstrap) {
        throw "Missing terraform outputs for Flink deploy (emr_cluster_id, artifacts/checkpoints buckets, MSK brokers)."
    }

    $jobsDir         = Join-Path $RepoRoot 'streaming/flink_jobs'
    $cfgDir          = Join-Path $RepoRoot 'streaming/config'
    $bootstrapScript = Join-Path $RepoRoot 'infra/emr-bootstrap/install_flink_connectors.sh'

    Write-Host "==> Syncing PyFlink job code" -ForegroundColor Cyan
    aws s3 sync $jobsDir "s3://$artifactsBucket/streaming/flink_jobs/" --exclude "__pycache__/*" --exclude "*.pyc"
    if ($LASTEXITCODE -ne 0) { throw "aws s3 sync (jobs) failed" }

    Write-Host "==> Syncing Flink config" -ForegroundColor Cyan
    aws s3 sync $cfgDir "s3://$artifactsBucket/streaming/config/"
    if ($LASTEXITCODE -ne 0) { throw "aws s3 sync (config) failed" }

    Write-Host "==> Uploading bootstrap script" -ForegroundColor Cyan
    aws s3 cp $bootstrapScript "s3://$artifactsBucket/bootstrap/install_flink_connectors.sh"
    if ($LASTEXITCODE -ne 0) { throw "aws s3 cp (bootstrap) failed" }

    Sync-Artifacts $artifactsBucket

    function Submit-FlinkStep([string]$JobName, [string]$EntryFile) {
        $stepName = "flink-$JobName"
        Write-Host "==> Submitting step $stepName" -ForegroundColor Cyan

        $bronzePrefix = $artifactsBucket -replace '-artifacts$', '-bronze'
        $silverPrefix = $artifactsBucket -replace '-artifacts$', '-silver'
        $stepArgs = @(
            'bash', '-lc',
            "export KAFKA_BOOTSTRAP_SERVERS='$mskBootstrap' " +
                "KAFKA_SECURITY_PROTOCOL='SASL_SSL' " +
                "KAFKA_SASL_MECHANISM='AWS_MSK_IAM' " +
                "ICEBERG_BRONZE_WAREHOUSE='s3://$bronzePrefix/iceberg' " +
                "ICEBERG_SILVER_WAREHOUSE='s3://$silverPrefix/iceberg' " +
                # Match Kafka partition counts (ingestion/kafka/topics.py):
                # inventory.events.v1=12, clickstream.events.v1=24.
                "INVENTORY_PARALLELISM='12' " +
                "CLICKSTREAM_PARALLELISM='24' " +
                "&& aws s3 sync s3://$artifactsBucket/streaming /opt/flink-config-src " +
                "&& flink run-application -t yarn-application " +
                "-Dyarn.application.name=$stepName " +
                "-Dstate.checkpoints.dir=s3://$checkpointsBucket/flink/checkpoints/$JobName " +
                "-Dstate.savepoints.dir=s3://$checkpointsBucket/flink/savepoints/$JobName " +
                "-pyfs /opt/flink-config-src/flink_jobs " +
                "-py /opt/flink-config-src/flink_jobs/$EntryFile"
        )

        $stepJson = @{
            Name            = $stepName
            ActionOnFailure = 'CONTINUE'
            HadoopJarStep   = @{
                Jar  = 'command-runner.jar'
                Args = $stepArgs
            }
        } | ConvertTo-Json -Depth 5 -Compress

        aws emr add-steps --cluster-id $clusterId --steps $stepJson | Out-Host
        if ($LASTEXITCODE -ne 0) { throw "add-steps failed for $stepName" }
    }

    if ($JobSelection -in @('clickstream', 'all')) {
        Submit-FlinkStep -JobName 'clickstream-bronze' -EntryFile 'clickstream_bronze_job.py'
    }
    if ($JobSelection -in @('inventory-bronze', 'inventory', 'all')) {
        Submit-FlinkStep -JobName 'inventory-bronze' -EntryFile 'inventory_bronze_job.py'
    }
    if ($JobSelection -in @('inventory-snapshot', 'inventory', 'all')) {
        Submit-FlinkStep -JobName 'inventory-hourly' -EntryFile 'inventory_silver_job.py'
    }

    Write-Host ""
    Write-Host "Flink steps submitted. Monitor with:" -ForegroundColor DarkGray
    Write-Host "  aws emr list-steps --cluster-id $clusterId --step-states RUNNING PENDING"
}

function Invoke-PlatformVerify {
    param([object]$Out)

    Write-Host "==> Platform smoke checks ($Env)" -ForegroundColor Cyan

    # 1. EMR cluster state
    $clusterId = $Out.emr_cluster_id.value
    if (-not $clusterId) {
        Write-Host "  EMR: skipped (no emr_cluster_id output)" -ForegroundColor Yellow
    } else {
        $emrState = aws emr describe-cluster --cluster-id $clusterId --region $Region `
            --query 'Cluster.Status.State' --output text 2>$null
        Write-Host "  EMR cluster $clusterId : $emrState"
        if ($emrState -notin @('RUNNING', 'WAITING', 'BOOTSTRAPPING')) {
            throw "EMR not healthy (state=$emrState)"
        }
    }

    # 2. S3 bronze prefixes
    $bronze = $Out.bronze_bucket_name.value
    if (-not $bronze) {
        Write-Host "  S3 bronze: skipped (no bronze_bucket_name output)" -ForegroundColor Yellow
    } else {
        foreach ($prefix in @('iceberg/bronze/clickstream_events/', 'iceberg/bronze/inventory_events/', 'iceberg/bronze/pos_transactions/')) {
            $count = (aws s3 ls "s3://$bronze/$prefix" --recursive --summarize --region $Region 2>$null |
                Select-String 'Total Objects:' | ForEach-Object { ($_ -split ':')[-1].Trim() })
            $objCount = if ($count) { [int]$count } else { 0 }
            $status = if ($objCount -gt 0) { 'OK' } else { 'EMPTY' }
            Write-Host "  S3 s3://$bronze/$prefix : $status ($objCount objects)"
        }
    }

    # 3. Redshift row counts (optional - requires redshift-data:ExecuteStatement)
    $wg = $Out.redshift_workgroup_name.value
    $db = $Out.redshift_database_name.value
    if (-not $wg -or -not $db) {
        Write-Host "  Redshift: skipped (no redshift outputs - run bootstrap_redshift.ps1 first)" -ForegroundColor Yellow
    } else {
        $sql = @"
SELECT 'dim_date' AS tbl, COUNT(*)::varchar AS rows FROM finance.dim_date
UNION ALL SELECT 'fact_sales', COUNT(*)::varchar FROM finance.fact_sales WHERE NOT is_voided
UNION ALL SELECT 'identity_graph', COUNT(*)::varchar FROM marketing.identity_graph
"@
        $stmtId = aws redshift-data execute-statement --workgroup-name $wg --database $db --sql $sql --region $Region --query 'Id' --output text 2>$null
        if (-not $stmtId) {
            Write-Host "  Redshift: execute-statement failed (check IAM redshift-data:ExecuteStatement)" -ForegroundColor Yellow
        } else {
            Start-Sleep -Seconds 3
            $rows = aws redshift-data get-statement-result --id $stmtId --region $Region --output json 2>$null | ConvertFrom-Json
            if ($rows.Records) {
                foreach ($rec in $rows.Records) { Write-Host "  Redshift $($rec[0].stringValue): $($rec[1].stringValue) rows" }
            } else {
                Write-Host "  Redshift: query returned no rows (bootstrap + dbt not run?)" -ForegroundColor Yellow
            }
        }
    }

    # 4. Dashboard HTTP (optional - only if enable_dashboard=true)
    $url = $Out.dashboard_url.value
    if (-not $url) {
        Write-Host "  Dashboard: skipped (enable_dashboard=false or service not created)" -ForegroundColor Yellow
    } else {
        try {
            $resp = Invoke-WebRequest -Uri $url -Method Head -TimeoutSec 15 -UseBasicParsing
            Write-Host "  Dashboard $url : HTTP $($resp.StatusCode)"
        } catch {
            throw "Dashboard unreachable: $url ($($_.Exception.Message))"
        }
    }
}

function Show-AirflowVariables {
    param([object]$Out)

    Write-Host "==> Airflow Variables for env '$Env' (from terraform output)" -ForegroundColor Cyan
    Write-Host ""
    Write-Host "Set in MWAA UI (Admin -> Variables):" -ForegroundColor Yellow

    $vars = $Out.airflow_variables.value
    foreach ($prop in $vars.PSObject.Properties | Sort-Object Name) {
        Write-Host ("  {0,-28} = {1}" -f $prop.Name, $prop.Value)
    }

    Write-Host ""
    Write-Host "Manual (from tfvars, not in outputs):" -ForegroundColor Yellow
    Write-Host "  redshift_user                = rs_admin (or your admin user)"
    Write-Host ""
    Write-Host "Do NOT set a redshift_password Variable. Tasks read the password" -ForegroundColor Yellow
    Write-Host "from Secrets Manager via redshift_secret_arn (listed above), so it"
    Write-Host "stays out of the Airflow metadata DB, rendered templates, and logs."
    Write-Host ""
    Write-Host "Optional (row-count reconciliation, P2.5 - defaults work if absent):" -ForegroundColor Yellow
    Write-Host "  gold_row_counts_baseline     = {}  (auto-seeds on first clean run)"
    Write-Host "  row_count_delta_threshold    = 0.20  (warn on >20% day-over-day delta)"
    Write-Host ""
    Write-Host "MWAA web UI:" -ForegroundColor Cyan
    if ($Out.mwaa_webserver_url.value) {
        Write-Host "  $($Out.mwaa_webserver_url.value)"
    } else {
        Write-Host "  (enable_mwaa = false - no MWAA URL)"
    }
}

$out = Get-PlatformOutputs
$artifacts = $out.artifacts_bucket_name.value

switch ($Action) {
    'status' {
        $clusterId = $out.emr_cluster_id.value
        $bronze    = $out.bronze_bucket_name.value
        Write-Host "==> Platform status ($Env)" -ForegroundColor Cyan
        aws emr describe-cluster --cluster-id $clusterId --region $Region `
            --query 'Cluster.{State:Status.State,Name:Name}' --output table
        aws s3 ls "s3://$bronze/iceberg/" --recursive --summarize --region $Region 2>$null | Select-Object -Last 3
        Write-Host "Redshift: $($out.redshift_endpoint.value)"
        if ($out.mwaa_webserver_url.value) {
            Write-Host "Airflow: $($out.mwaa_webserver_url.value)"
        }
    }

    'airflow-vars' {
        Show-AirflowVariables $out
    }

    'mwaa-sync' {
        Sync-Mwaa $artifacts
    }

    'flink' {
        Submit-FlinkJobs -Out $out -JobSelection $Job
    }

    'verify' {
        Invoke-PlatformVerify -Out $out
    }

    'all' {
        Sync-Mwaa $artifacts
        Submit-FlinkJobs -Out $out -JobSelection $Job
        Write-Host ""
        Write-Host "Next steps:" -ForegroundColor Yellow
        Write-Host "  1. .\scripts\cloud\bootstrap_redshift.ps1 -Env $Env  (if not done)"
        Write-Host "  2. .\scripts\cloud\run_msk_producers.ps1 -Env $Env -Stream both"
        Write-Host "  3. .\scripts\cloud\deploy_platform.ps1 -Env $Env -Action verify  (post-deploy smoke checks)"
        Write-Host "  4. .\scripts\cloud\deploy_platform.ps1 -Env $Env -Action airflow-vars  (set vars in MWAA UI)"
    }
}

Write-Host "==> Done ($Action / $Env)." -ForegroundColor Green
