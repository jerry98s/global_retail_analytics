<#
.SYNOPSIS
  Publish clickstream and/or inventory test events to MSK from the EMR master (VPC).

.DESCRIPTION
  Submits a short EMR step that installs Python deps, syncs ingestion code from S3,
  and runs the producer simulators with MSK IAM auth (instance profile on EMR).

  Requires: platform stack applied, scripts/cloud/deploy_platform.ps1 sync (ingestion on artifacts bucket),
  EMR cluster in WAITING or RUNNING state.

.PARAMETER Stream
  clickstream | inventory | both

.EXAMPLE
  .\scripts\cloud\run_msk_producers.ps1 -Env dev -Stream both -DurationSeconds 120
#>
[CmdletBinding()]
param(
    [ValidateSet('dev', 'prod')]
    [string]$Env = 'dev',

    [ValidateSet('clickstream', 'inventory', 'both')]
    [string]$Stream = 'both',

    [int]$DurationSeconds = 120,
    [int]$ClickstreamEps = 500,
    [int]$InventoryEps = 50
)

$ErrorActionPreference = 'Stop'

$RepoRoot    = Resolve-Path (Join-Path $PSScriptRoot '..')
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

$out = Get-PlatformOutputs
$clusterId    = $out.emr_cluster_id.value
$artifacts    = $out.artifacts_bucket_name.value
$mskBootstrap = $out.kafka_bootstrap_brokers_sasl_iam.value

if (-not $clusterId -or -not $artifacts -or -not $mskBootstrap) {
    throw "Missing terraform outputs (emr_cluster_id, artifacts_bucket_name, kafka_bootstrap_brokers_sasl_iam)."
}

Write-Host "==> Syncing ingestion to s3://$artifacts/ingestion/" -ForegroundColor Cyan
aws s3 sync (Join-Path $RepoRoot 'ingestion') "s3://$artifacts/ingestion/" `
    --exclude "__pycache__/*" --exclude "*.pyc" --region $Region
if ($LASTEXITCODE -ne 0) { throw "ingestion sync failed" }

$producerCmds = @()
if ($Stream -in @('clickstream', 'both')) {
    $producerCmds += "python3 clickstream_producer.py --duration $DurationSeconds --eps $ClickstreamEps"
}
if ($Stream -in @('inventory', 'both')) {
    $producerCmds += "python3 inventory_producer.py --duration $DurationSeconds --eps $InventoryEps"
}
$runProducers = ($producerCmds -join ' & ') + ' & wait'

$inner = @"
set -euo pipefail
export KAFKA_BOOTSTRAP_SERVERS='$mskBootstrap'
export KAFKA_SECURITY_PROTOCOL='SASL_SSL'
export AWS_REGION='$Region'
pip3 install --user -q -r /tmp/ingestion/kafka/requirements-msk.txt
export PATH="`$HOME/.local/bin:`$PATH"
aws s3 sync s3://$artifacts/ingestion /tmp/ingestion --region $Region
cd /tmp/ingestion/kafka/producer_sim
$runProducers
"@ -replace "`r`n", "`n"

$stepName = "msk-producers-$Stream"
Write-Host "==> Submitting EMR step: $stepName ($DurationSeconds s)" -ForegroundColor Cyan

$stepJson = @{
    Name            = $stepName
    ActionOnFailure = 'CONTINUE'
    HadoopJarStep   = @{
        Jar  = 'command-runner.jar'
        Args = @('bash', '-lc', $inner)
    }
} | ConvertTo-Json -Depth 5 -Compress

aws emr add-steps --cluster-id $clusterId --steps $stepJson --region $Region | Out-Host
if ($LASTEXITCODE -ne 0) { throw "add-steps failed" }

Write-Host ""
Write-Host "Producers submitted on EMR master. Monitor:" -ForegroundColor Yellow
Write-Host "  aws emr list-steps --cluster-id $clusterId --step-states RUNNING COMPLETED FAILED"
Write-Host "After COMPLETED, verify bronze in Redshift or S3 under iceberg/bronze/."
