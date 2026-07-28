<#
.SYNOPSIS
  Single entry point for the cloud platform lifecycle.

.DESCRIPTION
  Wraps the four cloud scripts in scripts/cloud/ behind one -Task switch, so the
  cloud side has the same shape as scripts/local/run_local_stack.ps1:

    run_terraform.ps1      -> apply (bootstrap, then platform for -Env)
    bootstrap_redshift.ps1  -> generate Redshift DDL + Spectrum + metadata SQL
    deploy_platform.ps1     -> MWAA sync + Flink submit (+ verify + status)
    run_msk_producers.ps1   -> publish test events to MSK via EMR step

  Default -Env dev. -Task all runs the full sequence end-to-end
  (apply -> bootstrap -> deploy -> producers -> verify) and auto-approves
  terraform apply so it is non-interactive. Individual -Task values run just
  that stage.

  This wrapper only orchestrates the sibling scripts; it holds no logic of its
  own. Run a sibling directly when you need its full flag set (e.g.
  bootstrap_redshift -MetadataOnly, or run_terraform -Action destroy).

.PARAMETER Task
  apply      - terraform apply (bootstrap, then platform for -Env)
  bootstrap  - bootstrap_redshift -IncludeSilver -IncludeMetadata
  deploy     - deploy_platform (MWAA sync + Flink submit)
  producers  - run_msk_producers -Stream <Stream> -DurationSeconds <n>
  verify     - deploy_platform -Action verify (post-deploy smoke checks)
  status     - deploy_platform -Action status
  all        - apply -> bootstrap -> deploy -> producers -> verify (auto-approve)

.PARAMETER Env
  dev (default) | prod

.PARAMETER Stream
  both (default) | clickstream | inventory  (producers task only)

.PARAMETER DurationSeconds
  MSK producer duration in seconds (producers task only, default 120)

.PARAMETER AutoApprove
  Pass -AutoApprove to terraform apply (apply task only). -Task all always
  auto-approves terraform regardless of this flag.

.EXAMPLE
  .\scripts\cloud\run_cloud_stack.ps1 -Task all
  .\scripts\cloud\run_cloud_stack.ps1 -Task apply -Env prod -AutoApprove
  .\scripts\cloud\run_cloud_stack.ps1 -Task deploy
  .\scripts\cloud\run_cloud_stack.ps1 -Task producers -Stream clickstream -DurationSeconds 60
  .\scripts\cloud\run_cloud_stack.ps1 -Task verify
  .\scripts\cloud\run_cloud_stack.ps1 -Task status
#>

[CmdletBinding()]
param(
    [ValidateSet("apply", "bootstrap", "deploy", "producers", "verify", "status", "all")]
    [string]$Task = "all",

    [ValidateSet("dev", "prod")]
    [string]$Env = "dev",

    [ValidateSet("both", "clickstream", "inventory")]
    [string]$Stream = "both",

    [int]$DurationSeconds = 120,

    [switch]$AutoApprove
)

$ErrorActionPreference = "Stop"

$RepoRoot  = (Resolve-Path (Join-Path $PSScriptRoot "..\..")).Path
$Terraform = Join-Path $PSScriptRoot "run_terraform.ps1"
$Bootstrap = Join-Path $PSScriptRoot "bootstrap_redshift.ps1"
$Deploy    = Join-Path $PSScriptRoot "deploy_platform.ps1"
$Producers = Join-Path $PSScriptRoot "run_msk_producers.ps1"

function Write-Stage([string]$Label) {
    Write-Host ""
    Write-Host "==> $Label (env=$Env)" -ForegroundColor Cyan
}

function Assert-Exit([string]$Label) {
    if ($LASTEXITCODE -ne 0) { throw "$Label failed (exit $LASTEXITCODE)" }
}

function Invoke-Apply {
    param([switch]$Auto)
    $extra = @{}
    if ($Auto) { $extra["AutoApprove"] = $true }

    Write-Stage "Terraform apply: bootstrap"
    & $Terraform -Stack bootstrap -Action apply @extra
    Assert-Exit "terraform bootstrap apply"

    Write-Stage "Terraform apply: platform"
    & $Terraform -Stack platform -Env $Env -Action apply @extra
    Assert-Exit "terraform platform apply"
}

function Invoke-Bootstrap {
    Write-Stage "Redshift bootstrap DDL"
    & $Bootstrap -Env $Env -IncludeSilver -IncludeMetadata
    Assert-Exit "bootstrap_redshift"
}

function Invoke-Deploy {
    Write-Stage "Deploy platform (MWAA sync + Flink)"
    & $Deploy -Env $Env
    Assert-Exit "deploy_platform"
}

function Invoke-Producers {
    Write-Stage "MSK producers ($Stream for ${DurationSeconds}s)"
    & $Producers -Env $Env -Stream $Stream -DurationSeconds $DurationSeconds
    Assert-Exit "run_msk_producers"
}

function Invoke-Verify {
    Write-Stage "Post-deploy smoke checks"
    & $Deploy -Env $Env -Action verify
    Assert-Exit "deploy_platform verify"
}

function Invoke-Status {
    Write-Stage "Platform status"
    & $Deploy -Env $Env -Action status
    Assert-Exit "deploy_platform status"
}

Push-Location $RepoRoot
try {
    switch ($Task) {
        "apply"     { Invoke-Apply -Auto:$AutoApprove }
        "bootstrap" { Invoke-Bootstrap }
        "deploy"    { Invoke-Deploy }
        "producers" { Invoke-Producers }
        "verify"    { Invoke-Verify }
        "status"    { Invoke-Status }
        "all" {
            Invoke-Apply -Auto
            Invoke-Bootstrap
            Invoke-Deploy
            Invoke-Producers
            Invoke-Verify
        }
    }
    Write-Host ""
    Write-Host "==> Done ($Task / $Env)." -ForegroundColor Green
}
finally {
    Pop-Location
}
