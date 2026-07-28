<#
.SYNOPSIS
  Single entry point for every Terraform operation in this repo.

.DESCRIPTION
  Picks the right backend HCL and per-env tfvars, then runs the requested
  action. This is the only sanctioned way to apply infra.

  Stacks:
    bootstrap  -> infra/terraform/bootstrap   (state bucket, locks, budget)
    platform   -> infra/terraform/            (MSK + EMR + Redshift + S3 + dashboard)

  Cloud dev and prod use the same platform stack code; only envs/<env>.tfvars
  differ. Local development uses Docker Compose on the local-testing-version
  branch — not this wrapper.

.EXAMPLE
  .\scripts\cloud\run_terraform.ps1 -Stack bootstrap              -Action apply
  .\scripts\cloud\run_terraform.ps1 -Stack platform -Env dev      -Action plan
  .\scripts\cloud\run_terraform.ps1 -Stack platform -Env dev      -Action apply
  .\scripts\cloud\run_terraform.ps1 -Stack platform -Env prod     -Action apply
  .\scripts\cloud\run_terraform.ps1 -Stack platform -Env dev      -Action output
#>

[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)]
    [ValidateSet("bootstrap", "platform")]
    [string]$Stack,

    [Parameter(Mandatory = $false)]
    [ValidateSet("dev", "stage", "prod")]
    [string]$Env,

    [Parameter(Mandatory = $true)]
    [ValidateSet("init", "fmt", "validate", "plan", "apply", "destroy", "output", "show")]
    [string]$Action,

    [Parameter(Mandatory = $false)]
    [switch]$AutoApprove
)

$ErrorActionPreference = "Stop"

$repoRoot = Split-Path -Parent $PSScriptRoot
$stackDir = switch ($Stack) {
    "bootstrap" { Join-Path $repoRoot "infra/terraform/bootstrap" }
    "platform"  { Join-Path $repoRoot "infra/terraform" }
}

if (-not (Test-Path $stackDir)) {
    throw "Stack directory not found: $stackDir"
}

$requiresEnv = $Stack -ne "bootstrap"
if ($requiresEnv -and -not $Env) {
    throw "-Env is required for stack '$Stack' (use dev | stage | prod)."
}

$backendFile = $null
$varFile     = $null
if ($requiresEnv) {
    $backendFile = Join-Path $stackDir "envs/${Env}.backend.hcl"
    $varFile     = Join-Path $stackDir "envs/${Env}.tfvars"
    $tag = "$Stack/$Env"
    if (-not (Test-Path $backendFile)) {
        throw ("Backend file missing for {0} -> {1}`nCreate envs/{2}.backend.hcl (see envs/README.md)." -f $tag, $backendFile, $Env)
    }
    if (-not (Test-Path $varFile) -and $Action -in @("plan", "apply", "destroy")) {
        $example = "$varFile.example"
        $hint = if (Test-Path $example) { " (copy from $example and edit)" } else { "" }
        throw ("tfvars file missing for {0} -> {1}{2}" -f $tag, $varFile, $hint)
    }
}

function Invoke-Tf {
    param([string[]]$TfArgs)
    Write-Host ">> terraform $($TfArgs -join ' ')" -ForegroundColor DarkGray
    & terraform @TfArgs
    if ($LASTEXITCODE -ne 0) {
        throw "terraform $($TfArgs[0]) failed (exit $LASTEXITCODE)"
    }
}

function Initialize-Backend {
    if ($Stack -eq "bootstrap") {
        Invoke-Tf -TfArgs @("init", "-upgrade")
    } else {
        Invoke-Tf -TfArgs @("init", "-reconfigure", "-backend-config=$backendFile")
    }
}

Push-Location $stackDir
try {
    Write-Host "==> Stack: $Stack   Env: $(if ($Env) { $Env } else { 'n/a' })   Action: $Action" -ForegroundColor Cyan

    switch ($Action) {
        "init"     { Initialize-Backend }
        "fmt"      { Invoke-Tf -TfArgs @("fmt", "-recursive") }
        "validate" { Initialize-Backend; Invoke-Tf -TfArgs @("validate") }

        "plan" {
            Initialize-Backend
            $tfArgs = @("plan")
            if ($varFile) { $tfArgs += "-var-file=$varFile" }
            Invoke-Tf -TfArgs $tfArgs
        }

        "apply" {
            Initialize-Backend
            $tfArgs = @("apply")
            if ($varFile)     { $tfArgs += "-var-file=$varFile" }
            if ($AutoApprove) { $tfArgs += "-auto-approve" }
            Invoke-Tf -TfArgs $tfArgs
        }

        "destroy" {
            Initialize-Backend
            $tfArgs = @("destroy")
            if ($varFile)     { $tfArgs += "-var-file=$varFile" }
            if ($AutoApprove) { $tfArgs += "-auto-approve" }
            Invoke-Tf -TfArgs $tfArgs
        }

        "output" { Initialize-Backend; Invoke-Tf -TfArgs @("output") }
        "show"   { Initialize-Backend; Invoke-Tf -TfArgs @("show") }
    }

    Write-Host "==> Done." -ForegroundColor Green
}
finally {
    Pop-Location
}
