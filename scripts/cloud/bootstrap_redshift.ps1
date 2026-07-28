<#
.SYNOPSIS
  Generate Redshift bootstrap SQL scripts for dev/prod (DDL + seeds + Spectrum)
  and/or the separate metadata database.

.DESCRIPTION
  Reads terraform outputs, substitutes placeholders in Spectrum SQL files, and
  concatenates run-order scripts into files for Redshift Query Editor v2.

  Does NOT execute SQL (no warehouse credentials in CI).

  Metadata mode writes TWO labelled scripts because Query Editor must switch
  databases between CREATE DATABASE and meta schema DDL:
    target/redshift_metadata_create_<env>.sql   — run on analytics DB (dev|prod)
    target/redshift_metadata_schema_<env>.sql   — run on database `metadata`

.EXAMPLE
  .\scripts\cloud\bootstrap_redshift.ps1 -Env dev
  .\scripts\cloud\bootstrap_redshift.ps1 -Env dev -IncludeSilver
  .\scripts\cloud\bootstrap_redshift.ps1 -Env dev -MetadataOnly
  .\scripts\cloud\bootstrap_redshift.ps1 -Env dev -IncludeMetadata
#>
[CmdletBinding()]
param(
    [ValidateSet('dev', 'prod')]
    [string]$Env = 'dev',

    [switch]$IncludeSilver,

    [switch]$MetadataOnly,

    [switch]$IncludeMetadata
)

$ErrorActionPreference = 'Stop'

# scripts/cloud -> repo root
$RepoRoot = (Resolve-Path (Join-Path $PSScriptRoot '..\..')).Path
$RedshiftRoot = Join-Path $RepoRoot 'transformation/redshift'
$PlatformDir = Join-Path $RepoRoot 'infra/terraform'
$BackendFile = Join-Path $PlatformDir "envs/$Env.backend.hcl"
$OutDir = Join-Path $RepoRoot 'target'
$OutFile = Join-Path $OutDir "redshift_bootstrap_$Env.sql"
$MetaCreateFile = Join-Path $OutDir "redshift_metadata_create_$Env.sql"
$MetaSchemaFile = Join-Path $OutDir "redshift_metadata_schema_$Env.sql"

if (-not (Test-Path $BackendFile)) {
    throw "No backend file for env '$Env' at $BackendFile"
}

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

function Read-TextFile([string]$Path) {
    if (-not (Test-Path $Path)) { throw "Missing file: $Path" }
    return [IO.File]::ReadAllText($Path)
}

function Write-MetadataScripts {
    param([object]$Out)

    $metaDb = 'metadata'
    if ($Out.PSObject.Properties.Name -contains 'redshift_metadata_database_name') {
        $metaDb = [string]$Out.redshift_metadata_database_name.value
    }
    $analyticsDb = [string]$Out.redshift_database_name.value

    New-Item -ItemType Directory -Force -Path $OutDir | Out-Null

    $createBody = Read-TextFile (Join-Path $RedshiftRoot 'metadata/00_create_database.sql')
    $createContent = @"
-- =============================================================================
-- METADATA DATABASE CREATE — env: $Env
-- Generated: $(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')
-- CONNECT Query Editor to analytics database: $analyticsDb
-- Then run this script (creates database `$metaDb`).
-- =============================================================================

$createBody
"@
    [IO.File]::WriteAllText($MetaCreateFile, $createContent)

    $schemaBody = Read-TextFile (Join-Path $RedshiftRoot 'metadata/01_meta_schema.sql')
    $schemaContent = @"
-- =============================================================================
-- METADATA SCHEMA DDL — env: $Env
-- Generated: $(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')
-- CONNECT Query Editor to database: $metaDb  (switch away from $analyticsDb)
-- Then run this script.
-- =============================================================================

$schemaBody
"@
    [IO.File]::WriteAllText($MetaSchemaFile, $schemaContent)

    Write-Host "==> Wrote metadata bootstrap SQL (two scripts):" -ForegroundColor Green
    Write-Host "    1. CREATE DB  (connect to $analyticsDb): $MetaCreateFile"
    Write-Host "    2. SCHEMA DDL (connect to $metaDb):      $MetaSchemaFile"
}

$out = Get-PlatformOutputs

if ($MetadataOnly) {
    Write-MetadataScripts -Out $out
    Write-Host ""
    Write-Host "Next steps:" -ForegroundColor Yellow
    Write-Host "  1. Query Editor → database $($out.redshift_database_name.value) → run create script"
    Write-Host "  2. Query Editor → database metadata → run schema script"
    Write-Host "  3. Set Airflow Variable redshift_metadata_database = metadata"
    return
}

$bronzeBucket = $out.bronze_bucket_name.value
$silverBucket = $out.silver_bucket_name.value
$glueBronze = $out.redshift_glue_bronze_database.value
$iamRole = $out.redshift_iam_role_arn.value

if (-not $bronzeBucket -or -not $glueBronze -or -not $iamRole) {
    throw "Missing terraform outputs (bronze_bucket_name, redshift_glue_bronze_database, redshift_iam_role_arn). Apply platform stack first."
}

New-Item -ItemType Directory -Force -Path $OutDir | Out-Null

$sections = New-Object System.Collections.Generic.List[string]

$sections.Add(@"
-- =============================================================================
-- Redshift bootstrap for env: $Env
-- Generated: $(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')
-- Database: use workgroup database '$($out.redshift_database_name.value)'
-- =============================================================================

"@)

$orderedFiles = @(
    'ddl/01_schemas.sql',
    'ddl/02_dim_date.sql',
    'ddl/03_dim_store.sql',
    'ddl/04_dim_product.sql',
    'ddl/05_dim_customer.sql',
    'ddl/06_identity_graph.sql',
    'ddl/07_fact_sales.sql',
    'ddl/08_fact_inventory_snapshot.sql',
    'ddl/09_fact_customer_session.sql',
    'seeds/dim_date.sql',
    'seeds/dim_store.sql'
)

foreach ($rel in $orderedFiles) {
    $path = Join-Path $RedshiftRoot $rel
    $sections.Add("-- --- $rel ---`r`n")
    $sections.Add((Read-TextFile $path))
    $sections.Add("`r`n")
}

$bronzeSpectrum = Read-TextFile (Join-Path $RedshiftRoot 'spectrum/bronze_external_tables.sql')
$bronzeSpectrum = $bronzeSpectrum.Replace('<REDSHIFT_IAM_ROLE_ARN>', $iamRole)
$bronzeSpectrum = $bronzeSpectrum.Replace('<BRONZE_BUCKET>', $bronzeBucket)
$bronzeSpectrum = $bronzeSpectrum.Replace('<GLUE_BRONZE_DB>', $glueBronze)
$sections.Add("-- --- spectrum/bronze_external_tables.sql ---`r`n")
$sections.Add($bronzeSpectrum)
$sections.Add("`r`n")

if ($IncludeSilver) {
    if (-not $silverBucket) { throw "silver_bucket_name output missing" }
    $silverSpectrum = Read-TextFile (Join-Path $RedshiftRoot 'spectrum/silver_external_tables.sql')
    $silverSpectrum = $silverSpectrum.Replace('<REDSHIFT_IAM_ROLE_ARN>', $iamRole)
    $silverSpectrum = $silverSpectrum.Replace('<SILVER_BUCKET>', $silverBucket)
    $silverSpectrum = $silverSpectrum.Replace('<GLUE_SILVER_DB>', "${glueBronze}_silver")
    $sections.Add("-- --- spectrum/silver_external_tables.sql ---`r`n")
    $sections.Add($silverSpectrum)
    $sections.Add("`r`n")
}

foreach ($rel in @('views/dim_product_current.sql', 'views/customer_360_serving.sql')) {
    $path = Join-Path $RedshiftRoot $rel
    $sections.Add("-- --- $rel ---`r`n")
    $sections.Add((Read-TextFile $path))
    $sections.Add("`r`n")
}

$content = ($sections -join '')
[IO.File]::WriteAllText($OutFile, $content)

Write-Host "==> Wrote bootstrap SQL:" -ForegroundColor Green
Write-Host "    $OutFile"
Write-Host ""
Write-Host "Next steps:" -ForegroundColor Yellow
Write-Host "  1. Open Redshift Query Editor v2 (database: $($out.redshift_database_name.value))"
Write-Host "  2. Paste/run the generated script (or upload the file)"
Write-Host "  3. After Flink + POS land data, re-run bronze COUNT(*) sanity queries at script end"
Write-Host "  4. dbt run / warehouse_daily_batch_pipeline for Gold marts"
Write-Host ""
Write-Host "Tip: run AFTER first Parquet lands in S3 if bronze COUNT(*) should be > 0 immediately."

if ($IncludeMetadata) {
    Write-Host ""
    Write-MetadataScripts -Out $out
}
