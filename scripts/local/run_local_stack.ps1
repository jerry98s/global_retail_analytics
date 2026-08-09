param(
    [Parameter(Mandatory = $false)]
    [ValidateSet("up", "topics", "simulate", "flink", "flink-stop", "pos-parquet", "load-duckdb", "dbt", "quality", "all")]
    [string]$Task = "all",
    [Parameter(Mandatory = $false)]
    [ValidateRange(100, 100000)]
    [int]$ClickstreamEventsPerSecond = 3000,
    [Parameter(Mandatory = $false)]
    [ValidateRange(5, 3600)]
    [int]$ClickstreamDurationSeconds = 30,
    [Parameter(Mandatory = $false)]
    [ValidateSet("clickstream", "inventory", "all")]
    [string]$FlinkJob = "all",
    # iceberg = reuse Flink Parquet + local POS Parquet; seed only dim_date/dim_store.
    # seeds   = curated CSV bronze/silver fixtures (CI identity scenarios).
    [Parameter(Mandatory = $false)]
    [ValidateSet("iceberg", "seeds")]
    [string]$DbtSource = "iceberg",
    [Parameter(Mandatory = $false)]
    [ValidateRange(30, 600)]
    [int]$IcebergWaitSeconds = 90,
    [Parameter(Mandatory = $false)]
    [int]$PosTransactionCount = 500
)

$ErrorActionPreference = "Stop"

# Resolve repo root + compose file paths relative to this script so it can be
# invoked from any cwd (PowerShell ISE, VS Code tasks, CI, scheduled jobs).
# The compose files live under infra/docker/compose/ — see infra/docker/README.md.
$RepoRoot         = (Resolve-Path (Join-Path $PSScriptRoot '..\..')).Path
$ComposeDir       = (Resolve-Path (Join-Path $PSScriptRoot '..\..\infra\docker\compose')).Path
$MainCompose      = Join-Path $ComposeDir 'docker-compose.yml'
$DashboardCompose = Join-Path $ComposeDir 'docker-compose.dashboard.yml'
$IcebergHostDir   = Join-Path $RepoRoot '.local\iceberg'
$DbtDir           = Join-Path $RepoRoot 'transformation\dbt_project'
$VenvDir          = Join-Path $RepoRoot '.venv'
$ProjectPython    = Join-Path $VenvDir 'Scripts\python.exe'
$ProjectDbt       = Join-Path $VenvDir 'Scripts\dbt.exe'

function Ensure-ProjectVenv {
    if (-not (Test-Path $ProjectPython)) {
        $uv = Get-Command uv -ErrorAction SilentlyContinue
        if ($uv) {
            Write-Host "Creating .venv via uv sync --group dev ..." -ForegroundColor DarkGray
            Push-Location $RepoRoot
            try {
                & uv sync --group dev
                if ($LASTEXITCODE -ne 0) {
                    throw "uv sync failed with exit code $LASTEXITCODE"
                }
            }
            finally {
                Pop-Location
            }
        } else {
            throw @"
Project virtualenv not found at $VenvDir
Install uv (https://docs.astral.sh/uv/) and run:
  uv sync --group dev
Or create .venv manually and pip install from pyproject.toml [dependency-groups].dev
"@
        }
    }
    if (-not (Test-Path $ProjectPython)) {
        throw "Project Python not found at $ProjectPython after venv setup."
    }
}

function Get-PythonCmd {
    Ensure-ProjectVenv
    return $ProjectPython
}

function Get-DbtCmd {
    Ensure-ProjectVenv
    if (Test-Path $ProjectDbt) {
        return $ProjectDbt
    }
    return $ProjectPython
}

function Invoke-ProjectPython {
    param(
        [Parameter(Mandatory = $true, ValueFromRemainingArguments = $true)]
        [string[]]$Args
    )
    Ensure-ProjectVenv
    Write-Host ">> $ProjectPython $($Args -join ' ')" -ForegroundColor DarkGray
    # Native stderr (tqdm/GE progress) must not abort under $ErrorActionPreference=Stop.
    $prevEap = $ErrorActionPreference
    $ErrorActionPreference = "Continue"
    try {
        & $ProjectPython @Args
        $code = $LASTEXITCODE
    }
    finally {
        $ErrorActionPreference = $prevEap
    }
    if ($code -ne 0) {
        throw "Python command failed with exit code ${code}: $($Args -join ' ')"
    }
}

function Invoke-ProjectDbt {
    param(
        [Parameter(Mandatory = $true)]
        [string[]]$Args,
        [string]$ExecutionId = ""
    )
    Ensure-ProjectVenv
    $dbtExe = Get-DbtCmd
    Write-Host ">> $dbtExe $($Args -join ' ')" -ForegroundColor DarkGray
    $prevEap = $ErrorActionPreference
    $ErrorActionPreference = "Continue"
    try {
        if ($dbtExe -eq $ProjectPython) {
            & $ProjectPython -m dbt @Args
        } else {
            & $dbtExe @Args
        }
        $code = $LASTEXITCODE
    }
    finally {
        $ErrorActionPreference = $prevEap
    }
    if ($ExecutionId) {
        # Parse run_results immediately before the next dbt command overwrites it.
        Invoke-MetadataObserver @(
            'parse-dbt',
            '--backend', 'local',
            '--execution-id', $ExecutionId,
            '--run-results', (Join-Path $DbtDir 'target\run_results.json')
        )
    }
    if ($code -ne 0) {
        throw "dbt command failed with exit code ${code}: $($Args -join ' ')"
    }
}

function Invoke-MetadataObserver {
    param(
        [Parameter(Mandatory = $true)]
        [string[]]$Args
    )
    # Fail-open: metadata CLI itself exits 0 on write errors.
    # Always use an absolute path — callers often Push-Location into dbt_project.
    $observer = Join-Path $RepoRoot 'scripts\common\metadata_observer.py'
    try {
        Invoke-ProjectPython $observer @Args
    } catch {
        Write-Host "WARNING: metadata observer failed (ignored): $_" -ForegroundColor Yellow
    }
}

function New-LocalExecutionId {
    return [guid]::NewGuid().ToString()
}

function Start-LocalPipelineRun {
    param(
        [Parameter(Mandatory = $true)][string]$ExecutionId,
        [Parameter(Mandatory = $true)][string]$Pipeline
    )
    Invoke-MetadataObserver @(
        'init-local',
        '--backend', 'local',
        '--metadata-duckdb', (Join-Path $DbtDir 'local_metadata.duckdb')
    )
    Invoke-MetadataObserver @(
        'start-run',
        '--backend', 'local',
        '--execution-id', $ExecutionId,
        '--pipeline', $Pipeline,
        '--environment', 'local',
        '--trigger', 'manual'
    )
}

function Finish-LocalPipelineRun {
    param(
        [Parameter(Mandatory = $true)][string]$ExecutionId,
        [Parameter(Mandatory = $true)][ValidateSet('SUCCESS','FAILED')][string]$Status,
        [string]$ErrorText = ""
    )
    $args = @(
        'finish-run',
        '--backend', 'local',
        '--execution-id', $ExecutionId,
        '--status', $Status
    )
    if ($ErrorText) {
        $args += @('--error', $ErrorText)
    }
    Invoke-MetadataObserver $args
}

function Invoke-LocalFreshness {
    param([Parameter(Mandatory = $true)][string]$ExecutionId)
    Invoke-MetadataObserver @(
        'collect-freshness',
        '--backend', 'local',
        '--execution-id', $ExecutionId,
        '--analytics-duckdb', (Join-Path $DbtDir 'local_retail.duckdb'),
        '--metadata-duckdb', (Join-Path $DbtDir 'local_metadata.duckdb')
    )
}

function Wait-ForKafka {
    param(
        [int]$TimeoutSec = 120
    )
    Write-Host "Waiting for Kafka on 127.0.0.1:9092 ..." -ForegroundColor DarkGray
    $deadline = (Get-Date).AddSeconds($TimeoutSec)
    while ((Get-Date) -lt $deadline) {
        try {
            $t = Test-NetConnection -ComputerName 127.0.0.1 -Port 9092 -WarningAction SilentlyContinue
            if ($t.TcpTestSucceeded) {
                Start-Sleep -Seconds 3
                Write-Host "Kafka port is open." -ForegroundColor DarkGray
                return
            }
        } catch { }
        Start-Sleep -Seconds 2
    }
    throw "Kafka did not accept TCP connections on 127.0.0.1:9092 within ${TimeoutSec}s. Check: docker compose -f $MainCompose ps, docker compose -f $MainCompose logs kafka"
}

function Invoke-Step {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Name,
        [Parameter(Mandatory = $true)]
        [scriptblock]$Action
    )
    Write-Host "==> $Name" -ForegroundColor Cyan
    & $Action
}

function Invoke-CheckedCommand {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Command
    )
    Write-Host ">> $Command" -ForegroundColor DarkGray
    Invoke-Expression $Command
    if ($LASTEXITCODE -ne 0) {
        throw "Command failed with exit code ${LASTEXITCODE}: $Command"
    }
}

function Ensure-LocalDbtProfile {
    $Profile = Join-Path $DbtDir 'profiles.yml'
    $ProfileExample = Join-Path $DbtDir 'profiles.yml.example'

    if (-not (Test-Path $Profile)) {
        Copy-Item $ProfileExample $Profile
        Write-Host "Created local dbt profile from profiles.yml.example." -ForegroundColor DarkGray
    }
}

function Ensure-IcebergHostDir {
    if (-not (Test-Path $IcebergHostDir)) {
        New-Item -ItemType Directory -Force -Path $IcebergHostDir | Out-Null
        Write-Host "Created $IcebergHostDir (Flink bind-mount)." -ForegroundColor DarkGray
    }
}

function Wait-ForIcebergParquet {
    param(
        [int]$TimeoutSec = 90,
        [string[]]$RelativeDirs = @(
            'bronze\clickstream_events\data',
            'bronze\inventory_events\data'
        )
    )
    Write-Host "Waiting up to ${TimeoutSec}s for Iceberg Parquet under $IcebergHostDir ..." -ForegroundColor DarkGray
    $deadline = (Get-Date).AddSeconds($TimeoutSec)
    while ((Get-Date) -lt $deadline) {
        $ready = $true
        foreach ($rel in $RelativeDirs) {
            $dir = Join-Path $IcebergHostDir $rel
            $hits = @()
            if (Test-Path $dir) {
                $hits = @(Get-ChildItem -Path $dir -Filter '*.parquet' -Recurse -ErrorAction SilentlyContinue)
            }
            if ($hits.Count -eq 0) { $ready = $false; break }
        }
        if ($ready) {
            Write-Host "Iceberg Parquet present." -ForegroundColor DarkGray
            return
        }
        Start-Sleep -Seconds 5
    }
    Write-Host "WARNING: timed out waiting for Iceberg Parquet - dbt load may fail." -ForegroundColor Yellow
}

function Invoke-PosParquetLocal {
    $outDir = Join-Path $IcebergHostDir 'bronze\pos_transactions'
    $txnDate = (Get-Date).ToString('yyyy-MM-dd')
    Invoke-ProjectPython -m ingestion.batch.generate_pos_parquet `
        --date $txnDate `
        --output-dir $outDir `
        --transaction-count $PosTransactionCount
}

function Invoke-LoadDuckdb {
    Invoke-ProjectPython scripts/local/load_iceberg_to_duckdb.py `
        --iceberg-dir $IcebergHostDir `
        --duckdb (Join-Path $DbtDir 'local_retail.duckdb')
}

# ADR-009: promote the pending Gold marts into live schemas in local_retail.duckdb.
# Reuses the shared canonical table list + DuckDB swap in the Airflow plugin so
# local and cloud stay aligned. After publish, re-run the consent-gated serving
# view against LIVE (wap_phase defaults to live) so DuckDB re-binds the view to
# the freshly swapped marketing tables.
function Invoke-WapPublishLocal {
    Invoke-ProjectPython -c @"
import sys
from pathlib import Path
sys.path.insert(0, str(Path(r'$RepoRoot')))
import duckdb
from orchestration.airflow.plugins.wap_publish import WAP_TABLES, publish_gold

con = duckdb.connect(r'$(Join-Path $DbtDir 'local_retail.duckdb')')
try:
    for schema in {s for s, _ in WAP_TABLES}:
        con.execute(f'CREATE SCHEMA IF NOT EXISTS {schema}')
    result = publish_gold(con, WAP_TABLES, dialect='duckdb')
    print('WAP published:', result['published'])
finally:
    con.close()
"@
    Push-Location $DbtDir
    try {
        Invoke-ProjectDbt @(
            'run', '--profiles-dir', '.', '--target', 'local',
            '--select', 'customer_360_serving'
        )
    }
    finally {
        Pop-Location
    }
}

function Invoke-DbtIceberg {
    param([string]$ExecutionId = "")
    Ensure-LocalDbtProfile
    Ensure-IcebergHostDir
    Invoke-Step "Generate local POS Parquet (not streamed to Iceberg)" {
        Invoke-PosParquetLocal
    }
    Invoke-Step "Load Iceberg + POS Parquet into DuckDB" {
        Invoke-LoadDuckdb
    }
    if ($ExecutionId) {
        Invoke-LocalFreshness -ExecutionId $ExecutionId
    }
    Push-Location $DbtDir
    try {
        Invoke-ProjectDbt @('deps', '--profiles-dir', '.', '--target', 'local') -ExecutionId $ExecutionId
        # Only reference dims - stream + POS tables already loaded from Parquet.
        Invoke-ProjectDbt @(
            'seed', '--profiles-dir', '.', '--target', 'local',
            '--select', 'dim_date', 'dim_store'
        ) -ExecutionId $ExecutionId
        # ADR-009: build Gold marts into *_pending schemas; publish comes after
        # the quality audits in the dbt/quality/all tasks.
        Invoke-ProjectDbt @(
            'run', '--profiles-dir', '.', '--target', 'local',
            '--vars', '{"wap_phase": "pending"}'
        ) -ExecutionId $ExecutionId
    }
    finally {
        Pop-Location
    }
    if ($ExecutionId) {
        Invoke-LocalFreshness -ExecutionId $ExecutionId
    }
}

function Invoke-DbtSeeds {
    param([string]$ExecutionId = "")
    Ensure-LocalDbtProfile
    $duckDb = Join-Path $DbtDir 'local_retail.duckdb'
    if (Test-Path $duckDb) {
        Remove-Item $duckDb -Force
        Write-Host "Removed $duckDb for clean CSV seed load." -ForegroundColor DarkGray
    }
    Push-Location $DbtDir
    try {
        Invoke-ProjectDbt @('deps', '--profiles-dir', '.', '--target', 'local') -ExecutionId $ExecutionId
        Invoke-ProjectDbt @('seed', '--profiles-dir', '.', '--target', 'local') -ExecutionId $ExecutionId
        # ADR-009: build Gold marts into *_pending schemas.
        Invoke-ProjectDbt @(
            'run', '--profiles-dir', '.', '--target', 'local',
            '--vars', '{"wap_phase": "pending"}'
        ) -ExecutionId $ExecutionId
    }
    finally {
        Pop-Location
    }
    if ($ExecutionId) {
        Invoke-LocalFreshness -ExecutionId $ExecutionId
    }
}

Push-Location $RepoRoot
try {
    switch ($Task) {
        "up" {
            Ensure-IcebergHostDir
            Invoke-Step "Starting local Kafka stack" {
                Invoke-CheckedCommand "docker compose -f $MainCompose up -d"
            }
            Wait-ForKafka
        }
        "topics" {
            Wait-ForKafka
            Invoke-Step "Creating Kafka topics" {
                if (-not $env:KAFKA_BOOTSTRAP_SERVERS) {
                    $env:KAFKA_BOOTSTRAP_SERVERS = "127.0.0.1:9092"
                }
                Invoke-ProjectPython ingestion/kafka/topics.py
            }
        }
        "simulate" {
            # POS is batch, not streamed: the dbt task's Invoke-PosParquetLocal
            # writes Parquet to .local/iceberg/bronze/pos_transactions (the same
            # mechanism as cloud --output-s3). No Flink job consumes a POS Kafka
            # topic, so a POS stream producer here would emit into a void.
            Invoke-Step "Running inventory producer" {
                # 90s spans >1 local silver window; with 5s local watermark delay
                # at least one tumble can close before the post-simulate wait.
                Invoke-ProjectPython -m ingestion.kafka.producer_sim.inventory_producer --duration 90
            }
            Invoke-Step "Running clickstream producer" {
                $code = "from ingestion.kafka.producer_sim.clickstream_producer import run_producer; run_producer(events_per_second=$ClickstreamEventsPerSecond, duration_seconds=$ClickstreamDurationSeconds)"
                Invoke-ProjectPython -c $code
            }
        }
        "flink" {
            Ensure-IcebergHostDir
            Invoke-Step "Building/starting local Flink cluster" {
                Invoke-CheckedCommand "docker compose -f $MainCompose up -d --build flink-jobmanager flink-taskmanager"
            }
            Invoke-Step "Preparing local Flink volumes" {
                Invoke-CheckedCommand "docker compose -f $MainCompose exec -T -u root flink-jobmanager chown -R flink:flink /tmp/flink-checkpoints /tmp/iceberg"
            }
            Invoke-Step "Waiting for Flink JobManager REST" {
                $deadline = (Get-Date).AddMinutes(2)
                while ((Get-Date) -lt $deadline) {
                    try {
                        $resp = Invoke-WebRequest -UseBasicParsing -Uri "http://localhost:8082/overview" -TimeoutSec 3
                        if ($resp.StatusCode -eq 200) { break }
                    } catch { Start-Sleep -Seconds 3 }
                }
            }
            $PyFiles = @(
                '/opt/streaming/flink_jobs/_config.py',
                '/opt/streaming/flink_jobs/event_types.py',
                '/opt/streaming/flink_jobs/lake_names.py'
            ) -join ','
            $submit = {
                param([string]$entry, [string]$jobName, [string]$pyFiles)
                Write-Host ">> Submitting $jobName" -ForegroundColor DarkGray
                Invoke-CheckedCommand "docker compose -f $MainCompose exec -T flink-jobmanager flink run -d -pyexec /usr/bin/python3 -pyclientexec /usr/bin/python3 -pyfs $pyFiles -py /opt/streaming/flink_jobs/$entry"
            }
            Invoke-Step "Submitting Flink jobs" {
                if ($FlinkJob -in @("clickstream","all")) {
                    & $submit "clickstream_bronze_job.py" "clickstream-bronze" $PyFiles
                }
                if ($FlinkJob -in @("inventory","all")) {
                    & $submit "inventory_bronze_job.py" "inventory-bronze" $PyFiles
                    & $submit "inventory_silver_job.py" "inventory-hourly" $PyFiles
                }
            }
            # The chown above runs before submission, but the jobs create the
            # Iceberg table dirs only once they start — and those come back
            # root-owned on Docker Desktop bind mounts, which makes every
            # checkpoint commit fail with Permission denied on a fresh clone.
            # Re-chown after submission, waiting for the dirs to appear first.
            Invoke-Step "Aligning Iceberg dir ownership post-submit" {
                $deadline = (Get-Date).AddSeconds(90)
                do {
                    Start-Sleep -Seconds 10
                    docker compose -f $MainCompose exec -T -u root flink-jobmanager chown -R flink:flink /tmp/iceberg 2>$null | Out-Null
                    $leftover = docker compose -f $MainCompose exec -T flink-jobmanager find /tmp/iceberg -user root -print -quit 2>$null
                } while ($leftover -and (Get-Date) -lt $deadline)
                if ($leftover) {
                    Write-Host "WARNING: $leftover still root-owned; Iceberg commits may fail (Permission denied)." -ForegroundColor Yellow
                }
            }
            Write-Host ""
            Write-Host "Flink Web UI:  http://localhost:8082" -ForegroundColor Yellow
            Write-Host "Iceberg host:  $IcebergHostDir" -ForegroundColor Yellow
            Write-Host "Tip: start Flink BEFORE simulate (latest-offset). Task all does this." -ForegroundColor Yellow
        }
        "flink-stop" {
            Invoke-Step "Cancelling running Flink jobs and stopping cluster" {
                try {
                    $jobs = docker compose -f $MainCompose exec -T flink-jobmanager flink list -r 2>$null
                    $jobs | Where-Object { $_ -match "^\d{2}\.\d{2}\.\d{4}.*\(" } | ForEach-Object {
                        if ($_ -match ":\s+([0-9a-f]{32})\s+:") {
                            $id = $Matches[1]
                            Write-Host ">> cancelling $id"
                            Invoke-Expression "docker compose -f $MainCompose exec -T flink-jobmanager flink cancel $id"
                        }
                    }
                } catch { Write-Host "No running jobs or jobmanager not reachable." }
                Invoke-CheckedCommand "docker compose -f $MainCompose stop flink-taskmanager flink-jobmanager"
            }
        }
        "pos-parquet" {
            Ensure-IcebergHostDir
            Invoke-Step "Generate local POS bronze Parquet" {
                Invoke-PosParquetLocal
            }
        }
        "load-duckdb" {
            Ensure-IcebergHostDir
            Invoke-Step "Load Iceberg warehouse into DuckDB" {
                Invoke-LoadDuckdb
            }
        }
        "dbt" {
            $execId = New-LocalExecutionId
            $pipelineStatus = "SUCCESS"
            $pipelineError = ""
            try {
                Start-LocalPipelineRun -ExecutionId $execId -Pipeline "local_dbt"
                if ($DbtSource -eq "iceberg") {
                    Invoke-Step "dbt local (Iceberg Parquet + dim seeds)" {
                        Invoke-DbtIceberg -ExecutionId $execId
                    }
                } else {
                    Invoke-Step "dbt local (CSV seeds fixture mode)" {
                        Invoke-DbtSeeds -ExecutionId $execId
                    }
                }
            } catch {
                $pipelineStatus = "FAILED"
                $pipelineError = "$_"
                throw
            } finally {
                Finish-LocalPipelineRun -ExecutionId $execId -Status $pipelineStatus -ErrorText $pipelineError
                Write-Host "Metadata execution_id: $execId" -ForegroundColor DarkGray
            }
        }
        "quality" {
            $execId = New-LocalExecutionId
            $pipelineStatus = "SUCCESS"
            $pipelineError = ""
            try {
                Start-LocalPipelineRun -ExecutionId $execId -Pipeline "local_quality"
                Invoke-Step "Running dbt tests (pending)" {
                    Ensure-LocalDbtProfile
                    Push-Location $DbtDir
                    try {
                        Invoke-ProjectDbt @(
                            'test', '--profiles-dir', '.', '--target', 'local',
                            '--vars', '{"wap_phase": "pending"}'
                        ) -ExecutionId $execId
                    }
                    finally {
                        Pop-Location
                    }
                }
                Invoke-Step "Running Great Expectations gold_layer_local (pending, DuckDB)" {
                    Invoke-ProjectPython scripts/local/run_ge_local.py `
                        --duckdb (Join-Path $DbtDir 'local_retail.duckdb') `
                        --execution-id $execId `
                        --metadata-duckdb (Join-Path $DbtDir 'local_metadata.duckdb') `
                        --schema-suffix _pending
                }
                Invoke-Step "WAP publish pending Gold -> live" {
                    Invoke-WapPublishLocal
                }
                Invoke-LocalFreshness -ExecutionId $execId
                Invoke-Step "Running offline pytest suite" {
                    Invoke-ProjectPython -m pytest tests/unit -q
                }
            } catch {
                $pipelineStatus = "FAILED"
                $pipelineError = "$_"
                throw
            } finally {
                Finish-LocalPipelineRun -ExecutionId $execId -Status $pipelineStatus -ErrorText $pipelineError
                Write-Host "Metadata execution_id: $execId" -ForegroundColor DarkGray
            }
        }
        "all" {
            # Flink before simulate: jobs use scan.startup.mode=latest-offset.
            $execId = New-LocalExecutionId
            $pipelineStatus = "SUCCESS"
            $pipelineError = ""
            try {
                Start-LocalPipelineRun -ExecutionId $execId -Pipeline "local_e2e"
                & $PSCommandPath -Task up
                & $PSCommandPath -Task topics
                & $PSCommandPath -Task flink -FlinkJob $FlinkJob
                & $PSCommandPath -Task simulate -ClickstreamEventsPerSecond $ClickstreamEventsPerSecond -ClickstreamDurationSeconds $ClickstreamDurationSeconds
                Wait-ForIcebergParquet -TimeoutSec $IcebergWaitSeconds
                # Allow silver 1-minute windows + idle timeout to close after produce stops.
                Write-Host "Waiting 75s for silver windows / checkpoints ..." -ForegroundColor DarkGray
                Start-Sleep -Seconds 75
                if ($DbtSource -eq "iceberg") {
                    Invoke-Step "dbt local (Iceberg Parquet + dim seeds)" {
                        Invoke-DbtIceberg -ExecutionId $execId
                    }
                } else {
                    Invoke-Step "dbt local (CSV seeds fixture mode)" {
                        Invoke-DbtSeeds -ExecutionId $execId
                    }
                }
                Invoke-Step "Running dbt tests (pending)" {
                    Ensure-LocalDbtProfile
                    Push-Location $DbtDir
                    try {
                        Invoke-ProjectDbt @(
                            'test', '--profiles-dir', '.', '--target', 'local',
                            '--vars', '{"wap_phase": "pending"}'
                        ) -ExecutionId $execId
                    }
                    finally {
                        Pop-Location
                    }
                }
                Invoke-Step "Running Great Expectations gold_layer_local (pending, DuckDB)" {
                    Invoke-ProjectPython scripts/local/run_ge_local.py `
                        --duckdb (Join-Path $DbtDir 'local_retail.duckdb') `
                        --execution-id $execId `
                        --metadata-duckdb (Join-Path $DbtDir 'local_metadata.duckdb') `
                        --schema-suffix _pending
                }
                Invoke-Step "WAP publish pending Gold -> live" {
                    Invoke-WapPublishLocal
                }
                Invoke-LocalFreshness -ExecutionId $execId
                Invoke-Step "Running offline pytest suite" {
                    Invoke-ProjectPython -m pytest tests/unit -q
                }
            } catch {
                $pipelineStatus = "FAILED"
                $pipelineError = "$_"
                throw
            } finally {
                Finish-LocalPipelineRun -ExecutionId $execId -Status $pipelineStatus -ErrorText $pipelineError
                Write-Host "Metadata execution_id: $execId (local_metadata.duckdb)" -ForegroundColor Yellow
            }
        }
    }
}
finally {
    Pop-Location
}

Write-Host "Completed task: $Task" -ForegroundColor Green
