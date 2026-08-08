param(
    [Parameter(Mandatory = $false)]
    [ValidateSet("up", "topics", "simulate", "flink", "flink-stop", "dbt", "quality", "all")]
    [string]$Task = "all",
    [Parameter(Mandatory = $false)]
    [ValidateRange(100, 100000)]
    [int]$ClickstreamEventsPerSecond = 3000,
    [Parameter(Mandatory = $false)]
    [ValidateRange(5, 3600)]
    [int]$ClickstreamDurationSeconds = 30,
    [Parameter(Mandatory = $false)]
    [ValidateSet("clickstream", "inventory", "all")]
    [string]$FlinkJob = "all"
)

$ErrorActionPreference = "Stop"

# Resolve repo root + compose file paths relative to this script so it can be
# invoked from any cwd (PowerShell ISE, VS Code tasks, CI, scheduled jobs).
# The compose files live under infra/docker/compose/ — see infra/docker/README.md.
$RepoRoot         = (Resolve-Path (Join-Path $PSScriptRoot '..\..')).Path
$ComposeDir       = (Resolve-Path (Join-Path $PSScriptRoot '..\..\infra\docker\compose')).Path
$MainCompose      = Join-Path $ComposeDir 'docker-compose.yml'
$DashboardCompose = Join-Path $ComposeDir 'docker-compose.dashboard.yml'

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
    $DbtDir = Join-Path $RepoRoot 'transformation\dbt_project'
    $Profile = Join-Path $DbtDir 'profiles.yml'
    $ProfileExample = Join-Path $DbtDir 'profiles.yml.example'

    if (-not (Test-Path $Profile)) {
        Copy-Item $ProfileExample $Profile
        Write-Host "Created local dbt profile from profiles.yml.example." -ForegroundColor DarkGray
    }
}

Push-Location $RepoRoot
try {
    switch ($Task) {
        "up" {
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
                Invoke-CheckedCommand "python ingestion/kafka/topics.py"
            }
        }
        "simulate" {
            # POS is batch, not streamed: generate_pos_parquet.py writes Parquet
            # to bronze (--output-s3 in cloud). No Flink job consumes a POS Kafka
            # topic, so a POS stream producer here would emit into a void.
            Invoke-Step "Running inventory producer" {
                Invoke-CheckedCommand "python -m ingestion.kafka.producer_sim.inventory_producer"
            }
            Invoke-Step "Running clickstream producer" {
                Invoke-CheckedCommand "python -c `"from ingestion.kafka.producer_sim.clickstream_producer import run_producer; run_producer(events_per_second=$ClickstreamEventsPerSecond, duration_seconds=$ClickstreamDurationSeconds)`""
            }
        }
        "flink" {
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
                # Flink 1.17 accepts a comma-separated file/archive list for
                # -pyfs, not a directory. Ship each shared helper explicitly.
                # -pyexec / -pyclientexec point Flink at python3 explicitly; Debian images
                # do not ship a `python` binary by default.
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
            Write-Host ""
            Write-Host "Flink Web UI:  http://localhost:8082" -ForegroundColor Yellow
            Write-Host "Iceberg data:  docker compose -f $MainCompose exec flink-taskmanager ls -lah /tmp/iceberg/" -ForegroundColor Yellow
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
        "dbt" {
            Invoke-Step "Installing dbt packages and running models" {
                Ensure-LocalDbtProfile
                Push-Location "transformation/dbt_project"
                try {
                    Invoke-CheckedCommand "dbt deps --profiles-dir . --target local"
                    Invoke-CheckedCommand "dbt seed --profiles-dir . --target local"
                    Invoke-CheckedCommand "dbt run --profiles-dir . --target local"
                }
                finally {
                    Pop-Location
                }
            }
        }
        "quality" {
            Invoke-Step "Running dbt tests" {
                Ensure-LocalDbtProfile
                Push-Location "transformation/dbt_project"
                try {
                    Invoke-CheckedCommand "dbt test --profiles-dir . --target local"
                }
                finally {
                    Pop-Location
                }
            }
            Invoke-Step "Running offline pytest suite" {
                Invoke-CheckedCommand "python -m pytest tests/unit -q"
            }
        }
        "all" {
            & $PSCommandPath -Task up
            & $PSCommandPath -Task topics
            & $PSCommandPath -Task simulate -ClickstreamEventsPerSecond $ClickstreamEventsPerSecond -ClickstreamDurationSeconds $ClickstreamDurationSeconds
            & $PSCommandPath -Task flink -FlinkJob $FlinkJob
            & $PSCommandPath -Task dbt
            & $PSCommandPath -Task quality
        }
    }
}
finally {
    Pop-Location
}

Write-Host "Completed task: $Task" -ForegroundColor Green
