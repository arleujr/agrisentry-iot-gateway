$ErrorActionPreference = "Stop"

$repoRoot = Resolve-Path (Join-Path $PSScriptRoot "..\..")
$composeFile = Join-Path $repoRoot "compose.local.yaml"

function Invoke-Compose {
    param(
        [Parameter(Mandatory = $true)]
        [string[]] $Arguments
    )

    & docker compose -f $composeFile @Arguments
    if ($LASTEXITCODE -ne 0) {
        throw "docker compose failed: $($Arguments -join ' ')"
    }
}

function Test-TableExists {
    param(
        [Parameter(Mandatory = $true)]
        [string] $TableName
    )

    $query = "SELECT to_regclass('public.$TableName') IS NOT NULL;"
    $result = & docker compose -f $composeFile exec -T db `
        psql -U agrisentry_local -d agrisentry_local -tAc $query

    if ($LASTEXITCODE -ne 0) {
        throw "Failed to inspect table $TableName."
    }

    return $result.Trim() -eq "t"
}

Push-Location $repoRoot
try {
    & docker version *> $null
    if ($LASTEXITCODE -ne 0) {
        throw "Docker Desktop is not available. Start Docker Desktop and retry."
    }

    $mqttMigration = "migrations/20260724000000_create_mqtt_v1_telemetry.sql"

    if (-not (Test-Path (Join-Path $repoRoot $mqttMigration))) {
        throw "Required migration not found: $mqttMigration. Pull the latest main branch first."
    }

    Write-Host "Starting local PostgreSQL/TimescaleDB and Mosquitto..." -ForegroundColor Cyan
    Invoke-Compose -Arguments @("up", "-d", "db", "mqtt")

    Write-Host "Waiting for PostgreSQL..." -ForegroundColor Cyan
    $databaseReady = $false

    for ($attempt = 1; $attempt -le 30; $attempt++) {
        & docker compose -f $composeFile exec -T db `
            pg_isready -U agrisentry_local -d agrisentry_local *> $null

        if ($LASTEXITCODE -eq 0) {
            $databaseReady = $true
            break
        }

        Start-Sleep -Seconds 2
    }

    if (-not $databaseReady) {
        throw "PostgreSQL did not become ready."
    }

    if (-not (Test-TableExists -TableName "telemetry_events")) {
        Write-Host "Applying MQTT v1 telemetry migration..." -ForegroundColor Yellow
        Invoke-Compose -Arguments @(
            "exec", "-T", "db",
            "psql",
            "-U", "agrisentry_local",
            "-d", "agrisentry_local",
            "-v", "ON_ERROR_STOP=1",
            "-f", "/migrations/20260724000000_create_mqtt_v1_telemetry.sql"
        )
    }
    else {
        Write-Host "MQTT v1 migration already applied." -ForegroundColor DarkGray
    }

    if (-not (Test-TableExists -TableName "telemetry_readings")) {
        throw "telemetry_readings was not created by the MQTT v1 migration."
    }

    Write-Host ""
    Write-Host "Local infrastructure is ready." -ForegroundColor Green
    Write-Host "PostgreSQL: 127.0.0.1:5432"
    Write-Host "MQTT:       127.0.0.1:1883"
    Write-Host ""
    Write-Host "Next terminal command:"
    Write-Host "  powershell -ExecutionPolicy Bypass -File .\scripts\local\run-gateway.ps1"
}
finally {
    Pop-Location
}
