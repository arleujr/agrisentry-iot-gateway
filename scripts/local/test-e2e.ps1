$ErrorActionPreference = "Stop"

$repoRoot = Resolve-Path (Join-Path $PSScriptRoot "..\..")
$composeFile = Join-Path $repoRoot "compose.local.yaml"
$mqttContainer = "agrisentry-local-mqtt"
$containerPayloadPath = "/tmp/agrisentry-telemetry.json"
$localPayloadPath = Join-Path ([System.IO.Path]::GetTempPath()) `
    "agrisentry-telemetry-$([guid]::NewGuid()).json"

$deviceId = "hydro-lab-node-01"
$eventId = [guid]::NewGuid().ToString()
$sequence = [DateTimeOffset]::UtcNow.ToUnixTimeMilliseconds()
$observedAt = [DateTime]::UtcNow.ToString(
    "yyyy-MM-ddTHH:mm:ss.fffZ",
    [System.Globalization.CultureInfo]::InvariantCulture
)

function Invoke-NativeCommand {
    param(
        [Parameter(Mandatory = $true)]
        [scriptblock] $Command,
        [Parameter(Mandatory = $true)]
        [string] $FailureMessage
    )

    & $Command

    if ($LASTEXITCODE -ne 0) {
        throw $FailureMessage
    }
}

Push-Location $repoRoot

try {
    try {
        $health = Invoke-RestMethod `
            -Uri "http://127.0.0.1:8080/health" `
            -Method Get `
            -TimeoutSec 3
    }
    catch {
        throw "Gateway is not running on port 8080. Keep run-gateway.ps1 open in another terminal."
    }

    if ($health.status -ne "healthy") {
        throw "Gateway health endpoint returned an unexpected response."
    }

    $payload = [ordered]@{
        protocol_version = "1.0"
        event_id = $eventId
        device_id = $deviceId
        sequence = $sequence
        observed_at = $observedAt
        firmware_version = "2.0.0-alpha.1"
        readings = @(
            [ordered]@{
                channel = "air_temperature"
                raw_value = 27.4
                value = 27.4
                unit = "celsius"
                quality = "valid"
                calibration_id = $null
            },
            [ordered]@{
                channel = "air_relative_humidity"
                raw_value = 68.2
                value = 68.2
                unit = "percent"
                quality = "valid"
                calibration_id = $null
            },
            [ordered]@{
                channel = "solution_temperature"
                raw_value = 2048
                value = 23.1
                unit = "celsius"
                quality = "estimated"
                calibration_id = "cal-ntc-local-001"
            },
            [ordered]@{
                channel = "solution_ph"
                raw_value = 1880
                value = 6.18
                unit = "ph"
                quality = "not_calibrated"
                calibration_id = $null
            },
            [ordered]@{
                channel = "solution_tds"
                raw_value = 1420
                value = 735
                unit = "ppm"
                quality = "estimated"
                calibration_id = "cal-tds-local-001"
            },
            [ordered]@{
                channel = "reservoir_level"
                raw_value = 2500
                value = 72
                unit = "percent"
                quality = "estimated"
                calibration_id = "cal-level-local-001"
            },
            [ordered]@{
                channel = "relative_light"
                raw_value = 3100
                value = 84
                unit = "percent"
                quality = "estimated"
                calibration_id = "cal-light-local-001"
            }
        )
    }

    $json = $payload | ConvertTo-Json -Depth 10 -Compress

    # Write UTF-8 without BOM and copy the file into the MQTT container.
    [System.IO.File]::WriteAllText(
        $localPayloadPath,
        $json,
        [System.Text.UTF8Encoding]::new($false)
    )

    Invoke-NativeCommand `
        -Command {
            docker cp `
                $localPayloadPath `
                "${mqttContainer}:${containerPayloadPath}"
        } `
        -FailureMessage "Failed to copy the telemetry JSON into the MQTT container."

    $topic = "agrisentry/v1/devices/$deviceId/telemetry"

    Write-Host "Publishing event once..." -ForegroundColor Cyan
    Invoke-NativeCommand `
        -Command {
            docker compose -f $composeFile exec -T mqtt `
                mosquitto_pub `
                -h 127.0.0.1 `
                -p 1883 `
                -q 1 `
                -t $topic `
                -f $containerPayloadPath
        } `
        -FailureMessage "The first MQTT publication failed."

    Write-Host "Publishing the same event again..." -ForegroundColor Cyan
    Invoke-NativeCommand `
        -Command {
            docker compose -f $composeFile exec -T mqtt `
                mosquitto_pub `
                -h 127.0.0.1 `
                -p 1883 `
                -q 1 `
                -t $topic `
                -f $containerPayloadPath
        } `
        -FailureMessage "The duplicate MQTT publication failed."

    Start-Sleep -Seconds 2

    $eventQuery = "SELECT COUNT(*) FROM telemetry_events WHERE event_id = '$eventId';"
    $readingQuery = "SELECT COUNT(*) FROM telemetry_readings WHERE event_id = '$eventId';"

    $eventCount = (& docker compose -f $composeFile exec -T db `
        psql -U agrisentry_local -d agrisentry_local -tAc $eventQuery).Trim()

    if ($LASTEXITCODE -ne 0) {
        throw "Failed to query telemetry_events."
    }

    $readingCount = (& docker compose -f $composeFile exec -T db `
        psql -U agrisentry_local -d agrisentry_local -tAc $readingQuery).Trim()

    if ($LASTEXITCODE -ne 0) {
        throw "Failed to query telemetry_readings."
    }

    if ($eventCount -ne "1") {
        throw "Idempotency failed: expected 1 event, found $eventCount."
    }

    if ($readingCount -ne "7") {
        throw "Atomic persistence failed: expected 7 readings, found $readingCount."
    }

    Write-Host ""
    Write-Host "E2E PASSED" -ForegroundColor Green
    Write-Host "Event ID:          $eventId"
    Write-Host "Messages published: 2"
    Write-Host "Events stored:      $eventCount"
    Write-Host "Readings stored:    $readingCount"
    Write-Host ""

    & docker compose -f $composeFile exec -T db `
        psql -U agrisentry_local -d agrisentry_local -c @"
SELECT
    e.device_id,
    e.sequence,
    e.observed_at,
    e.received_at,
    COUNT(r.channel) AS reading_count
FROM telemetry_events e
JOIN telemetry_readings r ON r.event_id = e.event_id
WHERE e.event_id = '$eventId'
GROUP BY e.event_id;
"@

    if ($LASTEXITCODE -ne 0) {
        throw "Failed to print the persisted event."
    }
}
finally {
    Remove-Item $localPayloadPath -Force -ErrorAction SilentlyContinue

    docker exec $mqttContainer `
        rm -f $containerPayloadPath *> $null

    Pop-Location
}
