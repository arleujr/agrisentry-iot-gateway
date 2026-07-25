$ErrorActionPreference = "Stop"

$repoRoot = Resolve-Path (Join-Path $PSScriptRoot "..\..")
Push-Location $repoRoot

try {
    $env:DATABASE_URL = "postgres://agrisentry_local:local_only_change_me@127.0.0.1:5432/agrisentry_local"

    if ($env:AGRISENTRY_MQTT_BIND_IP) {
        $env:MQTT_HOST = $env:AGRISENTRY_MQTT_BIND_IP
    }
    else {
        $env:MQTT_HOST = "127.0.0.1"
    }

    $env:MQTT_PORT = "1883"
    $env:MQTT_TLS = "false"
    $env:MQTT_USER = ""
    $env:MQTT_PASS = ""
    $env:MQTT_BUFFER_SIZE = "100"
    $env:MQTT_ENABLE_LEGACY = "false"
    $env:ANALYSIS_WORKER_ENABLED = "false"
    $env:PORT = "8080"
    $env:RUST_LOG = "info,agrisentry_iot_gateway=debug"

    Write-Host "Starting AgriSentry gateway with local infrastructure..." -ForegroundColor Cyan
    Write-Host "MQTT broker: $env:MQTT_HOST`:$env:MQTT_PORT" -ForegroundColor DarkGray
    Write-Host "Keep this terminal open. Stop with Ctrl+C." -ForegroundColor DarkGray

    cargo run

    if ($LASTEXITCODE -ne 0) {
        throw "The gateway exited with code $LASTEXITCODE."
    }
}
finally {
    Pop-Location
}