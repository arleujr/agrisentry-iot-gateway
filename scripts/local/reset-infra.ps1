$ErrorActionPreference = "Stop"

$repoRoot = Resolve-Path (Join-Path $PSScriptRoot "..\..")
$composeFile = Join-Path $repoRoot "compose.local.yaml"

Push-Location $repoRoot
try {
    Write-Warning "This removes the local AgriSentry database volume and all local test data."
    & docker compose -f $composeFile down -v --remove-orphans
    if ($LASTEXITCODE -ne 0) {
        throw "Failed to reset local infrastructure."
    }

    Write-Host "Local infrastructure and test data were removed." -ForegroundColor Green
}
finally {
    Pop-Location
}
