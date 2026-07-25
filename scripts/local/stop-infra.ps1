$ErrorActionPreference = "Stop"

$repoRoot = Resolve-Path (Join-Path $PSScriptRoot "..\..")
$composeFile = Join-Path $repoRoot "compose.local.yaml"

Push-Location $repoRoot
try {
    & docker compose -f $composeFile down
    if ($LASTEXITCODE -ne 0) {
        throw "Failed to stop local infrastructure."
    }
}
finally {
    Pop-Location
}
