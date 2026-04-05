param(
    [switch]$Build,
    [string]$PytestArgs = "tests/ops/test_readiness_orchestrator.py tests/ops/test_ingestion_validation.py tests/ops/test_release_gates.py tests/ops/test_live_cutover.py tests/ops/test_operational_claims.py -q"
)

$ErrorActionPreference = "Stop"
$repoRoot = Split-Path -Parent $PSScriptRoot

Push-Location $repoRoot
try {
    if ($Build) {
        docker compose build app
    }
    docker compose up -d app
    docker compose exec app sh -lc "poetry install"
    docker compose exec app sh -lc "poetry run pytest $PytestArgs"
}
finally {
    Pop-Location
}
