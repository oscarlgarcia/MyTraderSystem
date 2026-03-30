param(
    [switch] $Shell,
    [switch] $Persistent
)

# Runs tests inside the docker-compose service on Windows hosts.
# If -Shell is provided, open an interactive shell instead.

$composeCmd = "docker compose"

if ($Shell) {
    & $composeCmd exec app bash
    exit $LASTEXITCODE
}

if ($Persistent) {
    & $composeCmd exec app poetry run pytest
    exit $LASTEXITCODE
}

& $composeCmd run --rm app sh -c "poetry install && poetry run pytest"
exit $LASTEXITCODE
