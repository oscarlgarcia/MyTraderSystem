param(
    [switch] $Down
)

$composeCmd = "docker compose"

if ($Down) {
    & $composeCmd down
    exit $LASTEXITCODE
}

& $composeCmd up -d app
exit $LASTEXITCODE
