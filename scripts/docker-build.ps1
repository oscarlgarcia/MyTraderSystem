# Builds the docker-compose image on Windows hosts.

$composeCmd = "docker compose"
& $composeCmd build
exit $LASTEXITCODE
