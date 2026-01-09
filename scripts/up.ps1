$ErrorActionPreference = "Stop"
$root = Split-Path -Parent $PSScriptRoot
Set-Location $root

docker compose up -d
Start-Sleep -Seconds 2

$container = docker ps -q --filter "name=redpanda"
if ($container) {
  # create topic (ignore errors if exists)
  docker exec -it $container rpk topic create clicks -p 3
  docker exec -it $container rpk topic describe clicks
}

Write-Host "Kafka UI: http://localhost:8081"
