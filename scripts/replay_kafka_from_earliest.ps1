$ErrorActionPreference = "Stop"
$root = Split-Path -Parent $PSScriptRoot
Set-Location $root

# Stop jobs yourself (Ctrl+C) before running this.

# Wipe Bronze checkpoint so Kafka offsets reset for Bronze
Remove-Item -Recurse -Force .\out\_checkpoints\bronze\clicks_parquet -ErrorAction SilentlyContinue

# Set env so Bronze reads from earliest
$env:KAFKA_STARTING_OFFSETS = "earliest"

Write-Host "Replaying from earliest offsets. Now run: .\scripts\run_bronze.ps1"
