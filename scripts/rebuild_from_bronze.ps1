$ErrorActionPreference = "Stop"
$root = Split-Path -Parent $PSScriptRoot
Set-Location $root

Remove-Item -Recurse -Force .\out\_checkpoints\silver -ErrorAction SilentlyContinue
Remove-Item -Recurse -Force .\out\_checkpoints\gold  -ErrorAction SilentlyContinue

Write-Host "Now rerun: .\scripts\run_silver.ps1 then .\scripts\run_gold.ps1"
