# scripts/stop_all.ps1
$ErrorActionPreference = "Continue"

$root = Split-Path -Parent $PSScriptRoot
Set-Location $root

function Stop-Runner($name) {
  $pidfile = ".\logs\$name.pid"
  if (Test-Path $pidfile) {
    $pidValue = Get-Content $pidfile | Select-Object -First 1
    if ($pidValue) {
      Write-Host "Stopping $name (PID=$pidValue)"
      Stop-Process -Id $pidValue -Force -ErrorAction SilentlyContinue
    }
    Remove-Item $pidfile -Force -ErrorAction SilentlyContinue
  } else {
    Write-Host "No pid file for $name"
  }
}

Stop-Runner "gold"
Stop-Runner "silver"
Stop-Runner "bronze"
Stop-Runner "producer"

& "$PSScriptRoot\down.ps1"
Write-Host "Stopped all."

