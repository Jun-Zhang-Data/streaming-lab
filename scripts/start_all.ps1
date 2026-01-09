# scripts/start_all.ps1
# Starts producer + bronze + silver + gold from ONE terminal.
# Writes stdout/stderr to separate files in .\logs and stores PIDs in .\logs\<name>.pid
# Compatible with: Windows + conda + Spark 3.5.5 + HADOOP_HOME workaround.

$ErrorActionPreference = "Stop"

$root = Split-Path -Parent $PSScriptRoot
Set-Location $root

# Ensure Hadoop env for Spark-on-Windows in THIS session (also helpful for manual runs)
. "$PSScriptRoot\set_env.ps1"

# Start Docker (Redpanda + Kafka UI + topic)
& "$PSScriptRoot\up.ps1"

# Ensure logs folder exists
mkdir .\logs -Force | Out-Null

function Start-Runner($name, $commandLine) {
  $outLog  = Join-Path $root "logs\$name.out.log"
  $errLog  = Join-Path $root "logs\$name.err.log"
  $pidFile = Join-Path $root "logs\$name.pid"

  # Clear old logs so it's obvious what's from this run
  "" | Set-Content -Encoding UTF8 $outLog
  "" | Set-Content -Encoding UTF8 $errLog

  # This runs inside a NEW powershell.exe process.
  # We must initialize conda in that process via the conda hook.
  $cmd = @"
cd "$root"
& conda "shell.powershell" "hook" | Out-String | Invoke-Expression
conda activate streaming_lab | Out-Null

Write-Output "RUNNER STARTED: $name"
python -c "import sys; print('python exe:', sys.executable)"

$commandLine
"@

  $p = Start-Process -FilePath "powershell.exe" `
    -ArgumentList @("-NoProfile","-ExecutionPolicy","Bypass","-Command", $cmd) `
    -NoNewWindow `
    -RedirectStandardOutput $outLog `
    -RedirectStandardError  $errLog `
    -PassThru

  $p.Id | Set-Content -Encoding ASCII $pidFile

  Start-Sleep -Seconds 1
  if (-not (Get-Process -Id $p.Id -ErrorAction SilentlyContinue)) {
    Write-Host "❌ $name exited immediately. Check logs:"
    Write-Host "  stdout: $outLog"
    Write-Host "  stderr: $errLog"
  } else {
    Write-Host "✅ Started $name (PID=$($p.Id))"
    Write-Host "  stdout: $outLog"
    Write-Host "  stderr: $errLog"
  }
}

# Start components
Start-Runner "producer" "python -m src.producer"
Start-Runner "bronze"   "python -m src.jobs.bronze_ingest"
Start-Runner "silver"   "python -m src.jobs.silver_clean"
Start-Runner "gold"     "python -m src.jobs.gold_agg"

Write-Host ""
Write-Host "All start commands issued."
Write-Host "Follow logs (example):"
Write-Host "  Get-Content -Wait .\logs\gold.out.log"
Write-Host "  Get-Content -Wait .\logs\gold.err.log"
Write-Host ""
Write-Host "Stop all:"
Write-Host "  .\scripts\stop_all.ps1"



