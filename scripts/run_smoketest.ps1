$ErrorActionPreference = "Stop"
$root = Split-Path -Parent $PSScriptRoot
Set-Location $root
. "$PSScriptRoot\set_env.ps1"
conda activate streaming_lab | Out-Null
python -m src.smoketest_kafka_console
