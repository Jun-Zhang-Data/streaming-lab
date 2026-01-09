$ErrorActionPreference = "Stop"
$root = Split-Path -Parent $PSScriptRoot
Set-Location $root
conda activate streaming_lab | Out-Null
python -m src.producer
