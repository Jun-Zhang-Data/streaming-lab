$ErrorActionPreference = "Stop"
$root = Split-Path -Parent $PSScriptRoot
Set-Location $root

$schemaPath = ".\contracts\clicks-value.schema.json"
$schemaJson = Get-Content $schemaPath -Raw

# Confluent-compatible endpoint: /subjects/<subject>/versions :contentReference[oaicite:3]{index=3}
$body = @{
  schemaType = "JSON"
  schema     = $schemaJson
} | ConvertTo-Json -Compress

$subject = "clicks-value"
$uri = "http://localhost:8082/subjects/$subject/versions"

Invoke-RestMethod -Method Post -Uri $uri -ContentType "application/vnd.schemaregistry.v1+json" -Body $body
Write-Host "Registered schema subject: $subject"
