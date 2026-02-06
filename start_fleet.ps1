# Fleet Node Startup Script
# Sets all required environment variables and launches fleet_node.py
# IMPORTANT: Replace YOUR_BUCKET_NAME and your/repo.git before use!

Write-Host "=== Fleet Node Startup ===" -ForegroundColor Cyan

# AWS Resources
$env:FLEET_REGION = "ap-northeast-1"
$env:FLEET_SQS_NAME = "fleet-missions"
$env:FLEET_STATE_TABLE = "fleet-mission-state"
$env:FLEET_BUDGET_TABLE = "fleet-budget"
$env:FLEET_S3_BUCKET = "YOUR_BUCKET_NAME"  # Replace with your bucket name
$env:REPO_URL = "https://github.com/your/repo.git"  # Replace with your repo

# Bedrock Model IDs (using standard model IDs for ap-northeast-1)
$env:BEDROCK_SONNET_MODEL_ID = "anthropic.claude-3-5-sonnet-20240620-v1:0"
$env:BEDROCK_OPUS_MODEL_ID = "anthropic.claude-3-opus-20240229-v1:0"
$env:BEDROCK_MICRO_MODEL_ID = "amazon.nova-micro-v1:0"
$env:BEDROCK_LITE_MODEL_ID = "amazon.nova-lite-v1:0"
$env:BEDROCK_MISTRAL_MODEL_ID = "mistral.mistral-7b-instruct-v0:2"

Write-Host "Environment variables set:" -ForegroundColor Green
Write-Host "  Region: $env:FLEET_REGION"
Write-Host "  SQS: $env:FLEET_SQS_NAME"
Write-Host "  S3: $env:FLEET_S3_BUCKET"
Write-Host "  Models: Sonnet, Opus, Micro, Lite, Mistral"
Write-Host ""

Write-Host "Starting fleet_node.py..." -ForegroundColor Yellow
python fleet_node.py
