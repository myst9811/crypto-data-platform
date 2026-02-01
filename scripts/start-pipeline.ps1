# Crypto Data Platform - Pipeline Startup Script (Simplified)
# Run from project root: .\scripts\start-pipeline.ps1

param(
    [switch]$BuildImages,
    [switch]$Stop
)

$ErrorActionPreference = "Stop"

Write-Host ""
Write-Host "========================================" -ForegroundColor Cyan
Write-Host "  Crypto Data Platform Pipeline" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan
Write-Host ""

# Get script and project paths
$scriptPath = Split-Path -Parent $MyInvocation.MyCommand.Path
$projectPath = Split-Path -Parent $scriptPath
$dockerPath = Join-Path $projectPath "docker"

# Change to docker directory
Set-Location $dockerPath

# Handle stop command
if ($Stop) {
    Write-Host "Stopping all services..." -ForegroundColor Yellow
    docker-compose down
    Write-Host "All services stopped." -ForegroundColor Green
    Set-Location $projectPath
    exit 0
}

# Check Docker is running
Write-Host "Checking Docker status..." -ForegroundColor Yellow
try {
    $null = docker info 2>$null
    if ($LASTEXITCODE -ne 0) {
        throw "Docker not running"
    }
    Write-Host "Docker is running." -ForegroundColor Green
} catch {
    Write-Host "ERROR: Docker is not running!" -ForegroundColor Red
    Write-Host "Please start Docker Desktop and try again." -ForegroundColor Red
    Set-Location $projectPath
    exit 1
}

# Build images if requested
if ($BuildImages) {
    Write-Host ""
    Write-Host "Building Docker images..." -ForegroundColor Yellow
    docker-compose build --no-cache
    if ($LASTEXITCODE -ne 0) {
        Write-Host "ERROR: Failed to build images!" -ForegroundColor Red
        Set-Location $projectPath
        exit 1
    }
    Write-Host "Images built successfully." -ForegroundColor Green
}

# Step 1: Start Kafka Infrastructure
Write-Host ""
Write-Host "[Step 1/3] Starting Kafka infrastructure..." -ForegroundColor Yellow
docker-compose up -d zookeeper
Write-Host "Waiting for Zookeeper..."
Start-Sleep -Seconds 10

docker-compose up -d kafka kafka-ui
Write-Host "Waiting for Kafka to be healthy (this may take 30-60 seconds)..."
Start-Sleep -Seconds 40

# Step 2: Initialize Kafka Topics
Write-Host ""
Write-Host "[Step 2/3] Initializing Kafka topics..." -ForegroundColor Yellow
docker-compose run --rm kafka-init
Write-Host "Kafka topics created." -ForegroundColor Green

# Step 3: Start Producers
Write-Host ""
Write-Host "[Step 3/3] Starting exchange producers..." -ForegroundColor Yellow
docker-compose up -d binance-producer coinbase-producer kraken-producer
Write-Host "Producers started." -ForegroundColor Green

# Wait for producers to connect
Write-Host ""
Write-Host "Waiting for producers to connect to exchanges..."
Start-Sleep -Seconds 15

# Summary
Write-Host ""
Write-Host "========================================" -ForegroundColor Green
Write-Host "  Pipeline Started Successfully!" -ForegroundColor Green
Write-Host "========================================" -ForegroundColor Green
Write-Host ""
Write-Host "Access Points:" -ForegroundColor Cyan
Write-Host "  Kafka UI:  http://localhost:8080"
Write-Host ""
Write-Host "Verify data is flowing:" -ForegroundColor Cyan
Write-Host "  1. Open http://localhost:8080"
Write-Host "  2. Click on 'Topics'"
Write-Host "  3. Click on 'raw-trades' -> 'Messages'"
Write-Host "  4. You should see live crypto trade data!"
Write-Host ""
Write-Host "Commands:" -ForegroundColor Cyan
Write-Host "  View logs:     docker-compose logs -f binance-producer"
Write-Host "  Stop all:      .\scripts\start-pipeline.ps1 -Stop"
Write-Host "  Status:        docker-compose ps"
Write-Host ""

# Return to original directory
Set-Location $projectPath
