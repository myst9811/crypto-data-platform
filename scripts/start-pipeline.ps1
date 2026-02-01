# Crypto Data Platform - Pipeline Startup Script
# Run from project root: .\scripts\start-pipeline.ps1

param(
    [switch]$BuildImages,
    [switch]$SkipProducers,
    [switch]$SkipSpark,
    [switch]$SkipAirflow,
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
    exit 0
}

# Check Docker is running
Write-Host "Checking Docker status..." -ForegroundColor Yellow
$dockerInfo = docker info 2>&1
if ($LASTEXITCODE -ne 0) {
    Write-Host "ERROR: Docker is not running!" -ForegroundColor Red
    Write-Host "Please start Docker Desktop and try again." -ForegroundColor Red
    exit 1
}
Write-Host "Docker is running." -ForegroundColor Green

# Build images if requested
if ($BuildImages) {
    Write-Host ""
    Write-Host "Building Docker images..." -ForegroundColor Yellow
    docker-compose build
    if ($LASTEXITCODE -ne 0) {
        Write-Host "ERROR: Failed to build images!" -ForegroundColor Red
        exit 1
    }
    Write-Host "Images built successfully." -ForegroundColor Green
}

# Step 1: Start Infrastructure
Write-Host ""
Write-Host "[Step 1/5] Starting infrastructure (Zookeeper, Kafka, Spark)..." -ForegroundColor Yellow
docker-compose up -d zookeeper
Start-Sleep -Seconds 5

docker-compose up -d kafka kafka-ui
Write-Host "Waiting for Kafka to be healthy..."
Start-Sleep -Seconds 20

docker-compose up -d spark-master spark-worker
Write-Host "Infrastructure started." -ForegroundColor Green

# Step 2: Initialize Kafka Topics
Write-Host ""
Write-Host "[Step 2/5] Initializing Kafka topics..." -ForegroundColor Yellow
docker-compose run --rm kafka-init
if ($LASTEXITCODE -ne 0) {
    Write-Host "WARNING: Topic initialization may have failed. Continuing..." -ForegroundColor Yellow
}
Write-Host "Kafka topics ready." -ForegroundColor Green

# Step 3: Start Producers
if (-not $SkipProducers) {
    Write-Host ""
    Write-Host "[Step 3/5] Starting exchange producers..." -ForegroundColor Yellow
    docker-compose up -d binance-producer coinbase-producer kraken-producer
    Write-Host "Producers started. Waiting for connections..." -ForegroundColor Green
    Start-Sleep -Seconds 10
} else {
    Write-Host ""
    Write-Host "[Step 3/5] Skipping producers (--SkipProducers flag set)" -ForegroundColor Yellow
}

# Step 4: Start Spark Streaming
if (-not $SkipSpark) {
    Write-Host ""
    Write-Host "[Step 4/5] Starting Spark streaming application..." -ForegroundColor Yellow
    docker-compose up -d spark-streaming
    Write-Host "Spark streaming started." -ForegroundColor Green
} else {
    Write-Host ""
    Write-Host "[Step 4/5] Skipping Spark streaming (--SkipSpark flag set)" -ForegroundColor Yellow
}

# Step 5: Start Airflow (optional)
if (-not $SkipAirflow) {
    Write-Host ""
    Write-Host "[Step 5/5] Starting Airflow (monitoring)..." -ForegroundColor Yellow
    docker-compose up -d postgres
    Start-Sleep -Seconds 10
    docker-compose up -d airflow-webserver airflow-scheduler
    Write-Host "Airflow started." -ForegroundColor Green
} else {
    Write-Host ""
    Write-Host "[Step 5/5] Skipping Airflow (--SkipAirflow flag set)" -ForegroundColor Yellow
}

# Summary
Write-Host ""
Write-Host "========================================" -ForegroundColor Green
Write-Host "  Pipeline Started Successfully!" -ForegroundColor Green
Write-Host "========================================" -ForegroundColor Green
Write-Host ""
Write-Host "Access Points:" -ForegroundColor Cyan
Write-Host "  Kafka UI:       http://localhost:8080"
Write-Host "  Spark Master:   http://localhost:8081"
if (-not $SkipAirflow) {
    Write-Host "  Airflow:        http://localhost:8082 (admin/admin)"
}
Write-Host ""
Write-Host "Useful Commands:" -ForegroundColor Cyan
Write-Host "  View logs:      docker-compose logs -f [service-name]"
Write-Host "  Stop all:       .\scripts\start-pipeline.ps1 -Stop"
Write-Host "  Status:         docker-compose ps"
Write-Host ""
Write-Host "Check Kafka UI to verify messages are flowing!"
Write-Host ""

# Return to original directory
Set-Location $projectPath
