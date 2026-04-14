#!/usr/bin/env bash
set -euo pipefail

# ─── Crypto Data Platform — One-command startup ───
# Usage: ./start.sh [--no-producers] [--no-spark] [--no-api] [--no-dashboard]

PROJECT_ROOT="$(cd "$(dirname "$0")" && pwd)"
cd "$PROJECT_ROOT"

# ─── Parse flags ───
START_PRODUCERS=true
START_SPARK=true
START_API=true
START_DASHBOARD=true

for arg in "$@"; do
    case $arg in
        --no-producers)  START_PRODUCERS=false ;;
        --no-spark)      START_SPARK=false ;;
        --no-api)        START_API=false ;;
        --no-dashboard)  START_DASHBOARD=false ;;
    esac
done

# ─── Colors ───
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
NC='\033[0m'

log()  { echo -e "${GREEN}[+]${NC} $1"; }
warn() { echo -e "${YELLOW}[!]${NC} $1"; }
info() { echo -e "${CYAN}[i]${NC} $1"; }

# ─── Pre-flight checks ───
log "Running pre-flight checks..."

# Java
export JAVA_HOME="${JAVA_HOME:-/opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home}"
if [ ! -d "$JAVA_HOME" ]; then
    warn "Java not found. Install: brew install openjdk@17"
    exit 1
fi
export PATH="$JAVA_HOME/bin:$PATH"
info "Java: $(java -version 2>&1 | head -1)"

# Python venv
if [ ! -f ".venv/bin/activate" ]; then
    warn "No .venv found. Creating..."
    python3.12 -m venv .venv
    source .venv/bin/activate
    pip install -r requirements.txt
else
    source .venv/bin/activate
fi
info "Python: $(python --version)"

# Docker
if ! docker info > /dev/null 2>&1; then
    warn "Docker is not running. Start Docker Desktop first."
    exit 1
fi
info "Docker: running"

# PyTorch MPS fallback (Apple Silicon)
export PYTORCH_ENABLE_MPS_FALLBACK=1

# Create dirs
mkdir -p logs data/bronze data/silver data/gold data/checkpoints data/features

# ─── Step 1: Kafka infrastructure ───
log "Starting Kafka infrastructure..."
docker compose -f docker/docker-compose.yml up -d zookeeper kafka kafka-ui 2>&1 | tail -3

# Wait for Kafka health
info "Waiting for Kafka to be healthy..."
for i in $(seq 1 30); do
    if docker compose -f docker/docker-compose.yml ps kafka 2>/dev/null | grep -q "healthy"; then
        break
    fi
    sleep 2
done

# Init topics (idempotent)
docker compose -f docker/docker-compose.yml run --rm kafka-init 2>&1 | tail -5
log "Kafka ready on localhost:9092"

# ─── Step 2: Producers ───
if [ "$START_PRODUCERS" = true ]; then
    log "Starting exchange producers..."
    docker compose -f docker/docker-compose.yml up -d binance-producer coinbase-producer kraken-producer 2>&1 | tail -3
    log "Producers started (Binance, Coinbase, Kraken)"
else
    warn "Skipping producers (--no-producers)"
fi

# ─── Step 3: Spark Streaming ───
if [ "$START_SPARK" = true ]; then
    log "Starting Spark streaming pipeline (background)..."
    nohup python -m src.processing.spark_streaming --config config/spark_config.yaml \
        > logs/spark_streaming.log 2>&1 &
    SPARK_PID=$!
    echo "$SPARK_PID" > logs/spark.pid
    log "Spark streaming started (PID: $SPARK_PID, log: logs/spark_streaming.log)"
else
    warn "Skipping Spark (--no-spark)"
fi

# ─── Step 4: FastAPI ───
if [ "$START_API" = true ]; then
    log "Starting FastAPI server (background)..."
    nohup uvicorn src.serving.api.main:app --host 0.0.0.0 --port 8000 \
        > logs/api.log 2>&1 &
    API_PID=$!
    echo "$API_PID" > logs/api.pid
    log "API started (PID: $API_PID, http://localhost:8000/api/v1/docs)"
else
    warn "Skipping API (--no-api)"
fi

# ─── Step 5: Streamlit Dashboard ───
if [ "$START_DASHBOARD" = true ]; then
    log "Starting Streamlit dashboard (background)..."
    nohup streamlit run src/serving/dashboard/app.py \
        --server.port 8501 --server.headless true \
        > logs/dashboard.log 2>&1 &
    DASH_PID=$!
    echo "$DASH_PID" > logs/dashboard.pid
    log "Dashboard started (PID: $DASH_PID, http://localhost:8501)"
else
    warn "Skipping dashboard (--no-dashboard)"
fi

# ─── Summary ───
echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
log "All services started!"
echo ""
info "Kafka UI:    http://localhost:8080"
info "API docs:    http://localhost:8000/api/v1/docs"
info "Dashboard:   http://localhost:8501"
echo ""
info "Logs:        logs/*.log"
info "Stop all:    ./stop.sh"
echo ""
info "After data accumulates (~2-3 min), train ML models:"
echo "  python -m ml.features.feature_extractor"
echo "  python -m ml.training.train_xgboost"
echo "  python -m ml.training.train_isolation_forest"
echo "  python -m ml.training.train_garch"
echo "  python -m ml.training.train_lstm"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
