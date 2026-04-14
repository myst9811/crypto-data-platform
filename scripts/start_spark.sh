#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

cd "$PROJECT_ROOT"

# Activate virtual environment
if [ -f ".venv/bin/activate" ]; then
    source .venv/bin/activate
else
    echo "ERROR: .venv not found. Run: python3 -m venv .venv && pip install -r requirements.txt"
    exit 1
fi

# Ensure Java is available
export JAVA_HOME="${JAVA_HOME:-/opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home}"
if [ ! -d "$JAVA_HOME" ]; then
    echo "ERROR: JAVA_HOME not found at $JAVA_HOME"
    echo "Install Java: brew install openjdk@17"
    exit 1
fi
export PATH="$JAVA_HOME/bin:$PATH"

# MPS fallback for Apple Silicon + PyTorch
export PYTORCH_ENABLE_MPS_FALLBACK=1

# Create log directory
mkdir -p logs

echo "Starting Spark Streaming Pipeline (local mode)..."
echo "Logs: logs/spark_streaming.log"

python -m src.processing.spark_streaming --config config/spark_config.yaml 2>&1 | tee logs/spark_streaming.log
