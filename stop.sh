#!/usr/bin/env bash
set -uo pipefail

PROJECT_ROOT="$(cd "$(dirname "$0")" && pwd)"
cd "$PROJECT_ROOT"

GREEN='\033[0;32m'
NC='\033[0m'
log() { echo -e "${GREEN}[+]${NC} $1"; }

# Stop Python processes
for pidfile in logs/spark.pid logs/api.pid logs/dashboard.pid; do
    if [ -f "$pidfile" ]; then
        PID=$(cat "$pidfile")
        NAME=$(basename "$pidfile" .pid)
        if kill -0 "$PID" 2>/dev/null; then
            kill "$PID" 2>/dev/null
            log "Stopped $NAME (PID: $PID)"
        fi
        rm -f "$pidfile"
    fi
done

# Stop Docker services
log "Stopping Docker services..."
docker compose -f docker/docker-compose.yml down 2>&1 | tail -3

log "All services stopped."
