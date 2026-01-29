#!/bin/bash
# Monitor and start forecast generation when posted aggregates are ready

set -e

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$PROJECT_ROOT"

# Get output base using Python (more reliable)
export PATH="$HOME/.local/bin:$PATH"
export PYTHONPATH="${PROJECT_ROOT}/src:$PYTHONPATH"
OUTPUT_BASE=$(python3 -c "from utils.paths import get_output_base; print(get_output_base())")
POSTED_AGG="${OUTPUT_BASE}/posted_aggregates.parquet"
LOG_FILE="/tmp/start_forecast_when_ready.log"

echo "$(date): Monitoring for posted aggregates..." | tee -a "$LOG_FILE"
echo "  Looking for: $POSTED_AGG" | tee -a "$LOG_FILE"

# Wait for posted aggregates (max 2 hours)
MAX_WAIT=7200
START_TIME=$(date +%s)
CHECK_INTERVAL=30

while [ ! -f "$POSTED_AGG" ]; do
    ELAPSED=$(($(date +%s) - START_TIME))
    
    if [ $ELAPSED -ge $MAX_WAIT ]; then
        echo "$(date): Timeout waiting for posted aggregates (${MAX_WAIT}s)" | tee -a "$LOG_FILE"
        exit 1
    fi
    
    if [ $((ELAPSED % 60)) -eq 0 ]; then
        echo "$(date): Still waiting... (${ELAPSED}s elapsed)" | tee -a "$LOG_FILE"
    fi
    
    sleep $CHECK_INTERVAL
done

echo "$(date): ✅ Posted aggregates ready!" | tee -a "$LOG_FILE"
echo "  File: $POSTED_AGG" | tee -a "$LOG_FILE"
ls -lh "$POSTED_AGG" | tee -a "$LOG_FILE"

# Check if forecast is already running
if pgrep -f "generate_forecast.py" > /dev/null; then
    echo "$(date): ⚠️  Forecast generation already running, skipping" | tee -a "$LOG_FILE"
    exit 0
fi

# Start forecast generation
echo "$(date): Starting forecast generation..." | tee -a "$LOG_FILE"

export PATH="$HOME/.local/bin:$PATH"
export PYTHONPATH="${PROJECT_ROOT}/src:$PYTHONPATH"

nohup python3 scripts/generate_forecast.py > /tmp/generate_forecast.log 2>&1 &
FORECAST_PID=$!

echo "$(date): ✅ Forecast generation started" | tee -a "$LOG_FILE"
echo "  PID: $FORECAST_PID" | tee -a "$LOG_FILE"
echo "  Log: /tmp/generate_forecast.log" | tee -a "$LOG_FILE"
