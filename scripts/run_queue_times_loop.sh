#!/bin/bash
# run_queue_times_loop.sh - Queue-Times.com wait time fetcher loop
#
# Runs the fetcher in a loop: fetch, write to staging/queue_times, sleep, repeat.
# The scraper writes to staging only; the morning ETL merges yesterday's staging into fact_tables/clean.
# By default uses dimparkhours to only call the API when a park is in-window (open-90 to close+90 in park TZ).
#
# Usage:
#   ./scripts/run_queue_times_loop.sh
#   ./scripts/run_queue_times_loop.sh --interval 300 --output-base /path/to/output
#   
# Stop with Ctrl+C.

set -e

# Source common functions
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/common.sh"

# Defaults
INTERVAL_SECONDS=300
OUTPUT_BASE=""

# Parse arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --interval|-i)
            INTERVAL_SECONDS="$2"
            shift 2
            ;;
        --output-base|-o)
            OUTPUT_BASE="$2"
            shift 2
            ;;
        --help|-h)
            echo "Usage: $0 [--interval SECONDS] [--output-base PATH]"
            echo ""
            echo "Options:"
            echo "  --interval, -i     Seconds between fetches (default: 300)"
            echo "  --output-base, -o  Output directory (default: from config/config.json)"
            exit 0
            ;;
        *)
            echo "Unknown option: $1"
            exit 1
            ;;
    esac
done

# Setup
PROJECT_ROOT="$(get_project_root)"
PYTHON="$(get_python)"

if [[ -z "$OUTPUT_BASE" ]]; then
    OUTPUT_BASE="$(get_output_base "$PROJECT_ROOT")"
fi

cd "$PROJECT_ROOT"

log_info "Queue-Times loop: interval=${INTERVAL_SECONDS}s, output=$OUTPUT_BASE"
log_info "Stop with Ctrl+C."

# Run the fetcher with --interval (it handles the loop internally)
exec $PYTHON src/get_wait_times_from_queue_times.py \
    --output-base "$OUTPUT_BASE" \
    --interval "$INTERVAL_SECONDS"
