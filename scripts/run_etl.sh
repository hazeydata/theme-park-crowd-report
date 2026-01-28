#!/bin/bash
# run_etl.sh - Main ETL script wrapper
#
# Runs the S3 wait time ETL. Use for cron jobs or manual runs.
#
# Usage:
#   ./scripts/run_etl.sh
#   ./scripts/run_etl.sh --output-base /path/to/output
#   ./scripts/run_etl.sh --full-rebuild

set -e

# Source common functions
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/common.sh"

# Parse arguments
OUTPUT_BASE=""
EXTRA_ARGS=()

while [[ $# -gt 0 ]]; do
    case $1 in
        --output-base|-o)
            OUTPUT_BASE="$2"
            shift 2
            ;;
        --full-rebuild)
            EXTRA_ARGS+=("--full-rebuild")
            shift
            ;;
        --help|-h)
            echo "Usage: $0 [--output-base PATH] [--full-rebuild]"
            exit 0
            ;;
        *)
            EXTRA_ARGS+=("$1")
            shift
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
ensure_logs_dir "$OUTPUT_BASE"

log_info "Starting ETL. Output: $OUTPUT_BASE"

exec $PYTHON src/get_tp_wait_time_data_from_s3.py \
    --output-base "$OUTPUT_BASE" \
    "${EXTRA_ARGS[@]}"
