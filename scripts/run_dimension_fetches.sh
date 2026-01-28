#!/bin/bash
# run_dimension_fetches.sh - Fetch and build all dimension tables
#
# Fetches from S3: entity, park hours, events, metatable
# Builds locally: dimdategroupid, dimseason
#
# Usage:
#   ./scripts/run_dimension_fetches.sh
#   ./scripts/run_dimension_fetches.sh --output-base /path/to/output

set -e

# Source common functions
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/common.sh"

# Parse arguments
OUTPUT_BASE=""
while [[ $# -gt 0 ]]; do
    case $1 in
        --output-base|-o)
            OUTPUT_BASE="$2"
            shift 2
            ;;
        --help|-h)
            echo "Usage: $0 [--output-base PATH]"
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
ensure_logs_dir "$OUTPUT_BASE"

log_info "Starting dimension fetches. Output: $OUTPUT_BASE"

# Scripts to run in order
SCRIPTS=(
    "src/get_entity_table_from_s3.py"
    "src/get_park_hours_from_s3.py"
    "src/get_events_from_s3.py"
    "src/get_metatable_from_s3.py"
    "src/build_dimdategroupid.py"
    "src/build_dimseason.py"
)

for script in "${SCRIPTS[@]}"; do
    log_info "Running $script ..."
    if ! $PYTHON "$script" --output-base "$OUTPUT_BASE"; then
        log_error "Dimension step failed: $script"
        exit 1
    fi
done

log_info "All dimension fetches and builds completed."
