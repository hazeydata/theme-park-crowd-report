#!/bin/bash
# run_daily_pipeline.sh - Master script: run full pipeline in order (daily)
#
# Order: ETL → Dimensions → Posted Aggregates → Report → Training → Forecast → WTI
#
# Usage:
#   ./scripts/run_daily_pipeline.sh
#   ./scripts/run_daily_pipeline.sh --output-base /path/to/output
#   ./scripts/run_daily_pipeline.sh --no-stop-on-error   # continue on step failure, log and exit with 1 at end
#   ./scripts/run_daily_pipeline.sh --skip-etl --skip-training
#
# For cron: use one job that runs this script (e.g. 6:00 AM ET after network is up).

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/common.sh"

# Options
OUTPUT_BASE=""
STOP_ON_ERROR=true
SKIP_ETL=false
SKIP_DIMENSIONS=false
SKIP_AGGREGATES=false
SKIP_REPORT=false
SKIP_TRAINING=false
SKIP_FORECAST=false
SKIP_WTI=false

while [[ $# -gt 0 ]]; do
    case $1 in
        --output-base|-o)
            OUTPUT_BASE="$2"
            shift 2
            ;;
        --no-stop-on-error)
            STOP_ON_ERROR=false
            shift
            ;;
        --skip-etl)
            SKIP_ETL=true
            shift
            ;;
        --skip-dimensions)
            SKIP_DIMENSIONS=true
            shift
            ;;
        --skip-aggregates)
            SKIP_AGGREGATES=true
            shift
            ;;
        --skip-report)
            SKIP_REPORT=true
            shift
            ;;
        --skip-training)
            SKIP_TRAINING=true
            shift
            ;;
        --skip-forecast)
            SKIP_FORECAST=true
            shift
            ;;
        --skip-wti)
            SKIP_WTI=true
            shift
            ;;
        --help|-h)
            echo "Usage: $0 [OPTIONS]"
            echo ""
            echo "Runs full pipeline in order: ETL → Dimensions → Posted Aggregates → Report → Training → Forecast → WTI"
            echo ""
            echo "Options:"
            echo "  --output-base PATH    Output base (default: from config/config.json)"
            echo "  --no-stop-on-error    Continue on step failure; log and exit 1 at end"
            echo "  --skip-etl             Skip main ETL"
            echo "  --skip-dimensions      Skip dimension fetches"
            echo "  --skip-aggregates      Skip posted aggregates build"
            echo "  --skip-report           Skip wait time DB report"
            echo "  --skip-training        Skip batch training"
            echo "  --skip-forecast        Skip forecast generation"
            echo "  --skip-wti             Skip WTI calculation"
            exit 0
            ;;
        *)
            echo "Unknown option: $1" >&2
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

# Pipeline status file for dashboard
$PYTHON scripts/update_pipeline_status.py --output-base "$OUTPUT_BASE" pipeline-start 2>/dev/null || true

# Single daily log (append so multiple runs same day accumulate)
LOG_FILE="$OUTPUT_BASE/logs/daily_pipeline_$(date '+%Y-%m-%d').log"
echo "[$(date '+%Y-%m-%d %H:%M:%S')] Daily pipeline started. Output base: $OUTPUT_BASE" >> "$LOG_FILE"
exec > >(tee -a "$LOG_FILE") 2>&1

run_step() {
    local name="$1"
    shift
    local cmd=("$@")
    log_info "=== $name ==="
    if "${cmd[@]}"; then
        log_info "Done: $name"
        return 0
    else
        log_error "Failed: $name"
        return 1
    fi
}

run_step_optional() {
    local name="$1"
    shift
    if run_step "$name" "$@"; then
        return 0
    fi
    if $STOP_ON_ERROR; then
        log_error "Stopping on first failure (use --no-stop-on-error to continue)"
        exit 1
    fi
    return 1
}

FAILED_ANY=false

# 1. ETL (incremental)
if $SKIP_ETL; then
    log_info "=== ETL (skipped) ==="
    $PYTHON scripts/update_pipeline_status.py --output-base "$OUTPUT_BASE" step etl done 2>/dev/null || true
else
    if run_step "ETL (incremental)" "$SCRIPT_DIR/run_etl.sh" --output-base "$OUTPUT_BASE"; then
        $PYTHON scripts/update_pipeline_status.py --output-base "$OUTPUT_BASE" step etl done 2>/dev/null || true
    else
        FAILED_ANY=true
        $PYTHON scripts/update_pipeline_status.py --output-base "$OUTPUT_BASE" step etl failed 2>/dev/null || true
        $STOP_ON_ERROR && exit 1
    fi
fi

# 2. Dimension fetches
if $SKIP_DIMENSIONS; then
    log_info "=== Dimension fetches (skipped) ==="
    $PYTHON scripts/update_pipeline_status.py --output-base "$OUTPUT_BASE" step dimensions done 2>/dev/null || true
else
    if run_step "Dimension fetches" "$SCRIPT_DIR/run_dimension_fetches.sh" --output-base "$OUTPUT_BASE"; then
        $PYTHON scripts/update_pipeline_status.py --output-base "$OUTPUT_BASE" step dimensions done 2>/dev/null || true
    else
        FAILED_ANY=true
        $PYTHON scripts/update_pipeline_status.py --output-base "$OUTPUT_BASE" step dimensions failed 2>/dev/null || true
        $STOP_ON_ERROR && exit 1
    fi
fi

# 3. Posted aggregates
if $SKIP_AGGREGATES; then
    log_info "=== Posted aggregates (skipped) ==="
    $PYTHON scripts/update_pipeline_status.py --output-base "$OUTPUT_BASE" step aggregates done 2>/dev/null || true
else
    if run_step "Posted aggregates" $PYTHON scripts/build_posted_aggregates.py --output-base "$OUTPUT_BASE"; then
        $PYTHON scripts/update_pipeline_status.py --output-base "$OUTPUT_BASE" step aggregates done 2>/dev/null || true
    else
        FAILED_ANY=true
        $PYTHON scripts/update_pipeline_status.py --output-base "$OUTPUT_BASE" step aggregates failed 2>/dev/null || true
        $STOP_ON_ERROR && exit 1
    fi
fi

# 4. Wait time DB report
if $SKIP_REPORT; then
    log_info "=== Wait time DB report (skipped) ==="
    $PYTHON scripts/update_pipeline_status.py --output-base "$OUTPUT_BASE" step report done 2>/dev/null || true
else
    if run_step "Wait time DB report" $PYTHON scripts/report_wait_time_db.py --quick --lookback-days 14 --output-base "$OUTPUT_BASE"; then
        $PYTHON scripts/update_pipeline_status.py --output-base "$OUTPUT_BASE" step report done 2>/dev/null || true
    else
        FAILED_ANY=true
        $PYTHON scripts/update_pipeline_status.py --output-base "$OUTPUT_BASE" step report failed 2>/dev/null || true
        $STOP_ON_ERROR && exit 1
    fi
fi

# 5. Batch training (train_batch_entities.py updates status file for entities)
if $SKIP_TRAINING; then
    log_info "=== Batch training (skipped) ==="
    $PYTHON scripts/update_pipeline_status.py --output-base "$OUTPUT_BASE" step training done 2>/dev/null || true
else
    if run_step "Batch training" $PYTHON scripts/train_batch_entities.py --min-age-hours 24 --output-base "$OUTPUT_BASE"; then
        $PYTHON scripts/update_pipeline_status.py --output-base "$OUTPUT_BASE" step training done 2>/dev/null || true
    else
        FAILED_ANY=true
        $PYTHON scripts/update_pipeline_status.py --output-base "$OUTPUT_BASE" step training failed 2>/dev/null || true
        $STOP_ON_ERROR && exit 1
    fi
fi

# 6. Forecast
if $SKIP_FORECAST; then
    log_info "=== Forecast (skipped) ==="
    $PYTHON scripts/update_pipeline_status.py --output-base "$OUTPUT_BASE" step forecast done 2>/dev/null || true
else
    if run_step "Forecast" $PYTHON scripts/generate_forecast.py --output-base "$OUTPUT_BASE"; then
        $PYTHON scripts/update_pipeline_status.py --output-base "$OUTPUT_BASE" step forecast done 2>/dev/null || true
    else
        FAILED_ANY=true
        $PYTHON scripts/update_pipeline_status.py --output-base "$OUTPUT_BASE" step forecast failed 2>/dev/null || true
        $STOP_ON_ERROR && exit 1
    fi
fi

# 7. WTI
if $SKIP_WTI; then
    log_info "=== WTI (skipped) ==="
    $PYTHON scripts/update_pipeline_status.py --output-base "$OUTPUT_BASE" step wti done 2>/dev/null || true
else
    if run_step "WTI" $PYTHON scripts/calculate_wti.py --output-base "$OUTPUT_BASE"; then
        $PYTHON scripts/update_pipeline_status.py --output-base "$OUTPUT_BASE" step wti done 2>/dev/null || true
    else
        FAILED_ANY=true
        $PYTHON scripts/update_pipeline_status.py --output-base "$OUTPUT_BASE" step wti failed 2>/dev/null || true
        $STOP_ON_ERROR && exit 1
    fi
fi

if $FAILED_ANY; then
    log_error "Daily pipeline finished with one or more failures. Check log: $LOG_FILE"
    exit 1
fi

log_info "Daily pipeline completed successfully. Log: $LOG_FILE"
exit 0
