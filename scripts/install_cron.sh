#!/bin/bash
# install_cron.sh - Install cron jobs for the Theme Park pipeline
#
# Sets up scheduled tasks equivalent to the Windows Task Scheduler (daily tasks only):
#   - 5:00 AM ET: Main ETL (incremental)
#   - 5:30 AM ET: Wait time DB report
#   - 6:00 AM ET: Dimension fetches (entity, park hours, events, metatable + build dimdategroupid, dimseason)
#   - 7:00 AM ET: Secondary ETL (backup run)
#   - 8:00 AM ET: Batch training (entities needing modeling)
#
# Weekly tasks (Sunday) are skipped - will be set up on Mac mini next week.
#
# Note: Times are in system timezone. Set TZ=America/New_York if needed.
# For queue-times loop (continuous 5-min fetches), use systemd service or run manually.
#
# Usage:
#   ./scripts/install_cron.sh              # Install cron jobs
#   ./scripts/install_cron.sh --remove     # Remove cron jobs
#   ./scripts/install_cron.sh --show       # Show what would be installed

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/common.sh"

PROJECT_ROOT="$(get_project_root)"
OUTPUT_BASE="$(get_output_base "$PROJECT_ROOT")"
PYTHON="$(get_python)"
LOGS_DIR="$OUTPUT_BASE/logs"

# Marker for our cron entries
CRON_MARKER="# theme-park-crowd-report"

# Build cron entries (five separate jobs)
generate_cron_entries() {
    cat << EOF
# Theme Park Crowd Report Pipeline - Scheduled Tasks
# Installed by install_cron.sh - DO NOT EDIT MANUALLY
# Times are Eastern (system timezone should be America/New_York, or adjust times below)
$CRON_MARKER

# 5:00 AM Eastern - Main ETL (incremental)
0 5 * * * export PATH="\$HOME/.local/bin:\$PATH" && cd $PROJECT_ROOT && $SCRIPT_DIR/run_etl.sh >> "$LOGS_DIR/cron_etl_5am.log" 2>&1 $CRON_MARKER

# 5:30 AM Eastern - Wait time DB report
30 5 * * * export PATH="\$HOME/.local/bin:\$PATH" && cd $PROJECT_ROOT && $PYTHON scripts/report_wait_time_db.py --quick --lookback-days 14 >> "$LOGS_DIR/cron_report_530am.log" 2>&1 $CRON_MARKER

# 6:00 AM Eastern - Dimension fetches (entity, park hours, events, metatable + build dimdategroupid, dimseason)
0 6 * * * export PATH="\$HOME/.local/bin:\$PATH" && cd $PROJECT_ROOT && $SCRIPT_DIR/run_dimension_fetches.sh >> "$LOGS_DIR/cron_dimensions_6am.log" 2>&1 $CRON_MARKER

# 7:00 AM Eastern - Secondary ETL (backup run)
0 7 * * * export PATH="\$HOME/.local/bin:\$PATH" && cd $PROJECT_ROOT && $SCRIPT_DIR/run_etl.sh >> "$LOGS_DIR/cron_etl_7am.log" 2>&1 $CRON_MARKER

# 8:00 AM Eastern - Batch training (entities needing modeling)
0 8 * * * export PATH="\$HOME/.local/bin:\$PATH" && cd $PROJECT_ROOT && $PYTHON scripts/train_batch_entities.py --min-age-hours 24 >> "$LOGS_DIR/cron_training_8am.log" 2>&1 $CRON_MARKER

# Weekly tasks (Sunday) skipped - will be set up on Mac mini next week:
#   - Sunday 6:30 AM: Posted accuracy report
#   - Sunday 7:00 AM: Log cleanup

EOF
}

# Build cron entries (single daily master script: ETL → dimensions → aggregates → report → training → forecast → WTI)
generate_daily_master_entries() {
    cat << EOF
# Theme Park Crowd Report Pipeline - Daily Master (single run)
# Installed by install_cron.sh --daily-master - DO NOT EDIT MANUALLY
# Runs: ETL → Dimensions → Posted Aggregates → Report → Training → Forecast → WTI
$CRON_MARKER

# 6:00 AM Eastern - Full daily pipeline (run_daily_pipeline.sh; script also tees to this log)
0 6 * * * export PATH="\$HOME/.local/bin:\$PATH" && cd $PROJECT_ROOT && $SCRIPT_DIR/run_daily_pipeline.sh >> "$LOGS_DIR/daily_pipeline_\$(date +\\%Y-\\%m-\\%d).log" 2>&1 $CRON_MARKER

EOF
}

# Show what would be installed
show_cron() {
    local use_master="${1:-}"
    echo "=== Cron entries to be installed ==="
    echo ""
    if [[ "$use_master" == "--daily-master" ]]; then
        generate_daily_master_entries
        echo "(Single daily run: run_daily_pipeline.sh at 6:00 AM Eastern)"
    else
        generate_cron_entries
    fi
    echo ""
    echo "Project root: $PROJECT_ROOT"
    echo "Output base: $OUTPUT_BASE"
    echo "Python: $PYTHON"
}

# Remove our cron entries
remove_cron() {
    echo "Removing theme-park-crowd-report cron entries..."
    crontab -l 2>/dev/null | grep -v "$CRON_MARKER" | crontab - || true
    echo "Done. Removed all entries with marker: $CRON_MARKER"
}

# Install cron entries
install_cron() {
    local use_master="${1:-}"
    echo "Installing theme-park-crowd-report cron entries..."
    
    # Create logs directory (may fail if output_base not mounted, e.g. Dropbox on another machine)
    if ! mkdir -p "$LOGS_DIR" 2>/dev/null; then
        echo "Note: Could not create logs dir ($LOGS_DIR); cron will still be installed."
    fi
    
    # Remove old entries first, then add new ones
    if [[ "$use_master" == "--daily-master" ]]; then
        (crontab -l 2>/dev/null | grep -v "$CRON_MARKER" || true; generate_daily_master_entries) | crontab -
        echo "(Single daily pipeline at 6:00 AM Eastern)"
    else
        (crontab -l 2>/dev/null | grep -v "$CRON_MARKER" || true; generate_cron_entries) | crontab -
    fi
    
    echo ""
    echo "Done! Cron jobs installed."
    echo ""
    echo "View with: crontab -l"
    echo "Remove with: $0 --remove"
    echo ""
    echo "IMPORTANT: Times are in system timezone."
    echo "If your server isn't Eastern time, either:"
    echo "  1. Set system timezone: sudo timedatectl set-timezone America/New_York"
    echo "  2. Or prefix commands with TZ=America/New_York"
}

# Main
case "${1:-}" in
    --show)
        show_cron "$2"
        ;;
    --remove)
        remove_cron
        ;;
    --help|-h)
        echo "Usage: $0 [--show [--daily-master]|--remove|--daily-master|--help]"
        echo ""
        echo "Options:"
        echo "  (none)         Install five separate cron jobs (5am ETL, 5:30 report, 6am dimensions, 7am ETL, 8am training)"
        echo "  --daily-master Install single daily pipeline at 6:00 AM (run_daily_pipeline.sh: ETL → dimensions → aggregates → report → training → forecast → WTI)"
        echo "  --show         Show what would be installed (add --daily-master for master script)"
        echo "  --remove      Remove installed cron jobs"
        exit 0
        ;;
    --daily-master)
        install_cron "--daily-master"
        ;;
    "")
        install_cron
        ;;
    *)
        echo "Unknown option: $1"
        exit 1
        ;;
esac
