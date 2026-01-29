#!/bin/bash
# install_cron.sh - Install cron jobs for the Theme Park pipeline
#
# Sets up scheduled tasks equivalent to the Windows Task Scheduler (daily tasks only):
#   - 5:00 AM ET: Main ETL (incremental)
#   - 5:30 AM ET: Wait time DB report
#   - 6:00 AM ET: Dimension fetches (entity, park hours, events, metatable + build dimdategroupid, dimseason)
#   - 7:00 AM ET: Secondary ETL (backup run)
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

# Build cron entries
# Format: minute hour day month weekday command
# Note: Times are in system timezone. If system is not Eastern, set TZ=America/New_York or adjust times.
generate_cron_entries() {
    cat << EOF
# Theme Park Crowd Report Pipeline - Scheduled Tasks
# Installed by install_cron.sh - DO NOT EDIT MANUALLY
# Times are Eastern (system timezone should be America/New_York, or adjust times below)
$CRON_MARKER

# 5:00 AM Eastern - Main ETL (incremental)
0 5 * * * export PATH="\$HOME/.local/bin:\$PATH" && cd $PROJECT_ROOT && $SCRIPT_DIR/run_etl.sh >> $LOGS_DIR/cron_etl_5am.log 2>&1 $CRON_MARKER

# 5:30 AM Eastern - Wait time DB report
30 5 * * * export PATH="\$HOME/.local/bin:\$PATH" && cd $PROJECT_ROOT && $PYTHON scripts/report_wait_time_db.py --quick --lookback-days 14 >> $LOGS_DIR/cron_report_530am.log 2>&1 $CRON_MARKER

# 6:00 AM Eastern - Dimension fetches (entity, park hours, events, metatable + build dimdategroupid, dimseason)
0 6 * * * export PATH="\$HOME/.local/bin:\$PATH" && cd $PROJECT_ROOT && $SCRIPT_DIR/run_dimension_fetches.sh >> $LOGS_DIR/cron_dimensions_6am.log 2>&1 $CRON_MARKER

# 7:00 AM Eastern - Secondary ETL (backup run)
0 7 * * * export PATH="\$HOME/.local/bin:\$PATH" && cd $PROJECT_ROOT && $SCRIPT_DIR/run_etl.sh >> $LOGS_DIR/cron_etl_7am.log 2>&1 $CRON_MARKER

# Weekly tasks (Sunday) skipped - will be set up on Mac mini next week:
#   - Sunday 6:30 AM: Posted accuracy report
#   - Sunday 7:00 AM: Log cleanup

EOF
}

# Show what would be installed
show_cron() {
    echo "=== Cron entries to be installed ==="
    echo ""
    generate_cron_entries
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
    echo "Installing theme-park-crowd-report cron entries..."
    
    # Create logs directory
    mkdir -p "$LOGS_DIR"
    
    # Remove old entries first, then add new ones
    (crontab -l 2>/dev/null | grep -v "$CRON_MARKER" || true; generate_cron_entries) | crontab -
    
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
        show_cron
        ;;
    --remove)
        remove_cron
        ;;
    --help|-h)
        echo "Usage: $0 [--show|--remove|--help]"
        echo ""
        echo "Options:"
        echo "  (none)    Install cron jobs"
        echo "  --show    Show what would be installed"
        echo "  --remove  Remove installed cron jobs"
        exit 0
        ;;
    "")
        install_cron
        ;;
    *)
        echo "Unknown option: $1"
        exit 1
        ;;
esac
