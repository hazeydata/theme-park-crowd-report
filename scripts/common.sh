#!/bin/bash
# common.sh - Shared functions for Linux scripts
# Source this file: source "$(dirname "$0")/common.sh"

set -e

# Get the project root (parent of scripts/)
get_project_root() {
    local script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
    echo "$(dirname "$script_dir")"
}

# Read output_base from config/config.json or return default
get_output_base() {
    local project_root="$1"
    local config_file="$project_root/config/config.json"
    local default_base="$HOME/hazeydata/pipeline"
    
    if [[ -f "$config_file" ]]; then
        # Try to extract output_base using python (more reliable than jq for escapes)
        local base=$(python3 -c "
import json
try:
    with open('$config_file') as f:
        data = json.load(f)
    print(data.get('output_base', ''))
except:
    pass
" 2>/dev/null)
        if [[ -n "$base" ]]; then
            echo "$base"
            return
        fi
    fi
    echo "$default_base"
}

# Find python executable
get_python() {
    # Add user's local bin to PATH for pip-installed packages
    export PATH="$HOME/.local/bin:$PATH"
    
    if command -v python3 &> /dev/null; then
        echo "python3"
    elif command -v python &> /dev/null; then
        echo "python"
    else
        echo "ERROR: Python not found" >&2
        exit 1
    fi
}

# Logging helpers
log_info() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] INFO: $*"
}

log_error() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] ERROR: $*" >&2
}

# Ensure logs directory exists
ensure_logs_dir() {
    local output_base="$1"
    mkdir -p "$output_base/logs"
}

# Force-quit Dropbox before pipeline runs when output_base is on Dropbox (avoids file locks / partial reads).
# Call with output_base; if output_base path contains "Dropbox" and Dropbox is running, stops it.
# Usage: ensure_dropbox_stopped "$OUTPUT_BASE" || exit 1
ensure_dropbox_stopped() {
    local output_base="$1"
    # Only act when output is under a path that looks like Dropbox
    if [[ -z "$output_base" ]] || [[ "$output_base" != *[Dd]ropbox* ]]; then
        return 0
    fi
    # Is Dropbox process running?
    if ! pgrep -f '[Dd]ropbox' &>/dev/null; then
        return 0
    fi
    # Dropbox is running; force stop it
    if command -v dropbox &>/dev/null; then
        dropbox stop 2>/dev/null || true
    elif [[ -x "$HOME/dropbox.py" ]]; then
        python3 "$HOME/dropbox.py" stop 2>/dev/null || true
    else
        pkill -TERM -f '[Dd]ropbox' 2>/dev/null || true
    fi
    # Wait for process(es) to exit (up to 15 seconds)
    local wait_sec=0
    while pgrep -f '[Dd]ropbox' &>/dev/null && [[ $wait_sec -lt 15 ]]; do
        sleep 1
        ((wait_sec++)) || true
    done
    if pgrep -f '[Dd]ropbox' &>/dev/null; then
        echo "Dropbox did not stop after ${wait_sec}s. Pipeline may still run; sync could cause issues." >&2
    fi
    return 0
}
