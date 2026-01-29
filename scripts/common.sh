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
