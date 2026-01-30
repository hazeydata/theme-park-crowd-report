#!/bin/bash
# install_queue_times_service.sh - Install queue-times loop as systemd service (starts on boot)
#
# Runs the queue-times fetcher as a system service so it starts automatically on boot.
# Requires sudo to copy the unit file and enable the service.
#
# Usage:
#   ./scripts/install_queue_times_service.sh    # Install and enable
#   ./scripts/install_queue_times_service.sh --start-only   # Copy and start (don't enable on boot)
#   ./scripts/install_queue_times_service.sh --remove       # Stop and disable

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SERVICE_NAME="queue-times-loop.service"
UNIT_FILE="$SCRIPT_DIR/$SERVICE_NAME"
SYSTEMD_DIR="/etc/systemd/system"

case "${1:-}" in
    --remove)
        echo "Stopping and disabling queue-times-loop..."
        sudo systemctl stop "$SERVICE_NAME" 2>/dev/null || true
        sudo systemctl disable "$SERVICE_NAME" 2>/dev/null || true
        sudo rm -f "$SYSTEMD_DIR/$SERVICE_NAME"
        sudo systemctl daemon-reload
        echo "Done. Service removed."
        exit 0
        ;;
    --start-only)
        echo "Copying unit file to $SYSTEMD_DIR..."
        sudo cp "$UNIT_FILE" "$SYSTEMD_DIR/"
        sudo systemctl daemon-reload
        echo "Starting queue-times-loop..."
        sudo systemctl start "$SERVICE_NAME"
        echo "Done. Service started (not enabled on boot)."
        echo "  Status: sudo systemctl status $SERVICE_NAME"
        echo "  To enable on boot: sudo systemctl enable $SERVICE_NAME"
        exit 0
        ;;
    --help|-h)
        echo "Usage: $0 [--start-only|--remove|--help]"
        echo ""
        echo "  (none)       Install, enable, and start (runs on boot)"
        echo "  --start-only Copy unit file and start (don't enable on boot)"
        echo "  --remove     Stop, disable, and remove the service"
        exit 0
        ;;
    "")
        ;;
    *)
        echo "Unknown option: $1" >&2
        exit 1
        ;;
esac

echo "Installing queue-times-loop as systemd service (starts on boot)..."
echo "  Unit file: $UNIT_FILE"
echo "  Target:    $SYSTEMD_DIR/$SERVICE_NAME"
echo ""

sudo cp "$UNIT_FILE" "$SYSTEMD_DIR/"
sudo systemctl daemon-reload
sudo systemctl enable "$SERVICE_NAME"
sudo systemctl start "$SERVICE_NAME"

echo ""
echo "Done. Queue-times loop is installed and running; it will start automatically on boot."
echo ""
echo "  Status:  sudo systemctl status $SERVICE_NAME"
echo "  Logs:    sudo journalctl -u $SERVICE_NAME -f"
echo "  Stop:    sudo systemctl stop $SERVICE_NAME"
echo "  Remove:  $0 --remove"
echo ""
