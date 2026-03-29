#!/usr/bin/env bash
# install_cron.sh — Install a crontab entry to run daily_refresh.sh at 3 AM daily.
#
# Usage:
#   chmod +x scripts/install_cron.sh
#   ./scripts/install_cron.sh
#
# To remove:
#   crontab -l | grep -v 'daily_refresh.sh' | crontab -
#
# To verify:
#   crontab -l

set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
REFRESH_SCRIPT="$PROJECT_ROOT/scripts/daily_refresh.sh"
CRON_LOG="$PROJECT_ROOT/scripts/logs/cron.log"

# Verify the refresh script exists
if [ ! -f "$REFRESH_SCRIPT" ]; then
    echo "Error: daily_refresh.sh not found at $REFRESH_SCRIPT"
    exit 1
fi

# Make sure it's executable
chmod +x "$REFRESH_SCRIPT"

# Build the cron line
# Runs at 3:00 AM daily, logs stdout/stderr to cron.log
CRON_ENTRY="0 3 * * * $REFRESH_SCRIPT >> $CRON_LOG 2>&1"

# Check if the entry already exists
EXISTING=$(crontab -l 2>/dev/null || true)
if echo "$EXISTING" | grep -qF "daily_refresh.sh"; then
    echo "Cron entry for daily_refresh.sh already exists:"
    echo "$EXISTING" | grep "daily_refresh.sh"
    echo ""
    read -rp "Replace it? [y/N] " answer
    if [[ "$answer" =~ ^[Yy]$ ]]; then
        # Remove existing entry and add new one
        UPDATED=$(echo "$EXISTING" | grep -v "daily_refresh.sh")
        echo "$UPDATED
$CRON_ENTRY" | crontab -
        echo "Cron entry updated."
    else
        echo "No changes made."
        exit 0
    fi
else
    # Append to existing crontab
    (echo "$EXISTING"; echo "$CRON_ENTRY") | crontab -
    echo "Cron entry installed."
fi

echo ""
echo "Current crontab:"
crontab -l
echo ""
echo "The daily refresh will run at 3:00 AM every day."
echo "Logs: $CRON_LOG"
echo ""
echo "Note: Ensure environment variables (API keys, ADMIN_TOKEN, etc.) are"
echo "available to cron. You can add them to $PROJECT_ROOT/.env or set them"
echo "at the top of daily_refresh.sh."
