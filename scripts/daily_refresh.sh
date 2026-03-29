#!/usr/bin/env bash
# daily_refresh.sh — Automated daily data collection, cleaning, and deployment
# for the EV Charging Station platform.
#
# Usage:
#   chmod +x scripts/daily_refresh.sh
#   ./scripts/daily_refresh.sh
#
# This script:
#   1. Collects data from all configured sources (batch mode)
#   2. Cleans the raw data
#   3. Exports to GeoJSON
#   4. Deploys the new GeoJSON to the frontend and triggers an API reload
#
# All output is logged with timestamps to scripts/logs/refresh.log.
# If any critical step fails, existing data is preserved.

set -euo pipefail

# ── Project root (parent of scripts/) ──
PROJECT_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$PROJECT_ROOT"

# ── Source environment variables ──
# Load .env files so API keys (NREL_API_KEY, OPENCHARGEMAP_API_KEY, etc.)
# are available to the Python process. `set -a` auto-exports all variables.
if [ -f "$PROJECT_ROOT/.env" ]; then
    set -a
    # shellcheck disable=SC1091
    source "$PROJECT_ROOT/.env"
    set +a
fi
if [ -f "$PROJECT_ROOT/data_research_agent/.env" ]; then
    set -a
    # shellcheck disable=SC1091
    source "$PROJECT_ROOT/data_research_agent/.env"
    set +a
fi

# ── Logging ──
LOG_DIR="$PROJECT_ROOT/scripts/logs"
mkdir -p "$LOG_DIR"
LOG_FILE="$LOG_DIR/refresh.log"

log() {
    local level="$1"
    shift
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] [$level] $*" | tee -a "$LOG_FILE"
}

log_separator() {
    echo "" >> "$LOG_FILE"
    echo "================================================================" | tee -a "$LOG_FILE"
    log "INFO" "Daily refresh started"
    echo "================================================================" | tee -a "$LOG_FILE"
}

# ── Error handling ──
COLLECT_SUCCESS=false
CLEAN_SUCCESS=false
GEO_SUCCESS=false

cleanup_on_failure() {
    if [ "$GEO_SUCCESS" = false ]; then
        log "WARN" "Pipeline did not complete successfully — existing data preserved"
    fi
}
trap cleanup_on_failure EXIT

log_separator

# ── Activate virtual environment ──
VENV_PATH="$PROJECT_ROOT/data_research_agent/.venv"
if [ ! -d "$VENV_PATH" ]; then
    log "ERROR" "Virtual environment not found at $VENV_PATH"
    exit 1
fi

log "INFO" "Activating virtual environment: $VENV_PATH"
# shellcheck disable=SC1091
source "$VENV_PATH/bin/activate"

# ── Step 1: Data Collection ──
log "INFO" "[Step 1/5] Running data collection (batch mode)..."
cd "$PROJECT_ROOT/data_research_agent"

# Collect from all configured sources. Sources that require API keys
# will use environment variables (OPENCHARGEMAP_API_KEY, NREL_API_KEY, etc.).
# Sources with auth.type: none (OSM Overpass, Bundesnetzagentur, Supercharger,
# France IRVE, Australia NSW/VIC, Singapore LTA) work without keys.
if python main.py collect --mode batch 2>&1 | tee -a "$LOG_FILE"; then
    COLLECT_SUCCESS=true
    log "INFO" "Data collection completed successfully"
else
    log "ERROR" "Data collection failed (exit code: $?)"
    log "ERROR" "Aborting pipeline — existing data will NOT be overwritten"
    exit 1
fi

# ── Step 2: Find the latest batch directory ──
LATEST_BATCH=$(find "$PROJECT_ROOT/data_research_agent/data/raw" -maxdepth 1 -type d -name 'batch_*' | sort -r | head -1)
if [ -z "$LATEST_BATCH" ]; then
    log "ERROR" "No batch directory found after collection"
    exit 1
fi
log "INFO" "Latest batch: $LATEST_BATCH"

# Verify the batch has actual data files
BATCH_FILE_COUNT=$(find "$LATEST_BATCH" -name '*.json' | wc -l | tr -d ' ')
if [ "$BATCH_FILE_COUNT" -eq 0 ]; then
    log "ERROR" "Batch directory is empty — no JSON files produced"
    exit 1
fi
log "INFO" "Found $BATCH_FILE_COUNT source files in batch"

# ── Step 3: Clean the data ──
log "INFO" "[Step 2/5] Running cleaning pipeline on $LATEST_BATCH..."
if python main.py clean --input "$LATEST_BATCH" 2>&1 | tee -a "$LOG_FILE"; then
    CLEAN_SUCCESS=true
    log "INFO" "Data cleaning completed successfully"
else
    log "ERROR" "Data cleaning failed — existing data preserved"
    exit 1
fi

# ── Step 4: Export to GeoJSON ──
log "INFO" "[Step 3/5] Exporting to GeoJSON..."
if python main.py geo 2>&1 | tee -a "$LOG_FILE"; then
    GEO_SUCCESS=true
    log "INFO" "GeoJSON export completed successfully"
else
    log "ERROR" "GeoJSON export failed — existing data preserved"
    exit 1
fi

# ── Step 5: Deploy GeoJSON to frontend ──
cd "$PROJECT_ROOT"

SOURCE_GEOJSON="$PROJECT_ROOT/data_research_agent/data/geo/ev_stations.geojson"
DEST_GEOJSON="$PROJECT_ROOT/frontend/data/ev_stations.geojson"

if [ ! -f "$SOURCE_GEOJSON" ]; then
    log "ERROR" "Source GeoJSON not found: $SOURCE_GEOJSON"
    exit 1
fi

# Create backup of current data before overwriting
if [ -f "$DEST_GEOJSON" ]; then
    BACKUP="$DEST_GEOJSON.bak.$(date '+%Y%m%d_%H%M%S')"
    cp "$DEST_GEOJSON" "$BACKUP"
    log "INFO" "[Step 4/5] Backed up existing GeoJSON to $BACKUP"
fi

mkdir -p "$(dirname "$DEST_GEOJSON")"

# ── Regression guard: abort if new dataset is <80% of previous ──
# This prevents deploying a broken/partial collection that would wipe out
# good data (e.g. an API outage causing most sources to return 0 records).
if [ -f "$DEST_GEOJSON" ]; then
    OLD_COUNT=$(python3 -c "import json; d=json.load(open('$DEST_GEOJSON')); print(len(d.get('features',[])))" 2>/dev/null || echo "0")
    NEW_COUNT=$(python3 -c "import json; d=json.load(open('$SOURCE_GEOJSON')); print(len(d.get('features',[])))" 2>/dev/null || echo "0")
    if [ "$OLD_COUNT" -gt 0 ] 2>/dev/null && [ "$NEW_COUNT" -gt 0 ] 2>/dev/null; then
        THRESHOLD=$(( OLD_COUNT * 80 / 100 ))
        if [ "$NEW_COUNT" -lt "$THRESHOLD" ]; then
            log "ERROR" "REGRESSION GUARD: New dataset ($NEW_COUNT stations) is less than 80% of previous ($OLD_COUNT stations). Aborting deployment."
            log "ERROR" "Threshold was $THRESHOLD. Investigate source failures before re-running."
            exit 1
        fi
        log "INFO" "Regression check passed: $NEW_COUNT new vs $OLD_COUNT previous (threshold: $THRESHOLD)"
    fi
fi

cp "$SOURCE_GEOJSON" "$DEST_GEOJSON"
log "INFO" "[Step 4/5] Deployed new GeoJSON to $DEST_GEOJSON"

# Count stations in the new file
STATION_COUNT=$(python3 -c "import json; d=json.load(open('$DEST_GEOJSON')); print(len(d.get('features',[])))" 2>/dev/null || echo "unknown")
log "INFO" "New dataset contains $STATION_COUNT stations"

# ── Step 6: Trigger API server reload ──
log "INFO" "[Step 5/5] Triggering API server data reload..."

# Method 1: Try the hot-reload endpoint (preferred — no downtime)
ADMIN_TOKEN="${ADMIN_TOKEN:-admin-change-me}"
API_PORT="${PORT:-8000}"
API_HOST="${HOST:-127.0.0.1}"

RELOAD_URL="http://${API_HOST}:${API_PORT}/api/admin/reload"

if curl -s -f -X POST "$RELOAD_URL" \
    -H "Authorization: Bearer $ADMIN_TOKEN" \
    -H "Content-Type: application/json" \
    --max-time 10 2>&1 | tee -a "$LOG_FILE"; then
    log "INFO" "API server reloaded via hot-reload endpoint"
else
    log "WARN" "Hot-reload endpoint not reachable — trying SIGHUP..."

    # Method 2: Send SIGHUP to uvicorn process for graceful reload
    UVICORN_PID=$(pgrep -f "uvicorn.*api_server" | head -1 || true)
    if [ -n "$UVICORN_PID" ]; then
        kill -HUP "$UVICORN_PID" 2>/dev/null && \
            log "INFO" "Sent SIGHUP to uvicorn (PID: $UVICORN_PID)" || \
            log "WARN" "Failed to send SIGHUP to uvicorn (PID: $UVICORN_PID)"
    else
        log "WARN" "No uvicorn process found — API server may need manual restart"
        log "WARN" "Start it with: cd $PROJECT_ROOT && python -m api_server.server"
    fi
fi

# ── Cleanup old backups (keep last 7) ──
BACKUP_COUNT=$(ls -1 "$DEST_GEOJSON".bak.* 2>/dev/null | wc -l | tr -d ' ')
if [ "$BACKUP_COUNT" -gt 7 ]; then
    ls -1t "$DEST_GEOJSON".bak.* | tail -n +8 | xargs rm -f
    log "INFO" "Cleaned up old backups (kept 7 most recent)"
fi

# ── Done ──
log "INFO" "Daily refresh completed successfully"
log "INFO" "Summary: collected=$COLLECT_SUCCESS cleaned=$CLEAN_SUCCESS geo=$GEO_SUCCESS stations=$STATION_COUNT"
echo "================================================================" >> "$LOG_FILE"
