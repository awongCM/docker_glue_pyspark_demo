#!/usr/bin/env bash
# Track 05 — million-event lab (RAM-aware profiles). Runs SOLO, not in the 4-track orchestrator.
#
# RAM: default profile is "lite" (100k rows, local[1], 1g driver) for 8–16 GB laptops.
# If you see Py4JNetworkError or "batch is falling behind", do NOT raise throughput —
# stay on lite or lower TRACK05_ROWS_PER_SEC. See learning/00_START_HERE.md § Local RAM limits.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
CONTAINER="${GLUE_CONTAINER:-glue-pyspark-poc}"
LOG_DIR="${ROOT}/learning/logs"
PYTHON="${TRACK_PYTHON:-poetry run python}"

# Default: lite profile (100k rows, local[1], 1g driver) — fits 8–16 GB RAM laptops.
PROFILE="${TRACK05_PROFILE:-lite}"

red() { printf '\033[0;31m%s\033[0m\n' "$*"; }
green() { printf '\033[0;32m%s\033[0m\n' "$*"; }
yellow() { printf '\033[1;33m%s\033[0m\n' "$*"; }

mkdir -p "$LOG_DIR"
LOG="$LOG_DIR/track_05.log"

if ! docker ps --format '{{.Names}}' | grep -qx "$CONTAINER"; then
  yellow "Container $CONTAINER not running — starting stack..."
  (cd "$ROOT" && docker-compose up -d glue-pyspark)
  sleep 8
fi

if ! docker ps --format '{{.Names}}' | grep -qx "$CONTAINER"; then
  red "Failed to start $CONTAINER. Run: ./scripts/start-containers.bash"
  exit 1
fi

if ! docker exec "$CONTAINER" test -f /app/learning/track_05_million_event_lab.py; then
  red "learning/ not mounted. Restart: docker-compose up -d glue-pyspark"
  exit 1
fi

cat <<EOF

================================================================================
  TRACK 05 — MILLION-EVENT LAB (profile: ${PROFILE})
================================================================================
  Profiles:
    lite     — 100k rows, local[1]/1g   (default, laptop-friendly)
    standard — 500k rows, local[2]/2g   (16 GB+ RAM)
    million  — 1M rows, local[*]/4g     (32 GB+ RAM, generous Docker memory)

  Override: TRACK05_PROFILE=million ./learning/run_track05_million.sh

  Spark UI:  http://localhost:14040/
  Log:       ${LOG}
================================================================================

EOF

green "Starting Track 05..."
docker exec "$CONTAINER" bash -lc \
  "cd /app/learning && TRACK05_PROFILE='${PROFILE}' ${PYTHON} track_05_million_event_lab.py" \
  2>&1 | tee "$LOG"

green "Done. Full log: ${LOG}"
