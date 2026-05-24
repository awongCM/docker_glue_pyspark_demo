#!/usr/bin/env bash
# Launch all 4 PySpark curriculum tracks with staggered starts (avoids OOM on local[*]).
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
CONTAINER="${GLUE_CONTAINER:-glue-pyspark-poc}"
LOG_DIR="${ROOT}/learning/logs"
STAGGER_SEC="${TRACK_STAGGER_SEC:-5}"
PYTHON="${TRACK_PYTHON:-poetry run python}"

mkdir -p "$LOG_DIR"

red() { printf '\033[0;31m%s\033[0m\n' "$*"; }
green() { printf '\033[0;32m%s\033[0m\n' "$*"; }
yellow() { printf '\033[1;33m%s\033[0m\n' "$*"; }

ensure_container() {
  if docker ps --format '{{.Names}}' | grep -qx "$CONTAINER"; then
    green "✓ Container $CONTAINER is running"
    return 0
  fi
  yellow "Container $CONTAINER not running — starting stack..."
  (cd "$ROOT" && docker-compose up --build -d glue-pyspark jupyterlab)
  sleep 8
  if ! docker ps --format '{{.Names}}' | grep -qx "$CONTAINER"; then
    red "Failed to start $CONTAINER. Run: ./scripts/start-containers.bash"
    exit 1
  fi
  green "✓ Stack started"
}

verify_learning_mount() {
  if ! docker exec "$CONTAINER" test -f /app/learning/track_01_basic.py; then
    red "learning/ not mounted in container. Restart: docker-compose up -d glue-pyspark"
    exit 1
  fi
}

run_track() {
  local num="$1"
  local script="$2"
  local log="$LOG_DIR/track_0${num}.log"
  yellow "Starting track 0${num} → $log"
  docker exec "$CONTAINER" bash -lc \
    "cd /app/learning && TRACK_SPARK_MASTER=local[1] TRACK_DRIVER_MEMORY=512m $PYTHON $script" \
    > "$log" 2>&1 &
  echo $! > "$LOG_DIR/track_0${num}.pid"
}

print_urls() {
  cat <<EOF

================================================================================
  PARALLEL CURRICULUM RUNNER
================================================================================
  JupyterLab:     http://localhost:8888/     (token: test)
  Spark History:  http://localhost:18080/    (if history server enabled)
  LocalStack:     http://localhost:4566/
  Live Spark UI:  http://localhost:4040/     (active app — only while a track runs)

  Logs: ${LOG_DIR}/track_0{1,2,3,4}.log
  Tail all:  tail -f ${LOG_DIR}/track_*.log

  Recommended: staggered parallel (this script). Do NOT run 4× local[*] at once.

  Per-track observation guide → learning/00_START_HERE.md
================================================================================
EOF
}

main() {
  ensure_container
  verify_learning_mount
  rm -f "$LOG_DIR"/track_*.log "$LOG_DIR"/track_*.pid 2>/dev/null || true

  print_urls

  run_track 1 track_01_basic.py
  sleep "$STAGGER_SEC"
  run_track 2 track_02_intermediate.py
  sleep "$STAGGER_SEC"
  run_track 3 track_03_advanced.py
  sleep "$STAGGER_SEC"
  run_track 4 track_04_master.py

  green "All 4 tracks launched (stagger ${STAGGER_SEC}s). Waiting for completion..."
  wait
  green "Done. Review logs in ${LOG_DIR}/"
}

main "$@"
