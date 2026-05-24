#!/usr/bin/env bash
# Track 01 extension — bronze → silver → gold (plain pipeline). Requires terraform + Kafka data.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
CONTAINER="${GLUE_CONTAINER:-glue-pyspark-poc}"

echo "==> Medallion plain pipeline (bronze → silver → gold)"
echo "Prereqs: docker up, terraform applied, Kafka topic has events"
echo ""

if ! docker ps --format '{{.Names}}' | grep -qx "$CONTAINER"; then
  echo "Start stack first: ./scripts/start-containers.bash"
  exit 1
fi

echo "1) Generate sample Kafka events (10 messages)..."
docker exec "$CONTAINER" python /app/scripts/generate_plain_events.py 10

echo "2) Bronze job (Kafka → S3)..."
docker exec "$CONTAINER" poetry run python /app/plain/bronze_job.py &
BRONZE_PID=$!
sleep 15
kill $BRONZE_PID 2>/dev/null || true

echo "3) Silver job (S3 → Iceberg)..."
docker exec "$CONTAINER" poetry run python /app/plain/silver_job.py

echo "4) Gold job (Iceberg → DynamoDB)..."
docker exec "$CONTAINER" poetry run python /app/plain/gold_job.py

echo "Done. Query silver in Jupyter: see jupyterlab-snippets.md"
