
#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

echo "======================================"
echo " Inventory Platform V2 - Shutdown"
echo "======================================"

if ! docker info >/dev/null 2>&1; then
    echo "ERROR: Docker Desktop is not running."
    exit 1
fi

echo ""
echo "[1/3] Stopping Kafka consumer..."

# Stop ingestion first so no more Kafka events are processed.
docker compose -f compose.app.yml stop -t 30 kafka-consumer

echo ""
echo "[2/3] Stopping Live Silver worker..."

# Give the worker time to finish its current message.
docker compose -f compose.app.yml stop -t 60 live-silver-worker

echo ""
echo "[3/3] Stopping web applications..."

docker compose -f compose.app.yml stop -t 20 dashboard api

echo ""
echo "======================================"
echo " Application services stopped"
echo "======================================"
echo ""
echo "Kafka, Cassandra, ClickHouse, and RabbitMQ remain running."
echo "No containers or volumes were deleted."
echo ""
echo "To restart:"
echo "bash scripts/start.sh"
