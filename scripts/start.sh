
#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

APP_COMPOSE="compose.app.yml"
KAFKA_COMPOSE="compose.kafka.yml"

echo "======================================"
echo " Inventory Platform V2 - Startup"
echo "======================================"

# 1. Verify Docker is available
echo ""
echo "[1/5] Checking Docker..."

if ! docker info >/dev/null 2>&1; then
    echo "ERROR: Docker Desktop is not running."
    exit 1
fi

# 2. Verify existing infrastructure
echo ""
echo "[2/5] Checking infrastructure..."

for container in inventory-cassandra te-clickhouse inventory-rabbitmq-v2; do
    status=$(docker inspect -f '{{.State.Running}}' "$container" 2>/dev/null || true)

    if [ "$status" != "true" ]; then
        echo "ERROR: $container is not running."
        echo "Start your existing infrastructure before continuing."
        exit 1
    fi

    echo "OK: $container"
done

# Validate configurations before modifying containers
docker compose -f "$APP_COMPOSE" config --quiet
docker compose -f "$KAFKA_COMPOSE" config --quiet

# 3. Start Kafka
echo ""
echo "[3/5] Starting Kafka..."

docker compose -f "$KAFKA_COMPOSE" up -d --no-deps kafka

echo "Waiting for Kafka health check..."

healthy=false

for i in $(seq 1 36); do
    health=$(docker inspect -f \
      '{{if .State.Health}}{{.State.Health.Status}}{{else}}none{{end}}' \
      inventory-v2-kafka 2>/dev/null || true)

    if [ "$health" = "healthy" ]; then
        healthy=true
        break
    fi

    if [ "$health" = "unhealthy" ]; then
        echo "WARNING: Kafka is currently unhealthy; waiting for recovery..."
    fi

    sleep 5
done

if [ "$healthy" != "true" ]; then
    echo "ERROR: Kafka did not become healthy."
    docker logs inventory-v2-kafka --tail 30
    exit 1
fi

echo "OK: Kafka is healthy."

# 4. Start workers, then API and dashboard
echo ""
echo "[4/5] Starting application services..."

docker compose -f "$APP_COMPOSE" up -d --no-deps \
    live-silver-worker kafka-consumer api

echo "Waiting for API health check..."

api_healthy=false

for i in $(seq 1 24); do
    health=$(docker inspect -f \
      '{{if .State.Health}}{{.State.Health.Status}}{{else}}none{{end}}' \
      inventory-v2-api 2>/dev/null || true)

    if [ "$health" = "healthy" ]; then
        api_healthy=true
        break
    fi

    sleep 5
done

if [ "$api_healthy" != "true" ]; then
    echo "ERROR: API did not become healthy."
    docker logs inventory-v2-api --tail 30
    exit 1
fi

docker compose -f "$APP_COMPOSE" up -d --no-deps dashboard

# 5. Final status
echo ""
echo "[5/5] Checking services..."

docker compose -f "$APP_COMPOSE" ps -a

echo ""
echo "======================================"
echo " Startup finished"
echo "======================================"
echo "Dashboard: http://localhost:8052"
echo "FastAPI:   http://localhost:8081/docs"
echo ""
echo "Run: bash scripts/status.sh"
