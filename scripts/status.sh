
#!/usr/bin/env bash
set -uo pipefail

cd "$(dirname "$0")/.."

echo "======================================"
echo " Inventory Platform V2 - Status"
echo "======================================"

if ! docker info >/dev/null 2>&1; then
    echo "ERROR: Docker Desktop is not running."
    exit 1
fi

failed=0

echo ""
echo "[1/4] Infrastructure and application containers"
echo ""

check_container() {
    local name="$1"
    local state health

    state=$(docker inspect -f '{{.State.Status}}' "$name" 2>/dev/null) || {
        echo "MISSING  $name"
        failed=1
        return
    }

    health=$(docker inspect -f \
      '{{if .State.Health}}{{.State.Health.Status}}{{else}}not configured{{end}}' \
      "$name" 2>/dev/null)

    if [ "$state" != "running" ]; then
        echo "FAILED   $name ($state)"
        failed=1
    elif [ "$health" = "unhealthy" ]; then
        echo "FAILED   $name (unhealthy)"
        failed=1
    else
        echo "OK       $name ($state, $health)"
    fi
}

for container in \
    inventory-cassandra \
    te-clickhouse \
    inventory-rabbitmq-v2 \
    inventory-v2-kafka \
    inventory-v2-kafka-consumer \
    inventory-v2-live-silver-worker \
    inventory-v2-api \
    inventory-v2-dashboard
do
    check_container "$container"
done

echo ""
echo "[2/4] Kafka consumer group"
echo ""

if MSYS_NO_PATHCONV=1 docker exec inventory-v2-kafka \
    /opt/kafka/bin/kafka-consumer-groups.sh \
    --bootstrap-server localhost:9092 \
    --describe \
    --group inventory-v2-cassandra-consumer
then
    echo "Kafka group query completed."
else
    echo "ERROR: Could not query Kafka consumer group."
    failed=1
fi

echo ""
echo "[3/4] HTTP endpoints"
echo ""

check_http() {
    local name="$1"
    local url="$2"

    if curl -fsS --max-time 10 -o /dev/null "$url"; then
        echo "OK       $name"
    else
        echo "FAILED   $name ($url)"
        failed=1
    fi
}

check_http "FastAPI" "http://127.0.0.1:8081/openapi.json"
check_http "Dashboard" "http://127.0.0.1:8052/"

echo ""
echo "[4/4] Inventory baseline"
echo ""

if python -m scripts.phase22_baseline; then
    echo "Inventory validation completed."
else
    echo "ERROR: Inventory baseline failed."
    failed=1
fi

echo ""
echo "======================================"

if [ "$failed" -eq 0 ]; then
    echo "Platform checks completed successfully."
else
    echo "One or more platform checks failed."
fi

echo "======================================"

exit "$failed"
