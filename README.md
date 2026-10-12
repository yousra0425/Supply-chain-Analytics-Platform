
# Inventory Platform V2
### Supply Chain Inventory Analytics & Data Engineering

An end-to-end inventory data platform built with Python, Kafka, RabbitMQ, Cassandra, DuckDB, ClickHouse, Prefect, FastAPI, and Plotly Dash.

The system processes SAP-derived inventory exports through historical and simulated real-time pipelines, providing inventory analytics, blocked-stock monitoring, and interactive dashboards.

## 1. Architecture

### Historical Pipeline

SAP Excel Export
    |
    v
Prefect Orchestration
    |
    v
Parquet Staging
    |
    v
RabbitMQ
    |
    v
Singleton Historical Worker
    |
    v
DuckDB Bronze
    |
    v
Python Cleaning & Transformation
    |
    v
ClickHouse Historical Gold

### Simulated Live Pipeline

SAP-Derived Excel Snapshot
    |
    v
Python Replay Producer
    |
    v
Kafka (inventory.v2.raw)
    |
    v
Kafka Consumer
    |
    v
Cassandra Bronze
    |
    v
RabbitMQ (inventory.v2.live.ready)
    |
    v
Live Silver Worker
    |
    v
ClickHouse Live Gold

### Application Layer

ClickHouse
    |
    v
FastAPI
    |
    v
Plotly Dash Dashboard

The live pipeline replays events from a static dataset. It does not claim a direct real-time connection to SAP.

## 2. Technologies

| Category | Technologies |
|---|---|
| Programming | Python 3.13 |
| Data processing | Pandas / Polars, SQL |
| Orchestration | Prefect |
| Event streaming | Apache Kafka |
| Messaging | RabbitMQ |
| Operational database | Apache Cassandra |
| Historical analytics | DuckDB |
| Analytical database | ClickHouse |
| Backend API | FastAPI |
| Dashboard | Plotly Dash |
| Containerization | Docker, Docker Compose |
| Testing | Pytest |

## 3. Features

- Historical SAP inventory processing
- Kafka-based simulated live data ingestion
- Cassandra operational data storage
- RabbitMQ event-driven processing
- Historical and live analytical tables in ClickHouse
- Inventory quality and consistency checks
- Idempotency protection and retry handling
- Dead-letter processing support
- REST API and interactive dashboard
- Docker health checks and automatic restart policies
- Docker log rotation

## 4. Prerequisites

- Docker Desktop
- Git Bash (Windows)
- Python 3.13
- Project dependencies installed in a virtual environment
- Existing Cassandra, ClickHouse, and RabbitMQ infrastructure

This deployment uses the external Docker network `inventory-v2-network`.

Kafka's persistent data is stored in the external volume `inventory-v2-kafka-data`.

## 5. Installation

Clone the repository:

```bash
git clone <repository-url>
cd inventory-platform-v2
```

Create a virtual environment:

```bash
python -m venv .venv
source .venv/Scripts/activate
```

Install dependencies:

```bash
pip install -r requirements.txt
```

Configure environment variables in `.env` using the credentials and hostnames appropriate to your environment.

Do not commit `.env` or credentials to Git.

> Note: The current startup script expects existing infrastructure containers and persistent volumes. It is not yet a one-command deployment on a completely new machine.

## 6. Deployment

Start the platform:

```bash
bash scripts/start.sh
```

Check status:

```bash
bash scripts/status.sh
```

Stop the application:

```bash
bash scripts/stop.sh
```

The shutdown script leaves Kafka, Cassandra, ClickHouse, and RabbitMQ running to preserve the existing infrastructure.

## 7. Application Access

| Service | URL |
|---|---|
| Dashboard | http://localhost:8052 |
| FastAPI documentation | http://localhost:8081/docs |
| Kafka external listener | localhost:9092 |

## 8. Validation Results

The validated inventory snapshot contains:

| Metric | Value |
|---|---:|
| Inventory records | 1,886 |
| Unique record IDs | 1,886 |
| Materials | 547 |
| Plants | 6 |
| Blocked inventory value | EUR 1,158,169.31 |
| Missing material types | 0 |

Historical and live analytical views contain the same underlying inventory snapshot and must not be added together.

Validation completed:

- 44 Phase 22 automated tests passed
- Kafka consumer lag: 0
- Successful Kafka → Cassandra → RabbitMQ → ClickHouse processing
- API and dashboard health checks passed
- Kafka health check passed
- Log rotation configured for application containers and Kafka

Run baseline validation:

```bash
python -m scripts.phase22_baseline
```

## 9. Testing

Run the Phase 22 tests:

```bash
python -m pytest tests/test_phase22_*.py -q
```

On Windows, Cassandra driver compatibility may require executing the tests inside the Linux Docker image instead.

## 10. Project Limitations

- The live pipeline uses simulated event replay from a static SAP-derived export.
- Kafka is currently deployed as a single-node KRaft broker.
- The startup scripts depend on separately provisioned infrastructure.
- The application is configured for local deployment, not public internet exposure.
- Production authentication, high availability, and backup/recovery procedures require further work.

## 11. Future Improvements

- Infrastructure provisioning on a new machine
- Centralized metrics and observability
- Automated backup and restoration testing
- CI/CD with GitHub Actions
- Stronger application security
- Forecasting and anomaly detection
- Cloud deployment
