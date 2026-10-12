
import argparse
import time
from pathlib import Path

import duckdb
import pandas as pd
from src.quality.data_quality import main as run_quality_checks
from kafka import KafkaConsumer, TopicPartition
from cassandra.cluster import Cluster
from src.utils.cassandra_connection import CassandraConnection
from prefect import flow, task, get_run_logger

from src.ingestion.historical_flow import historical_ingestion_flow
from src.workers.historical_worker import calculate_file_hash
from src.streaming.kafka_producer import stream_data


PROJECT_ROOT = Path(__file__).resolve().parents[2]

PARQUET_FILE = (
    PROJECT_ROOT
    / "data/historical/blocked_stock_2026_08_17.parquet"
)

DUCKDB_FILE = (
    PROJECT_ROOT
    / "data/processed/inventory_history.duckdb"
)

KAFKA_TOPIC = "inventory.v2.raw"
KAFKA_SERVERS = "localhost:9092"


@task(name="Verify Historical DuckDB")
def verify_historical_ingestion():
    logger = get_run_logger()

    file_hash = calculate_file_hash(PARQUET_FILE)
    expected_rows = len(pd.read_parquet(PARQUET_FILE))

    with duckdb.connect(
        str(DUCKDB_FILE),
        read_only=True,
    ) as con:
        result = con.execute(
            """
            SELECT row_count
            FROM ingestion_history
            WHERE file_hash = ?
            """,
            [file_hash],
        ).fetchone()

    if result is None:
        raise ValueError(
            "Historical snapshot is missing from DuckDB"
        )

    if result[0] != expected_rows:
        raise ValueError(
            "Historical row count mismatch"
        )

    logger.info(
        "Historical verified: %s records",
        result[0],
    )

    return {
        "status": "VERIFIED",
        "rows": result[0],
    }


def get_topic_offset():
    consumer = KafkaConsumer(
        bootstrap_servers=KAFKA_SERVERS,
        enable_auto_commit=False,
    )

    try:
        partitions = consumer.partitions_for_topic(
            KAFKA_TOPIC
        )

        if partitions != {0}:
            raise RuntimeError(
                "This replay verification currently requires "
                "one Kafka partition (partition 0)."
            )

        tp = TopicPartition(KAFKA_TOPIC, 0)
        consumer.assign([tp])

        return consumer.end_offsets([tp])[tp]

    finally:
        consumer.close()


@task(name="Publish Kafka Replay")
def publish_kafka_replay(limit: int = 5):
    logger = get_run_logger()

    if not 1 <= limit <= 100:
        raise ValueError(
            "Replay limit must be between 1 and 100"
        )

    start_offset = get_topic_offset()

    logger.info(
        "Starting Kafka replay: %s messages",
        limit,
    )

    stream_data(limit=limit, delay=0.2)

    end_offset = get_topic_offset()

    published = end_offset - start_offset

    if published != limit:
        raise RuntimeError(
            f"Expected {limit} messages, observed {published}. "
            "Check for concurrent producers or partial publication "
            "before retrying; this task must not blindly replay."
        )

    logger.info(
        "Kafka replay published: offsets %s to %s",
        start_offset,
        end_offset - 1,
    )

    return {
        "start_offset": start_offset,
        "end_offset": end_offset,
        "published": published,
    }


@task(name="Verify Kafka Consumer Completion")
def verify_kafka_processing(
    replay_info: dict,
    timeout_seconds: int = 120,
):
    logger = get_run_logger()

    cluster = Cluster(
        ["127.0.0.1"],
        port=9042,
        connection_class=CassandraConnection,
        connect_timeout=15,
        control_connection_timeout=15,
    )

    try:
        session = cluster.connect("inventory_v2")

        query = session.prepare("""
            SELECT status
            FROM kafka_processing_ledger
            WHERE message_id = ?
        """)

        start = replay_info["start_offset"]
        end = replay_info["end_offset"]

        deadline = time.monotonic() + timeout_seconds

        while time.monotonic() < deadline:
            completed = 0
            dead_lettered = []

            for offset in range(start, end):
                message_id = (
                    f"{KAFKA_TOPIC}:0:{offset}"
                )

                row = session.execute(
                    query,
                    (message_id,),
                ).one()

                if row and row.status == "COMPLETED":
                    completed += 1
                elif row and row.status == "DEAD_LETTERED":
                    dead_lettered.append(message_id)

            if dead_lettered:
                raise RuntimeError(
                    "Kafka messages were dead-lettered: "
                    + ", ".join(dead_lettered)
                )

            if completed == end - start:
                logger.info(
                    "Kafka processing verified: %s messages",
                    completed,
                )

                return {
                    "status": "VERIFIED",
                    "completed": completed,
                    "start_offset": start,
                    "end_offset": end - 1,
                }

            logger.info(
                "Waiting for Kafka processing: %s/%s",
                completed,
                end - start,
            )

            time.sleep(3)

        raise TimeoutError(
            "Kafka processing was not completed "
            f"within {timeout_seconds} seconds"
        )

    finally:
        cluster.shutdown()

@task(name="Validate Data Quality")
def validate_pipeline_quality():
    logger = get_run_logger()

    report = run_quality_checks()

    logger.info(
        "Data quality: %s/%s checks passed",
        report["passed"],
        report["total_checks"],
    )

    return {
        "status": report["overall_status"],
        "passed": report["passed"],
        "failed": report["failed"],
    }

@flow(
    name="inventory-v2-orchestration",
    log_prints=True,
)
def inventory_pipeline(
    run_historical: bool = True,
    replay_limit: int = 0,
    run_quality: bool = True,
):
    logger = get_run_logger()

    if replay_limit < 0 or replay_limit > 100:
        raise ValueError(
            "replay_limit must be between 0 and 100"
        )

    logger.info("Starting Inventory V2 orchestration")

    results = {}

    # Historical branch
    if run_historical:
        logger.info("Starting historical branch")

        historical_ingestion_flow()

        results["historical"] = (
            verify_historical_ingestion()
        )

    # Simulated live replay branch
    if replay_limit > 0:
        logger.info("Starting live replay branch")

        replay_info = publish_kafka_replay(
            limit=replay_limit
        )

        results["live"] = verify_kafka_processing(
            replay_info
        )

    if not results:
        logger.info(
            "No branch selected. Nothing to execute."
        )

    logger.info("Inventory orchestration completed")

    if run_quality:
        logger.info("Starting data quality validation")
        results["quality"] = validate_pipeline_quality()
        
    return results





if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--replay-limit",
        type=int,
        default=0,
    )

    parser.add_argument(
        "--skip-historical",
        action="store_true",
    )

    args = parser.parse_args()

    result = inventory_pipeline(
        run_historical=not args.skip_historical,
        replay_limit=args.replay_limit,
    )

    print(result)
