import os
from datetime import timezone

from kafka import KafkaConsumer, TopicPartition
from cassandra.cluster import Cluster
from src.utils.cassandra_connection import CassandraConnection
TOPIC = "inventory.v2.raw"
GROUP_ID = "inventory-v2-cassandra-consumer"


def get_kafka_metrics():
    consumer = KafkaConsumer(
        bootstrap_servers=[
            os.getenv("KAFKA_BOOTSTRAP_SERVERS", "127.0.0.1:9092")
        ],
        group_id=GROUP_ID,
        enable_auto_commit=False,
    )

    try:
        partitions = consumer.partitions_for_topic(TOPIC)

        if not partitions:
            raise RuntimeError("Kafka topic is unavailable")

        topic_partitions = [
            TopicPartition(TOPIC, p)
            for p in sorted(partitions)
        ]

        beginning = consumer.beginning_offsets(topic_partitions)
        end = consumer.end_offsets(topic_partitions)

        published = 0
        committed = 0
        lag = 0

        for tp in topic_partitions:
            start = beginning[tp]
            latest = end[tp]
            position = consumer.committed(tp)

            published += latest - start

            if position is not None:
                committed += max(
                    0, min(position, latest) - start
                )
                lag += max(0, latest - position)
            else:
                lag += latest - start

        return {
            "topic": TOPIC,
            "consumer_group": GROUP_ID,
            "published_messages": published,
            "committed_messages": committed,
            "consumer_lag": lag,
            "partitions": len(topic_partitions),
        }

    finally:
        consumer.close()


def get_cassandra_metrics():
    cluster = Cluster(
        [os.getenv("CASSANDRA_HOST", "127.0.0.1")],
        port=9042,
        connection_class=CassandraConnection,    connect_timeout=15,
        control_connection_timeout=15,
    )

    try:
        session = cluster.connect("inventory_v2")

        # Acceptable for our small development dataset.
        # Replace with a monitoring table as data grows.
        statement = session.prepare(
            "SELECT received_at FROM blocked_stock_events"
        )
        statement.fetch_size = 500

        rows = session.execute(statement)

        count = 0
        last_received = None

        for row in rows:
            count += 1

            if row.received_at is not None:
                if (
                    last_received is None
                    or row.received_at > last_received
                ):
                    last_received = row.received_at

        if last_received is not None:
            if last_received.tzinfo is None:
                last_received = last_received.replace(
                    tzinfo=timezone.utc
                )

        return {
            "stored_records": count,
            "last_received_at": (
                last_received.isoformat()
                if last_received else None
            ),
        }

    finally:
        cluster.shutdown()


def get_live_metrics():
    kafka = get_kafka_metrics()
    cassandra = get_cassandra_metrics()

    return {
        "kafka": kafka,
        "cassandra": cassandra,
    }
