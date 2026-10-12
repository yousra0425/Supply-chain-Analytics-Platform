
import argparse
import json
import math
from datetime import datetime, timezone
import os
from kafka import KafkaConsumer, KafkaProducer, TopicPartition
from cassandra.cluster import Cluster
from src.utils.cassandra_connection import CassandraConnection

BOOTSTRAP_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS",
    "localhost:9092"
)
DLQ_TOPIC = "inventory.v2.dead-letter"
TEST_TOPIC = "inventory.v2.recovery-test"
RAW_TOPIC = "inventory.v2.raw"


def utc_now():
    return datetime.now(timezone.utc)


def read_failed_message(offset, partition=0):
    consumer = KafkaConsumer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        enable_auto_commit=False,
        value_deserializer=lambda b: json.loads(
            b.decode("utf-8")
        ),
    )

    try:
        tp = TopicPartition(DLQ_TOPIC, partition)
        consumer.assign([tp])
        consumer.seek(tp, offset)

        messages = consumer.poll(
            timeout_ms=10000,
            max_records=1,
        )

        for message in messages.get(tp, []):
            if message.offset == offset:
                return message.value

        raise ValueError(
            f"DLQ message not found: {partition}:{offset}"
        )
    finally:
        consumer.close()


def validate_event(event):
    required = (
        "event_id",
        "material",
        "plant",
        "blocked_quantity",
        "inventory_value",
        "raw_data",
    )

    for field in required:
        if event.get(field) is None:
            raise ValueError(f"Missing field: {field}")

    if not isinstance(event["raw_data"], dict):
        raise ValueError("raw_data must be an object")

    for field in ("event_id", "material", "plant"):
        if not str(event[field]).strip():
            raise ValueError(f"Empty field: {field}")

    for field in ("blocked_quantity", "inventory_value"):
        value = float(event[field])

        if not math.isfinite(value) or value < 0:
            raise ValueError(
                f"Invalid {field}: {value}"
            )

        event[field] = value

    return event


def connect_cassandra():
    cluster = Cluster(
       [os.getenv("CASSANDRA_HOST", "127.0.0.1")],
        port=9042,
        connection_class=CassandraConnection,        
        connect_timeout=15,
        control_connection_timeout=15,
    )
    try:
        session = cluster.connect("inventory_v2")
        return cluster, session
    except Exception:
        cluster.shutdown()
        raise


def get_recovery(session, dlq_id):
    return session.execute(
        """
        SELECT *
        FROM dlq_recovery_ledger
        WHERE dlq_message_id = %s
        """,
        (dlq_id,),
    ).one()


def publish_event(event, topic):
    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_serializer=lambda value: json.dumps(
            value,
            ensure_ascii=False,
        ).encode("utf-8"),
        acks="all",
        retries=3,
    )

    try:
        return producer.send(
            topic,
            value=event,
        ).get(timeout=30)
    finally:
        producer.close(timeout=10)


def main():
    parser = argparse.ArgumentParser(
        description="Recover a Kafka dead-letter event"
    )

    parser.add_argument("--offset", type=int, required=True)
    parser.add_argument("--partition", type=int, default=0)
    parser.add_argument("--quantity", type=float)
    parser.add_argument("--publish", action="store_true")

    args = parser.parse_args()

    if args.offset < 0 or args.partition < 0:
        parser.error("Offset and partition must be nonnegative")

    dlq_id = (
        f"{DLQ_TOPIC}:{args.partition}:{args.offset}"
    )

    cluster, session = connect_cassandra()

    try:
        # Check recovery status before doing anything else.
        existing = get_recovery(session, dlq_id)

        if existing:
            print("Recovery already registered.")
            print("Status:", existing.recovery_status)
            print("Destination:", existing.destination_topic)
            print("Offset:", existing.destination_offset)

            # Never automatically retry an existing claim.
            print("No new message published.")
            return

        failed = read_failed_message(
            args.offset,
            args.partition,
        )

        event = dict(failed["event"])

        print("\nOriginal failed event:")
        print(json.dumps(failed, indent=2))

        if args.quantity is not None:
            event["blocked_quantity"] = args.quantity

        event = validate_event(event)

        is_test = (
            str(event["event_id"]).startswith("invalid-test-")
            or str(event["material"]).startswith("TEST-")
        )

        destination = TEST_TOPIC if is_test else RAW_TOPIC

        event["recovery"] = {
            "original_message_id": failed["message_id"],
            "dlq_message_id": dlq_id,
            "recovered_at": utc_now().isoformat(),
        }

        print("\nCorrected event:")
        print(json.dumps(event, indent=2))
        print("Destination:", destination)

        if not args.publish:
            print("\nDRY RUN: Nothing published.")
            return

        # Atomically reserve this DLQ message.
        # Prevent two script instances publishing it together.
        claim = session.execute(
            """
            INSERT INTO dlq_recovery_ledger (
                dlq_message_id,
                original_message_id,
                destination_topic,
                recovery_status,
                recovered_at
            )
            VALUES (%s, %s, %s, %s, %s)
            IF NOT EXISTS
            """,
            (
                dlq_id,
                failed["message_id"],
                destination,
                "PUBLISHING",
                utc_now(),
            ),
        ).one()

        if not claim or not claim.applied:
            print("Another recovery already claimed this event.")
            return

        # Publish to Kafka and wait for acknowledgement.
        # If publication outcome is uncertain, preserve the
        # PUBLISHING state for manual investigation.
        metadata = publish_event(event, destination)

        session.execute(
            """
            UPDATE dlq_recovery_ledger
            SET recovery_status = %s,
                destination_partition = %s,
                destination_offset = %s,
                recovered_at = %s
            WHERE dlq_message_id = %s
            IF recovery_status = %s
            """,
            (
                "PUBLISHED",
                metadata.partition,
                metadata.offset,
                utc_now(),
                dlq_id,
                "PUBLISHING",
            ),
        )

        print("\nRecovery event published.")
        print("Topic:", destination)
        print("Partition:", metadata.partition)
        print("Offset:", metadata.offset)

    finally:
        cluster.shutdown()


if __name__ == "__main__":
    main()
