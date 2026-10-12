
import json
import time
from datetime import datetime, timezone
import os
from kafka import KafkaConsumer
from cassandra.cluster import Cluster
from src.utils.cassandra_connection import CassandraConnection
from cassandra.util import Date

from src.messaging.live_publisher import publish_live_event
from src.streaming.dead_letter import publish_dead_letter


TOPIC = "inventory.v2.raw"
BOOTSTRAP_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS",
    "localhost:9092"
)
GROUP_ID = "inventory-v2-cassandra-consumer"

MAX_ATTEMPTS = 3
RETRY_DELAY = 2


def utc_now():
    return datetime.now(timezone.utc)


def process_inventory_event(
    session,
    event,
    message_id,
    received_at,
    insert_bronze,
    insert_recent_v2,
):
    event_id = event["event_id"]

    blocked_quantity = (
        float(event["blocked_quantity"])
        if event.get("blocked_quantity") is not None
        else None
    )

    inventory_value = (
        float(event["inventory_value"])
        if event.get("inventory_value") is not None
        else None
    )

    # Step 1: Cassandra Bronze
    session.execute(
        insert_bronze,
        (
            event_id,
            event.get("material"),
            event.get("plant"),
            event.get("batch_number"),
            blocked_quantity,
            inventory_value,
            event.get("currency"),
            event.get("source_date"),
            received_at,
            json.dumps(
                event["raw_data"],
                ensure_ascii=False,
            ),
        ),
    )

    print(f"Bronze stored: {event.get('material')}")

    # Step 2: Cassandra Recent Events V2
    session.execute(
        insert_recent_v2,
        (
            Date(received_at.date()),
            received_at,
            message_id,
            event_id,
            event.get("material"),
            event.get("plant"),
            event.get("batch_number"),
            blocked_quantity,
            inventory_value,
            event.get("source_date"),
        ),
    )

    print(f"Recent event stored: {message_id}")

    # Step 3: RabbitMQ notification
    publish_live_event(event_id)

    print(f"RabbitMQ notified: {event_id[:12]}")


def main():
    cluster = Cluster(
        [os.getenv("CASSANDRA_HOST", "127.0.0.1")],
        port=int(os.getenv("CASSANDRA_PORT", "9042")),
        connection_class=CassandraConnection,
    )
    consumer = None

    try:
        session = cluster.connect("inventory_v2")

        insert_bronze = session.prepare("""
            INSERT INTO blocked_stock_events (
                event_id,
                material,
                plant,
                batch_number,
                blocked_quantity,
                inventory_value,
                currency,
                source_date,
                received_at,
                raw_data
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """)

        insert_recent_v2 = session.prepare("""
            INSERT INTO recent_inventory_events_v2 (
                event_date,
                received_at,
                message_id,
                event_id,
                material,
                plant,
                batch_number,
                blocked_quantity,
                blocked_value,
                source_date
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """)

        create_ledger = session.prepare("""
            INSERT INTO kafka_processing_ledger (
                message_id,
                event_id,
                first_received_at,
                status
            )
            VALUES (?, ?, ?, ?)
            IF NOT EXISTS
        """)

        read_ledger = session.prepare("""
            SELECT
                event_id,
                first_received_at,
                status
            FROM kafka_processing_ledger
            WHERE message_id = ?
        """)

        update_status = session.prepare("""
            UPDATE kafka_processing_ledger
            SET status = ?,
                processed_at = ?
            WHERE message_id = ?
            IF status = ?
        """)

        consumer = KafkaConsumer(
            TOPIC,
            bootstrap_servers=BOOTSTRAP_SERVERS,
            group_id=GROUP_ID,
            auto_offset_reset="earliest",
            enable_auto_commit=False,
            max_poll_interval_ms=600000,
            value_deserializer=lambda value: json.loads(
                value.decode("utf-8")
            ),
        )

        print("Kafka consumer is running...")
        print("Idempotency protection enabled.")
        print("Retry and dead-letter handling enabled.")
        print("Press Ctrl+C to stop.")

        for message in consumer:
            message_id = (
                f"{message.topic}:"
                f"{message.partition}:"
                f"{message.offset}"
            )

            event = message.value

            # Create a stable processing record.
            # A missing event ID is handled as a processing error.
            event_id = (
                event.get("event_id", "")
                if isinstance(event, dict)
                else ""
            )

            session.execute(
                create_ledger,
                (
                    message_id,
                    event_id,
                    utc_now(),
                    "PROCESSING",
                ),
            )

            ledger = session.execute(
                read_ledger,
                (message_id,),
            ).one()

            if ledger is None:
                raise RuntimeError(
                    f"Ledger entry missing: {message_id}"
                )

            if ledger.event_id != event_id:
                raise RuntimeError(
                    f"Event ID mismatch: {message_id}"
                )

            if ledger.status in ("COMPLETED", "DEAD_LETTERED"):
                print(
                    f"Already handled: {message_id} "
                    f"({ledger.status})"
                )
                consumer.commit()
                continue

            if ledger.status != "PROCESSING":
                raise RuntimeError(
                    f"Unexpected status: {ledger.status}"
                )

            received_at = ledger.first_received_at

            if received_at is None:
                raise RuntimeError(
                    f"Missing timestamp: {message_id}"
                )

            if received_at.tzinfo is None:
                received_at = received_at.replace(
                    tzinfo=timezone.utc
                )

            last_error = None
            succeeded = False

            # Step 1: Retry processing
            for attempt in range(1, MAX_ATTEMPTS + 1):
                try:
                    print(
                        f"Processing {message_id} "
                        f"(attempt {attempt}/{MAX_ATTEMPTS})"
                    )

                    process_inventory_event(
                        session=session,
                        event=event,
                        message_id=message_id,
                        received_at=received_at,
                        insert_bronze=insert_bronze,
                        insert_recent_v2=insert_recent_v2,
                    )

                    # Mark completed before committing Kafka.
                    result = session.execute(
                        update_status,
                        (
                            "COMPLETED",
                            utc_now(),
                            message_id,
                            "PROCESSING",
                        ),
                    ).one()

                    if not result or not result.applied:
                        raise RuntimeError(
                            f"Ledger completion failed: {message_id}"
                        )

                    # Success: safe to commit.
                    consumer.commit()

                    print(f"Completed: {message_id}")

                    succeeded = True
                    break

                except Exception as exc:
                    last_error = exc

                    print(
                        f"Attempt {attempt} failed "
                        f"for {message_id}: {exc}"
                    )

                    # A completion might already have succeeded
                    # even if the response or Kafka commit failed.
                    # Recheck before retrying side effects.
                    current = session.execute(
                        read_ledger,
                        (message_id,),
                    ).one()

                    if current and current.status == "COMPLETED":
                        consumer.commit()
                        print(
                            f"Completion confirmed: {message_id}"
                        )
                        succeeded = True
                        break

                    if attempt < MAX_ATTEMPTS:
                        delay = RETRY_DELAY * (2 ** (attempt - 1))
                        print(f"Retrying in {delay} seconds...")
                        time.sleep(delay)

            if succeeded:
                continue

            # Step 2: All processing attempts failed
            print(
                f"Maximum attempts reached: {message_id}"
            )

            # Publish the failed event to the dead-letter topic.
            # This must be acknowledged before offset commit.
            try:
                publish_dead_letter(
                    message=message,
                    error=last_error,
                    attempts=MAX_ATTEMPTS,
                )

                result = session.execute(
                    update_status,
                    (
                        "DEAD_LETTERED",
                        utc_now(),
                        message_id,
                        "PROCESSING",
                    ),
                ).one()

                if not result or not result.applied:
                    # Another state may have been recorded.
                    current = session.execute(
                        read_ledger,
                        (message_id,),
                    ).one()

                    if not current or current.status not in (
                        "COMPLETED",
                        "DEAD_LETTERED",
                    ):
                        raise RuntimeError(
                            f"DLQ ledger update failed: {message_id}"
                        )

                consumer.commit()

                print(
                    f"Dead-lettered and committed: {message_id}"
                )

            except Exception as exc:
                print(
                    f"Dead-letter handling failed: {exc}"
                )
                print(
                    "Kafka offset not committed. "
                    "Stopping consumer for recovery."
                )
                raise

    except KeyboardInterrupt:
        print("\nKafka consumer stopped.")

    finally:
        if consumer is not None:
            consumer.close()

        cluster.shutdown()


if __name__ == "__main__":
    main()
