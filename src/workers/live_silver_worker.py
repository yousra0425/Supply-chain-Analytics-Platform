
import json
import os

import pika
from cassandra.cluster import Cluster
from src.utils.cassandra_connection import CassandraConnection
from src.messaging.publisher import get_connection
from src.gold.live_updater import update_live_gold


QUEUE = "inventory.v2.live.ready"
FAILED_QUEUE = "inventory.v2.live.failed"


def main():
    cluster = Cluster(
        [os.getenv("CASSANDRA_HOST", "127.0.0.1")],
        port=int(os.getenv("CASSANDRA_PORT", "9042")),
        connection_class=CassandraConnection,
    )

    connection = None

    try:
        session = cluster.connect("inventory_v2")

        select_record = session.prepare("""
            SELECT event_id, raw_data, source_date
            FROM blocked_stock_events
            WHERE event_id = ?
        """)

        connection = get_connection()
        channel = connection.channel()

        channel.queue_declare(queue=QUEUE, durable=True)
        channel.queue_declare(queue=FAILED_QUEUE, durable=True)

        channel.basic_qos(prefetch_count=1)
        channel.confirm_delivery()

        def on_message(ch, method, properties, body):
            try:
                event = json.loads(body)

                if event.get("event_type") != "inventory.live.ready":
                    raise ValueError("Unexpected event type")

                event_id = event.get("event_id")

                if not isinstance(event_id, str) or not event_id:
                    raise ValueError("Missing event ID")

            except (ValueError, TypeError) as exc:
                # Invalid notifications are moved to a failed queue.
                try:
                    failed = {
                        "error": str(exc),
                        "original_message": body.decode(
                            "utf-8", errors="replace"
                        ),
                    }

                    ch.basic_publish(
                        exchange="",
                        routing_key=FAILED_QUEUE,
                        body=json.dumps(failed).encode("utf-8"),
                        properties=pika.BasicProperties(
                            delivery_mode=2
                        ),
                        mandatory=True,
                    )

                    ch.basic_ack(
                        delivery_tag=method.delivery_tag
                    )

                    print(f"Invalid notification: {exc}")

                except Exception:
                    ch.basic_nack(
                        delivery_tag=method.delivery_tag,
                        requeue=True,
                    )
                    ch.stop_consuming()

                return

            try:
                print(f"\nProcessing: {event_id[:12]}")

                row = session.execute(
                    select_record, [event_id]
                ).one()

                if row is None:
                    raise LookupError(
                        f"Event missing from Cassandra: {event_id}"
                    )

                update_live_gold(
                    event_id=row.event_id,
                    raw_data=row.raw_data,
                    source_date=row.source_date,
                )

                # Acknowledge only after ClickHouse accepts the insert.
                ch.basic_ack(
                    delivery_tag=method.delivery_tag
                )

                print(f"Gold updated: {event_id[:12]}")

            except Exception as exc:
                print(f"Processing failed: {exc}")

                # Preserve the message for retry and stop this worker.
                # This avoids continuously retrying in a tight loop.
                ch.basic_nack(
                    delivery_tag=method.delivery_tag,
                    requeue=True,
                )
                ch.stop_consuming()

        channel.basic_consume(
            queue=QUEUE,
            on_message_callback=on_message,
            auto_ack=False,
        )

        print("Live Silver worker running...")
        print(f"Waiting on queue: {QUEUE}")
        print("Press Ctrl+C to stop.")

        try:
            channel.start_consuming()
        except KeyboardInterrupt:
            print("\nWorker stopped.")
            if channel.is_open:
                channel.stop_consuming()

    finally:
        if connection is not None and connection.is_open:
            connection.close()

        cluster.shutdown()


if __name__ == "__main__":
    main()
