
import json
import os
from pathlib import Path

from dotenv import load_dotenv

from src.messaging.publisher import get_connection
from src.workers.historical_worker import process_snapshot


load_dotenv()

QUEUE = os.getenv("RABBITMQ_QUEUE", "historical.file.ready")
COMPLETED_QUEUE = "historical.file.completed"


def main():
    connection = get_connection()
    channel = connection.channel()

    # One active consumer across all instances of this queue
    channel.queue_declare(
        queue=QUEUE,
        durable=True,
        arguments={"x-single-active-consumer": True},
    )

    channel.queue_declare(
        queue=COMPLETED_QUEUE,
        durable=True,
    )

    channel.basic_qos(prefetch_count=1)
    channel.confirm_delivery()

    def on_message(ch, method, properties, body):
        try:
            event = json.loads(body)

            if event.get("event_type") != "historical.file.ready":
                raise ValueError("Unexpected event type")

            file_path = Path(event["file_path"])

            # Accept only snapshots from our historical directory
            allowed_dir = Path("data/historical").resolve()

            if file_path.resolve().parent != allowed_dir:
                raise ValueError("File is outside historical directory")

            if file_path.suffix.lower() != ".parquet":
                raise ValueError("Only Parquet snapshots are supported")

            print(f"\nReceived: {file_path.name}")

            process_snapshot(file_path)

            completion = {
                "event_type": "historical.file.completed",
                "file_path": str(file_path),
            }

            ch.basic_publish(
                exchange="",
                routing_key=COMPLETED_QUEUE,
                body=json.dumps(completion).encode(),
                properties=__import__("pika").BasicProperties(
                    delivery_mode=2,
                    content_type="application/json",
                ),
                mandatory=True,
            )

            ch.basic_ack(delivery_tag=method.delivery_tag)

            print("Completed and acknowledged.")

        except Exception as exc:
            print(f"Processing error: {exc}")
            ch.basic_nack(
                delivery_tag=method.delivery_tag,
                requeue=False,
            )

    channel.basic_consume(
        queue=QUEUE,
        on_message_callback=on_message,
        auto_ack=False,
    )

    print("Historical consumer waiting for events...")
    print("Press Ctrl+C to stop.")

    try:
        channel.start_consuming()
    except KeyboardInterrupt:
        channel.stop_consuming()
    finally:
        connection.close()


if __name__ == "__main__":
    main()
