
import json
import os
from pathlib import Path

import pika
from dotenv import load_dotenv


load_dotenv()


def get_connection():
    credentials = pika.PlainCredentials(
        os.getenv("RABBITMQ_USER"),
        os.getenv("RABBITMQ_PASSWORD"),
    )

    return pika.BlockingConnection(
        pika.ConnectionParameters(
            host=os.getenv("RABBITMQ_HOST", "localhost"),
            port=int(os.getenv("RABBITMQ_PORT", "5672")),
            credentials=credentials,
        )
    )


def publish_file_event(file_path: Path):
    file_path = file_path.resolve()

    if not file_path.is_file():
        raise FileNotFoundError(file_path)

    connection = get_connection()

    try:
        channel = connection.channel()

        queue = os.getenv(
            "RABBITMQ_QUEUE", "historical.file.ready"
        )

        channel.queue_declare(queue=queue, durable=True, arguments={"x-single-active-consumer": True}),

        message = {
            "event_type": "historical.file.ready",
            "file_path": str(file_path),
        }

        channel.confirm_delivery()

        channel.basic_publish(
            exchange="",
            routing_key=queue,
            body=json.dumps(message).encode("utf-8"),
            properties=pika.BasicProperties(
                delivery_mode=2,
                content_type="application/json",
            ),
            mandatory=True,
        )

        print("Event published successfully!")
        print(json.dumps(message, indent=2))

    finally:
        connection.close()


if __name__ == "__main__":
    publish_file_event(
        Path("data/historical/blocked_stock_2026_08_17.parquet")
    )
