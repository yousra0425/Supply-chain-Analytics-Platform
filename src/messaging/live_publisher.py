
import json

import pika

from src.messaging.publisher import get_connection


LIVE_QUEUE = "inventory.v2.live.ready"


def publish_live_event(event_id):
    connection = get_connection()

    try:
        channel = connection.channel()

        channel.queue_declare(
            queue=LIVE_QUEUE,
            durable=True,
        )

        channel.confirm_delivery()

        message = {
            "event_type": "inventory.live.ready",
            "event_id": event_id,
        }

        channel.basic_publish(
            exchange="",
            routing_key=LIVE_QUEUE,
            body=json.dumps(message).encode("utf-8"),
            properties=pika.BasicProperties(
                delivery_mode=2,
                content_type="application/json",
            ),
            mandatory=True,
        )

        print(f"Live event published: {event_id[:12]}")

    finally:
        connection.close()
